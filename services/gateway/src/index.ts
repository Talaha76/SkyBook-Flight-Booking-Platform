import fs from 'fs';
import path from 'path';
import { NextFunction, Request, Response } from 'express';
import Redis from 'ioredis';
import swaggerUi from 'swagger-ui-express';
import YAML from 'yaml';
import { z } from 'zod';
import {
  HttpError,
  createApp,
  createLogger,
  env,
  envInt,
  finalizeApp,
  onShutdown,
  promClient,
  registry,
  runMain,
  setServiceLabel,
} from '../../../packages/shared/src';
import { issueToken, login, verifyToken } from './auth';

const SERVICE = 'gateway';
const log = createLogger(SERVICE);
setServiceLabel(SERVICE);

const rateLimited = new promClient.Counter({ name: 'gateway_rate_limited_total', help: 'Requests rejected by the rate limiter', registers: [registry] });

const HOP_HEADERS = ['content-type', 'retry-after', 'idempotent-replayed', 'x-cache', 'location'];

runMain(log, async () => {
  const jwtSecret = env('JWT_SECRET', 'dev-only-change-me-please-32-chars-min');
  const internalToken = env('INTERNAL_TOKEN', 'dev-internal-token');
  const demoPassword = env('DEMO_PASSWORD', 'skybook');
  const rateLimit = envInt('RATE_LIMIT_PER_MINUTE', 600);
  const upstreams = {
    flights: env('FLIGHT_SEARCH_URL', 'http://localhost:3001'),
    booking: env('BOOKING_URL', 'http://localhost:3002'),
    inventory: env('INVENTORY_URL', 'http://localhost:3003'),
    payment: env('PAYMENT_URL', 'http://localhost:3004'),
    notification: env('NOTIFICATION_URL', 'http://localhost:3005'),
  };

  const redis = new Redis(env('REDIS_URL', 'redis://localhost:6379'), { maxRetriesPerRequest: 1, enableOfflineQueue: false });
  redis.on('error', (err) => log.warn({ err: err.message }, 'redis error'));

  const app = createApp({ log });

  // ---- docs ---------------------------------------------------------------------------------
  const specPath = process.env.OPENAPI_PATH ?? path.join(process.cwd(), 'services/gateway/openapi.yaml');
  const spec = YAML.parse(fs.readFileSync(specPath, 'utf8'));
  app.get('/openapi.json', (_req, res) => { res.json(spec); });
  app.use('/docs', swaggerUi.serve, swaggerUi.setup(spec));

  // ---- authentication (optional per route) ---------------------------------------------------
  app.use((req, _res, next) => {
    const user = verifyToken(req.header('authorization'), jwtSecret);
    if (user) req.user = user;
    next();
  });

  // ---- rate limiting: fixed one-minute window per user (or IP) in Redis; fails open --------------
  app.use(async (req, res, next) => {
    if (['/health', '/ready', '/metrics'].includes(req.path) || req.path.startsWith('/docs')) return next();
    const who = req.user?.id ?? req.ip ?? 'anon';
    const windowKey = `rl:${who}:${Math.floor(Date.now() / 60_000)}`;
    try {
      const n = await redis.incr(windowKey);
      if (n === 1) await redis.expire(windowKey, 61);
      res.setHeader('X-RateLimit-Limit', String(rateLimit));
      res.setHeader('X-RateLimit-Remaining', String(Math.max(0, rateLimit - n)));
      if (n > rateLimit) {
        rateLimited.inc();
        return next(new HttpError(429, 'rate limit exceeded', undefined, { 'Retry-After': String(60 - (Math.floor(Date.now() / 1000) % 60)) }));
      }
    } catch (err) {
      req.log.warn({ err: (err as Error).message }, 'rate limiter unavailable, allowing request');
    }
    next();
  });

  const auth = (role?: 'AGENT') => (req: Request, _res: Response, next: NextFunction) => {
    if (!req.user) return next(new HttpError(401, 'authentication required'));
    if (role && req.user.role !== role) return next(new HttpError(403, `${role} role required`));
    next();
  };

  async function proxy(req: Request, res: Response, base: string) {
    const headers: Record<string, string> = {
      'x-request-id': req.requestId,
      'x-internal-token': internalToken,
      accept: 'application/json',
    };
    if (req.user) { headers['x-user-id'] = req.user.id; headers['x-user-role'] = req.user.role; }
    const idem = req.header('idempotency-key');
    if (idem) headers['idempotency-key'] = idem;
    const hasBody = !['GET', 'HEAD'].includes(req.method);
    if (hasBody) headers['content-type'] = 'application/json';
    let upstream: globalThis.Response;
    try {
      upstream = await fetch(`${base}${req.originalUrl}`, {
        method: req.method,
        headers,
        body: hasBody ? JSON.stringify(req.body ?? {}) : undefined,
        signal: AbortSignal.timeout(envInt('UPSTREAM_TIMEOUT_MS', 10_000)),
      });
    } catch (err) {
      const timeout = (err as Error).name === 'TimeoutError';
      req.log.error({ err: (err as Error).message, base }, 'upstream call failed');
      throw new HttpError(timeout ? 504 : 502, timeout ? 'upstream timeout' : 'upstream unavailable');
    }
    for (const h of HOP_HEADERS) {
      const v = upstream.headers.get(h);
      if (v) res.setHeader(h, v);
    }
    res.status(upstream.status).send(Buffer.from(await upstream.arrayBuffer()));
  }

  // ---- routes ---------------------------------------------------------------------------------
  app.post('/auth/login', (req, res) => {
    const body = z.object({ username: z.string().min(1), password: z.string().min(1) }).parse(req.body);
    const user = login(body.username, body.password, demoPassword);
    res.json({ accessToken: issueToken(user, jwtSecret), tokenType: 'Bearer', expiresIn: 3600, user });
  });

  app.get('/flights/search', (req, res) => proxy(req, res, upstreams.flights));
  app.get('/flights/:id', (req, res) => proxy(req, res, upstreams.flights));
  app.get('/airports', (req, res) => proxy(req, res, upstreams.flights));

  app.post('/bookings', auth(), (req, res) => proxy(req, res, upstreams.booking));
  app.get('/bookings', auth(), (req, res) => proxy(req, res, upstreams.booking));
  app.get('/bookings/:id', auth(), (req, res) => proxy(req, res, upstreams.booking));

  app.get('/inventory/:flightId', auth('AGENT'), (req, res) => proxy(req, res, upstreams.inventory));
  app.get('/payments/:bookingId', auth('AGENT'), (req, res) => proxy(req, res, upstreams.payment));
  app.get('/notifications', auth('AGENT'), (req, res) => proxy(req, res, upstreams.notification));

  finalizeApp(app);
  const port = envInt('PORT', 8080);
  const server = app.listen(port, () => log.info({ port, upstreams }, 'gateway listening'));
  onShutdown(log, [() => redis.quit(), () => server.close()]);
});
