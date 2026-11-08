import express, { NextFunction, Request, Response } from 'express';
import { randomUUID } from 'crypto';
import { ZodError } from 'zod';
import { Logger } from './logger';
import { httpDuration, registry } from './metrics';

export class HttpError extends Error {
  constructor(public status: number, message: string, public details?: unknown, public headers?: Record<string, string>) {
    super(message);
  }
}

declare global {
  // eslint-disable-next-line @typescript-eslint/no-namespace
  namespace Express {
    interface Request {
      requestId: string;
      log: Logger;
      user?: { id: string; role: 'CUSTOMER' | 'AGENT' };
    }
  }
}

export interface AppOptions {
  log: Logger;
  /** If set, every non-infra route requires header x-internal-token to match (gateway -> service trust). */
  internalToken?: string;
  readiness?: () => Promise<void>;
}

/** Express app with request IDs, JSON logs, metrics, health/readiness and uniform error handling. */
export function createApp(opts: AppOptions) {
  const app = express();
  app.disable('x-powered-by');
  app.use(express.json({ limit: '100kb' }));

  app.use((req, res, next) => {
    const incoming = req.header('x-request-id');
    req.requestId = incoming && incoming.length <= 128 ? incoming : randomUUID();
    res.setHeader('X-Request-Id', req.requestId);
    req.log = opts.log.child({ requestId: req.requestId });
    const start = process.hrtime.bigint();
    res.on('finish', () => {
      const seconds = Number(process.hrtime.bigint() - start) / 1e9;
      const route = (req.route?.path as string | undefined) ?? (res.statusCode === 404 ? 'unmatched' : req.path);
      httpDuration.observe({ method: req.method, route, status: String(res.statusCode) }, seconds);
      if (req.path !== '/metrics' && req.path !== '/health') {
        req.log.info({ method: req.method, path: req.originalUrl, status: res.statusCode, ms: Math.round(seconds * 1000) }, 'request');
      }
    });
    next();
  });

  app.get('/health', (_req, res) => { res.json({ status: 'ok' }); });
  app.get('/ready', async (_req, res) => {
    try {
      await opts.readiness?.();
      res.json({ status: 'ready' });
    } catch (err) {
      res.status(503).json({ status: 'not_ready', error: (err as Error).message });
    }
  });
  app.get('/metrics', async (_req, res) => {
    res.setHeader('Content-Type', registry.contentType);
    res.send(await registry.metrics());
  });

  if (opts.internalToken) {
    app.use((req, _res, next) => {
      if (req.header('x-internal-token') !== opts.internalToken) return next(new HttpError(401, 'missing or invalid internal token'));
      const id = req.header('x-user-id');
      const role = req.header('x-user-role');
      if (id && (role === 'CUSTOMER' || role === 'AGENT')) req.user = { id, role };
      next();
    });
  }
  return app;
}

/** Registers 404 + error handlers. Call after all routes. */
export function finalizeApp(app: express.Express) {
  app.use((_req, _res, next) => next(new HttpError(404, 'not found')));
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  app.use((err: unknown, req: Request, res: Response, _next: NextFunction) => {
    if (err instanceof ZodError) {
      return res.status(400).json({ error: 'validation_error', issues: err.issues.map((i) => ({ path: i.path.join('.'), message: i.message })) });
    }
    if (err instanceof HttpError) {
      if (err.headers) for (const [k, v] of Object.entries(err.headers)) res.setHeader(k, v);
      return res.status(err.status).json({ error: err.message, details: err.details });
    }
    if ((err as { type?: string }).type === 'entity.parse.failed') return res.status(400).json({ error: 'invalid JSON body' });
    req.log?.error({ err }, 'unhandled error');
    return res.status(500).json({ error: 'internal_error', requestId: req.requestId });
  });
}

export function requireUser(req: Request) {
  if (!req.user) throw new HttpError(401, 'authentication required');
  return req.user;
}
