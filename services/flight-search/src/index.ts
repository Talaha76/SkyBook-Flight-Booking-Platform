import Redis from 'ioredis';
import { z } from 'zod';
import {
  EventHandler,
  EventTypes,
  HttpError,
  TOPICS,
  createApp,
  createKafka,
  createLogger,
  createPool,
  ensureTopics,
  env,
  envBool,
  envInt,
  finalizeApp,
  onShutdown,
  runMain,
  runMigrations,
  setServiceLabel,
  startConsumer,
  waitForDb,
} from '../../../packages/shared/src';
import { FLIGHT_MIGRATIONS } from './migrations';
import { SearchQuery, cacheKey, createSearch, toFlightDto } from './search';

const SERVICE = 'flight-search';
const log = createLogger(SERVICE);
setServiceLabel(SERVICE);

runMain(log, async () => {
  const pool = createPool(env('DATABASE_URL'), 20);
  await waitForDb(pool, log);
  await runMigrations(pool, SERVICE, FLIGHT_MIGRATIONS, log);

  const redis = new Redis(env('REDIS_URL', 'redis://localhost:6379'), { maxRetriesPerRequest: 1, enableOfflineQueue: false, lazyConnect: false });
  redis.on('error', (err) => log.warn({ err: err.message }, 'redis error'));
  const search = createSearch({ pool, redis, log, ttlSeconds: envInt('SEARCH_CACHE_TTL_SECONDS', 60), enabled: envBool('SEARCH_CACHE_ENABLED', true) });

  // Read model: keep seats_available in sync with Inventory and invalidate the cached search.
  const syncAvailability: EventHandler = async ({ client, event, afterCommit }) => {
    const p = z.object({ flightId: z.string().uuid(), available: z.number().int() }).parse(event.payload);
    const { rows } = await client.query(
      `UPDATE flights SET seats_available = $2, updated_at = now() WHERE id = $1
       RETURNING origin, destination, to_char(departure_date, 'YYYY-MM-DD') AS date`,
      [p.flightId, p.available],
    );
    if (rows[0]) {
      const key = cacheKey({ origin: rows[0].origin, destination: rows[0].destination, date: rows[0].date });
      afterCommit(async () => { await redis.del(key); });
    }
  };

  const kafka = createKafka(SERVICE, env('KAFKA_BROKERS', 'localhost:9092'), log);
  await ensureTopics(kafka, log);
  const consumer = await startConsumer({
    kafka,
    pool,
    groupId: 'flight-search-cache',
    topics: [TOPICS.inventory],
    handlers: { [EventTypes.SeatHeld]: syncAvailability, [EventTypes.SeatReleased]: syncAvailability, [EventTypes.SeatConfirmed]: syncAvailability },
    log,
  });

  // Search is public; the gateway still adds the internal token.
  const app = createApp({
    log,
    internalToken: env('INTERNAL_TOKEN', 'dev-internal-token'),
    readiness: async () => { await pool.query('SELECT 1'); },
  });

  app.get('/flights/search', async (req, res) => {
    const q = SearchQuery.parse(req.query);
    // Benchmark control: internal callers (already authenticated by x-internal-token) may skip the cache.
    // The gateway builds upstream headers explicitly and never forwards this one.
    const result = await search(q, { bypassCache: req.header('x-cache-bypass') === '1' });
    res.setHeader('X-Cache', result.cache);
    res.json({ query: q, count: result.flights.length, flights: result.flights });
  });

  app.get('/flights/:id', async (req, res) => {
    const id = z.string().uuid().parse(req.params.id);
    const { rows } = await pool.query(
      `SELECT id, flight_number, airline, origin, destination, to_char(departure_date, 'YYYY-MM-DD') AS departure_date,
              departure_time, arrival_time, price_cents, total_seats, seats_available FROM flights WHERE id = $1`,
      [id],
    );
    if (!rows[0]) throw new HttpError(404, 'flight not found');
    res.json(toFlightDto(rows[0]));
  });

  app.get('/airports', async (_req, res) => {
    const { rows } = await pool.query(`SELECT code, city, country FROM airports ORDER BY code`);
    res.json({ items: rows });
  });

  finalizeApp(app);
  const port = envInt('PORT', 3001);
  const server = app.listen(port, () => log.info({ port }, 'flight-search service listening'));
  onShutdown(log, [() => pool.end(), () => redis.quit(), () => consumer.disconnect(), () => server.close()]);
});
