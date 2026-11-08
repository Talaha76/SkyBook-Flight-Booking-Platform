import { z } from 'zod';
import {
  HttpError,
  TOPICS,
  createApp,
  createKafka,
  createLogger,
  createPool,
  createProducer,
  ensureTopics,
  env,
  envInt,
  finalizeApp,
  onShutdown,
  requireUser,
  runMain,
  runMigrations,
  setServiceLabel,
  startConsumer,
  startOutboxPublisher,
  waitForDb,
} from '../../../packages/shared/src';
import { inventoryHandlers, sweepExpiredHolds } from './handlers';
import { LockStrategy } from './holds';
import { INVENTORY_MIGRATIONS } from './migrations';

const SERVICE = 'inventory';
const log = createLogger(SERVICE);
setServiceLabel(SERVICE);

runMain(log, async () => {
  const pool = createPool(env('DATABASE_URL'));
  await waitForDb(pool, log);
  await runMigrations(pool, SERVICE, INVENTORY_MIGRATIONS, log);

  const strategy = z.enum(['pessimistic', 'optimistic']).parse(env('INVENTORY_LOCK_STRATEGY', 'pessimistic')) as LockStrategy;
  const holdTtlSeconds = envInt('SEAT_HOLD_TTL_SECONDS', 600);

  const kafka = createKafka(SERVICE, env('KAFKA_BROKERS', 'localhost:9092'), log);
  await ensureTopics(kafka, log);
  const producer = await createProducer(kafka);
  const publisher = startOutboxPublisher({ pool, producer, log });
  const consumer = await startConsumer({
    kafka,
    pool,
    groupId: 'inventory',
    topics: [TOPICS.booking, TOPICS.payment],
    handlers: inventoryHandlers({ strategy, holdTtlSeconds }),
    log,
  });

  const sweeper = setInterval(() => {
    sweepExpiredHolds(pool, log).catch((err) => log.warn({ err: (err as Error).message }, 'hold sweeper failed'));
  }, envInt('HOLD_SWEEP_INTERVAL_MS', 15_000));

  const app = createApp({ log, internalToken: env('INTERNAL_TOKEN', 'dev-internal-token'), readiness: async () => { await pool.query('SELECT 1'); } });

  app.get('/inventory/:flightId', async (req, res) => {
    const user = requireUser(req);
    if (user.role !== 'AGENT') throw new HttpError(403, 'agents only');
    const flightId = z.string().uuid().parse(req.params.flightId);
    const { rows } = await pool.query(`SELECT * FROM seat_inventory WHERE flight_id = $1`, [flightId]);
    if (!rows[0]) throw new HttpError(404, 'flight not found in inventory');
    const holds = await pool.query(`SELECT status, count(*)::int AS n FROM seat_holds WHERE flight_id = $1 GROUP BY status`, [flightId]);
    res.json({
      flightId,
      totalSeats: rows[0].total_seats,
      available: rows[0].available,
      priceCents: rows[0].price_cents,
      holds: Object.fromEntries(holds.rows.map((h) => [h.status, h.n])),
    });
  });

  finalizeApp(app);
  const port = envInt('PORT', 3003);
  const server = app.listen(port, () => log.info({ port, strategy }, 'inventory service listening'));
  onShutdown(log, [() => pool.end(), () => producer.disconnect(), () => publisher.stop(), () => consumer.disconnect(), () => clearInterval(sweeper), () => server.close()]);
});
