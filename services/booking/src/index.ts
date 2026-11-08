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
import { createBooking, toBookingDto } from './bookings';
import { deleteExpiredKeys } from './idempotency';
import { BOOKING_MIGRATIONS } from './migrations';
import { sagaHandlers } from './saga';

const SERVICE = 'booking';
const log = createLogger(SERVICE);
setServiceLabel(SERVICE);

runMain(log, async () => {
  const pool = createPool(env('DATABASE_URL'));
  await waitForDb(pool, log);
  await runMigrations(pool, SERVICE, BOOKING_MIGRATIONS, log);

  const kafka = createKafka(SERVICE, env('KAFKA_BROKERS', 'localhost:9092'), log);
  await ensureTopics(kafka, log);
  const producer = await createProducer(kafka);
  const publisher = startOutboxPublisher({ pool, producer, log });
  const consumer = await startConsumer({
    kafka,
    pool,
    groupId: 'booking-saga',
    topics: [TOPICS.inventory, TOPICS.payment],
    handlers: sagaHandlers,
    log,
  });

  const cleanup = setInterval(async () => {
    try {
      const n = await deleteExpiredKeys(pool);
      if (n) log.info({ deleted: n }, 'expired idempotency keys removed');
    } catch (err) {
      log.warn({ err: (err as Error).message }, 'idempotency cleanup failed');
    }
  }, envInt('IDEMPOTENCY_CLEANUP_MS', 3_600_000));

  const app = createApp({ log, internalToken: env('INTERNAL_TOKEN', 'dev-internal-token'), readiness: async () => { await pool.query('SELECT 1'); } });

  app.post('/bookings', async (req, res) => {
    const user = requireUser(req);
    const key = req.header('idempotency-key');
    if (!key) throw new HttpError(400, 'Idempotency-Key header is required');
    const result = await createBooking(pool, { userId: user.id, idempotencyKey: key, body: req.body, requestId: req.requestId });
    for (const [k, v] of Object.entries(result.headers ?? {})) res.setHeader(k, v);
    res.status(result.status).json(result.body);
  });

  app.get('/bookings/:id', async (req, res) => {
    const user = requireUser(req);
    const id = z.string().uuid().parse(req.params.id);
    const { rows } = await pool.query(`SELECT * FROM bookings WHERE id = $1`, [id]);
    if (!rows[0]) throw new HttpError(404, 'booking not found');
    if (user.role !== 'AGENT' && rows[0].user_id !== user.id) throw new HttpError(403, 'you can only view your own bookings');
    res.json(toBookingDto(rows[0]));
  });

  app.get('/bookings', async (req, res) => {
    const user = requireUser(req);
    const q = z
      .object({ limit: z.coerce.number().int().min(1).max(100).default(20), status: z.string().optional(), userId: z.string().uuid().optional() })
      .parse(req.query);
    const params: unknown[] = [];
    const where: string[] = [];
    const filterUser = user.role === 'AGENT' ? q.userId : user.id;
    if (filterUser) { params.push(filterUser); where.push(`user_id = $${params.length}`); }
    if (q.status) { params.push(q.status); where.push(`status = $${params.length}`); }
    params.push(q.limit);
    const { rows } = await pool.query(
      `SELECT * FROM bookings ${where.length ? 'WHERE ' + where.join(' AND ') : ''} ORDER BY created_at DESC LIMIT $${params.length}`,
      params,
    );
    res.json({ items: rows.map(toBookingDto) });
  });

  finalizeApp(app);
  const port = envInt('PORT', 3002);
  const server = app.listen(port, () => log.info({ port }, 'booking service listening'));

  onShutdown(log, [() => pool.end(), () => producer.disconnect(), () => publisher.stop(), () => consumer.disconnect(), () => clearInterval(cleanup), () => server.close()]);
});
