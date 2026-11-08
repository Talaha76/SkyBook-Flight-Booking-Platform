import { randomUUID } from 'crypto';
import { z } from 'zod';
import {
  COMMON_MIGRATIONS,
  EventHandler,
  EventTypes,
  HttpError,
  SeatHeldPayload,
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
  insertOutbox,
  makeEvent,
  onShutdown,
  requireUser,
  runMain,
  runMigrations,
  setServiceLabel,
  sleep,
  startConsumer,
  startOutboxPublisher,
  waitForDb,
} from '../../../packages/shared/src';
import { decidePayment } from './provider';

const SERVICE = 'payment';
const log = createLogger(SERVICE);
setServiceLabel(SERVICE);

export const PAYMENT_MIGRATIONS = [
  ...COMMON_MIGRATIONS,
  `CREATE TABLE IF NOT EXISTS payments (
     id             UUID PRIMARY KEY,
     booking_id     UUID NOT NULL UNIQUE,
     flight_id      UUID NOT NULL,
     amount_cents   INT NOT NULL,
     status         TEXT NOT NULL CHECK (status IN ('SUCCEEDED','FAILED')),
     failure_reason TEXT,
     provider_ref   TEXT,
     created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
   )`,
];

runMain(log, async () => {
  const pool = createPool(env('DATABASE_URL'));
  await waitForDb(pool, log);
  await runMigrations(pool, SERVICE, PAYMENT_MIGRATIONS, log);

  const failureRate = Number(env('PAYMENT_FAILURE_RATE', '0.1'));
  const delayMs = envInt('PAYMENT_DELAY_MS', 200);

  const handlers: Record<string, EventHandler> = {
    [EventTypes.SeatHeld]: async ({ client, event, log: elog }) => {
      const p = SeatHeldPayload.parse(event.payload);
      if (delayMs > 0) await sleep(delayMs);
      const outcome = decidePayment({ token: p.paymentToken, expiresAt: new Date(p.expiresAt), failureRate });
      const paymentId = randomUUID();
      const ins = await client.query(
        `INSERT INTO payments (id, booking_id, flight_id, amount_cents, status, failure_reason, provider_ref)
         VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (booking_id) DO NOTHING`,
        [paymentId, p.bookingId, p.flightId, p.amountCents, outcome.ok ? 'SUCCEEDED' : 'FAILED', outcome.ok ? null : outcome.reason, `mock_${paymentId.slice(0, 8)}`],
      );
      if (ins.rowCount === 0) return; // already charged for this booking
      elog.info({ amountCents: p.amountCents, ok: outcome.ok }, 'payment processed');
      await insertOutbox(
        client,
        TOPICS.payment,
        p.flightId,
        makeEvent(outcome.ok ? EventTypes.PaymentSucceeded : EventTypes.PaymentFailed, p.bookingId, {
          bookingId: p.bookingId,
          flightId: p.flightId,
          paymentId,
          amountCents: p.amountCents,
          ...(outcome.ok ? {} : { reason: outcome.reason }),
        }, event.requestId),
      );
    },
  };

  const kafka = createKafka(SERVICE, env('KAFKA_BROKERS', 'localhost:9092'), log);
  await ensureTopics(kafka, log);
  const producer = await createProducer(kafka);
  const publisher = startOutboxPublisher({ pool, producer, log });
  const consumer = await startConsumer({ kafka, pool, groupId: 'payment', topics: [TOPICS.inventory], handlers, log });

  const app = createApp({ log, internalToken: env('INTERNAL_TOKEN', 'dev-internal-token'), readiness: async () => { await pool.query('SELECT 1'); } });
  app.get('/payments/:bookingId', async (req, res) => {
    const user = requireUser(req);
    if (user.role !== 'AGENT') throw new HttpError(403, 'agents only');
    const bookingId = z.string().uuid().parse(req.params.bookingId);
    const { rows } = await pool.query(`SELECT * FROM payments WHERE booking_id = $1`, [bookingId]);
    if (!rows[0]) throw new HttpError(404, 'payment not found');
    const r = rows[0];
    res.json({ id: r.id, bookingId: r.booking_id, amountCents: r.amount_cents, status: r.status, failureReason: r.failure_reason, providerRef: r.provider_ref, createdAt: r.created_at });
  });
  finalizeApp(app);
  const port = envInt('PORT', 3004);
  const server = app.listen(port, () => log.info({ port, failureRate, delayMs }, 'payment service listening'));
  onShutdown(log, [() => pool.end(), () => producer.disconnect(), () => publisher.stop(), () => consumer.disconnect(), () => server.close()]);
});
