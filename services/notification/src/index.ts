import { randomUUID } from 'crypto';
import { PoolClient } from 'pg';
import { z } from 'zod';
import {
  BookingOutcomePayload,
  COMMON_MIGRATIONS,
  EventEnvelope,
  EventHandler,
  EventTypes,
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
  headersToRecord,
  insertOutbox,
  onShutdown,
  parseEnvelope,
  processEventOnce,
  promClient,
  registry,
  requireUser,
  runMain,
  runMigrations,
  setServiceLabel,
  startConsumer,
  startOutboxPublisher,
  waitForDb,
} from '../../../packages/shared/src';
import { RETRY_TOPICS, nextDestination, parseDelays } from './retry';
import { createSender, renderMessage } from './sender';

const SERVICE = 'notification';
const log = createLogger(SERVICE);
setServiceLabel(SERVICE);

const dlqCounter = new promClient.Counter({ name: 'dlq_messages_total', help: 'Messages moved to the notification DLQ', registers: [registry] });
const sentCounter = new promClient.Counter({ name: 'notifications_total', help: 'Notification send attempts', labelNames: ['result'], registers: [registry] });

const MIGRATIONS = [
  ...COMMON_MIGRATIONS,
  `CREATE TABLE IF NOT EXISTS notifications (
     id          UUID PRIMARY KEY,
     event_id    UUID NOT NULL UNIQUE,
     booking_id  UUID NOT NULL,
     type        TEXT NOT NULL,
     recipient   TEXT NOT NULL,
     subject     TEXT NOT NULL,
     body        TEXT NOT NULL,
     status      TEXT NOT NULL CHECK (status IN ('SENT','RETRYING','DEAD')),
     attempts    INT  NOT NULL DEFAULT 1,
     last_error  TEXT,
     created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
     updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
   )`,
  `CREATE INDEX IF NOT EXISTS idx_notifications_booking ON notifications (booking_id)`,
];

const OUTCOMES = [EventTypes.BookingConfirmed, EventTypes.BookingCancelled, EventTypes.BookingRejected];

runMain(log, async () => {
  const pool = createPool(env('DATABASE_URL'));
  await waitForDb(pool, log);
  await runMigrations(pool, SERVICE, MIGRATIONS, log);

  const delays = parseDelays(process.env.NOTIFICATION_RETRY_DELAYS_MS);
  const send = createSender(log, Number(env('NOTIFICATION_FAILURE_RATE', '0')));

  /** Sends; on failure routes the original event to the next retry topic or the DLQ, in the same transaction. */
  async function attempt(client: PoolClient, event: EventEnvelope<any>, attemptsSoFar: number, originalTopic: string, elog = log) {
    const p = BookingOutcomePayload.parse(event.payload);
    const msg = renderMessage(event.type, p);
    try {
      await send(msg);
      sentCounter.inc({ result: 'sent' });
      await client.query(
        `INSERT INTO notifications (id, event_id, booking_id, type, recipient, subject, body, status, attempts)
         VALUES ($1, $2, $3, $4, $5, $6, $7, 'SENT', $8)
         ON CONFLICT (event_id) DO UPDATE SET status = 'SENT', attempts = $8, last_error = NULL, updated_at = now()`,
        [randomUUID(), event.eventId, p.bookingId, event.type, msg.recipient, msg.subject, msg.body, attemptsSoFar + 1],
      );
    } catch (err) {
      const error = (err as Error).message;
      sentCounter.inc({ result: 'failed' });
      const dest = nextDestination(attemptsSoFar, delays);
      await client.query(
        `INSERT INTO notifications (id, event_id, booking_id, type, recipient, subject, body, status, attempts, last_error)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
         ON CONFLICT (event_id) DO UPDATE SET status = $8, attempts = $9, last_error = $10, updated_at = now()`,
        [randomUUID(), event.eventId, p.bookingId, event.type, msg.recipient, msg.subject, msg.body, dest.dlq ? 'DEAD' : 'RETRYING', attemptsSoFar + 1, error],
      );
      await insertOutbox(client, dest.topic, p.flightId, event, {
        outboxId: randomUUID(),
        headers: { 'x-attempt': String(dest.attempt), 'x-not-before': String(dest.notBefore), 'x-original-topic': originalTopic, 'x-error': error.slice(0, 300) },
      });
      if (dest.dlq) {
        dlqCounter.inc();
        elog.error({ error, attempts: attemptsSoFar + 1 }, 'notification moved to DLQ');
      } else {
        elog.warn({ error, nextTopic: dest.topic, attempt: dest.attempt }, 'notification failed, scheduled retry');
      }
    }
  }

  const kafka = createKafka(SERVICE, env('KAFKA_BROKERS', 'localhost:9092'), log);
  await ensureTopics(kafka, log);
  const producer = await createProducer(kafka);
  const publisher = startOutboxPublisher({ pool, producer, log });

  // 1) Live path: booking outcomes from booking.events.
  const liveHandler: EventHandler = async ({ client, event, log: elog }) => attempt(client, event, 0, TOPICS.booking, elog);
  const live = await startConsumer({
    kafka, pool, groupId: 'notification', topics: [TOPICS.booking], log,
    handlers: Object.fromEntries(OUTCOMES.map((t) => [t, liveHandler])),
  });

  // 2) Retry path: each retry topic is consumed only once its delay has passed (pause + seek + resume).
  const retry = kafka.consumer({ groupId: 'notification-retry' });
  await retry.connect();
  for (const topic of RETRY_TOPICS) await retry.subscribe({ topic, fromBeginning: true });
  await retry.run({
    eachBatchAutoResolve: false,
    eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale, pause }) => {
      for (const message of batch.messages) {
        if (!isRunning() || isStale()) break;
        const headers = headersToRecord(message.headers);
        const notBefore = Number(headers['x-not-before'] ?? 0);
        const wait = notBefore - Date.now();
        if (wait > 0) {
          const resume = pause();
          retry.seek({ topic: batch.topic, partition: batch.partition, offset: message.offset });
          setTimeout(resume, Math.min(wait, 60_000));
          return;
        }
        const event = parseEnvelope(message);
        if (event && OUTCOMES.includes(event.type as any)) {
          const attemptsSoFar = Number(headers['x-attempt'] ?? 1);
          const elog = log.child({ eventId: event.eventId, bookingId: event.payload.bookingId, requestId: event.requestId, attempt: attemptsSoFar });
          await processEventOnce(pool, 'notification-retry', event, ({ client }) => attempt(client, event, attemptsSoFar, headers['x-original-topic'] ?? TOPICS.booking, elog), elog, {
            dedupeId: headers['x-message-id'] && z.string().uuid().safeParse(headers['x-message-id']).success ? headers['x-message-id'] : randomUUID(),
          });
        }
        resolveOffset(message.offset);
        await heartbeat();
      }
    },
  });

  const app = createApp({ log, internalToken: env('INTERNAL_TOKEN', 'dev-internal-token'), readiness: async () => { await pool.query('SELECT 1'); } });
  app.get('/notifications', async (req, res) => {
    const user = requireUser(req);
    if (user.role !== 'AGENT') throw new HttpError(403, 'agents only');
    const q = z.object({ bookingId: z.string().uuid().optional(), status: z.enum(['SENT', 'RETRYING', 'DEAD']).optional() }).parse(req.query);
    const { rows } = await pool.query(
      `SELECT * FROM notifications WHERE ($1::uuid IS NULL OR booking_id = $1) AND ($2::text IS NULL OR status = $2) ORDER BY created_at DESC LIMIT 100`,
      [q.bookingId ?? null, q.status ?? null],
    );
    res.json({ items: rows.map((r) => ({ id: r.id, bookingId: r.booking_id, type: r.type, recipient: r.recipient, subject: r.subject, status: r.status, attempts: r.attempts, lastError: r.last_error, createdAt: r.created_at })) });
  });
  finalizeApp(app);
  const port = envInt('PORT', 3005);
  const server = app.listen(port, () => log.info({ port, delays }, 'notification service listening'));
  onShutdown(log, [() => pool.end(), () => producer.disconnect(), () => publisher.stop(), () => live.disconnect(), () => retry.disconnect(), () => server.close()]);
});
