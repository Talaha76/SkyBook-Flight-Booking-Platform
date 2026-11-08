import { Pool, PoolClient } from 'pg';
import { Producer } from 'kafkajs';
import { EventEnvelope } from './events';
import { Logger } from './logger';
import { outboxPending, outboxPublished } from './metrics';

/** Writes an event to the outbox inside the caller's transaction (no dual write). */
export async function insertOutbox(
  client: PoolClient,
  topic: string,
  key: string,
  event: EventEnvelope<any>,
  opts: { headers?: Record<string, string>; outboxId?: string } = {},
) {
  await client.query(
    `INSERT INTO outbox_events (event_id, aggregate_id, topic, event_key, payload, headers)
     VALUES ($1, $2, $3, $4, $5, $6)`,
    [
      opts.outboxId ?? event.eventId,
      event.aggregateId,
      topic,
      key,
      event,
      { 'x-request-id': event.requestId ?? '', 'event-type': event.type, 'x-message-id': opts.outboxId ?? event.eventId, ...(opts.headers ?? {}) },
    ],
  );
}

interface OutboxRow {
  id: string;
  topic: string;
  event_key: string;
  payload: unknown;
  headers: Record<string, string>;
}

/**
 * Publishes pending outbox rows to Kafka. Uses FOR UPDATE SKIP LOCKED so several publisher
 * replicas can run at once. Delivery is at-least-once; consumers deduplicate on eventId.
 * On a Kafka failure the whole loop backs off exponentially, which keeps per-key ordering intact.
 */
export function startOutboxPublisher(opts: {
  pool: Pool;
  producer: Producer;
  log: Logger;
  pollMs?: number;
  batchSize?: number;
}) {
  const { pool, producer, log } = opts;
  const pollMs = opts.pollMs ?? Number(process.env.OUTBOX_POLL_MS ?? 200);
  const batchSize = opts.batchSize ?? 100;
  let stopped = false;
  let failures = 0;
  let lastGauge = 0;

  async function publishOnce(): Promise<number> {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const { rows } = await client.query<OutboxRow>(
        `SELECT id, topic, event_key, payload, headers FROM outbox_events
          WHERE status = 'PENDING' AND next_attempt_at <= now()
          ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED`,
        [batchSize],
      );
      if (rows.length === 0) {
        await client.query('COMMIT');
        return 0;
      }
      const byTopic = new Map<string, OutboxRow[]>();
      for (const r of rows) byTopic.set(r.topic, [...(byTopic.get(r.topic) ?? []), r]);
      try {
        await producer.sendBatch({
          acks: -1,
          topicMessages: [...byTopic.entries()].map(([topic, msgs]) => ({
            topic,
            messages: msgs.map((m) => ({ key: m.event_key, value: JSON.stringify(m.payload), headers: m.headers })),
          })),
        });
      } catch (err) {
        await client.query(
          `UPDATE outbox_events SET attempt_count = attempt_count + 1, last_error = $2,
             next_attempt_at = now() + make_interval(secs => LEAST(power(2, attempt_count + 1), 60))
           WHERE id = ANY($1::bigint[])`,
          [rows.map((r) => r.id), (err as Error).message.slice(0, 500)],
        );
        await client.query('COMMIT');
        throw err;
      }
      await client.query(`UPDATE outbox_events SET status = 'PUBLISHED', published_at = now() WHERE id = ANY($1::bigint[])`, [
        rows.map((r) => r.id),
      ]);
      await client.query('COMMIT');
      for (const [topic, msgs] of byTopic) outboxPublished.inc({ topic }, msgs.length);
      return rows.length;
    } catch (err) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw err;
    } finally {
      client.release();
    }
  }

  async function loop() {
    while (!stopped) {
      try {
        const n = await publishOnce();
        failures = 0;
        if (Date.now() - lastGauge > 5000) {
          lastGauge = Date.now();
          const { rows } = await pool.query(`SELECT count(*)::int AS n FROM outbox_events WHERE status = 'PENDING'`);
          outboxPending.set(rows[0].n);
        }
        if (n < batchSize) await new Promise((r) => setTimeout(r, pollMs));
      } catch (err) {
        failures++;
        const backoff = Math.min(1000 * 2 ** failures, 30_000);
        log.error({ err: (err as Error).message, backoffMs: backoff }, 'outbox publish failed');
        await new Promise((r) => setTimeout(r, backoff));
      }
    }
  }
  void loop();
  return { stop: () => { stopped = true; }, publishOnce };
}
