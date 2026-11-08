import { Consumer, Kafka, KafkaMessage } from 'kafkajs';
import { Pool, PoolClient } from 'pg';
import { EventEnvelope, EventEnvelope as EnvelopeSchema } from './events';
import { Logger } from './logger';
import { kafkaConsumed, kafkaConsumerLag } from './metrics';
import { withTx } from './db';

export interface HandlerContext {
  client: PoolClient;
  event: EventEnvelope<any>;
  log: Logger;
  /** Registers work (e.g. cache invalidation) to run only after the DB transaction commits. */
  afterCommit: (fn: () => Promise<void> | void) => void;
  /** The raw Kafka message headers, for consumers that need them. */
  headers?: Record<string, string>;
}
export type EventHandler = (ctx: HandlerContext) => Promise<void>;

export function parseEnvelope(message: KafkaMessage): EventEnvelope<any> | null {
  if (!message.value) return null;
  try {
    const parsed = JSON.parse(message.value.toString());
    const r = EnvelopeSchema.safeParse(parsed);
    return r.success ? (r.data as EventEnvelope<any>) : null;
  } catch {
    return null;
  }
}

/**
 * Applies one event exactly once per consumer: the processed_events insert and the business
 * change share one DB transaction. Returns 'duplicate' if the event was already handled.
 */
export async function processEventOnce(
  pool: Pool,
  consumerName: string,
  event: EventEnvelope<any>,
  handler: EventHandler,
  log: Logger,
  opts: { dedupeId?: string; headers?: Record<string, string> } = {},
): Promise<'processed' | 'duplicate'> {
  const after: Array<() => Promise<void> | void> = [];
  const result = await withTx(pool, async (client) => {
    const ins = await client.query(
      `INSERT INTO processed_events (consumer, event_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
      [consumerName, opts.dedupeId ?? event.eventId],
    );
    if (ins.rowCount === 0) return 'duplicate' as const;
    await handler({ client, event, log, afterCommit: (fn) => after.push(fn), headers: opts.headers });
    return 'processed' as const;
  });
  for (const fn of after) {
    try { await fn(); } catch (err) { log.warn({ err: (err as Error).message }, 'afterCommit hook failed'); }
  }
  return result;
}

export function headersToRecord(headers: KafkaMessage['headers']): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(headers ?? {})) {
    if (v === undefined) continue;
    out[k] = Array.isArray(v) ? String(v[0]) : Buffer.isBuffer(v) ? v.toString() : String(v);
  }
  return out;
}

/**
 * Starts a duplicate-safe Kafka consumer. Offsets are resolved only after the DB transaction
 * commits, so a crash re-delivers the message and processed_events makes the retry harmless.
 */
export async function startConsumer(opts: {
  kafka: Kafka;
  pool: Pool;
  groupId: string;
  topics: string[];
  handlers: Record<string, EventHandler>;
  log: Logger;
  fromBeginning?: boolean;
}): Promise<Consumer> {
  const { kafka, pool, groupId, topics, handlers, log } = opts;
  const consumer = kafka.consumer({ groupId, sessionTimeout: 30_000, heartbeatInterval: 3_000 });
  await consumer.connect();
  for (const topic of topics) await consumer.subscribe({ topic, fromBeginning: opts.fromBeginning ?? true });

  await consumer.run({
    eachBatchAutoResolve: false,
    autoCommitInterval: 1000, // commitOffsetsIfNecessary() below commits resolved offsets at most once per second
    // Each partition is an independent ordered stream, so partitions are processed in parallel.
    partitionsConsumedConcurrently: Number(process.env.KAFKA_PARTITIONS_CONCURRENCY ?? 3),
    eachBatch: async ({ batch, resolveOffset, heartbeat, commitOffsetsIfNecessary, isRunning, isStale }) => {
      for (const message of batch.messages) {
        if (!isRunning() || isStale()) break;
        const event = parseEnvelope(message);
        if (!event) {
          log.warn({ topic: batch.topic, offset: message.offset }, 'skipping malformed message');
          kafkaConsumed.inc({ topic: batch.topic, type: 'unknown', result: 'malformed' });
        } else if (!handlers[event.type]) {
          kafkaConsumed.inc({ topic: batch.topic, type: event.type, result: 'ignored' });
        } else {
          const elog = log.child({ eventId: event.eventId, eventType: event.type, requestId: event.requestId, bookingId: event.payload.bookingId });
          const result = await processEventOnce(pool, groupId, event, handlers[event.type], elog, { headers: headersToRecord(message.headers) });
          kafkaConsumed.inc({ topic: batch.topic, type: event.type, result });
          elog.info({ result, topic: batch.topic, partition: batch.partition }, 'event handled');
        }
        resolveOffset(message.offset);
        await commitOffsetsIfNecessary(); // commit progress inside large batches (accurate lag, less redelivery after a crash)
        kafkaConsumerLag.set(
          { group: groupId, topic: batch.topic, partition: String(batch.partition) },
          Math.max(0, Number(batch.highWatermark) - Number(message.offset) - 1),
        );
        await heartbeat();
      }
    },
  });
  return consumer;
}
