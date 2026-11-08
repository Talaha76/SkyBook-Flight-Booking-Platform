import { SendMessageBatchCommand, SendMessageBatchCommandOutput } from '@aws-sdk/client-sqs';
import { SqsRecord } from './contracts';

export const SQS_BATCH_LIMIT = 10;

/** The subset of SQSClient the forwarder needs (easy to fake in tests). */
export interface SqsSender {
  send(cmd: SendMessageBatchCommand): Promise<Pick<SendMessageBatchCommandOutput, 'Successful' | 'Failed'>>;
}

export class ForwardError extends Error {
  constructor(message: string, public pending: number) {
    super(message);
  }
}

export interface ForwardResult {
  forwarded: number;
  /** Entries AWS rejected as the sender's fault (e.g. invalid attribute); retrying can never succeed. */
  rejected: { eventId: string; code?: string; message?: string }[];
  retriedEntries: number;
}

/**
 * Sends up to 10 records with SendMessageBatch. Entries listed in Failed[] are retried with
 * exponential backoff (only those entries). Throws ForwardError if transient failures remain after
 * maxAttempts, so the caller does NOT commit the Kafka offset.
 */
export async function forwardBatch(
  sqs: SqsSender,
  queueUrl: string,
  records: SqsRecord[],
  opts: { maxAttempts?: number; baseDelayMs?: number; onFailedEntries?: (n: number) => void } = {},
): Promise<ForwardResult> {
  if (records.length > SQS_BATCH_LIMIT) throw new Error(`batch too large: ${records.length}`);
  const maxAttempts = opts.maxAttempts ?? 5;
  const baseDelay = opts.baseDelayMs ?? 200;
  let pending = records.map((r, i) => ({ id: `m${i}`, r }));
  const result: ForwardResult = { forwarded: 0, rejected: [], retriedEntries: 0 };

  for (let attempt = 1; pending.length > 0; attempt++) {
    let out: Pick<SendMessageBatchCommandOutput, 'Successful' | 'Failed'>;
    try {
      out = await sqs.send(
        new SendMessageBatchCommand({
          QueueUrl: queueUrl,
          Entries: pending.map(({ id, r }) => ({
            Id: id,
            MessageBody: r.body,
            MessageAttributes: Object.fromEntries(Object.entries(r.attributes).map(([k, v]) => [k, { DataType: 'String', StringValue: v }])),
          })),
        }),
      );
    } catch (err) {
      // Whole call failed (network, throttling, 5xx): nothing from `pending` is known to be delivered.
      if (attempt >= maxAttempts) throw new ForwardError(`SendMessageBatch failed: ${(err as Error).message}`, pending.length);
      await new Promise((r) => setTimeout(r, baseDelay * 2 ** (attempt - 1)));
      continue;
    }
    const ok = new Set((out.Successful ?? []).map((s) => s.Id));
    result.forwarded += pending.filter((p) => ok.has(p.id)).length;
    const failed = out.Failed ?? [];
    if (failed.length) opts.onFailedEntries?.(failed.length);
    const retry: typeof pending = [];
    for (const f of failed) {
      const entry = pending.find((p) => p.id === f.Id);
      if (!entry) continue;
      if (f.SenderFault) result.rejected.push({ eventId: entry.r.eventId, code: f.Code, message: f.Message });
      else retry.push(entry);
    }
    // Anything neither successful nor failed is treated as not delivered.
    for (const p of pending) if (!ok.has(p.id) && !failed.some((f) => f.Id === p.id)) retry.push(p);
    pending = retry;
    if (pending.length === 0) break;
    result.retriedEntries += pending.length;
    if (attempt >= maxAttempts) throw new ForwardError(`${pending.length} entries still failing after ${attempt} attempts`, pending.length);
    await new Promise((r) => setTimeout(r, baseDelay * 2 ** (attempt - 1)));
  }
  return result;
}
