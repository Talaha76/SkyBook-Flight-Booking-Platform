import { TOPICS } from '../../../packages/shared/src';

export const RETRY_TOPICS = [TOPICS.notificationRetry1, TOPICS.notificationRetry2, TOPICS.notificationRetry3] as const;
export const MAX_RETRIES = RETRY_TOPICS.length;

export function parseDelays(spec: string | undefined): number[] {
  const d = (spec ?? '60000,300000,1800000').split(',').map((s) => Number(s.trim()));
  if (d.length !== MAX_RETRIES || d.some((n) => !Number.isFinite(n) || n < 0)) throw new Error('NOTIFICATION_RETRY_DELAYS_MS must be 3 comma-separated numbers');
  return d;
}

/**
 * Where a message goes after a failed send.
 * attemptsSoFar = number of retries already made (0 for the first, live attempt).
 *   0 -> retry.1m, 1 -> retry.5m, 2 -> retry.30m, 3 -> DLQ
 */
export function nextDestination(attemptsSoFar: number, delays: number[], now = Date.now()) {
  if (attemptsSoFar >= MAX_RETRIES) return { topic: TOPICS.notificationDlq, attempt: attemptsSoFar, notBefore: now, dlq: true as const };
  return { topic: RETRY_TOPICS[attemptsSoFar], attempt: attemptsSoFar + 1, notBefore: now + delays[attemptsSoFar], dlq: false as const };
}
