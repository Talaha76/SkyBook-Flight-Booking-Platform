import { decidePayment } from '../../payment/src/provider';
import { nextDestination, parseDelays } from './retry';
import { renderMessage } from './sender';

describe('retry routing', () => {
  const delays = parseDelays('60000,300000,1800000');
  test('walks 1m -> 5m -> 30m -> DLQ', () => {
    const now = 1_000_000;
    expect(nextDestination(0, delays, now)).toEqual({ topic: 'notification.retry.1m', attempt: 1, notBefore: now + 60000, dlq: false });
    expect(nextDestination(1, delays, now)).toMatchObject({ topic: 'notification.retry.5m', attempt: 2 });
    expect(nextDestination(2, delays, now)).toMatchObject({ topic: 'notification.retry.30m', attempt: 3 });
    expect(nextDestination(3, delays, now)).toMatchObject({ topic: 'notification.dlq', dlq: true });
  });
  test('rejects bad delay config', () => {
    expect(() => parseDelays('1,2')).toThrow();
  });
});

describe('message rendering', () => {
  test('confirmed message includes amount', () => {
    const m = renderMessage('BookingConfirmed', { bookingId: 'abcdef12-0000-4000-8000-000000000000', passengerName: 'Ann', passengerEmail: 'a@x.io', amountCents: 34900 });
    expect(m.subject).toContain('ABCDEF12');
    expect(m.body).toContain('$349.00');
  });
});

describe('mock payment provider', () => {
  const future = new Date(Date.now() + 60_000);
  test('deterministic tokens', () => {
    expect(decidePayment({ token: 'tok_success', expiresAt: future, failureRate: 1 })).toEqual({ ok: true });
    expect(decidePayment({ token: 'tok_fail', expiresAt: future, failureRate: 0 })).toEqual({ ok: false, reason: 'CARD_DECLINED' });
  });
  test('expired hold is never charged', () => {
    expect(decidePayment({ token: 'tok_success', expiresAt: new Date(Date.now() - 1), failureRate: 0 })).toEqual({ ok: false, reason: 'HOLD_EXPIRED' });
  });
  test('random failure rate', () => {
    expect(decidePayment({ token: 'x', expiresAt: future, failureRate: 0.5, random: () => 0.4 }).ok).toBe(false);
    expect(decidePayment({ token: 'x', expiresAt: future, failureRate: 0.5, random: () => 0.6 }).ok).toBe(true);
  });
});
