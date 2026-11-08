import { IdempotencyRepo, IdempotencyRow, beginIdempotent, canonicalJson, hashRequest } from './idempotency';
import { nextStatus } from './saga';

function mockRepo(overrides: Partial<IdempotencyRepo> = {}): IdempotencyRepo {
  return {
    tryInsert: jest.fn().mockResolvedValue(false),
    find: jest.fn().mockResolvedValue(null),
    takeOverStale: jest.fn().mockResolvedValue(false),
    release: jest.fn().mockResolvedValue(undefined),
    ...overrides,
  };
}
const row = (r: Partial<IdempotencyRow>): IdempotencyRow => ({
  key: 'k', user_id: 'u', request_hash: 'h', status: 'COMPLETED', response_code: 201, response_body: { id: 'b1' }, created_at: new Date(), ...r,
});

describe('beginIdempotent', () => {
  test('new key -> proceed', async () => {
    const repo = mockRepo({ tryInsert: jest.fn().mockResolvedValue(true) });
    expect(await beginIdempotent(repo, 'k', 'u', 'h')).toEqual({ kind: 'proceed' });
  });
  test('same hash + COMPLETED -> replay saved response', async () => {
    const repo = mockRepo({ find: jest.fn().mockResolvedValue(row({})) });
    expect(await beginIdempotent(repo, 'k', 'u', 'h')).toEqual({ kind: 'replay', code: 201, body: { id: 'b1' } });
  });
  test('same hash + IN_PROGRESS -> in_progress (409)', async () => {
    const repo = mockRepo({ find: jest.fn().mockResolvedValue(row({ status: 'IN_PROGRESS' })) });
    expect(await beginIdempotent(repo, 'k', 'u', 'h')).toEqual({ kind: 'in_progress' });
  });
  test('stale IN_PROGRESS is taken over -> proceed', async () => {
    const repo = mockRepo({ find: jest.fn().mockResolvedValue(row({ status: 'IN_PROGRESS' })), takeOverStale: jest.fn().mockResolvedValue(true) });
    expect(await beginIdempotent(repo, 'k', 'u', 'h')).toEqual({ kind: 'proceed' });
  });
  test('different hash -> mismatch (422)', async () => {
    const repo = mockRepo({ find: jest.fn().mockResolvedValue(row({ request_hash: 'other' })) });
    expect(await beginIdempotent(repo, 'k', 'u', 'h')).toEqual({ kind: 'mismatch' });
  });
});

describe('hashRequest', () => {
  test('is independent of key order', () => {
    expect(canonicalJson({ b: 1, a: { d: 2, c: 3 } })).toBe(canonicalJson({ a: { c: 3, d: 2 }, b: 1 }));
    expect(hashRequest('POST', '/bookings', 'u', { a: 1, b: 2 })).toBe(hashRequest('post', '/bookings', 'u', { b: 2, a: 1 }));
  });
  test('differs per user and per body', () => {
    const h = hashRequest('POST', '/bookings', 'u1', { a: 1 });
    expect(hashRequest('POST', '/bookings', 'u2', { a: 1 })).not.toBe(h);
    expect(hashRequest('POST', '/bookings', 'u1', { a: 2 })).not.toBe(h);
    expect(h).toHaveLength(64);
  });
});

describe('booking saga state machine', () => {
  test('happy path PENDING -> SEAT_HELD -> CONFIRMED', () => {
    expect(nextStatus('PENDING', 'SeatHeld')).toBe('SEAT_HELD');
    expect(nextStatus('SEAT_HELD', 'PaymentSucceeded')).toBe('CONFIRMED');
  });
  test('failures', () => {
    expect(nextStatus('PENDING', 'SeatHoldFailed')).toBe('REJECTED');
    expect(nextStatus('SEAT_HELD', 'PaymentFailed')).toBe('CANCELLED');
    expect(nextStatus('SEAT_HELD', 'SeatReleased', 'EXPIRED')).toBe('CANCELLED');
    expect(nextStatus('SEAT_HELD', 'SeatReleased', 'PAYMENT_FAILED')).toBeNull();
  });
  test('payment event arriving before SeatHeld still completes the saga', () => {
    expect(nextStatus('PENDING', 'PaymentSucceeded')).toBe('CONFIRMED');
    expect(nextStatus('CONFIRMED', 'SeatHeld')).toBeNull();
  });
  test('terminal states never change', () => {
    for (const s of ['CONFIRMED', 'REJECTED', 'CANCELLED'] as const) {
      for (const e of ['SeatHeld', 'SeatHoldFailed', 'PaymentSucceeded', 'PaymentFailed']) expect(nextStatus(s, e)).toBeNull();
    }
  });
});
