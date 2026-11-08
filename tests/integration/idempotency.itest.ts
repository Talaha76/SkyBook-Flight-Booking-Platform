import { randomUUID } from 'crypto';
import { Pool } from 'pg';
import { HttpError } from '../../packages/shared/src';
import { createBooking } from '../../services/booking/src/bookings';
import { resetDb, setupDb, teardownDb } from './helpers';

let pool: Pool;
beforeAll(async () => { pool = await setupDb(); });
afterAll(async () => { await teardownDb(pool); });
beforeEach(async () => { await resetDb(pool); });

const user = '11111111-1111-4111-8111-111111111111';
const body = () => ({ flightId: randomUUID(), passengerName: 'Alice', passengerEmail: 'alice@example.com', paymentToken: 'tok_success' });

async function attempt(key: string, b: unknown) {
  try {
    const r = await createBooking(pool, { userId: user, idempotencyKey: key, body: b });
    return { status: r.status, body: r.body as any, replayed: !!r.replayed };
  } catch (err) {
    if (err instanceof HttpError) return { status: err.status, body: null, replayed: false };
    throw err;
  }
}

test('50 identical concurrent requests produce exactly 1 booking', async () => {
  const key = randomUUID();
  const b = body();
  const results = await Promise.all(Array.from({ length: 50 }, () => attempt(key, b)));

  const { rows } = await pool.query(`SELECT count(*)::int AS n FROM bookings`);
  expect(rows[0].n).toBe(1);
  const outbox = await pool.query(`SELECT count(*)::int AS n FROM outbox_events`);
  expect(outbox.rows[0].n).toBe(1);

  const created = results.filter((r) => r.status === 201 && !r.replayed);
  const replays = results.filter((r) => r.status === 201 && r.replayed);
  const conflicts = results.filter((r) => r.status === 409);
  expect(created).toHaveLength(1);
  expect(replays.length + conflicts.length).toBe(49);
  for (const r of replays) expect(r.body.id).toBe(created[0].body.id);
  // eslint-disable-next-line no-console
  console.log(`race: 1 created, ${replays.length} replays, ${conflicts.length} x 409`);
});

test('resending a completed key returns the exact saved response', async () => {
  const key = randomUUID();
  const b = body();
  const first = await attempt(key, b);
  const second = await attempt(key, b);
  expect(first.status).toBe(201);
  expect(second.status).toBe(201);
  expect(second.replayed).toBe(true);
  expect(second.body).toEqual(first.body);
});

test('reusing a key with a different body returns 422 and creates nothing', async () => {
  const key = randomUUID();
  await attempt(key, body());
  const r = await attempt(key, body());
  expect(r.status).toBe(422);
  const { rows } = await pool.query(`SELECT count(*)::int AS n FROM bookings`);
  expect(rows[0].n).toBe(1);
});

test('validation failure releases the key so the client can retry', async () => {
  const key = randomUUID();
  await expect(createBooking(pool, { userId: user, idempotencyKey: key, body: { flightId: 'nope' } })).rejects.toThrow();
  const { rows } = await pool.query(`SELECT count(*)::int AS n FROM idempotency_keys`);
  expect(rows[0].n).toBe(0);
  expect((await attempt(key, body())).status).toBe(201);
});
