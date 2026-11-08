import { randomUUID } from 'crypto';
import { Pool } from 'pg';
import { EventTypes, makeEvent, processEventOnce, withTx } from '../../packages/shared/src';
import { inventoryHandlers, sweepExpiredHolds } from '../../services/inventory/src/handlers';
import { LockStrategy, holdSeat } from '../../services/inventory/src/holds';
import { log, resetDb, setupDb, teardownDb } from './helpers';

let pool: Pool;
beforeAll(async () => { pool = await setupDb(); });
afterAll(async () => { await teardownDb(pool); });
beforeEach(async () => { await resetDb(pool); });

async function seedFlight(seats: number, price = 10000) {
  const flightId = randomUUID();
  await pool.query(`INSERT INTO seat_inventory (flight_id, total_seats, available, price_cents) VALUES ($1, $2, $2, $3)`, [flightId, seats, price]);
  return flightId;
}

describe.each<LockStrategy>(['pessimistic', 'optimistic'])('overbooking protection (%s)', (strategy) => {
  test('200 concurrent holds on 150 seats -> exactly 150 held, 50 rejected', async () => {
    const flightId = await seedFlight(150);
    const started = Date.now();
    const results = await Promise.all(
      Array.from({ length: 200 }, () =>
        withTx(pool, (c) => holdSeat(c, { bookingId: randomUUID(), flightId, strategy, holdTtlSeconds: 600, maxRetries: 500 })),
      ),
    );
    const ms = Date.now() - started;
    const held = results.filter((r) => r.ok).length;
    const soldOut = results.filter((r) => !r.ok && r.reason === 'SOLD_OUT').length;
    expect(held).toBe(150);
    expect(soldOut).toBe(50);
    const { rows } = await pool.query(`SELECT available FROM seat_inventory WHERE flight_id = $1`, [flightId]);
    expect(rows[0].available).toBe(0);
    const holds = await pool.query(`SELECT count(*)::int AS n FROM seat_holds WHERE flight_id = $1`, [flightId]);
    expect(holds.rows[0].n).toBe(150);
    // eslint-disable-next-line no-console
    console.log(`${strategy}: 200 concurrent holds on 150 seats in ${ms} ms -> ${held} held / ${soldOut} rejected`);
  });
});

describe('duplicate-safe consumers + compensation', () => {
  const handlers = inventoryHandlers({ strategy: 'pessimistic', holdTtlSeconds: 600 });

  test('the same BookingRequested event delivered twice changes state once', async () => {
    const flightId = await seedFlight(10);
    const bookingId = randomUUID();
    const event = makeEvent(EventTypes.BookingRequested, bookingId, {
      bookingId, flightId, userId: randomUUID(), passengerName: 'A', passengerEmail: 'a@example.com', paymentToken: 'tok_success',
    });
    const first = await processEventOnce(pool, 'inventory', event, handlers[EventTypes.BookingRequested], log);
    const second = await processEventOnce(pool, 'inventory', event, handlers[EventTypes.BookingRequested], log);
    expect(first).toBe('processed');
    expect(second).toBe('duplicate');
    const { rows } = await pool.query(`SELECT available FROM seat_inventory WHERE flight_id = $1`, [flightId]);
    expect(rows[0].available).toBe(9);
    const outbox = await pool.query(`SELECT payload->>'type' AS type FROM outbox_events`);
    expect(outbox.rows).toEqual([{ type: 'SeatHeld' }]);
  });

  test('PaymentFailed releases the held seat (compensation) exactly once', async () => {
    const flightId = await seedFlight(5);
    const bookingId = randomUUID();
    await processEventOnce(pool, 'inventory', makeEvent(EventTypes.BookingRequested, bookingId, {
      bookingId, flightId, userId: randomUUID(), passengerName: 'A', passengerEmail: 'a@example.com', paymentToken: 'tok_fail',
    }), handlers[EventTypes.BookingRequested], log);
    const failed = makeEvent(EventTypes.PaymentFailed, bookingId, { bookingId, flightId, paymentId: randomUUID(), amountCents: 10000, reason: 'CARD_DECLINED' });
    await processEventOnce(pool, 'inventory', failed, handlers[EventTypes.PaymentFailed], log);
    // a different PaymentFailed event for the same booking must not release twice
    await processEventOnce(pool, 'inventory', { ...failed, eventId: randomUUID() }, handlers[EventTypes.PaymentFailed], log);
    const { rows } = await pool.query(`SELECT available FROM seat_inventory WHERE flight_id = $1`, [flightId]);
    expect(rows[0].available).toBe(5);
    const hold = await pool.query(`SELECT status FROM seat_holds WHERE booking_id = $1`, [bookingId]);
    expect(hold.rows[0].status).toBe('RELEASED');
  });

  test('expired holds are swept back into inventory', async () => {
    const flightId = await seedFlight(3);
    await withTx(pool, (c) => holdSeat(c, { bookingId: randomUUID(), flightId, strategy: 'pessimistic', holdTtlSeconds: 0 }));
    await new Promise((r) => setTimeout(r, 20));
    expect(await sweepExpiredHolds(pool, log)).toBe(1);
    const { rows } = await pool.query(`SELECT available FROM seat_inventory WHERE flight_id = $1`, [flightId]);
    expect(rows[0].available).toBe(3);
    const ev = await pool.query(`SELECT payload->'payload'->>'reason' AS reason FROM outbox_events`);
    expect(ev.rows[0].reason).toBe('EXPIRED');
  });
});
