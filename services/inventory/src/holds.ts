import { randomUUID } from 'crypto';
import { PoolClient } from 'pg';

export type LockStrategy = 'pessimistic' | 'optimistic';

export type HoldResult =
  | { ok: true; holdId: string; amountCents: number; available: number; expiresAt: Date; existing: boolean }
  | { ok: false; reason: 'FLIGHT_NOT_FOUND' | 'SOLD_OUT' | 'CONTENTION' };

/**
 * Takes one seat for a booking without overbooking (build guide 3.5).
 *  - pessimistic: SELECT ... FOR UPDATE row lock, then decrement.
 *  - optimistic:  versioned UPDATE ... WHERE version = $v AND available > 0, retried on conflict.
 * Must be called inside a transaction.
 */
export async function holdSeat(
  client: PoolClient,
  args: { bookingId: string; flightId: string; strategy: LockStrategy; holdTtlSeconds: number; maxRetries?: number },
): Promise<HoldResult> {
  const existing = await client.query(`SELECT h.*, i.available FROM seat_holds h JOIN seat_inventory i USING (flight_id) WHERE h.booking_id = $1`, [args.bookingId]);
  if (existing.rows[0]) {
    const h = existing.rows[0];
    return { ok: true, holdId: h.id, amountCents: h.amount_cents, available: h.available, expiresAt: h.expires_at, existing: true };
  }

  let price: number | undefined;
  let available: number | undefined;

  if (args.strategy === 'pessimistic') {
    const { rows } = await client.query(`SELECT available, price_cents FROM seat_inventory WHERE flight_id = $1 FOR UPDATE`, [args.flightId]);
    if (!rows[0]) return { ok: false, reason: 'FLIGHT_NOT_FOUND' };
    if (rows[0].available <= 0) return { ok: false, reason: 'SOLD_OUT' };
    const upd = await client.query(
      `UPDATE seat_inventory SET available = available - 1, version = version + 1, updated_at = now() WHERE flight_id = $1 RETURNING available`,
      [args.flightId],
    );
    available = upd.rows[0].available;
    price = rows[0].price_cents;
  } else {
    const maxRetries = args.maxRetries ?? 50;
    for (let attempt = 0; ; attempt++) {
      const { rows } = await client.query(`SELECT available, price_cents, version FROM seat_inventory WHERE flight_id = $1`, [args.flightId]);
      if (!rows[0]) return { ok: false, reason: 'FLIGHT_NOT_FOUND' };
      if (rows[0].available <= 0) return { ok: false, reason: 'SOLD_OUT' };
      const upd = await client.query(
        `UPDATE seat_inventory SET available = available - 1, version = version + 1, updated_at = now()
          WHERE flight_id = $1 AND version = $2 AND available > 0 RETURNING available`,
        [args.flightId, rows[0].version],
      );
      if (upd.rowCount === 1) {
        available = upd.rows[0].available;
        price = rows[0].price_cents;
        break;
      }
      if (attempt >= maxRetries) return { ok: false, reason: 'CONTENTION' };
    }
  }

  const holdId = randomUUID();
  const { rows } = await client.query(
    `INSERT INTO seat_holds (id, booking_id, flight_id, status, amount_cents, expires_at)
     VALUES ($1, $2, $3, 'HELD', $4, now() + make_interval(secs => $5)) RETURNING expires_at`,
    [holdId, args.bookingId, args.flightId, price, args.holdTtlSeconds],
  );
  return { ok: true, holdId, amountCents: price!, available: available!, expiresAt: rows[0].expires_at, existing: false };
}

/** Returns a HELD seat to inventory. Returns the new availability, or null if the hold was not HELD. */
export async function releaseHold(client: PoolClient, bookingId: string, newStatus: 'RELEASED' | 'EXPIRED') {
  const { rows } = await client.query(
    `UPDATE seat_holds SET status = $2, updated_at = now() WHERE booking_id = $1 AND status = 'HELD' RETURNING flight_id`,
    [bookingId, newStatus],
  );
  if (!rows[0]) return null;
  const inv = await client.query(
    `UPDATE seat_inventory SET available = available + 1, version = version + 1, updated_at = now() WHERE flight_id = $1 RETURNING available`,
    [rows[0].flight_id],
  );
  return { flightId: rows[0].flight_id as string, available: inv.rows[0].available as number };
}

export async function confirmHold(client: PoolClient, bookingId: string) {
  const { rows } = await client.query(
    `UPDATE seat_holds h SET status = 'CONFIRMED', updated_at = now() FROM seat_inventory i
      WHERE h.booking_id = $1 AND h.status = 'HELD' AND i.flight_id = h.flight_id RETURNING h.flight_id, i.available`,
    [bookingId],
  );
  return rows[0] ? { flightId: rows[0].flight_id as string, available: rows[0].available as number } : null;
}
