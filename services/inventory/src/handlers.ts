import { Pool } from 'pg';
import {
  BookingRequestedPayload,
  EventHandler,
  EventTypes,
  Logger,
  PaymentResultPayload,
  TOPICS,
  insertOutbox,
  makeEvent,
} from '../../../packages/shared/src';
import { LockStrategy, confirmHold, holdSeat, releaseHold } from './holds';

export function inventoryHandlers(cfg: { strategy: LockStrategy; holdTtlSeconds: number }): Record<string, EventHandler> {
  return {
    [EventTypes.BookingRequested]: async ({ client, event, log }) => {
      const p = BookingRequestedPayload.parse(event.payload);
      const r = await holdSeat(client, { bookingId: p.bookingId, flightId: p.flightId, strategy: cfg.strategy, holdTtlSeconds: cfg.holdTtlSeconds });
      if (r.ok) {
        if (r.existing) return; // hold already exists and its SeatHeld event was already emitted
        log.info({ flightId: p.flightId, available: r.available }, 'seat held');
        await insertOutbox(
          client,
          TOPICS.inventory,
          p.flightId,
          makeEvent(EventTypes.SeatHeld, p.bookingId, {
            bookingId: p.bookingId,
            flightId: p.flightId,
            holdId: r.holdId,
            amountCents: r.amountCents,
            expiresAt: new Date(r.expiresAt).toISOString(),
            paymentToken: p.paymentToken,
            available: r.available,
          }, event.requestId),
        );
      } else {
        log.info({ flightId: p.flightId, reason: r.reason }, 'seat hold failed');
        await insertOutbox(
          client,
          TOPICS.inventory,
          p.flightId,
          makeEvent(EventTypes.SeatHoldFailed, p.bookingId, { bookingId: p.bookingId, flightId: p.flightId, reason: r.reason }, event.requestId),
        );
      }
    },

    [EventTypes.PaymentSucceeded]: async ({ client, event }) => {
      const p = PaymentResultPayload.parse(event.payload);
      const r = await confirmHold(client, p.bookingId);
      if (r) {
        await insertOutbox(client, TOPICS.inventory, r.flightId,
          makeEvent(EventTypes.SeatConfirmed, p.bookingId, { bookingId: p.bookingId, flightId: r.flightId, available: r.available }, event.requestId));
      }
    },

    // Compensation: payment failed, give the seat back.
    [EventTypes.PaymentFailed]: async ({ client, event, log }) => {
      const p = PaymentResultPayload.parse(event.payload);
      const r = await releaseHold(client, p.bookingId, 'RELEASED');
      if (r) {
        log.info({ flightId: r.flightId, available: r.available }, 'seat released after payment failure');
        await insertOutbox(client, TOPICS.inventory, r.flightId,
          makeEvent(EventTypes.SeatReleased, p.bookingId, { bookingId: p.bookingId, flightId: r.flightId, reason: 'PAYMENT_FAILED', available: r.available }, event.requestId));
      }
    },
  };
}

/** Releases holds whose TTL passed (e.g. Payment crashed), so a seat is never stranded. */
export async function sweepExpiredHolds(pool: Pool, log: Logger, limit = 100): Promise<number> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const { rows } = await client.query(
      `SELECT booking_id FROM seat_holds WHERE status = 'HELD' AND expires_at < now() ORDER BY expires_at LIMIT $1 FOR UPDATE SKIP LOCKED`,
      [limit],
    );
    for (const row of rows) {
      const r = await releaseHold(client, row.booking_id, 'EXPIRED');
      if (r) {
        await insertOutbox(client, TOPICS.inventory, r.flightId,
          makeEvent(EventTypes.SeatReleased, row.booking_id, { bookingId: row.booking_id, flightId: r.flightId, reason: 'EXPIRED', available: r.available }));
      }
    }
    await client.query('COMMIT');
    if (rows.length) log.info({ released: rows.length }, 'expired seat holds released');
    return rows.length;
  } catch (err) {
    await client.query('ROLLBACK').catch(() => undefined);
    throw err;
  } finally {
    client.release();
  }
}
