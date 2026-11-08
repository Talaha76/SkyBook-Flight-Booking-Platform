import { PoolClient } from 'pg';
import {
  BookingOutcomePayload,
  EventHandler,
  EventTypes,
  PaymentResultPayload,
  SeatHeldPayload,
  SeatHoldFailedPayload,
  SeatReleasedPayload,
  TOPICS,
  insertOutbox,
  makeEvent,
} from '../../../packages/shared/src';

export type BookingStatus = 'PENDING' | 'SEAT_HELD' | 'CONFIRMED' | 'REJECTED' | 'CANCELLED';
export const TERMINAL: BookingStatus[] = ['CONFIRMED', 'REJECTED', 'CANCELLED'];

/**
 * Booking state machine. Returns the next status, or null when the event does not apply
 * (already terminal, or an out-of-order/duplicate event). Payment events may arrive before
 * SeatHeld because they travel on different topics, so PENDING accepts them too.
 */
export function nextStatus(current: BookingStatus, eventType: string, reason?: string): BookingStatus | null {
  if (TERMINAL.includes(current)) return null;
  switch (eventType) {
    case EventTypes.SeatHeld:
      return current === 'PENDING' ? 'SEAT_HELD' : null;
    case EventTypes.SeatHoldFailed:
      return 'REJECTED';
    case EventTypes.PaymentSucceeded:
      return 'CONFIRMED';
    case EventTypes.PaymentFailed:
      return 'CANCELLED';
    case EventTypes.SeatReleased:
      return reason === 'EXPIRED' ? 'CANCELLED' : null;
    default:
      return null;
  }
}

const OUTCOME_EVENT: Partial<Record<BookingStatus, string>> = {
  CONFIRMED: EventTypes.BookingConfirmed,
  CANCELLED: EventTypes.BookingCancelled,
  REJECTED: EventTypes.BookingRejected,
};

async function transition(
  client: PoolClient,
  bookingId: string,
  eventType: string,
  requestId: string | undefined,
  patch: { reason?: string; holdId?: string; paymentId?: string; amountCents?: number },
  log: { info: (o: object, m: string) => void },
) {
  const { rows } = await client.query(`SELECT * FROM bookings WHERE id = $1 FOR UPDATE`, [bookingId]);
  const booking = rows[0];
  if (!booking) {
    log.info({ bookingId }, 'event for unknown booking ignored');
    return;
  }
  const next = nextStatus(booking.status, eventType, patch.reason);
  if (!next) {
    log.info({ bookingId, status: booking.status, eventType }, 'transition not applicable');
    return;
  }
  const { rows: updated } = await client.query(
    `UPDATE bookings SET status = $2,
        failure_reason = COALESCE($3, failure_reason),
        hold_id = COALESCE($4, hold_id),
        payment_id = COALESCE($5, payment_id),
        amount_cents = COALESCE($6, amount_cents),
        updated_at = now()
      WHERE id = $1 RETURNING *`,
    [bookingId, next, next === 'CONFIRMED' ? null : patch.reason ?? null, patch.holdId ?? null, patch.paymentId ?? null, patch.amountCents ?? null],
  );
  const b = updated[0];
  log.info({ bookingId, from: booking.status, to: next }, 'booking transitioned');
  const outcomeType = OUTCOME_EVENT[next as BookingStatus];
  if (outcomeType) {
    const payload = BookingOutcomePayload.parse({
      bookingId: b.id,
      flightId: b.flight_id,
      userId: b.user_id,
      passengerName: b.passenger_name,
      passengerEmail: b.passenger_email,
      status: b.status,
      reason: b.failure_reason ?? undefined,
      amountCents: b.amount_cents,
    });
    await insertOutbox(client, TOPICS.booking, b.flight_id, makeEvent(outcomeType, b.id, payload, requestId));
  }
}

export const sagaHandlers: Record<string, EventHandler> = {
  [EventTypes.SeatHeld]: async ({ client, event, log }) => {
    const p = SeatHeldPayload.parse(event.payload);
    await transition(client, p.bookingId, event.type, event.requestId, { holdId: p.holdId, amountCents: p.amountCents }, log);
  },
  [EventTypes.SeatHoldFailed]: async ({ client, event, log }) => {
    const p = SeatHoldFailedPayload.parse(event.payload);
    await transition(client, p.bookingId, event.type, event.requestId, { reason: p.reason }, log);
  },
  [EventTypes.SeatReleased]: async ({ client, event, log }) => {
    const p = SeatReleasedPayload.parse(event.payload);
    await transition(client, p.bookingId, event.type, event.requestId, { reason: p.reason }, log);
  },
  [EventTypes.PaymentSucceeded]: async ({ client, event, log }) => {
    const p = PaymentResultPayload.parse(event.payload);
    await transition(client, p.bookingId, event.type, event.requestId, { paymentId: p.paymentId, amountCents: p.amountCents }, log);
  },
  [EventTypes.PaymentFailed]: async ({ client, event, log }) => {
    const p = PaymentResultPayload.parse(event.payload);
    await transition(client, p.bookingId, event.type, event.requestId, { paymentId: p.paymentId, reason: p.reason ?? 'PAYMENT_FAILED' }, log);
  },
};
