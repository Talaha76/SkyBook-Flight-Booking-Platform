import { randomUUID } from 'crypto';
import { z } from 'zod';

export const TOPICS = {
  booking: 'booking.events',
  inventory: 'inventory.events',
  payment: 'payment.events',
  notificationRetry1: 'notification.retry.1m',
  notificationRetry2: 'notification.retry.5m',
  notificationRetry3: 'notification.retry.30m',
  notificationDlq: 'notification.dlq',
} as const;

export const ALL_TOPICS = Object.values(TOPICS);

/** Every message on Kafka uses this envelope. Messages are keyed by flightId. */
export const EventEnvelope = z.object({
  eventId: z.string().uuid(),
  type: z.string().min(1),
  version: z.number().int().positive(),
  occurredAt: z.string(),
  aggregateId: z.string().uuid(),
  requestId: z.string().optional(),
  payload: z.record(z.string(), z.unknown()),
});
export type EventEnvelope<P = Record<string, unknown>> = Omit<z.infer<typeof EventEnvelope>, 'payload'> & { payload: P };

// ---- Event type names --------------------------------------------------------------------------
export const EventTypes = {
  BookingRequested: 'BookingRequested',
  BookingConfirmed: 'BookingConfirmed',
  BookingCancelled: 'BookingCancelled',
  BookingRejected: 'BookingRejected',
  SeatHeld: 'SeatHeld',
  SeatHoldFailed: 'SeatHoldFailed',
  SeatReleased: 'SeatReleased',
  SeatConfirmed: 'SeatConfirmed',
  PaymentSucceeded: 'PaymentSucceeded',
  PaymentFailed: 'PaymentFailed',
} as const;

// ---- Payload schemas (validated by consumers) ----------------------------------------------------
export const BookingRequestedPayload = z.object({
  bookingId: z.string().uuid(),
  flightId: z.string().uuid(),
  userId: z.string().uuid(),
  passengerName: z.string(),
  passengerEmail: z.string(),
  paymentToken: z.string(),
});
export const SeatHeldPayload = z.object({
  bookingId: z.string().uuid(),
  flightId: z.string().uuid(),
  holdId: z.string().uuid(),
  amountCents: z.number().int(),
  expiresAt: z.string(),
  paymentToken: z.string(),
  available: z.number().int(),
});
export const SeatHoldFailedPayload = z.object({
  bookingId: z.string().uuid(),
  flightId: z.string().uuid(),
  reason: z.string(),
});
export const SeatReleasedPayload = z.object({
  bookingId: z.string().uuid(),
  flightId: z.string().uuid(),
  reason: z.string(),
  available: z.number().int(),
});
export const SeatConfirmedPayload = z.object({
  bookingId: z.string().uuid(),
  flightId: z.string().uuid(),
  available: z.number().int(),
});
export const PaymentResultPayload = z.object({
  bookingId: z.string().uuid(),
  flightId: z.string().uuid(),
  paymentId: z.string().uuid(),
  amountCents: z.number().int(),
  reason: z.string().optional(),
});
export const BookingOutcomePayload = z.object({
  bookingId: z.string().uuid(),
  flightId: z.string().uuid(),
  userId: z.string().uuid(),
  passengerName: z.string(),
  passengerEmail: z.string(),
  status: z.string(),
  reason: z.string().optional(),
  amountCents: z.number().int().nullable().optional(),
});

export function makeEvent<P extends Record<string, unknown>>(
  type: string,
  aggregateId: string,
  payload: P,
  requestId?: string,
): EventEnvelope<P> {
  return { eventId: randomUUID(), type, version: 1, occurredAt: new Date().toISOString(), aggregateId, requestId, payload };
}
