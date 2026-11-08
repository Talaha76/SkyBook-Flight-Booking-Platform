import { EventEnvelope } from '../../../packages/shared/src';

/** Fields that must never leave the Kafka boundary. */
const SENSITIVE = new Set(['paymentToken']);

/** BookingConfirmed -> skybook.booking.confirmed.v1, SeatHoldFailed -> skybook.seat.hold_failed.v1 */
export function contractName(type: string, version = 1): string {
  const words = type.match(/[A-Z][a-z0-9]*/g) ?? [type];
  const [entity, ...action] = words.map((w) => w.toLowerCase());
  return `skybook.${entity}.${action.length ? action.join('_') : 'event'}.v${version}`;
}

export interface SqsRecord {
  eventId: string;
  contract: string;
  body: string;
  attributes: Record<string, string>;
  occurredAt: string;
}

/** Converts an internal event into a versioned, public contract for downstream AWS consumers (Project 2). */
export function toSqsRecord(event: EventEnvelope<any>): SqsRecord {
  const data: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(event.payload)) if (!SENSITIVE.has(k)) data[k] = v;
  const contract = contractName(event.type, event.version);
  const body = JSON.stringify({
    contract,
    eventId: event.eventId,
    occurredAt: event.occurredAt,
    bookingId: event.payload.bookingId ?? event.aggregateId,
    flightId: event.payload.flightId,
    data,
  });
  const attributes: Record<string, string> = { eventId: event.eventId, contract };
  if (typeof event.payload.flightId === 'string') attributes.flightId = event.payload.flightId;
  if (event.requestId) attributes.requestId = event.requestId; // SQS rejects empty attribute values
  return { eventId: event.eventId, contract, body, attributes, occurredAt: event.occurredAt };
}
