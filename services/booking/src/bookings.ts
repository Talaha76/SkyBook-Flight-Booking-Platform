import { randomUUID } from 'crypto';
import { Pool } from 'pg';
import { z } from 'zod';
import { EventTypes, HttpError, TOPICS, insertOutbox, makeEvent, withTx } from '../../../packages/shared/src';
import { beginIdempotent, hashRequest, pgIdempotencyRepo } from './idempotency';

export const CreateBookingBody = z.object({
  flightId: z.string().uuid(),
  passengerName: z.string().trim().min(1).max(200),
  passengerEmail: z.string().email().max(320),
  /** Fake payment token. tok_success always succeeds, tok_fail always fails, anything else uses the random failure rate. */
  paymentToken: z.string().min(3).max(100),
});
export type CreateBookingBody = z.infer<typeof CreateBookingBody>;

export interface BookingResult {
  status: number;
  body: unknown;
  headers?: Record<string, string>;
  replayed?: boolean;
}

export function toBookingDto(row: any) {
  return {
    id: row.id,
    userId: row.user_id,
    flightId: row.flight_id,
    passengerName: row.passenger_name,
    passengerEmail: row.passenger_email,
    status: row.status,
    amountCents: row.amount_cents,
    failureReason: row.failure_reason,
    createdAt: new Date(row.created_at).toISOString(),
    updatedAt: new Date(row.updated_at).toISOString(),
  };
}

/**
 * Idempotent booking creation (build guide section 3.1):
 * claim key -> in ONE transaction write booking + outbox event + completed idempotency row.
 */
export async function createBooking(
  pool: Pool,
  args: { userId: string; idempotencyKey: string; body: unknown; requestId?: string; path?: string },
): Promise<BookingResult> {
  if (!z.string().uuid().safeParse(args.idempotencyKey).success) {
    throw new HttpError(400, 'Idempotency-Key header must be a UUID');
  }
  const input = CreateBookingBody.parse(args.body);
  const hash = hashRequest('POST', args.path ?? '/bookings', args.userId, input);
  const repo = pgIdempotencyRepo(pool);
  const decision = await beginIdempotent(repo, args.idempotencyKey, args.userId, hash);

  switch (decision.kind) {
    case 'replay':
      return { status: decision.code, body: decision.body, headers: { 'Idempotent-Replayed': 'true' }, replayed: true };
    case 'in_progress':
      throw new HttpError(409, 'a request with this Idempotency-Key is still being processed', undefined, { 'Retry-After': '1' });
    case 'mismatch':
      throw new HttpError(422, 'Idempotency-Key was already used with a different request');
    case 'proceed':
      break;
  }

  try {
    return await withTx(pool, async (client) => {
      const bookingId = randomUUID();
      const { rows } = await client.query(
        `INSERT INTO bookings (id, user_id, flight_id, passenger_name, passenger_email, status)
         VALUES ($1, $2, $3, $4, $5, 'PENDING') RETURNING *`,
        [bookingId, args.userId, input.flightId, input.passengerName, input.passengerEmail],
      );
      const event = makeEvent(
        EventTypes.BookingRequested,
        bookingId,
        { bookingId, flightId: input.flightId, userId: args.userId, passengerName: input.passengerName, passengerEmail: input.passengerEmail, paymentToken: input.paymentToken },
        args.requestId,
      );
      await insertOutbox(client, TOPICS.booking, input.flightId, event);
      const body = toBookingDto(rows[0]);
      const upd = await client.query(
        `UPDATE idempotency_keys SET status = 'COMPLETED', response_code = 201, response_body = $2
          WHERE key = $1 AND status = 'IN_PROGRESS'`,
        [args.idempotencyKey, body],
      );
      if (upd.rowCount !== 1) throw new HttpError(409, 'idempotency key lost during processing', undefined, { 'Retry-After': '1' });
      return { status: 201, body };
    });
  } catch (err) {
    await repo.release(args.idempotencyKey).catch(() => undefined);
    throw err;
  }
}
