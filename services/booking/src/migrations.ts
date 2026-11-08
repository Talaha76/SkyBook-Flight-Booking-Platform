import { COMMON_MIGRATIONS } from '../../../packages/shared/src';

export const BOOKING_MIGRATIONS = [
  ...COMMON_MIGRATIONS,
  `CREATE TABLE IF NOT EXISTS bookings (
     id              UUID PRIMARY KEY,
     user_id         UUID NOT NULL,
     flight_id       UUID NOT NULL,
     passenger_name  TEXT NOT NULL,
     passenger_email TEXT NOT NULL,
     status          TEXT NOT NULL CHECK (status IN ('PENDING','SEAT_HELD','CONFIRMED','REJECTED','CANCELLED')),
     amount_cents    INT,
     hold_id         UUID,
     payment_id      UUID,
     failure_reason  TEXT,
     created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
     updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
   )`,
  `CREATE INDEX IF NOT EXISTS idx_bookings_user ON bookings (user_id, created_at DESC)`,
  `CREATE TABLE IF NOT EXISTS idempotency_keys (
     key            UUID PRIMARY KEY,
     user_id        UUID        NOT NULL,
     request_hash   CHAR(64)    NOT NULL,
     status         TEXT        NOT NULL CHECK (status IN ('IN_PROGRESS','COMPLETED')),
     response_code  INT,
     response_body  JSONB,
     created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
     expires_at     TIMESTAMPTZ NOT NULL DEFAULT now() + interval '24 hours'
   )`,
  `CREATE INDEX IF NOT EXISTS idx_idempotency_expires ON idempotency_keys (expires_at)`,
];
