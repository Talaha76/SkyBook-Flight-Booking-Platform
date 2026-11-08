import { COMMON_MIGRATIONS } from '../../../packages/shared/src';

export const INVENTORY_MIGRATIONS = [
  ...COMMON_MIGRATIONS,
  `CREATE TABLE IF NOT EXISTS seat_inventory (
     flight_id    UUID PRIMARY KEY,
     total_seats  INT NOT NULL CHECK (total_seats >= 0),
     available    INT NOT NULL CHECK (available >= 0),
     price_cents  INT NOT NULL CHECK (price_cents >= 0),
     version      INT NOT NULL DEFAULT 0,
     updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
     CHECK (available <= total_seats)
   )`,
  `CREATE TABLE IF NOT EXISTS seat_holds (
     id           UUID PRIMARY KEY,
     booking_id   UUID NOT NULL UNIQUE,
     flight_id    UUID NOT NULL REFERENCES seat_inventory (flight_id),
     status       TEXT NOT NULL CHECK (status IN ('HELD','CONFIRMED','RELEASED','EXPIRED')),
     amount_cents INT NOT NULL,
     expires_at   TIMESTAMPTZ NOT NULL,
     created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
     updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
   )`,
  `CREATE INDEX IF NOT EXISTS idx_seat_holds_expiry ON seat_holds (expires_at) WHERE status = 'HELD'`,
];
