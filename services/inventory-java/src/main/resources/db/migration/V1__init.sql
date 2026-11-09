-- Identical to the Node Inventory schema (packages/shared/src/db.ts COMMON_MIGRATIONS + services/inventory/src/migrations.ts).
-- Every statement is IF NOT EXISTS because the Node seed job may already have created these tables.

CREATE TABLE IF NOT EXISTS outbox_events (
  id              BIGSERIAL PRIMARY KEY,
  event_id        UUID UNIQUE NOT NULL,
  aggregate_id    UUID NOT NULL,
  topic           TEXT NOT NULL,
  event_key       TEXT NOT NULL,
  payload         JSONB NOT NULL,
  headers         JSONB NOT NULL DEFAULT '{}'::jsonb,
  status          TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING','PUBLISHED')),
  attempt_count   INT  NOT NULL DEFAULT 0,
  last_error      TEXT,
  next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at    TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox_events (status, next_attempt_at, id);

CREATE TABLE IF NOT EXISTS processed_events (
  consumer     TEXT NOT NULL,
  event_id     UUID NOT NULL,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer, event_id)
);

CREATE TABLE IF NOT EXISTS seat_inventory (
  flight_id    UUID PRIMARY KEY,
  total_seats  INT NOT NULL CHECK (total_seats >= 0),
  available    INT NOT NULL CHECK (available >= 0),
  price_cents  INT NOT NULL CHECK (price_cents >= 0),
  version      INT NOT NULL DEFAULT 0,
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (available <= total_seats)
);

CREATE TABLE IF NOT EXISTS seat_holds (
  id           UUID PRIMARY KEY,
  booking_id   UUID NOT NULL UNIQUE,
  flight_id    UUID NOT NULL REFERENCES seat_inventory (flight_id),
  status       TEXT NOT NULL CHECK (status IN ('HELD','CONFIRMED','RELEASED','EXPIRED')),
  amount_cents INT NOT NULL,
  expires_at   TIMESTAMPTZ NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_seat_holds_expiry ON seat_holds (expires_at) WHERE status = 'HELD';
