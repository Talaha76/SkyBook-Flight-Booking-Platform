import { COMMON_MIGRATIONS } from '../../../packages/shared/src';

export const FLIGHT_MIGRATIONS = [
  ...COMMON_MIGRATIONS,
  `CREATE TABLE IF NOT EXISTS airports (
     code TEXT PRIMARY KEY,
     city TEXT NOT NULL,
     country TEXT NOT NULL
   )`,
  `CREATE TABLE IF NOT EXISTS flights (
     id              UUID PRIMARY KEY,
     flight_number   TEXT NOT NULL,
     airline         TEXT NOT NULL,
     origin          TEXT NOT NULL,
     destination     TEXT NOT NULL,
     departure_date  DATE NOT NULL,
     departure_time  TIMESTAMPTZ NOT NULL,
     arrival_time    TIMESTAMPTZ NOT NULL,
     price_cents     INT NOT NULL,
     total_seats     INT NOT NULL,
     seats_available INT NOT NULL,
     updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
   )`,
  // Composite index for the main search query (see docs/benchmarks.md for the before/after plan).
  `CREATE INDEX IF NOT EXISTS idx_flights_route_date ON flights (origin, destination, departure_date, departure_time)`,
];
