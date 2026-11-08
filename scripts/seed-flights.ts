/**
 * Seeds the Flight Search read model and the Inventory service with the SAME flights.
 * Usage: npm run seed -- [--count 1000000] [--days 60] [--force]
 * Defaults come from SEED_FLIGHTS (20000) so `docker compose up` starts fast.
 * Always includes predictable demo flights YYZ->YVR (SB100/SB102) every day, plus SB999 (150 seats).
 */
import { randomUUID } from 'crypto';
import { Pool } from 'pg';
import { createLogger, createPool, env, runMigrations, waitForDb } from '../packages/shared/src';
import { FLIGHT_MIGRATIONS } from '../services/flight-search/src/migrations';
import { INVENTORY_MIGRATIONS } from '../services/inventory/src/migrations';

const log = createLogger('seed');

export const AIRPORTS: [string, string, string][] = [
  ['YYZ', 'Toronto', 'CA'], ['YVR', 'Vancouver', 'CA'], ['YUL', 'Montreal', 'CA'], ['YYC', 'Calgary', 'CA'], ['YOW', 'Ottawa', 'CA'],
  ['YEG', 'Edmonton', 'CA'], ['YWG', 'Winnipeg', 'CA'], ['YHZ', 'Halifax', 'CA'], ['YQB', 'Quebec City', 'CA'], ['YYJ', 'Victoria', 'CA'],
  ['JFK', 'New York', 'US'], ['LGA', 'New York', 'US'], ['BOS', 'Boston', 'US'], ['ORD', 'Chicago', 'US'], ['LAX', 'Los Angeles', 'US'],
  ['SFO', 'San Francisco', 'US'], ['SEA', 'Seattle', 'US'], ['MIA', 'Miami', 'US'], ['ATL', 'Atlanta', 'US'], ['DFW', 'Dallas', 'US'],
  ['DEN', 'Denver', 'US'], ['LAS', 'Las Vegas', 'US'], ['PHX', 'Phoenix', 'US'], ['IAD', 'Washington', 'US'], ['MCO', 'Orlando', 'US'],
  ['LHR', 'London', 'GB'], ['CDG', 'Paris', 'FR'], ['FRA', 'Frankfurt', 'DE'], ['AMS', 'Amsterdam', 'NL'], ['MAD', 'Madrid', 'ES'],
  ['FCO', 'Rome', 'IT'], ['ZRH', 'Zurich', 'CH'], ['DUB', 'Dublin', 'IE'], ['LIS', 'Lisbon', 'PT'], ['MUC', 'Munich', 'DE'],
  ['NRT', 'Tokyo', 'JP'], ['HND', 'Tokyo', 'JP'], ['ICN', 'Seoul', 'KR'], ['HKG', 'Hong Kong', 'HK'], ['SIN', 'Singapore', 'SG'],
  ['DXB', 'Dubai', 'AE'], ['DEL', 'Delhi', 'IN'], ['BOM', 'Mumbai', 'IN'], ['SYD', 'Sydney', 'AU'], ['MEX', 'Mexico City', 'MX'],
  ['CUN', 'Cancun', 'MX'], ['GRU', 'Sao Paulo', 'BR'], ['BOG', 'Bogota', 'CO'], ['LIM', 'Lima', 'PE'], ['JNB', 'Johannesburg', 'ZA'],
];
const AIRLINES = ['SkyBook Air', 'Maple Wings', 'Northern Jet', 'Polar Express'];

interface Flight {
  id: string; flightNumber: string; airline: string; origin: string; destination: string;
  date: string; dep: Date; arr: Date; price: number; seats: number;
}

/** Small deterministic PRNG so the same --count always produces a similar distribution. */
function rng(seed: number) {
  let s = seed >>> 0;
  return () => ((s = (s * 1664525 + 1013904223) >>> 0) / 2 ** 32);
}

const iso = (d: Date) => d.toISOString().slice(0, 10);

export function generateFlights(count: number, days: number, start = new Date()): Flight[] {
  const r = rng(42);
  const base = new Date(Date.UTC(start.getUTCFullYear(), start.getUTCMonth(), start.getUTCDate()));
  const out: Flight[] = [];
  const mk = (o: string, d: string, dayOffset: number, hour: number, num: string, seats: number, price: number): Flight => {
    const dep = new Date(base.getTime() + dayOffset * 86_400_000 + hour * 3_600_000);
    return { id: randomUUID(), flightNumber: num, airline: 'SkyBook Air', origin: o, destination: d, date: iso(dep), dep, arr: new Date(dep.getTime() + 5 * 3_600_000), price, seats };
  };
  // Predictable demo flights for docs / e2e.
  for (let day = 0; day < days; day++) {
    out.push(mk('YYZ', 'YVR', day, 13, 'SB100', 180, 34900));
    out.push(mk('YYZ', 'YVR', day, 22, 'SB102', 180, 29900));
    out.push(mk('YVR', 'YYZ', day, 15, 'SB101', 180, 33900));
    out.push(mk('YYZ', 'JFK', day, 12, 'SB999', 150, 19900));
  }
  // Popular routes get more traffic (Zipf-like), matching the k6 "popular routes" mix.
  while (out.length < count) {
    const oi = Math.floor(r() ** 2 * AIRPORTS.length);
    let di = Math.floor(r() ** 2 * AIRPORTS.length);
    if (di === oi) di = (di + 1) % AIRPORTS.length;
    const day = Math.floor(r() * days);
    const dep = new Date(base.getTime() + day * 86_400_000 + Math.floor(r() * 24 * 12) * 300_000);
    const durH = 1 + Math.floor(r() * 14);
    out.push({
      id: randomUUID(),
      flightNumber: `${['SB', 'MW', 'NJ', 'PX'][Math.floor(r() * 4)]}${100 + Math.floor(r() * 8900)}`,
      airline: AIRLINES[Math.floor(r() * AIRLINES.length)],
      origin: AIRPORTS[oi][0],
      destination: AIRPORTS[di][0],
      date: iso(dep),
      dep,
      arr: new Date(dep.getTime() + durH * 3_600_000),
      price: 9900 + Math.floor(r() * 150000),
      seats: [120, 150, 180, 220, 300][Math.floor(r() * 5)],
    });
  }
  return out;
}

async function insertBatch(flightsDb: Pool, inventoryDb: Pool, batch: Flight[]) {
  await flightsDb.query(
    `INSERT INTO flights (id, flight_number, airline, origin, destination, departure_date, departure_time, arrival_time, price_cents, total_seats, seats_available)
     SELECT * , t.seats FROM unnest($1::uuid[], $2::text[], $3::text[], $4::text[], $5::text[], $6::date[], $7::timestamptz[], $8::timestamptz[], $9::int[], $10::int[])
       AS t(id, fn, al, o, d, dd, dt, at, p, seats)`,
    [batch.map((f) => f.id), batch.map((f) => f.flightNumber), batch.map((f) => f.airline), batch.map((f) => f.origin), batch.map((f) => f.destination),
      batch.map((f) => f.date), batch.map((f) => f.dep), batch.map((f) => f.arr), batch.map((f) => f.price), batch.map((f) => f.seats)],
  );
  await inventoryDb.query(
    `INSERT INTO seat_inventory (flight_id, total_seats, available, price_cents)
     SELECT id, seats, seats, p FROM unnest($1::uuid[], $2::int[], $3::int[]) AS t(id, seats, p)`,
    [batch.map((f) => f.id), batch.map((f) => f.seats), batch.map((f) => f.price)],
  );
}

/** Seeds both databases. Exported so the benchmark tooling can reseed to a larger size. */
export async function runSeed(opts: { count: number; days: number; force: boolean; flightsUrl: string; inventoryUrl: string }) {
  const { count, days, force } = opts;
  const flightsDb = createPool(opts.flightsUrl);
  const inventoryDb = createPool(opts.inventoryUrl);
  try {
    await waitForDb(flightsDb, log);
    await waitForDb(inventoryDb, log);
    await runMigrations(flightsDb, 'flight-search', FLIGHT_MIGRATIONS, log);
    await runMigrations(inventoryDb, 'inventory', INVENTORY_MIGRATIONS, log);

    const { rows } = await flightsDb.query(`SELECT count(*)::int AS n FROM flights`);
    if (rows[0].n > 0 && !force) {
      log.info({ existing: rows[0].n }, 'flights already seeded - skipping (use --force to reseed)');
      return rows[0].n as number;
    }
    if (force) {
      log.info('force: truncating flights and inventory');
      await flightsDb.query(`TRUNCATE flights`);
      await inventoryDb.query(`TRUNCATE seat_holds, seat_inventory CASCADE`);
    }
    await flightsDb.query(
      `INSERT INTO airports (code, city, country) SELECT * FROM unnest($1::text[], $2::text[], $3::text[]) ON CONFLICT DO NOTHING`,
      [AIRPORTS.map((a) => a[0]), AIRPORTS.map((a) => a[1]), AIRPORTS.map((a) => a[2])],
    );
    const started = Date.now();
    const flights = generateFlights(count, days);
    const size = 5000;
    for (let i = 0; i < flights.length; i += size) {
      await insertBatch(flightsDb, inventoryDb, flights.slice(i, i + size));
      if ((i / size) % 20 === 0) log.info({ inserted: Math.min(i + size, flights.length), total: flights.length }, 'seeding');
    }
    await flightsDb.query('ANALYZE flights');
    log.info({ flights: flights.length, seconds: (Date.now() - started) / 1000 }, 'seed complete');
    return flights.length;
  } finally {
    await flightsDb.end();
    await inventoryDb.end();
  }
}

async function main() {
  const args = process.argv.slice(2);
  const arg = (name: string) => { const i = args.indexOf(`--${name}`); return i >= 0 ? args[i + 1] : undefined; };
  await runSeed({
    count: Number(arg('count') ?? process.env.SEED_FLIGHTS ?? 20000),
    days: Number(arg('days') ?? process.env.SEED_DAYS ?? 60),
    force: args.includes('--force'),
    flightsUrl: env('FLIGHTS_DATABASE_URL'),
    inventoryUrl: env('INVENTORY_DATABASE_URL'),
  });
}

if (require.main === module) {
  main().catch((err) => { log.fatal({ err }, 'seed failed'); process.exit(1); });
}
