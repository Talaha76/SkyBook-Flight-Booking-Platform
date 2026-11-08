/**
 * Prepares the Flight Search database for a benchmark run.
 *   node dist/scripts/bench-prep.js --rows 1000000 --index off|on
 * --rows   reseeds flights + inventory to exactly this many rows if the table holds fewer
 * --index  drops or (re)creates idx_flights_route_date, then ANALYZEs
 * Always clears cached search results so every run starts cold.
 * Env: FLIGHTS_DATABASE_URL, INVENTORY_DATABASE_URL, REDIS_URL
 */
import Redis from 'ioredis';
import { createLogger, createPool, env } from '../packages/shared/src';
import { runSeed } from './seed-flights';

const log = createLogger('bench-prep');

async function main() {
  const args = process.argv.slice(2);
  const arg = (n: string) => { const i = args.indexOf(`--${n}`); return i >= 0 ? args[i + 1] : undefined; };
  const rows = arg('rows') ? Number(arg('rows')) : undefined;
  const index = arg('index');

  const flightsUrl = env('FLIGHTS_DATABASE_URL');
  if (rows) {
    const pool = createPool(flightsUrl, 1);
    const { rows: r } = await pool.query(`SELECT count(*)::int AS n FROM flights`).catch((err) => {
      if (err.code === '42P01') return { rows: [{ n: 0 }] }; // table not created yet
      throw err;
    });
    await pool.end();
    if (r[0].n < rows) {
      console.log(`flights table has ${r[0].n} rows; reseeding to ${rows} (takes ~1 min per million rows)...`);
      await runSeed({ count: rows, days: 60, force: true, flightsUrl, inventoryUrl: env('INVENTORY_DATABASE_URL') });
    } else {
      console.log(`flights table already has ${r[0].n} rows`);
    }
  }

  const pool = createPool(flightsUrl, 1);
  if (index === 'off') await pool.query(`DROP INDEX IF EXISTS idx_flights_route_date`);
  if (index === 'on') await pool.query(`CREATE INDEX IF NOT EXISTS idx_flights_route_date ON flights (origin, destination, departure_date, departure_time)`);
  await pool.query(`ANALYZE flights`);
  const { rows: idx } = await pool.query(`SELECT count(*)::int AS n FROM pg_indexes WHERE indexname = 'idx_flights_route_date'`);
  const { rows: cnt } = await pool.query(`SELECT count(*)::int AS n FROM flights`);
  await pool.end();

  const redis = new Redis(env('REDIS_URL', 'redis://localhost:6379'), { maxRetriesPerRequest: 2 });
  let cleared = 0;
  for (const pattern of ['search:*', 'lock:search:*']) {
    let cursor = '0';
    do {
      const [next, keys] = await redis.scan(cursor, 'MATCH', pattern, 'COUNT', 1000);
      cursor = next;
      if (keys.length) cleared += await redis.del(...keys);
    } while (cursor !== '0');
  }
  await redis.quit();
  console.log(`ready: ${cnt[0].n} flights, composite index ${idx[0].n ? 'PRESENT' : 'ABSENT'}, ${cleared} cached searches cleared`);
}

main().catch((err) => { log.fatal({ err }, 'bench-prep failed'); process.exit(1); });
