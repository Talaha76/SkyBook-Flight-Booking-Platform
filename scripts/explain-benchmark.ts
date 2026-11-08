/**
 * Measures the main search query with and without the composite index (build guide 3.6).
 * Usage: FLIGHTS_DATABASE_URL=... node dist/scripts/explain-benchmark.js [--runs 50]
 * Drops idx_flights_route_date, times EXPLAIN (ANALYZE, BUFFERS) runs, recreates the index,
 * times again, prints a markdown table. The index is always restored at the end.
 */
import fs from 'fs';
import os from 'os';
import path from 'path';
import { createLogger, createPool, env } from '../packages/shared/src';

const log = createLogger('explain-benchmark');
const QUERY = `SELECT id, flight_number, departure_time, price_cents, seats_available FROM flights
  WHERE origin = $1 AND destination = $2 AND departure_date = $3 ORDER BY departure_time LIMIT 100`;

const pct = (xs: number[], p: number) => {
  const s = [...xs].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.ceil((p / 100) * s.length) - 1)];
};

async function main() {
  const runs = Number(process.argv[process.argv.indexOf('--runs') + 1]) || 50;
  const pool = createPool(env('FLIGHTS_DATABASE_URL'), 2);
  const { rows: [{ n }] } = await pool.query(`SELECT count(*)::int AS n FROM flights`);
  const { rows: samples } = await pool.query(
    `SELECT origin, destination, to_char(departure_date,'YYYY-MM-DD') AS d FROM flights ORDER BY random() LIMIT $1`, [runs],
  );

  async function measure(label: string) {
    const times: number[] = [];
    let plan = '';
    let sampleText = '';
    for (const s of samples) {
      const r = await pool.query(`EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ${QUERY}`, [s.origin, s.destination, s.d]);
      const j = r.rows[0]['QUERY PLAN'][0];
      times.push(j['Execution Time']);
      if (!plan) {
        const nodes: string[] = [];
        const walk = (p: any) => { nodes.push(p['Node Type'] + (p['Index Name'] ? ` (${p['Index Name']})` : '')); (p.Plans ?? []).forEach(walk); };
        walk(j.Plan);
        plan = nodes.join(' -> ');
        const t = await pool.query(`EXPLAIN (ANALYZE, BUFFERS) ${QUERY}`, [s.origin, s.destination, s.d]);
        sampleText = t.rows.map((x) => x['QUERY PLAN']).join('\n');
      }
    }
    return { label, plan, sampleText, p50: pct(times, 50), p95: pct(times, 95), p99: pct(times, 99) };
  }

  try {
    await pool.query(`DROP INDEX IF EXISTS idx_flights_route_date`);
    await pool.query(`ANALYZE flights`);
    const before = await measure('no index (sequential scan)');
    await pool.query(`CREATE INDEX idx_flights_route_date ON flights (origin, destination, departure_date, departure_time)`);
    await pool.query(`ANALYZE flights`);
    const after = await measure('composite index');

    const f = (x: number) => x.toFixed(3);
    console.log(`\n### Search query, ${n.toLocaleString()} rows, ${runs} random routes per run`);
    console.log(`Machine: ${os.cpus().length} vCPU, ${(os.totalmem() / 2 ** 30).toFixed(1)} GB RAM, ${os.platform()}; PostgreSQL server execution time from EXPLAIN ANALYZE.\n`);
    console.log('| Variant | Plan | p50 ms | p95 ms | p99 ms |\n|---|---|---|---|---|');
    for (const r of [before, after]) console.log(`| ${r.label} | ${r.plan} | ${f(r.p50)} | ${f(r.p95)} | ${f(r.p99)} |`);
    console.log(`\nSpeed-up at p95: ${(before.p95 / after.p95).toFixed(0)}x\n`);
    const dir = process.env.RESULTS_DIR ?? '/results';
    if (fs.existsSync(dir)) {
      const strip = ({ sampleText, ...r }: typeof before) => r;
      fs.writeFileSync(path.join(dir, 'explain.json'), JSON.stringify({ label: 'explain', rows: n, runs, before: strip(before), after: strip(after), planWithoutIndex: before.sampleText, planWithIndex: after.sampleText }, null, 2));
    }
    console.log('#### Sample plan without index\n```\n' + before.sampleText + '\n```\n#### Sample plan with index\n```\n' + after.sampleText + '\n```');
  } finally {
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_flights_route_date ON flights (origin, destination, departure_date, departure_time)`);
    await pool.end();
  }
}

main().catch((err) => { log.fatal({ err }, 'benchmark failed'); process.exit(1); });
