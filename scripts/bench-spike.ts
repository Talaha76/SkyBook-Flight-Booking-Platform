/**
 * Spike monitor: run it in the background while k6 booking-burst.js runs.
 *   node dist/scripts/bench-spike.js [--max 600]
 * Every second it samples, for each SkyBook consumer group, lag = high watermark - committed offset.
 * It reports peak lag, how long the consumers needed to catch up after production stopped, and
 * whether every produced message was consumed (committed == high watermark for every group/partition).
 * It also checks that every booking created during the window reached a final saga state.
 * Env: KAFKA_BROKERS, BOOKING_DATABASE_URL, RESULTS_DIR (default /results)
 */
import fs from 'fs';
import path from 'path';
import { Kafka, logLevel } from 'kafkajs';
import { Pool } from 'pg';

const GROUPS: Record<string, string[]> = {
  'booking-saga': ['inventory.events', 'payment.events'],
  inventory: ['booking.events', 'payment.events'],
  payment: ['inventory.events'],
  notification: ['booking.events'],
  'flight-search-cache': ['inventory.events'],
  'aws-bridge': ['booking.events', 'inventory.events'],
};
const PRODUCED_TOPICS = ['booking.events', 'inventory.events', 'payment.events'];

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const now = () => Date.now();

async function main() {
  const maxSec = Number(process.argv[process.argv.indexOf('--max') + 1]) || 900;
  const resultsDir = process.env.RESULTS_DIR ?? '/results';
  const kafka = new Kafka({ clientId: 'spike-monitor', brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','), logLevel: logLevel.NOTHING });
  const admin = kafka.admin();
  await admin.connect();
  const pool = process.env.BOOKING_DATABASE_URL ? new Pool({ connectionString: process.env.BOOKING_DATABASE_URL, max: 1 }) : null;

  const existing = new Set((await admin.listGroups()).groups.map((g) => g.groupId));
  const groups = Object.keys(GROUPS).filter((g) => existing.has(g));

  async function highWatermarks() {
    const hw = new Map<string, number>();
    for (const topic of PRODUCED_TOPICS) for (const p of await admin.fetchTopicOffsets(topic)) hw.set(`${topic}:${p.partition}`, Number(p.high));
    return hw;
  }
  async function lagByGroup(hw: Map<string, number>) {
    const lag: Record<string, number> = {};
    for (const g of groups) {
      let total = 0;
      for (const t of await admin.fetchOffsets({ groupId: g, topics: GROUPS[g] })) {
        for (const p of t.partitions) {
          const high = hw.get(`${t.topic}:${p.partition}`) ?? 0;
          const committed = Number(p.offset) < 0 ? 0 : Number(p.offset);
          total += Math.max(0, high - committed);
        }
      }
      lag[g] = total;
    }
    return lag;
  }
  const sum = (m: Map<string, number>) => [...m.values()].reduce((a, b) => a + b, 0);

  const startedAt = now();
  const hw0 = await highWatermarks();
  const produced0 = sum(hw0);
  console.log(`[spike-monitor] watching ${groups.join(', ')}; baseline ${produced0} events`);

  let lastProduced = produced0;
  let lastProducedChangeAt = startedAt;
  let sawTraffic = false;
  let peakLag = 0;
  let peakLagAt = 0;
  let peakByGroup: Record<string, number> = {};
  let caughtUpAt = 0;
  let zeroSince = 0;
  const samples: { t: number; produced: number; lag: number }[] = [];

  while ((now() - startedAt) / 1000 < maxSec) {
    const hw = await highWatermarks();
    const produced = sum(hw);
    const lag = await lagByGroup(hw);
    const total = Object.values(lag).reduce((a, b) => a + b, 0);
    if (produced !== lastProduced) { lastProduced = produced; lastProducedChangeAt = now(); if (produced - produced0 > 20) sawTraffic = true; caughtUpAt = 0; }
    if (total > peakLag) { peakLag = total; peakLagAt = now(); peakByGroup = { ...lag }; }
    samples.push({ t: Math.round((now() - startedAt) / 1000), produced: produced - produced0, lag: total });
    if (total === 0) { zeroSince = zeroSince || now(); if (!caughtUpAt) caughtUpAt = now(); } else { zeroSince = 0; caughtUpAt = 0; }
    // Done: traffic happened, nothing new produced for 15 s, and every consumer has been caught up for 5 s.
    if (sawTraffic && now() - lastProducedChangeAt > 15_000 && zeroSince && now() - zeroSince > 5_000) break;
    await sleep(1000);
  }

  const hwEnd = await highWatermarks();
  const lagEnd = await lagByGroup(hwEnd);
  const unconsumed = Object.values(lagEnd).reduce((a, b) => a + b, 0);
  const producedDuring = sum(hwEnd) - produced0;

  let bookings: Record<string, number> = {};
  let loadEndedAt = 0; // when the last spike booking request was accepted
  if (pool) {
    // Give in-flight sagas up to 60 s to reach a final state before counting.
    for (let i = 0; i < 60; i++) {
      const { rows } = await pool.query(
        `SELECT status, count(*)::int AS n FROM bookings WHERE passenger_name = 'Spike Test' AND created_at >= to_timestamp($1 / 1000.0) GROUP BY status`,
        [startedAt],
      );
      bookings = Object.fromEntries(rows.map((r) => [r.status, r.n]));
      if (!bookings.PENDING && !bookings.SEAT_HELD) break;
      await sleep(1000);
    }
    const { rows: last } = await pool.query(
      `SELECT (extract(epoch from max(created_at)) * 1000)::bigint AS t FROM bookings WHERE passenger_name = 'Spike Test' AND created_at >= to_timestamp($1 / 1000.0)`,
      [startedAt],
    );
    loadEndedAt = Number(last[0]?.t ?? 0);
    await pool.end();
  }
  const totalBookings = Object.values(bookings).reduce((a, b) => a + b, 0);
  const unfinished = (bookings.PENDING ?? 0) + (bookings.SEAT_HELD ?? 0);

  const out = {
    label: 'spike-monitor',
    groups,
    eventsProduced: producedDuring,
    peakConsumerLag: peakLag,
    peakLagByGroup: peakByGroup,
    peakLagAtSec: peakLagAt ? Math.round((peakLagAt - startedAt) / 1000) : 0,
    loadEndedAtSec: loadEndedAt ? Math.round((loadEndedAt - startedAt) / 1000) : null,
    allConsumedAtSec: caughtUpAt ? Math.round((caughtUpAt - startedAt) / 1000) : null,
    // Time from the last booking request until every consumer group had processed everything.
    catchUpSeconds: caughtUpAt && loadEndedAt ? Math.max(0, Math.round((caughtUpAt - loadEndedAt) / 1000)) : null,
    unconsumedAtEnd: unconsumed,
    // Only meaningful once consumers caught up; if the monitor hit --max first, the rest was still draining (not lost).
    timedOut: !(sawTraffic && zeroSince),
    lostMessages: sawTraffic && zeroSince ? unconsumed : null,
    bookings: { total: totalBookings, ...bookings, unfinished },
    samples,
    finishedAt: new Date().toISOString(),
  };
  if (fs.existsSync(resultsDir)) fs.writeFileSync(path.join(resultsDir, 'spike-monitor.json'), JSON.stringify(out, null, 2));
  console.log(`[spike-monitor] produced ${producedDuring} events; peak consumer lag ${peakLag} (at +${out.peakLagAtSec}s); ` +
    `all consumers caught up ${out.catchUpSeconds ?? '?'} s after the last booking request; unconsumed at end: ${unconsumed}; ` +
    `bookings ${totalBookings} (${JSON.stringify(bookings)}), unfinished ${unfinished}`);
  await admin.disconnect();
  if (out.timedOut) console.log(`[spike-monitor] WARNING: stopped after ${maxSec}s while consumers were still draining; re-run with a larger --max`);
  process.exit(unconsumed === 0 && unfinished === 0 ? 0 : 1);
}

main().catch((err) => { console.error(err); process.exit(1); });
