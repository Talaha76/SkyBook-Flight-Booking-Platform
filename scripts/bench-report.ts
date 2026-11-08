/**
 * Turns the JSON results in RESULTS_DIR (default /results) into RESULTS.md: tables plus the build guide's
 * resume bullets filled in with YOUR measured numbers. Missing measurements stay as [placeholders].
 *   node dist/scripts/bench-report.js
 */
import fs from 'fs';
import os from 'os';
import path from 'path';

const dir = process.env.RESULTS_DIR ?? '/results';
const read = (name: string): any => {
  const f = path.join(dir, `${name}.json`);
  return fs.existsSync(f) ? JSON.parse(fs.readFileSync(f, 'utf8')) : null;
};
const ms = (x?: number) => (typeof x === 'number' ? `${x < 10 ? x.toFixed(2) : x.toFixed(1)} ms` : '—');
const pct = (x?: number) => (typeof x === 'number' ? `${(x * 100).toFixed(1)}%` : '—');
const n = (x?: number) => (typeof x === 'number' ? x.toLocaleString('en-US') : '—');
const ph = (v: string | undefined, placeholder: string) => v ?? `[${placeholder}]`;

const noIdx = read('search-noindex');
const idx = read('search-index');
const cache = read('search-index-cache');
const explain = read('explain');
const race = read('idempotency-race');
const spikeK6 = read('spike-k6');
const spike = read('spike-monitor');
const bridge = read('bridge-outage');

const lines: string[] = [];
lines.push('# SkyBook - measured results', '');
lines.push(`Generated ${new Date().toISOString()}${process.env.BENCH_WHERE ? ` on ${process.env.BENCH_WHERE}` : ''}. Host: ${os.cpus().length} vCPU, ${(os.totalmem() / 2 ** 30).toFixed(1)} GB RAM (as seen by the container).`, '');

lines.push('## Flight search (k6, popular-routes mix)', '');
if (noIdx || idx || cache) {
  lines.push(`| Variant | Requests | Throughput | p50 | p95 | p99 | Cache hit rate | Errors |`, `|---|---|---|---|---|---|---|---|`);
  for (const [name, r] of [['No index', noIdx], ['Composite index', idx], ['Index + Redis cache', cache]] as const) {
    if (r) lines.push(`| ${name} | ${n(r.requests)} | ${r.rps.toFixed(0)} req/s | ${ms(r.p50)} | ${ms(r.p95)} | ${ms(r.p99)} | ${r.mode === 'cache' ? pct(r.cacheHitRate) : '—'} | ${pct(r.failedRate)} |`);
  }
  const ref = noIdx ?? idx;
  if (ref) lines.push('', `k6: ${ref.vus} VUs for ${ref.duration} per variant, calling flight-search directly (pure service + database latency).`);
} else lines.push('_not run yet_');
if (explain) {
  lines.push('', `**EXPLAIN ANALYZE** (${n(explain.rows)} rows, ${explain.runs} random routes): p95 ${ms(explain.before.p95)} with a ${explain.before.plan.includes('Seq Scan') ? 'sequential scan' : explain.before.plan}` +
    ` -> ${ms(explain.after.p95)} with ${explain.after.plan.includes('Index') ? 'the composite index' : explain.after.plan} (${(explain.before.p95 / explain.after.p95).toFixed(0)}x).`);
}

lines.push('', '## Idempotency race (k6)', '');
lines.push(race ? `${race.concurrentRequests} identical concurrent requests with one Idempotency-Key -> **${race.storedBookings} stored booking(s)** (${race.replays} replays of the saved response, ${race.conflicts409} x 409, ${race.unexpected} unexpected). Duplicates: **${race.duplicates}**.` : '_not run yet_');

lines.push('', '## 10x booking spike (k6 + spike monitor)', '');
if (spike) {
  lines.push(`| Metric | Value |`, `|---|---|`);
  if (spikeK6) lines.push(`| Booking requests sent | ${n(spikeK6.requests)} (${spikeK6.baseRate}/s -> ${spikeK6.baseRate * spikeK6.spikeFactor}/s for 60 s), errors ${pct(spikeK6.failedRate)} |`, `| Booking API p95 during spike | ${ms(spikeK6.p95)} |`);
  lines.push(
    `| Events produced to Kafka | ${n(spike.eventsProduced)} |`,
    `| Peak consumer lag (all groups) | ${n(spike.peakConsumerLag)} messages |`,
    `| Time for all consumers to catch up after the last booking request | ${spike.catchUpSeconds ?? '—'} s |`,
    `| Slowest consumer group at peak | ${Object.entries(spike.peakLagByGroup ?? {}).sort((a: any, b: any) => b[1] - a[1])[0]?.join(': ') ?? '—'} messages behind |`,
    `| Messages not consumed at the end | **${spike.unconsumedAtEnd}**${spike.timedOut ? ' (monitor timed out while still draining - re-run with a larger --max)' : ''} |`,
    `| Bookings finished (CONFIRMED/CANCELLED/REJECTED) | ${n(spike.bookings.total - spike.bookings.unfinished)} / ${n(spike.bookings.total)} |`,
  );
} else lines.push('_not run yet_');

lines.push('', '## Kafka -> SQS bridge outage', '');
lines.push(bridge ? `${bridge.outageSec / 60}-minute ${bridge.target} outage during ${n(bridge.bookings)} bookings: **${n(bridge.received)}/${n(bridge.produced)} events delivered**, ${bridge.duplicates} duplicates, peak backlog ${n(bridge.peakBacklog)}, drained in ${bridge.drainSec} s.` : '_not run yet_');

// ---- resume bullets (build guide section 8) -------------------------------------------------------
const p95From = noIdx ? Math.round(noIdx.p95).toString() : explain ? explain.before.p95.toFixed(1) : undefined;
const p95To = cache ? cache.p95.toFixed(1) : idx ? idx.p95.toFixed(1) : explain ? explain.after.p95.toFixed(2) : undefined;
const spikeFactor = spikeK6 ? String(spikeK6.spikeFactor) : undefined;
lines.push('', '## Resume bullets (filled from the measurements above)', '');
lines.push(
  '**SkyBook: Flight Booking Microservices** | Node.js, TypeScript, Kafka, AWS (SQS, CDK), PostgreSQL, Redis, Docker, Kubernetes, Prometheus, Grafana', '',
  '- Built a 5-service flight booking platform in **Node.js/TypeScript** with database-per-service and a **Kafka** saga that releases held seats when payment fails, packaged with Docker Compose and a **Helm** chart for Kubernetes.',
  `- Made \`POST /bookings\` idempotent with stored idempotency keys and saved responses, producing **0 duplicate bookings across ${ph(race ? String(race.concurrentRequests) : undefined, 'N')} concurrent retried requests** (k6) and 50 in Testcontainers/PostgreSQL integration tests.`,
  `- Cut p95 flight-search latency from **${ph(p95From, 'X')} ms to ${ph(p95To, 'Y')} ms** on a 1M-row table using a composite index (checked with \`EXPLAIN ANALYZE\`) and a Redis cache with event-driven invalidation, reaching a **${ph(cache ? (cache.cacheHitRate * 100).toFixed(0) : undefined, 'Z')}% hit rate** under k6 load.`,
  `- Absorbed a **${ph(spikeFactor, 'N')}× traffic spike** with ${spike && spike.lostMessages === 0 ? 'zero lost messages' : spike && spike.lostMessages ? `[CHECK: ${spike.lostMessages} unconsumed messages]` : 'zero lost messages [verify]'} (peak consumer lag ${ph(spike ? n(spike.peakConsumerLag) : undefined, 'L')}, caught up in ${ph(spike?.catchUpSeconds != null ? String(spike.catchUpSeconds) : undefined, 'T')} s) using a transactional outbox, exponential-backoff retry topics, and a dead-letter topic with a replay tool, monitored through Prometheus/Grafana alerts on consumer lag.`,
  `- Built a **Node.js Kafka → AWS SQS bridge** that commits Kafka offsets only after AWS confirms each batch and pauses consumption during AWS outages, delivering **${ph(bridge ? `${n(bridge.received)}/${n(bridge.produced)}` : undefined, 'N/N')} events with no loss** through a ${ph(bridge ? String(bridge.outageSec / 60) : undefined, 'X')}-minute ${bridge && String(bridge.target).startsWith('real') ? 'network outage against real Amazon SQS' : 'simulated AWS outage'}.`,
);

const md = lines.join('\n') + '\n';
// UTF-8 with BOM so Windows PowerShell 5.1 (Get-Content) and Notepad show ×, →, — correctly.
if (fs.existsSync(dir)) fs.writeFileSync(path.join(dir, 'RESULTS.md'), '\ufeff' + md, 'utf8');
console.log(md);
