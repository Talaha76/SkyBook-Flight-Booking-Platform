// k6 search load test with a skewed "popular routes" mix.
//   docker compose --profile tools run --rm k6 run -e MODE=nocache -e LABEL=search-index /scripts/search.js
// MODE=nocache sends x-cache-bypass (honoured only on internal calls) so the database path is measured;
// MODE=cache uses the Redis cache. Default target is flight-search directly (pure service latency);
// set TARGET=gateway to go through the API gateway instead.
import http from 'k6/http';
import { check } from 'k6';
import { Rate } from 'k6/metrics';

const MODE = __ENV.MODE || 'cache';
const TARGET = __ENV.TARGET || 'service';
const BASE = TARGET === 'gateway' ? (__ENV.GATEWAY_URL || 'http://gateway:8080') : (__ENV.SEARCH_URL || 'http://flight-search:3001');
const HEADERS = TARGET === 'gateway' ? {} : { 'x-internal-token': __ENV.INTERNAL_TOKEN || 'dev-internal-token' };
if (MODE === 'nocache') HEADERS['x-cache-bypass'] = '1';
const CODES = ['YYZ', 'YVR', 'YUL', 'YYC', 'YOW', 'YEG', 'YWG', 'YHZ', 'YQB', 'YYJ', 'JFK', 'LGA', 'BOS', 'ORD', 'LAX', 'SFO', 'SEA', 'MIA', 'ATL', 'DFW'];
const cacheHit = new Rate('cache_hit');

export const options = {
  scenarios: { search: { executor: 'constant-vus', vus: Number(__ENV.VUS || 20), duration: __ENV.DURATION || '60s' } },
  thresholds: { http_req_failed: ['rate<0.01'] },
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
};

// Popular routes are picked far more often (squared random), like real traffic.
function skewed(n) { return Math.floor(Math.pow(Math.random(), 2) * n); }

export default function () {
  const o = skewed(CODES.length);
  let d = skewed(CODES.length);
  if (d === o) d = (d + 1) % CODES.length;
  const day = new Date(Date.now() + (1 + skewed(30)) * 86400000).toISOString().slice(0, 10);
  const res = http.get(`${BASE}/flights/search?origin=${CODES[o]}&destination=${CODES[d]}&date=${day}`, { headers: HEADERS, tags: { name: 'search' } });
  check(res, { 'status 200': (r) => r.status === 200 });
  cacheHit.add(res.headers['X-Cache'] === 'HIT');
}

export function handleSummary(data) {
  const d = data.metrics.http_req_duration.values;
  const out = {
    label: __ENV.LABEL || `search-${MODE}`,
    mode: MODE,
    target: TARGET,
    vus: Number(__ENV.VUS || 20),
    duration: __ENV.DURATION || '60s',
    requests: data.metrics.http_reqs.values.count,
    rps: data.metrics.http_reqs.values.rate,
    failedRate: data.metrics.http_req_failed.values.rate,
    p50: d['p(50)'], p95: d['p(95)'], p99: d['p(99)'], avg: d.avg,
    cacheHitRate: data.metrics.cache_hit ? data.metrics.cache_hit.values.rate : 0,
    finishedAt: new Date().toISOString(),
  };
  const line = `${out.label}: ${out.requests} req, ${out.rps.toFixed(0)} req/s, p50 ${out.p50.toFixed(2)} ms, p95 ${out.p95.toFixed(2)} ms, p99 ${out.p99.toFixed(2)} ms, hit rate ${(out.cacheHitRate * 100).toFixed(1)}%, errors ${(out.failedRate * 100).toFixed(2)}%\n`;
  return { stdout: line, [`/results/${out.label}.json`]: JSON.stringify(out, null, 2) };
}
