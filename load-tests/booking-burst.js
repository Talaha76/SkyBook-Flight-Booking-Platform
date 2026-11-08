// k6 booking spike: steady 5 bookings/s, then 10x (50/s) for 60 s, then back to 5/s.
// Run together with the spike monitor (scripts/run-benchmarks.*) to measure consumer lag and lost messages.
//   docker compose --profile tools run --rm k6 run /scripts/booking-burst.js
import http from 'k6/http';
import { check } from 'k6';
import { uuidv4 } from './lib.js';

const BASE = __ENV.GATEWAY_URL || 'http://gateway:8080';
const BASE_RATE = Number(__ENV.BASE_RATE || 5);
const SPIKE = Number(__ENV.SPIKE_FACTOR || 10);

export const options = {
  scenarios: {
    burst: {
      executor: 'ramping-arrival-rate',
      startRate: BASE_RATE,
      timeUnit: '1s',
      preAllocatedVUs: 50,
      maxVUs: 300,
      stages: [
        { target: BASE_RATE, duration: '30s' },
        { target: BASE_RATE * SPIKE, duration: '5s' },
        { target: BASE_RATE * SPIKE, duration: '60s' },
        { target: BASE_RATE, duration: '5s' },
        { target: BASE_RATE, duration: '30s' },
      ],
    },
  },
  thresholds: { http_req_failed: ['rate<0.01'] },
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
};

export function setup() {
  const login = http.post(`${BASE}/auth/login`, JSON.stringify({ username: 'alice', password: __ENV.DEMO_PASSWORD || 'skybook' }), { headers: { 'Content-Type': 'application/json' } });
  const token = login.json('accessToken');
  const flights = [];
  for (let i = 1; i <= 30; i++) {
    const day = new Date(Date.now() + i * 86400000).toISOString().slice(0, 10);
    const r = http.get(`${BASE}/flights/search?origin=YYZ&destination=YVR&date=${day}`);
    for (const f of r.json('flights') || []) flights.push(f.id);
  }
  return { token, flights };
}

export default function (data) {
  const flightId = data.flights[Math.floor(Math.random() * data.flights.length)];
  const res = http.post(
    `${BASE}/bookings`,
    JSON.stringify({ flightId, passengerName: 'Spike Test', passengerEmail: 'spike@example.com', paymentToken: 'tok_random' }),
    { headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${data.token}`, 'Idempotency-Key': uuidv4() }, tags: { name: 'create_booking' } },
  );
  check(res, { 'status 201': (r) => r.status === 201 });
}

export function handleSummary(data) {
  const d = data.metrics.http_req_duration.values;
  const out = {
    label: 'spike-k6',
    baseRate: BASE_RATE,
    spikeFactor: SPIKE,
    requests: data.metrics.http_reqs.values.count,
    failedRate: data.metrics.http_req_failed.values.rate,
    p50: d['p(50)'], p95: d['p(95)'], p99: d['p(99)'],
    finishedAt: new Date().toISOString(),
  };
  return {
    stdout: `spike: ${out.requests} booking requests (${SPIKE}x spike to ${BASE_RATE * SPIKE}/s), p95 ${out.p95.toFixed(1)} ms, errors ${(out.failedRate * 100).toFixed(2)}%\n`,
    '/results/spike-k6.json': JSON.stringify(out, null, 2),
  };
}
