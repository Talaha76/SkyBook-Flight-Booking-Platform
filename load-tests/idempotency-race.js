// k6 idempotency race: N VUs fire the SAME request with the SAME Idempotency-Key at the same moment.
// Expect exactly one stored booking (the others are replays of the saved response or 409s).
//   docker compose --profile tools run --rm k6 run -e VUS=100 /scripts/idempotency-race.js
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';
import { uuidv4 } from './lib.js';

const BASE = __ENV.GATEWAY_URL || 'http://gateway:8080';
const VUS = Number(__ENV.VUS || 100);
const fresh = new Counter('fresh_bookings');
const replays = new Counter('replays');
const conflicts = new Counter('conflicts_409');
const other = new Counter('unexpected_status');
const stored = new Counter('stored_bookings');

export const options = {
  scenarios: { race: { executor: 'per-vu-iterations', vus: VUS, iterations: 1 } },
};

function login() {
  return http.post(`${BASE}/auth/login`, JSON.stringify({ username: 'alice', password: __ENV.DEMO_PASSWORD || 'skybook' }), { headers: { 'Content-Type': 'application/json' } }).json('accessToken');
}

export function setup() {
  const day = new Date(Date.now() + 5 * 86400000).toISOString().slice(0, 10);
  const flight = http.get(`${BASE}/flights/search?origin=YYZ&destination=YVR&date=${day}`).json('flights')[0];
  return { token: login(), flightId: flight.id, key: uuidv4(), name: `Race ${uuidv4().slice(0, 8)}` };
}

export default function (data) {
  const res = http.post(
    `${BASE}/bookings`,
    JSON.stringify({ flightId: data.flightId, passengerName: data.name, passengerEmail: 'race@example.com', paymentToken: 'tok_success' }),
    { headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${data.token}`, 'Idempotency-Key': data.key } },
  );
  check(res, { '201 or 409': (r) => r.status === 201 || r.status === 409 });
  if (res.status === 409) conflicts.add(1);
  else if (res.status === 201 && res.headers['Idempotent-Replayed'] === 'true') replays.add(1);
  else if (res.status === 201) fresh.add(1);
  else other.add(1);
}

export function teardown(data) {
  const items = http.get(`${BASE}/bookings?limit=100`, { headers: { Authorization: `Bearer ${login()}` } }).json('items') || [];
  stored.add(items.filter((b) => b.passengerName === data.name).length);
}

export function handleSummary(data) {
  const v = (m) => (data.metrics[m] ? data.metrics[m].values.count : 0);
  const out = {
    label: 'idempotency-race',
    concurrentRequests: VUS,
    freshBookings: v('fresh_bookings'),
    replays: v('replays'),
    conflicts409: v('conflicts_409'),
    unexpected: v('unexpected_status'),
    storedBookings: v('stored_bookings'),
    finishedAt: new Date().toISOString(),
  };
  out.duplicates = Math.max(0, out.storedBookings - 1);
  return {
    stdout: `idempotency race: ${VUS} concurrent identical requests -> ${out.storedBookings} stored booking(s), ${out.replays} replays, ${out.conflicts409} x 409, duplicates ${out.duplicates}\n`,
    '/results/idempotency-race.json': JSON.stringify(out, null, 2),
  };
}
