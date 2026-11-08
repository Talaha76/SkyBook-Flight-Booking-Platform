/**
 * End-to-end smoke test against a running stack (through the gateway only).
 * Usage: npm run e2e     (GATEWAY_URL defaults to http://localhost:8080)
 * Exits non-zero on any failed check.
 */
import { randomUUID } from 'crypto';

const BASE = process.env.GATEWAY_URL ?? 'http://localhost:8080';
const PASSWORD = process.env.DEMO_PASSWORD ?? 'skybook';
let failures = 0;

function check(name: string, ok: boolean, detail?: unknown) {
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${name}${!ok && detail !== undefined ? ` -> ${JSON.stringify(detail)}` : ''}`);
  if (!ok) failures++;
}

async function call(method: string, path: string, opts: { token?: string; body?: unknown; headers?: Record<string, string> } = {}) {
  const res = await fetch(`${BASE}${path}`, {
    method,
    headers: {
      'content-type': 'application/json',
      ...(opts.token ? { authorization: `Bearer ${opts.token}` } : {}),
      ...(opts.headers ?? {}),
    },
    body: opts.body === undefined ? undefined : JSON.stringify(opts.body),
  });
  const text = await res.text();
  let json: any = text;
  try { json = JSON.parse(text); } catch { /* not JSON */ }
  return { status: res.status, headers: res.headers, body: json };
}

async function loginAs(username: string) {
  const r = await call('POST', '/auth/login', { body: { username, password: PASSWORD } });
  if (r.status !== 200) throw new Error(`login ${username} failed: ${r.status} ${JSON.stringify(r.body)}`);
  return r.body.accessToken as string;
}

async function waitForStatus(token: string, id: string, statuses: string[], timeoutMs = 30_000) {
  const until = Date.now() + timeoutMs;
  let last: any;
  while (Date.now() < until) {
    last = (await call('GET', `/bookings/${id}`, { token })).body;
    if (statuses.includes(last?.status)) return last;
    await new Promise((r) => setTimeout(r, 300));
  }
  return last;
}

async function waitForGateway() {
  for (let i = 0; i < 60; i++) {
    try {
      const r = await fetch(`${BASE}/health`);
      if (r.ok) return;
    } catch { /* not up yet */ }
    await new Promise((r) => setTimeout(r, 2000));
  }
  throw new Error(`gateway at ${BASE} did not become healthy`);
}

async function main() {
  console.log(`SkyBook E2E against ${BASE}\n`);
  await waitForGateway();
  const alice = await loginAs('alice');
  const bob = await loginAs('bob');
  const agent = await loginAs('agent');
  check('login returns JWTs', Boolean(alice && bob && agent));

  // 1. Search
  const date = new Date(Date.now() + 2 * 86_400_000).toISOString().slice(0, 10);
  let search: Awaited<ReturnType<typeof call>> | undefined;
  for (let i = 0; i < 30; i++) {
    search = await call('GET', `/flights/search?origin=YYZ&destination=YVR&date=${date}`);
    if (search.status === 200 && search.body.count > 0) break;
    await new Promise((r) => setTimeout(r, 2000)); // seed may still be running
  }
  check('search YYZ->YVR returns flights', search!.status === 200 && search!.body.count > 0, search!.body);
  const flight = search!.body.flights.find((f: any) => f.flightNumber === 'SB100') ?? search!.body.flights[0];
  const search2 = await call('GET', `/flights/search?origin=YYZ&destination=YVR&date=${date}`);
  check('second identical search is served from Redis (X-Cache: HIT)', search2.headers.get('x-cache') === 'HIT', search2.headers.get('x-cache'));
  check('invalid search is rejected with 400', (await call('GET', '/flights/search?origin=Y&destination=YVR&date=x')).status === 400);

  // 2. Auth
  check('booking without token is 401', (await call('POST', '/bookings', { body: {} })).status === 401);
  check('customer cannot read inventory (403)', (await call('GET', `/inventory/${flight.id}`, { token: alice })).status === 403);
  const invBefore = (await call('GET', `/inventory/${flight.id}`, { token: agent })).body;
  check('agent can read inventory', typeof invBefore.available === 'number', invBefore);

  // 3. Successful booking + idempotent replay
  const key = randomUUID();
  const body = { flightId: flight.id, passengerName: 'Alice Martin', passengerEmail: 'alice@example.com', paymentToken: 'tok_success' };
  const created = await call('POST', '/bookings', { token: alice, body, headers: { 'Idempotency-Key': key } });
  check('POST /bookings returns 201 PENDING', created.status === 201 && created.body.status === 'PENDING', created.body);
  const replay = await call('POST', '/bookings', { token: alice, body, headers: { 'Idempotency-Key': key } });
  check('same Idempotency-Key replays the saved response', replay.status === 201 && replay.body.id === created.body.id && replay.headers.get('idempotent-replayed') === 'true', replay.body);
  const mismatch = await call('POST', '/bookings', { token: alice, body: { ...body, passengerName: 'Someone Else' }, headers: { 'Idempotency-Key': key } });
  check('same key with a different body is 422', mismatch.status === 422, mismatch.body);
  check('missing Idempotency-Key is 400', (await call('POST', '/bookings', { token: alice, body })).status === 400);

  const confirmed = await waitForStatus(alice, created.body.id, ['CONFIRMED', 'CANCELLED', 'REJECTED']);
  check('saga confirms booking (PENDING -> SEAT_HELD -> CONFIRMED)', confirmed?.status === 'CONFIRMED', confirmed);
  check('confirmed booking has amount from inventory', confirmed?.amountCents === flight.priceCents, confirmed);
  const invAfter = (await call('GET', `/inventory/${flight.id}`, { token: agent })).body;
  check('inventory decremented by exactly 1', invAfter.available === invBefore.available - 1, { before: invBefore.available, after: invAfter.available });
  check('customer cannot read another customer booking (403)', (await call('GET', `/bookings/${created.body.id}`, { token: bob })).status === 403);

  // 4. Payment failure -> compensation
  const failed = await call('POST', '/bookings', {
    token: bob,
    body: { flightId: flight.id, passengerName: 'Bob Chen', passengerEmail: 'bob@example.com', paymentToken: 'tok_fail' },
    headers: { 'Idempotency-Key': randomUUID() },
  });
  check('booking with failing card accepted (201)', failed.status === 201, failed.body);
  const cancelled = await waitForStatus(bob, failed.body.id, ['CANCELLED', 'CONFIRMED', 'REJECTED']);
  check('payment failure cancels booking', cancelled?.status === 'CANCELLED' && cancelled?.failureReason === 'CARD_DECLINED', cancelled);
  let invComp: any;
  for (let i = 0; i < 20; i++) {
    invComp = (await call('GET', `/inventory/${flight.id}`, { token: agent })).body;
    if (invComp.holds?.RELEASED >= 1 && invComp.available === invAfter.available) break;
    await new Promise((r) => setTimeout(r, 300));
  }
  check('compensation released the seat (availability restored)', invComp.available === invAfter.available, invComp);
  const pay = await call('GET', `/payments/${failed.body.id}`, { token: agent });
  check('payment recorded as FAILED', pay.body.status === 'FAILED', pay.body);

  // 5. Unknown flight -> rejected
  const rejected = await call('POST', '/bookings', {
    token: alice,
    body: { flightId: randomUUID(), passengerName: 'Alice Martin', passengerEmail: 'alice@example.com', paymentToken: 'tok_success' },
    headers: { 'Idempotency-Key': randomUUID() },
  });
  const rej = await waitForStatus(alice, rejected.body.id, ['REJECTED', 'CANCELLED', 'CONFIRMED']);
  check('booking for unknown flight is REJECTED', rej?.status === 'REJECTED' && rej?.failureReason === 'FLIGHT_NOT_FOUND', rej);

  // 6. Concurrency: 20 parallel requests with the same key -> exactly one booking
  const raceKey = randomUUID();
  const raceBody = { flightId: flight.id, passengerName: 'Race Test', passengerEmail: 'race@example.com', paymentToken: 'tok_success' };
  const race = await Promise.all(Array.from({ length: 20 }, () => call('POST', '/bookings', { token: alice, body: raceBody, headers: { 'Idempotency-Key': raceKey } })));
  const ids = new Set(race.filter((r) => r.status === 201).map((r) => r.body.id));
  check('20 concurrent identical requests -> 1 booking id (others replay or 409)', ids.size === 1 && race.every((r) => r.status === 201 || r.status === 409), race.map((r) => r.status));

  // 7. Search read model reflects availability and the cache was invalidated
  let fresh: any;
  for (let i = 0; i < 20; i++) {
    fresh = (await call('GET', `/flights/${flight.id}`)).body;
    if (fresh.seatsAvailable === invComp.available - 1 || fresh.seatsAvailable === invComp.available) break;
    await new Promise((r) => setTimeout(r, 300));
  }
  check('flight-search read model tracks inventory via inventory.events', fresh.seatsAvailable <= invBefore.available - 1, { search: fresh.seatsAvailable, inventory: invComp.available });

  // 8. Notifications
  let notes: any;
  for (let i = 0; i < 20; i++) {
    notes = (await call('GET', `/notifications?bookingId=${created.body.id}`, { token: agent })).body;
    if (notes.items?.length) break;
    await new Promise((r) => setTimeout(r, 300));
  }
  check('confirmation notification sent', notes.items?.[0]?.status === 'SENT', notes);

  console.log(`\n${failures === 0 ? 'ALL CHECKS PASSED' : `${failures} CHECK(S) FAILED`}`);
  process.exit(failures === 0 ? 0 : 1);
}

main().catch((err) => { console.error('E2E crashed:', err); process.exit(1); });
