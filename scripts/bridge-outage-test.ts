/**
 * AWS-outage failure test for the Kafka -> SQS bridge (build guide 3.11), using the fake SQS.
 *   node dist/scripts/bridge-outage-test.js [--bookings 300] [--rate 10] [--outage 120]
 * Env: GATEWAY_URL, FAKE_SQS_URL, KAFKA_BROKERS, BRIDGE_METRICS_URL, DEMO_PASSWORD
 *
 * Creates bookings through the gateway at a steady rate, cuts "AWS" off for --outage seconds part-way
 * through, then waits for the backlog to drain and compares events produced to Kafka (booking.events +
 * inventory.events) with unique events received by SQS. Exits non-zero if any event is missing.
 *
 * --manual (real AWS): no fake SQS. You cut your internet for --outage seconds when prompted, and the
 * script counts deliveries by draining the real queue (ReceiveMessage + DeleteMessage, so use a TEST
 * queue). Needs SQS_QUEUE_URL, AWS_REGION and an AWS profile allowed to receive/delete (AWS_PROFILE).
 */
import { randomUUID } from 'crypto';
import fs from 'fs';
import path from 'path';
import { DeleteMessageBatchCommand, ReceiveMessageCommand, SQSClient } from '@aws-sdk/client-sqs';
import { Kafka, logLevel } from 'kafkajs';

const GATEWAY = process.env.GATEWAY_URL ?? 'http://localhost:8080';
const SQS = process.env.FAKE_SQS_URL ?? 'http://localhost:4566';
const BRIDGE = process.env.BRIDGE_METRICS_URL ?? 'http://localhost:3006';
const BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');
const TOPICS = ['booking.events', 'inventory.events'];

const arg = (n: string, d: number) => { const i = process.argv.indexOf(`--${n}`); return i > 0 ? Number(process.argv[i + 1]) : d; };
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const t = () => new Date().toISOString().slice(11, 19);

async function json(method: string, url: string, body?: unknown, headers: Record<string, string> = {}) {
  const r = await fetch(url, { method, headers: { 'content-type': 'application/json', ...headers }, body: body === undefined ? undefined : JSON.stringify(body) });
  return { status: r.status, body: (await r.json().catch(() => null)) as any };
}

async function main() {
  const bookings = arg('bookings', 300);
  const rate = arg('rate', 10);
  const outageSec = arg('outage', 120);

  const admin = new Kafka({ clientId: 'outage-test', brokers: BROKERS, logLevel: logLevel.NOTHING }).admin();
  await admin.connect();
  const produced = async () => {
    let n = 0;
    for (const topic of TOPICS) for (const p of await admin.fetchTopicOffsets(topic)) n += Number(p.high);
    return n;
  };
  const manual = process.argv.includes('--manual');
  const seenReal = new Set<string>();
  let receivedReal = 0;
  const sqs = manual ? new SQSClient({ region: process.env.AWS_REGION ?? 'us-east-2' }) : null;
  const queueUrl = process.env.SQS_QUEUE_URL ?? '';
  if (manual && !queueUrl) throw new Error('--manual needs SQS_QUEUE_URL');
  /** Drains the real queue, remembering unique eventIds. */
  async function drainReal(maxRounds = 50) {
    for (let i = 0; i < maxRounds; i++) {
      const r = await sqs!.send(new ReceiveMessageCommand({ QueueUrl: queueUrl, MaxNumberOfMessages: 10, WaitTimeSeconds: 1, MessageAttributeNames: ['eventId'] }));
      const msgs = r.Messages ?? [];
      if (!msgs.length) return;
      for (const m of msgs) { receivedReal++; seenReal.add(m.MessageAttributes?.eventId?.StringValue ?? m.MessageId!); }
      await sqs!.send(new DeleteMessageBatchCommand({ QueueUrl: queueUrl, Entries: msgs.map((m, j) => ({ Id: `d${j}`, ReceiptHandle: m.ReceiptHandle! })) }));
    }
  }
  const received = async (): Promise<{ unique: number; received: number; duplicates: number }> => {
    if (!manual) return (await json('GET', `${SQS}/stats`)).body;
    try { await drainReal(); } catch { /* offline during the outage */ }
    return { unique: seenReal.size, received: receivedReal, duplicates: receivedReal - seenReal.size };
  };
  const bridgePaused = async () => {
    try {
      const txt = await (await fetch(`${BRIDGE}/metrics`)).text();
      return /^bridge_paused\{[^}]*\} 1$/m.test(txt) || /^bridge_paused 1$/m.test(txt);
    } catch { return false; }
  };

  const login = await json('POST', `${GATEWAY}/auth/login`, { username: 'alice', password: process.env.DEMO_PASSWORD ?? 'skybook' });
  const auth = { authorization: `Bearer ${login.body.accessToken}` };
  const flights: string[] = [];
  for (let d = 1; d <= 20; d++) {
    const day = new Date(Date.now() + d * 86_400_000).toISOString().slice(0, 10);
    const r = await json('GET', `${GATEWAY}/flights/search?origin=YYZ&destination=YVR&date=${day}`);
    flights.push(...(r.body?.flights ?? []).map((f: any) => f.id));
  }
  if (!flights.length) throw new Error('no flights found - is the stack seeded?');

  // Make sure the bridge has caught up before we start, so the baseline is clean.
  if (manual) {
    console.log(`[${t()}] draining ${queueUrl} (messages are deleted - use a test queue)...`);
    for (let quiet = 0, i = 0; quiet < 5 && i < 120; i++) { const before = seenReal.size; await received(); quiet = seenReal.size === before ? quiet + 1 : 0; await sleep(1000); }
    seenReal.clear();
    receivedReal = 0;
  } else {
    for (let i = 0; i < 60 && (await received()).unique < (await produced()); i++) await sleep(1000);
  }
  const base = { produced: await produced(), unique: (await received()).unique, received: (await received()).received };
  console.log(`[${t()}] baseline: produced=${base.produced} sqsUnique=${base.unique}`);

  let peakBacklog = 0;
  let sawPause = false;
  let outageEnd = 0;
  const outageAt = Math.floor(bookings / 4);
  let created = 0;
  const started = Date.now();

  const sampler = setInterval(async () => {
    const backlog = (await produced()) - base.produced - ((await received()).unique - base.unique);
    peakBacklog = Math.max(peakBacklog, backlog);
    if (await bridgePaused()) sawPause = true;
  }, 1000);

  for (let i = 0; i < bookings; i++) {
    if (i === outageAt) {
      if (manual) console.log(`\n>>> [${t()}] DISCONNECT YOUR INTERNET NOW (Wi-Fi off / cable out) and reconnect in ${outageSec}s <<<\n`);
      else await json('POST', `${SQS}/admin/outage?seconds=${outageSec}`);
      outageEnd = Date.now() + outageSec * 1000;
      console.log(`[${t()}] AWS outage started for ${outageSec}s (after ${i} bookings)`);
    }
    const r = await json('POST', `${GATEWAY}/bookings`, {
      flightId: flights[i % flights.length], passengerName: 'Outage Test', passengerEmail: 'outage@example.com', paymentToken: 'tok_success',
    }, { ...auth, 'idempotency-key': randomUUID() });
    if (r.status === 201) created++;
    await sleep(1000 / rate);
  }
  console.log(`[${t()}] ${created}/${bookings} bookings accepted (201) in ${((Date.now() - started) / 1000).toFixed(0)}s`);

  if (Date.now() < outageEnd) {
    console.log(`[${t()}] waiting for the outage window to end...`);
    await sleep(outageEnd - Date.now());
  }
  console.log(manual ? `\n>>> [${t()}] RECONNECT YOUR INTERNET NOW <<<\n` : `[${t()}] AWS restored`);
  const restoredAt = Date.now();

  // Drain: wait until every produced event is in SQS and production has stopped (saga finished).
  let drainedAt = 0;
  let last = -1;
  for (let i = 0; i < 600; i++) {
    const p = (await produced()) - base.produced;
    const u = (await received()).unique - base.unique;
    if (p === u && p === last) { drainedAt = Date.now(); break; }
    last = p;
    await sleep(1000);
  }
  clearInterval(sampler);

  const p = (await produced()) - base.produced;
  const s = await received();
  const u = s.unique - base.unique;
  const dup = s.received - base.received - u;
  const drainSec = drainedAt ? ((drainedAt - restoredAt) / 1000).toFixed(1) : 'did not drain';
  console.log('\n| Metric | Value |\n|---|---|');
  console.log(`| Bookings accepted during test | ${created} |`);
  console.log(`| Simulated AWS outage | ${outageSec} s |`);
  console.log(`| Bridge paused (circuit breaker) | ${sawPause ? 'yes' : 'no'} |`);
  console.log(`| Peak backlog held in Kafka | ${peakBacklog} events |`);
  console.log(`| Time to drain after AWS restored | ${drainSec} s |`);
  console.log(`| Events produced to Kafka | ${p} |`);
  console.log(`| Unique events received in SQS | ${u} |`);
  console.log(`| Duplicate deliveries (at-least-once, deduped by eventId downstream) | ${dup} |`);
  const dir = process.env.RESULTS_DIR ?? '/results';
  if (fs.existsSync(dir)) {
    fs.writeFileSync(path.join(dir, 'bridge-outage.json'), JSON.stringify({
      label: 'bridge-outage', target: manual ? 'real AWS (internet cut)' : 'simulated AWS (fake SQS 503s)', bookings: created, outageSec,
      paused: sawPause, peakBacklog, drainSec, produced: p, received: u, duplicates: dup, finishedAt: new Date().toISOString(),
    }, null, 2));
  }
  console.log(`\n${p === u ? `PASS: ${u}/${p} events delivered, none lost` : `FAIL: ${p - u} event(s) missing`}`);
  await admin.disconnect();
  process.exit(p === u ? 0 : 1);
}

main().catch((err) => { console.error(err); process.exit(1); });
