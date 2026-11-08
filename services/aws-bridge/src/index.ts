import { SQSClient } from '@aws-sdk/client-sqs';
import {
  TOPICS,
  createApp,
  createKafka,
  createLogger,
  ensureTopics,
  env,
  envInt,
  finalizeApp,
  kafkaConsumerLag,
  onShutdown,
  parseEnvelope,
  promClient,
  registry,
  runMain,
  setServiceLabel,
} from '../../../packages/shared/src';
import { SqsRecord, toSqsRecord } from './contracts';
import { ForwardError, SQS_BATCH_LIMIT, forwardBatch } from './forwarder';

const SERVICE = 'aws-bridge';
const log = createLogger(SERVICE);
setServiceLabel(SERVICE);

const forwarded = new promClient.Counter({ name: 'bridge_forwarded_total', help: 'Events confirmed by SQS', labelNames: ['contract'], registers: [registry] });
const failedEntries = new promClient.Counter({ name: 'bridge_failed_entries_total', help: 'Entries SQS returned in Failed[] (retried)', registers: [registry] });
const rejectedEntries = new promClient.Counter({ name: 'bridge_rejected_total', help: 'Entries rejected as sender fault (not retryable)', registers: [registry] });
const paused = new promClient.Gauge({ name: 'bridge_paused', help: '1 while the circuit breaker has paused consumption', registers: [registry] });
const e2eLag = new promClient.Histogram({
  name: 'bridge_end_to_end_seconds',
  help: 'Event occurredAt -> SQS acknowledgement',
  buckets: [0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300],
  registers: [registry],
});

runMain(log, async () => {
  const queueUrl = env('SQS_QUEUE_URL');
  const endpoint = process.env.SQS_ENDPOINT || undefined; // set only for the local fake / LocalStack
  // Credentials come from the default AWS chain: env vars, ~/.aws profile (AWS_PROFILE), or an
  // instance role / IRSA on EC2/EKS. Never hard-coded.
  const sqs = new SQSClient({ region: env('AWS_REGION', 'us-east-2'), endpoint, maxAttempts: 2 });
  const topics = env('BRIDGE_TOPICS', `${TOPICS.booking},${TOPICS.inventory}`).split(',').map((t) => t.trim());
  const maxCooldownMs = envInt('BRIDGE_MAX_COOLDOWN_MS', 60_000);
  let cooldownMs = envInt('BRIDGE_MIN_COOLDOWN_MS', 1_000);
  let consecutiveFailures = 0;
  // Paused partitions and their scheduled probe. When ANY partition gets through to AWS again,
  // every paused partition is resumed at once instead of waiting out its own cooldown.
  const pausedPartitions = new Map<string, { resume: () => void; timer: NodeJS.Timeout }>();
  const resumeAll = () => {
    for (const [, p] of pausedPartitions) { clearTimeout(p.timer); p.resume(); }
    pausedPartitions.clear();
  };

  const kafka = createKafka(SERVICE, env('KAFKA_BROKERS', 'localhost:9092'), log);
  await ensureTopics(kafka, log);
  const consumer = kafka.consumer({ groupId: env('BRIDGE_GROUP_ID', 'aws-bridge'), sessionTimeout: 30_000 });
  await consumer.connect();
  for (const topic of topics) await consumer.subscribe({ topic, fromBeginning: true });

  await consumer.run({
    eachBatchAutoResolve: false,
    autoCommitInterval: 1000,
    // Offsets are resolved only after SQS confirms a chunk, so (auto)commit can never move past an unacked event.
    eachBatch: async ({ batch, resolveOffset, commitOffsetsIfNecessary, heartbeat, isRunning, isStale, pause }) => {
      const msgs = batch.messages;
      let i = 0;
      while (i < msgs.length && isRunning() && !isStale()) {
        // Build a chunk of up to 10 consecutive messages; malformed messages are skipped in place.
        const chunk: { offset: string; record: SqsRecord }[] = [];
        let lastOffset = msgs[i].offset;
        while (i < msgs.length && chunk.length < SQS_BATCH_LIMIT) {
          const event = parseEnvelope(msgs[i]);
          if (event) chunk.push({ offset: msgs[i].offset, record: toSqsRecord(event) });
          else log.warn({ topic: batch.topic, offset: msgs[i].offset }, 'skipping malformed message');
          lastOffset = msgs[i].offset;
          i++;
        }
        const firstOffset = chunk[0]?.offset ?? lastOffset;
        try {
          if (chunk.length) {
            const r = await forwardBatch(sqs, queueUrl, chunk.map((c) => c.record), { onFailedEntries: (n) => failedEntries.inc(n) });
            for (const c of chunk) {
              if (r.rejected.some((x) => x.eventId === c.record.eventId)) continue;
              forwarded.inc({ contract: c.record.contract });
              e2eLag.observe(Math.max(0, (Date.now() - Date.parse(c.record.occurredAt)) / 1000));
            }
            for (const x of r.rejected) {
              rejectedEntries.inc();
              log.error({ eventId: x.eventId, code: x.code, message: x.message }, 'SQS rejected entry permanently (sender fault)');
            }
          }
          resolveOffset(lastOffset);
          await commitOffsetsIfNecessary();
          await heartbeat();
          if (consecutiveFailures > 0) log.info({ after: consecutiveFailures, resumed: pausedPartitions.size }, 'AWS reachable again, circuit closed');
          consecutiveFailures = 0;
          resumeAll();
          cooldownMs = envInt('BRIDGE_MIN_COOLDOWN_MS', 1_000);
          paused.set(0);
          kafkaConsumerLag.set({ group: 'aws-bridge', topic: batch.topic, partition: String(batch.partition) }, Math.max(0, Number(batch.highWatermark) - Number(lastOffset) - 1));
        } catch (err) {
          if (!(err instanceof ForwardError)) throw err;
          // Circuit breaker: stop pulling from this partition, let Kafka hold the backlog, retry later.
          consecutiveFailures++;
          paused.set(1);
          const resume = pause();
          consumer.seek({ topic: batch.topic, partition: batch.partition, offset: firstOffset });
          log.warn({ err: err.message, topic: batch.topic, partition: batch.partition, cooldownMs }, 'AWS unavailable, pausing partition');
          const key = `${batch.topic}:${batch.partition}`;
          clearTimeout(pausedPartitions.get(key)?.timer);
          const timer = setTimeout(() => { pausedPartitions.delete(key); resume(); }, cooldownMs);
          pausedPartitions.set(key, { resume, timer });
          cooldownMs = Math.min(cooldownMs * 2, maxCooldownMs);
          return;
        }
      }
    },
  });

  const app = createApp({ log });
  finalizeApp(app);
  const port = envInt('PORT', 3006);
  const server = app.listen(port, () => log.info({ port, queueUrl, endpoint: endpoint ?? 'aws', topics }, 'aws-bridge listening'));
  onShutdown(log, [() => consumer.disconnect(), () => server.close()]);
});
