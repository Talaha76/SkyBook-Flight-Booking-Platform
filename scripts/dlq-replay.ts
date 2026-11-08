/**
 * Re-publishes dead-lettered notifications once the underlying bug is fixed.
 * Usage: npm run dlq:replay -- --since 1h [--dry-run]
 * Messages go back to notification.retry.1m with attempt=1 and no delay.
 */
import { randomUUID } from 'crypto';
import { TOPICS, createKafka, createLogger, createProducer, env, headersToRecord } from '../packages/shared/src';

const log = createLogger('dlq-replay');

export function parseSince(s: string): number {
  const m = /^(\d+)([smhd])$/.exec(s);
  if (!m) throw new Error('--since must look like 30m, 1h, 2d');
  return Number(m[1]) * { s: 1_000, m: 60_000, h: 3_600_000, d: 86_400_000 }[m[2] as 's' | 'm' | 'h' | 'd'];
}

async function main() {
  const args = process.argv.slice(2);
  const sinceArg = args[args.indexOf('--since') + 1] ?? '1h';
  const dryRun = args.includes('--dry-run');
  const since = Date.now() - parseSince(args.includes('--since') ? sinceArg : '1h');

  const kafka = createKafka('dlq-replay', env('KAFKA_BROKERS', 'localhost:9092'), log);
  const admin = kafka.admin();
  await admin.connect();
  const ends = await admin.fetchTopicOffsets(TOPICS.notificationDlq);
  await admin.disconnect();
  const remaining = new Map(ends.filter((p) => Number(p.high) > Number(p.low)).map((p) => [p.partition, Number(p.high)]));
  if (remaining.size === 0) {
    console.log('DLQ is empty.');
    return;
  }

  const producer = dryRun ? null : await createProducer(kafka);
  const consumer = kafka.consumer({ groupId: `dlq-replay-${randomUUID()}` });
  await consumer.connect();
  await consumer.subscribe({ topic: TOPICS.notificationDlq, fromBeginning: true });
  let selected = 0;

  await new Promise<void>((resolve, reject) => {
    consumer.run({
      eachMessage: async ({ partition, message }) => {
        const headers = headersToRecord(message.headers);
        if (Number(message.timestamp) >= since) {
          selected++;
          console.log(`${dryRun ? '[dry-run] would replay' : 'replaying'} offset=${message.offset} partition=${partition} error="${headers['x-error'] ?? ''}"`);
          if (producer) {
            await producer.send({
              topic: TOPICS.notificationRetry1,
              messages: [{
                key: message.key,
                value: message.value,
                headers: { ...headers, 'x-attempt': '1', 'x-not-before': String(Date.now()), 'x-message-id': randomUUID(), 'x-replayed-from': 'notification.dlq' },
              }],
            });
          }
        }
        if (Number(message.offset) + 1 >= (remaining.get(partition) ?? 0)) remaining.delete(partition);
        if (remaining.size === 0) resolve();
      },
    }).catch(reject);
  });

  await consumer.disconnect();
  await producer?.disconnect();
  console.log(`${dryRun ? 'Would replay' : 'Replayed'} ${selected} message(s).`);
}

if (require.main === module) {
  main().catch((err) => { log.fatal({ err }, 'replay failed'); process.exit(1); });
}
