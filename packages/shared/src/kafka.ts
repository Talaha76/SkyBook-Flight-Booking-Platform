import { Kafka, logLevel, Partitioners, Producer } from 'kafkajs';
import { ALL_TOPICS } from './events';
import { Logger } from './logger';

process.env.KAFKAJS_NO_PARTITIONER_WARNING = '1';

export function createKafka(clientId: string, brokers: string, log: Logger): Kafka {
  return new Kafka({
    clientId,
    brokers: brokers.split(',').map((b) => b.trim()),
    retry: { initialRetryTime: 300, retries: 10 },
    logLevel: logLevel.WARN,
    logCreator: () => ({ namespace, level, log: entry }) => {
      const { message, ...extra } = entry;
      const fn = level <= logLevel.ERROR ? 'error' : level === logLevel.WARN ? 'warn' : 'debug';
      log[fn]({ kafka: namespace, ...extra }, message);
    },
  });
}

export async function createProducer(kafka: Kafka): Promise<Producer> {
  const producer = kafka.producer({
    createPartitioner: Partitioners.DefaultPartitioner,
    allowAutoTopicCreation: false,
    idempotent: false,
  });
  await producer.connect();
  return producer;
}

/** Creates all SkyBook topics if they do not exist (idempotent, safe on every startup). */
export async function ensureTopics(kafka: Kafka, log: Logger, partitions = Number(process.env.KAFKA_PARTITIONS ?? 3)) {
  const admin = kafka.admin();
  await admin.connect();
  try {
    const existing = new Set(await admin.listTopics());
    const missing = ALL_TOPICS.filter((t) => !existing.has(t));
    if (missing.length) {
      await admin.createTopics({
        waitForLeaders: true,
        topics: missing.map((topic) => ({ topic, numPartitions: partitions, replicationFactor: 1 })),
      });
      log.info({ topics: missing }, 'created kafka topics');
    }
  } catch (err) {
    // Another service may have created them concurrently; that is fine.
    log.warn({ err: (err as Error).message }, 'ensureTopics warning');
  } finally {
    await admin.disconnect();
  }
}
