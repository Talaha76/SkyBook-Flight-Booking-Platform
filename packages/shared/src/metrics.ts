import client from 'prom-client';

export const registry = new client.Registry();
client.collectDefaultMetrics({ register: registry });

export function setServiceLabel(service: string) {
  registry.setDefaultLabels({ service });
}

export const httpDuration = new client.Histogram({
  name: 'http_request_duration_seconds',
  help: 'HTTP request duration in seconds',
  labelNames: ['method', 'route', 'status'],
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5],
  registers: [registry],
});

export const kafkaConsumed = new client.Counter({
  name: 'kafka_messages_consumed_total',
  help: 'Kafka messages consumed',
  labelNames: ['topic', 'type', 'result'],
  registers: [registry],
});

export const kafkaConsumerLag = new client.Gauge({
  name: 'kafka_consumer_lag',
  help: 'Messages behind the partition high watermark (as seen by the last fetched batch)',
  labelNames: ['group', 'topic', 'partition'],
  registers: [registry],
});

export const outboxPending = new client.Gauge({
  name: 'outbox_pending_total',
  help: 'Outbox rows not yet published to Kafka',
  registers: [registry],
});

export const outboxPublished = new client.Counter({
  name: 'outbox_published_total',
  help: 'Outbox rows published to Kafka',
  labelNames: ['topic'],
  registers: [registry],
});

export { client as promClient };
