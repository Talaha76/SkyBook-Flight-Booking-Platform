package com.skybook.inventory.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.stereotype.Component;

/**
 * Micrometer meters registered under names that export to exactly the Prometheus names the Node services
 * use (prom-client), so the existing Grafana dashboard and alerts keep working. The common tag
 * service="inventory" comes from management.metrics.tags.service.
 *   http.request.duration       -> http_request_duration_seconds_bucket/_count/_sum
 *   kafka.messages.consumed     -> kafka_messages_consumed_total
 *   kafka.consumer.lag          -> kafka_consumer_lag
 *   outbox.pending.total        -> outbox_pending_total
 *   outbox.published            -> outbox_published_total
 */
@Component
public class SkybookMetrics {
    private static final Duration[] HTTP_BUCKETS = {
            Duration.ofMillis(5), Duration.ofMillis(10), Duration.ofMillis(25), Duration.ofMillis(50), Duration.ofMillis(100),
            Duration.ofMillis(250), Duration.ofMillis(500), Duration.ofSeconds(1), Duration.ofMillis(2500), Duration.ofSeconds(5)};

    private final MeterRegistry registry;
    private final AtomicLong outboxPending = new AtomicLong();
    private final Map<String, AtomicLong> lag = new ConcurrentHashMap<>();

    public SkybookMetrics(MeterRegistry registry) {
        this.registry = registry;
        Gauge.builder("outbox.pending.total", outboxPending, AtomicLong::get)
                .description("Outbox rows not yet published to Kafka").register(registry);
    }

    public void httpRequest(String method, String route, int status, long nanos) {
        Timer.builder("http.request.duration")
                .description("HTTP request duration in seconds")
                .tags("method", method, "route", route, "status", String.valueOf(status))
                .serviceLevelObjectives(HTTP_BUCKETS)
                .register(registry)
                .record(Duration.ofNanos(nanos));
    }

    public void consumed(String topic, String type, String result) {
        Counter.builder("kafka.messages.consumed").description("Kafka messages consumed")
                .tags("topic", topic, "type", type, "result", result).register(registry).increment();
    }

    public void consumerLag(String group, String topic, int partition, long value) {
        String key = group + "|" + topic + "|" + partition;
        lag.computeIfAbsent(key, k -> {
            AtomicLong holder = new AtomicLong();
            Gauge.builder("kafka.consumer.lag", holder, AtomicLong::get)
                    .description("Messages behind the partition end offset")
                    .tags("group", group, "topic", topic, "partition", String.valueOf(partition))
                    .register(registry);
            return holder;
        }).set(Math.max(0, value));
    }

    public void outboxPending(long value) {
        outboxPending.set(value);
    }

    public void outboxPublished(String topic, int count) {
        Counter.builder("outbox.published").description("Outbox rows published to Kafka")
                .tags("topic", topic).register(registry).increment(count);
    }
}
