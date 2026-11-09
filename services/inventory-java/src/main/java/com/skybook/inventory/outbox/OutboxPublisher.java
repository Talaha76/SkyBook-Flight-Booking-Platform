package com.skybook.inventory.outbox;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.skybook.inventory.metrics.SkybookMetrics;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Publishes pending outbox rows to Kafka (same algorithm as startOutboxPublisher in the shared Node package):
 * SELECT ... FOR UPDATE SKIP LOCKED LIMIT 100, send with acks=all, mark PUBLISHED, all in one DB transaction.
 * On a Kafka failure the rows get attempt_count+1 and a later next_attempt_at, and the whole loop backs off,
 * which keeps per-flight ordering intact. Delivery is at-least-once; consumers deduplicate on eventId.
 */
@Component
public class OutboxPublisher {
    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);
    private static final int BATCH = 100;

    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final SkybookMetrics metrics;
    private long backoffUntil = 0;
    private int failures = 0;
    private long lastGauge = 0;

    public OutboxPublisher(JdbcTemplate jdbc, TransactionTemplate tx, KafkaTemplate<String, String> kafka, ObjectMapper mapper, SkybookMetrics metrics) {
        this.jdbc = jdbc;
        this.tx = tx;
        this.kafka = kafka;
        this.mapper = mapper;
        this.metrics = metrics;
    }

    @Scheduled(fixedDelayString = "${skybook.outbox-poll-ms:200}")
    public void tick() {
        long now = System.currentTimeMillis();
        if (now < backoffUntil) return;
        try {
            int n;
            do {
                n = publishOnce();
            } while (n == BATCH); // keep draining a backlog without waiting for the next tick
            failures = 0;
        } catch (Exception e) {
            failures++;
            long backoff = Math.min(1000L * (1L << Math.min(failures, 5)), 30_000L);
            backoffUntil = System.currentTimeMillis() + backoff;
            log.error("outbox publish failed, backing off {} ms: {}", backoff, e.getMessage());
        }
        if (now - lastGauge > 5000) {
            lastGauge = now;
            Long pending = jdbc.queryForObject("SELECT count(*) FROM outbox_events WHERE status = 'PENDING'", Long.class);
            metrics.outboxPending(pending == null ? 0 : pending);
        }
    }

    /** Publishes one batch; returns how many rows were published. Throws (after recording the attempt) if Kafka failed. */
    int publishOnce() {
        String[] failure = new String[1];
        Integer published = tx.execute(status -> {
            List<Map<String, Object>> rows = jdbc.queryForList(
                    "SELECT id, topic, event_key, payload::text AS payload, headers::text AS headers FROM outbox_events "
                            + "WHERE status = 'PENDING' AND next_attempt_at <= now() ORDER BY id LIMIT ? FOR UPDATE SKIP LOCKED",
                    BATCH);
            if (rows.isEmpty()) return 0;
            List<Long> ids = new ArrayList<>();
            List<CompletableFuture<?>> sends = new ArrayList<>();
            Map<String, Integer> perTopic = new LinkedHashMap<>();
            try {
                for (Map<String, Object> r : rows) {
                    ids.add(((Number) r.get("id")).longValue());
                    String topic = (String) r.get("topic");
                    ProducerRecord<String, String> record = new ProducerRecord<>(topic, (String) r.get("event_key"), (String) r.get("payload"));
                    Map<String, String> headers = mapper.readValue((String) r.get("headers"), new TypeReference<Map<String, String>>() {});
                    headers.forEach((k, v) -> record.headers().add(k, v.getBytes(StandardCharsets.UTF_8)));
                    sends.add(kafka.send(record));
                    perTopic.merge(topic, 1, Integer::sum);
                }
                CompletableFuture.allOf(sends.toArray(CompletableFuture[]::new)).get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                // Record the failed attempt and COMMIT it (like the Node publisher), then report the failure.
                String error = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
                jdbc.update("UPDATE outbox_events SET attempt_count = attempt_count + 1, last_error = ?, "
                                + "next_attempt_at = now() + make_interval(secs => LEAST(power(2, attempt_count + 1), 60)) WHERE id IN (" + idList(ids) + ")",
                        error.substring(0, Math.min(500, error.length())));
                failure[0] = error;
                return 0;
            }
            jdbc.update("UPDATE outbox_events SET status = 'PUBLISHED', published_at = now() WHERE id IN (" + idList(ids) + ")");
            perTopic.forEach(metrics::outboxPublished);
            return rows.size();
        });
        if (failure[0] != null) throw new OutboxSendException(failure[0]);
        return published == null ? 0 : published;
    }

    /** ids are longs read from our own table, so inlining them is injection-safe. */
    private static String idList(List<Long> ids) {
        return ids.stream().map(String::valueOf).reduce((a, b) -> a + "," + b).orElse("0");
    }

    /** Raised after the failed attempt has been committed, so tick() backs off. */
    static class OutboxSendException extends RuntimeException {
        OutboxSendException(String message) {
            super(message);
        }
    }
}
