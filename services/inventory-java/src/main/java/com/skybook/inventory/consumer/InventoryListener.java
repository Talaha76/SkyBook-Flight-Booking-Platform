package com.skybook.inventory.consumer;

import com.skybook.inventory.events.EnvelopeParser;
import com.skybook.inventory.events.EventEnvelope;
import com.skybook.inventory.metrics.SkybookMetrics;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Spring Kafka hands over one record at a time (one thread per partition). The offset is acknowledged
 * only AFTER InventoryEventService has committed its transaction: if the service crashes in between,
 * the record is redelivered and processed_events makes the replay harmless. If handleOnce throws
 * (e.g. the database is down), no ack is sent and the DefaultErrorHandler retries with backoff.
 */
@Component
public class InventoryListener {
    private static final Logger log = LoggerFactory.getLogger(InventoryListener.class);

    private final EnvelopeParser parser;
    private final InventoryEventService service;
    private final SkybookMetrics metrics;

    public InventoryListener(EnvelopeParser parser, InventoryEventService service, SkybookMetrics metrics) {
        this.parser = parser;
        this.service = service;
        this.metrics = metrics;
    }

    @KafkaListener(
            id = "inventory",
            groupId = "${skybook.consumer-group:inventory}",
            topics = {"${skybook.topics.booking}", "${skybook.topics.payment}"},
            containerFactory = "kafkaListenerContainerFactory")
    public void onMessage(ConsumerRecord<String, String> record, Acknowledgment ack, Consumer<?, ?> consumer) {
        EventEnvelope event = parser.parse(record.value());
        if (event == null) {
            log.warn("skipping malformed message topic={} offset={}", record.topic(), record.offset());
            metrics.consumed(record.topic(), "unknown", "malformed");
            ack.acknowledge();
            return;
        }
        MDC.put("eventId", event.eventId());
        MDC.put("eventType", event.type());
        if (event.requestId() != null) MDC.put("requestId", event.requestId());
        if (event.payload().hasNonNull("bookingId")) MDC.put("bookingId", event.payload().get("bookingId").asText());
        try {
            InventoryEventService.Outcome outcome = service.handleOnce(event); // @Transactional, separate bean
            ack.acknowledge(); // reached only if the transaction committed
            String result = switch (outcome) {
                case PROCESSED -> "processed";
                case DUPLICATE -> "duplicate";
                case IGNORED -> "ignored";
                case INVALID -> "invalid";
            };
            metrics.consumed(record.topic(), event.type(), result);
            if (outcome != InventoryEventService.Outcome.IGNORED) {
                log.info("event handled result={} topic={} partition={}", result, record.topic(), record.partition());
            }
            consumer.currentLag(new TopicPartition(record.topic(), record.partition()))
                    .ifPresent(lag -> metrics.consumerLag("inventory", record.topic(), record.partition(), lag));
        } finally {
            MDC.clear();
        }
    }
}
