package com.skybook.inventory.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.skybook.inventory.events.EventEnvelope;
import com.skybook.inventory.events.EventFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Full loop through a real broker: BookingRequested in -> listener -> DB transaction -> outbox ->
 * publisher -> SeatHeld out, then PaymentFailed in -> SeatReleased out. A redelivered BookingRequested
 * produces nothing new (processed_events dedupe).
 */
class KafkaRoundTripIT extends IntegrationTestBase {
    @Autowired
    KafkaTemplate<String, String> kafka;
    @Autowired
    EventFactory events;
    @Autowired
    ObjectMapper mapper;
    @Value("${spring.kafka.bootstrap-servers}")
    String brokers;

    private KafkaConsumer<String, String> inventoryConsumer() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "it-reader-" + UUID.randomUUID());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        KafkaConsumer<String, String> c = new KafkaConsumer<>(p);
        c.subscribe(List.of(INVENTORY_TOPIC));
        return c;
    }

    private List<JsonNode> await(KafkaConsumer<String, String> c, String bookingId, int count, Duration timeout) throws Exception {
        List<JsonNode> found = new ArrayList<>();
        long deadline = System.nanoTime() + timeout.toNanos();
        while (found.size() < count && System.nanoTime() < deadline) {
            for (ConsumerRecord<String, String> r : c.poll(Duration.ofMillis(500))) {
                JsonNode n = mapper.readTree(r.value());
                if (bookingId.equals(n.path("aggregateId").asText())) found.add(n);
            }
        }
        return found;
    }

    private void send(String topic, String key, EventEnvelope e) throws Exception {
        kafka.send(topic, key, mapper.writeValueAsString(e)).get();
    }

    @Test
    void bookingRequestedToSeatHeldAndCompensation() throws Exception {
        UUID flightId = seedFlight(4, 21900);
        UUID bookingId = UUID.randomUUID();
        EventEnvelope requested = events.make("BookingRequested", bookingId.toString(), Map.of("bookingId", bookingId.toString(),
                "flightId", flightId.toString(), "userId", UUID.randomUUID().toString(), "passengerName", "A",
                "passengerEmail", "a@example.com", "paymentToken", "tok_fail"), "it-roundtrip");

        try (KafkaConsumer<String, String> c = inventoryConsumer()) {
            send(BOOKING_TOPIC, flightId.toString(), requested);
            send(BOOKING_TOPIC, flightId.toString(), requested); // redelivery: must be deduplicated

            List<JsonNode> held = await(c, bookingId.toString(), 1, Duration.ofSeconds(60));
            assertThat(held).hasSize(1);
            assertThat(held.get(0).path("type").asText()).isEqualTo("SeatHeld");
            assertThat(held.get(0).path("requestId").asText()).isEqualTo("it-roundtrip");
            assertThat(held.get(0).path("payload").path("amountCents").asInt()).isEqualTo(21900);
            assertThat(available(flightId)).isEqualTo(3);

            send(PAYMENT_TOPIC, flightId.toString(), events.make("PaymentFailed", bookingId.toString(), Map.of("bookingId", bookingId.toString(),
                    "flightId", flightId.toString(), "paymentId", UUID.randomUUID().toString(), "amountCents", 21900, "reason", "CARD_DECLINED"), null));
            List<JsonNode> released = await(c, bookingId.toString(), 1, Duration.ofSeconds(60));
            assertThat(released).hasSize(1);
            assertThat(released.get(0).path("type").asText()).isEqualTo("SeatReleased");
            assertThat(released.get(0).path("payload").path("reason").asText()).isEqualTo("PAYMENT_FAILED");
            assertThat(released.get(0).has("requestId")).isFalse(); // never null: Node's zod schema would reject it
            assertThat(available(flightId)).isEqualTo(4);

            // Nothing else for this booking (the duplicate BookingRequested produced no second SeatHeld).
            assertThat(await(c, bookingId.toString(), 1, Duration.ofSeconds(3))).isEmpty();
        }
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox_events WHERE status = 'PENDING'", Integer.class)).isZero();
    }
}
