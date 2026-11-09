package com.skybook.inventory.it;

import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Real PostgreSQL + real Kafka for every integration test.
 *
 * Uses TEST_DATABASE_URL / TEST_KAFKA_BROKERS when set (the Compose stack: see the "tests-java" service
 * in docker-compose.java.yml), otherwise starts throwaway containers with Testcontainers (needs Docker).
 * Topic names and the consumer group are unique per JVM run, so tests never interfere with the live
 * stack's booking.events / inventory.events / payment.events.
 */
// Spring Boot tests disable metrics export by default; turn it on so /metrics is tested for real.
@AutoConfigureObservability(tracing = false)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public abstract class IntegrationTestBase {
    static final String RUN = UUID.randomUUID().toString().substring(0, 8);
    static final String BOOKING_TOPIC = "it-" + RUN + ".booking.events";
    static final String INVENTORY_TOPIC = "it-" + RUN + ".inventory.events";
    static final String PAYMENT_TOPIC = "it-" + RUN + ".payment.events";

    private static final String DATABASE_URL;
    private static final String KAFKA_BROKERS;

    static {
        String db = System.getenv("TEST_DATABASE_URL");
        if (db == null || db.isBlank()) {
            PostgreSQLContainer<?> pg = new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"));
            pg.start(); // stopped by Testcontainers' Ryuk when the JVM exits
            db = pg.getJdbcUrl() + (pg.getJdbcUrl().contains("?") ? "&" : "?") + "user=" + pg.getUsername() + "&password=" + pg.getPassword();
        }
        String kafka = System.getenv("TEST_KAFKA_BROKERS");
        if (kafka == null || kafka.isBlank()) {
            KafkaContainer k = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));
            k.start();
            kafka = k.getBootstrapServers();
        }
        DATABASE_URL = db;
        KAFKA_BROKERS = kafka;
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry r) {
        r.add("skybook.database-url", () -> DATABASE_URL);
        r.add("spring.kafka.bootstrap-servers", () -> KAFKA_BROKERS);
        r.add("spring.kafka.consumer.group-id", () -> "inventory-it-" + RUN);
        r.add("skybook.consumer-group", () -> "inventory-it-" + RUN);
        r.add("skybook.topics.booking", () -> BOOKING_TOPIC);
        r.add("skybook.topics.inventory", () -> INVENTORY_TOPIC);
        r.add("skybook.topics.payment", () -> PAYMENT_TOPIC);
        r.add("skybook.internal-token", () -> "it-token");
        r.add("skybook.db-pool-max", () -> "40");
        // The sweeper is called directly by the tests; never let the schedule race them.
        r.add("skybook.hold-sweep-initial-delay-ms", () -> "3600000");
    }

    @Autowired
    protected JdbcTemplate jdbc;

    @BeforeEach
    void cleanTables() {
        jdbc.execute("TRUNCATE outbox_events, processed_events, seat_holds, seat_inventory CASCADE");
    }

    protected UUID seedFlight(int seats, int priceCents) {
        UUID flightId = UUID.randomUUID();
        jdbc.update("INSERT INTO seat_inventory (flight_id, total_seats, available, price_cents) VALUES (?, ?, ?, ?)",
                flightId, seats, seats, priceCents);
        return flightId;
    }

    protected int available(UUID flightId) {
        return jdbc.queryForObject("SELECT available FROM seat_inventory WHERE flight_id = ?", Integer.class, flightId);
    }
}
