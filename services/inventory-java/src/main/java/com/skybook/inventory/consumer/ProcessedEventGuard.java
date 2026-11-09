package com.skybook.inventory.consumer;

import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/** Exactly-once per consumer: the processed_events insert succeeds only the first time an event is seen. */
@Component
public class ProcessedEventGuard {
    private final JdbcTemplate jdbc;

    public ProcessedEventGuard(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** @return true if this is the first time; false if the event was already processed (duplicate). */
    public boolean markProcessed(String consumer, String eventId) {
        return jdbc.update(
                "INSERT INTO processed_events (consumer, event_id) VALUES (?, ?) ON CONFLICT DO NOTHING",
                consumer, UUID.fromString(eventId)) == 1;
    }
}
