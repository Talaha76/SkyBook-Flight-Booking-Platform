package com.skybook.inventory.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.skybook.inventory.events.EventEnvelope;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Writes an event to outbox_events inside the caller's transaction (no dual write).
 * Same columns and headers as insertOutbox() in packages/shared/src/outbox.ts.
 */
@Component
public class OutboxWriter {
    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;

    public OutboxWriter(JdbcTemplate jdbc, ObjectMapper mapper) {
        this.jdbc = jdbc;
        this.mapper = mapper;
    }

    public void add(String topic, String key, EventEnvelope event) {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("x-request-id", event.requestId() == null ? "" : event.requestId());
        headers.put("event-type", event.type());
        headers.put("x-message-id", event.eventId());
        try {
            jdbc.update(
                    "INSERT INTO outbox_events (event_id, aggregate_id, topic, event_key, payload, headers) VALUES (?, ?, ?, ?, ?::jsonb, ?::jsonb)",
                    UUID.fromString(event.eventId()), UUID.fromString(event.aggregateId()), topic, key,
                    mapper.writeValueAsString(event), mapper.writeValueAsString(headers));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("cannot serialize event " + event.eventId(), e);
        }
    }
}
