package com.skybook.inventory.events;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import org.springframework.stereotype.Component;

/**
 * Parses a Kafka message value into an EventEnvelope, applying the same rules as the zod schema in
 * packages/shared/src/events.ts. Returns null for anything malformed, so the caller can skip it.
 */
@Component
public class EnvelopeParser {
    private final ObjectMapper mapper;

    public EnvelopeParser(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public EventEnvelope parse(String value) {
        if (value == null || value.isBlank()) return null;
        try {
            JsonNode n = mapper.readTree(value);
            if (n == null || !n.isObject()) return null;
            String eventId = text(n, "eventId");
            String type = text(n, "type");
            String occurredAt = text(n, "occurredAt");
            String aggregateId = text(n, "aggregateId");
            JsonNode payload = n.get("payload");
            JsonNode version = n.get("version");
            if (!isUuid(eventId) || type == null || type.isEmpty() || occurredAt == null || !isUuid(aggregateId)) return null;
            if (version == null || !version.canConvertToInt() || !version.isIntegralNumber() || version.asInt() <= 0) return null;
            if (payload == null || !payload.isObject()) return null;
            JsonNode requestId = n.get("requestId");
            if (requestId != null && !requestId.isNull() && !requestId.isTextual()) return null;
            return new EventEnvelope(eventId, type, version.asInt(), occurredAt, aggregateId,
                    requestId == null || requestId.isNull() ? null : requestId.asText(), payload);
        } catch (Exception e) {
            return null;
        }
    }

    public <T> T payload(EventEnvelope event, Class<T> type) {
        try {
            return mapper.treeToValue(event.payload(), type);
        } catch (Payloads.InvalidPayloadException e) {
            throw e;
        } catch (Exception e) {
            Throwable cause = e.getCause();
            if (cause instanceof Payloads.InvalidPayloadException ipe) throw ipe;
            throw new Payloads.InvalidPayloadException("cannot read " + type.getSimpleName() + ": " + e.getMessage());
        }
    }

    private static String text(JsonNode n, String field) {
        JsonNode v = n.get(field);
        return v != null && v.isTextual() ? v.asText() : null;
    }

    private static boolean isUuid(String s) {
        if (s == null) return false;
        try {
            UUID.fromString(s);
            return s.length() == 36;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
}
