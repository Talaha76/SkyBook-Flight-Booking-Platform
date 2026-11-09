package com.skybook.inventory.events;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import org.springframework.stereotype.Component;

/** Builds outgoing envelopes exactly like makeEvent() in packages/shared/src/events.ts. */
@Component
public class EventFactory {
    private final ObjectMapper mapper;

    public EventFactory(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public EventEnvelope make(String type, String aggregateId, Object payload, String requestId) {
        return new EventEnvelope(
                UUID.randomUUID().toString(),
                type,
                1,
                Instant.now().truncatedTo(ChronoUnit.MILLIS).toString(),
                aggregateId,
                requestId == null || requestId.isEmpty() ? null : requestId,
                mapper.valueToTree(payload));
    }
}
