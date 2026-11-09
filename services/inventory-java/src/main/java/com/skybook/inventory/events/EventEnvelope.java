package com.skybook.inventory.events;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * The envelope every SkyBook service puts on Kafka (packages/shared/src/events.ts):
 * { eventId, type, version, occurredAt, aggregateId, requestId?, payload }.
 * NON_NULL matters: the Node consumers validate with zod, where an optional requestId may be absent
 * but must not be null.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record EventEnvelope(
        String eventId,
        String type,
        int version,
        String occurredAt,
        String aggregateId,
        String requestId,
        JsonNode payload) {}
