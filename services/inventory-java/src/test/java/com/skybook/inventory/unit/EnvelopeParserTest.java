package com.skybook.inventory.unit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.skybook.inventory.events.EnvelopeParser;
import com.skybook.inventory.events.EventEnvelope;
import com.skybook.inventory.events.EventFactory;
import com.skybook.inventory.events.Payloads;
import org.junit.jupiter.api.Test;

class EnvelopeParserTest {
    private final ObjectMapper mapper = new ObjectMapper();
    private final EnvelopeParser parser = new EnvelopeParser(mapper);

    private static final String VALID = """
            {"eventId":"0b7c3c2e-6f0a-4a43-9a52-2f3c1c8f1a11","type":"BookingRequested","version":1,
             "occurredAt":"2026-11-08T12:00:00.000Z","aggregateId":"7a1b0c20-2222-4222-8222-222222222222","requestId":"req-1",
             "payload":{"bookingId":"7a1b0c20-2222-4222-8222-222222222222","flightId":"9b2c0a40-1111-4111-8111-111111111111",
                        "userId":"11111111-1111-4111-8111-111111111111","passengerName":"A","passengerEmail":"a@x.io","paymentToken":"tok_success"}}
            """;

    @Test
    void parsesAValidEnvelopeAndPayload() {
        EventEnvelope e = parser.parse(VALID);
        assertThat(e).isNotNull();
        assertThat(e.type()).isEqualTo("BookingRequested");
        assertThat(e.requestId()).isEqualTo("req-1");
        Payloads.BookingRequested p = parser.payload(e, Payloads.BookingRequested.class);
        assertThat(p.paymentToken()).isEqualTo("tok_success");
    }

    @Test
    void rejectsMalformedMessages() {
        assertThat(parser.parse("not json")).isNull();
        assertThat(parser.parse("")).isNull();
        assertThat(parser.parse("[1,2]")).isNull();
        assertThat(parser.parse(VALID.replace("\"version\":1", "\"version\":0"))).isNull();
        assertThat(parser.parse(VALID.replace("0b7c3c2e-6f0a-4a43-9a52-2f3c1c8f1a11", "nope"))).isNull();
        assertThat(parser.parse(VALID.replace("\"type\":\"BookingRequested\",", ""))).isNull();
    }

    @Test
    void invalidPayloadThrowsInvalidPayloadException() {
        EventEnvelope e = parser.parse(VALID.replace("\"paymentToken\":\"tok_success\"", "\"paymentToken\":null"));
        assertThat(e).isNotNull();
        assertThatThrownBy(() -> parser.payload(e, Payloads.BookingRequested.class)).isInstanceOf(Payloads.InvalidPayloadException.class);
    }

    @Test
    void outgoingEnvelopeOmitsNullRequestId() throws Exception {
        // Node consumers validate with zod: requestId may be absent but must never be null.
        EventEnvelope out = new EventFactory(mapper).make("SeatHeld", "7a1b0c20-2222-4222-8222-222222222222", java.util.Map.of("a", 1), null);
        String json = mapper.writeValueAsString(out);
        assertThat(json).doesNotContain("requestId").contains("\"version\":1").contains("\"type\":\"SeatHeld\"");
        assertThat(parser.parse(json)).isNotNull();
    }
}
