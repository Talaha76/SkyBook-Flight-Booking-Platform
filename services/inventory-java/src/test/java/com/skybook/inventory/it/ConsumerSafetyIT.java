package com.skybook.inventory.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.skybook.inventory.consumer.InventoryEventService;
import com.skybook.inventory.consumer.InventoryEventService.Outcome;
import com.skybook.inventory.events.EventEnvelope;
import com.skybook.inventory.events.EventFactory;
import com.skybook.inventory.holds.HoldResult;
import com.skybook.inventory.holds.LockStrategy;
import com.skybook.inventory.holds.SeatHoldRepository;
import com.skybook.inventory.sweeper.ExpiredHoldSweeper;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.support.TransactionTemplate;

/** Mirrors the Node "duplicate-safe consumers + compensation" integration tests, plus the HTTP endpoint. */
class ConsumerSafetyIT extends IntegrationTestBase {
    @Autowired
    InventoryEventService service;
    @Autowired
    EventFactory events;
    @Autowired
    SeatHoldRepository holds;
    @Autowired
    ExpiredHoldSweeper sweeper;
    @Autowired
    TransactionTemplate tx;
    @Autowired
    TestRestTemplate http;

    private EventEnvelope bookingRequested(UUID bookingId, UUID flightId, String token) {
        return events.make("BookingRequested", bookingId.toString(), Map.of("bookingId", bookingId.toString(), "flightId", flightId.toString(),
                "userId", UUID.randomUUID().toString(), "passengerName", "A", "passengerEmail", "a@example.com", "paymentToken", token), "it");
    }

    private List<String> outboxTypes() {
        return jdbc.queryForList("SELECT payload->>'type' FROM outbox_events ORDER BY id", String.class);
    }

    @Test
    void sameBookingRequestedTwiceChangesStateOnce() {
        UUID flightId = seedFlight(10, 10000);
        EventEnvelope event = bookingRequested(UUID.randomUUID(), flightId, "tok_success");
        assertThat(service.handleOnce(event)).isEqualTo(Outcome.PROCESSED);
        assertThat(service.handleOnce(event)).isEqualTo(Outcome.DUPLICATE);
        assertThat(available(flightId)).isEqualTo(9);
        assertThat(outboxTypes()).containsExactly("SeatHeld");
    }

    @Test
    void paymentFailedReleasesTheSeatExactlyOnce() {
        UUID flightId = seedFlight(5, 10000);
        UUID bookingId = UUID.randomUUID();
        service.handleOnce(bookingRequested(bookingId, flightId, "tok_fail"));
        Map<String, Object> failed = Map.of("bookingId", bookingId.toString(), "flightId", flightId.toString(),
                "paymentId", UUID.randomUUID().toString(), "amountCents", 10000, "reason", "CARD_DECLINED");
        service.handleOnce(events.make("PaymentFailed", bookingId.toString(), failed, null));
        // a different PaymentFailed event for the same booking must not release twice
        service.handleOnce(events.make("PaymentFailed", bookingId.toString(), failed, null));
        assertThat(available(flightId)).isEqualTo(5);
        assertThat(jdbc.queryForObject("SELECT status FROM seat_holds WHERE booking_id = ?", String.class, bookingId)).isEqualTo("RELEASED");
        assertThat(outboxTypes()).containsExactly("SeatHeld", "SeatReleased");
    }

    @Test
    void paymentSucceededConfirmsTheHold() {
        UUID flightId = seedFlight(5, 10000);
        UUID bookingId = UUID.randomUUID();
        service.handleOnce(bookingRequested(bookingId, flightId, "tok_success"));
        service.handleOnce(events.make("PaymentSucceeded", bookingId.toString(), Map.of("bookingId", bookingId.toString(),
                "flightId", flightId.toString(), "paymentId", UUID.randomUUID().toString(), "amountCents", 10000), null));
        assertThat(available(flightId)).isEqualTo(4);
        assertThat(jdbc.queryForObject("SELECT status FROM seat_holds WHERE booking_id = ?", String.class, bookingId)).isEqualTo("CONFIRMED");
        assertThat(outboxTypes()).containsExactly("SeatHeld", "SeatConfirmed");
    }

    @Test
    void expiredHoldsAreSweptBackIntoInventory() throws Exception {
        UUID flightId = seedFlight(3, 10000);
        HoldResult r = tx.execute(s -> holds.holdSeat(UUID.randomUUID(), flightId, LockStrategy.PESSIMISTIC, 0, 50));
        assertThat(r).isInstanceOf(HoldResult.Held.class);
        Thread.sleep(20);
        assertThat(sweeper.sweep(100)).isEqualTo(1);
        assertThat(available(flightId)).isEqualTo(3);
        assertThat(jdbc.queryForObject("SELECT payload->'payload'->>'reason' FROM outbox_events", String.class)).isEqualTo("EXPIRED");
    }

    @Test
    void inventoryEndpointMatchesTheNodeContract() {
        UUID flightId = seedFlight(7, 34900);
        HttpHeaders h = new HttpHeaders();
        h.set("x-internal-token", "it-token");
        h.set("x-user-id", UUID.randomUUID().toString());
        h.set("x-user-role", "AGENT");
        ResponseEntity<Map> ok = http.exchange("/inventory/" + flightId, HttpMethod.GET, new HttpEntity<>(h), Map.class);
        assertThat(ok.getStatusCode().value()).isEqualTo(200);
        assertThat(ok.getBody()).containsEntry("flightId", flightId.toString()).containsEntry("totalSeats", 7)
                .containsEntry("available", 7).containsEntry("priceCents", 34900);

        assertThat(http.getForEntity("/inventory/" + flightId, Map.class).getStatusCode().value()).isEqualTo(401);
        h.set("x-user-role", "CUSTOMER");
        assertThat(http.exchange("/inventory/" + flightId, HttpMethod.GET, new HttpEntity<>(h), Map.class).getStatusCode().value()).isEqualTo(403);
        h.set("x-user-role", "AGENT");
        assertThat(http.exchange("/inventory/nope", HttpMethod.GET, new HttpEntity<>(h), Map.class).getStatusCode().value()).isEqualTo(400);
        assertThat(http.exchange("/inventory/" + UUID.randomUUID(), HttpMethod.GET, new HttpEntity<>(h), Map.class).getStatusCode().value()).isEqualTo(404);
        assertThat(http.getForEntity("/ready", String.class).getStatusCode().value()).isEqualTo(200);
        String metrics = http.getForObject("/metrics", String.class);
        assertThat(metrics).contains("http_request_duration_seconds").contains("outbox_pending");
    }
}
