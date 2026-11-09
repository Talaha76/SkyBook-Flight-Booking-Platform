package com.skybook.inventory.unit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.skybook.inventory.config.AppProperties;
import com.skybook.inventory.consumer.InventoryEventService;
import com.skybook.inventory.consumer.InventoryEventService.Outcome;
import com.skybook.inventory.consumer.ProcessedEventGuard;
import com.skybook.inventory.events.EnvelopeParser;
import com.skybook.inventory.events.EventEnvelope;
import com.skybook.inventory.events.EventFactory;
import com.skybook.inventory.holds.HoldResult;
import com.skybook.inventory.holds.LockStrategy;
import com.skybook.inventory.holds.SeatHoldRepository;
import com.skybook.inventory.outbox.OutboxWriter;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/** Handler logic with the repository and outbox mocked: one test per outcome. */
class InventoryEventServiceTest {
    private static final String BOOKING = "7a1b0c20-2222-4222-8222-222222222222";
    private static final String FLIGHT = "9b2c0a40-1111-4111-8111-111111111111";

    private final ObjectMapper mapper = new ObjectMapper();
    private final EventFactory factory = new EventFactory(mapper);
    private ProcessedEventGuard guard;
    private SeatHoldRepository holds;
    private OutboxWriter outbox;
    private InventoryEventService service;

    @BeforeEach
    void setUp() {
        guard = mock(ProcessedEventGuard.class);
        holds = mock(SeatHoldRepository.class);
        outbox = mock(OutboxWriter.class);
        AppProperties props = new AppProperties("postgres://x@y/z", "t", LockStrategy.PESSIMISTIC, 600, 15000, 200, 3, 3,
                new AppProperties.Topics("booking.events", "inventory.events", "payment.events"));
        service = new InventoryEventService(guard, holds, outbox, factory, new EnvelopeParser(mapper), props);
        when(guard.markProcessed(any(), any())).thenReturn(true);
    }

    private EventEnvelope bookingRequested() {
        return factory.make("BookingRequested", BOOKING, Map.of("bookingId", BOOKING, "flightId", FLIGHT, "userId", BOOKING,
                "passengerName", "A", "passengerEmail", "a@x.io", "paymentToken", "tok_success"), "req-1");
    }

    private EventEnvelope payment(String type) {
        return factory.make(type, BOOKING, Map.of("bookingId", BOOKING, "flightId", FLIGHT, "paymentId", BOOKING, "amountCents", 100), "req-2");
    }

    private EventEnvelope captureOutbox() {
        ArgumentCaptor<EventEnvelope> captor = ArgumentCaptor.forClass(EventEnvelope.class);
        verify(outbox).add(eq("inventory.events"), eq(FLIGHT), captor.capture());
        return captor.getValue();
    }

    @Test
    void heldSeatEmitsSeatHeldWithPriceAndToken() {
        when(holds.holdSeat(any(), any(), any(), anyInt(), anyInt()))
                .thenReturn(new HoldResult.Held(UUID.randomUUID().toString(), 34900, 179, Instant.parse("2026-11-08T12:10:00Z"), false));
        assertThat(service.handleOnce(bookingRequested())).isEqualTo(Outcome.PROCESSED);
        EventEnvelope out = captureOutbox();
        assertThat(out.type()).isEqualTo("SeatHeld");
        assertThat(out.requestId()).isEqualTo("req-1");
        assertThat(out.payload().get("amountCents").asInt()).isEqualTo(34900);
        assertThat(out.payload().get("available").asInt()).isEqualTo(179);
        assertThat(out.payload().get("paymentToken").asText()).isEqualTo("tok_success");
        assertThat(out.payload().get("expiresAt").asText()).isEqualTo("2026-11-08T12:10:00Z");
    }

    @Test
    void failedHoldEmitsSeatHoldFailedWithReason() {
        when(holds.holdSeat(any(), any(), any(), anyInt(), anyInt())).thenReturn(new HoldResult.Failed(HoldResult.Reason.SOLD_OUT));
        service.handleOnce(bookingRequested());
        EventEnvelope out = captureOutbox();
        assertThat(out.type()).isEqualTo("SeatHoldFailed");
        assertThat(out.payload().get("reason").asText()).isEqualTo("SOLD_OUT");
    }

    @Test
    void existingHoldEmitsNothing() {
        when(holds.holdSeat(any(), any(), any(), anyInt(), anyInt()))
                .thenReturn(new HoldResult.Held(UUID.randomUUID().toString(), 100, 5, Instant.now(), true));
        service.handleOnce(bookingRequested());
        verify(outbox, never()).add(any(), any(), any());
    }

    @Test
    void duplicateEventIsSkippedBeforeTouchingSeats() {
        when(guard.markProcessed(any(), any())).thenReturn(false);
        assertThat(service.handleOnce(bookingRequested())).isEqualTo(Outcome.DUPLICATE);
        verify(holds, never()).holdSeat(any(), any(), any(), anyInt(), anyInt());
    }

    @Test
    void paymentSucceededConfirmsTheHold() {
        when(holds.confirmHold(any())).thenReturn(Optional.of(new HoldResult.SeatChange(FLIGHT, 42)));
        service.handleOnce(payment("PaymentSucceeded"));
        EventEnvelope out = captureOutbox();
        assertThat(out.type()).isEqualTo("SeatConfirmed");
        assertThat(out.payload().get("available").asInt()).isEqualTo(42);
    }

    @Test
    void paymentFailedReleasesTheSeat() {
        when(holds.releaseHold(any(), eq("RELEASED"))).thenReturn(Optional.of(new HoldResult.SeatChange(FLIGHT, 43)));
        service.handleOnce(payment("PaymentFailed"));
        EventEnvelope out = captureOutbox();
        assertThat(out.type()).isEqualTo("SeatReleased");
        assertThat(out.payload().get("reason").asText()).isEqualTo("PAYMENT_FAILED");
    }

    @Test
    void missingHoldOnPaymentEventsEmitsNothing() {
        when(holds.confirmHold(any())).thenReturn(Optional.empty());
        when(holds.releaseHold(any(), any())).thenReturn(Optional.empty());
        service.handleOnce(payment("PaymentSucceeded"));
        service.handleOnce(payment("PaymentFailed"));
        verify(outbox, never()).add(any(), any(), any());
    }

    @Test
    void unknownTypesAreIgnoredWithoutRecordingThem() {
        assertThat(service.handleOnce(factory.make("SeatHeld", BOOKING, Map.of("x", 1), null))).isEqualTo(Outcome.IGNORED);
        verify(guard, never()).markProcessed(any(), any());
    }

    @Test
    void invalidPayloadIsSkippedNotRetried() {
        EventEnvelope bad = factory.make("BookingRequested", BOOKING, Map.of("bookingId", "not-a-uuid"), null);
        assertThat(service.handleOnce(bad)).isEqualTo(Outcome.INVALID);
        verify(holds, never()).holdSeat(any(), any(), any(), anyInt(), anyInt());
    }
}
