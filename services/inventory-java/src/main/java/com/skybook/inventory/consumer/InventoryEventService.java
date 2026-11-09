package com.skybook.inventory.consumer;

import com.skybook.inventory.config.AppProperties;
import com.skybook.inventory.events.EnvelopeParser;
import com.skybook.inventory.events.EventEnvelope;
import com.skybook.inventory.events.EventFactory;
import com.skybook.inventory.events.EventTypes;
import com.skybook.inventory.events.Payloads;
import com.skybook.inventory.holds.HoldResult;
import com.skybook.inventory.holds.SeatHoldRepository;
import com.skybook.inventory.outbox.OutboxWriter;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Applies one Kafka event exactly once. Everything in handleOnce() commits or rolls back together:
 * the processed_events row, the seat change and the outbox event. It lives in its own bean because
 * @Transactional works through a Spring proxy: calling it from the listener class itself would bypass
 * the proxy and silently run without a transaction.
 */
@Service
public class InventoryEventService {
    private static final Logger log = LoggerFactory.getLogger(InventoryEventService.class);
    public static final String CONSUMER = "inventory"; // same processed_events key as the Node service

    public enum Outcome { PROCESSED, DUPLICATE, IGNORED, INVALID }

    private final ProcessedEventGuard guard;
    private final SeatHoldRepository holds;
    private final OutboxWriter outbox;
    private final EventFactory events;
    private final EnvelopeParser parser;
    private final AppProperties props;

    public InventoryEventService(ProcessedEventGuard guard, SeatHoldRepository holds, OutboxWriter outbox,
                                 EventFactory events, EnvelopeParser parser, AppProperties props) {
        this.guard = guard;
        this.holds = holds;
        this.outbox = outbox;
        this.events = events;
        this.parser = parser;
        this.props = props;
    }

    public static boolean handles(String type) {
        return EventTypes.BOOKING_REQUESTED.equals(type) || EventTypes.PAYMENT_SUCCEEDED.equals(type) || EventTypes.PAYMENT_FAILED.equals(type);
    }

    @Transactional
    public Outcome handleOnce(EventEnvelope event) {
        if (!handles(event.type())) return Outcome.IGNORED;
        if (!guard.markProcessed(CONSUMER, event.eventId())) return Outcome.DUPLICATE;
        try {
            switch (event.type()) {
                case EventTypes.BOOKING_REQUESTED -> onBookingRequested(event);
                case EventTypes.PAYMENT_SUCCEEDED -> onPaymentSucceeded(event);
                case EventTypes.PAYMENT_FAILED -> onPaymentFailed(event);
                default -> { return Outcome.IGNORED; }
            }
        } catch (Payloads.InvalidPayloadException e) {
            // A payload that can never be valid: keep it marked as processed so it can't block the partition.
            log.warn("skipping event {} with invalid payload: {}", event.eventId(), e.getMessage());
            return Outcome.INVALID;
        }
        return Outcome.PROCESSED;
    }

    private void onBookingRequested(EventEnvelope event) {
        Payloads.BookingRequested p = parser.payload(event, Payloads.BookingRequested.class);
        HoldResult result = holds.holdSeat(UUID.fromString(p.bookingId()), UUID.fromString(p.flightId()),
                props.lockStrategy(), props.holdTtlSeconds(), 50);
        if (result instanceof HoldResult.Held held) {
            if (held.existing()) return; // the hold (and its SeatHeld event) already exist for this booking
            log.info("seat held flight={} available={}", p.flightId(), held.available());
            outbox.add(props.topics().inventory(), p.flightId(), events.make(EventTypes.SEAT_HELD, p.bookingId(),
                    new Payloads.SeatHeld(p.bookingId(), p.flightId(), held.holdId(), held.amountCents(),
                            held.expiresAt().toString(), p.paymentToken(), held.available()),
                    event.requestId()));
        } else if (result instanceof HoldResult.Failed failed) {
            log.info("seat hold failed flight={} reason={}", p.flightId(), failed.reason());
            outbox.add(props.topics().inventory(), p.flightId(), events.make(EventTypes.SEAT_HOLD_FAILED, p.bookingId(),
                    new Payloads.SeatHoldFailed(p.bookingId(), p.flightId(), failed.reason().name()), event.requestId()));
        }
    }

    private void onPaymentSucceeded(EventEnvelope event) {
        Payloads.PaymentResult p = parser.payload(event, Payloads.PaymentResult.class);
        holds.confirmHold(UUID.fromString(p.bookingId())).ifPresent(c ->
                outbox.add(props.topics().inventory(), c.flightId(), events.make(EventTypes.SEAT_CONFIRMED, p.bookingId(),
                        new Payloads.SeatConfirmed(p.bookingId(), c.flightId(), c.available()), event.requestId())));
    }

    /** Compensation: payment failed, give the seat back. */
    private void onPaymentFailed(EventEnvelope event) {
        Payloads.PaymentResult p = parser.payload(event, Payloads.PaymentResult.class);
        holds.releaseHold(UUID.fromString(p.bookingId()), "RELEASED").ifPresent(r -> {
            log.info("seat released after payment failure flight={} available={}", r.flightId(), r.available());
            outbox.add(props.topics().inventory(), r.flightId(), events.make(EventTypes.SEAT_RELEASED, p.bookingId(),
                    new Payloads.SeatReleased(p.bookingId(), r.flightId(), "PAYMENT_FAILED", r.available()), event.requestId()));
        });
    }
}
