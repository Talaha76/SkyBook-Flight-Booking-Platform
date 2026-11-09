package com.skybook.inventory.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Typed event payloads, checked at compile time (the Node version checks them at runtime with zod).
 * Incoming payloads validate their required fields in the compact constructor; a bad payload throws
 * InvalidPayloadException and is skipped, never retried forever.
 */
public final class Payloads {
    private Payloads() {}

    public static class InvalidPayloadException extends RuntimeException {
        public InvalidPayloadException(String message) {
            super(message);
        }
    }

    static void require(Object value, String field) {
        if (value == null || (value instanceof String s && s.isBlank())) throw new InvalidPayloadException("missing field: " + field);
    }

    static void requireUuid(String value, String field) {
        require(value, field);
        try {
            java.util.UUID.fromString(value);
        } catch (IllegalArgumentException e) {
            throw new InvalidPayloadException("not a UUID: " + field);
        }
    }

    // ---- incoming -------------------------------------------------------------------------------------
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record BookingRequested(String bookingId, String flightId, String userId, String passengerName, String passengerEmail, String paymentToken) {
        public BookingRequested {
            requireUuid(bookingId, "bookingId");
            requireUuid(flightId, "flightId");
            require(paymentToken, "paymentToken");
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record PaymentResult(String bookingId, String flightId, String paymentId, Integer amountCents, String reason) {
        public PaymentResult {
            requireUuid(bookingId, "bookingId");
            requireUuid(flightId, "flightId");
        }
    }

    // ---- outgoing (same field names as the Node service emits) --------------------------------------------
    public record SeatHeld(String bookingId, String flightId, String holdId, int amountCents, String expiresAt, String paymentToken, int available) {}

    public record SeatHoldFailed(String bookingId, String flightId, String reason) {}

    public record SeatReleased(String bookingId, String flightId, String reason, int available) {}

    public record SeatConfirmed(String bookingId, String flightId, int available) {}
}
