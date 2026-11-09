package com.skybook.inventory.holds;

import java.time.Instant;

/** Outcome of trying to hold one seat. */
public sealed interface HoldResult {

    /** A seat is held (existing = the booking already had a hold, so nothing new must be emitted). */
    record Held(String holdId, int amountCents, int available, Instant expiresAt, boolean existing) implements HoldResult {}

    record Failed(Reason reason) implements HoldResult {}

    enum Reason { FLIGHT_NOT_FOUND, SOLD_OUT, CONTENTION }

    /** Result of a release/confirm: the flight and its availability afterwards. */
    record SeatChange(String flightId, int available) {}
}
