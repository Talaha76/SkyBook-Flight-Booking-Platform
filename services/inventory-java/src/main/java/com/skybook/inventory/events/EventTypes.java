package com.skybook.inventory.events;

public final class EventTypes {
    public static final String BOOKING_REQUESTED = "BookingRequested";
    public static final String PAYMENT_SUCCEEDED = "PaymentSucceeded";
    public static final String PAYMENT_FAILED = "PaymentFailed";
    public static final String SEAT_HELD = "SeatHeld";
    public static final String SEAT_HOLD_FAILED = "SeatHoldFailed";
    public static final String SEAT_RELEASED = "SeatReleased";
    public static final String SEAT_CONFIRMED = "SeatConfirmed";

    private EventTypes() {}
}
