package com.skybook.inventory.holds;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Seat locking with plain SQL through JdbcTemplate (no ORM), so the exact statements are visible:
 * the same queries as services/inventory/src/holds.ts. Every method must run inside the caller's
 * transaction (InventoryEventService / ExpiredHoldSweeper are @Transactional).
 */
@Repository
public class SeatHoldRepository {
    private final JdbcTemplate jdbc;

    public SeatHoldRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public HoldResult holdSeat(UUID bookingId, UUID flightId, LockStrategy strategy, int holdTtlSeconds, int maxRetries) {
        // Idempotency beyond processed_events: a booking never gets two holds.
        List<Map<String, Object>> existing = jdbc.queryForList(
                "SELECT h.id, h.amount_cents, h.expires_at, i.available FROM seat_holds h JOIN seat_inventory i USING (flight_id) WHERE h.booking_id = ?",
                bookingId);
        if (!existing.isEmpty()) {
            Map<String, Object> h = existing.get(0);
            return new HoldResult.Held(h.get("id").toString(), ((Number) h.get("amount_cents")).intValue(),
                    ((Number) h.get("available")).intValue(), ((Timestamp) h.get("expires_at")).toInstant(), true);
        }

        int price;
        int available;
        if (strategy == LockStrategy.PESSIMISTIC) {
            List<Map<String, Object>> rows = jdbc.queryForList(
                    "SELECT available, price_cents FROM seat_inventory WHERE flight_id = ? FOR UPDATE", flightId);
            if (rows.isEmpty()) return new HoldResult.Failed(HoldResult.Reason.FLIGHT_NOT_FOUND);
            if (((Number) rows.get(0).get("available")).intValue() <= 0) return new HoldResult.Failed(HoldResult.Reason.SOLD_OUT);
            available = jdbc.queryForObject(
                    "UPDATE seat_inventory SET available = available - 1, version = version + 1, updated_at = now() WHERE flight_id = ? RETURNING available",
                    Integer.class, flightId);
            price = ((Number) rows.get(0).get("price_cents")).intValue();
        } else {
            Integer newAvailable = null;
            int rowPrice = 0;
            for (int attempt = 0; newAvailable == null; attempt++) {
                List<Map<String, Object>> rows = jdbc.queryForList(
                        "SELECT available, price_cents, version FROM seat_inventory WHERE flight_id = ?", flightId);
                if (rows.isEmpty()) return new HoldResult.Failed(HoldResult.Reason.FLIGHT_NOT_FOUND);
                Map<String, Object> r = rows.get(0);
                if (((Number) r.get("available")).intValue() <= 0) return new HoldResult.Failed(HoldResult.Reason.SOLD_OUT);
                rowPrice = ((Number) r.get("price_cents")).intValue();
                List<Integer> updated = jdbc.queryForList(
                        "UPDATE seat_inventory SET available = available - 1, version = version + 1, updated_at = now() "
                                + "WHERE flight_id = ? AND version = ? AND available > 0 RETURNING available",
                        Integer.class, flightId, ((Number) r.get("version")).intValue());
                if (!updated.isEmpty()) newAvailable = updated.get(0);
                else if (attempt >= maxRetries) return new HoldResult.Failed(HoldResult.Reason.CONTENTION);
            }
            available = newAvailable;
            price = rowPrice;
        }

        UUID holdId = UUID.randomUUID();
        Timestamp expiresAt = jdbc.queryForObject(
                "INSERT INTO seat_holds (id, booking_id, flight_id, status, amount_cents, expires_at) "
                        + "VALUES (?, ?, ?, 'HELD', ?, now() + make_interval(secs => ?)) RETURNING expires_at",
                Timestamp.class, holdId, bookingId, flightId, price, holdTtlSeconds);
        return new HoldResult.Held(holdId.toString(), price, available, expiresAt.toInstant(), false);
    }

    /** HELD -> RELEASED/EXPIRED and the seat goes back into stock. Empty if the hold was not HELD. */
    public Optional<HoldResult.SeatChange> releaseHold(UUID bookingId, String newStatus) {
        List<UUID> flights = jdbc.queryForList(
                "UPDATE seat_holds SET status = ?, updated_at = now() WHERE booking_id = ? AND status = 'HELD' RETURNING flight_id",
                UUID.class, newStatus, bookingId);
        if (flights.isEmpty()) return Optional.empty();
        Integer available = jdbc.queryForObject(
                "UPDATE seat_inventory SET available = available + 1, version = version + 1, updated_at = now() WHERE flight_id = ? RETURNING available",
                Integer.class, flights.get(0));
        return Optional.of(new HoldResult.SeatChange(flights.get(0).toString(), available));
    }

    /** HELD -> CONFIRMED (payment succeeded). Empty if the hold was not HELD. */
    public Optional<HoldResult.SeatChange> confirmHold(UUID bookingId) {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "UPDATE seat_holds h SET status = 'CONFIRMED', updated_at = now() FROM seat_inventory i "
                        + "WHERE h.booking_id = ? AND h.status = 'HELD' AND i.flight_id = h.flight_id RETURNING h.flight_id, i.available",
                bookingId);
        if (rows.isEmpty()) return Optional.empty();
        return Optional.of(new HoldResult.SeatChange(rows.get(0).get("flight_id").toString(), ((Number) rows.get(0).get("available")).intValue()));
    }

    /** Booking ids of expired HELD holds, locked so several replicas can sweep safely. */
    public List<UUID> lockExpiredHolds(int limit) {
        return jdbc.queryForList(
                "SELECT booking_id FROM seat_holds WHERE status = 'HELD' AND expires_at < now() ORDER BY expires_at LIMIT ? FOR UPDATE SKIP LOCKED",
                UUID.class, limit);
    }
}
