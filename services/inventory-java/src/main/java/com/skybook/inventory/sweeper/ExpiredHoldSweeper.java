package com.skybook.inventory.sweeper;

import com.skybook.inventory.config.AppProperties;
import com.skybook.inventory.events.EventFactory;
import com.skybook.inventory.events.EventTypes;
import com.skybook.inventory.events.Payloads;
import com.skybook.inventory.holds.SeatHoldRepository;
import com.skybook.inventory.outbox.OutboxWriter;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Releases holds whose TTL has passed (e.g. Payment crashed), so a seat is never stranded.
 * Runs as one transaction per sweep: lock up to 100 expired holds (SKIP LOCKED), mark them EXPIRED,
 * return the seats, and write SeatReleased(EXPIRED) to the outbox.
 */
@Component
public class ExpiredHoldSweeper {
    private static final Logger log = LoggerFactory.getLogger(ExpiredHoldSweeper.class);

    private final SeatHoldRepository holds;
    private final OutboxWriter outbox;
    private final EventFactory events;
    private final AppProperties props;

    public ExpiredHoldSweeper(SeatHoldRepository holds, OutboxWriter outbox, EventFactory events, AppProperties props) {
        this.holds = holds;
        this.outbox = outbox;
        this.events = events;
        this.props = props;
    }

    @Transactional
    public int sweep(int limit) {
        List<UUID> expired = holds.lockExpiredHolds(limit);
        for (UUID bookingId : expired) {
            holds.releaseHold(bookingId, "EXPIRED").ifPresent(r ->
                    outbox.add(props.topics().inventory(), r.flightId(), events.make(EventTypes.SEAT_RELEASED, bookingId.toString(),
                            new Payloads.SeatReleased(bookingId.toString(), r.flightId(), "EXPIRED", r.available()), null)));
        }
        if (!expired.isEmpty()) log.info("expired seat holds released count={}", expired.size());
        return expired.size();
    }
}
