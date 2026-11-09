package com.skybook.inventory.sweeper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Triggers the sweeper on a fixed delay. Kept in a separate bean from the @Transactional sweep() method,
 * so the call goes through the Spring proxy and really runs in a transaction.
 */
@Component
public class SweeperSchedule {
    private static final Logger log = LoggerFactory.getLogger(SweeperSchedule.class);
    private final ExpiredHoldSweeper sweeper;

    public SweeperSchedule(ExpiredHoldSweeper sweeper) {
        this.sweeper = sweeper;
    }

    @Scheduled(fixedDelayString = "${skybook.hold-sweep-interval-ms:15000}", initialDelayString = "${skybook.hold-sweep-initial-delay-ms:5000}")
    public void run() {
        try {
            sweeper.sweep(100);
        } catch (Exception e) {
            log.warn("hold sweeper failed: {}", e.getMessage());
        }
    }
}
