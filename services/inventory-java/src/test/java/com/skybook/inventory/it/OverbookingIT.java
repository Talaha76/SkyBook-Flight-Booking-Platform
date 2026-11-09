package com.skybook.inventory.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.skybook.inventory.holds.HoldResult;
import com.skybook.inventory.holds.LockStrategy;
import com.skybook.inventory.holds.SeatHoldRepository;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.support.TransactionTemplate;

/** Same scenario as tests/integration/inventory.itest.ts: 200 concurrent holds on a 150-seat flight. */
class OverbookingIT extends IntegrationTestBase {
    @Autowired
    SeatHoldRepository holds;
    @Autowired
    TransactionTemplate tx;

    @ParameterizedTest(name = "{0}: 200 concurrent holds on 150 seats -> exactly 150 held, 50 rejected")
    @EnumSource(LockStrategy.class)
    void neverOverbooks(LockStrategy strategy) throws Exception {
        UUID flightId = seedFlight(150, 10000);
        ExecutorService pool = Executors.newFixedThreadPool(32);
        List<Callable<HoldResult>> tasks = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            tasks.add(() -> tx.execute(s -> holds.holdSeat(UUID.randomUUID(), flightId, strategy, 600, 500)));
        }
        long started = System.nanoTime();
        List<HoldResult> results = new ArrayList<>();
        for (Future<HoldResult> f : pool.invokeAll(tasks)) results.add(f.get());
        long ms = (System.nanoTime() - started) / 1_000_000;
        pool.shutdown();

        long held = results.stream().filter(r -> r instanceof HoldResult.Held).count();
        long soldOut = results.stream().filter(r -> r instanceof HoldResult.Failed f && f.reason() == HoldResult.Reason.SOLD_OUT).count();
        assertThat(held).isEqualTo(150);
        assertThat(soldOut).isEqualTo(50);
        assertThat(available(flightId)).isZero();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM seat_holds WHERE flight_id = ?", Integer.class, flightId)).isEqualTo(150);
        System.out.printf("%s (java): 200 concurrent holds on 150 seats in %d ms -> %d held / %d rejected%n",
                strategy.name().toLowerCase(), ms, held, soldOut);
    }
}
