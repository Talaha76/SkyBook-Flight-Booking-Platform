package com.skybook.inventory.config;

import com.skybook.inventory.holds.LockStrategy;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Service settings, bound from the same environment variables the Node service reads (see application.yml). */
@ConfigurationProperties("skybook")
public record AppProperties(
        String databaseUrl,
        String internalToken,
        LockStrategy lockStrategy,
        int holdTtlSeconds,
        long holdSweepIntervalMs,
        long outboxPollMs,
        int kafkaConcurrency,
        int kafkaPartitions,
        Topics topics) {

    public record Topics(String booking, String inventory, String payment) {}
}
