package com.skybook.inventory.web;

import io.micrometer.prometheus.PrometheusMeterRegistry;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** /health (liveness), /ready (database check) and /metrics (Prometheus) on the same paths as the Node services. */
@RestController
public class HealthController {
    private final JdbcTemplate jdbc;
    private final ObjectProvider<PrometheusMeterRegistry> prometheus;

    /** Optional: Spring Boot tests switch metrics export off unless asked, and the service must still start. */
    public HealthController(JdbcTemplate jdbc, ObjectProvider<PrometheusMeterRegistry> prometheus) {
        this.jdbc = jdbc;
        this.prometheus = prometheus;
    }

    @GetMapping("/health")
    public Map<String, String> health() {
        return Map.of("status", "ok");
    }

    @GetMapping("/ready")
    public ResponseEntity<Map<String, String>> ready() {
        try {
            jdbc.queryForObject("SELECT 1", Integer.class);
            return ResponseEntity.ok(Map.of("status", "ready"));
        } catch (Exception e) {
            Map<String, String> body = new LinkedHashMap<>();
            body.put("status", "not_ready");
            body.put("error", String.valueOf(e.getMessage()));
            return ResponseEntity.status(503).body(body);
        }
    }

    @GetMapping(value = "/metrics", produces = MediaType.TEXT_PLAIN_VALUE)
    public ResponseEntity<String> metrics() {
        PrometheusMeterRegistry registry = prometheus.getIfAvailable();
        if (registry == null) return ResponseEntity.status(503).body("# prometheus registry not configured\n");
        return ResponseEntity.ok(registry.scrape());
    }
}
