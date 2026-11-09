package com.skybook.inventory.web;

import com.skybook.inventory.metrics.SkybookMetrics;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.servlet.HandlerMapping;

/**
 * Runs first on every request: reads x-request-id (or generates one), echoes it as X-Request-Id, puts it
 * in the logging MDC, records http_request_duration_seconds, and writes one access-log line, matching the
 * Node services' createApp() middleware.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class RequestIdFilter extends OncePerRequestFilter {
    private static final Logger log = LoggerFactory.getLogger("request");
    private final SkybookMetrics metrics;

    public RequestIdFilter(SkybookMetrics metrics) {
        this.metrics = metrics;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest req, HttpServletResponse res, FilterChain chain) throws ServletException, IOException {
        String incoming = req.getHeader("x-request-id");
        String requestId = incoming != null && !incoming.isBlank() && incoming.length() <= 128 ? incoming : UUID.randomUUID().toString();
        req.setAttribute("requestId", requestId);
        res.setHeader("X-Request-Id", requestId);
        MDC.put("requestId", requestId);
        long start = System.nanoTime();
        try {
            chain.doFilter(req, res);
        } finally {
            long nanos = System.nanoTime() - start;
            Object pattern = req.getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE);
            String route = pattern != null ? pattern.toString() : (res.getStatus() == 404 ? "unmatched" : req.getRequestURI());
            metrics.httpRequest(req.getMethod(), route, res.getStatus(), nanos);
            String path = req.getRequestURI();
            if (!"/metrics".equals(path) && !"/health".equals(path)) {
                log.info("request method={} path={} status={} ms={}", req.getMethod(), path, res.getStatus(), nanos / 1_000_000);
            }
            MDC.clear();
        }
    }
}
