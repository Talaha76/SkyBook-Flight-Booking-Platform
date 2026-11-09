package com.skybook.inventory.web;

import com.skybook.inventory.config.AppProperties;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Set;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Gateway-to-service trust (same rule as createApp({ internalToken }) in the Node services): every route
 * except /health, /ready and /metrics needs x-internal-token == INTERNAL_TOKEN, else 401. The caller's
 * identity comes from x-user-id / x-user-role, which only the gateway sets.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 10)
public class InternalAuthFilter extends OncePerRequestFilter {
    public static final String USER_ATTR = "skybook.user";
    private static final Set<String> OPEN = Set.of("/health", "/ready", "/metrics");

    private final String internalToken;

    public InternalAuthFilter(AppProperties props) {
        this.internalToken = props.internalToken();
    }

    public record User(String id, String role) {}

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return OPEN.contains(request.getRequestURI());
    }

    @Override
    protected void doFilterInternal(HttpServletRequest req, HttpServletResponse res, FilterChain chain) throws ServletException, IOException {
        if (internalToken == null || !internalToken.equals(req.getHeader("x-internal-token"))) {
            ApiErrors.write(res, 401, "missing or invalid internal token");
            return;
        }
        String id = req.getHeader("x-user-id");
        String role = req.getHeader("x-user-role");
        if (id != null && ("CUSTOMER".equals(role) || "AGENT".equals(role))) req.setAttribute(USER_ATTR, new User(id, role));
        chain.doFilter(req, res);
    }
}
