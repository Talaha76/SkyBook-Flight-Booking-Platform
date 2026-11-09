package com.skybook.inventory.web;

import jakarta.servlet.http.HttpServletRequest;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/** GET /inventory/{flightId}: agents only; same response shape as the Node service. */
@RestController
public class InventoryController {
    private final JdbcTemplate jdbc;

    public InventoryController(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @GetMapping("/inventory/{flightId}")
    public Map<String, Object> get(@PathVariable String flightId, HttpServletRequest req) {
        InternalAuthFilter.User user = (InternalAuthFilter.User) req.getAttribute(InternalAuthFilter.USER_ATTR);
        if (user == null) throw new ApiErrors.HttpError(401, "authentication required");
        if (!"AGENT".equals(user.role())) throw new ApiErrors.HttpError(403, "agents only");
        UUID id;
        try {
            id = UUID.fromString(flightId);
            if (flightId.length() != 36) throw new IllegalArgumentException();
        } catch (IllegalArgumentException e) {
            throw new ApiErrors.HttpError(400, "flightId must be a UUID");
        }
        List<Map<String, Object>> rows = jdbc.queryForList("SELECT * FROM seat_inventory WHERE flight_id = ?", id);
        if (rows.isEmpty()) throw new ApiErrors.HttpError(404, "flight not found in inventory");
        Map<String, Object> holds = new LinkedHashMap<>();
        for (Map<String, Object> h : jdbc.queryForList("SELECT status, count(*)::int AS n FROM seat_holds WHERE flight_id = ? GROUP BY status", id)) {
            holds.put((String) h.get("status"), ((Number) h.get("n")).intValue());
        }
        Map<String, Object> r = rows.get(0);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("flightId", flightId);
        body.put("totalSeats", ((Number) r.get("total_seats")).intValue());
        body.put("available", ((Number) r.get("available")).intValue());
        body.put("priceCents", ((Number) r.get("price_cents")).intValue());
        body.put("holds", holds);
        return body;
    }
}
