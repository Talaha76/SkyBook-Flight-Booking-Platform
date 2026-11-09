package com.skybook.inventory.config;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

/**
 * Converts the Node-style DATABASE_URL (postgres://user:pass@host:5432/db?sslmode=require)
 * into the JDBC URL + credentials that the PostgreSQL JDBC driver expects.
 */
public record DatabaseUrl(String jdbcUrl, String username, String password) {

    public static DatabaseUrl parse(String url) {
        if (url == null || url.isBlank()) throw new IllegalArgumentException("DATABASE_URL is empty");
        if (url.startsWith("jdbc:")) return new DatabaseUrl(url, null, null);
        URI uri = URI.create(url.replaceFirst("^postgresql://", "postgres://"));
        if (!"postgres".equals(uri.getScheme())) throw new IllegalArgumentException("DATABASE_URL must start with postgres://");
        String user = null;
        String pass = null;
        if (uri.getRawUserInfo() != null) {
            String[] parts = uri.getRawUserInfo().split(":", 2);
            user = decode(parts[0]);
            pass = parts.length > 1 ? decode(parts[1]) : null;
        }
        int port = uri.getPort() == -1 ? 5432 : uri.getPort();
        String query = uri.getRawQuery() == null ? "" : "?" + uri.getRawQuery();
        return new DatabaseUrl("jdbc:postgresql://" + uri.getHost() + ":" + port + uri.getRawPath() + query, user, pass);
    }

    private static String decode(String s) {
        return URLDecoder.decode(s, StandardCharsets.UTF_8);
    }
}
