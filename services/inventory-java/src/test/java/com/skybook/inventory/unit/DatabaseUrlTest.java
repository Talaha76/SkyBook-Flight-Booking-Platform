package com.skybook.inventory.unit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.skybook.inventory.config.DatabaseUrl;
import org.junit.jupiter.api.Test;

class DatabaseUrlTest {

    @Test
    void convertsNodeStyleUrl() {
        DatabaseUrl u = DatabaseUrl.parse("postgres://postgres:s3cr%40t@postgres:5432/skybook_inventory?sslmode=require");
        assertThat(u.jdbcUrl()).isEqualTo("jdbc:postgresql://postgres:5432/skybook_inventory?sslmode=require");
        assertThat(u.username()).isEqualTo("postgres");
        assertThat(u.password()).isEqualTo("s3cr@t");
    }

    @Test
    void defaultsPortAndAcceptsPostgresqlScheme() {
        assertThat(DatabaseUrl.parse("postgresql://u:p@db/x").jdbcUrl()).isEqualTo("jdbc:postgresql://db:5432/x");
    }

    @Test
    void passesJdbcUrlsThrough() {
        DatabaseUrl u = DatabaseUrl.parse("jdbc:postgresql://localhost:5432/x?user=a&password=b");
        assertThat(u.jdbcUrl()).startsWith("jdbc:postgresql://");
        assertThat(u.username()).isNull();
    }

    @Test
    void rejectsGarbage() {
        assertThatThrownBy(() -> DatabaseUrl.parse("mysql://x")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DatabaseUrl.parse("")).isInstanceOf(IllegalArgumentException.class);
    }
}
