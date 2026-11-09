package com.skybook.inventory.config;

import com.zaxxer.hikari.HikariDataSource;
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Builds the connection pool from DATABASE_URL so Compose/Helm need no Java-specific variables. */
@Configuration
public class DataSourceConfig {

    @Bean
    public DataSource dataSource(AppProperties props, @Value("${skybook.db-pool-max:10}") int poolMax) {
        DatabaseUrl url = DatabaseUrl.parse(props.databaseUrl());
        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl(url.jdbcUrl());
        if (url.username() != null) ds.setUsername(url.username());
        if (url.password() != null) ds.setPassword(url.password());
        ds.setMaximumPoolSize(poolMax);
        ds.setPoolName("inventory");
        return ds;
    }
}
