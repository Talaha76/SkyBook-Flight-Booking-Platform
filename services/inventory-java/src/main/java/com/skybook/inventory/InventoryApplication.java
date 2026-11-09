package com.skybook.inventory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * SkyBook Inventory service, Java/Spring Boot edition. A drop-in replacement for services/inventory (Node):
 * same Kafka topics and event envelope, same tables in skybook_inventory, same HTTP endpoints.
 */
@SpringBootApplication
@EnableScheduling
@ConfigurationPropertiesScan
public class InventoryApplication {
    public static void main(String[] args) {
        SpringApplication.run(InventoryApplication.class, args);
    }
}
