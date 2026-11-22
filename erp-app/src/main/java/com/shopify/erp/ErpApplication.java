package com.shopify.erp;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Entry point. Component scan, entity scan and repository scan all start at com.shopify.erp, so the domain
 * modules (erp-inventory, erp-purchasing, erp-orders) are picked up from their jars automatically.
 */
@SpringBootApplication
@EnableScheduling
public class ErpApplication {

    public static void main(String[] args) {
        SpringApplication.run(ErpApplication.class, args);
    }
}
