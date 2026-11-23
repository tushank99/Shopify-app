package com.shopify.erp.it;

import org.junit.jupiter.api.AfterEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MySQLContainer;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base for the ERP integration tests: the real Spring Boot app on a random port, a real MySQL 8.4 (Testcontainers,
 * one container for the whole run) with the real Flyway migration, and an in-process Kafka broker.
 *
 * Tests never clean the database; each one uses its own random SKUs, so they are independent and the shared data
 * makes the invariant checks in {@link #databaseIsConsistent()} stronger (they run over every row after every test).
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
                "erp.outbox.poll-ms=200"
        })
// KRaft (no ZooKeeper): the ZooKeeper-mode broker could hang on JVM shutdown, which killed the fork mid-way and
// corrupted the JaCoCo coverage file.
@EmbeddedKafka(kraft = true, partitions = 1, topics = "order.placed")
public abstract class AbstractIntegrationTest {

    static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4").withDatabaseName("erp_db");

    static {
        MYSQL.start(); // singleton: shared by every test class and Spring context in this JVM
    }

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", MYSQL::getJdbcUrl);
        registry.add("spring.datasource.username", MYSQL::getUsername);
        registry.add("spring.datasource.password", MYSQL::getPassword);
    }

    protected static final String CUSTOMER = "c0ffee00c0ffee00c0ffee00";

    @Autowired
    protected TestRestTemplate http;
    @Autowired
    protected JdbcTemplate jdbc;

    // ------------------------------------------------------------------ data integrity (runs after EVERY test)

    /** SQL checks that must hold for the whole database once no saga is in flight. Each query counts violations. */
    static final Map<String, String> INVARIANTS = Map.of(
            "stock never negative and reserved <= on_hand",
            "SELECT COUNT(*) FROM inventory_item WHERE on_hand < 0 OR reserved < 0 OR reserved > on_hand",

            "inventory.reserved equals the sum of open reservations",
            "SELECT COUNT(*) FROM inventory_item i WHERE i.reserved <> "
                    + "(SELECT COALESCE(SUM(r.quantity), 0) FROM stock_reservation r WHERE r.sku = i.sku AND r.status = 'RESERVED')",

            "every SHIPPED order has exactly one live ledger event (no duplicate, no missing posting)",
            "SELECT COUNT(*) FROM sales_order o WHERE o.status = 'SHIPPED' AND "
                    + "(SELECT COUNT(*) FROM outbox_event e WHERE e.aggregate_id = o.id AND e.status <> 'CANCELLED') <> 1",

            "at most one ledger event per order",
            "SELECT COUNT(*) FROM (SELECT aggregate_id FROM outbox_event GROUP BY aggregate_id HAVING COUNT(*) > 1) d",

            "a CANCELLED order holds no stock and has no live ledger event",
            "SELECT (SELECT COUNT(*) FROM stock_reservation r JOIN sales_order o ON o.id = r.order_id "
                    + "        WHERE o.status = 'CANCELLED' AND r.status <> 'RELEASED')"
                    + "   + (SELECT COUNT(*) FROM outbox_event e JOIN sales_order o ON o.id = e.aggregate_id "
                    + "        WHERE o.status = 'CANCELLED' AND e.status = 'PENDING')",

            "order total equals the sum of its lines",
            "SELECT COUNT(*) FROM sales_order o WHERE o.total_price <> "
                    + "(SELECT COALESCE(SUM(l.line_total), 0) FROM sales_order_line l WHERE l.order_id = o.id)",

            "shipped quantity per line equals the committed reservation",
            "SELECT COUNT(*) FROM sales_order_line l JOIN sales_order o ON o.id = l.order_id WHERE o.status = 'SHIPPED' "
                    + "AND l.quantity <> (SELECT COALESCE(SUM(r.quantity), 0) FROM stock_reservation r "
                    + "                   WHERE r.order_id = l.order_id AND r.sku = l.sku AND r.status = 'COMMITTED')",

            "line total = unit price x quantity",
            "SELECT COUNT(*) FROM sales_order_line WHERE line_total <> unit_price * quantity");

    @AfterEach
    void databaseIsConsistent() {
        List<String> violated = new ArrayList<>();
        INVARIANTS.forEach((name, sql) -> {
            Long count = jdbc.queryForObject(sql, Long.class);
            if (count == null || count != 0) {
                violated.add(name + " (" + count + " rows)");
            }
        });
        assertThat(violated).as("database invariants").isEmpty();
    }

    // ------------------------------------------------------------------ API helpers

    protected static String uniqueSku(String prefix) {
        return prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    protected ResponseEntity<Map> createItem(String sku, int stock, String price, int reorderPoint, int reorderQty) {
        return http.postForEntity("/api/inventory/items", Map.of(
                "sku", sku, "name", "Item " + sku,
                "unitPrice", new BigDecimal(price), "unitCost", new BigDecimal("1.00"),
                "initialStock", stock, "reorderPoint", reorderPoint, "reorderQty", reorderQty), Map.class);
    }

    protected ResponseEntity<Map> placeOrder(String idempotencyKey, List<Map<String, Object>> lines) {
        HttpHeaders headers = new HttpHeaders();
        if (idempotencyKey != null) {
            headers.set("Idempotency-Key", idempotencyKey);
        }
        return http.postForEntity("/api/orders",
                new HttpEntity<>(Map.of("customerId", CUSTOMER, "lines", lines), headers), Map.class);
    }

    protected static Map<String, Object> line(String sku, int quantity) {
        return Map.of("sku", sku, "quantity", quantity);
    }

    protected Map<?, ?> item(String sku) {
        return http.getForObject("/api/inventory/items/" + sku, Map.class);
    }

    protected static BigDecimal money(Object jsonNumber) {
        return new BigDecimal(String.valueOf(jsonNumber));
    }
}
