package com.shopify.erp.it;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** S-1, S-2, S-3 and the error contract, through the real HTTP API. */
class OrderApiIntegrationTest extends AbstractIntegrationTest {

    @Test
    void s1_orderOfTwoSkusShipsWithCorrectTotalsAndReducesStock() {
        String a = uniqueSku("TSHIRT");
        String b = uniqueSku("CAP");
        assertThat(createItem(a, 10, "499.00", 0, 0).getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(createItem(b, 5, "199.50", 0, 0).getStatusCode()).isEqualTo(HttpStatus.CREATED);

        ResponseEntity<Map> response = placeOrder(null, List.of(line(a, 2), line(b, 1)));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        Map<?, ?> order = response.getBody();
        assertThat(order.get("status")).isEqualTo("SHIPPED");
        assertThat(money(order.get("totalPrice"))).isEqualByComparingTo("1197.50");
        assertThat(order.get("currency")).isEqualTo("INR");
        assertThat((List<?>) order.get("lines")).hasSize(2);

        assertThat(item(a).get("onHand")).isEqualTo(8);
        assertThat(item(b).get("onHand")).isEqualTo(4);
        assertThat(item(a).get("reserved")).isEqualTo(0);

        List<?> saga = http.getForObject("/api/orders/" + order.get("id") + "/saga", List.class);
        assertThat(saga).extracting(s -> String.valueOf(((Map<?, ?>) s).get("step")))
                .containsExactly("RESERVE_STOCK", "POST_LEDGER", "SHIP");
    }

    @Test
    void s1_priceIsSnapshottedOnTheOrder() {
        String sku = uniqueSku("MUG");
        createItem(sku, 10, "100.00", 0, 0);
        Map<?, ?> order = placeOrder(null, List.of(line(sku, 1))).getBody();

        // change the price afterwards: the existing order must not move
        jdbc.update("UPDATE inventory_item SET unit_price = 999.00 WHERE sku = ?", sku);

        Map<?, ?> again = http.getForObject("/api/orders/" + order.get("id"), Map.class);
        assertThat(money(again.get("totalPrice"))).isEqualByComparingTo("100.00");
    }

    @Test
    void s2_sameIdempotencyKeyReturnsTheSameOrderAndReducesStockOnce() {
        String sku = uniqueSku("SOCKS");
        createItem(sku, 10, "50.00", 0, 0);
        String key = "retry-" + sku;

        ResponseEntity<Map> first = placeOrder(key, List.of(line(sku, 3)));
        ResponseEntity<Map> second = placeOrder(key, List.of(line(sku, 3)));

        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(second.getBody().get("id")).isEqualTo(first.getBody().get("id"));
        assertThat(item(sku).get("onHand")).isEqualTo(7);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM sales_order WHERE idempotency_key = ?", Long.class, key))
                .isEqualTo(1L);
    }

    @Test
    void s3_orderingMoreThanAvailableIs409AndReservesNothing() {
        String a = uniqueSku("JEANS");
        String b = uniqueSku("BELT");
        createItem(a, 10, "999.00", 0, 0);
        createItem(b, 1, "299.00", 0, 0);

        ResponseEntity<Map> response = placeOrder(null, List.of(line(a, 2), line(b, 2)));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat((String) response.getBody().get("detail")).contains("Insufficient stock").contains(b);
        // all-or-nothing: the line that WAS available is not left reserved either
        assertThat(item(a).get("reserved")).isEqualTo(0);
        assertThat(item(b).get("reserved")).isEqualTo(0);
        assertThat(jdbc.queryForObject(
                "SELECT status FROM sales_order o JOIN sales_order_line l ON l.order_id = o.id WHERE l.sku = ?",
                String.class, b)).isEqualTo("CANCELLED");
    }

    @Test
    void unknownSkuIs404AndCreatesNoOrder() {
        String ghost = uniqueSku("GHOST");

        ResponseEntity<Map> response = placeOrder(null, List.of(line(ghost, 1)));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM sales_order_line WHERE sku = ?", Long.class, ghost)).isZero();
    }

    @Test
    void invalidRequestsAre400ProblemDetailsWithFieldErrors() {
        ResponseEntity<Map> badCustomer = http.postForEntity("/api/orders",
                Map.of("customerId", "not-hex", "lines", List.of(line("X", 1))), Map.class);
        assertThat(badCustomer.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(((Map<?, ?>) badCustomer.getBody().get("errors")).keySet()).map(String::valueOf).contains("customerId");

        ResponseEntity<Map> zeroQty = placeOrder(null, List.of(line("X", 0)));
        assertThat(zeroQty.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(((Map<?, ?>) zeroQty.getBody().get("errors")).keySet()).map(String::valueOf)
                .contains("lines[0].quantity");

        ResponseEntity<Map> longKey = placeOrder("k".repeat(65), List.of(line("X", 1)));
        assertThat(longKey.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(((Map<?, ?>) longKey.getBody().get("errors")).keySet()).map(String::valueOf)
                .contains("Idempotency-Key");

        ResponseEntity<Map> noLines = placeOrder(null, List.of());
        assertThat(noLines.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);

        ResponseEntity<Map> badSku = createItem("has space", 1, "1.00", 0, 0);
        assertThat(badSku.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void duplicateSkuIs409() {
        String sku = uniqueSku("DUP");
        createItem(sku, 1, "1.00", 0, 0);

        assertThat(createItem(sku, 1, "1.00", 0, 0).getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
    }

    @Test
    void unknownOrderIs404() {
        ResponseEntity<Map> response = http.getForEntity("/api/orders/0123456789abcdef01234567", Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(response.getHeaders().getContentType().toString()).contains("problem+json");
    }

    @Test
    void openApiDocumentListsEveryResource() {
        Map<?, ?> doc = http.getForObject("/v3/api-docs", Map.class);

        assertThat(((Map<?, ?>) doc.get("paths")).keySet()).map(String::valueOf).contains(
                "/api/orders", "/api/orders/{orderId}", "/api/orders/{orderId}/saga",
                "/api/inventory/items", "/api/inventory/low-stock",
                "/api/purchase-orders", "/api/purchase-orders/{id}/receive");
    }

    @Test
    void readinessProbeIsUpWithTheDatabase() {
        Map<?, ?> readiness = http.getForObject("/actuator/health/readiness", Map.class);

        assertThat(readiness.get("status")).isEqualTo("UP");
    }
}
