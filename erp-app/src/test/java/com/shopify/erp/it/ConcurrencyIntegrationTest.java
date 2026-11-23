package com.shopify.erp.it;

import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** I-2 (no overselling under contention) and I-5 (concurrent duplicate Idempotency-Key). Real MySQL, real HTTP. */
class ConcurrencyIntegrationTest extends AbstractIntegrationTest {

    /** Fires all calls at the same instant (latch) and returns every response. */
    private List<ResponseEntity<Map>> concurrently(int n, Callable<ResponseEntity<Map>> call) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(n);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<ResponseEntity<Map>>> futures = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    return call.call();
                }));
            }
            start.countDown();
            List<ResponseEntity<Map>> responses = new ArrayList<>();
            for (Future<ResponseEntity<Map>> f : futures) {
                responses.add(f.get(60, TimeUnit.SECONDS));
            }
            return responses;
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void i2_twentyBuyersForFiveUnitsNeverOversell() throws Exception {
        String sku = uniqueSku("FLASH");
        createItem(sku, 5, "999.00", 0, 0);

        List<ResponseEntity<Map>> responses = concurrently(20, () -> placeOrder(null, List.of(line(sku, 1))));

        long shipped = responses.stream().filter(r -> r.getStatusCode().value() == 201).count();
        long rejected = responses.stream().filter(r -> r.getStatusCode().value() == 409).count();
        assertThat(shipped + rejected).as("only 201 or 409, never a 500").isEqualTo(20);
        assertThat(shipped).isEqualTo(5);

        Map<?, ?> item = item(sku);
        assertThat(item.get("onHand")).isEqualTo(0);
        assertThat(item.get("reserved")).isEqualTo(0);
        assertThat(jdbc.queryForObject(
                "SELECT COALESCE(SUM(quantity), 0) FROM stock_reservation WHERE sku = ? AND status = 'COMMITTED'",
                Long.class, sku)).isEqualTo(5L);
    }

    @Test
    void i5_concurrentRequestsWithTheSameKeyCreateOneOrder() throws Exception {
        String sku = uniqueSku("IDEM");
        createItem(sku, 100, "10.00", 0, 0);
        String key = "double-click-" + sku;

        List<ResponseEntity<Map>> responses = concurrently(8, () -> placeOrder(key, List.of(line(sku, 2))));

        assertThat(responses).allSatisfy(r -> assertThat(r.getStatusCode().value()).isEqualTo(201));
        assertThat(responses.stream().map(r -> r.getBody().get("id")).distinct()).hasSize(1);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM sales_order WHERE idempotency_key = ?", Long.class, key))
                .isEqualTo(1L);
        assertThat(item(sku).get("onHand")).isEqualTo(98);
    }
}
