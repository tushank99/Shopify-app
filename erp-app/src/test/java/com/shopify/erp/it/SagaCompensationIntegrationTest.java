package com.shopify.erp.it;

import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * S-6 / I-3: force the SHIP step to fail (own Spring context). The order must end CANCELLED, the stock released, the
 * unpublished ledger event cancelled, and the saga trail must show the compensation. The relay is off in this
 * context so "cancel the pending event" is deterministic.
 */
@Order(1) // see src/test/resources/junit-platform.properties
@DirtiesContext // only this class uses this context: close it now instead of at JVM exit
@TestPropertySource(properties = {
        "erp.saga.simulate-failure-at=SHIP",
        "erp.outbox.enabled=false"
})
class SagaCompensationIntegrationTest extends AbstractIntegrationTest {

    @Test
    void shipFailureIsFullyCompensated() {
        String sku = uniqueSku("SOFA");
        createItem(sku, 5, "15000.00", 0, 0);

        ResponseEntity<Map> response = placeOrder(null, List.of(line(sku, 2)));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat(response.getBody().get("failedStep")).isEqualTo("SHIP");
        String orderId = (String) response.getBody().get("orderId");

        Map<?, ?> order = http.getForObject("/api/orders/" + orderId, Map.class);
        assertThat(order.get("status")).isEqualTo("CANCELLED");
        assertThat((String) order.get("failureReason")).contains("SHIP");

        Map<?, ?> item = item(sku);
        assertThat(item.get("onHand")).isEqualTo(5);
        assertThat(item.get("reserved")).isEqualTo(0);

        assertThat(jdbc.queryForObject("SELECT status FROM outbox_event WHERE aggregate_id = ?", String.class, orderId))
                .isEqualTo("CANCELLED");

        List<?> saga = http.getForObject("/api/orders/" + orderId + "/saga", List.class);
        assertThat(saga).extracting(s -> ((Map<?, ?>) s).get("step") + ":" + ((Map<?, ?>) s).get("outcome"))
                .containsExactly(
                        "RESERVE_STOCK:SUCCEEDED",
                        "POST_LEDGER:SUCCEEDED",
                        "SHIP:FAILED",
                        "RELEASE_STOCK:COMPENSATED",
                        "CANCEL_LEDGER_EVENT:COMPENSATED");
    }
}
