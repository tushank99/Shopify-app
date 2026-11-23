package com.shopify.erp.it;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** S-7 / I-6: low stock -> reorder suggestion -> approve -> receive; receiving twice is rejected and not applied. */
class PurchasingIntegrationTest extends AbstractIntegrationTest {

    @Test
    void lowStockBecomesAPurchaseOrderThatRestocksExactlyOnce() {
        String sku = uniqueSku("BOLT");
        createItem(sku, 10, "5.00", 3, 50);

        // sell 8 -> available 2 <= reorder point 3
        assertThat(placeOrder(null, List.of(line(sku, 8))).getStatusCode()).isEqualTo(HttpStatus.CREATED);
        List<?> low = http.getForObject("/api/inventory/low-stock", List.class);
        assertThat(low).extracting(i -> String.valueOf(((Map<?, ?>) i).get("sku"))).contains(sku);

        ResponseEntity<Map> suggested = http.postForEntity("/api/purchase-orders/reorder-suggestions",
                Map.of("supplier", "Acme Fasteners"), Map.class);
        assertThat(suggested.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        Map<?, ?> po = suggested.getBody();
        assertThat(po.get("status")).isEqualTo("DRAFT");
        Map<?, ?> poLine = (Map<?, ?>) ((List<?>) po.get("lines")).stream()
                .filter(l -> sku.equals(((Map<?, ?>) l).get("sku"))).findFirst().orElseThrow();
        assertThat(poLine.get("quantity")).isEqualTo(50);

        Object id = po.get("id");
        // receiving before approval is a 409 and changes nothing
        assertThat(http.postForEntity("/api/purchase-orders/" + id + "/receive", null, Map.class).getStatusCode())
                .isEqualTo(HttpStatus.CONFLICT);
        assertThat(item(sku).get("onHand")).isEqualTo(2);

        assertThat(http.postForEntity("/api/purchase-orders/" + id + "/approve", null, Map.class).getBody().get("status"))
                .isEqualTo("APPROVED");
        assertThat(http.postForEntity("/api/purchase-orders/" + id + "/receive", null, Map.class).getBody().get("status"))
                .isEqualTo("RECEIVED");
        assertThat(item(sku).get("onHand")).isEqualTo(52);

        // second receipt and cancelling a received PO are both rejected; stock is not added twice
        assertThat(http.postForEntity("/api/purchase-orders/" + id + "/receive", null, Map.class).getStatusCode())
                .isEqualTo(HttpStatus.CONFLICT);
        assertThat(http.postForEntity("/api/purchase-orders/" + id + "/cancel", null, Map.class).getStatusCode())
                .isEqualTo(HttpStatus.CONFLICT);
        assertThat(item(sku).get("onHand")).isEqualTo(52);
    }

    @Test
    void manualPurchaseOrderForAnUnknownSkuIs404() {
        ResponseEntity<Map> response = http.postForEntity("/api/purchase-orders", Map.of(
                "supplier", "Acme",
                "lines", List.of(Map.of("sku", uniqueSku("GHOST"), "quantity", 1, "unitCost", 1.00))), Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void unknownPurchaseOrderIs404() {
        assertThat(http.getForEntity("/api/purchase-orders/987654321", Map.class).getStatusCode())
                .isEqualTo(HttpStatus.NOT_FOUND);
    }
}
