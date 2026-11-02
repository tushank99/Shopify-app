package com.shopify.ledger.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * order.placed v1 (contracts/order.placed.v1.schema.json).
 * Instances are only created AFTER the raw JSON passed schema validation, so every field is non-null.
 */
public record OrderPlacedEvent(
        UUID eventId,
        String eventType,
        int schemaVersion,
        String orderId,
        String userId,
        List<Item> items,
        BigDecimal totalPrice,
        String currency,
        Instant createdAt) {

    public record Item(String productId, int quantity, BigDecimal unitPrice) {

        public BigDecimal lineTotal() {
            return unitPrice.multiply(BigDecimal.valueOf(quantity));
        }
    }
}