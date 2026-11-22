package com.shopify.erp.orders;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

public record OrderResponse(String id, String customerId, String status, String failureReason, String currency,
                            BigDecimal totalPrice, Instant createdAt, List<Line> lines) {

    public record Line(int lineNo, String sku, String productId, int quantity, BigDecimal unitPrice, BigDecimal lineTotal) {
    }

    static OrderResponse from(SalesOrder o) {
        return new OrderResponse(o.getId(), o.getCustomerId(), o.getStatus().name(), o.getFailureReason(),
                o.getCurrency(), o.getTotalPrice(), o.getCreatedAt(),
                o.getLines().stream()
                        .map(l -> new Line(l.getLineNo(), l.getSku(), l.getProductId(), l.getQuantity(),
                                l.getUnitPrice(), l.getLineTotal()))
                        .toList());
    }
}
