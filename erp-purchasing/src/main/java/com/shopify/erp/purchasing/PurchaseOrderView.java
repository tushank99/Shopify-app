package com.shopify.erp.purchasing;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/** Read model returned by the API (entities never leave the transaction: open-in-view is off). */
public record PurchaseOrderView(Long id, String supplier, String status, BigDecimal total, Instant createdAt,
                                List<LineView> lines) {

    public record LineView(String sku, int quantity, BigDecimal unitCost, BigDecimal lineTotal) {
    }

    static PurchaseOrderView from(PurchaseOrder po) {
        return new PurchaseOrderView(po.getId(), po.getSupplier(), po.getStatus().name(), po.total(), po.getCreatedAt(),
                po.getLines().stream()
                        .map(l -> new LineView(l.getSku(), l.getQuantity(), l.getUnitCost(), l.lineTotal()))
                        .toList());
    }
}
