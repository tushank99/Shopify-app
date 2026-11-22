package com.shopify.erp.orders;

import java.util.Set;

/**
 * Order lifecycle driven by the saga:
 * CREATED -> STOCK_RESERVED -> LEDGER_POSTED -> SHIPPED, and CANCELLED from any non-final state (compensation).
 */
public enum OrderStatus {
    CREATED, STOCK_RESERVED, LEDGER_POSTED, SHIPPED, CANCELLED;

    public Set<OrderStatus> next() {
        return switch (this) {
            case CREATED -> Set.of(STOCK_RESERVED, CANCELLED);
            case STOCK_RESERVED -> Set.of(LEDGER_POSTED, CANCELLED);
            case LEDGER_POSTED -> Set.of(SHIPPED, CANCELLED);
            case SHIPPED, CANCELLED -> Set.of();
        };
    }
}
