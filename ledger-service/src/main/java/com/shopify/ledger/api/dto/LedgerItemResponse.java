package com.shopify.ledger.api.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.shopify.ledger.domain.LedgerItem;

import java.math.BigDecimal;

public record LedgerItemResponse(
        int lineNo,
        String productId,
        int quantity,
        @JsonFormat(shape = JsonFormat.Shape.STRING) BigDecimal unitPrice,
        @JsonFormat(shape = JsonFormat.Shape.STRING) BigDecimal lineTotal) {

    public static LedgerItemResponse from(LedgerItem item) {
        return new LedgerItemResponse(
                item.getLineNo(),
                item.getProductId(),
                item.getQuantity(),
                item.getUnitPrice(),
                item.getLineTotal());
    }
}