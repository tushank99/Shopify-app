package com.shopify.erp.inventory;

import com.shopify.erp.common.ConflictException;

public class InsufficientStockException extends ConflictException {
    public InsufficientStockException(String sku, int requested, int available) {
        super("Insufficient stock for SKU " + sku + ": requested " + requested + ", available " + available);
    }
}
