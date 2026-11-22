package com.shopify.erp.common;

/** A referenced business object (SKU, order, purchase order...) does not exist. Maps to HTTP 404. */
public class NotFoundException extends RuntimeException {
    public NotFoundException(String message) {
        super(message);
    }
}
