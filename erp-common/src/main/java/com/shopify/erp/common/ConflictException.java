package com.shopify.erp.common;

/** The request is valid but conflicts with the current state (insufficient stock, wrong status...). Maps to HTTP 409. */
public class ConflictException extends RuntimeException {
    public ConflictException(String message) {
        super(message);
    }
}
