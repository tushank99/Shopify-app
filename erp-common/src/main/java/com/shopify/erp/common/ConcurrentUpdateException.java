package com.shopify.erp.common;

/** Optimistic-lock retries were exhausted: another transaction kept changing the same row. Safe for the client to retry. */
public class ConcurrentUpdateException extends ConflictException {
    public ConcurrentUpdateException(String message) {
        super(message);
    }
}
