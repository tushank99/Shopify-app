package com.shopify.erp.orders;

/** A saga step failed after the order was accepted; compensation has already been attempted. Maps to HTTP 500. */
public class SagaFailedException extends RuntimeException {

    private final String orderId;
    private final String failedStep;

    public SagaFailedException(String orderId, String failedStep, String message) {
        super("Order " + orderId + " was cancelled: step " + failedStep + " failed (" + message + "); stock released");
        this.orderId = orderId;
        this.failedStep = failedStep;
    }

    public String getOrderId() { return orderId; }
    public String getFailedStep() { return failedStep; }
}
