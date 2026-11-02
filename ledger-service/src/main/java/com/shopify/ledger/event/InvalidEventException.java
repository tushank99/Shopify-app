package com.shopify.ledger.event;

/**
 * The event can never become valid by retrying (malformed JSON, schema violation, inconsistent totals).
 * The listener excludes this type from retries, so it goes straight to the dead-letter topic.
 */
public class InvalidEventException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /** Low-cardinality code used as a metric tag: empty_payload | malformed_json | schema_violation | total_mismatch. */
    private final String reason;

    public InvalidEventException(String reason, String message) {
        super(message);
        this.reason = reason;
    }

    public InvalidEventException(String reason, String message, Throwable cause) {
        super(message, cause);
        this.reason = reason;
    }

    public String getReason() {
        return reason;
    }
}