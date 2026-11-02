package com.shopify.ledger.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

@Embeddable
public class LedgerItemId implements Serializable {

    private static final long serialVersionUID = 1L;

    @Column(name = "event_id", nullable = false)
    private UUID eventId;

    @Column(name = "line_no", nullable = false)
    private int lineNo;

    protected LedgerItemId() {
        // required by JPA
    }

    public LedgerItemId(UUID eventId, int lineNo) {
        this.eventId = eventId;
        this.lineNo = lineNo;
    }

    public UUID getEventId() {
        return eventId;
    }

    public int getLineNo() {
        return lineNo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LedgerItemId other)) {
            return false;
        }
        return lineNo == other.lineNo && Objects.equals(eventId, other.eventId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, lineNo);
    }
}