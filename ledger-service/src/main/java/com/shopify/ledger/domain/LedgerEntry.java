package com.shopify.ledger.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.Immutable;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Read model of a ledger row. @Immutable: Hibernate never issues UPDATEs for it.
 * All writes go through {@code LedgerEntryRepository#insertIgnore} (INSERT ... ON CONFLICT DO NOTHING),
 * so never call save() on this entity.
 */
@Entity
@Immutable
@Table(name = "ledger_entry")
public class LedgerEntry {

    @Id
    @Column(name = "event_id", nullable = false, updatable = false)
    private UUID eventId;

    @Column(name = "order_id", nullable = false, length = 24)
    private String orderId;

    @Column(name = "user_id", nullable = false, length = 24)
    private String userId;

    @Column(name = "event_type", nullable = false, length = 64)
    private String eventType;

    @Column(name = "total_price", nullable = false, precision = 14, scale = 2)
    private BigDecimal totalPrice;

    @Column(name = "currency", nullable = false, length = 3)
    private String currency;

    @Column(name = "order_created_at", nullable = false)
    private Instant orderCreatedAt;

    @Column(name = "ingested_at", nullable = false)
    private Instant ingestedAt;

    protected LedgerEntry() {
        // required by JPA
    }

    public UUID getEventId() {
        return eventId;
    }

    public String getOrderId() {
        return orderId;
    }

    public String getUserId() {
        return userId;
    }

    public String getEventType() {
        return eventType;
    }

    public BigDecimal getTotalPrice() {
        return totalPrice;
    }

    public String getCurrency() {
        return currency;
    }

    public Instant getOrderCreatedAt() {
        return orderCreatedAt;
    }

    public Instant getIngestedAt() {
        return ingestedAt;
    }
}