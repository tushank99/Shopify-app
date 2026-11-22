package com.shopify.erp.orders;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/** Append-only audit trail of what the saga did for an order (also the evidence for compensation). */
@Entity
@Table(name = "saga_step")
public class SagaStep {

    public enum Outcome { SUCCEEDED, FAILED, COMPENSATED, COMPENSATION_REQUIRED }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "order_id", nullable = false, length = 24)
    private String orderId;

    @Column(nullable = false, length = 40)
    private String step;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 24)
    private Outcome outcome;

    @Column(length = 500)
    private String detail;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt = Instant.now().truncatedTo(ChronoUnit.MICROS);

    protected SagaStep() {
        // JPA
    }

    public SagaStep(String orderId, String step, Outcome outcome, String detail) {
        this.orderId = orderId;
        this.step = step;
        this.outcome = outcome;
        this.detail = detail == null ? null : detail.substring(0, Math.min(detail.length(), 500));
    }

    public Long getId() { return id; }
    public String getStep() { return step; }
    public Outcome getOutcome() { return outcome; }
    public String getDetail() { return detail; }
    public Instant getCreatedAt() { return createdAt; }
}
