package com.shopify.erp.purchasing;

import com.shopify.erp.common.ConflictException;
import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.OneToMany;
import jakarta.persistence.OrderBy;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

/** DRAFT -> APPROVED -> RECEIVED, or CANCELLED from DRAFT/APPROVED. Receiving increases inventory on-hand. */
@Entity
@Table(name = "purchase_order")
public class PurchaseOrder {

    public enum Status { DRAFT, APPROVED, RECEIVED, CANCELLED }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, length = 120)
    private String supplier;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 16)
    private Status status = Status.DRAFT;

    @Version
    @Column(nullable = false)
    private long version;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @OneToMany(mappedBy = "purchaseOrder", cascade = CascadeType.ALL, orphanRemoval = true)
    @OrderBy("id ASC")
    private List<PurchaseOrderLine> lines = new ArrayList<>();

    protected PurchaseOrder() {
        // JPA
    }

    public PurchaseOrder(String supplier) {
        this.supplier = supplier;
    }

    @PrePersist
    void onCreate() {
        createdAt = Instant.now().truncatedTo(ChronoUnit.MICROS);
        updatedAt = createdAt;
    }

    @PreUpdate
    void onUpdate() {
        updatedAt = Instant.now().truncatedTo(ChronoUnit.MICROS);
    }

    public void addLine(String sku, int quantity, BigDecimal unitCost) {
        lines.add(new PurchaseOrderLine(this, sku, quantity, unitCost));
    }

    public void approve() {
        requireStatus("approve", Status.DRAFT);
        status = Status.APPROVED;
    }

    public void markReceived() {
        requireStatus("receive", Status.APPROVED);
        status = Status.RECEIVED;
    }

    public void cancel() {
        requireStatus("cancel", Status.DRAFT, Status.APPROVED);
        status = Status.CANCELLED;
    }

    public BigDecimal total() {
        return lines.stream().map(PurchaseOrderLine::lineTotal).reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    private void requireStatus(String action, Status... allowed) {
        for (Status s : allowed) {
            if (status == s) {
                return;
            }
        }
        throw new ConflictException("Cannot " + action + " purchase order " + id + " in status " + status);
    }

    public Long getId() { return id; }
    public String getSupplier() { return supplier; }
    public Status getStatus() { return status; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
    public List<PurchaseOrderLine> getLines() { return lines; }
}
