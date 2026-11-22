package com.shopify.erp.inventory;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Stock of one SKU.  available = onHand - reserved.
 *
 * Overselling is prevented twice:
 *  1. {@link #version} (@Version): two transactions that read the same row and both try to reserve cannot both
 *     commit; the loser gets an OptimisticLockingFailureException and is retried against fresh data.
 *  2. A database CHECK (reserved <= on_hand) is the last line of defence if the code ever has a bug.
 */
@Entity
@Table(name = "inventory_item")
public class InventoryItem {

    @Id
    @Column(length = 64)
    private String sku;

    /** 24-hex id used in events/ledger. Can be set to a Mongo product _id to link the two catalogs. */
    @Column(name = "product_id", nullable = false, unique = true, length = 24)
    private String productId;

    @Column(nullable = false, length = 200)
    private String name;

    @Column(name = "unit_price", nullable = false, precision = 12, scale = 2)
    private BigDecimal unitPrice;

    /** Standard cost, used when a purchase order is suggested. */
    @Column(name = "unit_cost", nullable = false, precision = 12, scale = 2)
    private BigDecimal unitCost;

    @Column(name = "on_hand", nullable = false)
    private int onHand;

    @Column(nullable = false)
    private int reserved;

    @Column(name = "reorder_point", nullable = false)
    private int reorderPoint;

    @Column(name = "reorder_qty", nullable = false)
    private int reorderQty;

    @Version
    @Column(nullable = false)
    private long version;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected InventoryItem() {
        // JPA
    }

    public InventoryItem(String sku, String productId, String name, BigDecimal unitPrice, BigDecimal unitCost,
                         int onHand, int reorderPoint, int reorderQty) {
        this.sku = sku;
        this.productId = productId;
        this.name = name;
        this.unitPrice = unitPrice;
        this.unitCost = unitCost;
        this.onHand = onHand;
        this.reorderPoint = reorderPoint;
        this.reorderQty = reorderQty;
    }

    @PrePersist
    void onCreate() {
        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
        createdAt = now;
        updatedAt = now;
    }

    @PreUpdate
    void onUpdate() {
        updatedAt = Instant.now().truncatedTo(ChronoUnit.MICROS);
    }

    public int available() {
        return onHand - reserved;
    }

    public boolean needsReorder() {
        return reorderQty > 0 && available() <= reorderPoint;
    }

    /** Soft-allocates stock for an order. Nothing physically leaves the warehouse yet. */
    public void reserve(int quantity) {
        requirePositive(quantity);
        if (available() < quantity) {
            throw new InsufficientStockException(sku, quantity, available());
        }
        reserved += quantity;
    }

    /** Gives a reservation back (order cancelled / saga compensation). */
    public void release(int quantity) {
        requirePositive(quantity);
        if (quantity > reserved) {
            throw new IllegalStateException("Cannot release " + quantity + " of " + sku + ": only " + reserved + " reserved");
        }
        reserved -= quantity;
    }

    /** Goods physically ship: the reservation turns into a real decrease of on-hand stock. */
    public void commitShipment(int quantity) {
        requirePositive(quantity);
        if (quantity > reserved) {
            throw new IllegalStateException("Cannot ship " + quantity + " of " + sku + ": only " + reserved + " reserved");
        }
        reserved -= quantity;
        onHand -= quantity;
    }

    /** Goods receipt (purchase order received). */
    public void receive(int quantity) {
        requirePositive(quantity);
        onHand += quantity;
    }

    private static void requirePositive(int quantity) {
        if (quantity <= 0) {
            throw new IllegalArgumentException("quantity must be > 0");
        }
    }

    public String getSku() { return sku; }
    public String getProductId() { return productId; }
    public String getName() { return name; }
    public BigDecimal getUnitPrice() { return unitPrice; }
    public BigDecimal getUnitCost() { return unitCost; }
    public int getOnHand() { return onHand; }
    public int getReserved() { return reserved; }
    public int getReorderPoint() { return reorderPoint; }
    public int getReorderQty() { return reorderQty; }
    public long getVersion() { return version; }
    public Instant getUpdatedAt() { return updatedAt; }
}
