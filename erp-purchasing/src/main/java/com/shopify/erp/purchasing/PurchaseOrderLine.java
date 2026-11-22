package com.shopify.erp.purchasing;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;

import java.math.BigDecimal;

@Entity
@Table(name = "purchase_order_line")
public class PurchaseOrderLine {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "purchase_order_id", nullable = false)
    private PurchaseOrder purchaseOrder;

    @Column(nullable = false, length = 64)
    private String sku;

    @Column(nullable = false)
    private int quantity;

    @Column(name = "unit_cost", nullable = false, precision = 12, scale = 2)
    private BigDecimal unitCost;

    protected PurchaseOrderLine() {
        // JPA
    }

    PurchaseOrderLine(PurchaseOrder purchaseOrder, String sku, int quantity, BigDecimal unitCost) {
        this.purchaseOrder = purchaseOrder;
        this.sku = sku;
        this.quantity = quantity;
        this.unitCost = unitCost.setScale(2);
    }

    public BigDecimal lineTotal() {
        return unitCost.multiply(BigDecimal.valueOf(quantity));
    }

    public String getSku() { return sku; }
    public int getQuantity() { return quantity; }
    public BigDecimal getUnitCost() { return unitCost; }
}
