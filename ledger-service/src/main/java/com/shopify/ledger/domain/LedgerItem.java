package com.shopify.ledger.domain;

import jakarta.persistence.Column;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import org.hibernate.annotations.Immutable;

import java.math.BigDecimal;

/**
 * Read model of a ledger line. Rows are written by {@code LedgerItemRepository#insertItem}
 * in the same transaction as their ledger_entry.
 */
@Entity
@Immutable
@Table(name = "ledger_item")
public class LedgerItem {

    @EmbeddedId
    private LedgerItemId id;

    @Column(name = "product_id", nullable = false, length = 24)
    private String productId;

    @Column(name = "quantity", nullable = false)
    private int quantity;

    @Column(name = "unit_price", nullable = false, precision = 12, scale = 2)
    private BigDecimal unitPrice;

    @Column(name = "line_total", nullable = false, precision = 14, scale = 2)
    private BigDecimal lineTotal;

    protected LedgerItem() {
        // required by JPA
    }

    public LedgerItemId getId() {
        return id;
    }

    public int getLineNo() {
        return id.getLineNo();
    }

    public String getProductId() {
        return productId;
    }

    public int getQuantity() {
        return quantity;
    }

    public BigDecimal getUnitPrice() {
        return unitPrice;
    }

    public BigDecimal getLineTotal() {
        return lineTotal;
    }
}