package com.shopify.erp.orders;

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
@Table(name = "sales_order_line")
public class SalesOrderLine {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "order_id", nullable = false)
    private SalesOrder order;

    @Column(name = "line_no", nullable = false)
    private int lineNo;

    @Column(nullable = false, length = 64)
    private String sku;

    @Column(name = "product_id", nullable = false, length = 24)
    private String productId;

    @Column(nullable = false)
    private int quantity;

    /** Price snapshot at order time: later price changes never alter an existing order. */
    @Column(name = "unit_price", nullable = false, precision = 12, scale = 2)
    private BigDecimal unitPrice;

    @Column(name = "line_total", nullable = false, precision = 14, scale = 2)
    private BigDecimal lineTotal;

    protected SalesOrderLine() {
        // JPA
    }

    SalesOrderLine(SalesOrder order, int lineNo, String sku, String productId, int quantity, BigDecimal unitPrice) {
        this.order = order;
        this.lineNo = lineNo;
        this.sku = sku;
        this.productId = productId;
        this.quantity = quantity;
        this.unitPrice = unitPrice.setScale(2);
        this.lineTotal = this.unitPrice.multiply(BigDecimal.valueOf(quantity));
    }

    public int getLineNo() { return lineNo; }
    public String getSku() { return sku; }
    public String getProductId() { return productId; }
    public int getQuantity() { return quantity; }
    public BigDecimal getUnitPrice() { return unitPrice; }
    public BigDecimal getLineTotal() { return lineTotal; }
}
