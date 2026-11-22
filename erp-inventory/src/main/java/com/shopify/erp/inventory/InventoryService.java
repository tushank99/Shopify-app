package com.shopify.erp.inventory;

import com.shopify.erp.common.ConflictException;
import com.shopify.erp.common.NotFoundException;
import com.shopify.erp.common.ObjectIds;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Methods are @Transactional(REQUIRED): when the order saga calls them inside its own transaction they join it,
 * so "reserve stock + update order status" commit or roll back together.
 *
 * Optimistic-lock conflicts surface as OptimisticLockingFailureException. They are NOT retried here (a failed
 * transaction cannot be retried from inside itself); the caller wraps the transaction in OptimisticRetry.
 */
@Service
public class InventoryService {

    public record CreateItem(String sku, String productId, String name, BigDecimal unitPrice, BigDecimal unitCost,
                             int initialStock, int reorderPoint, int reorderQty) {
    }

    private final InventoryItemRepository items;
    private final StockReservationRepository reservations;

    public InventoryService(InventoryItemRepository items, StockReservationRepository reservations) {
        this.items = items;
        this.reservations = reservations;
    }

    @Transactional
    public InventoryItem create(CreateItem cmd) {
        if (items.existsById(cmd.sku())) {
            throw new ConflictException("SKU already exists: " + cmd.sku());
        }
        String productId = cmd.productId() != null ? cmd.productId() : ObjectIds.newId();
        if (items.existsByProductId(productId)) {
            throw new ConflictException("productId already used by another SKU: " + productId);
        }
        return items.saveAndFlush(new InventoryItem(cmd.sku(), productId, cmd.name(),
                cmd.unitPrice().setScale(2), cmd.unitCost().setScale(2),
                cmd.initialStock(), cmd.reorderPoint(), cmd.reorderQty()));
    }

    @Transactional(readOnly = true)
    public InventoryItem get(String sku) {
        return items.findById(sku).orElseThrow(() -> new NotFoundException("Unknown SKU " + sku));
    }

    @Transactional(readOnly = true)
    public List<InventoryItem> list() {
        return items.findAll();
    }

    @Transactional(readOnly = true)
    public List<InventoryItem> lowStock() {
        return items.findLowStock();
    }

    /** Loads items for pricing; fails on the first unknown SKU. Keeps the caller's order. */
    @Transactional(readOnly = true)
    public Map<String, InventoryItem> findAllOrFail(Collection<String> skus) {
        Map<String, InventoryItem> found = new LinkedHashMap<>();
        for (String sku : skus) {
            found.put(sku, items.findById(sku).orElseThrow(() -> new NotFoundException("Unknown SKU " + sku)));
        }
        return found;
    }

    @Transactional(readOnly = true)
    public List<StockReservation> reservationsFor(String orderId) {
        return reservations.findByOrderId(orderId);
    }

    /**
     * Reserves every line of an order, all-or-nothing (one transaction). Idempotent per order: a second call for
     * an order that already has reservations does nothing. SKUs are processed in sorted order.
     */
    @Transactional
    public void reserve(String orderId, Map<String, Integer> qtyBySku) {
        if (reservations.existsByOrderId(orderId)) {
            return;
        }
        for (Map.Entry<String, Integer> line : new TreeMap<>(qtyBySku).entrySet()) {
            InventoryItem item = items.findById(line.getKey())
                    .orElseThrow(() -> new NotFoundException("Unknown SKU " + line.getKey()));
            item.reserve(line.getValue());
            // flush NOW so a lost optimistic-lock race is detected here, not at some later commit
            items.saveAndFlush(item);
            reservations.save(new StockReservation(orderId, item.getSku(), line.getValue()));
        }
    }

    /** Ship: RESERVED -> COMMITTED and on-hand goes down. Idempotent. */
    @Transactional
    public void commitShipment(String orderId) {
        List<StockReservation> open = reservations.findByOrderIdAndStatus(orderId, StockReservation.Status.RESERVED);
        if (open.isEmpty() && reservations.findByOrderIdAndStatus(orderId, StockReservation.Status.COMMITTED).isEmpty()) {
            throw new IllegalStateException("No stock reservation to ship for order " + orderId);
        }
        for (StockReservation reservation : open) {
            InventoryItem item = items.findById(reservation.getSku()).orElseThrow();
            item.commitShipment(reservation.getQuantity());
            items.saveAndFlush(item);
            reservation.markCommitted();
        }
    }

    /** Compensation: RESERVED -> RELEASED. Idempotent; shipped (COMMITTED) lines are never touched. */
    @Transactional
    public void release(String orderId) {
        for (StockReservation reservation : reservations.findByOrderIdAndStatus(orderId, StockReservation.Status.RESERVED)) {
            InventoryItem item = items.findById(reservation.getSku()).orElseThrow();
            item.release(reservation.getQuantity());
            items.saveAndFlush(item);
            reservation.markReleased();
        }
    }

    /** Goods receipt from a purchase order. */
    @Transactional
    public void receive(String sku, int quantity) {
        InventoryItem item = items.findById(sku).orElseThrow(() -> new NotFoundException("Unknown SKU " + sku));
        item.receive(quantity);
        items.saveAndFlush(item);
    }
}
