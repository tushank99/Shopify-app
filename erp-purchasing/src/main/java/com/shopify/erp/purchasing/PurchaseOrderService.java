package com.shopify.erp.purchasing;

import com.shopify.erp.common.ConflictException;
import com.shopify.erp.common.NotFoundException;
import com.shopify.erp.common.OptimisticRetry;
import com.shopify.erp.inventory.InventoryItem;
import com.shopify.erp.inventory.InventoryService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Uses TransactionTemplate (not @Transactional) so the optimistic-lock retry can wrap the whole transaction. */
@Service
public class PurchaseOrderService {

    public record Line(String sku, int quantity, BigDecimal unitCost) {
    }

    private static final int MAX_ATTEMPTS = 5;

    private final PurchaseOrderRepository orders;
    private final InventoryService inventory;
    private final TransactionTemplate tx;

    public PurchaseOrderService(PurchaseOrderRepository orders, InventoryService inventory,
                                PlatformTransactionManager txManager) {
        this.orders = orders;
        this.inventory = inventory;
        this.tx = new TransactionTemplate(txManager);
    }

    public PurchaseOrderView create(String supplier, List<Line> lines) {
        return tx.execute(s -> {
            PurchaseOrder po = new PurchaseOrder(supplier);
            // merge duplicate SKUs so a PO never has two lines for the same item
            Map<String, Line> merged = new TreeMap<>();
            for (Line line : lines) {
                inventory.get(line.sku()); // 404 on unknown SKU
                merged.merge(line.sku(), line, (a, b) -> new Line(a.sku(), a.quantity() + b.quantity(), a.unitCost()));
            }
            merged.values().forEach(l -> po.addLine(l.sku(), l.quantity(), l.unitCost()));
            return PurchaseOrderView.from(orders.save(po));
        });
    }

    /** Draft PO for every item at/below its reorder point (qty = reorder_qty, cost = standard cost). */
    public PurchaseOrderView createFromLowStock(String supplier) {
        return tx.execute(s -> {
            List<InventoryItem> low = inventory.lowStock();
            if (low.isEmpty()) {
                throw new ConflictException("No items are at or below their reorder point");
            }
            PurchaseOrder po = new PurchaseOrder(supplier);
            low.forEach(i -> po.addLine(i.getSku(), i.getReorderQty(), i.getUnitCost()));
            return PurchaseOrderView.from(orders.save(po));
        });
    }

    public PurchaseOrderView approve(Long id) {
        return OptimisticRetry.run(MAX_ATTEMPTS, () -> tx.execute(s -> {
            PurchaseOrder po = load(id);
            po.approve();
            return PurchaseOrderView.from(orders.saveAndFlush(po));
        }));
    }

    public PurchaseOrderView cancel(Long id) {
        return OptimisticRetry.run(MAX_ATTEMPTS, () -> tx.execute(s -> {
            PurchaseOrder po = load(id);
            po.cancel();
            return PurchaseOrderView.from(orders.saveAndFlush(po));
        }));
    }

    /** Goods receipt: PO -> RECEIVED and every line is added to on-hand stock, in one transaction. */
    public PurchaseOrderView receive(Long id) {
        return OptimisticRetry.run(MAX_ATTEMPTS, () -> tx.execute(s -> {
            PurchaseOrder po = load(id);
            po.markReceived();
            po.getLines().forEach(l -> inventory.receive(l.getSku(), l.getQuantity()));
            return PurchaseOrderView.from(orders.saveAndFlush(po));
        }));
    }

    public PurchaseOrderView get(Long id) {
        return tx.execute(s -> PurchaseOrderView.from(load(id)));
    }

    public List<PurchaseOrderView> list() {
        return tx.execute(s -> orders.findAllByOrderByIdDesc().stream().map(PurchaseOrderView::from).toList());
    }

    private PurchaseOrder load(Long id) {
        return orders.findById(id).orElseThrow(() -> new NotFoundException("Purchase order " + id + " not found"));
    }
}
