package com.shopify.erp.inventory;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface InventoryItemRepository extends JpaRepository<InventoryItem, String> {

    boolean existsByProductId(String productId);

    /** Items whose available stock (on_hand - reserved) has dropped to their reorder point. */
    @Query("select i from InventoryItem i where i.reorderQty > 0 and (i.onHand - i.reserved) <= i.reorderPoint order by i.sku")
    List<InventoryItem> findLowStock();
}
