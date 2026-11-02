package com.shopify.ledger.repository;

import com.shopify.ledger.domain.LedgerItem;
import com.shopify.ledger.domain.LedgerItemId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

public interface LedgerItemRepository extends JpaRepository<LedgerItem, LedgerItemId> {

    /**
     * Plain insert (no ON CONFLICT): it only runs after the parent insertIgnore returned 1, inside the same
     * transaction, so a failure here must roll the whole ingestion back and let Kafka retry it.
     */
    @Modifying
    @Query(value = """
            INSERT INTO ledger_item
                (event_id, line_no, product_id, quantity, unit_price, line_total)
            VALUES
                (:eventId, :lineNo, :productId, :quantity, :unitPrice, :lineTotal)
            """, nativeQuery = true)
    int insertItem(@Param("eventId") UUID eventId,
                   @Param("lineNo") int lineNo,
                   @Param("productId") String productId,
                   @Param("quantity") int quantity,
                   @Param("unitPrice") BigDecimal unitPrice,
                   @Param("lineTotal") BigDecimal lineTotal);

    /** Derived query over the embedded id: id.eventId, ordered by id.lineNo. */
    List<LedgerItem> findByIdEventIdOrderByIdLineNoAsc(UUID eventId);
}