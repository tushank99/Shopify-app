package com.shopify.ledger.repository;

import com.shopify.ledger.domain.LedgerEntry;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

public interface LedgerEntryRepository extends JpaRepository<LedgerEntry, UUID> {

    /**
     * Idempotent insert.
     *
     * A plain save() would fail the whole Postgres transaction on a duplicate key, and Spring would then
     * throw UnexpectedRollbackException even if the exception were caught. ON CONFLICT DO NOTHING keeps
     * the transaction healthy and tells us the outcome through the row count:
     *
     *  - 1 => first time we see this eventId; the caller must now insert the child ledger_item rows
     *  - 0 => duplicate eventId (redelivery, replay, relay crash after send); caller logs it and returns,
     *         so the Kafka offset is committed normally
     *
     * Must run inside a transaction (the caller, LedgerService#ingest, is @Transactional).
     * A conflict on the second guard (order_id, event_type) is NOT swallowed here: it surfaces as a
     * DataIntegrityViolationException and is handled by the caller.
     */
    @Modifying
    @Query(value = """
            INSERT INTO ledger_entry
                (event_id, order_id, user_id, event_type, total_price, currency, order_created_at, ingested_at)
            VALUES
                (:eventId, :orderId, :userId, :eventType, :totalPrice, :currency, :orderCreatedAt, :ingestedAt)
            ON CONFLICT (event_id) DO NOTHING
            """, nativeQuery = true)
    int insertIgnore(@Param("eventId") UUID eventId,
                     @Param("orderId") String orderId,
                     @Param("userId") String userId,
                     @Param("eventType") String eventType,
                     @Param("totalPrice") BigDecimal totalPrice,
                     @Param("currency") String currency,
                     @Param("orderCreatedAt") Instant orderCreatedAt,
                     @Param("ingestedAt") Instant ingestedAt);

    Optional<LedgerEntry> findByOrderIdAndEventType(String orderId, String eventType);

    /** Sort is supplied by the caller's Pageable (newest first, event_id as tie-breaker). */
    Page<LedgerEntry> findByUserId(String userId, Pageable pageable);
}