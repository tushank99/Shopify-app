package com.shopify.ledger.service;

import com.shopify.ledger.event.OrderPlacedEvent;
import com.shopify.ledger.repository.LedgerEntryRepository;
import com.shopify.ledger.repository.LedgerItemRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Service
public class LedgerService {

    private final LedgerEntryRepository entries;
    private final LedgerItemRepository items;

    public LedgerService(LedgerEntryRepository entries, LedgerItemRepository items) {
        this.entries = entries;
        this.items = items;
    }

    /**
     * Writes the ledger entry and its lines atomically, exactly once per eventId.
     *
     * Idempotency: insertIgnore is INSERT ... ON CONFLICT (event_id) DO NOTHING.
     *  - returns 0 => duplicate: we return DUPLICATE_EVENT without touching anything, the transaction stays
     *    healthy (no UnexpectedRollbackException), and the caller lets the Kafka offset commit.
     *  - returns 1 => first delivery: insert the child rows in the SAME transaction. If any line fails, the
     *    whole transaction rolls back (no half-written order) and Kafka retries the record.
     *
     * A conflict on the second guard (order_id, event_type) propagates as DataIntegrityViolationException
     * (and rolls this transaction back). The listener turns that specific case into DUPLICATE_ORDER.
     */
    @Transactional
    public IngestionOutcome ingest(OrderPlacedEvent event) {
        int inserted = entries.insertIgnore(
                event.eventId(),
                event.orderId(),
                event.userId(),
                event.eventType(),
                event.totalPrice(),
                event.currency(),
                event.createdAt(),
                Instant.now());

        if (inserted == 0) {
            return IngestionOutcome.DUPLICATE_EVENT;
        }

        int lineNo = 1;
        for (OrderPlacedEvent.Item item : event.items()) {
            items.insertItem(
                    event.eventId(),
                    lineNo++,
                    item.productId(),
                    item.quantity(),
                    item.unitPrice(),
                    item.lineTotal());
        }

        return IngestionOutcome.INSERTED;
    }
}