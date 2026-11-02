package com.shopify.ledger.service;

import com.shopify.ledger.api.dto.LedgerEntryResponse;
import com.shopify.ledger.api.dto.PagedResponse;
import com.shopify.ledger.repository.LedgerEntryRepository;
import com.shopify.ledger.repository.LedgerItemRepository;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Service
@Transactional(readOnly = true)
public class LedgerQueryService {

    private static final String ORDER_PLACED = "order.placed";

    private final LedgerEntryRepository entries;
    private final LedgerItemRepository items;

    public LedgerQueryService(LedgerEntryRepository entries, LedgerItemRepository items) {
        this.entries = entries;
        this.items = items;
    }

    /** One order with its lines. Empty when the event has not been consumed (yet) or never existed. */
    public Optional<LedgerEntryResponse> findByOrderId(String orderId) {
        return entries.findByOrderIdAndEventType(orderId, ORDER_PLACED)
                .map(entry -> LedgerEntryResponse.detail(
                        entry, items.findByIdEventIdOrderByIdLineNoAsc(entry.getEventId())));
    }

    /**
     * A user's ledger, newest first. The sort (order_created_at DESC, event_id DESC) matches
     * idx_ledger_entry_user_created and the event_id tie-breaker keeps pages stable.
     * Lines are intentionally not included: fetch an order for its lines.
     */
    public PagedResponse<LedgerEntryResponse> listByUser(String userId, int page, int size) {
        Pageable pageable = PageRequest.of(page, size,
                Sort.by(Sort.Order.desc("orderCreatedAt"), Sort.Order.desc("eventId")));
        return PagedResponse.from(entries.findByUserId(userId, pageable).map(LedgerEntryResponse::summary));
    }
}