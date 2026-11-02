package com.shopify.ledger.api.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.shopify.ledger.domain.LedgerEntry;
import com.shopify.ledger.domain.LedgerItem;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Money is serialised as a string ("199.98"), the same representation as the order.placed contract,
 * so no client ever sees a binary-float rounding artefact.
 * {@code items} is null (and omitted from the JSON) in the paginated user listing.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record LedgerEntryResponse(
        UUID eventId,
        String orderId,
        String userId,
        String eventType,
        @JsonFormat(shape = JsonFormat.Shape.STRING) BigDecimal totalPrice,
        String currency,
        Instant orderCreatedAt,
        Instant ingestedAt,
        List<LedgerItemResponse> items) {

    /** List view: no lines (avoids N+1 queries and in-memory pagination). */
    public static LedgerEntryResponse summary(LedgerEntry entry) {
        return of(entry, null);
    }

    /** Detail view: entry plus its lines, ordered by line number. */
    public static LedgerEntryResponse detail(LedgerEntry entry, List<LedgerItem> items) {
        return of(entry, items.stream().map(LedgerItemResponse::from).toList());
    }

    private static LedgerEntryResponse of(LedgerEntry entry, List<LedgerItemResponse> items) {
        return new LedgerEntryResponse(
                entry.getEventId(),
                entry.getOrderId(),
                entry.getUserId(),
                entry.getEventType(),
                entry.getTotalPrice(),
                entry.getCurrency(),
                entry.getOrderCreatedAt(),
                entry.getIngestedAt(),
                items);
    }
}