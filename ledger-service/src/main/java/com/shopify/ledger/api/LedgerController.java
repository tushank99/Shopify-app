package com.shopify.ledger.api;

import com.shopify.ledger.api.dto.LedgerEntryResponse;
import com.shopify.ledger.api.dto.PagedResponse;
import com.shopify.ledger.service.LedgerQueryService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Phase 1: UNAUTHENTICATED. Do not publish this port beyond the Docker/K8s network (compose binds it to
 * 127.0.0.1 only). JWT validation arrives in Phase 2.
 */
@RestController
@RequestMapping("/api/ledger")
public class LedgerController {

    private static final Pattern OBJECT_ID = Pattern.compile("^[a-fA-F0-9]{24}$");
    private static final int MAX_PAGE_SIZE = 100;

    private final LedgerQueryService queries;

    public LedgerController(LedgerQueryService queries) {
        this.queries = queries;
    }

    /** 404 until the order.placed event for this order has been consumed (the pipeline is eventually consistent). */
    @GetMapping("/orders/{orderId}")
    public LedgerEntryResponse getOrder(@PathVariable String orderId) {
        String id = requireObjectId(orderId, "orderId");
        return queries.findByOrderId(id).orElseThrow(() ->
                new ResponseStatusException(HttpStatus.NOT_FOUND, "No ledger entry for orderId " + id));
    }

    /** A user with no ledger entries is a normal 200 with an empty page, not a 404. */
    @GetMapping("/users/{userId}")
    public PagedResponse<LedgerEntryResponse> getUserLedger(
            @PathVariable String userId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {
        String id = requireObjectId(userId, "userId");
        if (page < 0) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "page must be >= 0");
        }
        if (size < 1 || size > MAX_PAGE_SIZE) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "size must be between 1 and " + MAX_PAGE_SIZE);
        }
        return queries.listByUser(id, page, size);
    }

    /** Mongo ObjectIds are stored lower-case by the producer; accept any case, query in lower-case. */
    private static String requireObjectId(String value, String name) {
        if (value == null || !OBJECT_ID.matcher(value).matches()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, name + " must be a 24-character hex id");
        }
        return value.toLowerCase(Locale.ROOT);
    }
}