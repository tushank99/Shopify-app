package com.shopify.erp.orders;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Builds the order.placed v1 event (contracts/order.placed.v1.schema.json) that ledger-service already consumes,
 * so ERP orders are ledgered by the existing, idempotent pipeline. Money is always a 2-decimal string.
 */
@Component
public class LedgerEventFactory {

    public static final String TOPIC = "order.placed";

    private final ObjectMapper mapper;

    public LedgerEventFactory(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public OutboxEvent orderPlaced(SalesOrder order) {
        String eventId = UUID.randomUUID().toString();

        List<Map<String, Object>> items = new ArrayList<>();
        for (SalesOrderLine line : order.getLines()) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("productId", line.getProductId());
            item.put("quantity", line.getQuantity());
            item.put("unitPrice", line.getUnitPrice().toPlainString());
            items.add(item);
        }

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("eventId", eventId);
        payload.put("eventType", TOPIC);
        payload.put("schemaVersion", 1);
        payload.put("orderId", order.getId());
        payload.put("userId", order.getCustomerId());
        payload.put("items", items);
        payload.put("totalPrice", order.getTotalPrice().toPlainString());
        payload.put("currency", order.getCurrency());
        payload.put("createdAt", order.getCreatedAt().truncatedTo(ChronoUnit.MILLIS).toString());

        try {
            return new OutboxEvent(eventId, order.getId(), TOPIC, order.getCustomerId(), mapper.writeValueAsString(payload));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialize order.placed event", e);
        }
    }
}
