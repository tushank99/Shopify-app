package com.shopify.erp.orders;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * U-7: the event the ERP writes to the outbox must satisfy the SAME JSON schema that ledger-service validates
 * (contracts/order.placed.v1.schema.json). If this test fails, ledger-service would dead-letter ERP orders.
 */
class LedgerEventFactoryTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static JsonSchema schema;

    private final LedgerEventFactory factory = new LedgerEventFactory(MAPPER);

    @BeforeAll
    static void loadSharedContract() throws Exception {
        // surefire runs with the module directory as working dir; the contract lives at the repo root
        Path contract = Path.of("..", "contracts", "order.placed.v1.schema.json");
        try (InputStream in = Files.newInputStream(contract)) {
            schema = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7).getSchema(in);
        }
    }

    private static SalesOrder order() {
        SalesOrder order = new SalesOrder("0123456789abcdef01234567", "fedcba9876543210fedcba98", "k", "INR");
        order.addLine("A", "aaaaaaaaaaaaaaaaaaaaaaaa", 2, new BigDecimal("499"));
        order.addLine("B", "bbbbbbbbbbbbbbbbbbbbbbbb", 1, new BigDecimal("0.10"));
        return order;
    }

    @Test
    void payloadIsValidAgainstTheSharedContract() throws Exception {
        OutboxEvent event = factory.orderPlaced(order());

        JsonNode payload = MAPPER.readTree(event.getPayload());

        assertThat(schema.validate(payload)).isEmpty();
    }

    @Test
    void moneyIsAlwaysATwoDecimalStringAndTheTotalMatchesTheLines() throws Exception {
        JsonNode payload = MAPPER.readTree(factory.orderPlaced(order()).getPayload());

        assertThat(payload.get("totalPrice").isTextual()).isTrue();
        assertThat(payload.get("totalPrice").asText()).isEqualTo("998.10");
        assertThat(payload.get("items").get(0).get("unitPrice").asText()).isEqualTo("499.00");
        assertThat(payload.get("items").get(1).get("unitPrice").asText()).isEqualTo("0.10");
        assertThat(payload.get("currency").asText()).isEqualTo("INR");
    }

    @Test
    void outboxRowIsKeyedForOrderingAndDeduplication() throws Exception {
        SalesOrder order = order();

        OutboxEvent event = factory.orderPlaced(order);
        JsonNode payload = MAPPER.readTree(event.getPayload());

        assertThat(event.getTopic()).isEqualTo("order.placed");
        assertThat(event.getAggregateId()).isEqualTo(order.getId());
        assertThat(event.getPartitionKey()).isEqualTo(order.getCustomerId()); // per-customer ordering, like the Node relay
        assertThat(event.getStatus()).isEqualTo(OutboxEvent.Status.PENDING);
        assertThat(payload.get("eventId").asText()).isEqualTo(event.getEventId());
        assertThat(payload.get("orderId").asText()).isEqualTo(order.getId());
        assertThat(payload.get("userId").asText()).isEqualTo(order.getCustomerId());
    }

    @Test
    void everyCallGetsANewEventId() {
        SalesOrder order = order();

        assertThat(factory.orderPlaced(order).getEventId()).isNotEqualTo(factory.orderPlaced(order).getEventId());
    }

    @Test
    void theSchemaReallyRejectsBadMoney() throws Exception {
        // guard against a test that always passes: a number instead of a 2-decimal string must fail
        JsonNode payload = MAPPER.readTree(factory.orderPlaced(order()).getPayload());
        ((com.fasterxml.jackson.databind.node.ObjectNode) payload).put("totalPrice", 998.1);

        assertThat(schema.validate(payload)).isNotEmpty();
    }
}
