package com.shopify.ledger.event;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Turns the raw Kafka value into a trusted {@link OrderPlacedEvent}, or throws {@link InvalidEventException}.
 *
 * Why bytes and not a JsonDeserializer: Spring's JSON deserializer ignores unknown fields, so a payload like
 * {"corrupt":true} would "deserialize" into an object full of nulls and go down the normal retry path.
 * Validating the tree against the same JSON Schema the Node producer uses rejects it immediately.
 *
 * Stages: 1) valid JSON  2) JSON Schema  3) mapping  4) business invariant (sum of lines == total).
 */
@Component
public class OrderPlacedEventValidator {

    private static final String SCHEMA_RESOURCE = "/schemas/order.placed.v1.schema.json";
    private static final int MAX_REPORTED_ERRORS = 5;

    private final ObjectMapper mapper;
    private final ObjectReader strictTreeReader;
    private final JsonSchema schema;

    public OrderPlacedEventValidator(ObjectMapper mapper) {
        this.mapper = mapper;
        // FAIL_ON_TRAILING_TOKENS: '{"a":1}garbage' must be rejected, not silently accepted.
        this.strictTreeReader = mapper.readerFor(JsonNode.class)
                .with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
        this.schema = loadSchema();
    }

    private JsonSchema loadSchema() {
        try (InputStream in = OrderPlacedEventValidator.class.getResourceAsStream(SCHEMA_RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException(
                        "Event schema not found on classpath: " + SCHEMA_RESOURCE
                                + " (the Maven build copies ../contracts/*.json into classpath:schemas/)");
            }
            return JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7).getSchema(in);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read event schema " + SCHEMA_RESOURCE, e);
        }
    }

    public OrderPlacedEvent parseAndValidate(byte[] payload) {
        if (payload == null || payload.length == 0) {
            throw new InvalidEventException("empty_payload", "Kafka record has no value");
        }

        JsonNode tree;
        try {
            tree = strictTreeReader.readValue(payload);
        } catch (IOException e) {
            throw new InvalidEventException("malformed_json", "Payload is not valid JSON: " + e.getMessage(), e);
        }
        if (tree == null || tree.isNull() || tree.isMissingNode()) {
            throw new InvalidEventException("malformed_json", "Payload is JSON null");
        }

        Set<ValidationMessage> errors = schema.validate(tree);
        if (!errors.isEmpty()) {
            String detail = errors.stream()
                    .map(ValidationMessage::getMessage)
                    .sorted()
                    .limit(MAX_REPORTED_ERRORS)
                    .collect(Collectors.joining("; "));
            throw new InvalidEventException("schema_violation",
                    "Payload violates order.placed v1 (" + errors.size() + " error(s)): " + detail);
        }

        OrderPlacedEvent event;
        try {
            event = mapper.treeToValue(tree, OrderPlacedEvent.class);
        } catch (JsonProcessingException | IllegalArgumentException e) {
            throw new InvalidEventException("schema_violation", "Payload could not be mapped: " + e.getMessage(), e);
        }

        BigDecimal itemsSum = event.items().stream()
                .map(OrderPlacedEvent.Item::lineTotal)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
        if (itemsSum.compareTo(event.totalPrice()) != 0) {
            throw new InvalidEventException("total_mismatch",
                    "Sum of items " + itemsSum.toPlainString() + " != totalPrice " + event.totalPrice().toPlainString()
                            + " (eventId=" + event.eventId() + ")");
        }

        return event;
    }
}