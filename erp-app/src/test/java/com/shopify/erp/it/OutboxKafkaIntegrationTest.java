package com.shopify.erp.it;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * I-4 / S-4 (ERP side): a shipped order's outbox row is relayed to Kafka, marked PUBLISHED only after the ack, and the
 * record on the topic is the ledger contract (same orderId, total, event-id header == payload eventId).
 */
class OutboxKafkaIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    EmbeddedKafkaBroker broker;

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void shippedOrderIsPublishedToTheLedgerTopicExactlyAsStored() throws Exception {
        String sku = uniqueSku("LAMP");
        createItem(sku, 3, "1250.00", 0, 0);
        Map<?, ?> order = placeOrder(null, List.of(line(sku, 2))).getBody();
        String orderId = (String) order.get("id");

        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> assertThat(jdbc.queryForObject(
                "SELECT status FROM outbox_event WHERE aggregate_id = ?", String.class, orderId)).isEqualTo("PUBLISHED"));

        Map<String, Object> row = jdbc.queryForMap(
                "SELECT event_id, attempts, published_at FROM outbox_event WHERE aggregate_id = ?", orderId);
        assertThat(row.get("published_at")).isNotNull();

        ConsumerRecord<String, String> record = findRecordFor(orderId);
        JsonNode payload = mapper.readTree(record.value());

        assertThat(record.key()).isEqualTo(CUSTOMER);
        assertThat(new String(record.headers().lastHeader("event-id").value(), StandardCharsets.UTF_8))
                .isEqualTo(row.get("event_id"));
        assertThat(payload.get("eventId").asText()).isEqualTo(row.get("event_id"));
        assertThat(payload.get("totalPrice").asText()).isEqualTo("2500.00");
        assertThat(payload.get("items").get(0).get("quantity").asInt()).isEqualTo(2);
    }

    private ConsumerRecord<String, String> findRecordFor(String orderId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "it-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            TopicPartition partition = new TopicPartition("order.placed", 0);
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            long deadline = System.currentTimeMillis() + 15_000;
            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(500))) {
                    if (r.value().contains("\"orderId\":\"" + orderId + "\"")) {
                        return r;
                    }
                }
            }
        }
        throw new AssertionError("No order.placed record for order " + orderId);
    }
}
