package com.shopify.ledger;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.search.Search;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end test of the ledger consumer: real Kafka protocol (EmbeddedKafka, 3 partitions, topic auto-create OFF
 * like docker-compose.infra.yml) + real Postgres (Testcontainers) + the real Flyway migration.
 *
 * Covered: happy path, duplicate eventId (sequential and concurrent), duplicate order under another eventId,
 * and three poison pills (invalid JSON bytes, valid JSON with invalid schema, total mismatch).
 * Every "must not dead-letter" case asserts the DLT delta is 0; every poison pill asserts the DLT delta is
 * exactly 1, byte-for-byte equal to what was sent, with no retry-topic traffic.
 *
 * Needs Docker (Testcontainers). Web layer is NOT started (WebEnvironment.NONE): this class tests ingestion,
 * not the REST controllers.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
                // Same topology as production (6 attempts), just faster backoff so tests stay quick.
                "ledger.retry.delay-ms=200",
                "ledger.retry.max-delay-ms=1000"
        })
@EmbeddedKafka(
        partitions = 3,
        topics = {
                "order.placed",
                "order.placed-retry-0",
                "order.placed-retry-1",
                "order.placed-retry-2",
                "order.placed-retry-3",
                "order.placed-retry-4",
                "order.placed-dlt"
        },
        brokerProperties = {"auto.create.topics.enable=false"})
@Testcontainers
class LedgerIntegrationTest {

    private static final String TOPIC = "order.placed";
    private static final String RETRY_0_TOPIC = "order.placed-retry-0";
    private static final String DLT_TOPIC = "order.placed-dlt";

    private static final Duration AWAIT = Duration.ofSeconds(45);
    private static final Duration POLL = Duration.ofMillis(300);

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

    @DynamicPropertySource
    static void datasourceProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    MeterRegistry meters;

    @Autowired
    EmbeddedKafkaBroker embeddedKafka;

    // ---------------------------------------------------------------------------------------------------------
    // Tests
    // ---------------------------------------------------------------------------------------------------------

    @Test
    void happyPath_writesEntryAndLinesInOneTransaction() throws Exception {
        UUID eventId = UUID.randomUUID();
        String orderId = newId24();
        String userId = newId24();
        String productA = newId24();
        String productB = newId24();

        double ingestedBefore = counter("ledger.events.ingested");
        int dltBefore = readAll(DLT_TOPIC).size();

        send(userId, orderPlacedJson(eventId, orderId, userId, productA, productB, "199.98"));

        await().atMost(AWAIT).pollInterval(POLL)
                .untilAsserted(() -> assertThat(entryCount(eventId)).isEqualTo(1));
        await().atMost(AWAIT).pollInterval(POLL)
                .untilAsserted(() -> assertThat(counter("ledger.events.ingested")).isEqualTo(ingestedBefore + 1));

        LedgerRow row = entry(eventId);
        assertThat(row.orderId()).isEqualTo(orderId);
        assertThat(row.userId()).isEqualTo(userId);
        assertThat(row.eventType()).isEqualTo("order.placed");
        assertThat(row.totalPrice()).isEqualByComparingTo(new BigDecimal("199.98"));
        assertThat(row.currency()).isEqualTo("USD");
        assertThat(row.orderCreatedAt()).isEqualTo(Instant.parse("2026-11-01T08:00:00.000Z"));

        List<LedgerItemRow> items = items(eventId);
        assertThat(items).hasSize(2);

        assertThat(items.get(0).lineNo()).isEqualTo(1);
        assertThat(items.get(0).productId()).isEqualTo(productA);
        assertThat(items.get(0).quantity()).isEqualTo(2);
        assertThat(items.get(0).unitPrice()).isEqualByComparingTo(new BigDecimal("49.99"));
        assertThat(items.get(0).lineTotal()).isEqualByComparingTo(new BigDecimal("99.98"));

        assertThat(items.get(1).lineNo()).isEqualTo(2);
        assertThat(items.get(1).productId()).isEqualTo(productB);
        assertThat(items.get(1).quantity()).isEqualTo(1);
        assertThat(items.get(1).unitPrice()).isEqualByComparingTo(new BigDecimal("100.00"));
        assertThat(items.get(1).lineTotal()).isEqualByComparingTo(new BigDecimal("100.00"));

        assertThat(readAll(DLT_TOPIC)).hasSize(dltBefore);
    }

    @Test
    void duplicateEventId_isIgnored_withZeroDltEntries() throws Exception {
        UUID eventId = UUID.randomUUID();
        String orderId = newId24();
        String userId = newId24();
        String json = orderPlacedJson(eventId, orderId, userId, newId24(), newId24(), "199.98");

        double ingestedBefore = counter("ledger.events.ingested");
        double dupEventIdBefore = counter("ledger.events.duplicate", "reason", "event_id");
        int dltBefore = readAll(DLT_TOPIC).size();
        int retryBefore = readAll(RETRY_0_TOPIC).size();

        send(userId, json);
        await().atMost(AWAIT).pollInterval(POLL)
                .untilAsserted(() -> assertThat(entryCount(eventId)).isEqualTo(1));

        // Same bytes again (at-least-once redelivery / replay).
        send(userId, json);
        await().atMost(AWAIT).pollInterval(POLL).untilAsserted(() ->
                assertThat(counter("ledger.events.duplicate", "reason", "event_id")).isEqualTo(dupEventIdBefore + 1));

        assertThat(entryCount(eventId)).isEqualTo(1);
        assertThat(items(eventId)).hasSize(2);
        assertThat(counter("ledger.events.ingested")).isEqualTo(ingestedBefore + 1);
        assertThat(readAll(DLT_TOPIC)).hasSize(dltBefore);
        assertThat(readAll(RETRY_0_TOPIC)).hasSize(retryBefore);
    }

    @Test
    void sameEventDeliveredConcurrentlyOnDifferentKeys_createsExactlyOneEntry() throws Exception {
        UUID eventId = UUID.randomUUID();
        String orderId = newId24();
        String userId = newId24();
        String json = orderPlacedJson(eventId, orderId, userId, newId24(), newId24(), "199.98");

        double ingestedBefore = counter("ledger.events.ingested");
        // Sum over both reasons (event_id / order): only the total matters for the assertion.
        double duplicatesBefore = counter("ledger.events.duplicate");
        int dltBefore = readAll(DLT_TOPIC).size();

        // Different keys => (usually) different partitions => handled by different consumer threads at once.
        send("concurrent-a", json);
        send("concurrent-b", json);
        send("concurrent-c", json);

        await().atMost(AWAIT).pollInterval(POLL).untilAsserted(() ->
                assertThat(counter("ledger.events.duplicate")).isEqualTo(duplicatesBefore + 2));

        assertThat(entryCount(eventId)).isEqualTo(1);
        assertThat(items(eventId)).hasSize(2);
        assertThat(counter("ledger.events.ingested")).isEqualTo(ingestedBefore + 1);
        assertThat(readAll(DLT_TOPIC)).hasSize(dltBefore);
    }

    @Test
    void sameOrderUnderDifferentEventId_isIgnoredByTheSecondGuard_withZeroDltEntries() throws Exception {
        UUID firstEventId = UUID.randomUUID();
        UUID secondEventId = UUID.randomUUID();
        String orderId = newId24();
        String userId = newId24();

        double dupOrderBefore = counter("ledger.events.duplicate", "reason", "order");
        int dltBefore = readAll(DLT_TOPIC).size();

        send(userId, orderPlacedJson(firstEventId, orderId, userId, newId24(), newId24(), "199.98"));
        await().atMost(AWAIT).pollInterval(POLL)
                .untilAsserted(() -> assertThat(entryCount(firstEventId)).isEqualTo(1));

        // Same business order, brand-new eventId: hits uq_ledger_entry_order_event, not the event_id key.
        send(userId, orderPlacedJson(secondEventId, orderId, userId, newId24(), newId24(), "199.98"));
        await().atMost(AWAIT).pollInterval(POLL).untilAsserted(() ->
                assertThat(counter("ledger.events.duplicate", "reason", "order")).isEqualTo(dupOrderBefore + 1));

        assertThat(entryCountForOrder(orderId)).isEqualTo(1);
        assertThat(entryCount(secondEventId)).isZero();
        assertThat(readAll(DLT_TOPIC)).hasSize(dltBefore);
    }

    @Test
    void poisonPill_invalidJsonBytes_goesStraightToDlt_andNextOrderStillProcesses() throws Exception {
        String key = newId24();
        // Truncated JSON: not parseable at all.
        byte[] payload = "{\"eventId\":\"550e8400-e29b-41d4-a716-446655440000\",\"eventType\":\"order.placed\",\"items\":[{"
                .getBytes(StandardCharsets.UTF_8);

        assertPoisonPillGoesStraightToDlt(key, payload, "malformed_json");
    }

    @Test
    void poisonPill_validJsonButInvalidSchema_goesStraightToDlt_andNextOrderStillProcesses() throws Exception {
        String key = newId24();
        // The exact payload from the failure drill: valid JSON, but every required field is missing.
        byte[] payload = "{\"corrupt\":true}".getBytes(StandardCharsets.UTF_8);

        assertPoisonPillGoesStraightToDlt(key, payload, "schema_violation");
    }

    @Test
    void poisonPill_totalMismatch_goesStraightToDlt_andIsNeverLedgered() throws Exception {
        String key = newId24();
        UUID eventId = UUID.randomUUID();
        // Schema-valid, but 2 x 49.99 + 1 x 100.00 = 199.98, not 200.00.
        byte[] payload = orderPlacedJson(eventId, newId24(), key, newId24(), newId24(), "200.00")
                .getBytes(StandardCharsets.UTF_8);

        assertPoisonPillGoesStraightToDlt(key, payload, "total_mismatch");

        assertThat(entryCount(eventId)).isZero();
    }

    // ---------------------------------------------------------------------------------------------------------
    // Shared assertions
    // ---------------------------------------------------------------------------------------------------------

    /**
     * A poison pill must: land in the DLT exactly once with the original bytes, skip every retry topic,
     * bump ledger.events.invalid{reason}, and must not block the partition (a valid order with the same key,
     * therefore the same partition, is ledgered afterwards).
     */
    private void assertPoisonPillGoesStraightToDlt(String key, byte[] payload, String invalidReason)
            throws Exception {
        int dltBefore = readAll(DLT_TOPIC).size();
        int retryBefore = readAll(RETRY_0_TOPIC).size();
        double invalidBefore = counter("ledger.events.invalid", "reason", invalidReason);

        send(key, payload);

        await().atMost(AWAIT).pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> assertThat(readAll(DLT_TOPIC)).hasSize(dltBefore + 1));

        // Same key => same partition: if the poison pill blocked it, this valid order would never be ledgered.
        UUID followUpEventId = UUID.randomUUID();
        send(key, orderPlacedJson(followUpEventId, newId24(), key, newId24(), newId24(), "199.98"));
        await().atMost(AWAIT).pollInterval(POLL)
                .untilAsserted(() -> assertThat(entryCount(followUpEventId)).isEqualTo(1));

        List<ConsumerRecord<String, byte[]>> dlt = readAll(DLT_TOPIC);
        assertThat(dlt).hasSize(dltBefore + 1);

        List<ConsumerRecord<String, byte[]>> matching = dlt.stream()
                .filter(r -> Arrays.equals(r.value(), payload))
                .toList();
        assertThat(matching).hasSize(1);
        assertThat(matching.get(0).key()).isEqualTo(key);

        assertThat(readAll(RETRY_0_TOPIC)).hasSize(retryBefore);
        assertThat(counter("ledger.events.invalid", "reason", invalidReason)).isEqualTo(invalidBefore + 1);
    }

    // ---------------------------------------------------------------------------------------------------------
    // Kafka helpers
    // ---------------------------------------------------------------------------------------------------------

    private void send(String key, String json) throws Exception {
        send(key, json.getBytes(StandardCharsets.UTF_8));
    }

    private void send(String key, byte[] value) throws Exception {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        try (KafkaProducer<String, byte[]> producer =
                     new KafkaProducer<>(props, new StringSerializer(), new ByteArraySerializer())) {
            producer.send(new ProducerRecord<>(TOPIC, key, value)).get(15, TimeUnit.SECONDS);
            producer.flush();
        }
    }

    /** Reads every record currently in the topic (all partitions) with a throw-away consumer group. */
    private List<ConsumerRecord<String, byte[]>> readAll(String topic) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "ledger-it-reader-" + UUID.randomUUID());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, byte[]> consumer =
                     new KafkaConsumer<>(props, new StringDeserializer(), new ByteArrayDeserializer())) {
            List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                    .map(info -> new TopicPartition(info.topic(), info.partition()))
                    .toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);

            List<ConsumerRecord<String, byte[]>> records = new ArrayList<>();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (System.nanoTime() < deadline && !allAtEnd(consumer, partitions, endOffsets)) {
                consumer.poll(Duration.ofMillis(200)).forEach(records::add);
            }
            if (!allAtEnd(consumer, partitions, endOffsets)) {
                throw new IllegalStateException("Timed out reading all records of topic " + topic);
            }
            return records;
        }
    }

    private static boolean allAtEnd(KafkaConsumer<String, byte[]> consumer,
                                    List<TopicPartition> partitions,
                                    Map<TopicPartition, Long> endOffsets) {
        for (TopicPartition tp : partitions) {
            if (consumer.position(tp) < endOffsets.get(tp)) {
                return false;
            }
        }
        return true;
    }

    // ---------------------------------------------------------------------------------------------------------
    // Postgres / metrics helpers
    // ---------------------------------------------------------------------------------------------------------

    private int entryCount(UUID eventId) {
        Integer count = jdbc.queryForObject(
                "SELECT count(*) FROM ledger_entry WHERE event_id = ?", Integer.class, eventId);
        return count == null ? 0 : count;
    }

    private int entryCountForOrder(String orderId) {
        Integer count = jdbc.queryForObject(
                "SELECT count(*) FROM ledger_entry WHERE order_id = ?", Integer.class, orderId);
        return count == null ? 0 : count;
    }

    private LedgerRow entry(UUID eventId) {
        return jdbc.queryForObject(
                "SELECT order_id, user_id, event_type, total_price, currency, order_created_at "
                        + "FROM ledger_entry WHERE event_id = ?",
                (rs, rowNum) -> new LedgerRow(
                        rs.getString(1),
                        rs.getString(2),
                        rs.getString(3),
                        rs.getBigDecimal(4),
                        rs.getString(5),
                        rs.getObject(6, OffsetDateTime.class).toInstant()),
                eventId);
    }

    private List<LedgerItemRow> items(UUID eventId) {
        return jdbc.query(
                "SELECT line_no, product_id, quantity, unit_price, line_total "
                        + "FROM ledger_item WHERE event_id = ? ORDER BY line_no",
                (rs, rowNum) -> new LedgerItemRow(
                        rs.getInt(1),
                        rs.getString(2),
                        rs.getInt(3),
                        rs.getBigDecimal(4),
                        rs.getBigDecimal(5)),
                eventId);
    }

    /** Sum of all counters with this name (optionally narrowed by tag key/value pairs). */
    private double counter(String name, String... tagKeyValues) {
        Search search = meters.find(name);
        for (int i = 0; i < tagKeyValues.length; i += 2) {
            search = search.tag(tagKeyValues[i], tagKeyValues[i + 1]);
        }
        return search.counters().stream().mapToDouble(Counter::count).sum();
    }

    // ---------------------------------------------------------------------------------------------------------
    // Test data
    // ---------------------------------------------------------------------------------------------------------

    /** 24 lowercase hex chars: same shape as a Mongo ObjectId (^[0-9a-f]{24}$ in the contract). */
    private static String newId24() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 24);
    }

    /** 2 x 49.99 + 1 x 100.00 = 199.98 (the total the caller should pass for a consistent event). */
    private static String orderPlacedJson(UUID eventId, String orderId, String userId,
                                          String productA, String productB, String totalPrice) {
        return """
                {
                  "eventId": "%s",
                  "eventType": "order.placed",
                  "schemaVersion": 1,
                  "orderId": "%s",
                  "userId": "%s",
                  "items": [
                    { "productId": "%s", "quantity": 2, "unitPrice": "49.99" },
                    { "productId": "%s", "quantity": 1, "unitPrice": "100.00" }
                  ],
                  "totalPrice": "%s",
                  "currency": "USD",
                  "createdAt": "2026-11-01T08:00:00.000Z"
                }
                """.formatted(eventId, orderId, userId, productA, productB, totalPrice);
    }

    private record LedgerRow(String orderId, String userId, String eventType, BigDecimal totalPrice,
                             String currency, Instant orderCreatedAt) {
    }

    private record LedgerItemRow(int lineNo, String productId, int quantity, BigDecimal unitPrice,
                                 BigDecimal lineTotal) {
    }
}