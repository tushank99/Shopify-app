package com.shopify.ledger.kafka;

import com.shopify.ledger.event.InvalidEventException;
import com.shopify.ledger.event.OrderPlacedEvent;
import com.shopify.ledger.event.OrderPlacedEventValidator;
import com.shopify.ledger.service.IngestionOutcome;
import com.shopify.ledger.service.LedgerService;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * Consumes order.placed (delivery is at-least-once; idempotency lives in LedgerService/Postgres).
 *
 * Failure routing:
 *  - invalid event (bad JSON, schema violation, total mismatch) -> InvalidEventException is EXCLUDED from retry
 *    => straight to order.placed-dlt, no retry topics, no blocking of the partition
 *  - transient failure (Postgres down, deadlock, ...)           -> order.placed-retry-0 .. -retry-(attempts-2),
 *    exponential backoff, then order.placed-dlt
 *  - duplicate eventId / duplicate order                         -> logged + counted, method returns normally,
 *    so the offset is committed
 *
 * Topics are pre-created (docker-compose kafka-init); autoCreateTopics is off. Retry topic names use the index
 * suffix (-retry-0, -retry-1, ...) instead of the delay, so they do not change when the backoff is tuned.
 * Ordering is NOT guaranteed (retry topics, rebalances) and the ledger does not need it.
 */
@Component
public class OrderPlacedListener {

    private static final Logger log = LoggerFactory.getLogger(OrderPlacedListener.class);

    /** Name of the second guard in V1__init_ledger.sql: UNIQUE (order_id, event_type). */
    private static final String ORDER_EVENT_CONSTRAINT = "uq_ledger_entry_order_event";

    private final OrderPlacedEventValidator validator;
    private final LedgerService ledgerService;
    private final MeterRegistry meters;

    public OrderPlacedListener(OrderPlacedEventValidator validator, LedgerService ledgerService, MeterRegistry meters) {
        this.validator = validator;
        this.ledgerService = ledgerService;
        this.meters = meters;
    }

    @RetryableTopic(
            // Longer than the validation path on purpose: a ~60s Postgres outage is retried, not dead-lettered.
            attempts = "${ledger.retry.attempts:6}",
            backoff = @Backoff(
                    delayExpression = "${ledger.retry.delay-ms:2000}",
                    multiplierExpression = "${ledger.retry.multiplier:2.0}",
                    maxDelayExpression = "${ledger.retry.max-delay-ms:30000}"),
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            autoCreateTopics = "false",
            numPartitions = "3",
            replicationFactor = "1",
            kafkaTemplate = "dltKafkaTemplate",
            traversingCauses = "true",
            exclude = {InvalidEventException.class, DeserializationException.class})
    @KafkaListener(
            topics = "${ledger.topic:order.placed}",
            groupId = "${spring.kafka.consumer.group-id}",
            concurrency = "${ledger.concurrency:3}")
    public void onMessage(ConsumerRecord<String, byte[]> record) {
        OrderPlacedEvent event;
        try {
            event = validator.parseAndValidate(record.value());
        } catch (InvalidEventException e) {
            meters.counter("ledger.events.invalid", "reason", e.getReason()).increment();
            log.warn("Invalid order event -> dead-letter topic (topic={}, partition={}, offset={}, reason={}): {}",
                    record.topic(), record.partition(), record.offset(), e.getReason(), e.getMessage());
            throw e; // excluded from retries => goes directly to the DLT
        }

        if (record.key() != null && !record.key().equals(event.userId())) {
            // The key only drives partitioning; the payload is authoritative. Worth knowing, not worth failing.
            log.warn("Record key {} != event userId {} (eventId={})", record.key(), event.userId(), event.eventId());
        }

        IngestionOutcome outcome;
        try {
            outcome = ledgerService.ingest(event);
        } catch (DataIntegrityViolationException e) {
            if (!isOrderEventConflict(e)) {
                throw e; // some other integrity problem: let the retry/DLT machinery decide
            }
            outcome = IngestionOutcome.DUPLICATE_ORDER;
        }

        switch (outcome) {
            case INSERTED -> {
                meters.counter("ledger.events.ingested").increment();
                log.debug("Ledger entry written (eventId={}, orderId={}, partition={}, offset={})",
                        event.eventId(), event.orderId(), record.partition(), record.offset());
            }
            case DUPLICATE_EVENT -> {
                meters.counter("ledger.events.duplicate", "reason", "event_id").increment();
                log.info("Duplicate eventId ignored: {} (orderId={}, topic={}, partition={}, offset={})",
                        event.eventId(), event.orderId(), record.topic(), record.partition(), record.offset());
            }
            case DUPLICATE_ORDER -> {
                meters.counter("ledger.events.duplicate", "reason", "order").increment();
                log.warn("Duplicate order ignored: orderId={} already ledgered under another eventId "
                                + "(this eventId={}, topic={}, partition={}, offset={})",
                        event.orderId(), event.eventId(), record.topic(), record.partition(), record.offset());
            }
        }
    }

    /**
     * Terminal handler for order.placed-dlt. Never throws (FAIL_ON_ERROR would otherwise stop the container).
     * Logs the failure context and counts it. To replay after fixing the cause, see docs/OUTBOX_KAFKA.md
     * (scripts/dlt-redrive.mjs).
     */
    @DltHandler
    public void onDeadLetter(ConsumerRecord<String, byte[]> record) {
        meters.counter("ledger.events.dlt").increment();
        log.error("DEAD LETTER (originalTopic={}, key={}, dltPartition={}, dltOffset={}, exception={}, message={}, "
                        + "payloadBytes={}, payloadPreview={})",
                header(record, KafkaHeaders.ORIGINAL_TOPIC),
                record.key(),
                record.partition(),
                record.offset(),
                header(record, KafkaHeaders.EXCEPTION_FQCN),
                header(record, KafkaHeaders.EXCEPTION_MESSAGE),
                record.value() == null ? 0 : record.value().length,
                preview(record.value()));
    }

    private static boolean isOrderEventConflict(Throwable error) {
        Throwable root = NestedExceptionUtils.getMostSpecificCause(error);
        String message = root.getMessage();
        return message != null && message.contains(ORDER_EVENT_CONSTRAINT);
    }

    private static String header(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null || header.value() == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static String preview(byte[] value) {
        if (value == null) {
            return null;
        }
        String text = new String(value, StandardCharsets.UTF_8);
        return text.length() <= 300 ? text : text.substring(0, 300) + "...";
    }
}