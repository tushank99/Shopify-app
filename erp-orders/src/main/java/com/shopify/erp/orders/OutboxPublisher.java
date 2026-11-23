package com.shopify.erp.orders;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Relays PENDING outbox rows to Kafka. At-least-once: a row is marked PUBLISHED only after the broker acked it,
 * so a crash in between re-sends it, and ledger-service de-duplicates on eventId. Rows are claimed with
 * FOR UPDATE SKIP LOCKED, so running several instances is safe.
 */
@Component
@ConditionalOnProperty(name = "erp.outbox.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final OutboxEventRepository outbox;
    private final KafkaTemplate<String, String> kafka;
    private final TransactionTemplate tx;
    private final int batchSize;
    private final int maxAttempts;
    private final long sendTimeoutMs;

    public OutboxPublisher(OutboxEventRepository outbox, KafkaTemplate<String, String> kafka,
                           PlatformTransactionManager txManager,
                           @Value("${erp.outbox.batch-size:50}") int batchSize,
                           @Value("${erp.outbox.max-attempts:20}") int maxAttempts,
                           @Value("${erp.outbox.send-timeout-ms:8000}") long sendTimeoutMs) {
        this.outbox = outbox;
        this.kafka = kafka;
        this.tx = new TransactionTemplate(txManager);
        // Under MySQL's default REPEATABLE READ, "WHERE status='PENDING' ... FOR UPDATE" also takes a GAP lock on the
        // (status, id) index, so every saga inserting a new PENDING row (POST_LEDGER) waited for this transaction,
        // i.e. for the Kafka sends. Under load that held all pool connections and orders failed (found by the
        // 20-buyer concurrency test). READ COMMITTED takes record locks only: claimed rows stay locked
        // (SKIP LOCKED still keeps instances apart) but new orders are never blocked by the relay.
        this.tx.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        this.batchSize = batchSize;
        this.maxAttempts = maxAttempts;
        this.sendTimeoutMs = sendTimeoutMs;
    }

    @Scheduled(fixedDelayString = "${erp.outbox.poll-ms:1000}")
    public void publishPending() {
        try {
            tx.executeWithoutResult(s -> {
                List<OutboxEvent> batch = outbox.lockByStatus(OutboxEvent.Status.PENDING, PageRequest.of(0, batchSize));
                for (OutboxEvent event : batch) {
                    try {
                        send(event);
                        event.markPublished();
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    } catch (Exception e) {
                        event.recordFailure(e.getMessage(), maxAttempts);
                        log.warn("Outbox publish failed for event {} (attempt {}): {}", event.getEventId(), event.getAttempts(), e.getMessage());
                        break; // broker likely down: stop this batch, retry on the next poll
                    }
                }
            });
        } catch (RuntimeException e) {
            log.error("Outbox poll failed: {}", e.getMessage());
        }
    }

    private void send(OutboxEvent event) throws Exception {
        ProducerRecord<String, String> record =
                new ProducerRecord<>(event.getTopic(), event.getPartitionKey(), event.getPayload());
        record.headers().add("event-id", event.getEventId().getBytes(StandardCharsets.UTF_8));
        record.headers().add("content-type", "application/json".getBytes(StandardCharsets.UTF_8));
        kafka.send(record).get(sendTimeoutMs, TimeUnit.MILLISECONDS);
    }
}
