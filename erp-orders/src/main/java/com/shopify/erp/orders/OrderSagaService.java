package com.shopify.erp.orders;

import com.shopify.erp.common.ConflictException;
import com.shopify.erp.common.NotFoundException;
import com.shopify.erp.common.ObjectIds;
import com.shopify.erp.common.OptimisticRetry;
import com.shopify.erp.inventory.InventoryItem;
import com.shopify.erp.inventory.InventoryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Order-to-cash saga (orchestrated, synchronous):
 *
 *   RESERVE_STOCK  ->  POST_LEDGER  ->  SHIP
 *        |                 |             |
 *        +-- failure ------+-------------+--> compensate: release reservations, cancel the pending ledger event,
 *                                            mark the order CANCELLED
 *
 * Every step is its OWN database transaction (a saga is a sequence of local transactions, not one big one), and
 * each step records a row in saga_step. A step either commits completely or not at all, so a crash between steps
 * leaves a visible, recoverable state (order status + saga_step trail) instead of a half-written order.
 *
 * Known limit: if SHIP fails after the order.placed event was already published to Kafka, the ledger needs a
 * reversal event. The v1 contract has none, so that case is recorded as COMPENSATION_REQUIRED.
 */
@Service
public class OrderSagaService {

    public enum FailAt { NONE, POST_LEDGER, SHIP }

    public record Line(String sku, int quantity) {
    }

    public record PlaceOrder(String customerId, String idempotencyKey, List<Line> lines) {
    }

    private static final Logger log = LoggerFactory.getLogger(OrderSagaService.class);
    // hot SKUs (flash sale) make many transactions race on one row; losers re-read and retry with jittered backoff
    private static final int MAX_ATTEMPTS = 15;
    private static final BigDecimal MAX_TOTAL = new BigDecimal("999999999999.99"); // contract: 12 digits + 2 decimals

    static final String RESERVE_STOCK = "RESERVE_STOCK";
    static final String POST_LEDGER = "POST_LEDGER";
    static final String SHIP = "SHIP";

    private final SalesOrderRepository orders;
    private final SagaStepRepository sagaSteps;
    private final OutboxEventRepository outbox;
    private final InventoryService inventory;
    private final LedgerEventFactory ledgerEvents;
    private final OrderQueryService queries;
    private final TransactionTemplate tx;
    private final String currency;
    private final FailAt failAt;

    public OrderSagaService(SalesOrderRepository orders, SagaStepRepository sagaSteps, OutboxEventRepository outbox,
                            InventoryService inventory, LedgerEventFactory ledgerEvents, OrderQueryService queries,
                            PlatformTransactionManager txManager,
                            @Value("${erp.currency:INR}") String currency,
                            @Value("${erp.saga.simulate-failure-at:NONE}") FailAt failAt) {
        this.orders = orders;
        this.sagaSteps = sagaSteps;
        this.outbox = outbox;
        this.inventory = inventory;
        this.ledgerEvents = ledgerEvents;
        this.queries = queries;
        this.tx = new TransactionTemplate(txManager);
        this.currency = currency.toUpperCase();
        this.failAt = failAt;
    }

    public OrderResponse placeOrder(PlaceOrder cmd) {
        if (cmd.idempotencyKey() != null) {
            var existing = queries.findByIdempotencyKey(cmd.idempotencyKey());
            if (existing.isPresent()) {
                return existing.get(); // client retry: same answer, no second order
            }
        }

        Map<String, Integer> qtyBySku = mergeLines(cmd.lines());
        String orderId = ObjectIds.newId();

        try {
            tx.executeWithoutResult(s -> createOrder(orderId, cmd, qtyBySku));
        } catch (DataIntegrityViolationException duplicateKey) {
            // two concurrent requests with the same Idempotency-Key: the loser returns the winner's order
            if (cmd.idempotencyKey() != null) {
                var winner = queries.findByIdempotencyKey(cmd.idempotencyKey());
                if (winner.isPresent()) {
                    return winner.get();
                }
            }
            throw duplicateKey;
        }

        // ---- step 1: reserve stock (all-or-nothing, optimistic locking) ----
        try {
            OptimisticRetry.run(MAX_ATTEMPTS, () -> tx.execute(s -> {
                inventory.reserve(orderId, qtyBySku);
                load(orderId).transitionTo(OrderStatus.STOCK_RESERVED);
                recordStep(orderId, RESERVE_STOCK, SagaStep.Outcome.SUCCEEDED, null);
                return null;
            }));
        } catch (ConflictException | NotFoundException e) {
            // nothing was reserved (the reserve step is atomic), so there is nothing to compensate
            tx.executeWithoutResult(s -> {
                recordStep(orderId, RESERVE_STOCK, SagaStep.Outcome.FAILED, e.getMessage());
                load(orderId).cancel(e.getMessage());
            });
            throw e;
        }

        // ---- step 2 + 3, with compensation ----
        String step = POST_LEDGER;
        try {
            postLedger(orderId);
            step = SHIP;
            ship(orderId);
        } catch (RuntimeException e) {
            compensate(orderId, step, e);
            throw new SagaFailedException(orderId, step, e.getMessage());
        }

        return queries.get(orderId);
    }

    private void createOrder(String orderId, PlaceOrder cmd, Map<String, Integer> qtyBySku) {
        Map<String, InventoryItem> items = inventory.findAllOrFail(qtyBySku.keySet());
        SalesOrder order = new SalesOrder(orderId, cmd.customerId(), cmd.idempotencyKey(), currency);
        qtyBySku.forEach((sku, qty) -> {
            InventoryItem item = items.get(sku);
            order.addLine(sku, item.getProductId(), qty, item.getUnitPrice());
        });
        if (order.getTotalPrice().compareTo(MAX_TOTAL) > 0) {
            throw new IllegalArgumentException("Order total exceeds the supported maximum");
        }
        orders.saveAndFlush(order);
    }

    private void postLedger(String orderId) {
        simulateFailure(FailAt.POST_LEDGER);
        tx.executeWithoutResult(s -> {
            SalesOrder order = load(orderId);
            OutboxEvent event = ledgerEvents.orderPlaced(order);
            outbox.save(event);                              // same transaction as the status change
            order.transitionTo(OrderStatus.LEDGER_POSTED);
            recordStep(orderId, POST_LEDGER, SagaStep.Outcome.SUCCEEDED, "eventId=" + event.getEventId());
        });
    }

    private void ship(String orderId) {
        simulateFailure(FailAt.SHIP);
        OptimisticRetry.run(MAX_ATTEMPTS, () -> tx.execute(s -> {
            inventory.commitShipment(orderId);
            load(orderId).transitionTo(OrderStatus.SHIPPED);
            recordStep(orderId, SHIP, SagaStep.Outcome.SUCCEEDED, null);
            return null;
        }));
    }

    /** Undo what already happened, newest effect first. Never throws: a failure here is logged for an operator. */
    private void compensate(String orderId, String failedStep, RuntimeException cause) {
        try {
            OptimisticRetry.run(MAX_ATTEMPTS, () -> tx.execute(s -> {
                recordStep(orderId, failedStep, SagaStep.Outcome.FAILED, cause.getMessage());

                inventory.release(orderId);
                recordStep(orderId, "RELEASE_STOCK", SagaStep.Outcome.COMPENSATED, null);

                if (outbox.existsByAggregateId(orderId)) {
                    if (outbox.cancelPending(orderId) > 0) {
                        recordStep(orderId, "CANCEL_LEDGER_EVENT", SagaStep.Outcome.COMPENSATED,
                                "order.placed was still unpublished and has been cancelled");
                    } else {
                        recordStep(orderId, "REVERSE_LEDGER_ENTRY", SagaStep.Outcome.COMPENSATION_REQUIRED,
                                "order.placed already published; a ledger reversal event is needed (not in v1 contract)");
                    }
                }

                load(orderId).cancel("Saga failed at " + failedStep + ": " + cause.getMessage());
                return null;
            }));
            log.warn("Order {} compensated after {} failed: {}", orderId, failedStep, cause.getMessage());
        } catch (RuntimeException compensationError) {
            log.error("[CRITICAL] Compensation FAILED for order {} (step {}): {}. Stock may remain reserved.",
                    orderId, failedStep, compensationError.getMessage(), compensationError);
        }
    }

    private Map<String, Integer> mergeLines(List<Line> lines) {
        Map<String, Integer> merged = new LinkedHashMap<>();
        for (Line line : lines) {
            merged.merge(line.sku(), line.quantity(), (a, b) -> {
                long sum = (long) a + b;
                if (sum > 1_000_000) {
                    throw new IllegalArgumentException("Quantity for SKU " + line.sku() + " exceeds 1,000,000");
                }
                return (int) sum;
            });
        }
        return merged;
    }

    private SalesOrder load(String orderId) {
        return orders.findById(orderId).orElseThrow(() -> new NotFoundException("Order " + orderId + " not found"));
    }

    private void recordStep(String orderId, String step, SagaStep.Outcome outcome, String detail) {
        sagaSteps.save(new SagaStep(orderId, step, outcome, detail));
    }

    /** Demo/test hook (erp.saga.simulate-failure-at=SHIP) to exercise the compensation path on demand. */
    private void simulateFailure(FailAt at) {
        if (failAt == at) {
            throw new IllegalStateException("Simulated failure at " + at);
        }
    }
}
