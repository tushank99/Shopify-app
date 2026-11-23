package com.shopify.erp.orders;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.shopify.erp.common.NotFoundException;
import com.shopify.erp.common.ObjectIds;
import com.shopify.erp.inventory.InsufficientStockException;
import com.shopify.erp.inventory.InventoryItem;
import com.shopify.erp.inventory.InventoryService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.transaction.PlatformTransactionManager;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * U-6: the saga orchestration with every port mocked. Orders live in an in-memory map so status changes are visible;
 * saga_step rows are captured so the audit trail can be asserted exactly.
 */
@ExtendWith(MockitoExtension.class)
class OrderSagaServiceTest {

    private static final String CUSTOMER = "fedcba9876543210fedcba98";

    @Mock
    SalesOrderRepository orders;
    @Mock
    SagaStepRepository sagaSteps;
    @Mock
    OutboxEventRepository outbox;
    @Mock
    InventoryService inventory;
    @Mock
    OrderQueryService queries;

    private final Map<String, SalesOrder> db = new HashMap<>();
    private final List<SagaStep> steps = new ArrayList<>();

    @BeforeEach
    void inMemoryPorts() {
        lenient().when(orders.saveAndFlush(any())).thenAnswer(i -> {
            SalesOrder o = i.getArgument(0);
            db.put(o.getId(), o);
            return o;
        });
        lenient().when(orders.findById(anyString())).thenAnswer(i -> Optional.ofNullable(db.get(i.<String>getArgument(0))));
        lenient().when(sagaSteps.save(any())).thenAnswer(i -> {
            steps.add(i.getArgument(0));
            return i.getArgument(0);
        });
        lenient().when(outbox.save(any())).thenAnswer(i -> i.getArgument(0));
        lenient().when(queries.get(anyString())).thenAnswer(i -> OrderResponse.from(db.get(i.<String>getArgument(0))));
        lenient().when(inventory.findAllOrFail(any())).thenAnswer(i -> {
            Map<String, InventoryItem> found = new LinkedHashMap<>();
            for (String sku : i.<Iterable<String>>getArgument(0)) {
                found.put(sku, new InventoryItem(sku, ObjectIds.newId(), sku, new BigDecimal("100.00"), BigDecimal.ONE, 10, 0, 0));
            }
            return found;
        });
    }

    private OrderSagaService saga(OrderSagaService.FailAt failAt) {
        return new OrderSagaService(orders, sagaSteps, outbox, inventory, new LedgerEventFactory(new ObjectMapper()),
                queries, mock(PlatformTransactionManager.class), "inr", failAt);
    }

    private static OrderSagaService.PlaceOrder order(String key, OrderSagaService.Line... lines) {
        return new OrderSagaService.PlaceOrder(CUSTOMER, key, List.of(lines));
    }

    private static OrderSagaService.Line line(String sku, int qty) {
        return new OrderSagaService.Line(sku, qty);
    }

    private SalesOrder onlyOrder() {
        assertThat(db).hasSize(1);
        return db.values().iterator().next();
    }

    // ---------------------------------------------------------------- happy path

    @Test
    void happyPathReservesPostsToLedgerAndShipsInThatOrder() {
        OrderResponse result = saga(OrderSagaService.FailAt.NONE).placeOrder(order(null, line("A", 2), line("B", 1)));

        assertThat(result.status()).isEqualTo("SHIPPED");
        assertThat(result.totalPrice()).isEqualByComparingTo("300.00");
        assertThat(result.currency()).isEqualTo("INR");

        InOrder flow = inOrder(inventory, outbox);
        flow.verify(inventory).reserve(anyString(), anyMap());
        flow.verify(outbox).save(any(OutboxEvent.class));
        flow.verify(inventory).commitShipment(result.id());
        verify(inventory, never()).release(anyString());

        assertThat(steps).extracting(SagaStep::getStep, SagaStep::getOutcome).containsExactly(
                tuple("RESERVE_STOCK", SagaStep.Outcome.SUCCEEDED),
                tuple("POST_LEDGER", SagaStep.Outcome.SUCCEEDED),
                tuple("SHIP", SagaStep.Outcome.SUCCEEDED));
    }

    @Test
    @SuppressWarnings("unchecked")
    void duplicateSkuLinesAreMergedBeforeReserving() {
        saga(OrderSagaService.FailAt.NONE).placeOrder(order(null, line("A", 2), line("A", 3)));

        ArgumentCaptor<Map<String, Integer>> reserved = ArgumentCaptor.forClass(Map.class);
        verify(inventory).reserve(anyString(), reserved.capture());
        assertThat(reserved.getValue()).containsExactly(Map.entry("A", 5));
        assertThat(onlyOrder().getLines()).singleElement().extracting(SalesOrderLine::getQuantity).isEqualTo(5);
    }

    @Test
    void anOptimisticLockConflictWhileReservingIsRetried() {
        doThrow(new OptimisticLockingFailureException("lost race")).doNothing()
                .when(inventory).reserve(anyString(), anyMap());

        OrderResponse result = saga(OrderSagaService.FailAt.NONE).placeOrder(order(null, line("A", 1)));

        assertThat(result.status()).isEqualTo("SHIPPED");
        verify(inventory, times(2)).reserve(anyString(), anyMap());
    }

    // ---------------------------------------------------------------- failure in step 1: nothing to compensate

    @Test
    void insufficientStockCancelsTheOrderWithoutCompensation() {
        doThrow(new InsufficientStockException("A", 5, 1)).when(inventory).reserve(anyString(), anyMap());

        assertThatThrownBy(() -> saga(OrderSagaService.FailAt.NONE).placeOrder(order(null, line("A", 5))))
                .isInstanceOf(InsufficientStockException.class);

        SalesOrder order = onlyOrder();
        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        assertThat(order.getFailureReason()).contains("Insufficient stock");
        assertThat(steps).extracting(SagaStep::getStep, SagaStep::getOutcome)
                .containsExactly(tuple("RESERVE_STOCK", SagaStep.Outcome.FAILED));
        verify(inventory, never()).release(anyString());
        verify(outbox, never()).save(any());
    }

    @Test
    void anInfrastructureFailureWhileReservingStillClosesTheOrder() {
        // regression (DEF-2): a DB timeout here used to leave the order in CREATED forever
        doThrow(new org.springframework.transaction.CannotCreateTransactionException("pool exhausted"))
                .when(inventory).reserve(anyString(), anyMap());

        assertThatThrownBy(() -> saga(OrderSagaService.FailAt.NONE).placeOrder(order(null, line("A", 1))))
                .isInstanceOf(org.springframework.transaction.CannotCreateTransactionException.class);

        assertThat(onlyOrder().getStatus()).isEqualTo(OrderStatus.CANCELLED);
        assertThat(steps).extracting(SagaStep::getStep, SagaStep::getOutcome)
                .containsExactly(tuple("RESERVE_STOCK", SagaStep.Outcome.FAILED));
        verify(inventory, never()).release(anyString());
    }

    @Test
    void unknownSkuIsRejectedBeforeAnOrderExists() {
        doThrow(new NotFoundException("Unknown SKU GHOST")).when(inventory).findAllOrFail(any());

        assertThatThrownBy(() -> saga(OrderSagaService.FailAt.NONE).placeOrder(order(null, line("GHOST", 1))))
                .isInstanceOf(NotFoundException.class);

        assertThat(db).isEmpty();
        verify(inventory, never()).reserve(anyString(), anyMap());
    }

    // ---------------------------------------------------------------- failure in steps 2/3: compensation

    @Test
    void shipFailureReleasesStockAndCancelsTheUnpublishedLedgerEvent() {
        when(outbox.existsByAggregateId(anyString())).thenReturn(true);
        when(outbox.cancelPending(anyString())).thenReturn(1);

        assertThatThrownBy(() -> saga(OrderSagaService.FailAt.SHIP).placeOrder(order(null, line("A", 2))))
                .isInstanceOfSatisfying(SagaFailedException.class, e -> assertThat(e.getFailedStep()).isEqualTo("SHIP"));

        SalesOrder order = onlyOrder();
        verify(inventory).release(order.getId());
        verify(outbox).cancelPending(order.getId());
        verify(inventory, never()).commitShipment(anyString());
        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        assertThat(order.getFailureReason()).contains("SHIP");
        assertThat(steps).extracting(SagaStep::getStep, SagaStep::getOutcome).containsExactly(
                tuple("RESERVE_STOCK", SagaStep.Outcome.SUCCEEDED),
                tuple("POST_LEDGER", SagaStep.Outcome.SUCCEEDED),
                tuple("SHIP", SagaStep.Outcome.FAILED),
                tuple("RELEASE_STOCK", SagaStep.Outcome.COMPENSATED),
                tuple("CANCEL_LEDGER_EVENT", SagaStep.Outcome.COMPENSATED));
    }

    @Test
    void shipFailureAfterTheEventWasPublishedIsFlaggedForAReversal() {
        when(outbox.existsByAggregateId(anyString())).thenReturn(true);
        when(outbox.cancelPending(anyString())).thenReturn(0); // relay already sent it

        assertThatThrownBy(() -> saga(OrderSagaService.FailAt.SHIP).placeOrder(order(null, line("A", 1))))
                .isInstanceOf(SagaFailedException.class);

        assertThat(steps).last().satisfies(s -> {
            assertThat(s.getStep()).isEqualTo("REVERSE_LEDGER_ENTRY");
            assertThat(s.getOutcome()).isEqualTo(SagaStep.Outcome.COMPENSATION_REQUIRED);
        });
        assertThat(onlyOrder().getStatus()).isEqualTo(OrderStatus.CANCELLED);
    }

    @Test
    void postLedgerFailureReleasesStockAndHasNoEventToCancel() {
        when(outbox.existsByAggregateId(anyString())).thenReturn(false);

        assertThatThrownBy(() -> saga(OrderSagaService.FailAt.POST_LEDGER).placeOrder(order(null, line("A", 1))))
                .isInstanceOfSatisfying(SagaFailedException.class,
                        e -> assertThat(e.getFailedStep()).isEqualTo("POST_LEDGER"));

        verify(inventory).release(onlyOrder().getId());
        verify(outbox, never()).save(any());
        verify(outbox, never()).cancelPending(anyString());
        assertThat(steps).extracting(SagaStep::getStep).containsExactly("RESERVE_STOCK", "POST_LEDGER", "RELEASE_STOCK");
        assertThat(onlyOrder().getStatus()).isEqualTo(OrderStatus.CANCELLED);
    }

    @Test
    void aFailingCompensationNeverHidesTheOriginalError() {
        doThrow(new IllegalStateException("database gone")).when(inventory).release(anyString());

        assertThatThrownBy(() -> saga(OrderSagaService.FailAt.SHIP).placeOrder(order(null, line("A", 1))))
                .isInstanceOf(SagaFailedException.class)
                .hasMessageContaining("Simulated failure at SHIP");
    }

    // ---------------------------------------------------------------- idempotency

    @Test
    void aRepeatedIdempotencyKeyReturnsTheFirstOrderAndDoesNothingElse() {
        OrderResponse first = new OrderResponse("0123456789abcdef01234567", CUSTOMER, "SHIPPED", null, "INR",
                new BigDecimal("100.00"), null, List.of());
        when(queries.findByIdempotencyKey("key-1")).thenReturn(Optional.of(first));

        OrderResponse again = saga(OrderSagaService.FailAt.NONE).placeOrder(order("key-1", line("A", 1)));

        assertThat(again).isSameAs(first);
        verify(orders, never()).saveAndFlush(any());
        verifyNoInteractions(inventory);
    }

    @Test
    void theLoserOfAConcurrentDuplicateKeyRaceGetsTheWinnersOrder() {
        OrderResponse winner = new OrderResponse("0123456789abcdef01234567", CUSTOMER, "SHIPPED", null, "INR",
                new BigDecimal("100.00"), null, List.of());
        when(queries.findByIdempotencyKey("key-2")).thenReturn(Optional.empty()).thenReturn(Optional.of(winner));
        doThrow(new DataIntegrityViolationException("uk_sales_order_idempotency_key")).when(orders).saveAndFlush(any());

        OrderResponse result = saga(OrderSagaService.FailAt.NONE).placeOrder(order("key-2", line("A", 1)));

        assertThat(result).isSameAs(winner);
        verify(inventory, never()).reserve(anyString(), anyMap());
    }

    // ---------------------------------------------------------------- input limits

    @Test
    void mergedQuantityAboveTheLimitIsRejected() {
        assertThatThrownBy(() -> saga(OrderSagaService.FailAt.NONE)
                .placeOrder(order(null, line("A", 600_000), line("A", 600_000))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("1,000,000");
        verifyNoInteractions(inventory);
    }

    @Test
    void anOrderTotalBeyondTheContractLimitIsRejected() {
        org.mockito.Mockito.doReturn(Map.of("A",
                new InventoryItem("A", ObjectIds.newId(), "A", new BigDecimal("9999999999.99"), BigDecimal.ONE, 10, 0, 0)))
                .when(inventory).findAllOrFail(any());

        assertThatThrownBy(() -> saga(OrderSagaService.FailAt.NONE).placeOrder(order(null, line("A", 1_000_000))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maximum");
        verify(orders, never()).saveAndFlush(any());
    }
}
