package com.shopify.erp.orders;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** U-3: order status is a state machine; only forward saga moves or a cancel are legal, and the ends are final. */
class SalesOrderTest {

    private static final Set<String> LEGAL = Set.of(
            "CREATED>STOCK_RESERVED", "CREATED>CANCELLED",
            "STOCK_RESERVED>LEDGER_POSTED", "STOCK_RESERVED>CANCELLED",
            "LEDGER_POSTED>SHIPPED", "LEDGER_POSTED>CANCELLED");

    /** Every (from, to) pair of statuses: 25 cases, 6 legal. */
    static Stream<Arguments> allTransitions() {
        return Arrays.stream(OrderStatus.values())
                .flatMap(from -> Arrays.stream(OrderStatus.values()).map(to -> Arguments.of(from, to)));
    }

    private static SalesOrder orderIn(OrderStatus status) {
        SalesOrder order = new SalesOrder("aaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbb", null, "INR");
        // walk the happy path up to the requested status
        OrderStatus[] path = {OrderStatus.STOCK_RESERVED, OrderStatus.LEDGER_POSTED, OrderStatus.SHIPPED};
        if (status == OrderStatus.CANCELLED) {
            order.cancel("test");
            return order;
        }
        for (OrderStatus step : path) {
            if (order.getStatus() == status) {
                break;
            }
            order.transitionTo(step);
        }
        return order;
    }

    @ParameterizedTest(name = "{0} -> {1}")
    @MethodSource("allTransitions")
    void onlyTheSagaTransitionsAreAllowed(OrderStatus from, OrderStatus to) {
        SalesOrder order = orderIn(from);
        assertThat(order.getStatus()).isEqualTo(from);

        if (LEGAL.contains(from + ">" + to)) {
            order.transitionTo(to);
            assertThat(order.getStatus()).isEqualTo(to);
        } else {
            assertThatThrownBy(() -> order.transitionTo(to))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("illegal transition");
            assertThat(order.getStatus()).isEqualTo(from);
        }
    }

    @Test
    void shippedAndCancelledAreFinal() {
        assertThat(OrderStatus.SHIPPED.next()).isEmpty();
        assertThat(OrderStatus.CANCELLED.next()).isEmpty();
    }

    @Test
    void cancelStoresAReasonCappedAt500Characters() {
        SalesOrder order = orderIn(OrderStatus.LEDGER_POSTED);

        order.cancel("x".repeat(800));

        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        assertThat(order.getFailureReason()).hasSize(500);
    }

    @Test
    void aShippedOrderCannotBeCancelled() {
        SalesOrder order = orderIn(OrderStatus.SHIPPED);

        assertThatThrownBy(() -> order.cancel("too late")).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void linesSnapshotThePriceAndAddUpToTheTotal() {
        SalesOrder order = new SalesOrder("aaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbb", "key-1", "INR");
        BigDecimal price = new BigDecimal("499");

        order.addLine("A", "cccccccccccccccccccccccc", 2, price);
        order.addLine("B", "dddddddddddddddddddddddd", 1, new BigDecimal("0.5"));

        assertThat(order.getLines()).extracting(SalesOrderLine::getLineNo).containsExactly(1, 2);
        assertThat(order.getLines().get(0).getUnitPrice()).isEqualByComparingTo("499.00").hasScaleOf(2);
        assertThat(order.getLines().get(0).getLineTotal()).isEqualByComparingTo("998.00");
        assertThat(order.getTotalPrice()).isEqualByComparingTo("998.50").hasScaleOf(2);
    }

    @Test
    void outboxEventGivesUpAfterMaxAttempts() {
        OutboxEvent event = new OutboxEvent("e", "aaaaaaaaaaaaaaaaaaaaaaaa", "order.placed", "k", "{}");

        event.recordFailure("broker down", 3);
        event.recordFailure("broker down", 3);
        assertThat(event.getStatus()).isEqualTo(OutboxEvent.Status.PENDING);

        event.recordFailure("x".repeat(900), 3);
        assertThat(event.getStatus()).isEqualTo(OutboxEvent.Status.FAILED);
        assertThat(event.getAttempts()).isEqualTo(3);

        event.markPublished();
        assertThat(event.getStatus()).isEqualTo(OutboxEvent.Status.PUBLISHED);
    }
}
