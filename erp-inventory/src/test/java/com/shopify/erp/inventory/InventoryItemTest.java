package com.shopify.erp.inventory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** U-1, U-2: stock arithmetic of a single SKU. available = onHand - reserved, and it can never go negative. */
class InventoryItemTest {

    private static InventoryItem item(int onHand, int reorderPoint, int reorderQty) {
        return new InventoryItem("SKU-1", "64b7f0c2a1b2c3d4e5f60718", "Red T-shirt",
                new BigDecimal("499.00"), new BigDecimal("250.00"), onHand, reorderPoint, reorderQty);
    }

    @Test
    void newItemHasEverythingAvailable() {
        InventoryItem item = item(10, 2, 5);

        assertThat(item.getOnHand()).isEqualTo(10);
        assertThat(item.getReserved()).isZero();
        assertThat(item.available()).isEqualTo(10);
    }

    @Test
    void reserveMovesStockFromAvailableToReservedWithoutShippingIt() {
        InventoryItem item = item(10, 2, 5);

        item.reserve(4);

        assertThat(item.getReserved()).isEqualTo(4);
        assertThat(item.getOnHand()).isEqualTo(10);
        assertThat(item.available()).isEqualTo(6);
    }

    @Test
    void reservingExactlyTheLastUnitsIsAllowed() {
        InventoryItem item = item(3, 0, 0);

        item.reserve(3);

        assertThat(item.available()).isZero();
    }

    @Test
    void reservingMoreThanAvailableFailsAndChangesNothing() {
        InventoryItem item = item(5, 0, 0);
        item.reserve(3);

        assertThatThrownBy(() -> item.reserve(3))
                .isInstanceOf(InsufficientStockException.class)
                .hasMessageContaining("SKU-1")
                .hasMessageContaining("requested 3")
                .hasMessageContaining("available 2");

        assertThat(item.getReserved()).isEqualTo(3);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1, -100})
    void everyOperationRejectsNonPositiveQuantities(int qty) {
        InventoryItem item = item(10, 0, 0);
        item.reserve(5);

        assertThatThrownBy(() -> item.reserve(qty)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> item.release(qty)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> item.commitShipment(qty)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> item.receive(qty)).isInstanceOf(IllegalArgumentException.class);
        assertThat(item.getOnHand()).isEqualTo(10);
        assertThat(item.getReserved()).isEqualTo(5);
    }

    @Test
    void shipmentLowersBothReservedAndOnHand() {
        InventoryItem item = item(10, 0, 0);
        item.reserve(4);

        item.commitShipment(4);

        assertThat(item.getReserved()).isZero();
        assertThat(item.getOnHand()).isEqualTo(6);
        assertThat(item.available()).isEqualTo(6);
    }

    @Test
    void cannotShipMoreThanWasReserved() {
        InventoryItem item = item(10, 0, 0);
        item.reserve(2);

        assertThatThrownBy(() -> item.commitShipment(3)).isInstanceOf(IllegalStateException.class);
        assertThat(item.getOnHand()).isEqualTo(10);
    }

    @Test
    void releaseGivesTheReservationBack() {
        InventoryItem item = item(10, 0, 0);
        item.reserve(4);

        item.release(4);

        assertThat(item.getReserved()).isZero();
        assertThat(item.available()).isEqualTo(10);
    }

    @Test
    void cannotReleaseMoreThanWasReserved() {
        InventoryItem item = item(10, 0, 0);
        item.reserve(1);

        assertThatThrownBy(() -> item.release(2)).isInstanceOf(IllegalStateException.class);
        assertThat(item.getReserved()).isEqualTo(1);
    }

    @Test
    void goodsReceiptAddsToOnHand() {
        InventoryItem item = item(1, 0, 0);

        item.receive(20);

        assertThat(item.getOnHand()).isEqualTo(21);
    }

    @Test
    void needsReorderWhenAvailableDropsToTheReorderPoint() {
        InventoryItem item = item(10, 3, 20);
        assertThat(item.needsReorder()).isFalse();

        item.reserve(7); // available 3 == reorder point

        assertThat(item.needsReorder()).isTrue();
    }

    @Test
    void anItemWithoutReorderQuantityIsNeverSuggested() {
        InventoryItem item = item(0, 5, 0);

        assertThat(item.needsReorder()).isFalse();
    }
}
