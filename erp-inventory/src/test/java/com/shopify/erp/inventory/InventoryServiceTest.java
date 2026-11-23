package com.shopify.erp.inventory;

import com.shopify.erp.common.ConflictException;
import com.shopify.erp.common.NotFoundException;
import com.shopify.erp.common.ObjectIds;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class InventoryServiceTest {

    private static final String ORDER = "aaaaaaaaaaaaaaaaaaaaaaaa";

    @Mock
    InventoryItemRepository items;
    @Mock
    StockReservationRepository reservations;
    @InjectMocks
    InventoryService service;

    private static InventoryItem item(String sku, int onHand) {
        return new InventoryItem(sku, ObjectIds.newId(), sku, new BigDecimal("10.00"), new BigDecimal("5.00"), onHand, 0, 0);
    }

    // ---------- create ----------

    @Test
    void createRejectsADuplicateSku() {
        when(items.existsById("A")).thenReturn(true);

        assertThatThrownBy(() -> service.create(new InventoryService.CreateItem(
                "A", null, "A", BigDecimal.ONE, BigDecimal.ONE, 1, 0, 0)))
                .isInstanceOf(ConflictException.class);
        verify(items, never()).saveAndFlush(any());
    }

    @Test
    void createRejectsAProductIdUsedByAnotherSku() {
        String productId = ObjectIds.newId();
        when(items.existsByProductId(productId)).thenReturn(true);

        assertThatThrownBy(() -> service.create(new InventoryService.CreateItem(
                "A", productId, "A", BigDecimal.ONE, BigDecimal.ONE, 1, 0, 0)))
                .isInstanceOf(ConflictException.class)
                .hasMessageContaining(productId);
    }

    @Test
    void createGeneratesAProductIdAndNormalisesMoneyToTwoDecimals() {
        when(items.saveAndFlush(any())).thenAnswer(i -> i.getArgument(0));

        InventoryItem created = service.create(new InventoryService.CreateItem(
                "A", null, "Item A", new BigDecimal("499"), new BigDecimal("250.5"), 7, 2, 10));

        assertThat(created.getProductId()).matches(ObjectIds.PATTERN);
        assertThat(created.getUnitPrice()).isEqualByComparingTo("499.00").hasScaleOf(2);
        assertThat(created.getUnitCost()).hasScaleOf(2);
        assertThat(created.getOnHand()).isEqualTo(7);
    }

    @Test
    void getUnknownSkuIsNotFound() {
        when(items.findById("NOPE")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> service.get("NOPE")).isInstanceOf(NotFoundException.class);
    }

    // ---------- reserve ----------

    @Test
    void reserveIsIdempotentPerOrder() {
        when(reservations.existsByOrderId(ORDER)).thenReturn(true);

        service.reserve(ORDER, Map.of("A", 1));

        verifyNoInteractions(items);
        verify(reservations, never()).save(any());
    }

    @Test
    void reserveLocksSkusInSortedOrderToAvoidDeadlocks() {
        InventoryItem a = item("A", 5);
        InventoryItem b = item("B", 5);
        when(items.findById("A")).thenReturn(Optional.of(a));
        when(items.findById("B")).thenReturn(Optional.of(b));
        Map<String, Integer> request = new LinkedHashMap<>();
        request.put("B", 1); // caller order B, A
        request.put("A", 2);

        service.reserve(ORDER, request);

        InOrder order = inOrder(items);
        order.verify(items).findById("A");
        order.verify(items).saveAndFlush(a);
        order.verify(items).findById("B");
        order.verify(items).saveAndFlush(b);
        assertThat(a.getReserved()).isEqualTo(2);
        assertThat(b.getReserved()).isEqualTo(1);

        ArgumentCaptor<StockReservation> saved = ArgumentCaptor.forClass(StockReservation.class);
        verify(reservations, org.mockito.Mockito.times(2)).save(saved.capture());
        assertThat(saved.getAllValues()).extracting(StockReservation::getSku).containsExactly("A", "B");
        assertThat(saved.getAllValues()).allSatisfy(r -> {
            assertThat(r.getOrderId()).isEqualTo(ORDER);
            assertThat(r.getStatus()).isEqualTo(StockReservation.Status.RESERVED);
        });
    }

    @Test
    void reserveFailsOnAnUnknownSku() {
        when(items.findById("GHOST")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> service.reserve(ORDER, Map.of("GHOST", 1))).isInstanceOf(NotFoundException.class);
        verify(reservations, never()).save(any());
    }

    @Test
    void reserveFailsWhenStockIsShort() {
        when(items.findById("A")).thenReturn(Optional.of(item("A", 1)));

        assertThatThrownBy(() -> service.reserve(ORDER, Map.of("A", 2))).isInstanceOf(InsufficientStockException.class);
        verify(reservations, never()).save(any());
    }

    // ---------- ship / release / receive ----------

    @Test
    void commitShipmentWithoutAnyReservationIsAnError() {
        when(reservations.findByOrderIdAndStatus(ORDER, StockReservation.Status.RESERVED)).thenReturn(List.of());
        when(reservations.findByOrderIdAndStatus(ORDER, StockReservation.Status.COMMITTED)).thenReturn(List.of());

        assertThatThrownBy(() -> service.commitShipment(ORDER)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void commitShipmentTurnsReservationsIntoShippedStock() {
        InventoryItem a = item("A", 10);
        a.reserve(3);
        StockReservation r = new StockReservation(ORDER, "A", 3);
        when(reservations.findByOrderIdAndStatus(ORDER, StockReservation.Status.RESERVED)).thenReturn(List.of(r));
        when(items.findById("A")).thenReturn(Optional.of(a));

        service.commitShipment(ORDER);

        assertThat(r.getStatus()).isEqualTo(StockReservation.Status.COMMITTED);
        assertThat(a.getOnHand()).isEqualTo(7);
        assertThat(a.getReserved()).isZero();
    }

    @Test
    void commitShipmentIsIdempotentOnceEverythingIsShipped() {
        when(reservations.findByOrderIdAndStatus(ORDER, StockReservation.Status.RESERVED)).thenReturn(List.of());
        when(reservations.findByOrderIdAndStatus(ORDER, StockReservation.Status.COMMITTED))
                .thenReturn(List.of(new StockReservation(ORDER, "A", 1)));

        service.commitShipment(ORDER);

        verifyNoInteractions(items);
    }

    @Test
    void releaseOnlyTouchesOpenReservations() {
        InventoryItem a = item("A", 10);
        a.reserve(4);
        StockReservation r = new StockReservation(ORDER, "A", 4);
        when(reservations.findByOrderIdAndStatus(ORDER, StockReservation.Status.RESERVED)).thenReturn(List.of(r));
        when(items.findById("A")).thenReturn(Optional.of(a));

        service.release(ORDER);

        assertThat(r.getStatus()).isEqualTo(StockReservation.Status.RELEASED);
        assertThat(a.getReserved()).isZero();
        assertThat(a.getOnHand()).isEqualTo(10);
    }

    @Test
    void aReservationCannotChangeStateTwice() {
        StockReservation r = new StockReservation(ORDER, "A", 1);
        r.markCommitted();

        assertThatThrownBy(r::markReleased).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(r::markCommitted).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void receiveAddsToOnHand() {
        InventoryItem a = item("A", 1);
        when(items.findById("A")).thenReturn(Optional.of(a));

        service.receive("A", 9);

        assertThat(a.getOnHand()).isEqualTo(10);
        verify(items).saveAndFlush(a);
    }

    @Test
    void findAllOrFailKeepsTheCallersOrderAndFailsOnTheFirstUnknownSku() {
        when(items.findById("B")).thenReturn(Optional.of(item("B", 1)));
        when(items.findById("A")).thenReturn(Optional.of(item("A", 1)));

        assertThat(service.findAllOrFail(List.of("B", "A")).keySet()).containsExactly("B", "A");

        when(items.findById("X")).thenReturn(Optional.empty());
        assertThatThrownBy(() -> service.findAllOrFail(List.of("A", "X"))).isInstanceOf(NotFoundException.class);
    }
}
