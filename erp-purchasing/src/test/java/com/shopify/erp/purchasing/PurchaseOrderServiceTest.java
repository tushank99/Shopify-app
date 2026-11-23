package com.shopify.erp.purchasing;

import com.shopify.erp.common.ConflictException;
import com.shopify.erp.common.NotFoundException;
import com.shopify.erp.inventory.InventoryItem;
import com.shopify.erp.inventory.InventoryService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.transaction.PlatformTransactionManager;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PurchaseOrderServiceTest {

    @Mock
    PurchaseOrderRepository orders;
    @Mock
    InventoryService inventory;

    PurchaseOrderService service;

    @BeforeEach
    void setUp() {
        // A mocked transaction manager: TransactionTemplate just runs the callback (no real database here).
        service = new PurchaseOrderService(orders, inventory, mock(PlatformTransactionManager.class));
        lenient().when(orders.save(any())).thenAnswer(i -> i.getArgument(0));
        lenient().when(orders.saveAndFlush(any())).thenAnswer(i -> i.getArgument(0));
    }

    private static PurchaseOrder approvedPo() {
        PurchaseOrder po = new PurchaseOrder("Acme");
        po.addLine("A", 10, new BigDecimal("2.00"));
        po.addLine("B", 5, new BigDecimal("3.00"));
        po.approve();
        return po;
    }

    @Test
    void createMergesDuplicateSkusIntoOneLine() {
        PurchaseOrderView view = service.create("Acme", List.of(
                new PurchaseOrderService.Line("A", 2, new BigDecimal("1.00")),
                new PurchaseOrderService.Line("A", 3, new BigDecimal("1.00")),
                new PurchaseOrderService.Line("B", 1, new BigDecimal("4.00"))));

        assertThat(view.status()).isEqualTo("DRAFT");
        assertThat(view.lines()).extracting(PurchaseOrderView.LineView::sku).containsExactly("A", "B");
        assertThat(view.lines().get(0).quantity()).isEqualTo(5);
        assertThat(view.total()).isEqualByComparingTo("9.00");
    }

    @Test
    void createFailsOnUnknownSku() {
        when(inventory.get("GHOST")).thenThrow(new NotFoundException("Unknown SKU GHOST"));

        assertThatThrownBy(() -> service.create("Acme",
                List.of(new PurchaseOrderService.Line("GHOST", 1, BigDecimal.ONE))))
                .isInstanceOf(NotFoundException.class);
        verify(orders, never()).save(any());
    }

    @Test
    void reorderSuggestionUsesReorderQtyAndStandardCost() {
        InventoryItem low = new InventoryItem("A", "64b7f0c2a1b2c3d4e5f60718", "A",
                new BigDecimal("10.00"), new BigDecimal("4.00"), 1, 3, 20);
        when(inventory.lowStock()).thenReturn(List.of(low));

        PurchaseOrderView view = service.createFromLowStock("Acme");

        assertThat(view.lines()).singleElement().satisfies(l -> {
            assertThat(l.sku()).isEqualTo("A");
            assertThat(l.quantity()).isEqualTo(20);
            assertThat(l.unitCost()).isEqualByComparingTo("4.00");
        });
    }

    @Test
    void reorderSuggestionWithNothingLowIsAConflict() {
        when(inventory.lowStock()).thenReturn(List.of());

        assertThatThrownBy(() -> service.createFromLowStock("Acme")).isInstanceOf(ConflictException.class);
    }

    @Test
    void receiveAddsEveryLineToStockAndMarksTheOrderReceived() {
        PurchaseOrder po = approvedPo();
        when(orders.findById(1L)).thenReturn(Optional.of(po));

        PurchaseOrderView view = service.receive(1L);

        assertThat(view.status()).isEqualTo("RECEIVED");
        verify(inventory).receive("A", 10);
        verify(inventory).receive("B", 5);
    }

    @Test
    void receivingADraftIsRejectedAndStockIsUntouched() {
        PurchaseOrder po = new PurchaseOrder("Acme");
        po.addLine("A", 10, BigDecimal.ONE);
        when(orders.findById(1L)).thenReturn(Optional.of(po));

        assertThatThrownBy(() -> service.receive(1L)).isInstanceOf(ConflictException.class);
        verify(inventory, never()).receive(anyString(), anyInt());
    }

    @Test
    void anOptimisticLockConflictIsRetriedWithFreshData() {
        when(orders.findById(1L)).thenAnswer(i -> Optional.of(approvedPo()));
        when(orders.saveAndFlush(any()))
                .thenThrow(new OptimisticLockingFailureException("someone else approved it"))
                .thenAnswer(i -> i.getArgument(0));

        PurchaseOrderView view = service.receive(1L);

        assertThat(view.status()).isEqualTo("RECEIVED");
        verify(orders, times(2)).findById(1L); // re-read on the retry
    }

    @Test
    void approveAndCancel() {
        PurchaseOrder po = new PurchaseOrder("Acme");
        po.addLine("A", 1, BigDecimal.ONE);
        when(orders.findById(7L)).thenReturn(Optional.of(po));

        assertThat(service.approve(7L).status()).isEqualTo("APPROVED");
        assertThat(service.cancel(7L).status()).isEqualTo("CANCELLED");
    }

    @Test
    void unknownPurchaseOrderIsNotFound() {
        when(orders.findById(99L)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> service.get(99L)).isInstanceOf(NotFoundException.class);
    }
}
