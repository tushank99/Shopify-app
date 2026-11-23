package com.shopify.erp.purchasing;

import com.shopify.erp.common.ConflictException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** U-4: DRAFT -> APPROVED -> RECEIVED, CANCELLED only from DRAFT/APPROVED. Wrong-state actions are 409s. */
class PurchaseOrderTest {

    private static PurchaseOrder draft() {
        PurchaseOrder po = new PurchaseOrder("Acme Textiles");
        po.addLine("A", 10, new BigDecimal("2.5"));
        po.addLine("B", 3, new BigDecimal("100.00"));
        return po;
    }

    @Test
    void happyPathDraftApprovedReceived() {
        PurchaseOrder po = draft();
        assertThat(po.getStatus()).isEqualTo(PurchaseOrder.Status.DRAFT);

        po.approve();
        assertThat(po.getStatus()).isEqualTo(PurchaseOrder.Status.APPROVED);

        po.markReceived();
        assertThat(po.getStatus()).isEqualTo(PurchaseOrder.Status.RECEIVED);
    }

    @Test
    void cannotReceiveADraft() {
        assertThatThrownBy(() -> draft().markReceived()).isInstanceOf(ConflictException.class);
    }

    @Test
    void cannotApproveTwice() {
        PurchaseOrder po = draft();
        po.approve();

        assertThatThrownBy(po::approve).isInstanceOf(ConflictException.class).hasMessageContaining("APPROVED");
    }

    @Test
    void cannotReceiveTwice() {
        PurchaseOrder po = draft();
        po.approve();
        po.markReceived();

        assertThatThrownBy(po::markReceived).isInstanceOf(ConflictException.class);
    }

    @Test
    void draftAndApprovedCanBeCancelled() {
        PurchaseOrder d = draft();
        d.cancel();
        assertThat(d.getStatus()).isEqualTo(PurchaseOrder.Status.CANCELLED);

        PurchaseOrder a = draft();
        a.approve();
        a.cancel();
        assertThat(a.getStatus()).isEqualTo(PurchaseOrder.Status.CANCELLED);
    }

    @Test
    void receivedOrCancelledCannotBeCancelledOrApproved() {
        PurchaseOrder received = draft();
        received.approve();
        received.markReceived();
        assertThatThrownBy(received::cancel).isInstanceOf(ConflictException.class);

        PurchaseOrder cancelled = draft();
        cancelled.cancel();
        assertThatThrownBy(cancelled::approve).isInstanceOf(ConflictException.class);
        assertThatThrownBy(cancelled::cancel).isInstanceOf(ConflictException.class);
    }

    @Test
    void totalIsTheSumOfLinesAtTwoDecimals() {
        PurchaseOrder po = draft();

        assertThat(po.total()).isEqualByComparingTo("325.00");
        assertThat(po.getLines()).extracting(PurchaseOrderLine::getUnitCost)
                .allSatisfy(c -> assertThat(c.scale()).isEqualTo(2));
    }
}
