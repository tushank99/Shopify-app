package com.shopify.erp.purchasing;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.List;

@RestController
@RequestMapping("/api/purchase-orders")
@Tag(name = "Purchase orders", description = "Replenish stock from suppliers")
public class PurchaseOrderController {

    public record LineRequest(
            @NotBlank @Size(max = 64) String sku,
            @NotNull @Min(1) @Max(1_000_000) Integer quantity,
            @NotNull @DecimalMin("0.00") @DecimalMax("9999999999.99") @Digits(integer = 10, fraction = 2) BigDecimal unitCost) {
    }

    public record CreateRequest(
            @NotBlank @Size(max = 120) String supplier,
            @NotEmpty @Size(max = 200) List<@Valid LineRequest> lines) {
    }

    public record SuggestRequest(@NotBlank @Size(max = 120) String supplier) {
    }

    private final PurchaseOrderService service;

    public PurchaseOrderController(PurchaseOrderService service) {
        this.service = service;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    @Operation(summary = "Create a DRAFT purchase order")
    public PurchaseOrderView create(@Valid @RequestBody CreateRequest req) {
        return service.create(req.supplier(), req.lines().stream()
                .map(l -> new PurchaseOrderService.Line(l.sku(), l.quantity(), l.unitCost())).toList());
    }

    @PostMapping("/reorder-suggestions")
    @ResponseStatus(HttpStatus.CREATED)
    @Operation(summary = "Create a DRAFT purchase order for every item at/below its reorder point")
    public PurchaseOrderView suggest(@Valid @RequestBody SuggestRequest req) {
        return service.createFromLowStock(req.supplier());
    }

    @GetMapping
    @Operation(summary = "List purchase orders, newest first")
    public List<PurchaseOrderView> list() {
        return service.list();
    }

    @GetMapping("/{id}")
    @Operation(summary = "Get one purchase order")
    public PurchaseOrderView get(@PathVariable Long id) {
        return service.get(id);
    }

    @PostMapping("/{id}/approve")
    @Operation(summary = "DRAFT -> APPROVED")
    public PurchaseOrderView approve(@PathVariable Long id) {
        return service.approve(id);
    }

    @PostMapping("/{id}/receive")
    @Operation(summary = "Goods receipt: APPROVED -> RECEIVED and add the quantities to on-hand stock")
    public PurchaseOrderView receive(@PathVariable Long id) {
        return service.receive(id);
    }

    @PostMapping("/{id}/cancel")
    @Operation(summary = "Cancel a DRAFT or APPROVED purchase order")
    public PurchaseOrderView cancel(@PathVariable Long id) {
        return service.cancel(id);
    }
}
