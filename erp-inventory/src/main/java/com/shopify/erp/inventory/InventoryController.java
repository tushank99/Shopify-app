package com.shopify.erp.inventory;

import com.shopify.erp.common.ObjectIds;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
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
import java.time.Instant;
import java.util.List;

@RestController
@RequestMapping("/api/inventory")
@Tag(name = "Inventory", description = "Stock levels, reservations and reorder alerts")
public class InventoryController {

    public record CreateItemRequest(
            @NotBlank @Size(max = 64) @Pattern(regexp = "^[A-Za-z0-9._-]+$", message = "letters, digits, . _ - only") String sku,
            @Pattern(regexp = ObjectIds.PATTERN, message = "must be 24 lower-case hex characters") String productId,
            @NotBlank @Size(max = 200) String name,
            @NotNull @DecimalMin("0.00") @DecimalMax("9999999999.99") @Digits(integer = 10, fraction = 2) BigDecimal unitPrice,
            @DecimalMin("0.00") @DecimalMax("9999999999.99") @Digits(integer = 10, fraction = 2) BigDecimal unitCost,
            @Min(0) @Max(100_000_000) Integer initialStock,
            @Min(0) @Max(100_000_000) Integer reorderPoint,
            @Min(0) @Max(100_000_000) Integer reorderQty) {
    }

    public record ItemResponse(String sku, String productId, String name, BigDecimal unitPrice, BigDecimal unitCost,
                               int onHand, int reserved, int available, int reorderPoint, int reorderQty,
                               boolean needsReorder, long version, Instant updatedAt) {
        static ItemResponse from(InventoryItem i) {
            return new ItemResponse(i.getSku(), i.getProductId(), i.getName(), i.getUnitPrice(), i.getUnitCost(),
                    i.getOnHand(), i.getReserved(), i.available(), i.getReorderPoint(), i.getReorderQty(),
                    i.needsReorder(), i.getVersion(), i.getUpdatedAt());
        }
    }

    public record ReservationResponse(Long id, String orderId, String sku, int quantity, String status) {
        static ReservationResponse from(StockReservation r) {
            return new ReservationResponse(r.getId(), r.getOrderId(), r.getSku(), r.getQuantity(), r.getStatus().name());
        }
    }

    private final InventoryService inventory;

    public InventoryController(InventoryService inventory) {
        this.inventory = inventory;
    }

    @PostMapping("/items")
    @ResponseStatus(HttpStatus.CREATED)
    @Operation(summary = "Create an inventory item (SKU) with opening stock and reorder rules")
    public ItemResponse create(@Valid @RequestBody CreateItemRequest req) {
        return ItemResponse.from(inventory.create(new InventoryService.CreateItem(
                req.sku(), req.productId(), req.name(), req.unitPrice(),
                req.unitCost() != null ? req.unitCost() : BigDecimal.ZERO,
                req.initialStock() != null ? req.initialStock() : 0,
                req.reorderPoint() != null ? req.reorderPoint() : 0,
                req.reorderQty() != null ? req.reorderQty() : 0)));
    }

    @GetMapping("/items")
    @Operation(summary = "List all items with on-hand, reserved and available quantities")
    public List<ItemResponse> list() {
        return inventory.list().stream().map(ItemResponse::from).toList();
    }

    @GetMapping("/items/{sku}")
    @Operation(summary = "Stock position of one SKU")
    public ItemResponse get(@PathVariable String sku) {
        return ItemResponse.from(inventory.get(sku));
    }

    @GetMapping("/low-stock")
    @Operation(summary = "Items at or below their reorder point (candidates for a purchase order)")
    public List<ItemResponse> lowStock() {
        return inventory.lowStock().stream().map(ItemResponse::from).toList();
    }

    @GetMapping("/reservations/{orderId}")
    @Operation(summary = "Stock reservations held for an order")
    public List<ReservationResponse> reservations(@PathVariable String orderId) {
        return inventory.reservationsFor(orderId).stream().map(ReservationResponse::from).toList();
    }
}
