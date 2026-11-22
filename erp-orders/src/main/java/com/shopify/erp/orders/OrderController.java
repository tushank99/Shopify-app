package com.shopify.erp.orders;

import com.shopify.erp.common.ObjectIds;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/orders")
@Tag(name = "Orders", description = "Order-to-cash: place an order and follow its saga")
public class OrderController {

    public record LineRequest(
            @NotBlank @Size(max = 64) String sku,
            @NotNull @Min(1) @Max(1_000_000) Integer quantity) {
    }

    public record PlaceOrderRequest(
            @NotBlank @Pattern(regexp = ObjectIds.PATTERN, message = "must be 24 lower-case hex characters") String customerId,
            @NotEmpty @Size(max = 100) List<@Valid LineRequest> lines) {
    }

    private final OrderSagaService saga;
    private final OrderQueryService queries;

    public OrderController(OrderSagaService saga, OrderQueryService queries) {
        this.saga = saga;
        this.queries = queries;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    @Operation(summary = "Place an order",
            description = "Runs the saga: reserve stock -> post to ledger -> ship. Any failure compensates (stock released, "
                    + "order CANCELLED). Send an Idempotency-Key header to make client retries safe.")
    public OrderResponse place(
            @RequestHeader(value = "Idempotency-Key", required = false) @Size(min = 1, max = 64) String idempotencyKey,
            @Valid @RequestBody PlaceOrderRequest req) {
        return saga.placeOrder(new OrderSagaService.PlaceOrder(req.customerId(), idempotencyKey,
                req.lines().stream().map(l -> new OrderSagaService.Line(l.sku(), l.quantity())).toList()));
    }

    @GetMapping("/{orderId}")
    @Operation(summary = "Get an order with its lines and current status")
    public OrderResponse get(@PathVariable @Pattern(regexp = ObjectIds.PATTERN) String orderId) {
        return queries.get(orderId);
    }

    @GetMapping("/{orderId}/saga")
    @Operation(summary = "Saga audit trail: every step and compensation for this order")
    public List<SagaStepResponse> sagaSteps(@PathVariable @Pattern(regexp = ObjectIds.PATTERN) String orderId) {
        return queries.saga(orderId);
    }

    @GetMapping
    @Operation(summary = "A customer's orders, newest first")
    public List<OrderResponse> listForCustomer(
            @RequestParam @Pattern(regexp = ObjectIds.PATTERN) String customerId,
            @RequestParam(defaultValue = "20") @Min(1) @Max(100) int limit) {
        return queries.listForCustomer(customerId, limit);
    }
}
