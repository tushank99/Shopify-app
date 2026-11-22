package com.shopify.erp.orders;

import com.shopify.erp.common.NotFoundException;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

/** Read side. Maps entities to DTOs inside the transaction (open-in-view is off). */
@Service
@Transactional(readOnly = true)
public class OrderQueryService {

    private final SalesOrderRepository orders;
    private final SagaStepRepository sagaSteps;

    public OrderQueryService(SalesOrderRepository orders, SagaStepRepository sagaSteps) {
        this.orders = orders;
        this.sagaSteps = sagaSteps;
    }

    public OrderResponse get(String orderId) {
        return orders.findById(orderId).map(OrderResponse::from)
                .orElseThrow(() -> new NotFoundException("Order " + orderId + " not found"));
    }

    public Optional<OrderResponse> findByIdempotencyKey(String key) {
        return orders.findByIdempotencyKey(key).map(OrderResponse::from);
    }

    public List<OrderResponse> listForCustomer(String customerId, int limit) {
        return orders.findByCustomerIdOrderByCreatedAtDesc(customerId, PageRequest.of(0, limit))
                .stream().map(OrderResponse::from).toList();
    }

    public List<SagaStepResponse> saga(String orderId) {
        if (!orders.existsById(orderId)) {
            throw new NotFoundException("Order " + orderId + " not found");
        }
        return sagaSteps.findByOrderIdOrderByIdAsc(orderId).stream().map(SagaStepResponse::from).toList();
    }
}
