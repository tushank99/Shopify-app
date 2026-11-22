package com.shopify.erp.orders;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface SalesOrderRepository extends JpaRepository<SalesOrder, String> {

    Optional<SalesOrder> findByIdempotencyKey(String idempotencyKey);

    List<SalesOrder> findByCustomerIdOrderByCreatedAtDesc(String customerId, Pageable pageable);
}
