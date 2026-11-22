package com.shopify.erp.orders;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface SagaStepRepository extends JpaRepository<SagaStep, Long> {

    List<SagaStep> findByOrderIdOrderByIdAsc(String orderId);
}
