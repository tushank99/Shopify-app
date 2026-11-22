package com.shopify.erp.inventory;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface StockReservationRepository extends JpaRepository<StockReservation, Long> {

    boolean existsByOrderId(String orderId);

    List<StockReservation> findByOrderId(String orderId);

    List<StockReservation> findByOrderIdAndStatus(String orderId, StockReservation.Status status);
}
