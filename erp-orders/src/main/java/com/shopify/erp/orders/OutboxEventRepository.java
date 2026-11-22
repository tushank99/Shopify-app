package com.shopify.erp.orders;

import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, Long> {

    boolean existsByAggregateId(String aggregateId);

    /**
     * Claims pending rows with SELECT ... FOR UPDATE SKIP LOCKED (lock timeout -2): several service instances can
     * publish concurrently and never pick the same row.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints(@QueryHint(name = "jakarta.persistence.lock.timeout", value = "-2"))
    @Query("select e from OutboxEvent e where e.status = :status order by e.id")
    List<OutboxEvent> lockByStatus(@Param("status") OutboxEvent.Status status, Pageable page);

    /** Compensation: stop a not-yet-published event. Returns 0 when it was already published (or never existed). */
    @Modifying
    @Query("update OutboxEvent e set e.status = com.shopify.erp.orders.OutboxEvent.Status.CANCELLED "
            + "where e.aggregateId = :aggregateId and e.status = com.shopify.erp.orders.OutboxEvent.Status.PENDING")
    int cancelPending(@Param("aggregateId") String aggregateId);
}
