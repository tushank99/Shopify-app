package com.shopify.erp.common;

import org.junit.jupiter.api.Test;
import org.springframework.dao.OptimisticLockingFailureException;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** U-5: the retry wraps a whole unit of work and only retries optimistic-lock conflicts. */
class OptimisticRetryTest {

    @Test
    void returnsImmediatelyWhenTheFirstAttemptSucceeds() {
        AtomicInteger calls = new AtomicInteger();

        String result = OptimisticRetry.run(5, () -> {
            calls.incrementAndGet();
            return "ok";
        });

        assertThat(result).isEqualTo("ok");
        assertThat(calls).hasValue(1);
    }

    @Test
    void retriesAfterLosingARaceAndReturnsTheLaterResult() {
        AtomicInteger calls = new AtomicInteger();

        String result = OptimisticRetry.run(5, () -> {
            if (calls.incrementAndGet() < 3) {
                throw new OptimisticLockingFailureException("version mismatch");
            }
            return "won on attempt " + calls.get();
        });

        assertThat(result).isEqualTo("won on attempt 3");
        assertThat(calls).hasValue(3);
    }

    @Test
    void givesUpAfterMaxAttemptsWithAConflictTheApiMapsTo409() {
        AtomicInteger calls = new AtomicInteger();

        assertThatThrownBy(() -> OptimisticRetry.run(4, () -> {
            calls.incrementAndGet();
            throw new OptimisticLockingFailureException("always loses");
        }))
                .isInstanceOf(ConcurrentUpdateException.class)
                .isInstanceOf(ConflictException.class)
                .hasMessageContaining("4 attempts");

        assertThat(calls).hasValue(4);
    }

    @Test
    void doesNotRetryOtherExceptions() {
        AtomicInteger calls = new AtomicInteger();

        assertThatThrownBy(() -> OptimisticRetry.run(5, () -> {
            calls.incrementAndGet();
            throw new IllegalStateException("business rule");
        })).isInstanceOf(IllegalStateException.class).hasMessage("business rule");

        assertThat(calls).hasValue(1);
    }

    @Test
    void keepsTheInterruptFlagWhenInterruptedDuringBackoff() {
        AtomicInteger calls = new AtomicInteger();
        Thread.currentThread().interrupt();
        try {
            String result = OptimisticRetry.run(3, () -> {
                if (calls.incrementAndGet() == 1) {
                    throw new OptimisticLockingFailureException("lost");
                }
                return "done";
            });
            assertThat(result).isEqualTo("done");
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted(); // clear for the next test
        }
    }
}
