package com.shopify.erp.common;

import org.springframework.dao.OptimisticLockingFailureException;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

/**
 * Re-runs a whole unit of work when it loses an optimistic-lock race ({@code @Version} mismatch).
 *
 * The retry must wrap the TRANSACTION (not run inside it): a failed transaction is rolled back and the
 * work is re-read from the database, so the retry sees the winner's committed state.
 */
public final class OptimisticRetry {

    private OptimisticRetry() {
    }

    public static <T> T run(int maxAttempts, Supplier<T> unitOfWork) {
        for (int attempt = 1; ; attempt++) {
            try {
                return unitOfWork.get();
            } catch (OptimisticLockingFailureException e) {
                if (attempt >= maxAttempts) {
                    throw new ConcurrentUpdateException(
                            "Concurrent update on the same record, gave up after " + maxAttempts + " attempts. Please retry.");
                }
                backoff(attempt);
            }
        }
    }

    private static void backoff(int attempt) {
        try {
            // small jittered backoff so two losers do not collide again in lockstep
            Thread.sleep(ThreadLocalRandom.current().nextLong(5L * attempt, 25L * attempt + 1));
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }
}
