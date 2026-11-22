package com.shopify.erp.orders;

import java.time.Instant;

public record SagaStepResponse(String step, String outcome, String detail, Instant at) {

    static SagaStepResponse from(SagaStep s) {
        return new SagaStepResponse(s.getStep(), s.getOutcome().name(), s.getDetail(), s.getCreatedAt());
    }
}
