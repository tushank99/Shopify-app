package com.shopify.erp;

import com.shopify.erp.common.ConflictException;
import com.shopify.erp.common.NotFoundException;
import com.shopify.erp.orders.SagaFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.method.ParameterErrors;
import org.springframework.validation.method.ParameterValidationResult;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.HandlerMethodValidationException;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One place that turns exceptions into RFC 7807 problem responses. Unknown exceptions return a generic 500: the
 * real message goes to the log, never to the client.
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(NotFoundException.class)
    ProblemDetail notFound(NotFoundException e) {
        return problem(HttpStatus.NOT_FOUND, e.getMessage());
    }

    @ExceptionHandler(ConflictException.class)
    ProblemDetail conflict(ConflictException e) {
        return problem(HttpStatus.CONFLICT, e.getMessage());
    }

    @ExceptionHandler(SagaFailedException.class)
    ProblemDetail sagaFailed(SagaFailedException e) {
        log.error("Saga failed: {}", e.getMessage());
        ProblemDetail p = problem(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage());
        p.setProperty("orderId", e.getOrderId());
        p.setProperty("failedStep", e.getFailedStep());
        return p;
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    ProblemDetail invalidBody(MethodArgumentNotValidException e) {
        Map<String, String> errors = new LinkedHashMap<>();
        e.getBindingResult().getFieldErrors().forEach(f -> errors.putIfAbsent(f.getField(), f.getDefaultMessage()));
        ProblemDetail p = problem(HttpStatus.BAD_REQUEST, "Validation failed");
        p.setProperty("errors", errors);
        return p;
    }

    /**
     * Raised instead of MethodArgumentNotValidException when the controller method ALSO has constraints on plain
     * parameters (e.g. @Size on the Idempotency-Key header of POST /api/orders): Spring then validates the whole method
     * and reports body errors here. Field errors must be kept, or clients get a 400 with no hint of what is wrong.
     */
    @ExceptionHandler(HandlerMethodValidationException.class)
    ProblemDetail invalidParam(HandlerMethodValidationException e) {
        Map<String, String> errors = new LinkedHashMap<>();
        for (ParameterValidationResult result : e.getParameterValidationResults()) {
            if (result instanceof ParameterErrors bean) {
                bean.getFieldErrors().forEach(f -> errors.putIfAbsent(f.getField(), f.getDefaultMessage()));
            } else {
                RequestHeader header = result.getMethodParameter().getParameterAnnotation(RequestHeader.class);
                String name = header != null && !header.value().isEmpty()
                        ? header.value() : result.getMethodParameter().getParameterName();
                result.getResolvableErrors().forEach(err -> errors.putIfAbsent(name, err.getDefaultMessage()));
            }
        }
        ProblemDetail p = problem(HttpStatus.BAD_REQUEST, "Validation failed");
        p.setProperty("errors", errors);
        return p;
    }

    @ExceptionHandler({HttpMessageNotReadableException.class, MissingServletRequestParameterException.class,
            MethodArgumentTypeMismatchException.class, IllegalArgumentException.class})
    ProblemDetail badRequest(Exception e) {
        return problem(HttpStatus.BAD_REQUEST, e instanceof IllegalArgumentException ? e.getMessage() : "Malformed request");
    }

    @ExceptionHandler(DataIntegrityViolationException.class)
    ProblemDetail integrity(DataIntegrityViolationException e) {
        log.warn("Data integrity violation: {}", e.getMostSpecificCause().getMessage());
        return problem(HttpStatus.CONFLICT, "The request conflicts with existing data");
    }

    /**
     * The database is temporarily unavailable (connection pool exhausted, lock timeout, deadlock victim). Nothing was
     * committed for this request, so the client may safely retry: 503 + Retry-After, not a generic 500.
     */
    @ExceptionHandler({CannotCreateTransactionException.class, TransientDataAccessException.class})
    ResponseEntity<ProblemDetail> temporarilyUnavailable(Exception e) {
        log.warn("Database temporarily unavailable: {}", e.getMessage());
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header(HttpHeaders.RETRY_AFTER, "1")
                .body(problem(HttpStatus.SERVICE_UNAVAILABLE, "Temporarily unavailable, please retry"));
    }

    @ExceptionHandler(Exception.class)
    ProblemDetail unexpected(Exception e) {
        log.error("Unhandled error", e);
        return problem(HttpStatus.INTERNAL_SERVER_ERROR, "Unexpected error");
    }

    private static ProblemDetail problem(HttpStatus status, String detail) {
        return ProblemDetail.forStatusAndDetail(status, detail);
    }
}
