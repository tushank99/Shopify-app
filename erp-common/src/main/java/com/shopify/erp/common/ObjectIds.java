package com.shopify.erp.common;

import java.security.SecureRandom;
import java.util.HexFormat;
import java.util.regex.Pattern;

/**
 * 24-character lower-case hex identifiers (same shape as a Mongo ObjectId).
 * The shared order.placed contract (contracts/order.placed.v1.schema.json) requires this shape for orderId, userId
 * and productId, so ERP orders can flow into the existing ledger-service unchanged.
 */
public final class ObjectIds {

    public static final String PATTERN = "^[0-9a-f]{24}$";
    private static final Pattern COMPILED = Pattern.compile(PATTERN);
    private static final SecureRandom RANDOM = new SecureRandom();

    private ObjectIds() {
    }

    public static String newId() {
        byte[] bytes = new byte[12];
        RANDOM.nextBytes(bytes);
        return HexFormat.of().formatHex(bytes);
    }

    public static boolean isValid(String value) {
        return value != null && COMPILED.matcher(value).matches();
    }
}
