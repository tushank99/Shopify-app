package com.shopify.erp.common;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class ObjectIdsTest {

    @Test
    void generatedIdsMatchTheContractShapeAndAreUnique() {
        Set<String> ids = new HashSet<>();
        for (int i = 0; i < 1_000; i++) {
            String id = ObjectIds.newId();
            assertThat(id).matches(ObjectIds.PATTERN);
            assertThat(ObjectIds.isValid(id)).isTrue();
            ids.add(id);
        }
        assertThat(ids).hasSize(1_000);
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {
            "64B7F0C2A1B2C3D4E5F60718",   // upper case: the contract is lower-case only
            "64b7f0c2a1b2c3d4e5f6071",    // 23 chars
            "64b7f0c2a1b2c3d4e5f607189",  // 25 chars
            "64b7f0c2a1b2c3d4e5f6071g"    // not hex
    })
    void rejectsAnythingThatIsNot24LowerCaseHex(String value) {
        assertThat(ObjectIds.isValid(value)).isFalse();
    }
}
