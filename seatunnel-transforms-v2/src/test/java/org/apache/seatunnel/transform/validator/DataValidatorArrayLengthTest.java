/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.transform.validator;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.exception.TransformException;
import org.apache.seatunnel.transform.validator.rule.LengthValidationRule;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DataValidatorArrayLengthTest {
    static Stream<Arguments> arrays() {
        return Stream.<Object>of(
                        new Integer[] {10, 20},
                        new Long[] {10L, 20L},
                        new String[] {"a long value", "b"},
                        new Boolean[] {true, false},
                        new BigDecimal[] {BigDecimal.ONE, BigDecimal.TEN},
                        new Object[] {null, null},
                        new Integer[][] {{1, 2, 3}, {4}},
                        new byte[] {1, 2},
                        new short[] {1, 2},
                        new int[] {1, 2},
                        new long[] {1, 2},
                        new float[] {1, 2},
                        new double[] {1, 2},
                        new boolean[] {true, false},
                        new char[] {'a', 'b'})
                .map(value -> Arguments.of(value));
    }

    @ParameterizedTest
    @MethodSource("arrays")
    void countsArrayElementsInsteadOfTheirStringRepresentation(Object value) {
        LengthValidationRule exact = new LengthValidationRule(2);
        assertTrue(exact.validate(value, null, null).isValid());
        assertTrue(new LengthValidationRule(2, 2).validate(value, null, null).isValid());
        assertFalse(new LengthValidationRule(3, null).validate(value, null, null).isValid());
        assertFalse(new LengthValidationRule(null, 1).validate(value, null, null).isValid());
        assertEquals(
                "Expected length 1 but got 2",
                new LengthValidationRule(1).validate(value, null, null).getErrorMessage());
    }

    @Test
    void preservesEmptyNullStringCollectionAndFallbackBehavior() {
        assertTrue(
                new LengthValidationRule(0)
                        .validate(new Integer[0], ArrayType.INT_ARRAY_TYPE, null)
                        .isValid());
        assertTrue(new LengthValidationRule(0).validate(new byte[0], null, null).isValid());
        assertTrue(
                new LengthValidationRule(2)
                        .validate(null, ArrayType.INT_ARRAY_TYPE, null)
                        .isValid());
        assertTrue(
                new LengthValidationRule(2).validate("ab", BasicType.STRING_TYPE, null).isValid());
        assertTrue(
                new LengthValidationRule(2)
                        .validate("\uD83D\uDE00", BasicType.STRING_TYPE, null)
                        .isValid());
        assertTrue(
                new LengthValidationRule(2)
                        .validate(Arrays.asList("a", "b"), null, null)
                        .isValid());
        assertTrue(new LengthValidationRule(2).validate(12, BasicType.INT_TYPE, null).isValid());
    }

    @Test
    void preservesCustomErrorMessage() {
        LengthValidationRule rule = new LengthValidationRule(1);
        rule.setCustomMessage("wrong item count");
        assertEquals(
                "wrong item count",
                rule.validate(new Integer[] {1, 2}, ArrayType.INT_ARRAY_TYPE, null)
                        .getErrorMessage());
    }

    @ParameterizedTest
    @ValueSource(strings = {"FAIL", "SKIP", "ROUTE_TO_TABLE"})
    void keepsValidArrayRowsInTheNormalPipeline(String policy) {
        DataValidatorTransform transform = transform(policy);
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {new Integer[] {10, 20}});
        row.setTableId("db.source");
        SeaTunnelRow result = transform.map(row);
        assertSame(row, result);
        assertEquals("db.source", result.getTableId());
        assertEquals(
                ArrayType.INT_ARRAY_TYPE,
                transform.getProducedCatalogTable().getSeaTunnelRowType().getFieldType(0));
    }

    @ParameterizedTest
    @ValueSource(strings = {"FAIL", "SKIP", "ROUTE_TO_TABLE"})
    void preservesInvalidRowPolicies(String policy) {
        DataValidatorTransform transform = transform(policy);
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {new Integer[] {10}});
        if ("FAIL".equals(policy)) {
            assertThrows(TransformException.class, () -> transform.map(row));
        } else if ("SKIP".equals(policy)) {
            assertNull(transform.map(row));
        } else {
            assertEquals("db.invalid", transform.map(row).getTableId());
        }
    }

    private static DataValidatorTransform transform(String policy) {
        Map<String, Object> rule = new HashMap<>();
        rule.put("field_name", "items");
        rule.put("rule_type", "LENGTH");
        rule.put("exact_length", 2);
        Map<String, Object> options = new HashMap<>();
        options.put("field_rules", Collections.singletonList(rule));
        options.put("row_error_handle_way", policy);
        options.put("row_error_handle_way.error_table", "invalid");
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"items"},
                        new SeaTunnelDataType<?>[] {ArrayType.INT_ARRAY_TYPE});
        CatalogTable table =
                CatalogTableUtil.getCatalogTable("catalog", "db", null, "source", rowType);
        return new DataValidatorTransform(ReadonlyConfig.fromMap(options), table);
    }
}
