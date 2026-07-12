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

package org.apache.seatunnel.transform.encrypt;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.transform.exception.TransformException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class FieldEncryptTransformTest {
    public static final String KEY =
            "base64:"
                    + Base64.getEncoder()
                            .encodeToString(
                                    "0123456789abcdef"
                                            .getBytes(java.nio.charset.StandardCharsets.UTF_8));
    private static CatalogTable catalogTable;
    private static Object[] values;
    private static Object[] original;
    private List<String> encryptFields = Arrays.asList("key2", "key3");

    @BeforeAll
    static void setUp() {
        catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("catalog", TablePath.DEFAULT),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "key1",
                                                BasicType.STRING_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "key2",
                                                BasicType.STRING_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "key3",
                                                BasicType.STRING_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "key4",
                                                BasicType.STRING_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "key5",
                                                BasicType.STRING_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .build(),
                        new HashMap<>(),
                        new ArrayList<>(),
                        "comment");
        values = new Object[] {"value1", "value2", "value3", "value4", "value5"};
        original = Arrays.copyOf(values, values.length);
    }

    @Test
    void testEncryption() {
        SeaTunnelRow output = encryption();
        for (int i = 0; i < original.length; i++) {
            if (i == 1 || i == 2) {
                Assertions.assertNotEquals(original[i], output.getField(i));
            } else {
                Assertions.assertEquals(original[i], output.getField(i));
            }
        }
    }

    @Test
    void testDecryption() {
        SeaTunnelRow output = encryption();
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(FieldEncryptTransformConfig.FIELDS.key(), encryptFields);
        configMap.put(FieldEncryptTransformConfig.KEY.key(), KEY);
        configMap.put(FieldEncryptTransformConfig.MODE.key(), "decrypt");

        FieldEncryptTransform fieldEncryptTransform =
                new FieldEncryptTransform(ReadonlyConfig.fromMap(configMap), catalogTable);

        SeaTunnelRow input = new SeaTunnelRow(output.getFields());
        SeaTunnelRow decryptedRow = fieldEncryptTransform.transformRow(input);
        Assertions.assertNotNull(decryptedRow);
        Assertions.assertEquals("value2", decryptedRow.getField(1));
        Assertions.assertEquals("value3", decryptedRow.getField(2));
    }

    @Test
    void testNullField() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(FieldEncryptTransformConfig.FIELDS.key(), encryptFields);
        configMap.put(FieldEncryptTransformConfig.KEY.key(), KEY);

        FieldEncryptTransform fieldEncryptTransform =
                new FieldEncryptTransform(ReadonlyConfig.fromMap(configMap), catalogTable);

        Object[] valuesWithNull = new Object[] {"value1", null, "value3", "value4", "value5"};
        SeaTunnelRow input = new SeaTunnelRow(valuesWithNull);
        SeaTunnelRow output = fieldEncryptTransform.transformRow(input);

        Assertions.assertNull(output.getField(1));
        Assertions.assertNotNull(output.getField(2));
    }

    @Test
    void testEmptyString() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(FieldEncryptTransformConfig.FIELDS.key(), encryptFields);
        configMap.put(FieldEncryptTransformConfig.KEY.key(), KEY);

        FieldEncryptTransform fieldEncryptTransform =
                new FieldEncryptTransform(ReadonlyConfig.fromMap(configMap), catalogTable);

        Object[] valuesWithEmpty = new Object[] {"value1", "", "   ", "value4", "value5"};
        SeaTunnelRow input = new SeaTunnelRow(valuesWithEmpty);
        Assertions.assertDoesNotThrow(() -> fieldEncryptTransform.transformRow(input));
    }

    @Test
    void testFieldNotFound() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(FieldEncryptTransformConfig.FIELDS.key(), Arrays.asList("nonExistentField"));
        configMap.put(FieldEncryptTransformConfig.KEY.key(), KEY);

        Assertions.assertThrows(
                TransformException.class,
                () -> new FieldEncryptTransform(ReadonlyConfig.fromMap(configMap), catalogTable));
    }

    @Test
    void testInvalidKeyLength() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(FieldEncryptTransformConfig.FIELDS.key(), encryptFields);
        configMap.put(FieldEncryptTransformConfig.KEY.key(), "base64:AAAAAAA=");

        FieldEncryptTransform fieldEncryptTransform =
                new FieldEncryptTransform(ReadonlyConfig.fromMap(configMap), catalogTable);
        SeaTunnelRow input = new SeaTunnelRow(values);

        Assertions.assertThrows(
                SeaTunnelRuntimeException.class, () -> fieldEncryptTransform.transformRow(input));
    }

    @Test
    void testUnsupportedAlgorithm() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(FieldEncryptTransformConfig.FIELDS.key(), encryptFields);
        configMap.put(FieldEncryptTransformConfig.KEY.key(), KEY);
        configMap.put(FieldEncryptTransformConfig.ALGORITHM.key(), "INVALID_ALGORITHM");

        FieldEncryptTransform fieldEncryptTransform =
                new FieldEncryptTransform(ReadonlyConfig.fromMap(configMap), catalogTable);
        SeaTunnelRow input = new SeaTunnelRow(values);

        Assertions.assertThrows(
                SeaTunnelRuntimeException.class,
                () -> {
                    fieldEncryptTransform.transformRow(input);
                });
    }

    @Test
    void testInvalidMode() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(FieldEncryptTransformConfig.FIELDS.key(), encryptFields);
        configMap.put(FieldEncryptTransformConfig.KEY.key(), KEY);
        configMap.put(FieldEncryptTransformConfig.MODE.key(), "invalid_mode");

        FieldEncryptTransform fieldEncryptTransform =
                new FieldEncryptTransform(ReadonlyConfig.fromMap(configMap), catalogTable);
        SeaTunnelRow input = new SeaTunnelRow(values);

        Assertions.assertThrows(
                SeaTunnelRuntimeException.class,
                () -> {
                    fieldEncryptTransform.transformRow(input);
                });
    }

    @Test
    void testNonStringField() {
        CatalogTable intCatalogTable =
                CatalogTable.of(
                        TableIdentifier.of("catalog", TablePath.DEFAULT),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "key1",
                                                BasicType.INT_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .build(),
                        new HashMap<>(),
                        new ArrayList<>(),
                        "comment");

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(FieldEncryptTransformConfig.FIELDS.key(), Arrays.asList("key1"));
        configMap.put(FieldEncryptTransformConfig.KEY.key(), KEY);

        Assertions.assertThrows(
                SeaTunnelRuntimeException.class,
                () ->
                        new FieldEncryptTransform(
                                ReadonlyConfig.fromMap(configMap), intCatalogTable));
    }

    @Test
    void testFieldExceedsMaxLength() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(FieldEncryptTransformConfig.FIELDS.key(), encryptFields);
        configMap.put(FieldEncryptTransformConfig.KEY.key(), KEY);
        configMap.put(FieldEncryptTransformConfig.MAX_FIELD_LENGTH.key(), 10);

        FieldEncryptTransform fieldEncryptTransform =
                new FieldEncryptTransform(ReadonlyConfig.fromMap(configMap), catalogTable);

        Object[] oversizedValues =
                new Object[] {"value1", "thisvalueiswaytoolong", "value3", "value4", "value5"};
        SeaTunnelRow input = new SeaTunnelRow(oversizedValues);

        Assertions.assertThrows(
                SeaTunnelRuntimeException.class, () -> fieldEncryptTransform.transformRow(input));
    }

    @Test
    void testFieldExactlyMaxLength() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(FieldEncryptTransformConfig.FIELDS.key(), encryptFields);
        configMap.put(FieldEncryptTransformConfig.KEY.key(), KEY);
        configMap.put(FieldEncryptTransformConfig.MAX_FIELD_LENGTH.key(), 6);

        FieldEncryptTransform fieldEncryptTransform =
                new FieldEncryptTransform(ReadonlyConfig.fromMap(configMap), catalogTable);

        Object[] exactValues = new Object[] {"value1", "value2", "value3", "value4", "value5"};
        SeaTunnelRow input = new SeaTunnelRow(exactValues);
        SeaTunnelRow output = fieldEncryptTransform.transformRow(input);

        Assertions.assertNotNull(output);
        Assertions.assertNotEquals("value2", output.getField(1));
    }

    @Test
    void testMaxFieldLengthWithNullField() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(FieldEncryptTransformConfig.FIELDS.key(), encryptFields);
        configMap.put(FieldEncryptTransformConfig.KEY.key(), KEY);
        configMap.put(FieldEncryptTransformConfig.MAX_FIELD_LENGTH.key(), 3);

        FieldEncryptTransform fieldEncryptTransform =
                new FieldEncryptTransform(ReadonlyConfig.fromMap(configMap), catalogTable);

        Object[] valuesWithNull = new Object[] {"value1", null, "val", "value4", "value5"};
        SeaTunnelRow input = new SeaTunnelRow(valuesWithNull);
        SeaTunnelRow output = fieldEncryptTransform.transformRow(input);

        Assertions.assertNull(output.getField(1));
    }

    private SeaTunnelRow encryption() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(FieldEncryptTransformConfig.FIELDS.key(), encryptFields);
        configMap.put(FieldEncryptTransformConfig.KEY.key(), KEY);

        FieldEncryptTransform fieldEncryptTransform =
                new FieldEncryptTransform(ReadonlyConfig.fromMap(configMap), catalogTable);

        SeaTunnelRow input = new SeaTunnelRow(values);
        return fieldEncryptTransform.transformRow(input);
    }
}
