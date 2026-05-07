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

package org.apache.seatunnel.transform.replace;

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
import java.util.HashMap;
import java.util.Map;

class ReplaceTransformTest {

    private static CatalogTable catalogTable;

    @BeforeAll
    static void setUp() {
        catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("catalog", TablePath.DEFAULT),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "id",
                                                BasicType.INT_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "name",
                                                BasicType.STRING_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "title",
                                                BasicType.STRING_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .build(),
                        new HashMap<>(),
                        new ArrayList<>(),
                        "comment");
    }

    @Test
    void testSingleFieldReplaceWithString() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("replace_field", "name");
        configMap.put(ReplaceTransformConfig.KEY_PATTERN.key(), "before");
        configMap.put(ReplaceTransformConfig.KEY_REPLACEMENT.key(), "after");

        ReplaceTransform transform =
                new ReplaceTransform(ReadonlyConfig.fromMap(configMap), catalogTable);

        SeaTunnelRow input = new SeaTunnelRow(new Object[] {1, "before name", "before title"});
        SeaTunnelRow output = transform.transformRow(input);

        Assertions.assertEquals("after name", output.getField(1));
        Assertions.assertEquals("before title", output.getField(2));
    }

    @Test
    void testMultipleFieldReplaceWithList() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(
                ReplaceTransformConfig.KEY_REPLACE_FIELDS.key(), Arrays.asList("name", "title"));
        configMap.put(ReplaceTransformConfig.KEY_PATTERN.key(), "before");
        configMap.put(ReplaceTransformConfig.KEY_REPLACEMENT.key(), "after");

        ReplaceTransform transform =
                new ReplaceTransform(ReadonlyConfig.fromMap(configMap), catalogTable);

        SeaTunnelRow input = new SeaTunnelRow(new Object[] {1, "before name", "before title"});
        SeaTunnelRow output = transform.transformRow(input);

        Assertions.assertEquals("after name", output.getField(1));
        Assertions.assertEquals("after title", output.getField(2));
        Assertions.assertEquals(1, output.getField(0));
    }

    @Test
    void testRejectConflictingReplaceFieldKeys() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("replace_field", "name");
        configMap.put(
                ReplaceTransformConfig.KEY_REPLACE_FIELDS.key(), Arrays.asList("name", "title"));
        configMap.put(ReplaceTransformConfig.KEY_PATTERN.key(), "before");
        configMap.put(ReplaceTransformConfig.KEY_REPLACEMENT.key(), "after");

        TransformException exception =
                Assertions.assertThrows(
                        TransformException.class,
                        () ->
                                new ReplaceTransform(
                                        ReadonlyConfig.fromMap(configMap), catalogTable));

        Assertions.assertTrue(exception.getMessage().contains("cannot be configured together"));
    }

    @Test
    void testNullFieldSkipped() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(
                ReplaceTransformConfig.KEY_REPLACE_FIELDS.key(), Arrays.asList("name", "title"));
        configMap.put(ReplaceTransformConfig.KEY_PATTERN.key(), "before");
        configMap.put(ReplaceTransformConfig.KEY_REPLACEMENT.key(), "after");

        ReplaceTransform transform =
                new ReplaceTransform(ReadonlyConfig.fromMap(configMap), catalogTable);

        SeaTunnelRow input = new SeaTunnelRow(new Object[] {1, null, "before title"});
        SeaTunnelRow output = transform.transformRow(input);

        Assertions.assertNull(output.getField(1));
        Assertions.assertEquals("after title", output.getField(2));
    }

    @Test
    void testReplaceFieldsNotFound() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(
                ReplaceTransformConfig.KEY_REPLACE_FIELDS.key(),
                Arrays.asList("name", "nonExistentField"));
        configMap.put(ReplaceTransformConfig.KEY_PATTERN.key(), "before");
        configMap.put(ReplaceTransformConfig.KEY_REPLACEMENT.key(), "after");

        Assertions.assertThrows(
                TransformException.class,
                () -> new ReplaceTransform(ReadonlyConfig.fromMap(configMap), catalogTable));
    }

    @Test
    void testInvalidRegexPattern() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(
                ReplaceTransformConfig.KEY_REPLACE_FIELDS.key(), Arrays.asList("name", "title"));
        configMap.put(ReplaceTransformConfig.KEY_PATTERN.key(), "[");
        configMap.put(ReplaceTransformConfig.KEY_REPLACEMENT.key(), "NUM");
        configMap.put(ReplaceTransformConfig.KEY_IS_REGEX.key(), true);

        Assertions.assertThrows(
                SeaTunnelRuntimeException.class,
                () -> new ReplaceTransform(ReadonlyConfig.fromMap(configMap), catalogTable));
    }

    @Test
    void testEmptyReplaceFieldsValidation() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ReplaceTransformConfig.KEY_REPLACE_FIELDS.key(), new ArrayList<String>());
        configMap.put(ReplaceTransformConfig.KEY_PATTERN.key(), "before");
        configMap.put(ReplaceTransformConfig.KEY_REPLACEMENT.key(), "after");

        TransformException exception =
                Assertions.assertThrows(
                        TransformException.class,
                        () ->
                                new ReplaceTransform(
                                        ReadonlyConfig.fromMap(configMap), catalogTable));

        Assertions.assertTrue(exception.getMessage().contains("must not be empty"));
    }

    @Test
    void testSingleFieldRegexReplace() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("replace_field", "name");
        configMap.put(ReplaceTransformConfig.KEY_PATTERN.key(), "\\d+");
        configMap.put(ReplaceTransformConfig.KEY_REPLACEMENT.key(), "NUM");
        configMap.put(ReplaceTransformConfig.KEY_IS_REGEX.key(), true);

        ReplaceTransform transform =
                new ReplaceTransform(ReadonlyConfig.fromMap(configMap), catalogTable);

        SeaTunnelRow input = new SeaTunnelRow(new Object[] {1, "abc123def456", "title"});
        SeaTunnelRow output = transform.transformRow(input);

        Assertions.assertEquals("abcNUMdefNUM", output.getField(1));
    }

    @Test
    void testRegexReplace() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(
                ReplaceTransformConfig.KEY_REPLACE_FIELDS.key(), Arrays.asList("name", "title"));
        configMap.put(ReplaceTransformConfig.KEY_PATTERN.key(), "\\d+");
        configMap.put(ReplaceTransformConfig.KEY_REPLACEMENT.key(), "NUM");
        configMap.put(ReplaceTransformConfig.KEY_IS_REGEX.key(), true);

        ReplaceTransform transform =
                new ReplaceTransform(ReadonlyConfig.fromMap(configMap), catalogTable);

        SeaTunnelRow input = new SeaTunnelRow(new Object[] {1, "abc123def456", "xyz789uvw012"});
        SeaTunnelRow output = transform.transformRow(input);

        Assertions.assertEquals("abcNUMdefNUM", output.getField(1));
        Assertions.assertEquals("xyzNUMuvwNUM", output.getField(2));
    }

    @Test
    void testSingleFieldRegexReplaceFirst() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("replace_field", "name");
        configMap.put(ReplaceTransformConfig.KEY_PATTERN.key(), "\\d+");
        configMap.put(ReplaceTransformConfig.KEY_REPLACEMENT.key(), "NUM");
        configMap.put(ReplaceTransformConfig.KEY_IS_REGEX.key(), true);
        configMap.put(ReplaceTransformConfig.KEY_REPLACE_FIRST.key(), true);

        ReplaceTransform transform =
                new ReplaceTransform(ReadonlyConfig.fromMap(configMap), catalogTable);

        SeaTunnelRow input = new SeaTunnelRow(new Object[] {1, "abc123def456", "title"});
        SeaTunnelRow output = transform.transformRow(input);

        Assertions.assertEquals("abcNUMdef456", output.getField(1));
    }

    @Test
    void testMultipleFieldRegexReplaceFirst() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(
                ReplaceTransformConfig.KEY_REPLACE_FIELDS.key(), Arrays.asList("name", "title"));
        configMap.put(ReplaceTransformConfig.KEY_PATTERN.key(), "\\d+");
        configMap.put(ReplaceTransformConfig.KEY_REPLACEMENT.key(), "NUM");
        configMap.put(ReplaceTransformConfig.KEY_IS_REGEX.key(), true);
        configMap.put(ReplaceTransformConfig.KEY_REPLACE_FIRST.key(), true);

        ReplaceTransform transform =
                new ReplaceTransform(ReadonlyConfig.fromMap(configMap), catalogTable);

        SeaTunnelRow input = new SeaTunnelRow(new Object[] {1, "abc123def456", "xyz789uvw012"});
        SeaTunnelRow output = transform.transformRow(input);

        Assertions.assertEquals("abcNUMdef456", output.getField(1));
        Assertions.assertEquals("xyzNUMuvw012", output.getField(2));
    }
}
