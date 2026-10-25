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

package org.apache.seatunnel.transform.adaptsink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.common.TransformCommonOptions;
import org.apache.seatunnel.transform.common.TransformLevelRuleAssertions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class DefineSinkTypeTransformFactoryTest {

    @Test
    void testTransformLevelRuleDetectionMatchesTheTableRule() {
        TransformLevelRuleAssertions.assertMatchesFactoryRule(
                new DefineSinkTypeTransformFactory(), DefineSinkTypeTransformConfig.COLUMNS);
    }

    private final OptionRule rule = new DefineSinkTypeTransformFactory().optionRule();

    private void validate(Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule);
    }

    private Map<String, Object> columnEntry(String column, String type) {
        Map<String, Object> entry = new HashMap<>();
        entry.put("column", column);
        entry.put("type", type);
        return entry;
    }

    @Test
    void testValidConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(
                "columns",
                Arrays.asList(columnEntry("id", "bigint"), columnEntry("name", "string")));
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    @Test
    void testMissingColumnsFails() {
        Map<String, Object> cfg = new HashMap<>();
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testEmptyColumnsFails() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("columns", Collections.emptyList());
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testColumnWithNullNameFails() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("columns", Arrays.asList(columnEntry(null, "bigint")));
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testColumnWithNullTypeFails() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("columns", Arrays.asList(columnEntry("id", null)));
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testColumnWithEmptyNameFails() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("columns", Arrays.asList(columnEntry("", "bigint")));
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testDuplicateColumnNameFails() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("columns", Arrays.asList(columnEntry("id", "bigint"), columnEntry("id", "string")));
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testTableTransformOnlyConfigPasses() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(TransformCommonOptions.MULTI_TABLES.key(), Collections.singletonList(tableRule()));
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    @Test
    void testTableTransformEntryWithoutTablePathFails() {
        Map<String, Object> entry = tableRule();
        entry.remove(TransformCommonOptions.TABLE_PATH.key());
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(TransformCommonOptions.MULTI_TABLES.key(), Collections.singletonList(entry));
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testTableTransformEntryMustSatisfyTheRule() {
        Map<String, Object> entry = tableRule();
        entry.remove("columns");
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(TransformCommonOptions.MULTI_TABLES.key(), Collections.singletonList(entry));
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    /** A complete rule for a single table, used as a {@code table_transform} entry. */
    private Map<String, Object> tableRule() {
        Map<String, Object> entry = new HashMap<>();
        entry.put(TransformCommonOptions.TABLE_PATH.key(), "test.xyz");
        entry.put("columns", Arrays.asList(columnEntry("id", "bigint")));
        return entry;
    }

    /**
     * Simulates the sink side check of the E2E suite: the Assert sink cannot compare sink types, so
     * the produced catalog table is verified directly.
     */
    @Test
    void testSinkTypeIsDeclaredForSingleTableMode() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("columns", Arrays.asList(columnEntry("id", "nvarchar2(10)")));

        DefineSinkTypeTransform transform =
                new DefineSinkTypeTransform(
                        DefineSinkTypeTransformConfig.of(ReadonlyConfig.fromMap(cfg)),
                        catalogTable("test.xyz"));

        Assertions.assertEquals(
                "nvarchar2(10)", sinkTypeOf(transform.getProducedCatalogTable(), "id"));
    }

    /** The same verification for the multi-table mode, where every entry declares its own type. */
    @Test
    void testSinkTypeIsDeclaredPerTableInMultiTableMode() {
        CatalogTable listed = catalogTable("xyz");
        CatalogTable unlisted = catalogTable("abc");
        Map<String, Object> entry = new HashMap<>();
        entry.put(
                TransformCommonOptions.TABLE_PATH.key(),
                listed.getTableId().toTablePath().toString());
        entry.put("columns", Arrays.asList(columnEntry("id", "bigint")));

        Map<String, Object> cfg = new HashMap<>();
        cfg.put(TransformCommonOptions.MULTI_TABLES.key(), Collections.singletonList(entry));

        DefineSinkTypeMultiCatalogTransform transform =
                new DefineSinkTypeMultiCatalogTransform(
                        Arrays.asList(listed, unlisted), ReadonlyConfig.fromMap(cfg));

        // only test.xyz has an entry, so only its column carries the declared sink type
        Assertions.assertEquals(
                "bigint", sinkTypeOf(transform.getProducedCatalogTables().get(0), "id"));
        Assertions.assertNull(sinkTypeOf(transform.getProducedCatalogTables().get(1), "id"));
    }

    private static String sinkTypeOf(CatalogTable catalogTable, String columnName) {
        Column column =
                catalogTable.getTableSchema().getColumns().stream()
                        .filter(candidate -> columnName.equals(candidate.getName()))
                        .findFirst()
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "column not found: " + columnName));
        return column.getSinkType();
    }

    private static CatalogTable catalogTable(String tableName) {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new SeaTunnelDataType[] {BasicType.LONG_TYPE, BasicType.STRING_TYPE});
        return CatalogTableUtil.getCatalogTable("catalog", "test", null, tableName, rowType);
    }
}
