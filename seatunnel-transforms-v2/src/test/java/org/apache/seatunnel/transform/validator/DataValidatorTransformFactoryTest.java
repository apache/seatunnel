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
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.common.TransformCommonOptions;
import org.apache.seatunnel.transform.common.TransformLevelRuleAssertions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

class DataValidatorTransformFactoryTest {

    @Test
    void testTransformLevelRuleDetectionMatchesTheTableRule() {
        TransformLevelRuleAssertions.assertMatchesFactoryRule(
                new DataValidatorTransformFactory(), DataValidatorTransformConfig.FIELD_RULES);
    }

    private final OptionRule rule = new DataValidatorTransformFactory().optionRule();

    private void validate(Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule);
    }

    @Test
    void testValidConfig() {
        Map<String, Object> cfg = new HashMap<>();
        Map<String, Object> fieldRule = new HashMap<>();
        fieldRule.put("field_name", "age");
        fieldRule.put("rule_type", "NOT_NULL");
        cfg.put("field_rules", Arrays.asList(fieldRule));
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    @Test
    void testMissingFieldRulesFails() {
        Map<String, Object> cfg = new HashMap<>();
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testEmptyFieldRulesFails() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("field_rules", Collections.emptyList());
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testMissingFieldNameFails() {
        Map<String, Object> cfg = new HashMap<>();
        Map<String, Object> fieldRule = new HashMap<>();
        fieldRule.put("rule_type", "NOT_NULL");
        cfg.put("field_rules", Arrays.asList(fieldRule));
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testMissingRuleTypeAndRulesFails() {
        Map<String, Object> cfg = new HashMap<>();
        Map<String, Object> fieldRule = new HashMap<>();
        fieldRule.put("field_name", "age");
        cfg.put("field_rules", Arrays.asList(fieldRule));
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testNestedRulesValid() {
        Map<String, Object> cfg = new HashMap<>();
        Map<String, Object> fieldRule = new HashMap<>();
        fieldRule.put("field_name", "email");
        Map<String, Object> nestedRule = new HashMap<>();
        nestedRule.put("rule_type", "NOT_NULL");
        fieldRule.put("rules", Arrays.asList(nestedRule));
        cfg.put("field_rules", Arrays.asList(fieldRule));
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    @Test
    void testEmptyRulesListFails() {
        Map<String, Object> cfg = new HashMap<>();
        Map<String, Object> fieldRule = new HashMap<>();
        fieldRule.put("field_name", "email");
        fieldRule.put("rules", Collections.emptyList());
        cfg.put("field_rules", Arrays.asList(fieldRule));
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
        entry.remove("field_rules");
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(TransformCommonOptions.MULTI_TABLES.key(), Collections.singletonList(entry));
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testGlobalPolicyOptionDoesNotRequireTheTransformLevelRule() {
        // 'row_error_handle_way' is a global policy, it must not make the per-table rule partial
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(TransformCommonOptions.MULTI_TABLES.key(), Collections.singletonList(tableRule()));
        cfg.put("row_error_handle_way", "SKIP");
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    /** A complete rule for a single table, used as a {@code table_transform} entry. */
    private Map<String, Object> tableRule() {
        Map<String, Object> entry = new HashMap<>();
        entry.put(TransformCommonOptions.TABLE_PATH.key(), "test.xyz");
        Map<String, Object> fieldRule = new HashMap<>();
        fieldRule.put("field_name", "age");
        fieldRule.put("rule_type", "NOT_NULL");
        entry.put("field_rules", Arrays.asList(fieldRule));
        return entry;
    }

    /**
     * In {@code ROUTE_TO_TABLE} mode the transform produces the validated table plus an error
     * table. Both schemas have to be visible to the sink, otherwise the routed rows are written
     * with the row type of the validated table and fail the assertions of the error table.
     */
    @Test
    void testRouteToTableExposesErrorTableSchema() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("row_error_handle_way", "ROUTE_TO_TABLE");
        cfg.put(TransformCommonOptions.ERROR_TABLE_OPTION.key(), "error_data");
        Map<String, Object> fieldRule = new HashMap<>();
        fieldRule.put("field_name", "name");
        fieldRule.put("rule_type", "NOT_NULL");
        cfg.put("field_rules", Arrays.asList(fieldRule));

        CatalogTable catalogTable = catalogTable("db", "fake");
        DataValidatorMultiCatalogTransform transform =
                new DataValidatorMultiCatalogTransform(
                        Collections.singletonList(catalogTable), ReadonlyConfig.fromMap(cfg));

        Map<String, CatalogTable> producedTables = new HashMap<>();
        transform
                .getProducedCatalogTables()
                .forEach(
                        table ->
                                producedTables.put(
                                        table.getTableId().toTablePath().toString(), table));

        Assertions.assertEquals(
                new HashSet<>(Arrays.asList("db.fake", "db.error_data")), producedTables.keySet());
        Assertions.assertTrue(
                Arrays.asList(producedTables.get("db.error_data").getTableSchema().getFieldNames())
                        .contains(DataValidatorTransform.SOURCE_TABLE_ID));
    }

    private static CatalogTable catalogTable(String database, String tableName) {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name", "age", "email"},
                        new SeaTunnelDataType[] {
                            BasicType.INT_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.INT_TYPE,
                            BasicType.STRING_TYPE
                        });
        return CatalogTableUtil.getCatalogTable("catalog", database, null, tableName, rowType);
    }

    /**
     * The error handling policy is configured once at the transform level, so it has to reach the
     * transforms that are built from a {@code table_transform} entry as well.
     */
    @Test
    void testGlobalRouteToTablePolicyReachesTableTransformEntries() {
        CatalogTable catalogTable = catalogTable("db", "fake");
        Map<String, Object> entry = new HashMap<>();
        entry.put(
                TransformCommonOptions.TABLE_PATH.key(),
                catalogTable.getTableId().toTablePath().toString());
        Map<String, Object> fieldRule = new HashMap<>();
        fieldRule.put("field_name", "name");
        fieldRule.put("rule_type", "NOT_NULL");
        entry.put("field_rules", Arrays.asList(fieldRule));

        Map<String, Object> cfg = new HashMap<>();
        cfg.put("row_error_handle_way", "ROUTE_TO_TABLE");
        cfg.put(TransformCommonOptions.ERROR_TABLE_OPTION.key(), "error_data");
        cfg.put(TransformCommonOptions.MULTI_TABLES.key(), Collections.singletonList(entry));

        DataValidatorMultiCatalogTransform transform =
                new DataValidatorMultiCatalogTransform(
                        Collections.singletonList(catalogTable), ReadonlyConfig.fromMap(cfg));

        Map<String, CatalogTable> producedTables = new HashMap<>();
        transform
                .getProducedCatalogTables()
                .forEach(
                        table ->
                                producedTables.put(
                                        table.getTableId().toTablePath().toString(), table));
        Assertions.assertEquals(
                new HashSet<>(Arrays.asList("db.fake", "db.error_data")), producedTables.keySet());

        SeaTunnelRow invalidRow = new SeaTunnelRow(new Object[] {1, null, 18, "a@b.c"});
        invalidRow.setTableId(catalogTable.getTableId().toTablePath().toString());
        SeaTunnelRow routedRow = transform.map(invalidRow);
        Assertions.assertNotNull(routedRow);
        Assertions.assertEquals("db.error_data", routedRow.getTableId());
    }

    /** An entry is a complete rule for its table, so its own policy wins over the global one. */
    @Test
    void testTableTransformEntryPolicyOverridesTheGlobalPolicy() {
        CatalogTable catalogTable = catalogTable("db", "fake");
        Map<String, Object> entry = new HashMap<>();
        entry.put(
                TransformCommonOptions.TABLE_PATH.key(),
                catalogTable.getTableId().toTablePath().toString());
        entry.put("row_error_handle_way", "SKIP");
        Map<String, Object> fieldRule = new HashMap<>();
        fieldRule.put("field_name", "name");
        fieldRule.put("rule_type", "NOT_NULL");
        entry.put("field_rules", Arrays.asList(fieldRule));

        Map<String, Object> cfg = new HashMap<>();
        cfg.put("row_error_handle_way", "ROUTE_TO_TABLE");
        cfg.put(TransformCommonOptions.ERROR_TABLE_OPTION.key(), "error_data");
        cfg.put(TransformCommonOptions.MULTI_TABLES.key(), Collections.singletonList(entry));

        DataValidatorMultiCatalogTransform transform =
                new DataValidatorMultiCatalogTransform(
                        Collections.singletonList(catalogTable), ReadonlyConfig.fromMap(cfg));

        Map<String, CatalogTable> producedTables = new HashMap<>();
        transform
                .getProducedCatalogTables()
                .forEach(
                        table ->
                                producedTables.put(
                                        table.getTableId().toTablePath().toString(), table));
        Assertions.assertEquals(Collections.singleton("db.fake"), producedTables.keySet());

        SeaTunnelRow invalidRow = new SeaTunnelRow(new Object[] {1, null, 18, "a@b.c"});
        invalidRow.setTableId(catalogTable.getTableId().toTablePath().toString());
        Assertions.assertNull(transform.map(invalidRow));
    }

    /**
     * {@code ALL_MATCH} chains the rules of a table. A row that a rule routes to the error table
     * carries the schema of that table, so the remaining rules of the chain must not validate it
     * again, and the error table schema has to be exposed.
     */
    @Test
    void testAllMatchRouteToTableStopsTheChainAndExposesTheErrorTable() {
        CatalogTable catalogTable = catalogTable("db", "fake");
        String tablePath = catalogTable.getTableId().toTablePath().toString();

        Map<String, Object> cfg = new HashMap<>();
        cfg.put("row_error_handle_way", "ROUTE_TO_TABLE");
        cfg.put(TransformCommonOptions.ERROR_TABLE_OPTION.key(), "error_data");
        cfg.put(TransformCommonOptions.RULE_MATCH_MODE.key(), "ALL_MATCH");
        cfg.put(
                TransformCommonOptions.MULTI_TABLES.key(),
                Arrays.asList(
                        tableRule(tablePath, fieldRule("name", "NOT_NULL")),
                        tableRule(
                                tablePath, fieldRule("name", "REGEX", "pattern", "^[A-Za-z ]+$"))));

        DataValidatorMultiCatalogTransform transform =
                new DataValidatorMultiCatalogTransform(
                        Collections.singletonList(catalogTable), ReadonlyConfig.fromMap(cfg));

        Map<String, CatalogTable> producedTables = new HashMap<>();
        transform
                .getProducedCatalogTables()
                .forEach(
                        table ->
                                producedTables.put(
                                        table.getTableId().toTablePath().toString(), table));
        Assertions.assertEquals(
                new HashSet<>(Arrays.asList("db.fake", "db.error_data")), producedTables.keySet());

        List<String> errorFieldNames =
                Arrays.asList(producedTables.get("db.error_data").getTableSchema().getFieldNames());
        int originalDataIndex = errorFieldNames.indexOf(DataValidatorTransform.ORIGINAL_DATA);
        Assertions.assertTrue(originalDataIndex >= 0);

        // The first rule routes the row, the second one must not validate the error row.
        SeaTunnelRow routedByFirstRule = map(transform, tablePath, 1, null, 18, "jane@example.com");
        assertRoutedOnce(routedByFirstRule, originalDataIndex);

        // When the first rule passes, the chained second rule still applies.
        SeaTunnelRow routedBySecondRule =
                map(transform, tablePath, 1, "John.Doe", 18, "jane@example.com");
        assertRoutedOnce(routedBySecondRule, originalDataIndex);

        SeaTunnelRow validRow = map(transform, tablePath, 1, "John Doe", 18, "jane@example.com");
        Assertions.assertEquals(tablePath, validRow.getTableId());
    }

    private static void assertRoutedOnce(SeaTunnelRow routedRow, int originalDataIndex) {
        Assertions.assertEquals("db.error_data", routedRow.getTableId());
        Assertions.assertFalse(
                String.valueOf(routedRow.getField(originalDataIndex))
                        .contains(DataValidatorTransform.SOURCE_TABLE_ID),
                "the routed row must not be routed a second time: "
                        + routedRow.getField(originalDataIndex));
    }

    private static Map<String, Object> tableRule(String tablePath, Map<String, Object> fieldRule) {
        Map<String, Object> entry = new HashMap<>();
        entry.put(TransformCommonOptions.TABLE_PATH.key(), tablePath);
        entry.put("field_rules", Collections.singletonList(fieldRule));
        return entry;
    }

    private static Map<String, Object> fieldRule(
            String fieldName, String ruleType, Object... extraRuleValues) {
        Map<String, Object> rule = new HashMap<>();
        rule.put("field_name", fieldName);
        rule.put("rule_type", ruleType);
        for (int i = 0; i < extraRuleValues.length; i += 2) {
            rule.put((String) extraRuleValues[i], extraRuleValues[i + 1]);
        }
        return rule;
    }

    private static SeaTunnelRow map(
            DataValidatorMultiCatalogTransform transform, String tablePath, Object... fields) {
        SeaTunnelRow row = new SeaTunnelRow(fields);
        row.setTableId(tablePath);
        return transform.map(row);
    }
}
