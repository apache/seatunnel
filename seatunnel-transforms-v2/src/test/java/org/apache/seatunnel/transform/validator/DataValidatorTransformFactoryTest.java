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
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.common.TransformCommonOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

class DataValidatorTransformFactoryTest {

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
}
