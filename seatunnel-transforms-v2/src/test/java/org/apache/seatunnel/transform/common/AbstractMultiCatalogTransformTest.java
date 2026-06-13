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

package org.apache.seatunnel.transform.common;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class AbstractMultiCatalogTransformTest {

    private static final Option<String> FIELD_NAME =
            Options.key("field_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Field name to append");

    private static final Option<String> REQUIRED_FIELD =
            Options.key("required_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Field that must exist before this rule is built");

    @Test
    void defaultModeUsesFirstExactTablePathRule() {
        CatalogTable table = catalogTable();
        String tablePath = tablePath(table);
        ReadonlyConfig config =
                config(tableRule(tablePath, "first"), tableRule(tablePath, "second"));
        TestMapMultiCatalogTransform transform =
                new TestMapMultiCatalogTransform(Collections.singletonList(table), config);

        SeaTunnelRow output = transform.map(row(tablePath));

        Assertions.assertEquals(Arrays.asList("id", "first"), fieldNames(transform));
        Assertions.assertEquals(2, output.getArity());
        Assertions.assertEquals("first", output.getField(1));
    }

    @Test
    void firstMatchModeUsesOnlyFirstExactTablePathRule() {
        CatalogTable table = catalogTable();
        String tablePath = tablePath(table);
        ReadonlyConfig config =
                config(
                        TransformCommonOptions.RuleMatchMode.FIRST_MATCH,
                        tableRule(tablePath, "first"),
                        tableRule(tablePath, "second"));
        TestMapMultiCatalogTransform transform =
                new TestMapMultiCatalogTransform(Collections.singletonList(table), config);

        SeaTunnelRow output = transform.map(row(tablePath));

        Assertions.assertEquals(Arrays.asList("id", "first"), fieldNames(transform));
        Assertions.assertEquals(2, output.getArity());
        Assertions.assertEquals("first", output.getField(1));
    }

    @Test
    void allMatchModeAppliesExactTablePathRulesInDeclarationOrder() {
        CatalogTable table = catalogTable();
        String tablePath = tablePath(table);
        Map<String, Object> secondRule = tableRule(tablePath, "second");
        secondRule.put(REQUIRED_FIELD.key(), "first");
        ReadonlyConfig config =
                config(
                        TransformCommonOptions.RuleMatchMode.ALL_MATCH,
                        tableRule(tablePath, "first"),
                        secondRule);
        TestMapMultiCatalogTransform transform =
                new TestMapMultiCatalogTransform(Collections.singletonList(table), config);

        SeaTunnelRow output = transform.map(row(tablePath));

        Assertions.assertEquals(Arrays.asList("id", "first", "second"), fieldNames(transform));
        Assertions.assertEquals(3, output.getArity());
        Assertions.assertEquals("first", output.getField(1));
        Assertions.assertEquals("second", output.getField(2));
    }

    @Test
    void allMatchModeWorksForFlatMapTransforms() {
        CatalogTable table = catalogTable();
        String tablePath = tablePath(table);
        ReadonlyConfig config =
                config(
                        TransformCommonOptions.RuleMatchMode.ALL_MATCH,
                        tableRule(tablePath, "first"),
                        tableRule(tablePath, "second"));
        TestFlatMapMultiCatalogTransform transform =
                new TestFlatMapMultiCatalogTransform(Collections.singletonList(table), config);

        List<SeaTunnelRow> outputRows = transform.flatMap(row(tablePath));

        Assertions.assertEquals(Arrays.asList("id", "first", "second"), fieldNames(transform));
        Assertions.assertEquals(1, outputRows.size());
        Assertions.assertEquals("first", outputRows.get(0).getField(1));
        Assertions.assertEquals("second", outputRows.get(0).getField(2));
    }

    @Test
    void tableMatchRegexIsFallbackWhenExactTablePathRuleDoesNotMatch() {
        CatalogTable table = catalogTable();
        String tablePath = tablePath(table);
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(TransformCommonOptions.TABLE_MATCH_REGEX.key(), ".*orders");
        configMap.put(FIELD_NAME.key(), "fallback");
        configMap.put(
                TransformCommonOptions.MULTI_TABLES.key(),
                Collections.singletonList(tableRule("database.schema.customers", "specific")));
        TestMapMultiCatalogTransform transform =
                new TestMapMultiCatalogTransform(
                        Collections.singletonList(table), ReadonlyConfig.fromMap(configMap));

        SeaTunnelRow output = transform.map(row(tablePath));

        Assertions.assertEquals(Arrays.asList("id", "fallback"), fieldNames(transform));
        Assertions.assertEquals("fallback", output.getField(1));
    }

    @Test
    void identityTransformIsUsedWhenNoRuleMatches() {
        CatalogTable table = catalogTable();
        String tablePath = tablePath(table);
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(TransformCommonOptions.TABLE_MATCH_REGEX.key(), "does-not-match");
        TestMapMultiCatalogTransform transform =
                new TestMapMultiCatalogTransform(
                        Collections.singletonList(table), ReadonlyConfig.fromMap(configMap));
        SeaTunnelRow input = row(tablePath);

        SeaTunnelRow output = transform.map(input);

        Assertions.assertSame(input, output);
        Assertions.assertEquals(Collections.singletonList("id"), fieldNames(transform));
    }

    private static ReadonlyConfig config(Map<String, Object>... tableRules) {
        return config(null, tableRules);
    }

    private static ReadonlyConfig config(
            TransformCommonOptions.RuleMatchMode ruleMatchMode, Map<String, Object>... tableRules) {
        Map<String, Object> configMap = new HashMap<>();
        if (ruleMatchMode != null) {
            configMap.put(TransformCommonOptions.RULE_MATCH_MODE.key(), ruleMatchMode);
        }
        configMap.put(TransformCommonOptions.MULTI_TABLES.key(), Arrays.asList(tableRules));
        return ReadonlyConfig.fromMap(configMap);
    }

    private static Map<String, Object> tableRule(String tablePath, String fieldName) {
        Map<String, Object> rule = new HashMap<>();
        rule.put(TransformCommonOptions.TABLE_PATH.key(), tablePath);
        rule.put(FIELD_NAME.key(), fieldName);
        return rule;
    }

    private static CatalogTable catalogTable() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});
        return CatalogTableUtil.getCatalogTable("catalog", "database", "schema", "orders", rowType);
    }

    private static String tablePath(CatalogTable table) {
        return table.getTableId().toTablePath().toString();
    }

    private static SeaTunnelRow row(String tablePath) {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1});
        row.setTableId(tablePath);
        return row;
    }

    private static List<String> fieldNames(SeaTunnelTransform<SeaTunnelRow> transform) {
        return transform.getProducedCatalogTable().getTableSchema().getColumns().stream()
                .map(Column::getName)
                .collect(Collectors.toList());
    }

    private static SeaTunnelRow appendField(SeaTunnelRow inputRow, String fieldName) {
        Object[] fields = Arrays.copyOf(inputRow.getFields(), inputRow.getArity() + 1);
        fields[fields.length - 1] = fieldName;
        SeaTunnelRow outputRow = new SeaTunnelRow(fields);
        outputRow.setTableId(inputRow.getTableId());
        outputRow.setRowKind(inputRow.getRowKind());
        if (inputRow.getOptionsOrNull() != null) {
            outputRow.setOptions(inputRow.getOptionsOrNull());
        }
        return outputRow;
    }

    private static TableSchema appendField(CatalogTable inputCatalogTable, String fieldName) {
        List<Column> columns =
                inputCatalogTable.getTableSchema().getColumns().stream()
                        .map(Column::copy)
                        .collect(Collectors.toCollection(ArrayList::new));
        columns.add(PhysicalColumn.of(fieldName, BasicType.STRING_TYPE, 0L, true, null, null));
        return TableSchema.builder()
                .columns(columns)
                .primaryKey(inputCatalogTable.getTableSchema().getPrimaryKey())
                .constraintKey(inputCatalogTable.getTableSchema().getConstraintKeys())
                .build();
    }

    private static void assertRequiredField(CatalogTable inputCatalogTable, ReadonlyConfig config) {
        config.getOptional(REQUIRED_FIELD)
                .ifPresent(
                        field ->
                                inputCatalogTable
                                        .getTableSchema()
                                        .toPhysicalRowDataType()
                                        .indexOf(field));
    }

    private static class TestMapMultiCatalogTransform extends AbstractMultiCatalogMapTransform {

        private TestMapMultiCatalogTransform(
                List<CatalogTable> inputCatalogTables, ReadonlyConfig config) {
            super(inputCatalogTables, config);
        }

        @Override
        public String getPluginName() {
            return "TestMap";
        }

        @Override
        protected SeaTunnelTransform<SeaTunnelRow> buildTransform(
                CatalogTable inputCatalogTable, ReadonlyConfig config) {
            return new AppendFieldMapTransform(inputCatalogTable, config);
        }

        @Override
        protected SeaTunnelTransform<SeaTunnelRow> createIdentityTransform(
                CatalogTable catalogTable) {
            return new IdentityMapTransform(catalogTable);
        }
    }

    private static class TestFlatMapMultiCatalogTransform
            extends AbstractMultiCatalogFlatMapTransform {

        private TestFlatMapMultiCatalogTransform(
                List<CatalogTable> inputCatalogTables, ReadonlyConfig config) {
            super(inputCatalogTables, config);
        }

        @Override
        public String getPluginName() {
            return "TestFlatMap";
        }

        @Override
        protected SeaTunnelTransform<SeaTunnelRow> buildTransform(
                CatalogTable inputCatalogTable, ReadonlyConfig config) {
            return new AppendFieldFlatMapTransform(inputCatalogTable, config);
        }

        @Override
        protected SeaTunnelTransform<SeaTunnelRow> createIdentityTransform(
                CatalogTable catalogTable) {
            return new IdentityFlatMapTransform(catalogTable);
        }
    }

    private static class AppendFieldMapTransform extends AbstractCatalogSupportMapTransform {

        private final String fieldName;

        private AppendFieldMapTransform(CatalogTable inputCatalogTable, ReadonlyConfig config) {
            super(inputCatalogTable);
            assertRequiredField(inputCatalogTable, config);
            this.fieldName = config.get(FIELD_NAME);
        }

        @Override
        public String getPluginName() {
            return "AppendFieldMap";
        }

        @Override
        protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
            return appendField(inputRow, fieldName);
        }

        @Override
        protected TableSchema transformTableSchema() {
            return appendField(inputCatalogTable, fieldName);
        }

        @Override
        protected TableIdentifier transformTableIdentifier() {
            return inputCatalogTable.getTableId().copy();
        }
    }

    private static class AppendFieldFlatMapTransform
            extends AbstractCatalogSupportFlatMapTransform {

        private final String fieldName;

        private AppendFieldFlatMapTransform(CatalogTable inputCatalogTable, ReadonlyConfig config) {
            super(inputCatalogTable);
            assertRequiredField(inputCatalogTable, config);
            this.fieldName = config.get(FIELD_NAME);
        }

        @Override
        public String getPluginName() {
            return "AppendFieldFlatMap";
        }

        @Override
        protected List<SeaTunnelRow> transformRow(SeaTunnelRow inputRow) {
            return Collections.singletonList(appendField(inputRow, fieldName));
        }

        @Override
        protected TableSchema transformTableSchema() {
            return appendField(inputCatalogTable, fieldName);
        }

        @Override
        protected TableIdentifier transformTableIdentifier() {
            return inputCatalogTable.getTableId().copy();
        }
    }
}
