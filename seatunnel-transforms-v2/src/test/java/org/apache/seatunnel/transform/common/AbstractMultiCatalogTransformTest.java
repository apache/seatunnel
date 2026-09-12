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
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.transform.SeaTunnelMapTransform;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
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

    private static final Option<Boolean> FILTER_SCHEMA_CHANGE_EVENT =
            Options.key("filter_schema_change_event")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Return null for schema-change events");

    private static final Option<Boolean> FAIL_ON_SCHEMA_CHANGE_EVENT =
            Options.key("fail_on_schema_change_event")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Fail if a schema-change event reaches this rule");

    @Test
    void defaultModeRejectsDuplicateExactTablePathRules() {
        CatalogTable table = catalogTable();
        String tablePath = tablePath(table);
        ReadonlyConfig config =
                config(tableRule(tablePath, "first"), tableRule(tablePath, "second"));

        IllegalStateException exception =
                Assertions.assertThrows(
                        IllegalStateException.class,
                        () ->
                                new TestMapMultiCatalogTransform(
                                        Collections.singletonList(table), config));

        Assertions.assertTrue(exception.getMessage().contains(tablePath));
    }

    @Test
    void defaultModeRejectsDuplicateExactTablePathRulesBeforeTableMatching() {
        CatalogTable table = catalogTable();
        String duplicateTablePath = "database.schema.customers";
        ReadonlyConfig config =
                config(
                        tableRule(duplicateTablePath, "first"),
                        tableRule(duplicateTablePath, "second"));

        IllegalStateException exception =
                Assertions.assertThrows(
                        IllegalStateException.class,
                        () ->
                                new TestMapMultiCatalogTransform(
                                        Collections.singletonList(table), config));

        Assertions.assertTrue(exception.getMessage().contains(duplicateTablePath));
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
    void chainedMapTransformStopsSchemaChangeAfterFilteredEvent() {
        CatalogTable table = catalogTable();
        String tablePath = tablePath(table);
        Map<String, Object> firstRule = tableRule(tablePath, "first");
        firstRule.put(FILTER_SCHEMA_CHANGE_EVENT.key(), true);
        Map<String, Object> secondRule = tableRule(tablePath, "second");
        secondRule.put(FAIL_ON_SCHEMA_CHANGE_EVENT.key(), true);
        ReadonlyConfig config =
                config(TransformCommonOptions.RuleMatchMode.ALL_MATCH, firstRule, secondRule);
        TestMapMultiCatalogTransform transform =
                new TestMapMultiCatalogTransform(Collections.singletonList(table), config);

        SchemaChangeEvent event = transform.mapSchemaChangeEvent(addColumnEvent(table));

        Assertions.assertNull(event);
    }

    @Test
    void chainedMapTransformClosesEveryTransformAfterFailure() {
        AtomicBoolean firstClosed = new AtomicBoolean();
        AtomicBoolean secondClosed = new AtomicBoolean();
        ChainedMapTransform transform =
                new ChainedMapTransform(
                        Arrays.asList(
                                new CloseTrackingMapTransform(firstClosed, true),
                                new CloseTrackingMapTransform(secondClosed, false)));

        RuntimeException closeFailure =
                Assertions.assertThrows(RuntimeException.class, transform::close);

        Assertions.assertEquals("expected close failure", closeFailure.getMessage());
        Assertions.assertTrue(firstClosed.get());
        Assertions.assertTrue(secondClosed.get());
    }

    @Test
    void chainedFlatMapTransformStopsSchemaChangeAfterFilteredEvent() {
        CatalogTable table = catalogTable();
        String tablePath = tablePath(table);
        Map<String, Object> firstRule = tableRule(tablePath, "first");
        firstRule.put(FILTER_SCHEMA_CHANGE_EVENT.key(), true);
        Map<String, Object> secondRule = tableRule(tablePath, "second");
        secondRule.put(FAIL_ON_SCHEMA_CHANGE_EVENT.key(), true);
        ReadonlyConfig config =
                config(TransformCommonOptions.RuleMatchMode.ALL_MATCH, firstRule, secondRule);
        TestFlatMapMultiCatalogTransform transform =
                new TestFlatMapMultiCatalogTransform(Collections.singletonList(table), config);

        SchemaChangeEvent event = transform.mapSchemaChangeEvent(addColumnEvent(table));

        Assertions.assertNull(event);
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

    private static SchemaChangeEvent addColumnEvent(CatalogTable table) {
        return AlterTableAddColumnEvent.add(
                table.getTableId(),
                PhysicalColumn.of("created_at", BasicType.STRING_TYPE, 0L, true, null, null));
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

    private static class CloseTrackingMapTransform implements SeaTunnelMapTransform<SeaTunnelRow> {

        private final AtomicBoolean closed;
        private final boolean failOnClose;

        private CloseTrackingMapTransform(AtomicBoolean closed, boolean failOnClose) {
            this.closed = closed;
            this.failOnClose = failOnClose;
        }

        @Override
        public String getPluginName() {
            return "CloseTracking";
        }

        @Override
        public SeaTunnelRow map(SeaTunnelRow row) {
            return row;
        }

        @Override
        public CatalogTable getProducedCatalogTable() {
            return catalogTable();
        }

        @Override
        public List<CatalogTable> getProducedCatalogTables() {
            return Collections.singletonList(getProducedCatalogTable());
        }

        @Override
        public void close() {
            closed.set(true);
            if (failOnClose) {
                throw new RuntimeException("expected close failure");
            }
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

        private final boolean filterSchemaChangeEvent;

        private final boolean failOnSchemaChangeEvent;

        private AppendFieldMapTransform(CatalogTable inputCatalogTable, ReadonlyConfig config) {
            super(inputCatalogTable);
            assertRequiredField(inputCatalogTable, config);
            this.fieldName = config.get(FIELD_NAME);
            this.filterSchemaChangeEvent = config.get(FILTER_SCHEMA_CHANGE_EVENT);
            this.failOnSchemaChangeEvent = config.get(FAIL_ON_SCHEMA_CHANGE_EVENT);
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
        public SchemaChangeEvent mapSchemaChangeEvent(SchemaChangeEvent event) {
            if (failOnSchemaChangeEvent) {
                throw new AssertionError("Schema-change event should not reach this rule");
            }
            if (filterSchemaChangeEvent) {
                return null;
            }
            return super.mapSchemaChangeEvent(event);
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

        private final boolean filterSchemaChangeEvent;

        private final boolean failOnSchemaChangeEvent;

        private AppendFieldFlatMapTransform(CatalogTable inputCatalogTable, ReadonlyConfig config) {
            super(inputCatalogTable);
            assertRequiredField(inputCatalogTable, config);
            this.fieldName = config.get(FIELD_NAME);
            this.filterSchemaChangeEvent = config.get(FILTER_SCHEMA_CHANGE_EVENT);
            this.failOnSchemaChangeEvent = config.get(FAIL_ON_SCHEMA_CHANGE_EVENT);
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
        public SchemaChangeEvent mapSchemaChangeEvent(SchemaChangeEvent event) {
            if (failOnSchemaChangeEvent) {
                throw new AssertionError("Schema-change event should not reach this rule");
            }
            if (filterSchemaChangeEvent) {
                return null;
            }
            return event;
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
