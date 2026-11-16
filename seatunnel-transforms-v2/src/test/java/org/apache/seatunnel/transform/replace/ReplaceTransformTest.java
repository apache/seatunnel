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
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.api.table.schema.event.RestoreTableSchemaEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.transform.exception.TransformException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class ReplaceTransformTest {

    private static CatalogTable catalogTable;

    @Test
    void testRestoreSchemaRebindsFieldsBeforeNextRow() {
        ReplaceTransform transform = replacementTransform("name", "title");
        transform.getProducedCatalogTable();
        CatalogTable restored = reorderedTable("title", "id", "name");
        RestoreTableSchemaEvent event = new RestoreTableSchemaEvent(restored);

        Assertions.assertSame(event, transform.mapSchemaChangeEvent(event));
        Assertions.assertArrayEquals(
                new Object[] {"after title", 1, "after name"},
                transform
                        .map(new SeaTunnelRow(new Object[] {"before title", 1, "before name"}))
                        .getFields());
        Assertions.assertEquals(restored.getTableSchema(), event.getChangeAfter().getTableSchema());
    }

    @Test
    void testInputSchemaHandoffRebindsFieldsBeforeNextRow() {
        ReplaceTransform transform = replacementTransform("name", "title");
        transform.getProducedCatalogTable();
        CatalogTable reordered = reorderedTable("title", "name", "id");
        transform.setInputCatalogTable(reordered);

        Assertions.assertArrayEquals(
                new Object[] {"after title", "after name", 1},
                transform
                        .map(new SeaTunnelRow(new Object[] {"before title", "before name", 1}))
                        .getFields());
        Assertions.assertEquals(
                reordered.getTableSchema(), transform.getProducedCatalogTable().getTableSchema());
    }

    private static ReplaceTransform replacementTransform(String... fields) {
        return new ReplaceTransform(replacementConfig(fields), catalogTable);
    }

    private static ReadonlyConfig replacementConfig(String... fields) {
        Map<String, Object> config = new HashMap<>();
        config.put("replace_fields", Arrays.asList(fields));
        config.put("pattern", "before");
        config.put("replacement", "after");
        return ReadonlyConfig.fromMap(config);
    }

    private static CatalogTable reorderedTable(String... fields) {
        TableSchema.Builder schema = TableSchema.builder();
        for (String field : fields) {
            schema.column(catalogTable.getTableSchema().getColumn(field));
        }
        return CatalogTable.of(
                catalogTable.getTableId(),
                schema.build(),
                catalogTable.getOptions(),
                catalogTable.getPartitionKeys(),
                catalogTable.getComment());
    }

    @Test
    void testAddDropAndReorderRebindFields() {
        ReplaceTransform transform = replacementTransform("name", "title");
        transform.mapSchemaChangeEvent(
                AlterTableAddColumnEvent.addFirst(
                        catalogTable.getTableId(),
                        PhysicalColumn.of("prefix", BasicType.STRING_TYPE, 10L, true, null, null)));
        Assertions.assertArrayEquals(
                new Object[] {"before prefix", 1, "after name", "after title"},
                transform
                        .map(
                                new SeaTunnelRow(
                                        new Object[] {
                                            "before prefix", 1, "before name", "before title"
                                        }))
                        .getFields());

        transform.mapSchemaChangeEvent(
                new AlterTableDropColumnEvent(catalogTable.getTableId(), "id"));
        Assertions.assertArrayEquals(
                new Object[] {"before prefix", "after name", "after title"},
                transform
                        .map(
                                new SeaTunnelRow(
                                        new Object[] {
                                            "before prefix", "before name", "before title"
                                        }))
                        .getFields());

        transform.mapSchemaChangeEvent(
                AlterTableModifyColumnEvent.modifyFirst(
                        catalogTable.getTableId(),
                        catalogTable.getTableSchema().getColumn("title")));
        Assertions.assertArrayEquals(
                new Object[] {"after title", "before prefix", "after name"},
                transform
                        .map(
                                new SeaTunnelRow(
                                        new Object[] {
                                            "before title", "before prefix", "before name"
                                        }))
                        .getFields());
    }

    @Test
    void testMissingTargetFailsDuringSchemaRefresh() {
        SchemaChangeEvent[] events = {
            new RestoreTableSchemaEvent(reorderedTable("id", "title")),
            new AlterTableDropColumnEvent(catalogTable.getTableId(), "name"),
            AlterTableChangeColumnEvent.change(
                    catalogTable.getTableId(),
                    "name",
                    PhysicalColumn.of("renamed", BasicType.STRING_TYPE, 10L, true, null, null))
        };
        for (SchemaChangeEvent event : events) {
            ReplaceTransform transform = replacementTransform("title", "name");
            TransformException error =
                    Assertions.assertThrows(
                            TransformException.class, () -> transform.mapSchemaChangeEvent(event));
            Assertions.assertTrue(error.getMessage().contains("name"));
        }
        ReplaceTransform transform = replacementTransform("name");
        Assertions.assertThrows(
                TransformException.class,
                () -> transform.setInputCatalogTable(reorderedTable("id", "title")));
    }

    @Test
    void testRestorePreservesRowMetadataNullsAndInput() {
        ReplaceTransform transform = replacementTransform("name", "title");
        transform.mapSchemaChangeEvent(
                new RestoreTableSchemaEvent(reorderedTable("title", "id", "name")));
        SeaTunnelRow input = new SeaTunnelRow(new Object[] {null, 1, "before name"});
        input.setRowKind(RowKind.UPDATE_AFTER);
        input.setTableId(catalogTable.getTableId().toTablePath().toString());
        input.getOptions().put("sequence", 123L);
        SeaTunnelRow output = transform.map(input);
        Assertions.assertArrayEquals(new Object[] {null, 1, "after name"}, output.getFields());
        Assertions.assertArrayEquals(new Object[] {null, 1, "before name"}, input.getFields());
        Assertions.assertEquals(input.getRowKind(), output.getRowKind());
        Assertions.assertEquals(input.getTableId(), output.getTableId());
        Assertions.assertEquals(input.getOptions(), output.getOptions());
    }

    @Test
    void testTypeChangeKeepsExistingValueConversion() {
        ReplaceTransform transform = replacementTransform("name");
        transform.mapSchemaChangeEvent(
                AlterTableModifyColumnEvent.modifyFirst(
                        catalogTable.getTableId(),
                        PhysicalColumn.of("name", BasicType.INT_TYPE, 10L, true, null, null)));
        Assertions.assertArrayEquals(
                new Object[] {"123", 1, "before title"},
                transform.map(new SeaTunnelRow(new Object[] {123, 1, "before title"})).getFields());
        Assertions.assertEquals(
                BasicType.INT_TYPE,
                transform.getProducedCatalogTable().getSeaTunnelRowType().getFieldType(0));
    }

    @Test
    void testMultiTableRestoreOnlyRebindsAffectedTable() {
        CatalogTable second =
                CatalogTable.of(
                        TableIdentifier.of("catalog", TablePath.of("other")),
                        catalogTable.getTableSchema(),
                        new HashMap<>(),
                        new ArrayList<>(),
                        "other");
        ReplaceMultiCatalogTransform transform =
                new ReplaceMultiCatalogTransform(
                        Arrays.asList(catalogTable, second), replacementConfig("name", "title"));
        CatalogTable restored = reorderedTable("title", "id", "name");
        transform.mapSchemaChangeEvent(new RestoreTableSchemaEvent(restored));

        SeaTunnelRow firstInput = new SeaTunnelRow(new Object[] {"before title", 1, "before name"});
        firstInput.setTableId(catalogTable.getTableId().toTablePath().toString());
        Assertions.assertArrayEquals(
                new Object[] {"after title", 1, "after name"},
                transform.map(firstInput).getFields());
        SeaTunnelRow secondInput =
                new SeaTunnelRow(new Object[] {2, "before other", "before title"});
        secondInput.setTableId(second.getTableId().toTablePath().toString());
        Assertions.assertArrayEquals(
                new Object[] {2, "after other", "after title"},
                transform.map(secondInput).getFields());
        Assertions.assertEquals(
                restored.getTableSchema(),
                transform.getProducedCatalogTables().get(0).getTableSchema());
        Assertions.assertEquals(
                second.getTableSchema(),
                transform.getProducedCatalogTables().get(1).getTableSchema());
    }

    @Test
    void testAllMatchChainRebindsOnRestoreAndInputHandoff() {
        Map<String, Object> firstRule = new HashMap<>();
        firstRule.put("table_path", catalogTable.getTableId().toTablePath().toString());
        firstRule.put("replace_fields", Arrays.asList("name", "title"));
        firstRule.put("pattern", "before");
        firstRule.put("replacement", "middle");
        Map<String, Object> secondRule = new HashMap<>(firstRule);
        secondRule.put("pattern", "middle");
        secondRule.put("replacement", "after");
        Map<String, Object> config = new HashMap<>();
        config.put("rule_match_mode", "ALL_MATCH");
        config.put("table_transform", Arrays.asList(firstRule, secondRule));
        ReplaceMultiCatalogTransform transform =
                new ReplaceMultiCatalogTransform(
                        Collections.singletonList(catalogTable), ReadonlyConfig.fromMap(config));
        transform.mapSchemaChangeEvent(
                new RestoreTableSchemaEvent(reorderedTable("title", "id", "name")));
        Assertions.assertArrayEquals(
                new Object[] {"after title", 1, "after name"},
                transform
                        .map(new SeaTunnelRow(new Object[] {"before title", 1, "before name"}))
                        .getFields());
        transform.setInputCatalogTables(
                Collections.singletonList(reorderedTable("name", "title", "id")));
        Assertions.assertArrayEquals(
                new Object[] {"after name", "after title", 1},
                transform
                        .map(new SeaTunnelRow(new Object[] {"before name", "before title", 1}))
                        .getFields());
    }

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
    void testFallbackKeyUsedWhenPrimaryAbsent() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("replace_field", "name");
        configMap.put(ReplaceTransformConfig.KEY_PATTERN.key(), "before");
        configMap.put(ReplaceTransformConfig.KEY_REPLACEMENT.key(), "after");

        ReplaceTransform transform =
                new ReplaceTransform(ReadonlyConfig.fromMap(configMap), catalogTable);
        SeaTunnelRow input = new SeaTunnelRow(new Object[] {1, "before name", "title"});
        SeaTunnelRow output = transform.transformRow(input);
        Assertions.assertEquals("after name", output.getField(1));
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
        OptionRule rule = new ReplaceTransformFactory().optionRule();
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ReplaceTransformConfig.KEY_REPLACE_FIELDS.key(), new ArrayList<String>());
        configMap.put(ReplaceTransformConfig.KEY_PATTERN.key(), "before");
        configMap.put(ReplaceTransformConfig.KEY_REPLACEMENT.key(), "after");

        OptionValidationException exception =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () -> ConfigValidator.of(ReadonlyConfig.fromMap(configMap)).validate(rule));

        Assertions.assertTrue(
                exception.getMessage().contains("replace_fields"),
                "Should mention replace_fields: " + exception.getMessage());
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

    @Test
    void testReplaceDoesNotMutateInputRowForFanOut() {
        Map<String, Object> phoneReplaceConfig = new HashMap<>();
        phoneReplaceConfig.put("replace_field", "title");
        phoneReplaceConfig.put(ReplaceTransformConfig.KEY_PATTERN.key(), "1");
        phoneReplaceConfig.put(ReplaceTransformConfig.KEY_REPLACEMENT.key(), "a");

        Map<String, Object> nameReplaceConfig = new HashMap<>();
        nameReplaceConfig.put("replace_field", "name");
        nameReplaceConfig.put(ReplaceTransformConfig.KEY_PATTERN.key(), "before");
        nameReplaceConfig.put(ReplaceTransformConfig.KEY_REPLACEMENT.key(), "after");

        ReplaceTransform phoneReplaceTransform =
                new ReplaceTransform(ReadonlyConfig.fromMap(phoneReplaceConfig), catalogTable);
        ReplaceTransform nameReplaceTransform =
                new ReplaceTransform(ReadonlyConfig.fromMap(nameReplaceConfig), catalogTable);

        SeaTunnelRow input = new SeaTunnelRow(new Object[] {1, "before name", "17111999384"});

        SeaTunnelRow phoneOutput = phoneReplaceTransform.transformRow(input);
        SeaTunnelRow nameOutput = nameReplaceTransform.transformRow(input);

        Assertions.assertEquals("a7aaa999384", phoneOutput.getField(2));
        Assertions.assertEquals("after name", nameOutput.getField(1));

        // The sibling transform branch should still see the original title value.
        Assertions.assertEquals("17111999384", nameOutput.getField(2));

        // ReplaceTransform should not mutate the shared input row.
        Assertions.assertEquals("before name", input.getField(1));
        Assertions.assertEquals("17111999384", input.getField(2));
    }
}
