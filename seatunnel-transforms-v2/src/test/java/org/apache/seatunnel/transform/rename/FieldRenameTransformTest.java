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

package org.apache.seatunnel.transform.rename;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.api.table.type.BasicType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;

public class FieldRenameTransformTest {

    private static final CatalogTable DEFAULT_TABLE =
            CatalogTable.of(
                    TableIdentifier.of("test", "Database-x", "Schema-x", "Table-x"),
                    TableSchema.builder()
                            .column(
                                    PhysicalColumn.of(
                                            "f1",
                                            BasicType.LONG_TYPE,
                                            null,
                                            null,
                                            false,
                                            null,
                                            null))
                            .column(
                                    PhysicalColumn.of(
                                            "f2",
                                            BasicType.LONG_TYPE,
                                            null,
                                            null,
                                            true,
                                            null,
                                            null))
                            .column(
                                    PhysicalColumn.of(
                                            "f3",
                                            BasicType.LONG_TYPE,
                                            null,
                                            null,
                                            true,
                                            null,
                                            null))
                            .primaryKey(PrimaryKey.of("pk1", Arrays.asList("f1")))
                            .constraintKey(
                                    ConstraintKey.of(
                                            ConstraintKey.ConstraintType.UNIQUE_KEY,
                                            "uk1",
                                            Arrays.asList(
                                                    ConstraintKey.ConstraintKeyColumn.of(
                                                            "f2", ConstraintKey.ColumnSortType.ASC),
                                                    ConstraintKey.ConstraintKeyColumn.of(
                                                            "f3",
                                                            ConstraintKey.ColumnSortType.ASC))))
                            .build(),
                    Collections.emptyMap(),
                    Collections.singletonList("f2"),
                    null);

    @Test
    public void testRename() {
        AlterTableAddColumnEvent addColumnEvent =
                AlterTableAddColumnEvent.add(
                        DEFAULT_TABLE.getTableId(),
                        PhysicalColumn.of("f4", BasicType.LONG_TYPE, null, null, true, null, null));
        AlterTableModifyColumnEvent modifyColumnEvent =
                AlterTableModifyColumnEvent.modify(
                        DEFAULT_TABLE.getTableId(),
                        PhysicalColumn.of("f4", BasicType.INT_TYPE, null, null, true, null, null));
        AlterTableChangeColumnEvent changeColumnEvent =
                AlterTableChangeColumnEvent.change(
                        DEFAULT_TABLE.getTableId(),
                        "f4",
                        PhysicalColumn.of("f5", BasicType.INT_TYPE, null, null, true, null, null));
        AlterTableDropColumnEvent dropColumnEvent =
                new AlterTableDropColumnEvent(DEFAULT_TABLE.getTableId(), "f5");

        FieldRenameConfig config = new FieldRenameConfig().setConvertCase(ConvertCase.LOWER);
        FieldRenameTransform transform = new FieldRenameTransform(config, DEFAULT_TABLE);
        CatalogTable outputCatalogTable = transform.getProducedCatalogTable();
        AlterTableAddColumnEvent outputAddEvent =
                (AlterTableAddColumnEvent) transform.mapSchemaChangeEvent(addColumnEvent);
        AlterTableModifyColumnEvent outputModifyEvent =
                (AlterTableModifyColumnEvent) transform.mapSchemaChangeEvent(modifyColumnEvent);
        AlterTableChangeColumnEvent outputChangeEvent =
                (AlterTableChangeColumnEvent) transform.mapSchemaChangeEvent(changeColumnEvent);
        AlterTableDropColumnEvent outputDropEvent =
                (AlterTableDropColumnEvent) transform.mapSchemaChangeEvent(dropColumnEvent);

        Assertions.assertIterableEquals(
                Arrays.asList("f1", "f2", "f3"),
                Arrays.asList(outputCatalogTable.getTableSchema().getFieldNames()));
        Assertions.assertIterableEquals(
                Arrays.asList("f1"),
                outputCatalogTable.getTableSchema().getPrimaryKey().getColumnNames());
        outputCatalogTable.getTableSchema().getConstraintKeys().stream()
                .forEach(
                        key ->
                                Assertions.assertIterableEquals(
                                        Arrays.asList("f2", "f3"),
                                        key.getColumnNames().stream()
                                                .map(
                                                        ConstraintKey.ConstraintKeyColumn
                                                                ::getColumnName)
                                                .collect(Collectors.toList())));
        Assertions.assertEquals("f4", outputAddEvent.getColumn().getName());
        Assertions.assertEquals("f4", outputModifyEvent.getColumn().getName());
        Assertions.assertEquals("f4", outputChangeEvent.getOldColumn());
        Assertions.assertEquals("f5", outputChangeEvent.getColumn().getName());
        Assertions.assertEquals("f5", outputDropEvent.getColumn());

        config = new FieldRenameConfig().setConvertCase(ConvertCase.UPPER);
        transform = new FieldRenameTransform(config, DEFAULT_TABLE);
        outputCatalogTable = transform.getProducedCatalogTable();
        outputAddEvent = (AlterTableAddColumnEvent) transform.mapSchemaChangeEvent(addColumnEvent);
        outputModifyEvent =
                (AlterTableModifyColumnEvent) transform.mapSchemaChangeEvent(modifyColumnEvent);
        outputChangeEvent =
                (AlterTableChangeColumnEvent) transform.mapSchemaChangeEvent(changeColumnEvent);
        outputDropEvent =
                (AlterTableDropColumnEvent) transform.mapSchemaChangeEvent(dropColumnEvent);
        Assertions.assertIterableEquals(
                Arrays.asList("F1", "F2", "F3"),
                Arrays.asList(outputCatalogTable.getTableSchema().getFieldNames()));
        Assertions.assertIterableEquals(
                Arrays.asList("F1"),
                outputCatalogTable.getTableSchema().getPrimaryKey().getColumnNames());
        outputCatalogTable.getTableSchema().getConstraintKeys().stream()
                .forEach(
                        key ->
                                Assertions.assertIterableEquals(
                                        Arrays.asList("F2", "F3"),
                                        key.getColumnNames().stream()
                                                .map(
                                                        ConstraintKey.ConstraintKeyColumn
                                                                ::getColumnName)
                                                .collect(Collectors.toList())));
        Assertions.assertEquals("F4", outputAddEvent.getColumn().getName());
        Assertions.assertEquals("F4", outputModifyEvent.getColumn().getName());
        Assertions.assertEquals("f4", outputChangeEvent.getOldColumn());
        Assertions.assertEquals("f5", outputChangeEvent.getColumn().getName());
        Assertions.assertEquals("F5", outputDropEvent.getColumn());

        config = new FieldRenameConfig().setPrefix("p-").setSuffix("-s");
        transform = new FieldRenameTransform(config, DEFAULT_TABLE);
        outputCatalogTable = transform.getProducedCatalogTable();
        outputAddEvent = (AlterTableAddColumnEvent) transform.mapSchemaChangeEvent(addColumnEvent);
        outputModifyEvent =
                (AlterTableModifyColumnEvent) transform.mapSchemaChangeEvent(modifyColumnEvent);
        outputChangeEvent =
                (AlterTableChangeColumnEvent) transform.mapSchemaChangeEvent(changeColumnEvent);
        outputDropEvent =
                (AlterTableDropColumnEvent) transform.mapSchemaChangeEvent(dropColumnEvent);
        Assertions.assertIterableEquals(
                Arrays.asList("p-f1-s", "p-f2-s", "p-f3-s"),
                Arrays.asList(outputCatalogTable.getTableSchema().getFieldNames()));
        Assertions.assertIterableEquals(
                Arrays.asList("p-f1-s"),
                outputCatalogTable.getTableSchema().getPrimaryKey().getColumnNames());
        outputCatalogTable.getTableSchema().getConstraintKeys().stream()
                .forEach(
                        key ->
                                Assertions.assertIterableEquals(
                                        Arrays.asList("p-f2-s", "p-f3-s"),
                                        key.getColumnNames().stream()
                                                .map(
                                                        ConstraintKey.ConstraintKeyColumn
                                                                ::getColumnName)
                                                .collect(Collectors.toList())));
        Assertions.assertEquals("p-f4-s", outputAddEvent.getColumn().getName());
        Assertions.assertEquals("p-f4-s", outputModifyEvent.getColumn().getName());
        Assertions.assertEquals("f4", outputChangeEvent.getOldColumn());
        Assertions.assertEquals("f5", outputChangeEvent.getColumn().getName());
        Assertions.assertEquals("p-f5-s", outputDropEvent.getColumn());

        config =
                new FieldRenameConfig()
                        .setReplacementsWithRegex(
                                Arrays.asList(
                                        new FieldRenameConfig.ReplacementsWithRegex(
                                                "f1", "t1", true),
                                        new FieldRenameConfig.ReplacementsWithRegex(
                                                "f1", "t2", true)));
        transform = new FieldRenameTransform(config, DEFAULT_TABLE);
        outputCatalogTable = transform.getProducedCatalogTable();
        outputAddEvent = (AlterTableAddColumnEvent) transform.mapSchemaChangeEvent(addColumnEvent);
        outputModifyEvent =
                (AlterTableModifyColumnEvent) transform.mapSchemaChangeEvent(modifyColumnEvent);
        outputChangeEvent =
                (AlterTableChangeColumnEvent) transform.mapSchemaChangeEvent(changeColumnEvent);
        outputDropEvent =
                (AlterTableDropColumnEvent) transform.mapSchemaChangeEvent(dropColumnEvent);
        Assertions.assertIterableEquals(
                Arrays.asList("t2", "f2", "f3"),
                Arrays.asList(outputCatalogTable.getTableSchema().getFieldNames()));
        Assertions.assertIterableEquals(
                Arrays.asList("t2"),
                outputCatalogTable.getTableSchema().getPrimaryKey().getColumnNames());
        outputCatalogTable.getTableSchema().getConstraintKeys().stream()
                .forEach(
                        key ->
                                Assertions.assertIterableEquals(
                                        Arrays.asList("f2", "f3"),
                                        key.getColumnNames().stream()
                                                .map(
                                                        ConstraintKey.ConstraintKeyColumn
                                                                ::getColumnName)
                                                .collect(Collectors.toList())));
        Assertions.assertEquals("f4", outputAddEvent.getColumn().getName());
        Assertions.assertEquals("f4", outputModifyEvent.getColumn().getName());
        Assertions.assertEquals("f4", outputChangeEvent.getOldColumn());
        Assertions.assertEquals("f5", outputChangeEvent.getColumn().getName());
        Assertions.assertEquals("f5", outputDropEvent.getColumn());
    }
}
