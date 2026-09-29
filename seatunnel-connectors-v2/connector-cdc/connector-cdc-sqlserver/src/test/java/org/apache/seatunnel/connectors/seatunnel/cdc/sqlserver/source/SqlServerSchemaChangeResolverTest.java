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

package org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.source;

import org.apache.seatunnel.api.event.EventType;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.cdc.debezium.ConnectTableChangeSerializer;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.data.Envelope;
import io.debezium.relational.Table;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.TableChanges;

import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;

public class SqlServerSchemaChangeResolverTest {

    private static final String DATABASE_NAME = "test_db";
    private static final String SCHEMA_NAME = "dbo";
    private static final String TABLE_NAME = "customers";
    private static final TableIdentifier TABLE_IDENTIFIER =
            TableIdentifier.of(null, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME);
    private static final Schema KEY_SCHEMA =
            SchemaBuilder.struct().name("io.debezium.connector.sqlserver.SchemaChangeKey").build();
    private static final Schema SOURCE_SCHEMA =
            SchemaBuilder.struct()
                    .field(AbstractSourceInfo.DATABASE_NAME_KEY, Schema.STRING_SCHEMA)
                    .field(AbstractSourceInfo.SCHEMA_NAME_KEY, Schema.STRING_SCHEMA)
                    .field(AbstractSourceInfo.TABLE_NAME_KEY, Schema.STRING_SCHEMA)
                    .build();
    private static final Schema VALUE_SCHEMA =
            SchemaBuilder.struct()
                    .field(Envelope.FieldName.SOURCE, SOURCE_SCHEMA)
                    .field(HistoryRecord.Fields.DDL_STATEMENTS, Schema.OPTIONAL_STRING_SCHEMA)
                    .field(
                            HistoryRecord.Fields.TABLE_CHANGES,
                            SchemaBuilder.array(ConnectTableChangeSerializer.CHANGE_SCHEMA).build())
                    .build();

    private final SqlServerSchemaChangeResolver resolver = new SqlServerSchemaChangeResolver();
    private final ConnectTableChangeSerializer serializer = new ConnectTableChangeSerializer();

    @Test
    void shouldRecognizeSqlServerSchemaChangeRecord() {
        SourceRecord record = createRecord(createAlterTableChange(varcharColumn("email", 3, 128)));

        Assertions.assertTrue(resolver.support(record));
    }

    @Test
    void shouldResolveAddColumnEventFromTableChanges() {
        SourceRecord record = createRecord(createAlterTableChange(varcharColumn("email", 3, 128)));

        SchemaChangeEvent event =
                resolver.resolve(record, Collections.singletonList(createCatalogTable()));

        Assertions.assertInstanceOf(AlterTableColumnsEvent.class, event);
        AlterTableColumnsEvent columnsEvent = (AlterTableColumnsEvent) event;
        Assertions.assertEquals(
                EventType.SCHEMA_CHANGE_UPDATE_COLUMNS, columnsEvent.getEventType());
        Assertions.assertEquals(1, columnsEvent.getEvents().size());
        Assertions.assertInstanceOf(
                AlterTableAddColumnEvent.class, columnsEvent.getEvents().get(0));
        AlterTableAddColumnEvent addColumnEvent =
                (AlterTableAddColumnEvent) columnsEvent.getEvents().get(0);
        Assertions.assertEquals("email", addColumnEvent.getColumn().getName());
        Assertions.assertEquals("name", addColumnEvent.getAfterColumn());
    }

    @Test
    void shouldResolveRenameColumnEventFromTableChanges() {
        TableChanges tableChanges = new TableChanges();
        tableChanges.alter(
                Table.editor()
                        .tableId(
                                new io.debezium.relational.TableId(
                                        DATABASE_NAME, SCHEMA_NAME, TABLE_NAME))
                        .setPrimaryKeyNames(Collections.singletonList("id"))
                        .setColumns(
                                Arrays.asList(
                                        intColumn("id", 1), varcharColumn("full_name", 2, 64)))
                        .create());
        SourceRecord record =
                createRecord(
                        tableChanges,
                        DATABASE_NAME,
                        SCHEMA_NAME,
                        TABLE_NAME,
                        "EXEC sp_rename 'dbo.customers.name', 'full_name', 'COLUMN'");

        SchemaChangeEvent event =
                resolver.resolve(record, Collections.singletonList(createCatalogTable()));

        Assertions.assertInstanceOf(AlterTableColumnsEvent.class, event);
        AlterTableColumnsEvent columnsEvent = (AlterTableColumnsEvent) event;
        Assertions.assertEquals(1, columnsEvent.getEvents().size());
        Assertions.assertInstanceOf(
                AlterTableChangeColumnEvent.class, columnsEvent.getEvents().get(0));
        AlterTableChangeColumnEvent changeEvent =
                (AlterTableChangeColumnEvent) columnsEvent.getEvents().get(0);
        Assertions.assertEquals("name", changeEvent.getOldColumn());
        Assertions.assertEquals("full_name", changeEvent.getColumn().getName());
        Assertions.assertEquals("id", changeEvent.getAfterColumn());
    }

    @Test
    void shouldEmitDropAndAddWhenDdlIsNotExplicitRename() {
        TableChanges tableChanges = new TableChanges();
        tableChanges.alter(
                Table.editor()
                        .tableId(
                                new io.debezium.relational.TableId(
                                        DATABASE_NAME, SCHEMA_NAME, TABLE_NAME))
                        .setPrimaryKeyNames(Collections.singletonList("id"))
                        .setColumns(
                                Arrays.asList(
                                        intColumn("id", 1), varcharColumn("full_name", 2, 64)))
                        .create());
        SourceRecord record =
                createRecord(
                        tableChanges,
                        DATABASE_NAME,
                        SCHEMA_NAME,
                        TABLE_NAME,
                        "ALTER TABLE dbo.customers DROP COLUMN name; ALTER TABLE dbo.customers ADD full_name VARCHAR(64)");

        SchemaChangeEvent event =
                resolver.resolve(record, Collections.singletonList(createCatalogTable()));

        Assertions.assertInstanceOf(AlterTableColumnsEvent.class, event);
        AlterTableColumnsEvent columnsEvent = (AlterTableColumnsEvent) event;
        Assertions.assertEquals(2, columnsEvent.getEvents().size());
        Assertions.assertInstanceOf(
                AlterTableAddColumnEvent.class, columnsEvent.getEvents().get(0));
        Assertions.assertInstanceOf(
                AlterTableDropColumnEvent.class, columnsEvent.getEvents().get(1));

        AlterTableAddColumnEvent addEvent =
                (AlterTableAddColumnEvent) columnsEvent.getEvents().get(0);
        Assertions.assertEquals("full_name", addEvent.getColumn().getName());
        Assertions.assertEquals("id", addEvent.getAfterColumn());

        AlterTableDropColumnEvent dropEvent =
                (AlterTableDropColumnEvent) columnsEvent.getEvents().get(1);
        Assertions.assertEquals("name", dropEvent.getColumn());
    }

    @Test
    void shouldResolveSchemaChangeWhenIdentifiersAreBracketed() {
        SourceRecord record =
                createRecord(
                        createAlterTableChange(varcharColumn("email", 3, 128)),
                        "[test_db]",
                        "[dbo]",
                        "[customers]",
                        "ALTER TABLE [test_db].[dbo].[customers] ADD [email] VARCHAR(128)");

        SchemaChangeEvent event =
                resolver.resolve(record, Collections.singletonList(createCatalogTable()));

        Assertions.assertInstanceOf(AlterTableColumnsEvent.class, event);
        AlterTableColumnsEvent columnsEvent = (AlterTableColumnsEvent) event;
        Assertions.assertEquals(1, columnsEvent.getEvents().size());
        Assertions.assertInstanceOf(
                AlterTableAddColumnEvent.class, columnsEvent.getEvents().get(0));
    }

    private CatalogTable createCatalogTable() {
        return CatalogTable.of(
                TABLE_IDENTIFIER,
                TableSchema.builder()
                        .column(
                                PhysicalColumn.builder()
                                        .name("id")
                                        .dataType(BasicType.INT_TYPE)
                                        .nullable(false)
                                        .columnLength(10L)
                                        .sourceType("int")
                                        .build())
                        .column(
                                PhysicalColumn.builder()
                                        .name("name")
                                        .dataType(BasicType.STRING_TYPE)
                                        .nullable(true)
                                        .columnLength(64L)
                                        .sourceType("varchar(64)")
                                        .build())
                        .build(),
                Collections.emptyMap(),
                Collections.emptyList(),
                null,
                null);
    }

    private TableChanges createAlterTableChange(io.debezium.relational.Column newColumn) {
        TableChanges tableChanges = new TableChanges();
        tableChanges.alter(
                Table.editor()
                        .tableId(
                                new io.debezium.relational.TableId(
                                        DATABASE_NAME, SCHEMA_NAME, TABLE_NAME))
                        .setPrimaryKeyNames(Collections.singletonList("id"))
                        .setColumns(
                                Arrays.asList(
                                        intColumn("id", 1),
                                        varcharColumn("name", 2, 64),
                                        newColumn))
                        .create());
        return tableChanges;
    }

    private SourceRecord createRecord(TableChanges tableChanges) {
        return createRecord(tableChanges, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, "ALTER TABLE");
    }

    private SourceRecord createRecord(
            TableChanges tableChanges,
            String databaseName,
            String schemaName,
            String tableName,
            String ddl) {
        Struct value = new Struct(VALUE_SCHEMA);
        value.put(
                Envelope.FieldName.SOURCE,
                new Struct(SOURCE_SCHEMA)
                        .put(AbstractSourceInfo.DATABASE_NAME_KEY, databaseName)
                        .put(AbstractSourceInfo.SCHEMA_NAME_KEY, schemaName)
                        .put(AbstractSourceInfo.TABLE_NAME_KEY, tableName));
        value.put(HistoryRecord.Fields.DDL_STATEMENTS, ddl);
        value.put(HistoryRecord.Fields.TABLE_CHANGES, serializer.serialize(tableChanges));
        return new SourceRecord(
                Collections.emptyMap(),
                Collections.emptyMap(),
                "topic",
                null,
                KEY_SCHEMA,
                new Struct(KEY_SCHEMA),
                VALUE_SCHEMA,
                value);
    }

    private io.debezium.relational.Column intColumn(String name, int position) {
        return io.debezium.relational.Column.editor()
                .name(name)
                .jdbcType(Types.INTEGER)
                .nativeType(Types.INTEGER)
                .type("int", "int")
                .position(position)
                .optional(false)
                .create();
    }

    private io.debezium.relational.Column varcharColumn(String name, int position, int length) {
        return io.debezium.relational.Column.editor()
                .name(name)
                .jdbcType(Types.VARCHAR)
                .nativeType(Types.VARCHAR)
                .type("varchar", "varchar(" + length + ")")
                .length(length)
                .position(position)
                .optional(true)
                .create();
    }
}
