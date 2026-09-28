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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.exception.SchemaEvolutionException;
import org.apache.seatunnel.api.table.schema.exception.SchemaValidationException;
import org.apache.seatunnel.api.table.type.BasicType;

import org.apache.kafka.connect.source.SourceRecord;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;

class PostgresRelationSchemaChangeResolverTest {

    private static final String DATABASE_NAME = "postgres";
    private static final String SCHEMA_NAME = "inventory";
    private static final String TABLE_NAME = "customers";
    private static final TableIdentifier TABLE_IDENTIFIER =
            TableIdentifier.of(null, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME);

    private final PostgresRelationSchemaChangeResolver resolver =
            new PostgresRelationSchemaChangeResolver();

    @Test
    void shouldResolveAddedColumnsFromRelationRecord() {
        SourceRecord record =
                createRecord(
                        intColumn("id", 1),
                        varcharColumn("name", 2, 64),
                        varcharColumn("email", 3, 128),
                        booleanColumn("enabled", 4));

        Assertions.assertTrue(resolver.support(record));
        SchemaChangeEvent event =
                resolver.resolve(record, Collections.singletonList(createCatalogTable()));

        Assertions.assertInstanceOf(AlterTableColumnsEvent.class, event);
        AlterTableColumnsEvent columnsEvent = (AlterTableColumnsEvent) event;
        Assertions.assertEquals(2, columnsEvent.getEvents().size());

        AlterTableAddColumnEvent emailEvent =
                (AlterTableAddColumnEvent) columnsEvent.getEvents().get(0);
        Assertions.assertEquals("email", emailEvent.getColumn().getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, emailEvent.getColumn().getDataType());
        Assertions.assertEquals("name", emailEvent.getAfterColumn());

        AlterTableAddColumnEvent enabledEvent =
                (AlterTableAddColumnEvent) columnsEvent.getEvents().get(1);
        Assertions.assertEquals("enabled", enabledEvent.getColumn().getName());
        Assertions.assertEquals(BasicType.BOOLEAN_TYPE, enabledEvent.getColumn().getDataType());
        Assertions.assertEquals("email", enabledEvent.getAfterColumn());
    }

    @Test
    void shouldIgnoreUnchangedRelationRecord() {
        SourceRecord record = createRecord(intColumn("id", 1), varcharColumn("name", 2, 64));

        SchemaChangeEvent event =
                resolver.resolve(record, Collections.singletonList(createCatalogTable()));

        Assertions.assertNull(event);
    }

    @Test
    void shouldRejectNonAddColumnChanges() {
        SourceRecord record = createRecord(intColumn("id", 1));

        SchemaValidationException exception =
                Assertions.assertThrows(
                        SchemaValidationException.class,
                        () ->
                                resolver.resolve(
                                        record, Collections.singletonList(createCatalogTable())));

        Assertions.assertTrue(exception.getMessage().contains("supports only ADD COLUMN"));
    }

    @Test
    void shouldRejectRelationWhenCachedTableIsMissing() {
        SourceRecord record = createRecord(intColumn("id", 1), varcharColumn("name", 2, 64));

        SchemaValidationException exception =
                Assertions.assertThrows(
                        SchemaValidationException.class,
                        () -> resolver.resolve(record, Collections.emptyList()));

        Assertions.assertTrue(exception.getMessage().contains("Cannot find cached schema"));
    }

    @Test
    void shouldFailFastWhenAddedColumnTypeCannotBeConverted() {
        SourceRecord record =
                createRecord(
                        intColumn("id", 1),
                        varcharColumn("name", 2, 64),
                        unsupportedArrayColumn("roles", 3));

        SchemaEvolutionException exception =
                Assertions.assertThrows(
                        SchemaEvolutionException.class,
                        () ->
                                resolver.resolve(
                                        record, Collections.singletonList(createCatalogTable())));

        Assertions.assertTrue(
                exception
                        .getMessage()
                        .contains("Failed to resolve PostgreSQL RELATION schema change"));
        Assertions.assertNotNull(exception.getCause());
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
                                        .sourceType("int4")
                                        .build())
                        .column(
                                PhysicalColumn.builder()
                                        .name("name")
                                        .dataType(BasicType.STRING_TYPE)
                                        .nullable(true)
                                        .sourceType("varchar(64)")
                                        .build())
                        .build(),
                Collections.emptyMap(),
                Collections.emptyList(),
                null,
                null);
    }

    private SourceRecord createRecord(Column... columns) {
        Table relation =
                Table.editor()
                        .tableId(new TableId(null, SCHEMA_NAME, TABLE_NAME))
                        .setPrimaryKeyNames(Collections.singletonList("id"))
                        .setColumns(Arrays.asList(columns))
                        .create();
        return PostgresRelationSchemaRecord.create(
                relation,
                Collections.singletonMap("server", "postgres"),
                Collections.singletonMap("lsn", 100L),
                "postgres.inventory.customers");
    }

    private Column intColumn(String name, int position) {
        return Column.editor()
                .name(name)
                .jdbcType(Types.INTEGER)
                .nativeType(Types.INTEGER)
                .type("int4", "int4")
                .position(position)
                .optional(false)
                .create();
    }

    private Column varcharColumn(String name, int position, int length) {
        return Column.editor()
                .name(name)
                .jdbcType(Types.VARCHAR)
                .nativeType(Types.VARCHAR)
                .type("varchar", "varchar(" + length + ")")
                .length(length)
                .position(position)
                .optional(true)
                .create();
    }

    private Column booleanColumn(String name, int position) {
        return Column.editor()
                .name(name)
                .jdbcType(Types.BOOLEAN)
                .nativeType(Types.BOOLEAN)
                .type("bool", "bool")
                .position(position)
                .optional(false)
                .create();
    }

    private Column unsupportedArrayColumn(String name, int position) {
        return Column.editor()
                .name(name)
                .jdbcType(Types.ARRAY)
                .nativeType(Types.ARRAY)
                .type("_uuid", "uuid[]")
                .position(position)
                .optional(true)
                .create();
    }
}
