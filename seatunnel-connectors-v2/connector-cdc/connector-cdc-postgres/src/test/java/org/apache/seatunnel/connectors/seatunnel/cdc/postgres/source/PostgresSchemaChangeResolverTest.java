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
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.BasicType;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.data.Envelope;
import io.debezium.relational.history.HistoryRecord;

import java.util.Collections;
import java.util.List;

public class PostgresSchemaChangeResolverTest {

    @Test
    public void testSupportRecognizesPostgresSchemaChangeRecord() {
        PostgresSchemaChangeResolver resolver =
                new PostgresSchemaChangeResolver(
                        new org.apache.seatunnel.connectors.seatunnel.cdc.postgres.config
                                        .PostgresSourceConfigFactory()
                                .hostname("localhost")
                                .port(5432)
                                .username("postgres")
                                .password("postgres")
                                .databaseList("postgres_cdc"));

        SourceRecord record = buildRecord("ALTER TABLE t1 ADD COLUMN f_added bigint");

        Assertions.assertTrue(resolver.support(record));
    }

    @Test
    public void testResolveRenameColumnKeepsColumnType() {
        PostgresSchemaChangeResolver resolver =
                new PostgresSchemaChangeResolver(
                        new org.apache.seatunnel.connectors.seatunnel.cdc.postgres.config
                                        .PostgresSourceConfigFactory()
                                .hostname("localhost")
                                .port(5432)
                                .username("postgres")
                                .password("postgres")
                                .databaseList("postgres_cdc"));

        TablePath tablePath = TablePath.of("postgres_cdc", "inventory", "t1");
        SourceRecord record = buildRecord("ALTER TABLE t1 RENAME COLUMN f_int TO f_integer");
        List<CatalogTable> catalogTables =
                Collections.singletonList(
                        CatalogTable.of(
                                TableIdentifier.of(null, tablePath),
                                TableSchema.builder()
                                        .column(
                                                PhysicalColumn.of(
                                                        "f_int",
                                                        BasicType.INT_TYPE,
                                                        (Long) null,
                                                        true,
                                                        null,
                                                        null))
                                        .build(),
                                Collections.emptyMap(),
                                Collections.emptyList(),
                                null));

        SchemaChangeEvent schemaChangeEvent = resolver.resolve(record, catalogTables);
        Assertions.assertNotNull(schemaChangeEvent);
        Assertions.assertTrue(schemaChangeEvent instanceof AlterTableColumnsEvent);
        AlterTableColumnsEvent columnsEvent = (AlterTableColumnsEvent) schemaChangeEvent;
        Assertions.assertEquals(1, columnsEvent.getEvents().size());
        AlterTableChangeColumnEvent changeColumnEvent =
                (AlterTableChangeColumnEvent) columnsEvent.getEvents().get(0);
        Assertions.assertEquals("f_int", changeColumnEvent.getOldColumn());
        Assertions.assertEquals("f_integer", changeColumnEvent.getColumn().getName());
        Assertions.assertEquals(BasicType.INT_TYPE, changeColumnEvent.getColumn().getDataType());
    }

    private SourceRecord buildRecord(String ddl) {
        Schema sourceSchema =
                SchemaBuilder.struct()
                        .name("io.debezium.connector.postgresql.Source")
                        .field(AbstractSourceInfo.DATABASE_NAME_KEY, Schema.STRING_SCHEMA)
                        .field(AbstractSourceInfo.SCHEMA_NAME_KEY, Schema.STRING_SCHEMA)
                        .field(AbstractSourceInfo.TABLE_NAME_KEY, Schema.STRING_SCHEMA)
                        .build();
        Schema valueSchema =
                SchemaBuilder.struct()
                        .name("test.History")
                        .field(Envelope.FieldName.SOURCE, sourceSchema)
                        .field(HistoryRecord.Fields.DDL_STATEMENTS, Schema.STRING_SCHEMA)
                        .field(
                                HistoryRecord.Fields.TABLE_CHANGES,
                                SchemaBuilder.array(SchemaBuilder.struct().build()).build())
                        .build();
        Struct value =
                new Struct(valueSchema)
                        .put(
                                Envelope.FieldName.SOURCE,
                                new Struct(sourceSchema)
                                        .put(AbstractSourceInfo.DATABASE_NAME_KEY, "postgres_cdc")
                                        .put(AbstractSourceInfo.SCHEMA_NAME_KEY, "inventory")
                                        .put(AbstractSourceInfo.TABLE_NAME_KEY, "t1"))
                        .put(HistoryRecord.Fields.DDL_STATEMENTS, ddl)
                        .put(
                                HistoryRecord.Fields.TABLE_CHANGES,
                                Collections.singletonList(
                                        new Struct(SchemaBuilder.struct().build())));
        Schema keySchema =
                SchemaBuilder.struct()
                        .name("io.debezium.connector.postgresql.SchemaChangeKey")
                        .build();
        return new SourceRecord(
                Collections.emptyMap(),
                Collections.emptyMap(),
                "test-topic",
                null,
                keySchema,
                new Struct(keySchema),
                valueSchema,
                value);
    }
}
