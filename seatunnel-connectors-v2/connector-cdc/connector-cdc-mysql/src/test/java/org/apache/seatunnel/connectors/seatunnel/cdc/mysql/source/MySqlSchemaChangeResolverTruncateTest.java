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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source;

import org.apache.seatunnel.api.event.EventType;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.operation.event.TableOperationEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config.MySqlSourceConfigFactory;

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

class MySqlSchemaChangeResolverTruncateTest {

    private static final Schema KEY_SCHEMA =
            SchemaBuilder.struct().name("io.debezium.connector.mysql.SchemaChangeKey").build();
    private static final Schema SOURCE_SCHEMA =
            SchemaBuilder.struct()
                    .field(AbstractSourceInfo.DATABASE_NAME_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                    .build();
    private static final Schema VALUE_SCHEMA =
            SchemaBuilder.struct()
                    .field(Envelope.FieldName.SOURCE, SOURCE_SCHEMA)
                    .field(HistoryRecord.Fields.DDL_STATEMENTS, Schema.OPTIONAL_STRING_SCHEMA)
                    .field(
                            HistoryRecord.Fields.TABLE_CHANGES,
                            SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build())
                    .build();

    private static CatalogTable capturedTable(String database, String table) {
        return CatalogTable.of(
                TableIdentifier.of("mysql", database, table),
                TableSchema.builder()
                        .column(PhysicalColumn.of("id", BasicType.INT_TYPE, 11L, false, null, ""))
                        .build(),
                Collections.emptyMap(),
                Collections.emptyList(),
                null);
    }

    private static SourceRecord truncateRecord(String database, String ddl) {
        Struct value = new Struct(VALUE_SCHEMA);
        Struct source = new Struct(SOURCE_SCHEMA);
        if (database != null) {
            source.put(AbstractSourceInfo.DATABASE_NAME_KEY, database);
        }
        value.put(Envelope.FieldName.SOURCE, source);
        value.put(HistoryRecord.Fields.DDL_STATEMENTS, ddl);
        value.put(HistoryRecord.Fields.TABLE_CHANGES, Collections.emptyList());
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

    private static MySqlSchemaChangeResolver resolver(boolean tableOperationsEnabled) {
        return resolver(tableOperationsEnabled, true);
    }

    private static MySqlSchemaChangeResolver resolver(
            boolean tableOperationsEnabled, boolean schemaChangesEnabled) {
        MySqlSourceConfigFactory factory = new MySqlSourceConfigFactory();
        factory.hostname("localhost");
        factory.username("test");
        factory.password("test");
        return new MySqlSchemaChangeResolver(factory, tableOperationsEnabled, schemaChangesEnabled);
    }

    private static SourceRecord alterRecord(String database, String ddl) {
        Struct value = new Struct(VALUE_SCHEMA);
        Struct source = new Struct(SOURCE_SCHEMA);
        if (database != null) {
            source.put(AbstractSourceInfo.DATABASE_NAME_KEY, database);
        }
        value.put(Envelope.FieldName.SOURCE, source);
        value.put(HistoryRecord.Fields.DDL_STATEMENTS, ddl);
        value.put(HistoryRecord.Fields.TABLE_CHANGES, Collections.singletonList("change"));
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

    @Test
    void supportRequiresTableOperationsEnabled() {
        SourceRecord record = truncateRecord("shop", "TRUNCATE TABLE products");
        Assertions.assertFalse(resolver(false).support(record));
        Assertions.assertTrue(resolver(true).support(record));
    }

    @Test
    void resolvesQualifiedTruncateForCapturedTable() {
        MySqlSchemaChangeResolver changeResolver = resolver(true);
        TableOperationEvent event =
                changeResolver.resolveTableOperation(
                        truncateRecord("shop", "TRUNCATE TABLE shop.products"),
                        Collections.singletonList(capturedTable("shop", "products")));
        Assertions.assertNotNull(event);
        Assertions.assertEquals(EventType.TABLE_OPERATION_TRUNCATE, event.getEventType());
        Assertions.assertEquals("shop", event.tablePath().getDatabaseName());
        Assertions.assertEquals("products", event.tablePath().getTableName());
    }

    @Test
    void resolvesUnqualifiedTruncateUsingSourceDatabase() {
        MySqlSchemaChangeResolver changeResolver = resolver(true);
        TableOperationEvent event =
                changeResolver.resolveTableOperation(
                        truncateRecord("shop", "TRUNCATE TABLE products"),
                        Collections.singletonList(capturedTable("shop", "products")));
        Assertions.assertNotNull(event);
        Assertions.assertEquals("shop", event.tablePath().getDatabaseName());
        Assertions.assertEquals("products", event.tablePath().getTableName());
    }

    @Test
    void skipsUncapturedTable() {
        MySqlSchemaChangeResolver changeResolver = resolver(true);
        Assertions.assertNull(
                changeResolver.resolveTableOperation(
                        truncateRecord("shop", "TRUNCATE TABLE shop.other"),
                        Collections.singletonList(capturedTable("shop", "products"))));
    }

    @Test
    void disabledResolverReturnsNull() {
        Assertions.assertNull(
                resolver(false)
                        .resolveTableOperation(
                                truncateRecord("shop", "TRUNCATE TABLE products"),
                                Collections.singletonList(capturedTable("shop", "products"))));
    }

    @Test
    void supportDoesNotAdmitAlterWhenOnlyTableOperationsEnabled() {
        SourceRecord alter = alterRecord("shop", "ALTER TABLE products ADD COLUMN c INT");
        SourceRecord truncate = truncateRecord("shop", "TRUNCATE TABLE products");
        MySqlSchemaChangeResolver tableOperationsOnly = resolver(true, false);
        Assertions.assertFalse(tableOperationsOnly.support(alter));
        Assertions.assertTrue(tableOperationsOnly.support(truncate));

        MySqlSchemaChangeResolver schemaChangesEnabled = resolver(false, true);
        Assertions.assertTrue(schemaChangesEnabled.support(alter));
        Assertions.assertFalse(schemaChangesEnabled.support(truncate));
    }

    @Test
    void skipsTruncateWhenEventDatabaseIsMissing() {
        MySqlSchemaChangeResolver changeResolver = resolver(true);
        Assertions.assertNull(
                changeResolver.resolveTableOperation(
                        truncateRecord(null, "TRUNCATE TABLE products"),
                        Collections.singletonList(capturedTable("shop", "products"))));
    }
}
