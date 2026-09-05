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

package org.apache.seatunnel.connectors.cdc.debezium.row;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.CreateTableEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.cdc.debezium.ConnectTableChangeSerializer;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.TableChanges;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Tests dynamic table metadata registration in {@link SeaTunnelRowDebeziumDeserializeSchema}.
 *
 * <p>The covered regression risk is dropping rows for a table whose CREATE TABLE record appears
 * after the reader has entered binlog mode.
 */
public class SeaTunnelRowDebeziumDeserializeSchemaTest {

    /**
     * Verifies that CREATE TABLE schema records do not change produced types when the dynamic
     * binlog-table switch is disabled.
     */
    @Test
    public void testDisabledBinlogNewlyAddedTableDoesNotRegisterCreateTableRecord()
            throws Exception {
        SeaTunnelRowDebeziumDeserializeSchema schema =
                SeaTunnelRowDebeziumDeserializeSchema.builder()
                        .setTables(
                                new ArrayList<>(
                                        Collections.singletonList(
                                                catalogTable("db1", "old_table"))))
                        .setScanBinlogNewlyAddedTableEnabled(false)
                        .setTableChangeCatalogTableConverter(
                                tableChange -> catalogTable(tableChange.getId()))
                        .build();

        schema.deserialize(
                schemaChangeRecord(TableId.parse("db1.new_table")), new EmptyCollector());

        Assertions.assertEquals(1, schema.getProducedType().size());
    }

    /**
     * Verifies that CREATE TABLE schema records append a new table and rebuild row converters when
     * the dynamic binlog-table switch is enabled.
     */
    @Test
    public void testEnabledBinlogNewlyAddedTableRegistersCreateTableRecord() throws Exception {
        EmptyCollector collector = new EmptyCollector();
        SeaTunnelRowDebeziumDeserializeSchema schema =
                SeaTunnelRowDebeziumDeserializeSchema.builder()
                        .setTables(
                                new ArrayList<>(
                                        Collections.singletonList(
                                                catalogTable("db1", "old_table"))))
                        .setScanBinlogNewlyAddedTableEnabled(true)
                        .setTableChangeCatalogTableConverter(
                                tableChange -> catalogTable(tableChange.getId()))
                        .build();

        schema.deserialize(schemaChangeRecord(TableId.parse("db1.new_table")), collector);

        List<CatalogTable> producedType = schema.getProducedType();
        Assertions.assertEquals(2, producedType.size());
        Assertions.assertTrue(
                producedType.stream()
                        .anyMatch(
                                catalogTable ->
                                        TablePath.of("db1", "new_table")
                                                .equals(catalogTable.getTablePath())));
        Assertions.assertEquals(1, collector.getSchemaEvents().size());
        Assertions.assertTrue(collector.getSchemaEvents().get(0) instanceof CreateTableEvent);
    }

    /**
     * Builds a MySQL schema change record that contains the Debezium tableChanges payload.
     *
     * <p>The base deserializer reads this exact payload to update table history and dynamic table
     * metadata.
     */
    private static SourceRecord schemaChangeRecord(TableId tableId) {
        TableChanges tableChanges = new TableChanges();
        tableChanges.create(debeziumTable(tableId));
        List<Struct> tableChangeStructs =
                new ConnectTableChangeSerializer().serialize(tableChanges);
        Schema valueSchema =
                SchemaBuilder.struct()
                        .name("io.debezium.connector.mysql.SchemaChangeValue")
                        .field(HistoryRecord.Fields.DDL_STATEMENTS, Schema.STRING_SCHEMA)
                        .field(
                                HistoryRecord.Fields.TABLE_CHANGES,
                                SchemaBuilder.array(tableChangeStructs.get(0).schema()).build())
                        .build();
        Struct value =
                new Struct(valueSchema)
                        .put(
                                HistoryRecord.Fields.DDL_STATEMENTS,
                                "CREATE TABLE " + tableId.table() + " (id INT)")
                        .put(HistoryRecord.Fields.TABLE_CHANGES, tableChangeStructs);

        return new SourceRecord(
                Collections.emptyMap(),
                Collections.emptyMap(),
                "mysql-schema-changes",
                SchemaBuilder.struct().name("io.debezium.connector.mysql.SchemaChangeKey").build(),
                null,
                valueSchema,
                value);
    }

    /**
     * Builds the minimum Debezium table metadata required by the tableChanges serializer.
     *
     * <p>The table contains one non-null primary key column so row converters can be built.
     */
    private static Table debeziumTable(TableId tableId) {
        return Table.editor()
                .tableId(tableId)
                .addColumns(Column.editor().name("id").jdbcType(Types.INTEGER).type("int").create())
                .setPrimaryKeyNames(Collections.singletonList("id"))
                .create();
    }

    /**
     * Converts one Debezium CREATE TABLE change to a simple SeaTunnel catalog table.
     *
     * <p>The converter mimics connector-specific metadata conversion without depending on MySQL
     * classes from the base module test.
     */
    private static CatalogTable catalogTable(TableChanges.TableChange tableChange) {
        return catalogTable(tableChange.getId());
    }

    /**
     * Converts a Debezium table id to a simple SeaTunnel catalog table.
     *
     * <p>The test only needs database and table names, so schema name is intentionally ignored.
     */
    private static CatalogTable catalogTable(TableId tableId) {
        return catalogTable(tableId.catalog(), tableId.table());
    }

    /**
     * Builds a simple single-primary-key SeaTunnel catalog table for deserializer tests.
     *
     * <p>The table shape matches the Debezium metadata created by {@link #debeziumTable(TableId)}.
     */
    private static CatalogTable catalogTable(String databaseName, String tableName) {
        return CatalogTable.of(
                TableIdentifier.of("test", TablePath.of(databaseName, tableName)),
                TableSchema.builder()
                        .column(
                                PhysicalColumn.builder()
                                        .name("id")
                                        .dataType(BasicType.INT_TYPE)
                                        .nullable(false)
                                        .build())
                        .primaryKey(PrimaryKey.of("pk", Collections.singletonList("id")))
                        .build(),
                Collections.emptyMap(),
                Collections.emptyList(),
                null);
    }

    /**
     * Collector implementation used when the test only needs schema side effects.
     *
     * <p>Schema registration happens before data collection, so this collector intentionally stores
     * no rows.
     */
    private static class EmptyCollector implements Collector<SeaTunnelRow> {
        private final List<SchemaChangeEvent> schemaEvents = new ArrayList<>();

        /**
         * Data records are irrelevant for schema-change side-effect tests.
         *
         * <p>The method intentionally has no side effects.
         */
        @Override
        public void collect(SeaTunnelRow record) {}

        @Override
        public void collect(SchemaChangeEvent event) {
            schemaEvents.add(event);
        }

        /**
         * Returns a stable local checkpoint lock for the collector contract.
         *
         * <p>No concurrent checkpoint coordination is required in this unit test.
         */
        @Override
        public Object getCheckpointLock() {
            return this;
        }

        public List<SchemaChangeEvent> getSchemaEvents() {
            return schemaEvents;
        }
    }
}
