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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.handler.AlterTableSchemaEventHandler;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config.MySqlSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.data.Envelope;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.HistoryRecord;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * MySQL DDL that names an existing column in another case than the table stores it.
 *
 * <p>Each case runs a DDL record through {@link MySqlSchemaChangeResolver} and applies the event
 * with {@link AlterTableSchemaEventHandler}, as the source does. The resulting schema must keep the
 * column names of Debezium's own table model, because the change records that follow the DDL carry
 * those names, and a column the schema names differently gets no value.
 */
class MySqlSchemaChangeResolverColumnCaseTest {

    private static final String DATABASE = "test_db";
    private static final String TABLE = "orders";
    private static final Schema SOURCE_SCHEMA =
            SchemaBuilder.struct()
                    .field(AbstractSourceInfo.DATABASE_NAME_KEY, Schema.STRING_SCHEMA)
                    .field(AbstractSourceInfo.TABLE_NAME_KEY, Schema.STRING_SCHEMA)
                    .build();
    private static final Schema VALUE_SCHEMA =
            SchemaBuilder.struct()
                    .field(Envelope.FieldName.SOURCE, SOURCE_SCHEMA)
                    .field(HistoryRecord.Fields.DDL_STATEMENTS, Schema.OPTIONAL_STRING_SCHEMA)
                    .build();

    /** MySQL column names are case-insensitive, so AFTER may name `name` as `NAME`. */
    @Test
    void addColumnAfterAColumnNamedInAnotherCase() {
        Assertions.assertEquals(
                Arrays.asList("id", "name", "email", "amount"),
                apply("ALTER TABLE orders ADD COLUMN email VARCHAR(64) AFTER NAME"));
    }

    @Test
    void dropColumnNamedInAnotherCase() {
        Assertions.assertEquals(
                Arrays.asList("id", "amount"), apply("ALTER TABLE orders DROP COLUMN Name"));
    }

    @Test
    void modifyColumnNamedInAnotherCase() {
        // Debezium keeps the existing name, here name, and its change records use it
        TableSchema schema = applySchema("ALTER TABLE orders MODIFY COLUMN NAME VARCHAR(128)");
        Assertions.assertEquals(Arrays.asList("id", "name", "amount"), names(schema));
        Assertions.assertEquals("VARCHAR(128)", schema.getColumn("name").getSourceType());
    }

    @Test
    void modifyColumnAfterAColumnNamedInAnotherCase() {
        Assertions.assertEquals(
                Arrays.asList("id", "amount", "name"),
                apply("ALTER TABLE orders MODIFY COLUMN name VARCHAR(64) AFTER AMOUNT"));
    }

    @Test
    void changeColumnNamedInAnotherCase() {
        Assertions.assertEquals(
                Arrays.asList("id", "full_name", "amount"),
                apply("ALTER TABLE orders CHANGE COLUMN NAME full_name VARCHAR(64)"));
    }

    /** A change to the same name in another case keeps the column name, as Debezium does. */
    @Test
    void changeColumnToItsOwnNameInAnotherCase() {
        TableSchema schema = applySchema("ALTER TABLE orders CHANGE COLUMN name NAME VARCHAR(128)");
        Assertions.assertEquals(Arrays.asList("id", "name", "amount"), names(schema));
        Assertions.assertEquals("VARCHAR(128)", schema.getColumn("name").getSourceType());
    }

    /** A later part of the statement may name a column that an earlier part added. */
    @Test
    void addColumnAfterAColumnAddedEarlierInTheStatement() {
        Assertions.assertEquals(
                Arrays.asList("id", "name", "email", "phone", "amount"),
                apply(
                        "ALTER TABLE orders ADD COLUMN email VARCHAR(64) AFTER NAME,"
                                + " ADD COLUMN phone VARCHAR(32) AFTER EMAIL"));
    }

    /** Sinks read the column names from the event, so it must carry the names of the table. */
    @Test
    void eventCarriesTheColumnNamesOfTheTable() {
        AlterTableColumnsEvent event =
                (AlterTableColumnsEvent)
                        resolve(
                                "ALTER TABLE orders ADD COLUMN email VARCHAR(64) AFTER NAME,"
                                        + " DROP COLUMN AMOUNT");
        Assertions.assertEquals(
                "name", ((AlterTableAddColumnEvent) event.getEvents().get(0)).getAfterColumn());
        Assertions.assertEquals(
                "amount", ((AlterTableDropColumnEvent) event.getEvents().get(1)).getColumn());
        Assertions.assertEquals(
                DatabaseIdentifier.MYSQL, event.getEvents().get(0).getSourceDialectName());
    }

    /** A name that matches no column is kept, so the schema handler reports it. */
    @Test
    void columnThatMatchesNoColumnIsKept() {
        AlterTableColumnsEvent event =
                (AlterTableColumnsEvent)
                        resolve("ALTER TABLE orders ADD COLUMN email VARCHAR(64) AFTER missing");
        Assertions.assertEquals(
                "missing", ((AlterTableAddColumnEvent) event.getEvents().get(0)).getAfterColumn());
    }

    /**
     * The change records carry the column names of Debezium's table model, so after each statement
     * the schema must have the same names, in the same order.
     */
    @Test
    void columnNamesMatchTheTableModelOfDebezium() {
        for (String ddl :
                Arrays.asList(
                        "ALTER TABLE orders ADD COLUMN email VARCHAR(64) AFTER NAME",
                        "ALTER TABLE orders DROP COLUMN Name",
                        "ALTER TABLE orders MODIFY COLUMN NAME VARCHAR(128)",
                        "ALTER TABLE orders MODIFY COLUMN name VARCHAR(64) AFTER AMOUNT",
                        "ALTER TABLE orders CHANGE COLUMN NAME full_name VARCHAR(64)",
                        "ALTER TABLE orders CHANGE COLUMN name NAME VARCHAR(128)",
                        "ALTER TABLE orders CHANGE COLUMN Name NAME VARCHAR(128)",
                        "ALTER TABLE orders ADD COLUMN email VARCHAR(64) AFTER NAME,"
                                + " ADD COLUMN phone VARCHAR(32) AFTER EMAIL")) {
            Assertions.assertEquals(debeziumColumnNames(ddl), apply(ddl), ddl);
        }
    }

    private static List<String> debeziumColumnNames(String ddl) {
        MySqlAntlrDdlParser parser = new MySqlAntlrDdlParser();
        Tables tables = new Tables();
        parser.setCurrentDatabase(DATABASE);
        parser.parse(
                "CREATE TABLE orders (id INT NOT NULL, name VARCHAR(64), amount BIGINT)", tables);
        parser.parse(ddl, tables);
        return tables.forTable(new TableId(DATABASE, null, TABLE)).columns().stream()
                .map(io.debezium.relational.Column::name)
                .collect(Collectors.toList());
    }

    private static List<String> apply(String ddl) {
        return names(applySchema(ddl));
    }

    private static List<String> names(TableSchema schema) {
        return schema.getColumns().stream().map(Column::getName).collect(Collectors.toList());
    }

    private static SchemaChangeEvent resolve(String ddl) {
        MySqlSourceConfigFactory factory = new MySqlSourceConfigFactory();
        factory.hostname("localhost");
        factory.username("test");
        factory.password("test");
        MySqlSchemaChangeResolver resolver = new MySqlSchemaChangeResolver(factory);
        SchemaChangeEvent event =
                resolver.resolve(record(ddl), Collections.singletonList(catalogTable()));
        Assertions.assertNotNull(event, ddl);
        return event;
    }

    private static TableSchema applySchema(String ddl) {
        return new AlterTableSchemaEventHandler()
                .reset(catalogTable().getTableSchema())
                .apply(resolve(ddl));
    }

    private static SourceRecord record(String ddl) {
        Struct source =
                new Struct(SOURCE_SCHEMA)
                        .put(AbstractSourceInfo.DATABASE_NAME_KEY, DATABASE)
                        .put(AbstractSourceInfo.TABLE_NAME_KEY, TABLE);
        Struct value =
                new Struct(VALUE_SCHEMA)
                        .put(Envelope.FieldName.SOURCE, source)
                        .put(HistoryRecord.Fields.DDL_STATEMENTS, ddl);
        return new SourceRecord(
                Collections.emptyMap(),
                Collections.emptyMap(),
                "topic",
                null,
                null,
                null,
                VALUE_SCHEMA,
                value);
    }

    private static CatalogTable catalogTable() {
        TableSchema schema =
                TableSchema.builder()
                        .column(PhysicalColumn.of("id", BasicType.INT_TYPE, 0, false, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "name", BasicType.STRING_TYPE, 64, true, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "amount", BasicType.LONG_TYPE, 0, true, null, null))
                        .build();
        return CatalogTable.of(
                TableIdentifier.of("mysql", DATABASE, null, TABLE),
                schema,
                Collections.emptyMap(),
                Collections.emptyList(),
                null);
    }
}
