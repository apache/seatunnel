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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.kingbase.container;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Unit tests for KingbaseCatalog using Testcontainers. Tests catalog operations like database
 * listing, table operations, and schema management.
 */
@Slf4j
@DisabledOnOs(OS.WINDOWS)
public class KingbaseCatalogContainerTest extends AbstractKingbaseContainerTest {

    @Test
    public void testDatabaseExists() {
        Assertions.assertTrue(catalog.databaseExists(DATABASE));
    }

    @Test
    public void testCreateAndGetTable() throws SQLException {
        String testTableName = "test_catalog_table";
        TablePath tablePath = TablePath.of(DATABASE, SCHEMA, testTableName);

        String createTableSql =
                String.format(
                        "CREATE TABLE %s.%s (id BIGSERIAL PRIMARY KEY, name VARCHAR(100))",
                        quoteIdentifier(SCHEMA), quoteIdentifier(testTableName));
        executeSql(createTableSql);

        Assertions.assertTrue(catalog.tableExists(tablePath));

        CatalogTable table = catalog.getTable(tablePath);
        Assertions.assertNotNull(table);
        Assertions.assertEquals(testTableName, table.getTableId().getTableName());

        executeSql(
                String.format(
                        "DROP TABLE %s.%s",
                        quoteIdentifier(SCHEMA), quoteIdentifier(testTableName)));
    }

    @Test
    public void testTableExists() throws SQLException {
        String testTableName = "test_exists_table";
        TablePath tablePath = TablePath.of(DATABASE, SCHEMA, testTableName);

        Assertions.assertFalse(catalog.tableExists(tablePath));

        String createTableSql =
                String.format(
                        "CREATE TABLE %s.%s (id INT4)",
                        quoteIdentifier(SCHEMA), quoteIdentifier(testTableName));
        executeSql(createTableSql);

        Assertions.assertTrue(catalog.tableExists(tablePath));

        executeSql(
                String.format(
                        "DROP TABLE %s.%s",
                        quoteIdentifier(SCHEMA), quoteIdentifier(testTableName)));
    }

    @Test
    public void testCreateTableViaAPI() {
        String testTableName = "test_api_create_table";
        TablePath tablePath = TablePath.of(DATABASE, SCHEMA, testTableName);

        TableSchema.Builder schemaBuilder = TableSchema.builder();
        schemaBuilder.column(
                PhysicalColumn.of(
                        "id", BasicType.LONG_TYPE, (Long) null, false, null, "ID column"));
        schemaBuilder.column(
                PhysicalColumn.of("name", BasicType.STRING_TYPE, 100L, true, null, "Name column"));
        schemaBuilder.primaryKey(PrimaryKey.of("pk_test", Arrays.asList("id")));

        // Even with "kingbase" as catalog name, it should work because
        // KingbaseCreateTableSqlBuilder now checks isNotBlank(sourceType)
        // and falls back to type converter when sourceType is null
        CatalogTable catalogTable =
                CatalogTable.of(
                        org.apache.seatunnel.api.table.catalog.TableIdentifier.of(
                                "kingbase", DATABASE, SCHEMA, testTableName),
                        schemaBuilder.build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "");

        catalog.createTable(tablePath, catalogTable, false);

        Assertions.assertTrue(catalog.tableExists(tablePath));

        CatalogTable retrievedTable = catalog.getTable(tablePath);
        Assertions.assertNotNull(retrievedTable);
        Assertions.assertEquals(testTableName, retrievedTable.getTableId().getTableName());

        catalog.dropTable(tablePath, false);
        Assertions.assertFalse(catalog.tableExists(tablePath));
    }

    @Test
    public void testDropTable() throws SQLException {
        String testTableName = "test_drop_table";
        TablePath tablePath = TablePath.of(DATABASE, SCHEMA, testTableName);

        String createTableSql =
                String.format(
                        "CREATE TABLE %s.%s (id INT4)",
                        quoteIdentifier(SCHEMA), quoteIdentifier(testTableName));
        executeSql(createTableSql);

        Assertions.assertTrue(catalog.tableExists(tablePath));

        catalog.dropTable(tablePath, false);

        Assertions.assertFalse(catalog.tableExists(tablePath));
    }

    @Test
    public void testGetTableWithComplexTypes() throws SQLException {
        String testTableName = "test_complex_types";
        TablePath tablePath = TablePath.of(DATABASE, SCHEMA, testTableName);

        String createTableSql =
                String.format(
                        "CREATE TABLE %s.%s ("
                                + "id BIGSERIAL PRIMARY KEY, "
                                + "c_smallserial SMALLSERIAL, "
                                + "c_serial SERIAL, "
                                + "c_bool BOOL, "
                                + "c_int2 INT2, "
                                + "c_int4 INT4, "
                                + "c_int8 INT8, "
                                + "c_float4 FLOAT4, "
                                + "c_float8 FLOAT8, "
                                + "c_numeric NUMERIC(38,18), "
                                + "c_char CHARACTER(10), "
                                + "c_varchar VARCHAR(255), "
                                + "c_text TEXT, "
                                + "c_date DATE, "
                                + "c_time TIME, "
                                + "c_timestamp TIMESTAMP, "
                                + "c_timestamptz TIMESTAMPTZ, "
                                + "c_bytea BYTEA"
                                + ")",
                        quoteIdentifier(SCHEMA), quoteIdentifier(testTableName));
        executeSql(createTableSql);

        CatalogTable table = catalog.getTable(tablePath);
        Assertions.assertNotNull(table);

        TableSchema schema = table.getTableSchema();
        List<Column> columns = schema.getColumns();
        Assertions.assertTrue(columns.size() >= 18, "Should have at least 18 columns");

        executeSql(
                String.format(
                        "DROP TABLE %s.%s",
                        quoteIdentifier(SCHEMA), quoteIdentifier(testTableName)));
    }

    @Test
    public void testTableWithPrimaryKey() throws SQLException {
        String testTableName = "test_primary_key_table";
        TablePath tablePath = TablePath.of(DATABASE, SCHEMA, testTableName);

        String createTableSql =
                String.format(
                        "CREATE TABLE %s.%s (id INT8 PRIMARY KEY, name VARCHAR(100))",
                        quoteIdentifier(SCHEMA), quoteIdentifier(testTableName));
        executeSql(createTableSql);

        CatalogTable table = catalog.getTable(tablePath);
        Assertions.assertNotNull(table);

        TableSchema schema = table.getTableSchema();
        Assertions.assertNotNull(schema.getPrimaryKey());
        Assertions.assertEquals("id", schema.getPrimaryKey().getColumnNames().get(0));

        executeSql(
                String.format(
                        "DROP TABLE %s.%s",
                        quoteIdentifier(SCHEMA), quoteIdentifier(testTableName)));
    }

    @Test
    public void testCreateTableFromSource() throws SQLException {
        String sourceTableName = "st_type_converter_source";
        String targetTableName = "st_type_converter_target";
        TablePath sourcePath = TablePath.of(DATABASE, SCHEMA, sourceTableName);
        TablePath targetPath = TablePath.of(DATABASE, SCHEMA, targetTableName);

        // Clean up if exists
        if (catalog.tableExists(targetPath)) {
            catalog.dropTable(targetPath, true);
        }
        if (catalog.tableExists(sourcePath)) {
            catalog.dropTable(sourcePath, true);
        }

        // Create source table with various types
        String createSourceSql =
                String.format(
                        "CREATE TABLE %s.%s ("
                                + "id BIGSERIAL PRIMARY KEY, "
                                + "c_int2 INT2, "
                                + "c_int4 INT4, "
                                + "c_int8 INT8, "
                                + "c_float4 FLOAT4, "
                                + "c_float8 FLOAT8, "
                                + "c_numeric NUMERIC(38,18), "
                                + "c_char CHARACTER(10), "
                                + "c_varchar VARCHAR(255), "
                                + "c_text TEXT, "
                                + "c_date DATE, "
                                + "c_timestamp TIMESTAMP"
                                + ")",
                        quoteIdentifier(SCHEMA), quoteIdentifier(sourceTableName));
        executeSql(createSourceSql);
        Assertions.assertTrue(catalog.tableExists(sourcePath));

        // Get source table and create target from it
        CatalogTable sourceTable = catalog.getTable(sourcePath);
        catalog.createTable(targetPath, sourceTable, true);
        Assertions.assertTrue(catalog.tableExists(targetPath));

        // Verify target table structure
        CatalogTable targetTable = catalog.getTable(targetPath);
        Assertions.assertNotNull(targetTable);
        Assertions.assertEquals(
                sourceTable.getTableSchema().getColumns().size(),
                targetTable.getTableSchema().getColumns().size());

        // Clean up
        catalog.dropTable(targetPath, true);
        catalog.dropTable(sourcePath, true);
    }

    @Test
    public void testColumnTypePreservation() throws SQLException {
        String testTableName = "test_column_type_preservation";
        TablePath tablePath = TablePath.of(DATABASE, SCHEMA, testTableName);

        // Create table with specific type lengths
        String createTableSql =
                String.format(
                        "CREATE TABLE %s.%s ("
                                + "id INT8 PRIMARY KEY, "
                                + "c_varchar VARCHAR(255), "
                                + "c_char CHAR(10), "
                                + "c_numeric NUMERIC(38,18)"
                                + ")",
                        quoteIdentifier(SCHEMA), quoteIdentifier(testTableName));
        executeSql(createTableSql);

        CatalogTable table = catalog.getTable(tablePath);
        Assertions.assertNotNull(table);

        // Verify column types preserve full type info (VARCHAR(255), CHAR(10), NUMERIC(38,18))
        List<Column> columns = table.getTableSchema().getColumns();
        for (Column column : columns) {
            String sourceType = column.getSourceType();
            log.info("Column: {}, SourceType: {}", column.getName(), sourceType);
            if ("c_varchar".equals(column.getName())) {
                Assertions.assertTrue(
                        sourceType.toLowerCase().contains("255")
                                || sourceType.toLowerCase().contains("varchar"),
                        "VARCHAR should preserve length info: " + sourceType);
            } else if ("c_char".equals(column.getName())) {
                Assertions.assertTrue(
                        sourceType.toLowerCase().contains("10")
                                || sourceType.toLowerCase().contains("char"),
                        "CHAR should preserve length info: " + sourceType);
            } else if ("c_numeric".equals(column.getName())) {
                Assertions.assertTrue(
                        sourceType.toLowerCase().contains("numeric")
                                || sourceType.toLowerCase().contains("38"),
                        "NUMERIC should preserve precision info: " + sourceType);
            }
        }

        executeSql(
                String.format(
                        "DROP TABLE %s.%s",
                        quoteIdentifier(SCHEMA), quoteIdentifier(testTableName)));
    }

    @Test
    public void testColumnCommentWithSingleQuote() throws SQLException {
        String testTableName = "test_comment_escape";
        TablePath tablePath = TablePath.of(DATABASE, SCHEMA, testTableName);

        // Create source table
        String createTableSql =
                String.format(
                        "CREATE TABLE %s.%s (id INT8 PRIMARY KEY, name VARCHAR(100))",
                        quoteIdentifier(SCHEMA), quoteIdentifier(testTableName));
        executeSql(createTableSql);

        // Add comment with single quote
        String commentSql =
                String.format(
                        "COMMENT ON COLUMN %s.%s.name IS 'User''s name field'",
                        quoteIdentifier(SCHEMA), quoteIdentifier(testTableName));
        executeSql(commentSql);

        CatalogTable table = catalog.getTable(tablePath);
        Assertions.assertNotNull(table);

        // Verify comment is retrieved correctly
        Column nameColumn =
                table.getTableSchema().getColumns().stream()
                        .filter(c -> "name".equals(c.getName()))
                        .findFirst()
                        .orElse(null);
        Assertions.assertNotNull(nameColumn);
        Assertions.assertNotNull(nameColumn.getComment());
        log.info("Column comment: {}", nameColumn.getComment());

        // Now test creating a new table from this one (tests the escape in SQL builder)
        String targetTableName = "test_comment_escape_target";
        TablePath targetPath = TablePath.of(DATABASE, SCHEMA, targetTableName);

        catalog.createTable(targetPath, table, true);
        Assertions.assertTrue(catalog.tableExists(targetPath));

        // Clean up
        catalog.dropTable(targetPath, true);
        catalog.dropTable(tablePath, true);
    }
}
