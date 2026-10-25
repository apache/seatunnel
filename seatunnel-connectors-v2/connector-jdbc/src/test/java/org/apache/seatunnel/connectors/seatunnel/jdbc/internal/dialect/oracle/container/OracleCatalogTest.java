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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle.container;

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
 * Unit tests for OracleCatalog using Testcontainers. Tests catalog operations like database
 * listing, table operations, and schema management.
 */
@Slf4j
@DisabledOnOs(OS.WINDOWS)
public class OracleCatalogTest extends AbstractOracleContainerTest {

    @Test
    public void testListDatabases() {
        List<String> databases = catalog.listDatabases();
        Assertions.assertNotNull(databases);
        Assertions.assertFalse(databases.isEmpty());
    }

    @Test
    public void testDatabaseExists() {
        List<String> databases = catalog.listDatabases();
        log.info("Available databases: {}", databases);

        // OracleCatalog.databaseExists() always returns true because Oracle uses schemas,
        // not separate databases
        Assertions.assertTrue(catalog.databaseExists(DATABASE));
        Assertions.assertTrue(catalog.databaseExists("FREEPDB1"));
        Assertions.assertTrue(catalog.databaseExists("TOTALLY_FAKE_DB_12345"));
    }

    @Test
    public void testCreateAndGetTable() throws SQLException {
        String testTableName = "TEST_CATALOG_TABLE";
        TablePath tablePath = TablePath.of(DATABASE, SCHEMA, testTableName);

        String createTableSql =
                String.format(
                        "CREATE TABLE %s.%s (ID NUMBER(10) PRIMARY KEY, NAME VARCHAR2(100))",
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
        String testTableName = "TEST_EXISTS_TABLE";
        TablePath tablePath = TablePath.of(DATABASE, SCHEMA, testTableName);

        Assertions.assertFalse(catalog.tableExists(tablePath));

        String createTableSql =
                String.format(
                        "CREATE TABLE %s.%s (ID NUMBER(10))",
                        quoteIdentifier(SCHEMA), quoteIdentifier(testTableName));
        executeSql(createTableSql);

        Assertions.assertTrue(catalog.tableExists(tablePath));

        executeSql(
                String.format(
                        "DROP TABLE %s.%s",
                        quoteIdentifier(SCHEMA), quoteIdentifier(testTableName)));
    }

    @Test
    public void testCreateTableViaAPI() throws SQLException {
        String testTableName = "TEST_API_CREATE_TABLE";
        TablePath tablePath = TablePath.of(DATABASE, SCHEMA, testTableName);

        TableSchema.Builder schemaBuilder = TableSchema.builder();
        schemaBuilder.column(
                PhysicalColumn.of(
                        "ID", BasicType.LONG_TYPE, (Long) null, false, null, "ID column"));
        schemaBuilder.column(
                PhysicalColumn.of(
                        "NAME", BasicType.STRING_TYPE, (Long) null, true, null, "Name column"));
        schemaBuilder.primaryKey(PrimaryKey.of("PK_TEST", Arrays.asList("ID")));

        CatalogTable catalogTable =
                CatalogTable.of(
                        org.apache.seatunnel.api.table.catalog.TableIdentifier.of(
                                "oracle", DATABASE, SCHEMA, testTableName),
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
        String testTableName = "TEST_DROP_TABLE";
        TablePath tablePath = TablePath.of(DATABASE, SCHEMA, testTableName);

        String createTableSql =
                String.format(
                        "CREATE TABLE %s.%s (ID NUMBER(10))",
                        quoteIdentifier(SCHEMA), quoteIdentifier(testTableName));
        executeSql(createTableSql);

        Assertions.assertTrue(catalog.tableExists(tablePath));

        catalog.dropTable(tablePath, false);

        Assertions.assertFalse(catalog.tableExists(tablePath));
    }

    @Test
    public void testListTables() throws SQLException {
        String testTableName1 = "TEST_LIST_TABLE_1";
        String testTableName2 = "TEST_LIST_TABLE_2";

        executeSql(
                String.format(
                        "CREATE TABLE %s.%s (ID NUMBER(10))",
                        quoteIdentifier(SCHEMA), quoteIdentifier(testTableName1)));
        executeSql(
                String.format(
                        "CREATE TABLE %s.%s (ID NUMBER(10))",
                        quoteIdentifier(SCHEMA), quoteIdentifier(testTableName2)));

        List<String> tables = null;
        try {
            tables = catalog.listTables(DATABASE + "." + SCHEMA);
        } catch (Exception e) {
            tables = catalog.listTables(DATABASE);
        }

        Assertions.assertNotNull(tables);
        log.info("Tables found: {}", tables);

        // Tables may be returned with schema prefix (SCHEMA.TABLE_NAME)
        boolean hasTable1 =
                tables.contains(testTableName1) || tables.contains(SCHEMA + "." + testTableName1);
        boolean hasTable2 =
                tables.contains(testTableName2) || tables.contains(SCHEMA + "." + testTableName2);

        Assertions.assertTrue(hasTable1, "Expected " + testTableName1 + " in " + tables);
        Assertions.assertTrue(hasTable2, "Expected " + testTableName2 + " in " + tables);

        executeSql(
                String.format(
                        "DROP TABLE %s.%s",
                        quoteIdentifier(SCHEMA), quoteIdentifier(testTableName1)));
        executeSql(
                String.format(
                        "DROP TABLE %s.%s",
                        quoteIdentifier(SCHEMA), quoteIdentifier(testTableName2)));
    }

    @Test
    public void testGetTableWithComplexTypes() throws SQLException {
        String testTableName = "TEST_COMPLEX_TYPES";
        TablePath tablePath = TablePath.of(DATABASE, SCHEMA, testTableName);

        String createTableSql =
                String.format(
                        "CREATE TABLE %s.%s ("
                                + "ID NUMBER(10) PRIMARY KEY, "
                                + "VARCHAR_COL VARCHAR2(100), "
                                + "CHAR_COL CHAR(10), "
                                + "NUMBER_COL NUMBER(10,2), "
                                + "DATE_COL DATE, "
                                + "TIMESTAMP_COL TIMESTAMP, "
                                + "CLOB_COL CLOB, "
                                + "BLOB_COL BLOB"
                                + ")",
                        quoteIdentifier(SCHEMA), quoteIdentifier(testTableName));
        executeSql(createTableSql);

        CatalogTable table = catalog.getTable(tablePath);
        Assertions.assertNotNull(table);

        TableSchema schema = table.getTableSchema();
        List<Column> columns = schema.getColumns();
        Assertions.assertTrue(columns.size() >= 8, "Should have at least 8 columns");

        executeSql(
                String.format(
                        "DROP TABLE %s.%s",
                        quoteIdentifier(SCHEMA), quoteIdentifier(testTableName)));
    }

    @Test
    public void testTableWithPrimaryKey() throws SQLException {
        String testTableName = "TEST_PRIMARY_KEY_TABLE";
        TablePath tablePath = TablePath.of(DATABASE, SCHEMA, testTableName);

        String createTableSql =
                String.format(
                        "CREATE TABLE %s.%s (ID NUMBER(10) PRIMARY KEY, NAME VARCHAR2(100))",
                        quoteIdentifier(SCHEMA), quoteIdentifier(testTableName));
        executeSql(createTableSql);

        CatalogTable table = catalog.getTable(tablePath);
        Assertions.assertNotNull(table);

        TableSchema schema = table.getTableSchema();
        Assertions.assertNotNull(schema.getPrimaryKey());
        Assertions.assertEquals("ID", schema.getPrimaryKey().getColumnNames().get(0));

        executeSql(
                String.format(
                        "DROP TABLE %s.%s",
                        quoteIdentifier(SCHEMA), quoteIdentifier(testTableName)));
    }
}
