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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.duckdb;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class DuckDBCatalogTest {

    private static final String DATABASE_NAME = "default";
    private static final String SCHEMA_NAME = "main";
    private static final String TABLE_NAME = "test_Table";
    private static final String TABLE_NAME_COPY = "test_Table_copy";
    private static final String CATALOG_NAME = "duckdb";
    private static final String DB_FILE = "DuckDBCatalogTest.db";

    private static DuckDBCatalog catalog;
    private static String jdbcUrl;

    @BeforeAll
    public static void setUp() throws Exception {
        // Delete existing database file if it exists
        File dbFile = new File(DB_FILE);
        if (dbFile.exists()) {
            dbFile.delete();
        }
        // Setup JDBC connection
        jdbcUrl = "jdbc:duckdb:" + dbFile.getAbsolutePath();
        // Create catalog instance
        JdbcUrlUtil.UrlInfo urlInfo = DuckDBURLParser.parse(jdbcUrl);
        catalog = new DuckDBCatalog(CATALOG_NAME, urlInfo, SCHEMA_NAME);
        catalog.open();
    }

    @AfterAll
    public static void tearDown() {
        // Delete database file
        File dbFile = new File(DB_FILE);
        if (dbFile.exists()) {
            dbFile.delete();
        }
        catalog.close();
    }

    @Test
    @Order(0)
    public void testDatabaseExists() {
        Assertions.assertTrue(catalog.databaseExists(DATABASE_NAME));
        Assertions.assertTrue(catalog.databaseExists("non_existing_db"));
    }

    @Test
    @Order(1)
    public void testCreateTableAndExists() throws Exception {
        TablePath tablePath = getMainTablePath(TABLE_NAME);
        Assertions.assertFalse(catalog.tableExists(tablePath));
        createTestTable(TABLE_NAME);
        Assertions.assertTrue(catalog.tableExists(tablePath));
    }

    @Test
    @Order(2)
    public void testQueryGetCatalogTable() throws Exception {
        CatalogTable catalogTable =
                catalog.getTable(
                        String.format("select * from \"%s\".\"%s\"", SCHEMA_NAME, TABLE_NAME));
        Map<String, Column> columnMap =
                catalogTable.getTableSchema().getColumns().stream()
                        .collect(Collectors.toMap(Column::getName, c -> c));
        Assertions.assertEquals(BasicType.BOOLEAN_TYPE, columnMap.get("c_boolean").getDataType());
        Assertions.assertEquals(BasicType.BYTE_TYPE, columnMap.get("c_tinyint").getDataType());
        Assertions.assertEquals(BasicType.SHORT_TYPE, columnMap.get("c_smallint").getDataType());
        Assertions.assertEquals(BasicType.INT_TYPE, columnMap.get("c_integer").getDataType());
        Assertions.assertEquals(BasicType.LONG_TYPE, columnMap.get("c_bigint").getDataType());
        Assertions.assertEquals(BasicType.FLOAT_TYPE, columnMap.get("c_float").getDataType());
        Assertions.assertEquals(BasicType.DOUBLE_TYPE, columnMap.get("c_double").getDataType());
        Assertions.assertEquals(new DecimalType(10, 2), columnMap.get("c_decimal").getDataType());
        Assertions.assertEquals(BasicType.STRING_TYPE, columnMap.get("c_varchar").getDataType());
        Assertions.assertEquals(
                LocalTimeType.LOCAL_DATE_TYPE, columnMap.get("c_date").getDataType());
        Assertions.assertEquals(
                LocalTimeType.LOCAL_TIME_TYPE, columnMap.get("c_time").getDataType());
        Assertions.assertEquals(
                LocalTimeType.LOCAL_DATE_TIME_TYPE, columnMap.get("c_timestamp").getDataType());
    }

    @Test
    @Order(3)
    public void testGetCatalogTableFromPathAndCreateCopy() {
        TablePath tablePath = getMainTablePath(TABLE_NAME);
        CatalogTable catalogTable = catalog.getTable(tablePath);
        PhysicalColumn nameColumn =
                (PhysicalColumn)
                        catalogTable.getTableSchema().getColumns().stream()
                                .filter(column -> "c_varchar".equals(column.getName()))
                                .findFirst()
                                .get();
        Assertions.assertEquals(0L, nameColumn.getColumnLength());
        Assertions.assertEquals("varchar column", nameColumn.getComment());
        Assertions.assertEquals("'duck'", nameColumn.getDefaultValue());
        PhysicalColumn decimalColumn =
                (PhysicalColumn)
                        catalogTable.getTableSchema().getColumns().stream()
                                .filter(column -> "c_decimal".equals(column.getName()))
                                .findFirst()
                                .get();
        Assertions.assertEquals(38L, decimalColumn.getColumnLength());
        Assertions.assertEquals(2, decimalColumn.getScale());
        TablePath copyPath = getMainTablePath(TABLE_NAME_COPY);
        catalog.createTable(copyPath, catalogTable, true);
        Assertions.assertTrue(catalog.tableExists(copyPath));
    }

    @Test
    @Order(4)
    public void testListTables() {
        List<String> tables = catalog.listTables(DATABASE_NAME);
        Assertions.assertEquals(2, tables.size());
        Assertions.assertTrue(tables.contains(String.format("%s.%s", SCHEMA_NAME, TABLE_NAME)));
        Assertions.assertTrue(
                tables.contains(String.format("%s.%s", SCHEMA_NAME, TABLE_NAME_COPY)));
    }

    @Test
    @Order(5)
    public void testGetTablesWithPattern() {
        Assertions.assertTrue(catalog.tableExists(getMainTablePath(TABLE_NAME)));
        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        Collections.singletonMap(
                                ConnectorCommonOptions.TABLE_PATTERN.key(),
                                ".*test_Table(_copy)?$"));
        List<CatalogTable> catalogTables = catalog.getTables(config);
        List<String> tableNames =
                catalogTables.stream()
                        .map(table -> table.getTableId().toTablePath().getSchemaAndTableName())
                        .collect(Collectors.toList());
        Assertions.assertTrue(tableNames.contains(String.format("%s.%s", SCHEMA_NAME, TABLE_NAME)));
        Assertions.assertTrue(
                tableNames.contains(String.format("%s.%s", SCHEMA_NAME, TABLE_NAME_COPY)));
    }

    @Test
    @Order(6)
    public void testTruncateTable() throws Exception {
        TablePath tablePath = getMainTablePath(TABLE_NAME);
        insertRow();
        Assertions.assertTrue(hasData(tablePath));
        catalog.truncateTable(tablePath, false);
        Assertions.assertFalse(hasData(tablePath));
    }

    @Test
    @Order(7)
    public void testDropTable() throws Exception {
        TablePath tablePath = getMainTablePath(TABLE_NAME);
        TablePath copyPath = getMainTablePath(TABLE_NAME_COPY);
        catalog.dropTable(tablePath, false);
        catalog.dropTable(copyPath, false);
        Assertions.assertFalse(catalog.tableExists(tablePath));
        Assertions.assertFalse(catalog.tableExists(copyPath));
    }

    @Test
    @Order(8)
    public void testSaveModeOperationsWithQuotedIdentifiers() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE_NAME, "mode\"schema", "mode\"table");
        try (Statement statement = catalog.getConnection(jdbcUrl).createStatement()) {
            statement.execute("CREATE SCHEMA \"mode\"\"schema\"");
            statement.execute("CREATE TABLE \"mode\"\"schema\".\"mode\"\"table\" (id INTEGER)");
            try {
                Assertions.assertFalse(catalog.isExistsData(tablePath));
                statement.execute("INSERT INTO \"mode\"\"schema\".\"mode\"\"table\" VALUES (7)");
                Assertions.assertTrue(catalog.isExistsData(tablePath));
                catalog.truncateTable(tablePath, false);
                Assertions.assertTrue(catalog.tableExists(tablePath));
                Assertions.assertFalse(catalog.isExistsData(tablePath));
                catalog.dropTable(tablePath, false);
                Assertions.assertFalse(catalog.tableExists(tablePath));
                catalog.dropTable(tablePath, true);
                Assertions.assertThrows(
                        TableNotExistException.class, () -> catalog.dropTable(tablePath, false));
                catalog.truncateTable(tablePath, true);
                Assertions.assertThrows(
                        TableNotExistException.class,
                        () -> catalog.truncateTable(tablePath, false));
            } finally {
                statement.execute("DROP TABLE IF EXISTS \"mode\"\"schema\".\"mode\"\"table\"");
                statement.execute("DROP SCHEMA \"mode\"\"schema\"");
            }
        }
    }

    @Test
    @Order(9)
    public void testSaveModeOperationsRetainAttachedCatalog(@TempDir Path directory)
            throws Exception {
        TablePath tablePath = TablePath.of("lake\"catalog", "main", "save_mode_target");
        try (Statement statement = catalog.getConnection(jdbcUrl).createStatement()) {
            statement.execute(
                    "ATTACH '"
                            + directory.resolve("attached.db").toString().replace("'", "''")
                            + "' AS \"lake\"\"catalog\"");
            try {
                statement.execute("CREATE TABLE main.save_mode_target (id INTEGER)");
                statement.execute("INSERT INTO main.save_mode_target VALUES (42)");
                statement.execute(
                        "CREATE TABLE \"lake\"\"catalog\".main.save_mode_target (id INTEGER)");
                Assertions.assertFalse(catalog.isExistsData(tablePath));
                statement.execute(
                        "INSERT INTO \"lake\"\"catalog\".main.save_mode_target VALUES (7)");
                Assertions.assertTrue(catalog.isExistsData(tablePath));
                catalog.truncateTable(tablePath, false);
                Assertions.assertFalse(catalog.isExistsData(tablePath));
                catalog.dropTable(tablePath, false);
                try (ResultSet rows =
                        statement.executeQuery("SELECT id FROM main.save_mode_target")) {
                    Assertions.assertTrue(rows.next());
                    Assertions.assertEquals(42, rows.getInt(1));
                    Assertions.assertFalse(rows.next());
                }
                try (ResultSet rows =
                        statement.executeQuery(
                                "SELECT COUNT(*) FROM information_schema.tables WHERE table_catalog = 'lake\"catalog' AND table_name = 'save_mode_target'")) {
                    Assertions.assertTrue(rows.next());
                    Assertions.assertEquals(0, rows.getInt(1));
                }
            } finally {
                statement.execute("DROP TABLE IF EXISTS main.save_mode_target");
                statement.execute("DETACH \"lake\"\"catalog\"");
            }
        }
    }

    private void createTestTable(String tableName) throws Exception {
        Connection connection = catalog.getConnection(jdbcUrl);
        try (Statement statement = connection.createStatement()) {
            statement.execute(getCreateTableSql(tableName));
            statement.execute(
                    String.format("COMMENT ON TABLE %s IS 'table comment'", quoteTable(tableName)));
            statement.execute(
                    String.format(
                            "COMMENT ON COLUMN %s.\"c_varchar\" IS 'varchar column'",
                            quoteTable(tableName)));
        }
    }

    private String getCreateTableSql(String tableName) {
        return String.format(
                "CREATE TABLE %s (\n"
                        + "    id INTEGER PRIMARY KEY,\n"
                        + "    c_boolean BOOLEAN,\n"
                        + "    c_tinyint TINYINT,\n"
                        + "    c_smallint SMALLINT,\n"
                        + "    c_integer INTEGER,\n"
                        + "    c_bigint BIGINT,\n"
                        + "    c_float FLOAT,\n"
                        + "    c_double DOUBLE,\n"
                        + "    c_decimal DECIMAL(10,2),\n"
                        + "    c_varchar VARCHAR(30) DEFAULT 'duck',\n"
                        + "    c_date DATE,\n"
                        + "    c_time TIME,\n"
                        + "    c_timestamp TIMESTAMP\n"
                        + ")",
                quoteTable(tableName));
    }

    private void insertRow() throws Exception {
        Connection connection = catalog.getConnection(jdbcUrl);
        try (Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format(
                            "INSERT INTO %s VALUES "
                                    + "(1, true, 1, 2, 3, 4, 1.1, 2.2, 12345.67,"
                                    + " 'duck', DATE '2024-01-01', TIME '12:00:00',"
                                    + " TIMESTAMP '2024-01-01 12:00:00')",
                            quoteTable(TABLE_NAME)));
        }
    }

    private boolean hasData(TablePath tablePath) throws Exception {
        Connection connection = catalog.getConnection(jdbcUrl);
        try (Statement statement = connection.createStatement();
                ResultSet rs =
                        statement.executeQuery(
                                String.format("SELECT 1 FROM %s LIMIT 1", quoteTable(tablePath)))) {
            return rs.next();
        }
    }

    private TablePath getMainTablePath(String tableName) {
        return TablePath.of(DATABASE_NAME, SCHEMA_NAME, tableName);
    }

    private String quoteTable(TablePath tablePath) {
        return String.format("\"%s\".\"%s\"", tablePath.getSchemaName(), tablePath.getTableName());
    }

    private String quoteTable(String tableName) {
        return String.format("\"%s\".\"%s\"", SCHEMA_NAME, tableName);
    }
}
