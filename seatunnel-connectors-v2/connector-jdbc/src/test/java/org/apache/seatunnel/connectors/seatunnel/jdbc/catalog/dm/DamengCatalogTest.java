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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.dm;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link DamengCatalog}. Tests SQL generation methods without requiring a real
 * database connection.
 */
public class DamengCatalogTest {

    private static final String TEST_URL = "jdbc:dm://localhost:5236/DAMENG";
    private static DamengCatalog catalog;

    @BeforeAll
    static void setUp() {
        JdbcUrlUtil.UrlInfo urlInfo = JdbcUrlUtil.getUrlInfo(TEST_URL);
        catalog =
                new TestDamengCatalog(
                        "dameng_test",
                        "SYSDBA",
                        "SYSDBA",
                        urlInfo,
                        null,
                        "dm.jdbc.driver.DmDriver");
    }

    @Test
    void testGetListDatabaseSql() {
        Assertions.assertEquals("SELECT name FROM v$database", catalog.exposedGetListDatabaseSql());
    }

    @Test
    void testGetListTableSql() {
        String sql = catalog.exposedGetListTableSql("DAMENG");
        Assertions.assertEquals("SELECT OWNER, TABLE_NAME FROM ALL_TABLES", sql);
    }

    @Test
    void testListDatabasesExecutesCatalogMethod() throws SQLException {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        when(connection.prepareStatement("SELECT name FROM v$database")).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, false);
        when(resultSet.getString(1)).thenReturn("DAMENG");
        catalog.setConnection(connection);

        List<String> databases = catalog.listDatabases();

        Assertions.assertEquals(1, databases.size());
        Assertions.assertEquals("DAMENG", databases.get(0));
    }

    @Test
    void testListTablesExecutesCatalogMethod() throws SQLException {
        Connection connection = mock(Connection.class);
        PreparedStatement databaseExistsStatement = mock(PreparedStatement.class);
        PreparedStatement listTablesStatement = mock(PreparedStatement.class);
        ResultSet databaseExistsResultSet = mock(ResultSet.class);
        ResultSet listTablesResultSet = mock(ResultSet.class);
        when(connection.prepareStatement("SELECT name FROM v$database where name = 'DAMENG'"))
                .thenReturn(databaseExistsStatement);
        when(connection.prepareStatement("SELECT OWNER, TABLE_NAME FROM ALL_TABLES"))
                .thenReturn(listTablesStatement);
        when(databaseExistsStatement.executeQuery()).thenReturn(databaseExistsResultSet);
        when(listTablesStatement.executeQuery()).thenReturn(listTablesResultSet);
        when(databaseExistsResultSet.next()).thenReturn(true, false);
        when(listTablesResultSet.next()).thenReturn(true, true, false);
        when(listTablesResultSet.getString(1)).thenReturn("SYSDBA", "SYSDBA");
        when(listTablesResultSet.getString(2)).thenReturn("users", "orders");
        catalog.setConnection(connection);

        List<String> tables = catalog.listTables("DAMENG");

        Assertions.assertEquals(2, tables.size());
        Assertions.assertEquals("SYSDBA.users", tables.get(0));
        Assertions.assertEquals("SYSDBA.orders", tables.get(1));
    }

    @Test
    void testTableExistsExecutesCatalogMethod() throws SQLException {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        TablePath tablePath = TablePath.of("DAMENG", "SYSDBA", "users");
        when(connection.prepareStatement(
                        "SELECT OWNER, TABLE_NAME FROM ALL_TABLES"
                                + " where OWNER = 'SYSDBA' and TABLE_NAME = 'users'"))
                .thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, false);
        catalog.setConnection(connection);

        Assertions.assertTrue(catalog.tableExists(tablePath));
    }

    @Test
    void testGetExistDataSql() {
        TablePath tablePath = TablePath.of("DAMENG", "SYSDBA", "users");
        String sql = catalog.getExistDataSql(tablePath);
        Assertions.assertEquals("select * from \"SYSDBA\".\"users\" LIMIT 1", sql);
    }

    @Test
    void testGetDropTableSql() {
        TablePath tablePath = TablePath.of("DAMENG", "SYSDBA", "users");
        String sql = catalog.exposedGetDropTableSql(tablePath);
        Assertions.assertEquals("DROP TABLE \"SYSDBA\".\"users\"", sql);
    }

    @Test
    void testGetTableName() {
        TablePath tablePath = TablePath.of("DAMENG", "SYSDBA", "users");
        String name = catalog.exposedGetTableName(tablePath);
        Assertions.assertEquals("\"SYSDBA\".\"users\"", name);
    }

    @Test
    void testGetTruncateTableSql() {
        TablePath tablePath = TablePath.of("DAMENG", "SYSDBA", "users");
        String sql = catalog.exposedGetTruncateTableSql(tablePath);
        Assertions.assertEquals("TRUNCATE TABLE \"SYSDBA\".\"users\"", sql);
    }

    @Test
    void testGetSelectColumnsSql() {
        TablePath tablePath = TablePath.of("DAMENG", "SYSDBA", "users");
        String sql = catalog.exposedGetSelectColumnsSql(tablePath);
        // Verify key structural parts: SELECT columns, FROM/JOIN, WHERE filter, ORDER BY
        Assertions.assertTrue(sql.startsWith("SELECT COLUMNS.COLUMN_NAME"));
        Assertions.assertTrue(sql.contains("FROM ALL_TAB_COLUMNS COLUMNS "));
        Assertions.assertTrue(sql.contains("LEFT JOIN ALL_COL_COMMENTS COMMENTS "));
        Assertions.assertTrue(sql.contains("ON COLUMNS.OWNER = COMMENTS.SCHEMA_NAME "));
        Assertions.assertTrue(sql.contains("WHERE COLUMNS.OWNER = 'SYSDBA' "));
        Assertions.assertTrue(sql.contains("AND COLUMNS.TABLE_NAME = 'users' "));
        Assertions.assertTrue(sql.endsWith("ORDER BY COLUMNS.COLUMN_ID ASC"));
    }

    @Test
    void testGetOptionTableName() {
        TablePath tablePath = TablePath.of("DAMENG", "SYSDBA", "users");
        String name = catalog.exposedGetOptionTableName(tablePath);
        Assertions.assertEquals("SYSDBA.users", name);
    }

    @Test
    void testGetUrlFromDatabaseName() {
        String url = catalog.exposedGetUrlFromDatabaseName("ANY_DB");
        // DamengCatalog always returns defaultUrl regardless of databaseName
        Assertions.assertEquals(TEST_URL, url);
    }

    @Test
    void testGetDatabaseWithConditionSql() {
        String sql = catalog.exposedGetDatabaseWithConditionSql("DAMENG");
        Assertions.assertEquals("SELECT name FROM v$database where name = 'DAMENG'", sql);
    }

    @Test
    void testGetTableWithConditionSql() {
        TablePath tablePath = TablePath.of("DAMENG", "SYSDBA", "users");
        String sql = catalog.exposedGetTableWithConditionSql(tablePath);
        Assertions.assertEquals(
                "SELECT OWNER, TABLE_NAME FROM ALL_TABLES"
                        + " where OWNER = 'SYSDBA' and TABLE_NAME = 'users'",
                sql);
    }

    @Test
    void testCreateDatabaseInternalThrowsUnsupported() {
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> catalog.exposedCreateDatabaseInternal("test_db"));
    }

    @Test
    void testDropDatabaseInternalThrowsUnsupported() {
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> catalog.exposedDropDatabaseInternal("test_db"));
    }

    @Test
    void testSqlGenerationWithSpacesInIdentifiers() {
        TablePath tablePath = TablePath.of("DAMENG", "MY SCHEMA", "my table");
        // getTableName wraps with double quotes, spaces should be preserved inside quotes
        Assertions.assertEquals(
                "\"MY SCHEMA\".\"my table\"", catalog.exposedGetTableName(tablePath));
        Assertions.assertEquals(
                "DROP TABLE \"MY SCHEMA\".\"my table\"", catalog.exposedGetDropTableSql(tablePath));
        Assertions.assertEquals(
                "TRUNCATE TABLE \"MY SCHEMA\".\"my table\"",
                catalog.exposedGetTruncateTableSql(tablePath));
        // getOptionTableName returns unquoted form
        Assertions.assertEquals("MY SCHEMA.my table", catalog.exposedGetOptionTableName(tablePath));
    }

    @Test
    void testSqlGenerationWithReservedWordIdentifiers() {
        TablePath tablePath = TablePath.of("DAMENG", "SELECT", "TABLE");
        // SQL reserved words used as identifiers should be quoted properly
        Assertions.assertEquals("\"SELECT\".\"TABLE\"", catalog.exposedGetTableName(tablePath));
        String existSql = catalog.getExistDataSql(tablePath);
        Assertions.assertEquals("select * from \"SELECT\".\"TABLE\" LIMIT 1", existSql);
        String conditionSql = catalog.exposedGetTableWithConditionSql(tablePath);
        Assertions.assertEquals(
                "SELECT OWNER, TABLE_NAME FROM ALL_TABLES"
                        + " where OWNER = 'SELECT' and TABLE_NAME = 'TABLE'",
                conditionSql);
        String columnsSql = catalog.exposedGetSelectColumnsSql(tablePath);
        Assertions.assertTrue(columnsSql.contains("WHERE COLUMNS.OWNER = 'SELECT' "));
        Assertions.assertTrue(columnsSql.contains("AND COLUMNS.TABLE_NAME = 'TABLE' "));
    }

    /**
     * Test subclass that exposes protected methods of {@link DamengCatalog} for unit testing
     * without requiring a database connection.
     */
    static class TestDamengCatalog extends DamengCatalog {
        private Connection connection;

        TestDamengCatalog(
                String catalogName,
                String username,
                String pwd,
                JdbcUrlUtil.UrlInfo urlInfo,
                String defaultSchema,
                String driverClass) {
            super(catalogName, username, pwd, urlInfo, defaultSchema, driverClass);
        }

        @Override
        protected Connection getConnection(String url) {
            if (connection != null) {
                return connection;
            }
            return super.getConnection(url);
        }

        void setConnection(Connection connection) {
            this.connection = connection;
        }

        String exposedGetListDatabaseSql() {
            return getListDatabaseSql();
        }

        String exposedGetListTableSql(String databaseName) {
            return getListTableSql(databaseName);
        }

        String exposedGetDropTableSql(TablePath tablePath) {
            return getDropTableSql(tablePath);
        }

        String exposedGetTableName(TablePath tablePath) {
            return getTableName(tablePath);
        }

        String exposedGetTruncateTableSql(TablePath tablePath) {
            return getTruncateTableSql(tablePath);
        }

        String exposedGetSelectColumnsSql(TablePath tablePath) {
            return getSelectColumnsSql(tablePath);
        }

        String exposedGetOptionTableName(TablePath tablePath) {
            return getOptionTableName(tablePath);
        }

        String exposedGetUrlFromDatabaseName(String databaseName) {
            return getUrlFromDatabaseName(databaseName);
        }

        String exposedGetDatabaseWithConditionSql(String databaseName) {
            return getDatabaseWithConditionSql(databaseName);
        }

        String exposedGetTableWithConditionSql(TablePath tablePath) {
            return getTableWithConditionSql(tablePath);
        }

        void exposedCreateDatabaseInternal(String databaseName) {
            createDatabaseInternal(databaseName);
        }

        void exposedDropDatabaseInternal(String databaseName) throws CatalogException {
            dropDatabaseInternal(databaseName);
        }
    }
}
