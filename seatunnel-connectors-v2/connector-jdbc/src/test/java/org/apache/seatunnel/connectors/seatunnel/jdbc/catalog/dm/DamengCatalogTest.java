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
import org.apache.seatunnel.common.utils.JdbcUrlUtil;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link DamengCatalog}. Exercises the catalog directly (table listing, DDL
 * generation) against a {@link MockDamengDriver} registered as a real {@link Driver}, so the
 * assertions run through the actual JDBC-facing code path rather than a test-only subclass. No
 * running database required.
 */
public class DamengCatalogTest {

    private static final String TEST_URL = "jdbc:dm://localhost:5236/DAMENG";
    private static final MockDamengDriver MOCK_DRIVER = new MockDamengDriver();
    private DamengCatalog catalog;

    @BeforeAll
    static void setUpDriver() throws SQLException {
        DriverManager.registerDriver(MOCK_DRIVER);
    }

    @AfterAll
    static void tearDownDriver() throws SQLException {
        DriverManager.deregisterDriver(MOCK_DRIVER);
    }

    @BeforeEach
    void setUp() {
        JdbcUrlUtil.UrlInfo urlInfo = JdbcUrlUtil.getUrlInfo(TEST_URL);
        catalog =
                new DamengCatalog(
                        "dameng_test",
                        "SYSDBA",
                        "SYSDBA",
                        urlInfo,
                        null,
                        MockDamengDriver.class.getName());
    }

    @AfterEach
    void tearDown() {
        catalog.close();
        MOCK_DRIVER.setConnection(null);
    }

    @Test
    void testGetListDatabaseSql() {
        Assertions.assertEquals("SELECT name FROM v$database", catalog.getListDatabaseSql());
    }

    @Test
    void testGetListTableSql() {
        String sql = catalog.getListTableSql("DAMENG");
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
        MOCK_DRIVER.setConnection(connection);

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
        MOCK_DRIVER.setConnection(connection);

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
        MOCK_DRIVER.setConnection(connection);

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
        String sql = catalog.getDropTableSql(tablePath);
        Assertions.assertEquals("DROP TABLE \"SYSDBA\".\"users\"", sql);
    }

    @Test
    void testGetTableName() {
        TablePath tablePath = TablePath.of("DAMENG", "SYSDBA", "users");
        String name = catalog.getTableName(tablePath);
        Assertions.assertEquals("\"SYSDBA\".\"users\"", name);
    }

    @Test
    void testGetTruncateTableSql() {
        TablePath tablePath = TablePath.of("DAMENG", "SYSDBA", "users");
        String sql = catalog.getTruncateTableSql(tablePath);
        Assertions.assertEquals("TRUNCATE TABLE \"SYSDBA\".\"users\"", sql);
    }

    @Test
    void testGetSelectColumnsSql() {
        TablePath tablePath = TablePath.of("DAMENG", "SYSDBA", "users");
        String sql = catalog.getSelectColumnsSql(tablePath);
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
        String name = catalog.getOptionTableName(tablePath);
        Assertions.assertEquals("SYSDBA.users", name);
    }

    @Test
    void testGetUrlFromDatabaseName() {
        String url = catalog.getUrlFromDatabaseName("ANY_DB");
        // DamengCatalog always returns defaultUrl regardless of databaseName
        Assertions.assertEquals(TEST_URL, url);
    }

    @Test
    void testGetDatabaseWithConditionSql() {
        String sql = catalog.getDatabaseWithConditionSql("DAMENG");
        Assertions.assertEquals("SELECT name FROM v$database where name = 'DAMENG'", sql);
    }

    @Test
    void testGetTableWithConditionSql() {
        TablePath tablePath = TablePath.of("DAMENG", "SYSDBA", "users");
        String sql = catalog.getTableWithConditionSql(tablePath);
        Assertions.assertEquals(
                "SELECT OWNER, TABLE_NAME FROM ALL_TABLES"
                        + " where OWNER = 'SYSDBA' and TABLE_NAME = 'users'",
                sql);
    }

    @Test
    void testCreateDatabaseInternalThrowsUnsupported() {
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> catalog.createDatabaseInternal("test_db"));
    }

    @Test
    void testDropDatabaseInternalThrowsUnsupported() {
        Assertions.assertThrows(
                UnsupportedOperationException.class, () -> catalog.dropDatabaseInternal("test_db"));
    }

    @Test
    void testSqlGenerationWithSpacesInIdentifiers() {
        TablePath tablePath = TablePath.of("DAMENG", "MY SCHEMA", "my table");
        // getTableName wraps with double quotes, spaces should be preserved inside quotes
        Assertions.assertEquals("\"MY SCHEMA\".\"my table\"", catalog.getTableName(tablePath));
        Assertions.assertEquals(
                "DROP TABLE \"MY SCHEMA\".\"my table\"", catalog.getDropTableSql(tablePath));
        Assertions.assertEquals(
                "TRUNCATE TABLE \"MY SCHEMA\".\"my table\"",
                catalog.getTruncateTableSql(tablePath));
        // getOptionTableName returns unquoted form
        Assertions.assertEquals("MY SCHEMA.my table", catalog.getOptionTableName(tablePath));
    }

    @Test
    void testSqlGenerationWithReservedWordIdentifiers() {
        TablePath tablePath = TablePath.of("DAMENG", "SELECT", "TABLE");
        // SQL reserved words used as identifiers should be quoted properly
        Assertions.assertEquals("\"SELECT\".\"TABLE\"", catalog.getTableName(tablePath));
        String existSql = catalog.getExistDataSql(tablePath);
        Assertions.assertEquals("select * from \"SELECT\".\"TABLE\" LIMIT 1", existSql);
        String conditionSql = catalog.getTableWithConditionSql(tablePath);
        Assertions.assertEquals(
                "SELECT OWNER, TABLE_NAME FROM ALL_TABLES"
                        + " where OWNER = 'SELECT' and TABLE_NAME = 'TABLE'",
                conditionSql);
        String columnsSql = catalog.getSelectColumnsSql(tablePath);
        Assertions.assertTrue(columnsSql.contains("WHERE COLUMNS.OWNER = 'SELECT' "));
        Assertions.assertTrue(columnsSql.contains("AND COLUMNS.TABLE_NAME = 'TABLE' "));
    }

    private static final class MockDamengDriver implements Driver {
        private final AtomicReference<Connection> connectionRef = new AtomicReference<>();

        void setConnection(Connection connection) {
            connectionRef.set(connection);
        }

        @Override
        public Connection connect(String url, Properties info) throws SQLException {
            if (!acceptsURL(url)) {
                return null;
            }
            Connection connection = connectionRef.get();
            if (connection == null) {
                throw new SQLException("No mock connection configured for url: " + url);
            }
            return connection;
        }

        @Override
        public boolean acceptsURL(String url) {
            return TEST_URL.equals(url);
        }

        @Override
        public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
            return new DriverPropertyInfo[0];
        }

        @Override
        public int getMajorVersion() {
            return 1;
        }

        @Override
        public int getMinorVersion() {
            return 0;
        }

        @Override
        public boolean jdbcCompliant() {
            return false;
        }

        @Override
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            throw new SQLFeatureNotSupportedException("Parent logger is not supported");
        }
    }
}
