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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AbstractJdbcCatalogTest {

    /** Database configured in the catalog URL. */
    private static final String CATALOG_DATABASE = "db_a";

    private static final String CATALOG_DATABASE_URL = "jdbc:testdb://localhost:5432/db_a";

    /** Database referenced by the table path, different from the catalog database. */
    private static final String TABLE_DATABASE = "db_b";

    private static final String TABLE_DATABASE_URL = "jdbc:testdb://localhost:5432/db_b";

    @Test
    void shouldCloseRemainingConnectionsAndKeepSuppressedExceptionsWhenCloseFails()
            throws SQLException {
        TestJdbcCatalog catalog = new TestJdbcCatalog();
        Connection failedConnection = mock(Connection.class);
        Connection remainingConnection = mock(Connection.class);
        doThrow(new SQLException("first close failed")).when(failedConnection).close();
        doThrow(new SQLException("second close failed")).when(remainingConnection).close();
        catalog.addConnection("jdbc:test:first", failedConnection);
        catalog.addConnection("jdbc:test:second", remainingConnection);

        CatalogException exception =
                Assertions.assertThrows(CatalogException.class, catalog::close);

        verify(failedConnection).close();
        verify(remainingConnection).close();
        Assertions.assertEquals(1, exception.getSuppressed().length);
    }

    /**
     * Control: a table inside the catalog database is truncated on the catalog default connection.
     */
    @Test
    void shouldTruncateTableOnCatalogDefaultConnection() throws SQLException {
        TestJdbcCatalog catalog = new TestJdbcCatalog(CATALOG_DATABASE_URL);
        Connection catalogConnection = registerMockConnection(catalog, CATALOG_DATABASE_URL);
        Connection tableDatabaseConnection = registerMockConnection(catalog, TABLE_DATABASE_URL);

        catalog.truncateTable(TablePath.of(CATALOG_DATABASE, "public", "t"), false);

        verify(catalogConnection).prepareStatement(contains("TRUNCATE"));
        verify(tableDatabaseConnection, never()).prepareStatement(anyString());
    }

    /**
     * The truncate DDL only qualifies schema and table (same as {@code PostgresCatalog}), so the
     * connection URL is the only thing that decides which database gets truncated. Truncating
     * {@code db_b.public.t} through a catalog whose own database is {@code db_a} must therefore run
     * on the {@code db_b} connection, not on the catalog default one.
     */
    @Test
    void shouldRouteTruncateToTableDatabaseWhenItDiffersFromCatalogDefault() throws SQLException {
        TestJdbcCatalog catalog = new TestJdbcCatalog(CATALOG_DATABASE_URL);
        Connection catalogConnection = registerMockConnection(catalog, CATALOG_DATABASE_URL);
        Connection tableDatabaseConnection = registerMockConnection(catalog, TABLE_DATABASE_URL);

        catalog.truncateTable(TablePath.of(TABLE_DATABASE, "public", "t"), false);

        verify(tableDatabaseConnection).prepareStatement(contains("TRUNCATE"));
        verify(catalogConnection, never()).prepareStatement(anyString());
    }

    /**
     * Control: a table path without database has no database to route to and keeps using the
     * catalog default connection.
     */
    @Test
    void shouldTruncateTableOnCatalogDefaultConnectionWhenTablePathHasNoDatabase()
            throws SQLException {
        TestJdbcCatalog catalog = new TestJdbcCatalog(CATALOG_DATABASE_URL);
        Connection catalogConnection = registerMockConnection(catalog, CATALOG_DATABASE_URL);

        // truncateTableInternal() is invoked directly because tableExists() cannot probe a
        // database-less table path; the existence check is not part of the routing contract here.
        catalog.truncateTableInternal(new TablePath(null, "public", "t"));

        verify(catalogConnection).prepareStatement(contains("TRUNCATE"));
    }

    private static Connection registerMockConnection(TestJdbcCatalog catalog, String url)
            throws SQLException {
        Connection connection = mock(Connection.class);
        when(connection.prepareStatement(anyString())).thenReturn(mock(PreparedStatement.class));
        catalog.addConnection(url, connection);
        return connection;
    }

    private static class TestJdbcCatalog extends AbstractJdbcCatalog {

        private TestJdbcCatalog() {
            super(
                    "test",
                    "user",
                    "password",
                    new JdbcUrlUtil.UrlInfo(
                            "jdbc:test:default", "jdbc:test:", "localhost", 0, "default", null),
                    null,
                    null);
        }

        private TestJdbcCatalog(String origin) {
            super("test", "user", "password", JdbcUrlUtil.getUrlInfo(origin), null, null);
        }

        private void addConnection(String url, Connection connection) {
            connectionMap.put(url, connection);
        }

        /** The table is assumed to exist: existence checks are not part of the reproduction. */
        @Override
        public boolean tableExists(TablePath tablePath) {
            return true;
        }

        /** Mirrors {@code PostgresCatalog}: schema and table only, no database qualifier. */
        @Override
        protected String getTruncateTableSql(TablePath tablePath) {
            return "TRUNCATE TABLE \""
                    + tablePath.getSchemaName()
                    + "\".\""
                    + tablePath.getTableName()
                    + "\"";
        }
    }
}
