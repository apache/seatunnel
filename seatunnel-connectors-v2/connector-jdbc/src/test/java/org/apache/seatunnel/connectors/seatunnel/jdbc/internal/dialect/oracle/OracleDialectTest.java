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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceTable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OracleDialectTest {

    @Test
    public void testSampleDataFromColumnOrdersByDatabaseCollationForTablePath() throws Exception {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        when(connection.prepareStatement(
                        anyString(),
                        eq(ResultSet.TYPE_FORWARD_ONLY),
                        eq(ResultSet.CONCUR_READ_ONLY)))
                .thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        JdbcSourceTable table =
                JdbcSourceTable.builder()
                        .tablePath(TablePath.of("oracle_catalog", "oracle_schema", "oracle_tbl"))
                        .build();

        new OracleDialect().sampleDataFromColumn(connection, table, "id", 1, 100);

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(connection)
                .prepareStatement(
                        sqlCaptor.capture(),
                        eq(ResultSet.TYPE_FORWARD_ONLY),
                        eq(ResultSet.CONCUR_READ_ONLY));
        // The sample must be ordered by the database collation, not re-sorted on the Java
        // side: chunk boundaries are later compared by the database in its own collation.
        Assertions.assertEquals(
                "SELECT \"id\" FROM \"oracle_schema\".\"oracle_tbl\" ORDER BY \"id\"",
                sqlCaptor.getValue());
    }

    @Test
    public void testSampleDataFromColumnOrdersByDatabaseCollationForQuery() throws Exception {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        when(connection.prepareStatement(
                        anyString(),
                        eq(ResultSet.TYPE_FORWARD_ONLY),
                        eq(ResultSet.CONCUR_READ_ONLY)))
                .thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        JdbcSourceTable table =
                JdbcSourceTable.builder()
                        .tablePath(TablePath.of("oracle_catalog", "oracle_schema", "oracle_tbl"))
                        .query("SELECT id FROM custom_src WHERE x = 1")
                        .build();

        new OracleDialect().sampleDataFromColumn(connection, table, "id", 1, 100);

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(connection)
                .prepareStatement(
                        sqlCaptor.capture(),
                        eq(ResultSet.TYPE_FORWARD_ONLY),
                        eq(ResultSet.CONCUR_READ_ONLY));
        Assertions.assertEquals(
                "SELECT \"id\" FROM (SELECT id FROM custom_src WHERE x = 1) T ORDER BY \"id\"",
                sqlCaptor.getValue());
    }

    @Test
    void testAllKeyTableOmitsEmptyUpdateSet() {
        JdbcDialect dialect = new OracleDialect();
        String[] allFields = {"id", "name", "age"};
        Optional<String> upsert =
                dialect.getUpsertStatement("test_db", "test_table", allFields, allFields);
        Assertions.assertTrue(upsert.isPresent(), "upsert statement should be present");
        String sql = upsert.get().toUpperCase();
        Assertions.assertFalse(
                sql.contains("WHEN MATCHED THEN UPDATE SET"),
                "all-key table must NOT emit an empty 'WHEN MATCHED THEN UPDATE SET' (got: "
                        + sql
                        + ")");
        Assertions.assertTrue(
                sql.contains("WHEN NOT MATCHED"), "all-key table must still insert unmatched rows");
        Assertions.assertTrue(
                sql.contains("INSERT"), "all-key table statement must contain an INSERT branch");
    }

    @Test
    void testPartialKeyTableStillUpdates() {
        JdbcDialect dialect = new OracleDialect();
        String[] allFields = {"id", "name", "age"};
        String[] uniqueKeys = {"id"};
        Optional<String> upsert =
                dialect.getUpsertStatement("test_db", "test_table", allFields, uniqueKeys);
        Assertions.assertTrue(upsert.isPresent(), "upsert statement should be present");
        String sql = upsert.get().toUpperCase();
        Assertions.assertTrue(
                sql.contains("WHEN MATCHED THEN UPDATE SET"),
                "partial-key table must still emit 'WHEN MATCHED THEN UPDATE SET' (got: "
                        + sql
                        + ")");
    }
}
