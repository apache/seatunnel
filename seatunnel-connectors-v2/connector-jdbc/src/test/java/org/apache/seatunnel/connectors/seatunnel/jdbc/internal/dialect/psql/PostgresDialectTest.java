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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceTable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class PostgresDialectTest {

    @Test
    void testUpsertStatement() {
        PostgresDialect dialect = new PostgresDialect();
        final String database = "seatunnel";
        final String tableName = "role";
        final String[] fieldNames = {
            "id", "type", "role_name", "description", "create_time", "update_time"
        };
        final String[] doUpdateKeyFields = {"id"};
        final String[] doNothingKeyFields = {
            "id", "type", "role_name", "description", "create_time", "update_time"
        };

        String doUpdateSql =
                dialect.getUpsertStatement(database, tableName, fieldNames, doUpdateKeyFields)
                        .orElseThrow(
                                () ->
                                        new AssertionError(
                                                "Expected doUpdateSql String to be present"));
        Assertions.assertEquals(
                doUpdateSql,
                "INSERT INTO \"seatunnel\".\"role\" (\"id\", \"type\", \"role_name\", \"description\", \"create_time\", \"update_time\") VALUES (:id, :type, :role_name, :description, :create_time, :update_time) ON CONFLICT (\"id\") DO UPDATE SET \"type\"=EXCLUDED.\"type\", \"role_name\"=EXCLUDED.\"role_name\", \"description\"=EXCLUDED.\"description\", \"create_time\"=EXCLUDED.\"create_time\", \"update_time\"=EXCLUDED.\"update_time\"");
        String doNothingSql =
                dialect.getUpsertStatement(database, tableName, fieldNames, doNothingKeyFields)
                        .orElseThrow(
                                () ->
                                        new AssertionError(
                                                "Expected doNothingSql String to be present"));
        Assertions.assertEquals(
                doNothingSql,
                "INSERT INTO \"seatunnel\".\"role\" (\"id\", \"type\", \"role_name\", \"description\", \"create_time\", \"update_time\") VALUES (:id, :type, :role_name, :description, :create_time, :update_time) ON CONFLICT (\"id\", \"type\", \"role_name\", \"description\", \"create_time\", \"update_time\") DO NOTHING");
    }

    @Test
    void testApproximateRowCntStatementCoversPartitionedTables() throws SQLException {
        PostgresDialect dialect = new PostgresDialect();
        JdbcSourceTable table =
                JdbcSourceTable.builder().tablePath(TablePath.of("test.abc")).build();

        Connection connection = Mockito.mock(Connection.class);
        Statement statement = Mockito.mock(Statement.class);
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        Mockito.when(connection.createStatement()).thenReturn(statement);
        Mockito.when(statement.executeQuery(Mockito.anyString())).thenReturn(resultSet);
        Mockito.when(resultSet.next()).thenReturn(true);
        Mockito.when(resultSet.getLong(1)).thenReturn(42L);

        Assertions.assertEquals(42L, dialect.approximateRowCntStatement(connection, table));

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(statement).executeQuery(sqlCaptor.capture());
        Assertions.assertTrue(
                sqlCaptor.getValue().contains("relkind IN ('r', 'p')"),
                "The row count query must include partitioned tables: " + sqlCaptor.getValue());
    }
}
