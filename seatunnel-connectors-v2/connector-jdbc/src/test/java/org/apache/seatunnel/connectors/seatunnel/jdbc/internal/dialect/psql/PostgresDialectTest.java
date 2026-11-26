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
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.condition.UpdateCondition;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dialectenum.FieldIdeEnum;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceTable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

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
    void testConditionalUpsertStatement() {
        PostgresDialect dialect = new PostgresDialect();
        String[] fieldNames = {"id", "name", "update_time"};
        UpdateCondition condition =
                UpdateCondition.parse(
                        ":update_time > target.update_time", Arrays.asList(fieldNames));

        Assertions.assertEquals(
                "INSERT INTO \"seatunnel\".\"role\" AS target (\"id\", \"name\", \"update_time\")"
                        + " VALUES (:id, :name, :update_time) ON CONFLICT (\"id\") DO UPDATE SET"
                        + " \"name\"=EXCLUDED.\"name\", \"update_time\"=EXCLUDED.\"update_time\""
                        + " WHERE (EXCLUDED.\"update_time\" > target.\"update_time\")",
                dialect.getConditionalUpsertStatement(
                        "seatunnel", "role", fieldNames, new String[] {"id"}, condition));

        Assertions.assertEquals(
                "INSERT INTO \"seatunnel\".\"role\" AS target (\"id\", \"name\", \"update_time\")"
                        + " VALUES (:id, :name, :update_time) ON CONFLICT (\"id\", \"name\") DO UPDATE"
                        + " SET \"update_time\"=EXCLUDED.\"update_time\""
                        + " WHERE (EXCLUDED.\"update_time\" > target.\"update_time\")",
                dialect.getConditionalUpsertStatement(
                        "seatunnel", "role", fieldNames, new String[] {"id", "name"}, condition));

        Assertions.assertEquals(
                "INSERT INTO \"seatunnel\".\"role\" AS target (\"id\", \"name\", \"update_time\")"
                        + " VALUES (:id, :name, :update_time) ON CONFLICT (\"id\", \"name\","
                        + " \"update_time\") DO NOTHING",
                dialect.getConditionalUpsertStatement(
                        "seatunnel", "role", fieldNames, fieldNames, condition));
    }

    @Test
    void testConditionalUpsertStatementFollowsFieldIde() {
        PostgresDialect dialect = new PostgresDialect(FieldIdeEnum.UPPERCASE.getValue());
        String[] fieldNames = {"id", "update_time"};
        String sql =
                dialect.getConditionalUpsertStatement(
                        "seatunnel",
                        "role",
                        fieldNames,
                        new String[] {"id"},
                        UpdateCondition.parse(
                                ":update_time > target.update_time", Arrays.asList(fieldNames)));

        Assertions.assertTrue(
                sql.endsWith(
                        "DO UPDATE SET \"UPDATE_TIME\"=EXCLUDED.\"UPDATE_TIME\""
                                + " WHERE (EXCLUDED.\"UPDATE_TIME\" > target.\"UPDATE_TIME\")"),
                sql);
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
