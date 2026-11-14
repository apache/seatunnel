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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oceanbase;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceTable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OceanBaseMysqlDialectTest {

    @Test
    public void testSampleDataFromColumnOrdersByDatabaseCollationForTablePath() throws Exception {
        Connection connection = mock(Connection.class);
        Statement stmt = mock(Statement.class);
        ResultSet rs = mock(ResultSet.class);
        when(connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY))
                .thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(rs.next()).thenReturn(false);

        JdbcSourceTable table =
                JdbcSourceTable.builder()
                        .tablePath(TablePath.of("oceanbase_db", "oceanbase_tbl"))
                        .build();

        new OceanBaseMysqlDialect().sampleDataFromColumn(connection, table, "id", 1, 100);

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(stmt).executeQuery(sqlCaptor.capture());
        // The sample must be ordered by the database collation, not re-sorted on the Java
        // side: chunk boundaries are later compared by the database in its own collation.
        Assertions.assertEquals(
                "SELECT `id` FROM `oceanbase_db`.`oceanbase_tbl` ORDER BY `id`",
                sqlCaptor.getValue());
    }

    @Test
    public void testSampleDataFromColumnOrdersByDatabaseCollationForQuery() throws Exception {
        Connection connection = mock(Connection.class);
        Statement stmt = mock(Statement.class);
        ResultSet rs = mock(ResultSet.class);
        when(connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY))
                .thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(rs.next()).thenReturn(false);

        JdbcSourceTable table =
                JdbcSourceTable.builder()
                        .tablePath(TablePath.of("oceanbase_db", "oceanbase_tbl"))
                        .query("SELECT id FROM custom_src WHERE x = 1")
                        .build();

        new OceanBaseMysqlDialect().sampleDataFromColumn(connection, table, "id", 1, 100);

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(stmt).executeQuery(sqlCaptor.capture());
        Assertions.assertEquals(
                "SELECT `id` FROM (SELECT id FROM custom_src WHERE x = 1) AS T ORDER BY `id`",
                sqlCaptor.getValue());
    }

    @Test
    public void testValidateTableOptions() {
        OceanBaseMysqlDialect dialect = new OceanBaseMysqlDialect();
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("engine", "InnoDB");
        tableOptions.put("charset", "utf8mb4");
        tableOptions.put("collate", "utf8mb4_unicode_ci");

        Assertions.assertDoesNotThrow(() -> dialect.validateTableOptions(tableOptions));
    }

    @Test
    public void testValidateTableOptionsWithUnknownKey() {
        OceanBaseMysqlDialect dialect = new OceanBaseMysqlDialect();

        JdbcConnectorException exception =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () ->
                                dialect.validateTableOptions(
                                        Collections.singletonMap("bucket_num", "3")));
        Assertions.assertTrue(exception.getMessage().contains("Unsupported JDBC table_options"));
        Assertions.assertTrue(exception.getMessage().contains("OceanBase"));
    }
}
