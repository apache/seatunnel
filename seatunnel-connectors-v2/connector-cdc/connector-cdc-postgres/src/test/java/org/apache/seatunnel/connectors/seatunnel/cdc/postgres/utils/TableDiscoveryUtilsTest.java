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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgres.utils;

import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.config.PostgresSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.config.PostgresSourceConfigFactory;

import org.junit.jupiter.api.Test;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableId;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TableDiscoveryUtilsTest {

    /** Simulates a JDBC connection whose INFORMATION_SCHEMA.TABLES returns the given rows. */
    private static class FakePostgresConnection extends PostgresConnection {

        private final List<String[]> tableRows;

        private FakePostgresConnection(List<String[]> tableRows) {
            super(JdbcConfiguration.empty(), "table-discovery-test");
            this.tableRows = tableRows;
        }

        @Override
        public JdbcConnection query(String query, ResultSetConsumer consumer) throws SQLException {
            if (query.startsWith("select datname")) {
                consumer.accept(mockResultSet(Collections.singletonList(new String[] {"testdb"})));
                return this;
            }
            consumer.accept(mockResultSet(tableRows));
            return this;
        }
    }

    /** Builds a ResultSet mock backed by rows of {TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME}. */
    private static ResultSet mockResultSet(List<String[]> rows) throws SQLException {
        ResultSet resultSet = mock(ResultSet.class);
        AtomicInteger cursor = new AtomicInteger(-1);
        when(resultSet.next()).thenAnswer(invocation -> cursor.incrementAndGet() < rows.size());
        when(resultSet.getString(anyInt()))
                .thenAnswer(
                        invocation -> {
                            int column = invocation.getArgument(0);
                            return rows.get(cursor.get())[column - 1];
                        });
        return resultSet;
    }

    private static RelationalTableFilters tableFilters() {
        PostgresSourceConfig config =
                (PostgresSourceConfig)
                        new PostgresSourceConfigFactory()
                                .hostname("localhost")
                                .username("user")
                                .password("password")
                                .databaseList("testdb")
                                .create(0);
        return config.getTableFilters();
    }

    /**
     * HighGo returns the same BASE TABLE row six times from INFORMATION_SCHEMA.TABLES. The
     * discovery result must contain a single TableId with the original catalog/schema/table
     * identity preserved.
     */
    @Test
    public void shouldDeduplicateRepeatedCatalogRows() throws SQLException {
        List<String[]> rows = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            rows.add(new String[] {"highgo", "testdb", "test_a"});
        }

        List<TableId> tableIds =
                TableDiscoveryUtils.listTables(new FakePostgresConnection(rows), tableFilters());

        assertEquals(1, tableIds.size());
        assertEquals(new TableId("highgo", "testdb", "test_a"), tableIds.get(0));
    }

    /** An ordinary PostgreSQL catalog without duplicates must pass through unchanged, in order. */
    @Test
    public void shouldPreserveDistinctTablesInOrder() throws SQLException {
        List<String[]> rows =
                Arrays.asList(
                        new String[] {"testdb", "public", "orders"},
                        new String[] {"testdb", "public", "users"},
                        new String[] {"testdb", "inventory", "shipments"});

        List<TableId> tableIds =
                TableDiscoveryUtils.listTables(new FakePostgresConnection(rows), tableFilters());

        assertEquals(
                Arrays.asList(
                        new TableId("testdb", "public", "orders"),
                        new TableId("testdb", "public", "users"),
                        new TableId("testdb", "inventory", "shipments")),
                tableIds);
    }

    /** Duplicates interleaved with distinct tables must collapse to their first occurrence. */
    @Test
    public void shouldKeepFirstOccurrenceWhenDuplicatesInterleave() throws SQLException {
        List<String[]> rows =
                Arrays.asList(
                        new String[] {"testdb", "public", "orders"},
                        new String[] {"highgo", "testdb", "test_a"},
                        new String[] {"testdb", "public", "users"},
                        new String[] {"highgo", "testdb", "test_a"},
                        new String[] {"testdb", "public", "orders"});

        List<TableId> tableIds =
                TableDiscoveryUtils.listTables(new FakePostgresConnection(rows), tableFilters());

        assertEquals(
                Arrays.asList(
                        new TableId("testdb", "public", "orders"),
                        new TableId("highgo", "testdb", "test_a"),
                        new TableId("testdb", "public", "users")),
                tableIds);
    }
}
