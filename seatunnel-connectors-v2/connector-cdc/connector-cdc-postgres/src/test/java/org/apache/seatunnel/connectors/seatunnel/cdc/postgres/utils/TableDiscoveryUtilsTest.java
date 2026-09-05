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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.debezium.config.Configuration;
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

import static org.mockito.Mockito.when;

class TableDiscoveryUtilsTest {

    @Test
    void shouldOnlyQueryDatabasesAllowedByConfiguredFilter() throws SQLException {
        RelationalTableFilters tableFilters = Mockito.mock(RelationalTableFilters.class);
        when(tableFilters.databaseFilter()).thenReturn("selected"::equals);
        when(tableFilters.dataCollectionFilter()).thenReturn(tableId -> true);

        MockJdbcConnection jdbc = new MockJdbcConnection();

        List<TableId> tableIds = TableDiscoveryUtils.listTables(jdbc, tableFilters);

        Assertions.assertEquals(
                Collections.singletonList(new TableId("selected", "public", "orders")), tableIds);
        Assertions.assertEquals(
                Arrays.asList(
                        "select datname from pg_database",
                        "SELECT * FROM \"selected\".INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE';"),
                jdbc.getQueries());
    }

    private static class MockJdbcConnection extends JdbcConnection {
        private final List<String> queries = new ArrayList<>();

        MockJdbcConnection() {
            super(
                    JdbcConfiguration.adapt(Configuration.from(Collections.emptyMap())),
                    config -> null,
                    "\"",
                    "\"");
        }

        @Override
        public JdbcConnection query(String query, ResultSetConsumer resultConsumer)
                throws SQLException {
            queries.add(query);
            ResultSet resultSet = Mockito.mock(ResultSet.class);
            if (query.equals("select datname from pg_database")) {
                when(resultSet.next()).thenReturn(true, true, false);
                when(resultSet.getString(1)).thenReturn("selected", "unwanted");
            } else if (query.contains("\"selected\"")) {
                when(resultSet.next()).thenReturn(true, false);
                when(resultSet.getString(1)).thenReturn("selected");
                when(resultSet.getString(2)).thenReturn("public");
                when(resultSet.getString(3)).thenReturn("orders");
            } else {
                throw new AssertionError("Unexpected database query: " + query);
            }
            resultConsumer.accept(resultSet);
            return this;
        }

        List<String> getQueries() {
            return queries;
        }
    }
}
