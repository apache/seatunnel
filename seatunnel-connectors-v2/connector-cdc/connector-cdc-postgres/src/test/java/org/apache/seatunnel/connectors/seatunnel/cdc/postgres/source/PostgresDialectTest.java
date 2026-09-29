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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source;

import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.config.PostgresSourceConfigFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ServerInfo;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.TableId;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

public class PostgresDialectTest {
    private static class TestPostgresConnection extends PostgresConnection {

        private final ServerInfo.ReplicaIdentity identity;

        public TestPostgresConnection(ServerInfo.ReplicaIdentity identity) {
            super(JdbcConfiguration.empty(), "test");
            this.identity = identity;
        }

        @Override
        public ServerInfo.ReplicaIdentity readReplicaIdentityInfo(TableId tableId) {
            return identity;
        }
    }

    @Test
    public void shouldThrowWhenNotFullAndRequired() throws SQLException {
        PostgresSourceConfigFactory configFactory =
                (PostgresSourceConfigFactory)
                        new PostgresSourceConfigFactory()
                                .hostname("localhost")
                                .username("user")
                                .password("password")
                                .databaseList("database");

        List<TableId> tableIds =
                Collections.singletonList(new TableId("catalog", "schema", "table"));

        PostgresConnection conn = new TestPostgresConnection(ServerInfo.ReplicaIdentity.DEFAULT);
        PostgresDialect dialect = new PostgresDialect(configFactory, Collections.emptyList());

        Assertions.assertThrows(
                SeaTunnelException.class,
                () -> dialect.checkAllTablesEnabledCapture(conn, tableIds));
    }

    @Test
    public void shouldNotThrowWhenFullAndRequired() throws SQLException {
        PostgresSourceConfigFactory configFactory =
                (PostgresSourceConfigFactory)
                        new PostgresSourceConfigFactory()
                                .hostname("localhost")
                                .username("user")
                                .password("password")
                                .databaseList("database");

        List<TableId> tableIds =
                Collections.singletonList(new TableId("catalog", "schema", "table"));

        PostgresConnection conn = new TestPostgresConnection(ServerInfo.ReplicaIdentity.FULL);
        PostgresDialect dialect = new PostgresDialect(configFactory, Collections.emptyList());
        Assertions.assertDoesNotThrow(() -> dialect.checkAllTablesEnabledCapture(conn, tableIds));
    }

    @Test
    public void shouldNotThrowWhenNotFullAndNotRequired() throws SQLException {
        PostgresSourceConfigFactory configFactory =
                (PostgresSourceConfigFactory)
                        new PostgresSourceConfigFactory()
                                .hostname("localhost")
                                .username("user")
                                .password("password")
                                .databaseList("database");

        List<TableId> tableIds =
                Collections.singletonList(new TableId("catalog", "schema", "table"));

        PostgresDialect dialect =
                new PostgresDialect(configFactory, Collections.emptyList(), false);
        PostgresConnection conn = new TestPostgresConnection(ServerInfo.ReplicaIdentity.DEFAULT);
        Assertions.assertDoesNotThrow(() -> dialect.checkAllTablesEnabledCapture(conn, tableIds));
    }
}
