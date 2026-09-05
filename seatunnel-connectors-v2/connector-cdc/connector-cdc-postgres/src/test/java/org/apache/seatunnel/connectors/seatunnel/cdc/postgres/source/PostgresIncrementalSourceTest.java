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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.cdc.base.option.JdbcSourceOptions;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.config.PostgresIncrementalSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcCommonOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ServerInfo;
import io.debezium.relational.TableId;

import java.io.ObjectStreamClass;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Pins that {@code createDataSourceDialect()} resolves {@code require-replica-identity-full} from
 * the job config.
 *
 * <p>This matters because the value used to be read into a {@code final} field assigned after
 * {@code super(...)} returned, while {@code createDataSourceDialect()} is invoked from inside that
 * constructor — so the dialect always saw the Java default {@code false} and the check was silently
 * a no-op. Reading it inside {@code createDataSourceDialect(config)} restores the documented
 * default of {@code true}; without this test, the same timing bug could be reintroduced unnoticed.
 */
public class PostgresIncrementalSourceTest {

    private static final TableId ORDERS = new TableId(null, "public", "orders");

    /**
     * Locks the UID computed from the released 2.3.13 class for persisted job DAG upgrades.
     *
     * <p>Changing the constant would make released logical DAG payloads fail deserialization.
     */
    @Test
    public void testSerialVersionUidMatchesReleasedPostgresSource() {
        Assertions.assertEquals(
                -9086519839702872016L,
                ObjectStreamClass.lookup(PostgresIncrementalSource.class).getSerialVersionUID());
    }

    @Test
    public void testDialectEnforcesReplicaIdentityFullByDefault() throws Exception {
        PostgresDialect dialect = buildDialect(new HashMap<>());

        SeaTunnelException exception =
                Assertions.assertThrows(
                        SeaTunnelException.class,
                        () ->
                                dialect.checkAllTablesEnabledCapture(
                                        connectionReporting(ServerInfo.ReplicaIdentity.DEFAULT),
                                        Collections.singletonList(ORDERS)));

        Assertions.assertTrue(exception.getMessage().contains("full replica identity"));
        Assertions.assertTrue(exception.getMessage().contains("public.orders"));
    }

    @Test
    public void testDialectHonorsConfiguredReplicaIdentityOptOut() throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put(PostgresIncrementalSourceOptions.REQUIRE_REPLICA_IDENTITY_FULL.key(), false);

        PostgresDialect dialect = buildDialect(options);

        Assertions.assertDoesNotThrow(
                () ->
                        dialect.checkAllTablesEnabledCapture(
                                connectionReporting(ServerInfo.ReplicaIdentity.DEFAULT),
                                Collections.singletonList(ORDERS)));
    }

    @Test
    public void testDialectAcceptsFullReplicaIdentityWhenEnforced() throws Exception {
        PostgresDialect dialect = buildDialect(new HashMap<>());

        Assertions.assertDoesNotThrow(
                () ->
                        dialect.checkAllTablesEnabledCapture(
                                connectionReporting(ServerInfo.ReplicaIdentity.FULL),
                                Collections.singletonList(ORDERS)));
    }

    /**
     * Builds the dialect through the real {@code createDataSourceDialect(config)} entry point, so
     * the test exercises the wiring under review rather than constructing the dialect directly.
     */
    private static PostgresDialect buildDialect(Map<String, Object> extraOptions) {
        Map<String, Object> options = new HashMap<>(baseOptions());
        options.putAll(extraOptions);
        ReadonlyConfig config = ReadonlyConfig.fromMap(options);

        TestingPostgresIncrementalSource source = new TestingPostgresIncrementalSource(config);
        return (PostgresDialect) source.createDataSourceDialect(config);
    }

    private static Map<String, Object> baseOptions() {
        Map<String, Object> options = new HashMap<>();
        options.put(JdbcCommonOptions.URL.key(), "jdbc:postgresql://127.0.0.1:5432/inventory");
        options.put(JdbcSourceOptions.USERNAME.key(), "user");
        options.put(JdbcSourceOptions.PASSWORD.key(), "pwd");
        options.put(JdbcSourceOptions.DATABASE_NAMES.key(), Collections.singletonList("inventory"));
        options.put(
                ConnectorCommonOptions.TABLE_NAMES.key(),
                Collections.singletonList("inventory.public.orders"));
        return options;
    }

    /** A connection whose replica-identity probe reports the given identity for every table. */
    private static PostgresConnection connectionReporting(ServerInfo.ReplicaIdentity identity)
            throws Exception {
        PostgresConnection connection = mock(PostgresConnection.class);
        when(connection.readReplicaIdentityInfo(any(TableId.class))).thenReturn(identity);
        return connection;
    }

    private static List<CatalogTable> catalogTables() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});
        return Collections.singletonList(
                CatalogTableUtil.getCatalogTable(
                        "postgres", "inventory", "public", "orders", rowType));
    }

    /**
     * Keeps the real config-factory and dialect wiring and only stubs out schema loading, which
     * would otherwise open a JDBC connection during construction.
     */
    private static final class TestingPostgresIncrementalSource
            extends PostgresIncrementalSource<SeaTunnelRow> {

        private TestingPostgresIncrementalSource(ReadonlyConfig options) {
            super(options, catalogTables());
        }

        @Override
        public DebeziumDeserializationSchema<SeaTunnelRow> createDebeziumDeserializationSchema(
                ReadonlyConfig config) {
            return null;
        }
    }
}
