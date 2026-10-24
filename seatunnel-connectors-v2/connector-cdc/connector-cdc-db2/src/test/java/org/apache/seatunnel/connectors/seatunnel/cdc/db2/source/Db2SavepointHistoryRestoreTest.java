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

package org.apache.seatunnel.connectors.seatunnel.cdc.db2.source;

import org.apache.seatunnel.connectors.cdc.base.dialect.JdbcDataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.debezium.ConnectTableChangeSerializer;
import org.apache.seatunnel.connectors.cdc.debezium.EmbeddedDatabaseHistory;
import org.apache.seatunnel.connectors.seatunnel.cdc.db2.config.Db2SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.db2.config.Db2SourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.db2.source.offset.LsnOffset;
import org.apache.seatunnel.connectors.seatunnel.cdc.db2.source.reader.fetch.Db2SourceFetchTaskContext;
import org.apache.seatunnel.connectors.seatunnel.cdc.db2.utils.Db2ConnectionUtils;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import io.debezium.config.Configuration;
import io.debezium.connector.db2.Db2Connection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Restores serialized split history through the production JDBC registration path without live
 * schema fallback, including the catalog-first contract used by MySQL checkpoints.
 */
class Db2SavepointHistoryRestoreTest {

    @Test
    void shouldRestoreDb2SchemaFirstHistoryWithoutLiveSchemaFallback() throws Exception {
        assertRestored(
                Arrays.asList(
                        new TableId("", "DB2INST1", "CUSTOMERS"),
                        new TableId(null, "部门.一", "表.二")));
    }

    @Test
    void shouldPreserveCatalogFirstAndThreePartCheckpointIds() throws Exception {
        assertRestored(
                Arrays.asList(
                        new TableId("inventory", null, "customers"),
                        new TableId("database", "schema", "table")));
    }

    private static void assertRestored(List<TableId> tableIds) throws Exception {
        Map<TableId, byte[]> history = new HashMap<>();
        JsonConverter converter = new JsonConverter();
        converter.configure(Collections.singletonMap("schemas.enable", true), false);
        try {
            for (TableId tableId : tableIds) {
                Table table =
                        Table.editor()
                                .tableId(tableId)
                                .addColumn(
                                        Column.editor()
                                                .name("ID")
                                                .jdbcType(Types.INTEGER)
                                                .type("INTEGER")
                                                .position(1)
                                                .optional(false)
                                                .create())
                                .setPrimaryKeyNames(Collections.singletonList("ID"))
                                .create();
                Struct change =
                        new ConnectTableChangeSerializer()
                                .serialize(new TableChanges().create(table))
                                .get(0);
                history.put(tableId, converter.fromConnectData("history", change.schema(), change));
            }
        } finally {
            converter.close();
        }
        IncrementalSplit split =
                new IncrementalSplit(
                        "stream",
                        tableIds,
                        LsnOffset.INITIAL_OFFSET,
                        LsnOffset.NO_STOPPING_OFFSET,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        history);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(split);
        }
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            split = (IncrementalSplit) input.readObject();
        }

        Db2SourceConfigFactory factory = new Db2SourceConfigFactory();
        factory.hostname("localhost")
                .port(50000)
                .username("user")
                .password("password")
                .databaseList("E2E");
        Db2SourceConfig config = factory.create(0);
        JdbcDataSourceDialect dialect = Mockito.mock(JdbcDataSourceDialect.class);
        Mockito.when(dialect.queryTableSchema(Mockito.any(), Mockito.any()))
                .thenThrow(new AssertionError("Saved history must not fall back to live metadata"));
        Db2Connection connection = Mockito.mock(Db2Connection.class);
        String historyName =
                config.getDbzConfiguration()
                        .getString(EmbeddedDatabaseHistory.DATABASE_HISTORY_INSTANCE_NAME);
        try (MockedStatic<Db2ConnectionUtils> connections =
                Mockito.mockStatic(Db2ConnectionUtils.class)) {
            connections
                    .when(
                            () ->
                                    Db2ConnectionUtils.createDb2Connection(
                                            Mockito.any(Configuration.class)))
                    .thenReturn(connection);
            HistoryContext context = new HistoryContext(config, dialect);
            try {
                context.register(split);
                Collection<TableChanges.TableChange> restored =
                        EmbeddedDatabaseHistory.removeHistory(historyName);
                List<Table> tables =
                        restored.stream()
                                .map(TableChanges.TableChange::getTable)
                                .collect(Collectors.toList());
                Assertions.assertEquals(tableIds.size(), tables.size());
                Assertions.assertEquals(
                        tableIds, tables.stream().map(Table::id).collect(Collectors.toList()));
                for (int i = 0; i < tables.size(); i++) {
                    String catalog = tableIds.get(i).catalog();
                    Assertions.assertEquals(
                            catalog == null || catalog.isEmpty() ? null : catalog,
                            tables.get(i).id().catalog());
                    Assertions.assertEquals(tableIds.get(i).schema(), tables.get(i).id().schema());
                    Assertions.assertEquals(tableIds.get(i).table(), tables.get(i).id().table());
                    Assertions.assertEquals(
                            Collections.singletonList("ID"), tables.get(i).primaryKeyColumnNames());
                    Assertions.assertEquals(
                            Types.INTEGER, tables.get(i).columnWithName("ID").jdbcType());
                    Assertions.assertFalse(tables.get(i).columnWithName("ID").isOptional());
                }
            } finally {
                context.close();
                EmbeddedDatabaseHistory.removeHistory(historyName);
            }
        }
    }

    /**
     * Exposes the protected production history entry point without changing runtime code for tests.
     */
    private static class HistoryContext extends Db2SourceFetchTaskContext {
        HistoryContext(Db2SourceConfig config, JdbcDataSourceDialect dialect) {
            super(config, dialect);
        }

        void register(IncrementalSplit split) {
            registerDatabaseHistory(split, getDataConnection());
        }
    }
}
