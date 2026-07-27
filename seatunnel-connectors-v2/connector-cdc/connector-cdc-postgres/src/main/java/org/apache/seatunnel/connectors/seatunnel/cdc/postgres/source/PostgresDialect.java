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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.JdbcDataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.splitter.ChunkSplitter;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.base.utils.CatalogTableUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.config.PostgresSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.config.PostgresSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.enumerator.PostgresChunkSplitter;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.offset.LsnOffset;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.reader.PostgresSourceFetchTaskContext;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.reader.snapshot.PostgresSnapshotFetchTask;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.reader.wal.PostgresWalFetchTask;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.utils.PostgresSchema;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.utils.TableDiscoveryUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresObjectUtils;
import io.debezium.connector.postgresql.PostgresTaskContext;
import io.debezium.connector.postgresql.PostgresTopicSelector;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.connection.ServerInfo;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import io.debezium.schema.TopicSelector;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.debezium.connector.postgresql.PostgresConnectorConfig.DROP_SLOT_ON_STOP;
import static io.debezium.connector.postgresql.PostgresConnectorConfig.PLUGIN_NAME;
import static io.debezium.connector.postgresql.PostgresConnectorConfig.SLOT_NAME;
import static org.apache.seatunnel.connectors.seatunnel.cdc.postgres.utils.PostgresConnectionUtils.newPostgresValueConverterBuilder;

@Slf4j
public class PostgresDialect implements JdbcDataSourceDialect {

    private static final long serialVersionUID = 1L;
    private final PostgresSourceConfig sourceConfig;

    private transient PostgresSchema postgresSchema;
    private PostgresWalFetchTask postgresWalFetchTask;

    private final Map<TableId, CatalogTable> tableMap;
    private boolean requireReplicaIdentityFull = true;

    public PostgresDialect(
            PostgresSourceConfigFactory configFactory, List<CatalogTable> catalogTables) {
        this.sourceConfig = configFactory.create(0);
        this.tableMap = CatalogTableUtils.convertTables(catalogTables);
    }

    protected PostgresDialect(
            PostgresSourceConfigFactory configFactory,
            List<CatalogTable> catalogTables,
            boolean requireReplicaIdentityFull) {
        this(configFactory, catalogTables);
        this.requireReplicaIdentityFull = requireReplicaIdentityFull;
    }

    @Override
    public String getName() {
        return DatabaseIdentifier.POSTGRESQL;
    }

    /**
     * Creates the configured streaming slot before any snapshot split records its low watermark.
     */
    @Override
    public void openEnumerator(JdbcSourceConfig sourceConfig) {
        if (!requiresStreamingSlotForSnapshot(sourceConfig)) {
            return;
        }

        PostgresConnectorConfig connectorConfig =
                (PostgresConnectorConfig) sourceConfig.getDbzConnectorConfig();
        PostgresConnectorConfig.LogicalDecoder logicalDecoder =
                PostgresConnectorConfig.LogicalDecoder.parse(
                        connectorConfig.getConfig().getString(PLUGIN_NAME));
        try (PostgresConnection connection =
                (PostgresConnection) openJdbcConnection(sourceConfig)) {
            String slotName = connectorConfig.getConfig().getString(SLOT_NAME);
            SlotState slotState =
                    connection.getReplicationSlotState(
                            slotName, logicalDecoder.getPostgresPluginName());
            if (slotState != null) {
                return;
            }

            PostgresConnectorConfig bootstrapConfig =
                    new PostgresConnectorConfig(
                            connectorConfig
                                    .getConfig()
                                    .edit()
                                    .with(DROP_SLOT_ON_STOP, false)
                                    .build());
            TopicSelector<TableId> topicSelector = PostgresTopicSelector.create(bootstrapConfig);
            TypeRegistry typeRegistry = connection.getTypeRegistry();
            io.debezium.connector.postgresql.PostgresSchema schema =
                    PostgresObjectUtils.newSchema(
                            connection,
                            bootstrapConfig,
                            typeRegistry,
                            topicSelector,
                            newPostgresValueConverterBuilder(
                                            bootstrapConfig,
                                            "postgres-enumerator-slot-bootstrap",
                                            sourceConfig.getServerTimeZone())
                                    .build(typeRegistry));
            PostgresTaskContext taskContext =
                    PostgresObjectUtils.newTaskContext(bootstrapConfig, schema, topicSelector);
            ReplicationConnection replicationConnection =
                    PostgresObjectUtils.createReplicationConnection(
                            taskContext, connection, false, bootstrapConfig);
            try {
                replicationConnection.createReplicationSlot().orElse(null);
            } catch (SQLException e) {
                if (!"42710".equals(e.getSQLState())) {
                    throw e;
                }
                log.debug("PostgreSQL streaming slot was created concurrently: {}", slotName);
            } finally {
                replicationConnection.close();
            }
        } catch (Exception e) {
            throw new SeaTunnelException(
                    "Failed to prepare the PostgreSQL streaming replication slot", e);
        }
    }

    /**
     * Honors {@code slot.drop.on.stop} for the slot owned by the snapshot enumerator.
     *
     * <p>An active incremental reader retains responsibility for dropping its own slot.
     */
    @Override
    public void closeEnumerator(JdbcSourceConfig sourceConfig) {
        if (!requiresStreamingSlotForSnapshot(sourceConfig)) {
            return;
        }

        PostgresConnectorConfig connectorConfig =
                (PostgresConnectorConfig) sourceConfig.getDbzConnectorConfig();
        if (!connectorConfig.getConfig().getBoolean(DROP_SLOT_ON_STOP)) {
            return;
        }

        PostgresConnectorConfig.LogicalDecoder logicalDecoder =
                PostgresConnectorConfig.LogicalDecoder.parse(
                        connectorConfig.getConfig().getString(PLUGIN_NAME));
        String slotName = connectorConfig.getConfig().getString(SLOT_NAME);
        try (PostgresConnection connection =
                (PostgresConnection) openJdbcConnection(sourceConfig)) {
            SlotState slotState =
                    connection.getReplicationSlotState(
                            slotName, logicalDecoder.getPostgresPluginName());
            if (slotState == null || connection.dropReplicationSlot(slotName)) {
                return;
            }

            SlotState remainingSlot =
                    connection.getReplicationSlotState(
                            slotName, logicalDecoder.getPostgresPluginName());
            if (remainingSlot != null && remainingSlot.slotIsActive()) {
                log.debug(
                        "PostgreSQL streaming slot {} is still active; its reader will drop it",
                        slotName);
                return;
            }
            if (remainingSlot != null) {
                throw new SeaTunnelException(
                        "Failed to drop PostgreSQL streaming replication slot " + slotName);
            }
        } catch (SQLException e) {
            throw new SeaTunnelException(
                    "Failed to clean up PostgreSQL streaming replication slot " + slotName, e);
        }
    }

    /**
     * Returns whether an exactly-once initial snapshot needs a persistent streaming slot.
     *
     * <p>Snapshot-only and non-exactly-once modes keep their existing slot lifecycle.
     */
    private boolean requiresStreamingSlotForSnapshot(JdbcSourceConfig sourceConfig) {
        return sourceConfig.isExactlyOnce()
                && sourceConfig.getStartupConfig().getStartupMode() == StartupMode.INITIAL;
    }

    @Override
    public boolean isDataCollectionIdCaseSensitive(JdbcSourceConfig sourceConfig) {
        // todo: need to check the case sensitive of the database
        return true;
    }

    @Override
    public JdbcConnection openJdbcConnection(JdbcSourceConfig sourceConfig) {
        PostgresConnectorConfig conf =
                (PostgresConnectorConfig) sourceConfig.getDbzConnectorConfig();
        return new PostgresConnection(
                conf.getJdbcConfig(),
                newPostgresValueConverterBuilder(
                        conf, "postgres-dialect", sourceConfig.getServerTimeZone()),
                "postgres-dialect");
    }

    @Override
    public ChunkSplitter createChunkSplitter(JdbcSourceConfig sourceConfig) {
        return new PostgresChunkSplitter(sourceConfig, this);
    }

    @Override
    public List<TableId> discoverDataCollections(JdbcSourceConfig sourceConfig) {
        PostgresSourceConfig postgresSourceConfig = (PostgresSourceConfig) sourceConfig;
        try (JdbcConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
            List<TableId> tables =
                    TableDiscoveryUtils.listTables(
                            jdbcConnection, postgresSourceConfig.getTableFilters());
            this.checkAllTablesEnabledCapture(jdbcConnection, tables);
            return tables;
        } catch (SQLException e) {
            throw new SeaTunnelException("Error to discover tables: " + e.getMessage(), e);
        }
    }

    @Override
    public void checkAllTablesEnabledCapture(JdbcConnection jdbcConnection, List<TableId> tableIds)
            throws SQLException {
        PostgresConnection postgresConnection = (PostgresConnection) jdbcConnection;
        for (TableId tableId : tableIds) {
            ServerInfo.ReplicaIdentity replicaIdentity =
                    postgresConnection.readReplicaIdentityInfo(tableId);
            if (requireReplicaIdentityFull
                    && !ServerInfo.ReplicaIdentity.FULL.equals(replicaIdentity)) {
                throw new SeaTunnelException(
                        String.format(
                                "Table %s does not have a full replica identity, please execute: ALTER TABLE %s REPLICA IDENTITY FULL;",
                                tableId, tableId));
            }
        }
    }

    @Override
    public TableChanges.TableChange queryTableSchema(JdbcConnection jdbc, TableId tableId) {
        if (postgresSchema == null) {
            postgresSchema = new PostgresSchema(sourceConfig.getDbzConnectorConfig(), tableMap);
        }
        return postgresSchema.getTableSchema(jdbc, tableId);
    }

    @Override
    public PostgresSourceFetchTaskContext createFetchTaskContext(
            SourceSplitBase sourceSplitBase, JdbcSourceConfig taskSourceConfig) {

        RelationalDatabaseConnectorConfig dbzConnectorConfig =
                taskSourceConfig.getDbzConnectorConfig();

        PostgresConnection jdbcConnection =
                new PostgresConnection(
                        dbzConnectorConfig.getJdbcConfig(),
                        newPostgresValueConverterBuilder(
                                (PostgresConnectorConfig) dbzConnectorConfig,
                                "postgres-source-fetch-task",
                                taskSourceConfig.getServerTimeZone()),
                        "postgres-source-fetch-task");

        List<TableChanges.TableChange> tableChangeList = new ArrayList<>();
        // TODO: support save table schema
        if (sourceSplitBase instanceof SnapshotSplit) {
            SnapshotSplit snapshotSplit = (SnapshotSplit) sourceSplitBase;
            tableChangeList.add(queryTableSchema(jdbcConnection, snapshotSplit.getTableId()));
        } else {
            IncrementalSplit incrementalSplit = (IncrementalSplit) sourceSplitBase;
            for (TableId tableId : incrementalSplit.getTableIds()) {
                tableChangeList.add(queryTableSchema(jdbcConnection, tableId));
            }
        }

        return new PostgresSourceFetchTaskContext(
                taskSourceConfig, this, jdbcConnection, tableChangeList);
    }

    @Override
    public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase) {
        if (sourceSplitBase.isSnapshotSplit()) {
            return new PostgresSnapshotFetchTask(sourceSplitBase.asSnapshotSplit());
        } else {
            try (JdbcConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
                List<TableId> tables = sourceSplitBase.asIncrementalSplit().getTableIds();
                this.checkAllTablesEnabledCapture(jdbcConnection, tables);
            } catch (SQLException e) {
                throw new SeaTunnelException("Error to check tables: " + e.getMessage(), e);
            }
            postgresWalFetchTask = new PostgresWalFetchTask(sourceSplitBase.asIncrementalSplit());
            return postgresWalFetchTask;
        }
    }

    @Override
    public void commitChangeLogOffset(Offset offset) throws Exception {
        if (postgresWalFetchTask != null) {
            postgresWalFetchTask.commitCurrentOffset((LsnOffset) offset);
        }
    }

    @Override
    public Optional<PrimaryKey> getPrimaryKey(JdbcConnection jdbcConnection, TableId tableId) {
        return Optional.ofNullable(tableMap.get(tableId).getTableSchema().getPrimaryKey());
    }

    @Override
    public List<ConstraintKey> getConstraintKeys(JdbcConnection jdbcConnection, TableId tableId) {
        return tableMap.get(tableId).getTableSchema().getConstraintKeys();
    }
}
