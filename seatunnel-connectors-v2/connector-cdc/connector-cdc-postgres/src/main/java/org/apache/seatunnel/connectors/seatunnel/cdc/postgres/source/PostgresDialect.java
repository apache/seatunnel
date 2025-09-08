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
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ServerInfo;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.seatunnel.connectors.seatunnel.cdc.postgres.utils.PostgresConnectionUtils.newPostgresValueConverterBuilder;

public class PostgresDialect implements JdbcDataSourceDialect {

    private static final long serialVersionUID = 1L;
    private final PostgresSourceConfig sourceConfig;

    private transient PostgresSchema postgresSchema;
    private PostgresWalFetchTask postgresWalFetchTask;

    private final Map<TableId, CatalogTable> tableMap;

    public PostgresDialect(
            PostgresSourceConfigFactory configFactory, List<CatalogTable> catalogTables) {
        this.sourceConfig = configFactory.create(0);
        this.tableMap = CatalogTableUtils.convertTables(catalogTables);
    }

    @Override
    public String getName() {
        return DatabaseIdentifier.POSTGRESQL;
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
            if (!ServerInfo.ReplicaIdentity.FULL.equals(replicaIdentity)) {
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
