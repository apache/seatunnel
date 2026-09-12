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

package org.apache.seatunnel.connectors.seatunnel.cdc.gaussdb;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.base.utils.CatalogTableUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.config.PostgresSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.PostgresDialect;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.offset.LsnOffset;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.reader.PostgresSourceFetchTaskContext;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.reader.snapshot.PostgresSnapshotFetchTask;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** GaussDB-compatible PostgreSQL dialect that optionally replaces the WAL reader with mppdb. */
final class GaussDBDialect extends PostgresDialect {

    private static final long serialVersionUID = 1L;

    /** Source configuration used to open per-split JDBC connections. */
    private final JdbcSourceConfig sourceConfig;

    /** Captured catalog tables used for split schema baselines and primary key metadata. */
    private final Map<TableId, CatalogTable> tableMap;

    /** Validated database-specific replication settings and selected decoding plugin. */
    private final GaussDBMppdbConfig mppdbConfig;

    /** Active incremental task receiving completed-checkpoint acknowledgements. */
    private transient GaussDBWalFetchTask walFetchTask;

    /** Creates the GaussDB dialect while retaining PostgreSQL snapshot and catalog behavior. */
    GaussDBDialect(
            PostgresSourceConfigFactory configFactory,
            List<CatalogTable> catalogTables,
            boolean requireReplicaIdentityFull,
            GaussDBMppdbConfig mppdbConfig) {
        super(configFactory, catalogTables, requireReplicaIdentityFull);
        this.sourceConfig = configFactory.create(0);
        this.tableMap = CatalogTableUtils.convertTables(catalogTables);
        this.mppdbConfig = mppdbConfig;
    }

    /** Opens a GaussDB connection without applying PostgreSQL's version-number semantics. */
    @Override
    public JdbcConnection openJdbcConnection(JdbcSourceConfig sourceConfig) {
        PostgresConnectorConfig connectorConfig =
                (PostgresConnectorConfig) sourceConfig.getDbzConnectorConfig();
        return new GaussDBPostgresConnection(
                connectorConfig.getJdbcConfig(),
                GaussDBPostgresConnection.newValueConverterBuilder(
                        connectorConfig, "gaussdb-dialect", sourceConfig.getServerTimeZone()),
                "gaussdb-dialect");
    }

    /**
     * Creates a version-compatible PostgreSQL context and adds mppdb slot handling when selected.
     */
    @Override
    public PostgresSourceFetchTaskContext createFetchTaskContext(
            SourceSplitBase sourceSplit, JdbcSourceConfig taskSourceConfig) {
        PostgresConnectorConfig connectorConfig =
                (PostgresConnectorConfig) taskSourceConfig.getDbzConnectorConfig();
        // Resolve the converter builder once through the GaussDB adapter; the stock PostgreSQL
        // helper would reject GaussDB's reported 9.2 server version.
        PostgresConnection.PostgresValueConverterBuilder valueConverterBuilder =
                GaussDBPostgresConnection.newValueConverterBuilder(
                        connectorConfig,
                        "gaussdb-source-fetch-task",
                        taskSourceConfig.getServerTimeZone());
        PostgresConnection jdbcConnection =
                new GaussDBPostgresConnection(
                        connectorConfig.getJdbcConfig(),
                        valueConverterBuilder,
                        "gaussdb-source-fetch-task");

        List<TableChanges.TableChange> tableChanges = new ArrayList<>();
        if (sourceSplit instanceof SnapshotSplit) {
            tableChanges.add(
                    queryTableSchema(jdbcConnection, sourceSplit.asSnapshotSplit().getTableId()));
        } else {
            for (TableId tableId : sourceSplit.asIncrementalSplit().getTableIds()) {
                tableChanges.add(queryTableSchema(jdbcConnection, tableId));
            }
        }

        List<CatalogTable> schemaBaseline = new ArrayList<>(tableMap.values());
        if (sourceSplit.isIncrementalSplit()
                && sourceSplit.asIncrementalSplit().getCheckpointTables() != null
                && !sourceSplit.asIncrementalSplit().getCheckpointTables().isEmpty()) {
            schemaBaseline = sourceSplit.asIncrementalSplit().getCheckpointTables();
        }
        if (mppdbConfig.usesMppdbDecoding()) {
            return new GaussDBSourceFetchTaskContext(
                    taskSourceConfig,
                    this,
                    jdbcConnection,
                    tableChanges,
                    schemaBaseline,
                    valueConverterBuilder,
                    mppdbConfig);
        }
        return new PostgresSourceFetchTaskContext(
                taskSourceConfig,
                this,
                jdbcConnection,
                tableChanges,
                schemaBaseline,
                valueConverterBuilder);
    }

    /** Uses the existing snapshot task and the GaussDB-specific incremental task. */
    @Override
    public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplit) {
        if (!mppdbConfig.usesMppdbDecoding()) {
            return super.createFetchTask(sourceSplit);
        }
        if (sourceSplit.isSnapshotSplit()) {
            return new PostgresSnapshotFetchTask(sourceSplit.asSnapshotSplit());
        }
        try (JdbcConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
            List<TableId> tables = sourceSplit.asIncrementalSplit().getTableIds();
            checkAllTablesEnabledCapture(jdbcConnection, tables);
        } catch (SQLException e) {
            throw new SeaTunnelException("Error checking GaussDB captured tables", e);
        }
        walFetchTask = new GaussDBWalFetchTask(sourceSplit.asIncrementalSplit());
        return walFetchTask;
    }

    /** Sends checkpoint-completed LSNs to the active mppdb stream. */
    @Override
    public void commitChangeLogOffset(Offset offset) throws Exception {
        if (!mppdbConfig.usesMppdbDecoding()) {
            super.commitChangeLogOffset(offset);
            return;
        }
        if (walFetchTask != null) {
            walFetchTask.commitCurrentOffset((LsnOffset) offset);
        }
    }

    /** Returns whether this dialect uses the native mppdb WAL task. */
    boolean usesMppdbDecoding() {
        return mppdbConfig.usesMppdbDecoding();
    }

    /** Ensures the mppdb slot exists before latest-offset enumeration reads its boundary. */
    void ensureSlot(JdbcSourceConfig taskSourceConfig) {
        try (JdbcConnection jdbcConnection = openJdbcConnection(taskSourceConfig)) {
            MppdbReplicationStream stream =
                    new MppdbReplicationStream(
                            jdbcConnection.connection(),
                            mppdbConfig,
                            taskSourceConfig.getUsername(),
                            taskSourceConfig.getPassword());
            stream.ensureSlot();
            jdbcConnection.commit();
        } catch (Exception e) {
            throw new SeaTunnelException(
                    "Failed to prepare GaussDB mppdb_decoding slot before offset discovery", e);
        }
    }
}
