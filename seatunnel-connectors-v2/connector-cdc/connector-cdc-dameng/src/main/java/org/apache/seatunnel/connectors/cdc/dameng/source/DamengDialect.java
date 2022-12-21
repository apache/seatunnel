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

package org.apache.seatunnel.connectors.cdc.dameng.source;

import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.JdbcDataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.relational.connection.JdbcConnectionFactory;
import org.apache.seatunnel.connectors.cdc.base.relational.connection.JdbcConnectionPoolFactory;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.splitter.ChunkSplitter;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.JdbcSourceFetchTaskContext;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.dameng.config.DamengSourceConfig;
import org.apache.seatunnel.connectors.cdc.dameng.config.DamengSourceConfigFactory;
import org.apache.seatunnel.connectors.cdc.dameng.source.eumerator.DamengChunkSplitter;
import org.apache.seatunnel.connectors.cdc.dameng.source.reader.fetch.DamengSourceFetchTaskContext;
import org.apache.seatunnel.connectors.cdc.dameng.source.reader.fetch.logminer.DamengLogMinerFetchTask;
import org.apache.seatunnel.connectors.cdc.dameng.source.reader.fetch.snapshot.DamengSnapshotFetchTask;
import org.apache.seatunnel.connectors.cdc.dameng.utils.DamengSchema;

import io.debezium.config.Configuration;
import io.debezium.connector.dameng.DamengConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DamengDialect implements JdbcDataSourceDialect {
    private final DamengSourceConfig sourceConfig;
    private transient DamengSchema damengSchema;

    public DamengDialect(DamengSourceConfigFactory configFactory) {
        this(configFactory.create(0));
    }

    public DamengDialect(DamengSourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    @Override
    public String getName() {
        return "Dameng";
    }

    @Override
    public DamengConnection openJdbcConnection(JdbcSourceConfig sourceConfig) {
        DamengConnection connection = new DamengConnection(sourceConfig.getDbzConfiguration(),
            new JdbcConnectionFactory(sourceConfig, getPooledDataSourceFactory()));
        try {
            return connection.connect();
        } catch (Exception e) {
            throw new SeaTunnelException(e);
        }
    }

    @Override
    public boolean isDataCollectionIdCaseSensitive(JdbcSourceConfig sourceConfig) {
        try (DamengConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
            return jdbcConnection.isCaseSensitive();
        } catch (SQLException e) {
            throw new SeaTunnelException("Error reading Dameng system config: " + e.getMessage(), e);
        }
    }

    @Override
    public ChunkSplitter createChunkSplitter(JdbcSourceConfig sourceConfig) {
        return new DamengChunkSplitter(sourceConfig, this);
    }

    @Override
    public List<TableId> discoverDataCollections(JdbcSourceConfig sourceConfig) {
        DamengSourceConfig damengSourceConfig = (DamengSourceConfig) sourceConfig;
        try (DamengConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
            return jdbcConnection.listTables(damengSourceConfig.getTableFilters());
        } catch (SQLException e) {
            throw new SeaTunnelException("Error to discover tables: " + e.getMessage(), e);
        }
    }

    @Override
    public JdbcConnectionPoolFactory getPooledDataSourceFactory() {
        return new DamengPooledDataSourceFactory();
    }

    @Override
    public TableChanges.TableChange queryTableSchema(JdbcConnection jdbc, TableId tableId) {
        if (damengSchema == null) {
            synchronized (this) {
                if (damengSchema == null) {
                    damengSchema = new DamengSchema();
                }
            }
        }
        return damengSchema.getTableSchema(jdbc, tableId);
    }

    @Override
    public JdbcSourceFetchTaskContext createFetchTaskContext(SourceSplitBase sourceSplitBase,
                                                             JdbcSourceConfig taskSourceConfig) {
        Configuration jdbcConfig = taskSourceConfig.getDbzConnectorConfig().getJdbcConfig();
        DamengConnection jdbcConnection = new DamengConnection(jdbcConfig);
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

        return new DamengSourceFetchTaskContext(taskSourceConfig,
            this, jdbcConnection, tableChangeList);
    }

    @Override
    public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase) {
        if (sourceSplitBase.isSnapshotSplit()) {
            return new DamengSnapshotFetchTask(sourceSplitBase.asSnapshotSplit());
        } else {
            return new DamengLogMinerFetchTask(sourceSplitBase.asIncrementalSplit());
        }
    }
}
