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

package org.apache.searunnel.connectors.cdc.db2.source;

import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.JdbcDataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.relational.connection.JdbcConnectionPoolFactory;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.splitter.ChunkSplitter;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;

import org.apache.searunnel.connectors.cdc.db2.config.Db2SourceConfig;
import org.apache.searunnel.connectors.cdc.db2.config.Db2SourceConfigFactory;
import org.apache.searunnel.connectors.cdc.db2.source.eumerator.Db2ChunkSplitter;
import org.apache.searunnel.connectors.cdc.db2.source.reader.fetch.Db2SourceFetchTaskContext;
import org.apache.searunnel.connectors.cdc.db2.source.reader.fetch.scan.Db2SnapshotFetchTask;
import org.apache.searunnel.connectors.cdc.db2.source.reader.fetch.transactionlog.Db2TransactionLogFetchTask;
import org.apache.searunnel.connectors.cdc.db2.utils.Db2ConnectionUtils;
import org.apache.searunnel.connectors.cdc.db2.utils.Db2Schema;
import org.apache.searunnel.connectors.cdc.db2.utils.TableDiscoveryUtils;

import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

import java.sql.SQLException;
import java.util.List;

/** The {@link JdbcDataSourceDialect} implementation for Db2 datasource. */
public class Db2Dialect implements JdbcDataSourceDialect {

    private static final long serialVersionUID = 1L;
    private final Db2SourceConfig sourceConfig;

    private transient Db2Schema db2Schema;

    public Db2Dialect(Db2SourceConfigFactory configFactory) {
        this.sourceConfig = configFactory.create(0);
    }

    @Override
    public String getName() {
        return "Db2";
    }

    @Override
    public boolean isDataCollectionIdCaseSensitive(JdbcSourceConfig sourceConfig) {
        // todo: need to check the case sensitive of the database
        return true;
    }

    @Override
    public JdbcConnection openJdbcConnection(JdbcSourceConfig sourceConfig) {
        return Db2ConnectionUtils.createDb2Connection((Configuration) sourceConfig);
    }

    @Override
    public ChunkSplitter createChunkSplitter(JdbcSourceConfig sourceConfig) {
        return new Db2ChunkSplitter(sourceConfig, this);
    }

    @Override
    public JdbcConnectionPoolFactory getPooledDataSourceFactory() {
        return new Db2PooledDataSourceFactory();
    }

    @Override
    public List<TableId> discoverDataCollections(JdbcSourceConfig sourceConfig) {
        Db2SourceConfig Db2SourceConfig = (Db2SourceConfig) sourceConfig;
        try (JdbcConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
            return TableDiscoveryUtils.listTables(
                    jdbcConnection, Db2SourceConfig.getTableFilters());
        } catch (SQLException e) {
            throw new SeaTunnelException("Error to discover tables: " + e.getMessage(), e);
        }
    }

    @Override
    public TableChanges.TableChange queryTableSchema(JdbcConnection jdbc, TableId tableId) {
        if (db2Schema == null) {
            db2Schema = new Db2Schema(sourceConfig.getDbzConnectorConfig());
        }
        return db2Schema.getTableSchema(jdbc, tableId);
    }

    @Override
    public Db2SourceFetchTaskContext createFetchTaskContext(
            SourceSplitBase sourceSplitBase, JdbcSourceConfig taskSourceConfig) {

        return new Db2SourceFetchTaskContext(taskSourceConfig, this);
    }

    @Override
    public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase) {
        if (sourceSplitBase.isSnapshotSplit()) {
            return new Db2SnapshotFetchTask(sourceSplitBase.asSnapshotSplit());
        } else {
            return new Db2TransactionLogFetchTask(sourceSplitBase.asIncrementalSplit());
        }
    }
}
