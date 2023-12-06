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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgresql.source;

import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.JdbcDataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.relational.connection.JdbcConnectionPoolFactory;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.splitter.ChunkSplitter;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgresql.config.PostgresSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgresql.config.PostgresSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgresql.source.enumerator.PostgresChunkSplitter;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgresql.source.reader.PostgresSourceFetchTaskContext;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgresql.source.reader.snapshot.PostgresSnapshotFetchTask;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgresql.source.reader.wal.PostgresWalFetchTask;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgresql.utils.PostgresSchema;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgresql.utils.TableDiscoveryUtils;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

import java.sql.SQLException;
import java.util.List;

import static org.apache.seatunnel.connectors.seatunnel.cdc.postgresql.utils.PostgresConnectionUtils.createPostgresConnection;

public class PostgresDialect implements JdbcDataSourceDialect {

    private static final long serialVersionUID = 1L;
    private final PostgresSourceConfig sourceConfig;

    private transient PostgresSchema postgresSchema;

    public PostgresDialect(PostgresSourceConfigFactory configFactory) {
        this.sourceConfig = configFactory.create(0);
    }

    @Override
    public String getName() {
        return "Postgres";
    }

    @Override
    public boolean isDataCollectionIdCaseSensitive(JdbcSourceConfig sourceConfig) {
        // todo: need to check the case sensitive of the database
        return true;
    }

    @Override
    public JdbcConnection openJdbcConnection(JdbcSourceConfig sourceConfig) {
        return createPostgresConnection(sourceConfig.getDbzConfiguration());
    }

    @Override
    public ChunkSplitter createChunkSplitter(JdbcSourceConfig sourceConfig) {
        return new PostgresChunkSplitter(sourceConfig, this);
    }

    @Override
    public JdbcConnectionPoolFactory getPooledDataSourceFactory() {
        return new PostgresPooledDataSourceFactory();
    }

    @Override
    public List<TableId> discoverDataCollections(JdbcSourceConfig sourceConfig) {
        PostgresSourceConfig postgresSourceConfig = (PostgresSourceConfig) sourceConfig;
        try (JdbcConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
            return TableDiscoveryUtils.listTables(
                    jdbcConnection, postgresSourceConfig.getTableFilters());
        } catch (SQLException e) {
            throw new SeaTunnelException("Error to discover tables: " + e.getMessage(), e);
        }
    }

    @Override
    public TableChanges.TableChange queryTableSchema(JdbcConnection jdbc, TableId tableId) {
        if (postgresSchema == null) {
            postgresSchema = new PostgresSchema(sourceConfig.getDbzConnectorConfig());
        }
        return postgresSchema.getTableSchema(jdbc, tableId);
    }

    @Override
    public PostgresSourceFetchTaskContext createFetchTaskContext(
            SourceSplitBase sourceSplitBase, JdbcSourceConfig taskSourceConfig) {

        return new PostgresSourceFetchTaskContext(taskSourceConfig, this);
    }

    @Override
    public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase) {
        if (sourceSplitBase.isSnapshotSplit()) {
            return new PostgresSnapshotFetchTask(sourceSplitBase.asSnapshotSplit());
        } else {
            return new PostgresWalFetchTask(sourceSplitBase.asIncrementalSplit());
        }
    }
}
