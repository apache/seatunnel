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

package org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.source;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.JdbcDataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.splitter.ChunkSplitter;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.base.utils.CatalogTableUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.config.MariaDbSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.config.MariaDbSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.source.enumerator.MariaDbChunkSplitter;
import org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.source.reader.fetch.MariaDbSourceFetchTaskContext;
import org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.source.reader.fetch.binlog.MariaDbBinlogFetchTask;
import org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.source.reader.fetch.scan.MariaDbSnapshotFetchTask;
import org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.utils.MariaDbConnectionUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.utils.MariaDbSchema;
import org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.utils.TableDiscoveryUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.utils.MariaDbConnectionUtils.isTableIdCaseSensitive;

/** The {@link JdbcDataSourceDialect} implementation for MariaDB datasource. */
public class MariaDbDialect implements JdbcDataSourceDialect {
    private static final String QUOTED_CHARACTER = "`";
    private static final long serialVersionUID = 1L;
    private final MariaDbSourceConfig sourceConfig;
    private transient MariaDbSchema mariaDbSchema;
    private final Map<TableId, CatalogTable> tableMap;

    public MariaDbDialect(
            MariaDbSourceConfigFactory configFactory, List<CatalogTable> catalogTables) {
        this.sourceConfig = configFactory.create(0);
        this.tableMap = CatalogTableUtils.convertTables(catalogTables);
    }

    @Override
    public String getName() {
        return DatabaseIdentifier.MARIADB;
    }

    @Override
    public boolean isDataCollectionIdCaseSensitive(JdbcSourceConfig sourceConfig) {
        try (JdbcConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
            return isDataCollectionIdCaseSensitive(jdbcConnection);
        } catch (SQLException e) {
            throw new SeaTunnelException("Error reading MariaDB variables: " + e.getMessage(), e);
        }
    }

    private boolean isDataCollectionIdCaseSensitive(JdbcConnection jdbcConnection) {
        return isTableIdCaseSensitive(jdbcConnection);
    }

    @Override
    public JdbcConnection openJdbcConnection(JdbcSourceConfig sourceConfig) {
        return MariaDbConnectionUtils.createMySqlConnection(sourceConfig.getDbzConfiguration());
    }

    @Override
    public ChunkSplitter createChunkSplitter(JdbcSourceConfig sourceConfig) {
        return new MariaDbChunkSplitter(sourceConfig, this);
    }

    @Override
    public List<TableId> discoverDataCollections(JdbcSourceConfig sourceConfig) {
        MariaDbSourceConfig mariaDbSourceConfig = (MariaDbSourceConfig) sourceConfig;
        try (JdbcConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
            return TableDiscoveryUtils.listTables(
                    jdbcConnection, mariaDbSourceConfig.getTableFilters());
        } catch (SQLException e) {
            throw new SeaTunnelException("Error to discover tables: " + e.getMessage(), e);
        }
    }

    @Override
    public TableChanges.TableChange queryTableSchema(JdbcConnection jdbc, TableId tableId) {
        if (mariaDbSchema == null) {
            mariaDbSchema =
                    new MariaDbSchema(
                            sourceConfig, isDataCollectionIdCaseSensitive(jdbc), tableMap);
        }
        return mariaDbSchema.getTableSchema(jdbc, tableId);
    }

    @Override
    public MariaDbSourceFetchTaskContext createFetchTaskContext(
            SourceSplitBase sourceSplitBase, JdbcSourceConfig taskSourceConfig) {
        return new MariaDbSourceFetchTaskContext(taskSourceConfig, this);
    }

    @Override
    public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase) {
        if (sourceSplitBase.isSnapshotSplit()) {
            return new MariaDbSnapshotFetchTask(sourceSplitBase.asSnapshotSplit());
        } else {
            return new MariaDbBinlogFetchTask(sourceSplitBase.asIncrementalSplit());
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
