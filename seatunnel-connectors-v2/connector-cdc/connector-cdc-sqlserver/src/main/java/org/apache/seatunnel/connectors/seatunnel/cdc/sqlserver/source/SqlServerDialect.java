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

package org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.source;

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
import org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.config.SqlServerSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.config.SqlServerSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.source.enumerator.SqlServerChunkSplitter;
import org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.source.reader.fetch.SqlServerSourceFetchTaskContext;
import org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.source.reader.fetch.scan.SqlServerSnapshotFetchTask;
import org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.source.reader.fetch.transactionlog.SqlServerTransactionLogFetchTask;
import org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.utils.SqlServerConnectionUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.utils.SqlServerSchema;
import org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.utils.TableDiscoveryUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import io.debezium.connector.sqlserver.SqlServerChangeTable;
import io.debezium.connector.sqlserver.SqlServerConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** The {@link JdbcDataSourceDialect} implementation for MySQL datasource. */
public class SqlServerDialect implements JdbcDataSourceDialect {

    private static final long serialVersionUID = 1L;
    private final SqlServerSourceConfig sourceConfig;

    private transient SqlServerSchema sqlServerSchema;
    private final Map<TableId, CatalogTable> tableMap;

    public SqlServerDialect(
            SqlServerSourceConfigFactory configFactory, List<CatalogTable> catalogTables) {
        this.sourceConfig = configFactory.create(0);
        this.tableMap = CatalogTableUtils.convertTables(catalogTables);
    }

    @Override
    public String getName() {
        return DatabaseIdentifier.SQLSERVER;
    }

    @Override
    public boolean isDataCollectionIdCaseSensitive(JdbcSourceConfig sourceConfig) {
        // todo: need to check the case sensitive of the database
        return true;
    }

    @Override
    public JdbcConnection openJdbcConnection(JdbcSourceConfig sourceConfig) {
        return SqlServerConnectionUtils.createSqlServerConnection(
                sourceConfig.getDbzConfiguration());
    }

    @Override
    public ChunkSplitter createChunkSplitter(JdbcSourceConfig sourceConfig) {
        return new SqlServerChunkSplitter(sourceConfig, this);
    }

    @Override
    public List<TableId> discoverDataCollections(JdbcSourceConfig sourceConfig) {
        SqlServerSourceConfig sqlServerSourceConfig = (SqlServerSourceConfig) sourceConfig;
        try (JdbcConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
            List<TableId> tables =
                    TableDiscoveryUtils.listTables(
                            jdbcConnection, sqlServerSourceConfig.getTableFilters());
            this.checkAllTablesEnabledCapture(jdbcConnection, tables);
            return tables;
        } catch (SQLException e) {
            throw new SeaTunnelException("Error to discover tables: " + e.getMessage(), e);
        }
    }

    @Override
    public void checkAllTablesEnabledCapture(JdbcConnection jdbcConnection, List<TableId> tableIds)
            throws SQLException {
        Map<String, List<TableId>> databases =
                tableIds.stream()
                        .collect(Collectors.groupingBy(TableId::catalog, Collectors.toList()));
        for (String database : databases.keySet()) {
            Set<TableId> tables =
                    ((SqlServerConnection) jdbcConnection)
                            .getChangeTables(database).stream()
                                    .map(SqlServerChangeTable::getSourceTableId)
                                    .collect(Collectors.toSet());
            for (TableId tableId : databases.get(database)) {
                if (!tables.contains(tableId)) {
                    throw new SeaTunnelException(
                            "Table " + tableId + " is not enabled for capture");
                }
            }
        }
    }

    @Override
    public TableChanges.TableChange queryTableSchema(JdbcConnection jdbc, TableId tableId) {
        if (sqlServerSchema == null) {
            sqlServerSchema = new SqlServerSchema(sourceConfig.getDbzConnectorConfig(), tableMap);
        }
        return sqlServerSchema.getTableSchema(jdbc, tableId);
    }

    @Override
    public SqlServerSourceFetchTaskContext createFetchTaskContext(
            SourceSplitBase sourceSplitBase, JdbcSourceConfig taskSourceConfig) {

        return new SqlServerSourceFetchTaskContext((SqlServerSourceConfig) taskSourceConfig, this);
    }

    @Override
    public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase) {
        if (sourceSplitBase.isSnapshotSplit()) {
            return new SqlServerSnapshotFetchTask(sourceSplitBase.asSnapshotSplit());
        } else {
            try (JdbcConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
                List<TableId> tables = sourceSplitBase.asIncrementalSplit().getTableIds();
                this.checkAllTablesEnabledCapture(jdbcConnection, tables);
            } catch (SQLException e) {
                throw new SeaTunnelException("Error to check tables: " + e.getMessage(), e);
            }
            return new SqlServerTransactionLogFetchTask(sourceSplitBase.asIncrementalSplit());
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
