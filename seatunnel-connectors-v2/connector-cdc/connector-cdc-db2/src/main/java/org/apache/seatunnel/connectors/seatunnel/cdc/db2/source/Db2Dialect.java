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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.JdbcDataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.splitter.ChunkSplitter;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.base.utils.CatalogTableUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.db2.config.Db2SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.db2.config.Db2SourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.db2.source.enumerator.Db2ChunkSplitter;
import org.apache.seatunnel.connectors.seatunnel.cdc.db2.source.reader.fetch.Db2SourceFetchTaskContext;
import org.apache.seatunnel.connectors.seatunnel.cdc.db2.source.reader.fetch.scan.Db2SnapshotFetchTask;
import org.apache.seatunnel.connectors.seatunnel.cdc.db2.source.reader.fetch.transactionlog.Db2TransactionLogFetchTask;
import org.apache.seatunnel.connectors.seatunnel.cdc.db2.utils.Db2ConnectionUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.db2.utils.Db2Schema;
import org.apache.seatunnel.connectors.seatunnel.cdc.db2.utils.TableDiscoveryUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import io.debezium.connector.db2.Db2ChangeTable;
import io.debezium.connector.db2.Db2Connection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** The {@link JdbcDataSourceDialect} implementation for Db2 datasource. */
public class Db2Dialect implements JdbcDataSourceDialect {

    private static final long serialVersionUID = 1L;
    private final Db2SourceConfig sourceConfig;

    private transient Db2Schema db2Schema;
    private final Map<TableId, CatalogTable> tableMap;

    public Db2Dialect(Db2SourceConfigFactory configFactory, List<CatalogTable> catalogTables) {
        this.sourceConfig = configFactory.create(0);
        this.tableMap = createDb2TableMap(catalogTables);
    }

    @Override
    public String getName() {
        return DatabaseIdentifier.DB_2;
    }

    @Override
    public boolean isDataCollectionIdCaseSensitive(JdbcSourceConfig sourceConfig) {
        // todo: need to check the case sensitive of the database
        return true;
    }

    @Override
    public JdbcConnection openJdbcConnection(JdbcSourceConfig sourceConfig) {
        return Db2ConnectionUtils.createDb2Connection(sourceConfig.getDbzConfiguration());
    }

    @Override
    public ChunkSplitter createChunkSplitter(JdbcSourceConfig sourceConfig) {
        return new Db2ChunkSplitter(sourceConfig, this);
    }

    @Override
    public List<TableId> discoverDataCollections(JdbcSourceConfig sourceConfig) {
        Db2SourceConfig db2SourceConfig = (Db2SourceConfig) sourceConfig;
        try (JdbcConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
            List<TableId> tables =
                    TableDiscoveryUtils.listTables(
                            jdbcConnection, db2SourceConfig.getTableFilters());
            TableDiscoveryUtils.validateExplicitCaptureTables(
                    db2SourceConfig.getTableList(), tables);
            return tables;
        } catch (SQLException e) {
            throw new SeaTunnelException("Error to discover tables: " + e.getMessage(), e);
        }
    }

    /**
     * Converts a SeaTunnel table path to the empty-catalog identifier emitted by Db2 Debezium.
     *
     * @param tablePath table path from checkpoint schema state
     * @return Db2 Debezium table identifier
     */
    @Override
    public TableId toTableId(TablePath tablePath) {
        return new TableId("", tablePath.getSchemaName(), tablePath.getTableName());
    }

    @Override
    public void checkAllTablesEnabledCapture(JdbcConnection jdbcConnection, List<TableId> tableIds)
            throws SQLException {
        Set<TableId> tables =
                ((Db2Connection) jdbcConnection)
                        .listOfChangeTables().stream()
                                .map(Db2ChangeTable::getSourceTableId)
                                .collect(java.util.stream.Collectors.toSet());
        for (TableId tableId : tableIds) {
            if (!tables.contains(toDb2TableId(tableId))) {
                throw new SeaTunnelException("Table " + tableId + " is not enabled for capture");
            }
        }
    }

    @Override
    public TableChanges.TableChange queryTableSchema(JdbcConnection jdbc, TableId tableId) {
        if (db2Schema == null) {
            db2Schema = new Db2Schema(sourceConfig.getDbzConnectorConfig(), tableMap);
        }
        return db2Schema.getTableSchema(jdbc, toDb2TableId(tableId));
    }

    @Override
    public Db2SourceFetchTaskContext createFetchTaskContext(
            SourceSplitBase sourceSplitBase, JdbcSourceConfig taskSourceConfig) {

        return new Db2SourceFetchTaskContext((Db2SourceConfig) taskSourceConfig, this);
    }

    @Override
    public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase) {
        if (sourceSplitBase.isSnapshotSplit()) {
            return new Db2SnapshotFetchTask(sourceSplitBase.asSnapshotSplit());
        } else {
            try (JdbcConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
                List<TableId> tables = sourceSplitBase.asIncrementalSplit().getTableIds();
                this.checkAllTablesEnabledCapture(jdbcConnection, tables);
            } catch (SQLException e) {
                throw new SeaTunnelException("Error to check tables: " + e.getMessage(), e);
            }
            return new Db2TransactionLogFetchTask(sourceSplitBase.asIncrementalSplit());
        }
    }

    @Override
    public Optional<PrimaryKey> getPrimaryKey(JdbcConnection jdbcConnection, TableId tableId) {
        return Optional.ofNullable(
                tableMap.get(toDb2TableId(tableId)).getTableSchema().getPrimaryKey());
    }

    @Override
    public List<ConstraintKey> getConstraintKeys(JdbcConnection jdbcConnection, TableId tableId) {
        return tableMap.get(toDb2TableId(tableId)).getTableSchema().getConstraintKeys();
    }

    private static Map<TableId, CatalogTable> createDb2TableMap(List<CatalogTable> catalogTables) {
        Map<TableId, CatalogTable> tables = new HashMap<>();
        CatalogTableUtils.convertTables(catalogTables)
                .forEach(
                        (tableId, catalogTable) -> {
                            tables.put(tableId, catalogTable);
                            tables.put(toDb2TableId(tableId), catalogTable);
                        });
        return tables;
    }

    /**
     * Debezium Db2 reports captured table ids with an empty catalog because the connector captures
     * a single configured database. SeaTunnel catalog tables keep the database name, so all runtime
     * lookups are normalized before comparing with Debezium metadata.
     */
    private static TableId toDb2TableId(TableId tableId) {
        return new TableId("", tableId.schema(), tableId.table());
    }
}
