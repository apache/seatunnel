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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source;

import static org.apache.seatunnel.connectors.seatunnel.cdc.mysql.utils.MySqlConnectionUtils.createBinaryClient;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mysql.utils.MySqlConnectionUtils.createMySqlConnection;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mysql.utils.MySqlConnectionUtils.isTableIdCaseSensitive;

import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.JdbcDataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.relational.connection.JdbcConnectionPoolFactory;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.splitter.ChunkSplitter;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config.MySqlSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config.MySqlSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.eumerator.MySqlChunkSplitter;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.reader.fetch.MySqlSourceFetchTaskContext;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.reader.fetch.binlog.MySqlBinlogFetchTask;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.reader.fetch.scan.MySqlSnapshotFetchTask;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.utils.MySqlSchema;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.utils.TableDiscoveryUtils;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

import java.sql.SQLException;
import java.util.List;

/** The {@link JdbcDataSourceDialect} implementation for MySQL datasource. */

public class MySqlDialect implements JdbcDataSourceDialect {

    private static final long serialVersionUID = 1L;
    private final MySqlSourceConfig sourceConfig;
    private transient MySqlSchema mySqlSchema;

    public MySqlDialect(MySqlSourceConfigFactory configFactory) {
        this.sourceConfig = configFactory.create(0);
    }

    @Override
    public String getName() {
        return "MySQL";
    }

    @Override
    public boolean isDataCollectionIdCaseSensitive(JdbcSourceConfig sourceConfig) {
        try (JdbcConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
            return isTableIdCaseSensitive(jdbcConnection);
        } catch (SQLException e) {
            throw new SeaTunnelException("Error reading MySQL variables: " + e.getMessage(), e);
        }
    }

    @Override
    public ChunkSplitter createChunkSplitter(JdbcSourceConfig sourceConfig) {
        return new MySqlChunkSplitter(sourceConfig, this);
    }

    @Override
    public JdbcConnectionPoolFactory getPooledDataSourceFactory() {
        return new MysqlPooledDataSourceFactory();
    }

    @Override
    public List<TableId> discoverDataCollections(JdbcSourceConfig sourceConfig) {
        MySqlSourceConfig mySqlSourceConfig = (MySqlSourceConfig) sourceConfig;
        try (JdbcConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
            return TableDiscoveryUtils.listTables(
                    jdbcConnection, mySqlSourceConfig.getTableFilters());
        } catch (SQLException e) {
            throw new SeaTunnelException("Error to discover tables: " + e.getMessage(), e);
        }
    }

    @Override
    public TableChanges.TableChange queryTableSchema(JdbcConnection jdbc, TableId tableId) {
        if (mySqlSchema == null) {
            mySqlSchema = new MySqlSchema(sourceConfig, isDataCollectionIdCaseSensitive(sourceConfig));
        }
        return mySqlSchema.getTableSchema(jdbc, tableId);
    }

    @Override
    public MySqlSourceFetchTaskContext createFetchTaskContext(
        SourceSplitBase sourceSplitBase, JdbcSourceConfig taskSourceConfig) {
        final MySqlConnection jdbcConnection =
                createMySqlConnection(taskSourceConfig.getDbzConfiguration());
        final BinaryLogClient binaryLogClient =
                createBinaryClient(taskSourceConfig.getDbzConfiguration());
        return new MySqlSourceFetchTaskContext(
                taskSourceConfig, this, jdbcConnection, binaryLogClient);
    }

    @Override
    public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase) {
        if (sourceSplitBase.isSnapshotSplit()) {
            return new MySqlSnapshotFetchTask(sourceSplitBase.asSnapshotSplit());
        } else {
            return new MySqlBinlogFetchTask(sourceSplitBase.asIncrementalSplit());
        }
    }
}
