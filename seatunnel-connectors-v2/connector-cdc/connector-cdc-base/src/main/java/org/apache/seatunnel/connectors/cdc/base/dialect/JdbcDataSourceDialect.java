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

package org.apache.seatunnel.connectors.cdc.base.dialect;

import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.relational.connection.JdbcConnectionFactory;
import org.apache.seatunnel.connectors.cdc.base.relational.connection.JdbcConnectionPoolFactory;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.JdbcSourceFetchTaskContext;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

import java.util.List;

public interface JdbcDataSourceDialect extends DataSourceDialect<JdbcSourceConfig> {

    /** Discovers the list of table to capture. */
    @Override
    List<TableId> discoverDataCollections(JdbcSourceConfig sourceConfig);

    /**
     * Creates and opens a new {@link JdbcConnection} backing connection pool.
     *
     * @param sourceConfig a basic source configuration.
     * @return a utility that simplifies using a JDBC connection.
     */
    default JdbcConnection openJdbcConnection(JdbcSourceConfig sourceConfig) {
        JdbcConnection jdbc =
                new JdbcConnection(
                        sourceConfig.getDbzConfiguration(),
                        new JdbcConnectionFactory(sourceConfig, getPooledDataSourceFactory()));
        try {
            jdbc.connect();
        } catch (Exception e) {
            throw new SeaTunnelException(e);
        }
        return jdbc;
    }

    /** Get a connection pool factory to create connection pool. */
    JdbcConnectionPoolFactory getPooledDataSourceFactory();

    /** Query and build the schema of table. */
    TableChanges.TableChange queryTableSchema(JdbcConnection jdbc, TableId tableId);

    @Override
    FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase);

    @Override
    JdbcSourceFetchTaskContext createFetchTaskContext(
            SourceSplitBase sourceSplitBase, JdbcSourceConfig taskSourceConfig);
}
