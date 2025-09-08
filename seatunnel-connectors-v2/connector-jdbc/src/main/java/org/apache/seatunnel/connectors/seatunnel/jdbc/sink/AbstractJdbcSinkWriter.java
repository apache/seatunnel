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

package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSinkWriter;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.handler.TableSchemaChangeEventDispatcher;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcOutputFormat;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcOutputFormatBuilder;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSinkState;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.XidInfo;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;

@Slf4j
public abstract class AbstractJdbcSinkWriter<ResourceT>
        implements SinkWriter<SeaTunnelRow, XidInfo, JdbcSinkState>,
                SupportMultiTableSinkWriter<ResourceT>,
                SupportSchemaEvolutionSinkWriter {

    protected JdbcDialect dialect;
    protected TablePath sinkTablePath;
    protected TableSchema tableSchema;
    protected TableSchema databaseTableSchema;
    protected transient boolean isOpen;
    protected JdbcConnectionProvider connectionProvider;
    protected JdbcSinkConfig jdbcSinkConfig;
    protected JdbcOutputFormat<SeaTunnelRow, JdbcBatchStatementExecutor<SeaTunnelRow>> outputFormat;
    protected TableSchemaChangeEventDispatcher tableSchemaChanger =
            new TableSchemaChangeEventDispatcher();

    @Override
    public void applySchemaChange(SchemaChangeEvent event) throws IOException {
        this.tableSchema = tableSchemaChanger.reset(tableSchema).apply(event);
        reOpenOutputFormat(event);
    }

    protected void reOpenOutputFormat(SchemaChangeEvent event) throws IOException {
        this.prepareCommit();
        JdbcConnectionProvider refreshTableSchemaConnectionProvider =
                dialect.getJdbcConnectionProvider(jdbcSinkConfig.getJdbcConnectionConfig());
        try (Connection connection =
                refreshTableSchemaConnectionProvider.getOrEstablishConnection()) {
            dialect.applySchemaChange(connection, sinkTablePath, event);
        } catch (Throwable e) {
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.REFRESH_PHYSICAL_TABLESCHEMA_BY_SCHEMA_CHANGE_EVENT, e);
        }
        this.outputFormat =
                new JdbcOutputFormatBuilder(
                                dialect,
                                connectionProvider,
                                jdbcSinkConfig,
                                tableSchema,
                                databaseTableSchema)
                        .build();
        this.outputFormat.open();
    }
}
