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
import org.apache.seatunnel.api.sink.SupportSchemaRefreshSinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.handler.TableSchemaChangeEventDispatcher;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcOutputFormat;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcOutputFormatBuilder;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSinkState;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.XidInfo;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Optional;

@Slf4j
public abstract class AbstractJdbcSinkWriter<ResourceT>
        implements SinkWriter<SeaTunnelRow, XidInfo, JdbcSinkState>,
                SupportMultiTableSinkWriter<ResourceT>,
                SupportSchemaEvolutionSinkWriter,
                SupportSchemaRefreshSinkWriter {

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
        TableSchema evolvedSchema =
                event.getChangeAfter() == null
                        ? tableSchemaChanger.reset(tableSchema).apply(event)
                        : event.getChangeAfter().getTableSchema();
        this.prepareCommit();
        try (JdbcSchemaChangeApplier applier =
                new JdbcSchemaChangeApplier(dialect, jdbcSinkConfig, sinkTablePath)) {
            applier.apply(event);
        }
        refreshTableSchema(evolvedSchema);
    }

    /**
     * Exposes the resolved physical JDBC sink table so the multi-table coordinator can fan schema
     * changes out to every sibling writer that shares the same destination table.
     */
    @Override
    public Optional<String> getPhysicalSinkTableIdentifier() {
        return sinkTablePath == null ? Optional.empty() : Optional.of(sinkTablePath.getFullName());
    }

    @Override
    public void refreshSchema(CatalogTable evolvedSchema) throws IOException {
        if (evolvedSchema == null) {
            throw new IllegalArgumentException("evolvedSchema cannot be null");
        }
        refreshTableSchema(evolvedSchema.getTableSchema());
    }

    protected void refreshTableSchema(TableSchema evolvedSchema) throws IOException {
        if (isOpen) {
            outputFormat.closeStatements();
        }
        this.tableSchema = evolvedSchema;
        this.outputFormat =
                new JdbcOutputFormatBuilder(
                                dialect,
                                connectionProvider,
                                jdbcSinkConfig,
                                tableSchema,
                                databaseTableSchema)
                        .build();
        if (isOpen) {
            this.outputFormat.open();
        }
    }
}
