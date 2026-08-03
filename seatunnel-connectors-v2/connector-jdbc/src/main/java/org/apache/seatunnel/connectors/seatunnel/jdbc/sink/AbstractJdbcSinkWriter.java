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
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.CreateTableEvent;
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
import org.apache.seatunnel.connectors.seatunnel.jdbc.utils.JdbcCatalogUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.util.Optional;

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
        if (event instanceof CreateTableEvent) {
            applyCreateTableEvent((CreateTableEvent) event);
            return;
        }
        this.tableSchema = tableSchemaChanger.reset(tableSchema).apply(event);
        reOpenOutputFormat(event);
    }

    /**
     * Exposes the resolved physical JDBC sink table so the multi-table coordinator can fan schema
     * changes out to every sibling writer that shares the same destination table.
     */
    @Override
    public Optional<String> getPhysicalSinkTableIdentifier() {
        return sinkTablePath == null ? Optional.empty() : Optional.of(sinkTablePath.getFullName());
    }

    /**
     * Creates the physical table for a runtime-discovered CDC table and refreshes the output format
     * so subsequent rows can be written immediately.
     */
    protected void applyCreateTableEvent(CreateTableEvent event) throws IOException {
        if (event.getChangeAfter() == null) {
            throw new IOException("CreateTableEvent changeAfter cannot be null");
        }
        this.tableSchema = event.getChangeAfter().getTableSchema();
        if (this.databaseTableSchema == null) {
            this.databaseTableSchema = this.tableSchema;
        }
        if (isOpen) {
            prepareCommit();
        }
        createPhysicalTableIfAbsent(event);
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

    /**
     * Uses the JDBC catalog abstraction so dialect-specific create-table SQL generation stays in
     * one place.
     */
    protected void createPhysicalTableIfAbsent(CreateTableEvent event) throws IOException {
        Optional<Catalog> catalogOptional = findCatalog();
        if (!catalogOptional.isPresent()) {
            throw new IOException(
                    String.format(
                            "JDBC sink dialect %s does not provide a catalog for runtime create-table",
                            dialect.dialectName()));
        }
        try (Catalog catalog = catalogOptional.get()) {
            catalog.open();
            catalog.createTable(
                    sinkTablePath, event.getChangeAfter(), true, jdbcSinkConfig.isCreateIndex());
        } catch (Exception e) {
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.REFRESH_PHYSICAL_TABLESCHEMA_BY_SCHEMA_CHANGE_EVENT, e);
        }
    }

    /** Finds the JDBC catalog used by runtime table-creation handling. */
    protected Optional<Catalog> findCatalog() {
        return JdbcCatalogUtils.findCatalog(jdbcSinkConfig.getJdbcConnectionConfig(), dialect);
    }
}
