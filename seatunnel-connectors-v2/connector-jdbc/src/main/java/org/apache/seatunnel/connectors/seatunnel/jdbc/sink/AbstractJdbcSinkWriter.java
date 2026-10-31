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

import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSinkWriter;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.CreateTableEvent;
import org.apache.seatunnel.api.table.schema.event.RestoreTableSchemaEvent;
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
import org.apache.seatunnel.connectors.seatunnel.jdbc.sink.savemode.JdbcSaveModeHandler;
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

    /**
     * Whether every successful batch flush should commit the connection when it uses manual commit.
     * Only the non-XA writer sets this to true, and only when checkpointing is disabled, so rebuilt
     * output formats keep the same commit boundary after schema changes.
     */
    protected boolean commitOnFlush;

    /** Resolved sink metadata, never the source event's target or options, controls runtime DDL. */
    private CatalogTable runtimeSinkTable;
    /** Runtime replay uses non-destructive schema save-mode semantics. */
    private SchemaSaveMode runtimeSchemaSaveMode;

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
     * Creates the physical table for a runtime-discovered CDC table before any rows reach the
     * already configured writer.
     */
    protected void applyCreateTableEvent(CreateTableEvent event) throws IOException {
        if (event.getChangeAfter() == null) {
            throw new IOException("CreateTableEvent changeAfter cannot be null");
        }
        if (!supportsNewlyCreatedTable() || runtimeSinkTable == null) {
            throw new IOException(
                    "JDBC writer is not configured for runtime table " + sinkTablePath);
        }
        // The factory already built the output format from resolved sink metadata. Rebuilding it
        // here would discard its commit/error-handling settings and reset later ALTERs on replay.
        createPhysicalTableIfAbsent(event);
    }

    /**
     * Pins runtime DDL to the same resolved target as row writes. Called during writer
     * construction, before the writer is registered with the multi-table coordinator.
     */
    void configureRuntimeTableCreation(CatalogTable table, SchemaSaveMode schemaSaveMode) {
        if (!sinkTablePath.equals(table.getTablePath())) {
            throw new IllegalArgumentException(
                    "Runtime DDL target must match the JDBC writer target");
        }
        this.runtimeSinkTable = table;
        this.runtimeSchemaSaveMode = schemaSaveMode;
    }

    protected void reOpenOutputFormat(SchemaChangeEvent event) throws IOException {
        this.prepareCommit();
        if (!(event instanceof RestoreTableSchemaEvent)) {
            JdbcConnectionProvider refreshTableSchemaConnectionProvider =
                    dialect.getJdbcConnectionProvider(jdbcSinkConfig.getJdbcConnectionConfig());
            try (Connection connection =
                    refreshTableSchemaConnectionProvider.getOrEstablishConnection()) {
                dialect.applySchemaChange(connection, sinkTablePath, event);
            } catch (Throwable e) {
                throw new JdbcConnectorException(
                        JdbcConnectorErrorCode.REFRESH_PHYSICAL_TABLESCHEMA_BY_SCHEMA_CHANGE_EVENT,
                        e);
            }
        } else {
            log.info(
                    "Restore runtime schema for table {} without applying physical DDL",
                    sinkTablePath);
        }

        this.outputFormat =
                new JdbcOutputFormatBuilder(
                                dialect,
                                connectionProvider,
                                jdbcSinkConfig,
                                tableSchema,
                                databaseTableSchema)
                        .commitOnFlush(commitOnFlush)
                        .build();
        this.outputFormat.open();
    }

    /**
     * Uses the JDBC catalog abstraction so dialect-specific create-table SQL generation stays in
     * one place.
     */
    protected void createPhysicalTableIfAbsent(CreateTableEvent event) throws IOException {
        if (runtimeSchemaSaveMode == SchemaSaveMode.IGNORE) {
            return;
        }
        Optional<Catalog> catalogOptional = findCatalog();
        if (!catalogOptional.isPresent()) {
            throw new IOException(
                    String.format(
                            "JDBC sink dialect %s does not provide a catalog for runtime create-table",
                            dialect.dialectName()));
        }
        try (JdbcSaveModeHandler handler =
                new JdbcSaveModeHandler(
                        runtimeSchemaSaveMode,
                        DataSaveMode.APPEND_DATA,
                        catalogOptional.get(),
                        sinkTablePath,
                        runtimeSinkTable,
                        null,
                        jdbcSinkConfig.isCreateIndex())) {
            handler.open();
            // Replicas, retries and checkpoint replay can deliver CREATE more than once. Never
            // drop/truncate an existing table or re-run custom data processing on this path.
            handler.handleSchemaSaveModeWithRestore();
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
