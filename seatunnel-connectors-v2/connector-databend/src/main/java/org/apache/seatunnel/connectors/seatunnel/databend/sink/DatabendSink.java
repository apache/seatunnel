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

package org.apache.seatunnel.connectors.seatunnel.databend.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.DefaultSaveModeHandler;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.databend.catalog.DatabendCatalog;
import org.apache.seatunnel.connectors.seatunnel.databend.catalog.DatabendCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.databend.config.DatabendOptions;
import org.apache.seatunnel.connectors.seatunnel.databend.config.DatabendSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.databend.config.DatabendSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.databend.exception.DatabendConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.databend.exception.DatabendConnectorException;
import org.apache.seatunnel.connectors.seatunnel.databend.util.DatabendUtil;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class DatabendSink
        implements SeaTunnelSink<SeaTunnelRow, Void, Void, Void>, SupportSaveMode {

    private final CatalogTable catalogTable;
    private final SchemaSaveMode schemaSaveMode;
    private final DataSaveMode dataSaveMode;
    private final String database;
    private final String table;
    private final String customSql;
    private final boolean autoCommit;
    private final int batchSize;
    private final int executeTimeoutSec;
    private final DatabendSinkConfig databendSinkConfig;
    private ReadonlyConfig readonlyConfig;

    public DatabendSink(CatalogTable catalogTable, ReadonlyConfig options) {
        this.catalogTable = catalogTable;
        this.databendSinkConfig = DatabendSinkConfig.of(options);
        this.schemaSaveMode = options.get(DatabendSinkOptions.SCHEMA_SAVE_MODE);
        this.dataSaveMode = options.get(DatabendSinkOptions.DATA_SAVE_MODE);
        this.customSql = options.getOptional(DatabendSinkOptions.CUSTOM_SQL).orElse(null);
        this.database =
                options.getOptional(DatabendOptions.DATABASE)
                        .orElse(catalogTable.getTableId().getDatabaseName());
        String configuredTable = options.get(DatabendOptions.TABLE);
        if (configuredTable == null || configuredTable.isEmpty()) {
            log.warn(
                    "Table name not specified in options, using table name from catalog: {}",
                    catalogTable.getTableId().getTableName());
            this.table = catalogTable.getTableId().getTableName();
        } else {
            this.table = configuredTable;
        }
        this.autoCommit = options.get(DatabendOptions.AUTO_COMMIT);
        this.batchSize = options.get(DatabendOptions.BATCH_SIZE);
        this.executeTimeoutSec = options.get(DatabendSinkOptions.EXECUTE_TIMEOUT_SEC);
        this.readonlyConfig = options;

        // detail schema log
        log.info("DatabendSink initialized with catalog table: {}", catalogTable);
        log.info("Catalog table ID: {}", catalogTable.getTableId());
        log.info("Catalog table schema: {}", catalogTable.getTableSchema());
        log.info("Catalog table row type: {}", catalogTable.getSeaTunnelRowType());
        if (catalogTable.getSeaTunnelRowType() != null) {
            log.info(
                    "Field names: {}",
                    String.join(", ", catalogTable.getSeaTunnelRowType().getFieldNames()));
            log.info(
                    "Field types: {}",
                    String.join(
                            ", ",
                            Arrays.stream(catalogTable.getSeaTunnelRowType().getFieldTypes())
                                    .map(type -> type.getSqlType().name())
                                    .collect(Collectors.toList())));
        }
        log.info("Target table path: {}.{}", database, table);
        log.info("Schema save mode: {}", schemaSaveMode);
        log.info("Data save mode: {}", dataSaveMode);
        log.info("Custom SQL: {}", customSql);
        log.info("Auto commit: {}", autoCommit);
        log.info("Batch size: {}", batchSize);
        log.info("Execute timeout: {} seconds", executeTimeoutSec);
    }

    @Override
    public String getPluginName() {
        return "Databend";
    }

    @Override
    public DatabendSinkWriter createWriter(@NonNull SinkWriter.Context context) throws IOException {
        try {
            Connection connection = DatabendUtil.createConnection(databendSinkConfig);
            connection.setAutoCommit(autoCommit);

            return new DatabendSinkWriter(
                    context,
                    connection,
                    catalogTable,
                    databendSinkConfig,
                    customSql,
                    database,
                    table,
                    batchSize,
                    executeTimeoutSec);
        } catch (SQLException e) {
            throw new DatabendConnectorException(
                    DatabendConnectorErrorCode.CONNECT_FAILED,
                    "Failed to connect to Databend: " + e.getMessage(),
                    e);
        }
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.of(catalogTable);
    }

    @Override
    public Optional<SaveModeHandler> getSaveModeHandler() {
        try {
            // create table path
            TablePath tablePath = TablePath.of(database, table);

            // create DatabendCatalog
            DatabendCatalog databendCatalog =
                    new DatabendCatalog(readonlyConfig, DatabendCatalogFactory.IDENTIFIER);

            // return SaveModeHandler
            return Optional.of(
                    new DefaultSaveModeHandler(
                            schemaSaveMode,
                            dataSaveMode,
                            databendCatalog,
                            tablePath,
                            catalogTable,
                            customSql));
        } catch (Exception e) {
            throw new DatabendConnectorException(
                    DatabendConnectorErrorCode.CONNECT_FAILED,
                    "Failed to create SaveModeHandler: " + e.getMessage(),
                    e);
        }
    }

    private boolean executeSql(Connection connection, String sql) {
        try (java.sql.Statement statement = connection.createStatement()) {
            log.info("Executing SQL: {}", sql);
            statement.execute(sql);
            return true;
        } catch (SQLException e) {
            throw new DatabendConnectorException(
                    DatabendConnectorErrorCode.SQL_OPERATION_FAILED,
                    "Failed to execute SQL: " + sql + ", error: " + e.getMessage(),
                    e);
        }
    }

    /** Convert SeaTunnel data type to Databend data type */
    private String convertToDatabendType(SeaTunnelDataType<?> dataType) {
        switch (dataType.getSqlType()) {
            case STRING:
                return "STRING";
            case BOOLEAN:
                return "BOOLEAN";
            case TINYINT:
                return "TINYINT";
            case SMALLINT:
                return "SMALLINT";
            case INT:
                return "INT";
            case BIGINT:
                return "BIGINT";
            case FLOAT:
                return "FLOAT";
            case DOUBLE:
                return "DOUBLE";
            case DECIMAL:
                return "DECIMAL";
            case BYTES:
                return "VARCHAR";
            case DATE:
                return "DATE";
            case TIME:
                return "TIMESTAMP";
            case TIMESTAMP:
                return "TIMESTAMP";
            default:
                return "STRING"; // Default to STRING for complex types
        }
    }
}
