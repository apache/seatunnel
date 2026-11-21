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

package org.apache.seatunnel.connectors.seatunnel.jdbc.sink.savemode;

import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * Save-mode handler for JDBC sink when writing through custom {@code query} SQL.
 *
 * <p>The catalog-based save-mode path is unavailable in query mode ({@code JdbcSink#getCatalog()}
 * returns empty when {@code simpleSql} is set). This handler still runs {@code custom_sql} once at
 * the save-mode stage by executing it directly over a JDBC connection.
 *
 * <p>Schema save modes are intentionally no-ops here: query mode does not resolve a catalog table
 * for DDL. Other data save modes remain unsupported on the query path.
 */
@Slf4j
public class JdbcQueryCustomSqlSaveModeHandler implements SaveModeHandler {

    private final SchemaSaveMode schemaSaveMode;
    private final DataSaveMode dataSaveMode;
    private final TablePath tablePath;
    private final String customSql;
    private final JdbcConnectionProvider connectionProvider;

    public JdbcQueryCustomSqlSaveModeHandler(
            SchemaSaveMode schemaSaveMode,
            DataSaveMode dataSaveMode,
            TablePath tablePath,
            String customSql,
            JdbcConnectionProvider connectionProvider) {
        this.schemaSaveMode = schemaSaveMode;
        this.dataSaveMode = dataSaveMode;
        this.tablePath = tablePath;
        this.customSql = customSql;
        this.connectionProvider = connectionProvider;
    }

    @Override
    public void open() {
        try {
            connectionProvider.getOrEstablishConnection();
        } catch (SQLException | ClassNotFoundException e) {
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.CONNECT_DATABASE_FAILED,
                    "Failed to open JDBC connection for custom_sql execution",
                    e);
        }
    }

    /**
     * Query mode cannot manage schema through catalog. Schema save mode is skipped; a warning is
     * logged when a non-IGNORE mode was configured so users are not surprised.
     */
    @Override
    public void handleSchemaSaveMode() {
        if (schemaSaveMode != SchemaSaveMode.IGNORE) {
            log.warn(
                    "JDBC sink query mode does not apply schema_save_mode={}. "
                            + "Only custom_sql under CUSTOM_PROCESSING is executed on this path.",
                    schemaSaveMode);
        }
    }

    /**
     * Executes {@code custom_sql} exactly once when {@link DataSaveMode#CUSTOM_PROCESSING} is
     * configured. Invoked at the save-mode stage before writers start, not per batch or record.
     */
    @Override
    public void handleDataSaveMode() {
        if (dataSaveMode != DataSaveMode.CUSTOM_PROCESSING) {
            return;
        }
        log.info("Executing custom SQL in JDBC query mode: {}", customSql);
        try {
            Connection connection = connectionProvider.getOrEstablishConnection();
            try (PreparedStatement statement = connection.prepareStatement(customSql)) {
                statement.execute();
            }
            if (!connection.getAutoCommit()) {
                connection.commit();
            }
        } catch (SQLException | ClassNotFoundException e) {
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.CONNECT_DATABASE_FAILED,
                    String.format("Failed to execute custom_sql: %s", customSql),
                    e);
        }
    }

    @Override
    public void handleSchemaSaveModeWithRestore() {
        // Restore must not re-run schema/data mutations; query mode has no schema handling.
    }

    @Override
    public SchemaSaveMode getSchemaSaveMode() {
        return schemaSaveMode;
    }

    @Override
    public DataSaveMode getDataSaveMode() {
        return dataSaveMode;
    }

    @Override
    public TablePath getHandleTablePath() {
        return tablePath;
    }

    @Override
    public Catalog getHandleCatalog() {
        // Query mode has no catalog; SaveModeExecuteWrapper only reads name() for logging.
        return new JdbcQueryNamedCatalog();
    }

    @Override
    public void close() {
        connectionProvider.closeConnection();
    }

    /**
     * Minimal {@link Catalog} stub so {@code SaveModeExecuteWrapper} can log catalog name. All
     * other catalog operations are unsupported and never invoked on this path.
     */
    private static final class JdbcQueryNamedCatalog implements Catalog {

        @Override
        public String name() {
            return "JdbcQuery";
        }

        @Override
        public void open() {}

        @Override
        public void close() {}

        @Override
        public String getDefaultDatabase() {
            throw unsupported();
        }

        @Override
        public boolean databaseExists(String databaseName) {
            throw unsupported();
        }

        @Override
        public List<String> listDatabases() {
            throw unsupported();
        }

        @Override
        public List<String> listTables(String databaseName) {
            throw unsupported();
        }

        @Override
        public boolean tableExists(TablePath tablePath) {
            throw unsupported();
        }

        @Override
        public CatalogTable getTable(TablePath tablePath) {
            throw unsupported();
        }

        @Override
        public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists) {
            throw unsupported();
        }

        @Override
        public void dropTable(TablePath tablePath, boolean ignoreIfNotExists) {
            throw unsupported();
        }

        @Override
        public void createDatabase(TablePath tablePath, boolean ignoreIfExists) {
            throw unsupported();
        }

        @Override
        public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists) {
            throw unsupported();
        }

        private static UnsupportedOperationException unsupported() {
            return new UnsupportedOperationException(
                    "JDBC query mode does not provide a catalog; "
                            + "custom_sql is executed directly via JDBC connection.");
        }
    }
}
