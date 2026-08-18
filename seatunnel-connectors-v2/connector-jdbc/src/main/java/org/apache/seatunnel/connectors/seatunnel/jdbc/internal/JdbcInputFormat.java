/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectLoader;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.ChunkSplitter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceSplit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 * InputFormat to read data from a database and generate Rows. The InputFormat has to be configured
 * using the supplied InputFormatBuilder. A valid RowTypeInfo must be properly configured in the
 * builder
 */
public class JdbcInputFormat implements Serializable {

    private static final long serialVersionUID = 2L;
    private static final Logger LOG = LoggerFactory.getLogger(JdbcInputFormat.class);

    private final JdbcDialect jdbcDialect;
    private final JdbcRowConverter jdbcRowConverter;
    private final Map<TablePath, CatalogTable> tables;
    private final ChunkSplitter chunkSplitter;
    private final boolean configuredAutoCommit;

    private transient String splitTableId;
    private transient TableSchema splitTableSchema;
    private transient PreparedStatement statement;
    private transient ResultSet resultSet;
    private volatile boolean hasNext;

    public JdbcInputFormat(JdbcSourceConfig config, Map<TablePath, CatalogTable> tables) {
        this(
                JdbcDialectLoader.load(
                        config.getJdbcConnectionConfig().getUrl(),
                        config.getJdbcConnectionConfig().getDialect(),
                        config.getCompatibleMode()),
                ChunkSplitter.create(config),
                tables,
                config.getJdbcConnectionConfig().isAutoCommit());
    }

    JdbcInputFormat(
            JdbcDialect jdbcDialect,
            ChunkSplitter chunkSplitter,
            Map<TablePath, CatalogTable> tables,
            boolean configuredAutoCommit) {
        this.jdbcDialect = jdbcDialect;
        this.chunkSplitter = chunkSplitter;
        this.jdbcRowConverter = jdbcDialect.getRowConverter();
        this.tables = tables;
        this.configuredAutoCommit = configuredAutoCommit;
    }

    public void openInputFormat() {}

    public void closeInputFormat() throws IOException {
        try {
            close();
        } finally {
            if (chunkSplitter != null) {
                chunkSplitter.close();
            }
        }
    }

    /**
     * Connects to the source database and executes the query
     *
     * @param inputSplit which is ignored if this InputFormat is executed as a non-parallel source,
     *     a "hook" to the query parameters otherwise (using its <i>parameterId</i>)
     * @throws IOException if there's an error during the execution of the query
     */
    public void open(JdbcSourceSplit inputSplit) throws IOException {
        try {
            splitTableSchema = tables.get(inputSplit.getTablePath()).getTableSchema();
            splitTableId = inputSplit.getTablePath().toString();

            statement = chunkSplitter.generateSplitStatement(inputSplit, splitTableSchema);
            resultSet = statement.executeQuery();
            hasNext = resultSet.next();
        } catch (SQLException se) {
            cleanupAfterOpenFailure(se);
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.CONNECT_DATABASE_FAILED,
                    "open() failed." + se.getMessage(),
                    se);
        } catch (RuntimeException runtimeException) {
            cleanupAfterOpenFailure(runtimeException);
            throw runtimeException;
        }
    }

    private void cleanupAfterOpenFailure(Throwable openException) {
        boolean shouldDiscardConnection = statement == null;
        try {
            close();
        } catch (IOException cleanupException) {
            openException.addSuppressed(cleanupException);
            shouldDiscardConnection = true;
        } finally {
            if (shouldDiscardConnection) {
                // Statement creation may establish or mutate the cached connection before failing
                // without returning a statement. Discard it because close() cannot identify and
                // finish that transaction safely.
                chunkSplitter.close();
            }
        }
    }

    /**
     * Closes all resources used.
     *
     * @throws IOException Indicates that a resource could not be closed.
     */
    public void close() throws IOException {
        Connection connection = getStatementConnection();
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                LOG.info("ResultSet couldn't be closed - " + e.getMessage());
            } finally {
                resultSet = null;
            }
        }
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                LOG.info("Statement couldn't be closed - " + e.getMessage());
            } finally {
                statement = null;
            }
        }

        hasNext = false;
        splitTableSchema = null;
        splitTableId = null;
        finishReadTransaction(connection);
    }

    private Connection getStatementConnection() {
        if (statement == null) {
            return null;
        }
        try {
            Connection connection = statement.getConnection();
            if (connection == null) {
                LOG.warn(
                        "The JDBC source statement returned no connection. "
                                + "Closing the cached connection to avoid reusing an unknown "
                                + "transaction.");
                chunkSplitter.close();
            }
            return connection;
        } catch (SQLException e) {
            LOG.warn(
                    "Failed to get the JDBC source connection from the current statement. "
                            + "Closing the cached connection to avoid reusing an unknown "
                            + "transaction.",
                    e);
            chunkSplitter.close();
            return null;
        }
    }

    private void finishReadTransaction(Connection connection) {
        try {
            finishReadTransaction(connection, configuredAutoCommit);
        } catch (SQLException e) {
            LOG.warn(
                    "Failed to finish the JDBC source read transaction. "
                            + "Closing the connection to avoid leaving or reusing an idle "
                            + "transaction.",
                    e);
            discardConnection(connection, e);
        }
    }

    private void discardConnection(Connection connection, SQLException cleanupException) {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException closeException) {
            cleanupException.addSuppressed(closeException);
            LOG.warn(
                    "Failed to close the JDBC source connection after transaction cleanup failed.",
                    cleanupException);
        } finally {
            // Clear the provider's cached reference and retry close for drivers whose first close
            // attempt failed.
            chunkSplitter.close();
        }
    }

    static void finishReadTransaction(Connection connection, boolean configuredAutoCommit)
            throws SQLException {
        if (connection == null || connection.isClosed()) {
            return;
        }

        boolean currentAutoCommit = connection.getAutoCommit();
        if (!currentAutoCommit) {
            // JDBC source reads do not have changes to commit. Rollback ends the server-side cursor
            // transaction, releases its snapshot, and also recovers a transaction in failed state.
            connection.rollback();
        }
        if (currentAutoCommit != configuredAutoCommit) {
            connection.setAutoCommit(configuredAutoCommit);
        }
    }

    /**
     * Checks whether all data has been read.
     *
     * @return boolean value indication whether all data has been read.
     */
    public boolean reachedEnd() {
        return !hasNext;
    }

    /** Convert a row of data to seatunnelRow */
    public SeaTunnelRow nextRecord() {
        try {
            if (!hasNext) {
                return null;
            }
            SeaTunnelRow seaTunnelRow = jdbcRowConverter.toInternal(resultSet, splitTableSchema);
            seaTunnelRow.setTableId(splitTableId);
            seaTunnelRow.setRowKind(RowKind.INSERT);

            // update hasNext after we've read the record
            hasNext = resultSet.next();
            return seaTunnelRow;
        } catch (SQLException se) {
            throw new JdbcConnectorException(
                    CommonErrorCodeDeprecated.SQL_OPERATION_FAILED,
                    String.format(
                            "Failed to read data from table '%s': %s",
                            splitTableId, se.getMessage()),
                    se);
        } catch (NullPointerException npe) {
            throw new JdbcConnectorException(
                    CommonErrorCodeDeprecated.SQL_OPERATION_FAILED,
                    String.format(
                            "Failed to access resultSet for table '%s': NullPointerException occurred",
                            splitTableId),
                    npe);
        }
    }
}
