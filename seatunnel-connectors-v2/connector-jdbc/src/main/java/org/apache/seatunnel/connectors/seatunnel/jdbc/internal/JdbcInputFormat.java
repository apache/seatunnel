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

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
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
    private final Map<TablePath, SeaTunnelRowType> tables;
    private final ChunkSplitter chunkSplitter;

    private transient String splitTableId;
    private transient SeaTunnelRowType splitRowType;
    private transient PreparedStatement statement;
    private transient ResultSet resultSet;
    private volatile boolean hasNext;

    public JdbcInputFormat(JdbcSourceConfig config, Map<TablePath, SeaTunnelRowType> tables) {
        this.jdbcDialect =
                JdbcDialectLoader.load(
                        config.getJdbcConnectionConfig().getUrl(), config.getCompatibleMode());
        this.chunkSplitter = ChunkSplitter.create(config);
        this.jdbcRowConverter = jdbcDialect.getRowConverter();
        this.tables = tables;
    }

    public void openInputFormat() {}

    public void closeInputFormat() throws IOException {
        close();

        if (chunkSplitter != null) {
            chunkSplitter.close();
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
            splitRowType = tables.get(inputSplit.getTablePath());
            splitTableId = inputSplit.getTablePath().toString();

            statement = chunkSplitter.generateSplitStatement(inputSplit);
            resultSet = statement.executeQuery();
            hasNext = resultSet.next();
        } catch (SQLException se) {
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.CONNECT_DATABASE_FAILED,
                    "open() failed." + se.getMessage(),
                    se);
        }
    }

    /**
     * Closes all resources used.
     *
     * @throws IOException Indicates that a resource could not be closed.
     */
    public void close() throws IOException {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                LOG.info("ResultSet couldn't be closed - " + e.getMessage());
            }
        }
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                LOG.info("Statement couldn't be closed - " + e.getMessage());
            }
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
            SeaTunnelRow seaTunnelRow = jdbcRowConverter.toInternal(resultSet, splitRowType);
            seaTunnelRow.setTableId(splitTableId);
            seaTunnelRow.setRowKind(RowKind.INSERT);

            // update hasNext after we've read the record
            hasNext = resultSet.next();
            return seaTunnelRow;
        } catch (SQLException se) {
            throw new JdbcConnectorException(
                    CommonErrorCode.SQL_OPERATION_FAILED,
                    "Couldn't read data - " + se.getMessage(),
                    se);
        } catch (NullPointerException npe) {
            throw new JdbcConnectorException(
                    CommonErrorCode.SQL_OPERATION_FAILED, "Couldn't access resultSet", npe);
        }
    }
}
