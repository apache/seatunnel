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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.copy;

import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.CopyManagerProxy;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.ChunkSplitter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceSplit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/** Unified entry for PostgreSQL COPY input (CSV or BINARY). */
public final class PgCopyInput implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(PgCopyInput.class);

    private final JdbcSourceConfig config;
    private final JdbcDialect dialect;
    private final ChunkSplitter chunkSplitter;
    private final TableSchema tableSchema;
    private final String tableId;

    private final boolean useBinary;

    private boolean hasNext;

    private transient CopyManagerProxy copyManagerProxy;
    private transient InputStream copyStream;
    private transient PgCopyReader reader;

    public PgCopyInput(
            JdbcSourceConfig config,
            JdbcDialect dialect,
            ChunkSplitter chunkSplitter,
            TableSchema tableSchema,
            String tableId) {
        this.config = config;
        this.dialect = dialect;
        this.chunkSplitter = chunkSplitter;
        this.tableSchema = tableSchema;
        this.tableId = tableId;
        this.useBinary = config.isBinary();
    }

    /** Open a COPY stream for a given split. */
    public void open(JdbcSourceSplit split) {
        try {
            String selectSql = chunkSplitter.generateSplitQuerySQL(split, tableSchema);
            String copySql =
                    String.format(
                            "COPY (%s) TO STDOUT WITH %s", selectSql, useBinary ? "BINARY" : "CSV");

            Connection conn = getConnection();
            LOG.info("Open PG COPY split={}, sql={}", split.splitId(), copySql);

            copyManagerProxy = new CopyManagerProxy(conn);
            copyStream = copyManagerProxy.copyOutAsStream(copySql);

            reader = createReader(copyStream);
            hasNext = reader.hasNext();
        } catch (Exception e) {
            throw new JdbcConnectorException(
                    CommonErrorCodeDeprecated.SQL_OPERATION_FAILED,
                    "Failed to open PG COPY stream: " + e.getMessage(),
                    e);
        }
    }

    private Connection getConnection() throws SQLException, ClassNotFoundException {
        JdbcConnectionProvider provider =
                dialect.getJdbcConnectionProvider(config.getJdbcConnectionConfig());
        return provider.getOrEstablishConnection();
    }

    private PgCopyReader createReader(InputStream stream) throws Exception {
        if (useBinary) {
            return new PgCopyBinaryReader(stream, tableSchema, config.getPgCopyBufferSize());
        } else {
            return new PgCopyCsvReader(stream, tableSchema);
        }
    }

    public boolean hasNext() {
        return hasNext;
    }

    public SeaTunnelRow next() {
        if (reader == null) {
            throw new JdbcConnectorException(
                    CommonErrorCodeDeprecated.SQL_OPERATION_FAILED,
                    "COPY reader not initialized. Did you call open()?");
        }
        if (!hasNext) {
            throw new JdbcConnectorException(
                    CommonErrorCodeDeprecated.SQL_OPERATION_FAILED,
                    "No more data available in PG COPY stream");
        }

        SeaTunnelRow row = reader.next();
        if (row == null) {
            hasNext = false;
            throw new JdbcConnectorException(
                    CommonErrorCodeDeprecated.SQL_OPERATION_FAILED,
                    "Unexpected end of PG COPY stream");
        }
        hasNext = reader.hasNext();
        return row;
    }

    @Override
    public void close() {
        List<Object> resources = Arrays.asList(reader, copyStream, copyManagerProxy);
        for (Object r : resources) {
            PgCopyUtils.closeQuietly(r);
        }
    }
}
