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

package org.apache.seatunnel.connectors.seatunnel.deeplake.sink;

import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.deeplake.client.DeepLakeClient;
import org.apache.seatunnel.connectors.seatunnel.deeplake.client.DeepLakeSql;
import org.apache.seatunnel.connectors.seatunnel.deeplake.config.DeepLakeSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.deeplake.exception.DeepLakeConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.deeplake.exception.DeepLakeConnectorException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Buffers append-only rows and writes parameterized batches to a Deep Lake table. */
public class DeepLakeSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void>
        implements SupportMultiTableSinkWriter<Void> {

    private final DeepLakeClient client;
    private final SeaTunnelRowType rowType;
    private final int batchSize;
    private final String insertSql;
    private final List<List<Object>> rows;
    private boolean failed;

    public DeepLakeSinkWriter(CatalogTable catalogTable, DeepLakeSinkConfig config) {
        this.rowType = catalogTable.getSeaTunnelRowType();
        this.batchSize = config.getBatchSize();
        this.insertSql = DeepLakeSql.insertSql(config.getWorkspace(), config.getTable(), rowType);
        this.rows = new ArrayList<>(batchSize);
        this.client = new DeepLakeClient(config);

        try {
            if (config.getSchemaSaveMode() == SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST) {
                client.execute(
                        DeepLakeSql.createTableSql(
                                config.getWorkspace(), config.getTable(), catalogTable));
            } else if (config.getSchemaSaveMode() == SchemaSaveMode.ERROR_WHEN_SCHEMA_NOT_EXIST) {
                client.execute(
                        "SELECT 1 FROM "
                                + DeepLakeSql.qualifiedTable(
                                        config.getWorkspace(), config.getTable())
                                + " LIMIT 0");
            }
        } catch (RuntimeException | Error e) {
            try {
                client.close();
            } catch (IOException closeError) {
                e.addSuppressed(closeError);
            }
            throw e;
        }
    }

    @Override
    public void write(SeaTunnelRow element) {
        ensureActive();
        if (element.getRowKind() != RowKind.INSERT) {
            throw new DeepLakeConnectorException(
                    DeepLakeConnectorErrorCode.UNSUPPORTED_ROW_KIND,
                    "DeepLake sink supports append-only input, but received "
                            + element.getRowKind());
        }
        try {
            rows.add(DeepLakeRowConverter.convert(element, rowType));
            if (rows.size() >= batchSize) {
                flush();
            }
        } catch (RuntimeException | Error e) {
            failed = true;
            throw e;
        }
    }

    @Override
    public Optional<Void> prepareCommit() {
        ensureActive();
        flush();
        return Optional.empty();
    }

    void flush() {
        ensureActive();
        if (rows.isEmpty()) {
            return;
        }
        try {
            client.executeBatch(insertSql, rows);
        } catch (RuntimeException | Error e) {
            failed = true;
            throw e;
        }
        rows.clear();
    }

    private void ensureActive() {
        if (failed) {
            throw new DeepLakeConnectorException(
                    DeepLakeConnectorErrorCode.REQUEST_FAILED,
                    "DeepLake sink writer cannot continue after a failed write");
        }
    }

    int bufferedRows() {
        return rows.size();
    }

    @Override
    public void close() throws IOException {
        Throwable primary = null;
        if (!failed) {
            try {
                flush();
            } catch (Throwable t) {
                primary = t;
            }
        }
        try {
            client.close();
        } catch (Throwable t) {
            if (primary == null) {
                primary = t;
            } else {
                primary.addSuppressed(t);
            }
        }
        if (primary instanceof IOException) {
            throw (IOException) primary;
        }
        if (primary instanceof RuntimeException) {
            throw (RuntimeException) primary;
        }
        if (primary instanceof Error) {
            throw (Error) primary;
        }
        if (primary != null) {
            throw new IOException(primary);
        }
    }
}
