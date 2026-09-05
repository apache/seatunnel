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

package org.apache.seatunnel.connectors.seatunnel.nebulagraph.sink;

import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.nebulagraph.client.NebulaGraphClient;
import org.apache.seatunnel.connectors.seatunnel.nebulagraph.client.SessionPoolNebulaGraphClient;
import org.apache.seatunnel.connectors.seatunnel.nebulagraph.config.NebulaGraphSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.nebulagraph.config.NebulaGraphWriteMode;
import org.apache.seatunnel.connectors.seatunnel.nebulagraph.exception.NebulaGraphConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.nebulagraph.exception.NebulaGraphConnectorException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class NebulaGraphSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private final NebulaGraphSinkConfig config;
    private final NebulaGraphClient client;
    private final NebulaGraphRowConverter converter;
    private final NebulaGraphStatementBuilder statementBuilder;
    private final List<NebulaGraphVertex> buffer;

    private boolean failed;
    private boolean closed;

    public NebulaGraphSinkWriter(NebulaGraphSinkConfig config, SeaTunnelRowType rowType) {
        this(config, rowType, null);
    }

    NebulaGraphSinkWriter(
            NebulaGraphSinkConfig config, SeaTunnelRowType rowType, NebulaGraphClient client) {
        this.config = config;
        this.converter = new NebulaGraphRowConverter(config, rowType);
        this.statementBuilder =
                new NebulaGraphStatementBuilder(
                        config.getTag(), converter.getPropertyNames(), config.getWriteMode());
        this.buffer = new ArrayList<>(config.getBatchSize());
        this.client = client == null ? new SessionPoolNebulaGraphClient(config) : client;
    }

    @Override
    public void write(SeaTunnelRow row) throws IOException {
        ensureWritable();
        RowKind rowKind = row.getRowKind();
        if (rowKind == RowKind.DELETE) {
            throw unsupportedRowKind(rowKind);
        }
        if (config.getWriteMode() == NebulaGraphWriteMode.INSERT && rowKind != RowKind.INSERT) {
            throw unsupportedRowKind(rowKind);
        }
        if (rowKind == RowKind.UPDATE_BEFORE) {
            return;
        }

        buffer.add(converter.convert(row));
        if (buffer.size() >= config.getBatchSize()) {
            flush();
        }
    }

    @Override
    public Optional<Void> prepareCommit() {
        try {
            ensureWritable();
            flush();
            return Optional.empty();
        } catch (IOException e) {
            throw new NebulaGraphConnectorException(
                    NebulaGraphConnectorErrorCode.WRITE_FAILED,
                    "Failed to flush the NebulaGraph sink before commit.",
                    e);
        }
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        IOException failure = null;
        if (!failed) {
            try {
                flush();
            } catch (IOException e) {
                failure = e;
            }
        }
        try {
            client.close();
        } catch (IOException e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        } finally {
            closed = true;
        }
        if (failure != null) {
            throw failure;
        }
    }

    private void flush() throws IOException {
        if (buffer.isEmpty()) {
            return;
        }
        NebulaGraphWriteRequest request = statementBuilder.build(buffer);
        try {
            client.execute(request.getStatement(), request.getParameters());
            buffer.clear();
        } catch (IOException | RuntimeException e) {
            failed = true;
            throw new IOException(
                    "Failed to write "
                            + buffer.size()
                            + " vertices to NebulaGraph tag '"
                            + config.getTag()
                            + "'. The writer will not retry this batch during close.",
                    e);
        }
    }

    private void ensureWritable() throws IOException {
        if (closed) {
            throw new IOException("NebulaGraph sink writer is already closed.");
        }
        if (failed) {
            throw new IOException("NebulaGraph sink writer is in a failed state.");
        }
    }

    private IOException unsupportedRowKind(RowKind rowKind) {
        return new IOException(
                "Row kind "
                        + rowKind
                        + " is not supported in NebulaGraph "
                        + config.getWriteMode()
                        + " mode.");
    }
}
