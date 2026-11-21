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

package org.apache.seatunnel.connectors.seatunnel.salesforce.sink;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.salesforce.client.SalesforceClient;
import org.apache.seatunnel.connectors.seatunnel.salesforce.config.SalesforceSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.salesforce.exception.SalesforceConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.salesforce.exception.SalesforceConnectorException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Buffers detached rows and verifies every upsert result before allowing a checkpoint to advance.
 */
public final class SalesforceSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    // Exact UTF-8 size of {"allOrNone":true,"records":[]} before adding records and commas.
    private static final int ENVELOPE_BYTES = 31;
    private final SalesforceSinkConfig config;
    private final SalesforceRowSerializer serializer;
    private final SalesforceClient client;
    private final List<ObjectNode> batch = new ArrayList<>();
    private final Set<String> externalIds = new HashSet<>();
    private int batchBytes = ENVELOPE_BYTES;
    private RuntimeException failure;
    private boolean closed;

    public SalesforceSinkWriter(SeaTunnelRowType rowType, SalesforceSinkConfig config)
            throws IOException {
        this.config = config;
        this.serializer = new SalesforceRowSerializer(rowType, config);
        this.client = SalesforceClient.forSink(config.getParameters());
        try {
            client.authenticate();
        } catch (RuntimeException authenticationFailure) {
            try {
                client.close();
            } catch (IOException closeFailure) {
                authenticationFailure.addSuppressed(closeFailure);
            }
            throw authenticationFailure;
        }
    }

    @Override
    public synchronized void write(SeaTunnelRow row) throws IOException {
        checkOpen();
        try {
            if (row.getRowKind() != RowKind.INSERT && row.getRowKind() != RowKind.UPDATE_AFTER) {
                throw new IllegalArgumentException(
                        "Salesforce sink supports INSERT and UPDATE_AFTER only");
            }
            ObjectNode record = serializer.serialize(row);
            int bytes = MAPPER.writeValueAsBytes(record).length;
            if ((long) ENVELOPE_BYTES + bytes > config.getBatchMaxBytes()) {
                throw new IllegalArgumentException("A Salesforce record exceeds batch_max_bytes");
            }
            String key = serializer.externalIdKey(record);
            if (!batch.isEmpty()
                    && (externalIds.contains(key)
                            || (long) batchBytes + 1 + bytes > config.getBatchMaxBytes())) {
                flush();
            }
            batchBytes += bytes + (batch.isEmpty() ? 0 : 1);
            batch.add(record);
            externalIds.add(key);
            if (batch.size() >= config.getBatchSize()) {
                flush();
            }
        } catch (RuntimeException e) {
            failure = e;
            throw e;
        } catch (IOException e) {
            failure =
                    new SalesforceConnectorException(
                            SalesforceConnectorErrorCode.WRITE_FAILED,
                            "Could not serialize a Salesforce record");
            throw failure;
        }
    }

    /** A checkpoint can advance only after all buffered records have succeeded remotely. */
    @Override
    public synchronized Optional<Void> prepareCommit() {
        flush();
        return Optional.empty();
    }

    /** Flush on the writer/engine thread; no connector-owned timer or executor is created. */
    public synchronized void flush() {
        checkOpen();
        if (batch.isEmpty()) {
            return;
        }
        try {
            client.upsert(
                    config.getObjectName(),
                    config.getExternalIdField(),
                    batch,
                    config.getMaxRetries(),
                    config.getRetryIntervalMs());
            batch.clear();
            externalIds.clear();
            batchBytes = ENVELOPE_BYTES;
        } catch (RuntimeException e) {
            failure = e;
            throw e;
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        RuntimeException flushFailure = null;
        try {
            // A failed batch must not be silently retried during abort/cleanup.
            if (failure == null) {
                flush();
            }
        } catch (RuntimeException e) {
            flushFailure = e;
            throw e;
        } finally {
            closed = true;
            try {
                client.close();
            } catch (IOException closeFailure) {
                if (flushFailure != null) {
                    flushFailure.addSuppressed(closeFailure);
                } else {
                    throw closeFailure;
                }
            }
        }
    }

    private void checkOpen() {
        if (failure != null) {
            throw failure;
        }
        if (closed) {
            throw new IllegalStateException("Salesforce sink writer is closed");
        }
    }
}
