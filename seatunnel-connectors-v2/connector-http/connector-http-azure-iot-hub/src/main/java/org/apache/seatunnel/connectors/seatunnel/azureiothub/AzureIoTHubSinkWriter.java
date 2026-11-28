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

package org.apache.seatunnel.connectors.seatunnel.azureiothub;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.apache.seatunnel.connectors.seatunnel.azureiothub.AzureIoTHubClient.failure;

/**
 * Admits and acknowledges one INSERT row synchronously. The same monitor protects checkpoint
 * preparation, while close can interrupt transport independently without waiting for that monitor.
 */
final class AzureIoTHubSinkWriter implements SinkWriter<SeaTunnelRow, Void, Void> {
    private final AzureIoTHubConfig config;
    private final AzureIoTHubClient client;
    private final AzureIoTHubPayload json;
    private final int arity;
    // Sticky safe failure prevents checkpoints or later rows from passing a failed delivery.
    private String failed;

    AzureIoTHubSinkWriter(AzureIoTHubConfig config, SeaTunnelRowType rowType) {
        this.config = config;
        client = new AzureIoTHubClient(config);
        json =
                config.format == AzureIoTHubConfig.Format.JSON
                        ? new AzureIoTHubPayload(rowType)
                        : null;
        arity = rowType.getTotalFields();
    }

    /**
     * Validates and acknowledges one row before releasing its input values. Any safe delivery
     * failure becomes sticky so a later checkpoint cannot turn it into a successful write.
     */
    @Override
    public synchronized void write(SeaTunnelRow row) throws IOException {
        ensureHealthy();
        try {
            byte[] payload = payload(row);
            if (payload.length > config.maxBodyBytes) {
                throw failure("payload exceeds configured or service size limit");
            }
            client.send(payload);
        } catch (IOException e) {
            failed = e.getMessage();
            client.close();
            throw e;
        }
    }

    /**
     * A checkpoint cannot pass a write waiting for acknowledgement. Successful writes leave no
     * transaction or pending batch state to commit or serialize.
     */
    @Override
    public synchronized Optional<Void> prepareCommit() throws IOException {
        ensureHealthy();
        return Optional.empty();
    }

    /**
     * Aborts local transport only; Azure cannot roll back a previously accepted telemetry event.
     */
    @Override
    public void abortPrepare() {
        client.close();
    }

    /**
     * Closes the client without acquiring the write monitor, allowing a pending I/O wait to end.
     */
    @Override
    public void close() {
        client.close();
    }

    /*
     * JSON traversal and exact output are independently bounded before network admission. TEXT
     * retains strict Unicode encoding; BINARY remains owned by this synchronous write call.
     */
    private byte[] payload(SeaTunnelRow row) throws IOException {
        if (row.getRowKind() != RowKind.INSERT || row.getArity() != arity) {
            throw failure("expected an INSERT row matching the input schema");
        }
        if (json != null) {
            try {
                return json.json(row, config.maxBodyBytes);
            } catch (IOException | RuntimeException e) {
                throw failure("could not encode input row as JSON within the payload budget");
            }
        }
        Object field = row.getField(config.fieldIndex);
        if (config.format == AzureIoTHubConfig.Format.BINARY && field instanceof byte[]) {
            return (byte[]) field;
        }
        if (config.format == AzureIoTHubConfig.Format.TEXT && field instanceof String) {
            String text = (String) field;
            if (text.length() > config.maxBodyBytes) {
                throw failure("payload exceeds configured or service size limit");
            }
            try {
                ByteBuffer encoded =
                        StandardCharsets.UTF_8.newEncoder().encode(CharBuffer.wrap(text));
                if (encoded.remaining() > config.maxBodyBytes) {
                    throw failure("payload exceeds configured or service size limit");
                }
                byte[] bytes = new byte[encoded.remaining()];
                encoded.get(bytes);
                return bytes;
            } catch (CharacterCodingException e) {
                throw failure("TEXT field is not valid Unicode");
            }
        }
        throw failure("selected field is null or does not match the configured format");
    }

    private void ensureHealthy() throws IOException {
        if (failed != null) {
            throw new IOException(failed);
        }
    }
}
