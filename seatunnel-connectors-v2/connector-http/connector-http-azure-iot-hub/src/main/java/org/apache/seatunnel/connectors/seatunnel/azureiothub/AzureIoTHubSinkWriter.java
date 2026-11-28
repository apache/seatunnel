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
import org.apache.seatunnel.format.json.JsonSerializationSchema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.apache.seatunnel.connectors.seatunnel.azureiothub.AzureIoTHubClient.failure;

/** Does not complete write or prepareCommit while an unacknowledged request is pending. */
final class AzureIoTHubSinkWriter implements SinkWriter<SeaTunnelRow, Void, Void> {
    private final AzureIoTHubConfig config;
    private final AzureIoTHubClient client;
    private final JsonSerializationSchema json;
    private final int arity;
    private String failed;

    AzureIoTHubSinkWriter(AzureIoTHubConfig config, SeaTunnelRowType rowType) {
        this.config = config;
        client = new AzureIoTHubClient(config);
        json =
                config.format == AzureIoTHubConfig.Format.JSON
                        ? new JsonSerializationSchema(rowType)
                        : null;
        arity = rowType.getTotalFields();
    }

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

    private byte[] payload(SeaTunnelRow row) throws IOException {
        if (row.getRowKind() != RowKind.INSERT || row.getArity() != arity) {
            throw failure("expected an INSERT row matching the input schema");
        }
        if (json != null) {
            LimitedBody body = new LimitedBody(config.maxBodyBytes);
            try {
                json.getMapper().writeValue(body, json.convert(row));
                return body.bytes.toByteArray();
            } catch (IOException | RuntimeException e) {
                if (body.exceeded) {
                    throw failure("payload exceeds configured or service size limit");
                }
                throw failure("could not encode input row as JSON");
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

    /** Successful rows are acknowledged synchronously, and the writer has no commit state. */
    @Override
    public synchronized Optional<Void> prepareCommit() throws IOException {
        ensureHealthy();
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {
        client.close();
    }

    @Override
    public void close() {
        client.close();
    }

    private void ensureHealthy() throws IOException {
        if (failed != null) {
            throw new IOException(failed);
        }
    }

    private static final class LimitedBody extends OutputStream {
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        private final int limit;
        private boolean exceeded;

        private LimitedBody(int limit) {
            this.limit = limit;
        }

        @Override
        public void write(int value) throws IOException {
            reserve(1);
            bytes.write(value);
        }

        @Override
        public void write(byte[] value, int offset, int length) throws IOException {
            reserve(length);
            bytes.write(value, offset, length);
        }

        private void reserve(int length) throws IOException {
            if (length > limit - bytes.size()) {
                exceeded = true;
                throw failure("payload exceeds configured or service size limit");
            }
        }
    }
}
