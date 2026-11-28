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

package org.apache.seatunnel.connectors.seatunnel.websocket.sink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.format.json.JsonSerializationSchema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

final class WebSocketSinkWriter implements SinkWriter<SeaTunnelRow, Void, Void> {
    private final WebSocketSinkConfig config;
    private final WebSocketSinkClient client;
    private final JsonSerializationSchema json;
    private final int arity;
    private boolean closed;

    WebSocketSinkWriter(WebSocketSinkConfig config, SeaTunnelRowType type) {
        this.config = config;
        client = new WebSocketSinkClient(config);
        json =
                config.format == WebSocketSinkConfig.Format.JSON
                        ? new JsonSerializationSchema(type)
                        : null;
        arity = type.getTotalFields();
    }

    @Override
    public synchronized void write(SeaTunnelRow row) throws IOException {
        if (closed) {
            throw problem("writer is closed");
        }
        try {
            client.checkFailure();
            byte[] payload = encode(row);
            if (payload.length == 0) {
                throw problem("empty TEXT or BINARY messages are unsupported");
            }
            if (payload.length > config.frameBytes) {
                throw problem("message exceeds max_frame_bytes");
            }
            client.send(payload, config.format == WebSocketSinkConfig.Format.BINARY);
        } catch (IOException e) {
            client.abort(e.getMessage());
            throw e;
        }
    }

    private byte[] encode(SeaTunnelRow row) throws IOException {
        if (row.getRowKind() != RowKind.INSERT || row.getArity() != arity) {
            throw problem("expected an INSERT row matching the configured input schema");
        }
        if (json != null) {
            FrameBuffer buffer = new FrameBuffer(config.frameBytes);
            try {
                json.getMapper().writeValue(buffer, json.convert(row));
                return buffer.bytes.toByteArray();
            } catch (IOException | RuntimeException e) {
                if (buffer.exceeded) {
                    throw problem("message exceeds max_frame_bytes");
                }
                // The format implementation can include source-row values in its exception.
                throw problem("could not serialize row as JSON");
            }
        }
        Object field = row.getField(config.fieldIndex);
        if (config.format == WebSocketSinkConfig.Format.BINARY && field instanceof byte[]) {
            return (byte[]) field;
        }
        if (config.format == WebSocketSinkConfig.Format.TEXT && field instanceof String) {
            String text = (String) field;
            if (text.length() > config.frameBytes) {
                throw problem("message exceeds max_frame_bytes");
            }
            return text.getBytes(StandardCharsets.UTF_8);
        }
        throw problem("selected field must have a non-null value matching the message format");
    }

    /** Drains the local send queue. No receiver acknowledgement is implied. */
    synchronized void flush() throws IOException {
        try {
            client.drain();
        } catch (IOException e) {
            client.abort(e.getMessage());
            throw e;
        }
    }

    @Override
    public Optional<Void> prepareCommit() throws IOException {
        flush();
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {
        client.abort("WebSocket sink: writer aborted");
    }

    @Override
    public synchronized void close() throws IOException {
        if (!closed) {
            closed = true;
            client.close();
        }
    }

    private static IOException problem(String message) {
        return new IOException("WebSocket sink: " + message);
    }

    private static final class FrameBuffer extends OutputStream {
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        private final int limit;
        private boolean exceeded;

        private FrameBuffer(int limit) {
            this.limit = limit;
        }

        @Override
        public void write(int value) throws IOException {
            reserve(1);
            bytes.write(value);
        }

        @Override
        public void write(byte[] buffer, int offset, int length) throws IOException {
            reserve(length);
            bytes.write(buffer, offset, length);
        }

        private void reserve(int length) throws IOException {
            if (length > limit - bytes.size()) {
                exceeded = true;
                throw problem("message exceeds max_frame_bytes");
            }
        }
    }
}
