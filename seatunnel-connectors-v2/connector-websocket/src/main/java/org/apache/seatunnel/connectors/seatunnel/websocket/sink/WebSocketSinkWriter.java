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

/**
 * Serializes write, flush and close so JSON conversion and send admission have one producer. A
 * failure aborts the owned client; no retry or receiver acknowledgement state is created.
 */
final class WebSocketSinkWriter implements SinkWriter<SeaTunnelRow, Void, Void> {
    private final WebSocketSinkConfig config;
    private final WebSocketSinkClient client;
    private final JsonSerializationSchema json;
    private final int arity;
    // Guarded by the writer monitor; abortPrepare deliberately cancels without acquiring it.
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

    /*
     * Rejects invalid rows before lazy open and checks asynchronous failures at each write boundary.
     */
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

    /*
     * Drains only local writes and rethrows delayed failures; no receiver acknowledgement is implied.
     */
    synchronized void flush() throws IOException {
        try {
            client.drain();
        } catch (IOException e) {
            client.abort(e.getMessage());
            throw e;
        }
    }

    /*
     * Checkpoints drain local sends but return no commit info or transactional recovery guarantee.
     */
    @Override
    public Optional<Void> prepareCommit() throws IOException {
        flush();
        return Optional.empty();
    }

    /*
     * Releases an in-flight client wait without needing the writer monitor held by that wait.
     */
    @Override
    public void abortPrepare() {
        client.abort("WebSocket sink: writer aborted");
    }

    /*
     * Closes once; any drain or close-handshake failure remains visible to the engine.
     */
    @Override
    public synchronized void close() throws IOException {
        if (!closed) {
            closed = true;
            client.close();
        }
    }

    /*
     * Uses the existing JSON converter with bounded serialized output, or the selected raw typed value.
     * The client copies outgoing bytes before asynchronous ownership crosses to the event loop.
     */
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

    private static IOException problem(String message) {
        return new IOException("WebSocket sink: " + message);
    }

    /**
     * Enforces the serialized JSON limit during writes, before an oversized output array is built.
     */
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
