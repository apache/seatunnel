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

package org.apache.seatunnel.connectors.seatunnel.astra;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.astra.AstraConfig.Mode;
import org.apache.seatunnel.format.json.JsonSerializationSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Buffers bounded input in order and acknowledges the Astra document writes at checkpoint
 * boundaries. External writes are visible before checkpoints and cannot be rolled back.
 */
final class AstraWriter implements SinkWriter<SeaTunnelRow, Void, AstraSink.State> {
    private static final int FLUSH_CHUNKS = 100;
    private static final int FLUSH_RECORDS = 100;
    private final AstraConfig config;
    private final String run;
    private final AstraHttp http;
    private final AstraDataApi data;
    private final AstraEmbeddings embeddings;
    private final AstraDocument documents;
    private final JsonSerializationSchema serializer;
    private final List<AstraDocument.Record> pending = new ArrayList<>();
    private long position;
    private long pendingBytes;
    private int pendingChunks;
    private boolean collectionReady;
    private boolean overwriteDone;
    private boolean closed;

    AstraWriter(AstraConfig config, SeaTunnelRowType row, AstraSink.State state) {
        this.config = config;
        this.run = state.run;
        this.position = state.position;
        this.overwriteDone = state.overwriteDone;
        this.serializer = new JsonSerializationSchema(row);
        this.http = new AstraHttp(config);
        this.data = new AstraDataApi(config, http);
        this.embeddings = new AstraEmbeddings(config, http);
        this.documents = new AstraDocument(config, new AstraSplitter(config, http));
    }

    /**
     * Captures the exact routing, text and metadata before adding a row to the bounded buffer.
     * Deletes and before-images affect only the deduplicated write mode.
     */
    @Override
    public synchronized void write(SeaTunnelRow row) throws IOException {
        ensureActive();
        boolean deletion =
                row.getRowKind() == RowKind.DELETE || row.getRowKind() == RowKind.UPDATE_BEFORE;
        if (deletion && config.mode != Mode.APPEND_DEDUP) {
            return;
        }
        try {
            byte[] serialized;
            try {
                serialized = serializer.serialize(row);
            } catch (RuntimeException ignored) {
                throw new IOException("Astra sink: cannot serialize the input row");
            }
            if (serialized.length > config.maxRow) {
                throw new IOException("Astra sink: input exceeds max_row_bytes");
            }
            if (position == Long.MAX_VALUE) {
                throw new IOException("Astra sink: input sequence exhausted");
            }
            AstraDocument.Record record =
                    documents.prepare(AstraJson.readObject(serialized), deletion, position);
            if (!pending.isEmpty()
                    && (pendingBytes + record.bytes > config.maxBuffer
                            || pendingChunks + record.chunks.size() > FLUSH_CHUNKS)) {
                flush();
            }
            pending.add(record);
            pendingBytes += record.bytes;
            pendingChunks += record.chunks.size();
            position++;
            if (pending.size() >= FLUSH_RECORDS || pendingChunks >= FLUSH_CHUNKS) {
                flush();
            }
        } catch (IllegalArgumentException | IOException error) {
            abortPrepare();
            if (error instanceof IOException) {
                throw (IOException) error;
            }
            throw new IOException(error.getMessage());
        }
    }

    /**
     * Validates all outgoing payloads before cleanup. Repeated dedup keys are written in input
     * order. The connector never deletes every document in a collection or resends an unknown
     * mutation here.
     */
    synchronized void flush() throws IOException {
        ensureActive();
        if (pending.isEmpty()
                && collectionReady
                && (config.mode != Mode.OVERWRITE || overwriteDone)) {
            return;
        }
        http.beginFlush();
        try {
            List<AstraDocument.Chunk> chunks = new ArrayList<>(pendingChunks);
            for (AstraDocument.Record record : pending) {
                chunks.addAll(record.chunks);
            }
            List<ArrayNode> vectors = embeddings.embed(chunks);
            List<List<ObjectNode>> outgoing = new ArrayList<>(pending.size());
            int offset = 0;
            for (AstraDocument.Record record : pending) {
                List<ObjectNode> serialized = new ArrayList<>(record.chunks.size());
                for (int i = 0; i < record.chunks.size(); i++) {
                    ObjectNode document = documents.document(record, run, i, vectors.get(offset++));
                    data.validateSize(document);
                    serialized.add(document);
                }
                outgoing.add(serialized);
            }
            if (!collectionReady) {
                data.ensureCollection();
                collectionReady = true;
            }
            if (config.mode == Mode.OVERWRITE && !overwriteDone) {
                data.delete(documents.fixedScope().key, null);
                overwriteDone = true;
            }
            if (config.mode == Mode.APPEND_DEDUP) {
                for (int i = 0; i < pending.size(); i++) {
                    AstraDocument.Record record = pending.get(i);
                    data.delete(record.scope.key, record.id);
                    data.insert(outgoing.get(i));
                }
            } else {
                List<ObjectNode> all = new ArrayList<>(pendingChunks);
                outgoing.forEach(all::addAll);
                data.insert(all);
            }
            clear();
        } catch (IOException error) {
            abortPrepare();
            throw error;
        } finally {
            http.endFlush();
        }
    }

    @Override
    public synchronized Optional<Void> prepareCommit() throws IOException {
        flush();
        return Optional.empty();
    }

    /**
     * Stores completed progress and the overwrite fence without rows, vectors or credentials. A
     * barrier can only store progress after every buffered mutation has been acknowledged.
     */
    @Override
    public synchronized List<AstraSink.State> snapshotState(long checkpointId) throws IOException {
        flush();
        return List.of(new AstraSink.State(config.fingerprint, run, position, overwriteDone));
    }

    @Override
    public void abortPrepare() {
        close();
    }

    /**
     * Discards pending rows on abort or close and releases the socket independently of writer
     * state. A successful job uses its final prepare barrier to flush before closing.
     */
    @Override
    public void close() {
        http.close();
        synchronized (this) {
            closed = true;
            clear();
        }
    }

    private void clear() {
        pending.clear();
        pendingBytes = 0;
        pendingChunks = 0;
    }

    private void ensureActive() throws IOException {
        if (closed) {
            throw new IOException("Astra sink: writer is closed");
        }
    }
}
