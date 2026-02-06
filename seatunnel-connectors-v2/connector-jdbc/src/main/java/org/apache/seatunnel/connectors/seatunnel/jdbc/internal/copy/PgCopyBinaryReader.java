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
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayDeque;
import java.util.Deque;

public final class PgCopyBinaryReader implements PgCopyReader {
    private static final Logger LOG = LoggerFactory.getLogger(PgCopyBinaryReader.class);
    /**
     * Per-stream metrics for PostgreSQL COPY (binary) parsing. Tracks cumulative bytes read (B),
     * rows parsed (rows), and buffer expansion count (times). Provides derived throughput metrics
     * (rows/s, bytes/s) based on elapsed time since creation.
     */
    private static final class Metrics {
        private long bytesReadTotal;
        private long rowsParsedTotal;
        private long bufferExpansionCount;
        private final long startTimeNanos = System.nanoTime();

        void addBytesRead(int n) {
            if (n > 0) bytesReadTotal += n;
        }

        void incRowsParsed() {
            rowsParsedTotal++;
        }

        void incExpansion() {
            bufferExpansionCount++;
        }

        long getElapsedMillis() {
            return (System.nanoTime() - startTimeNanos) / 1_000_000L;
        }

        double getRowsPerSecond() {
            long elapsed = System.nanoTime() - startTimeNanos;
            return elapsed <= 0 ? 0.0 : rowsParsedTotal * 1_000_000_000.0 / elapsed;
        }

        double getBytesPerSecond() {
            long elapsed = System.nanoTime() - startTimeNanos;
            return elapsed <= 0 ? 0.0 : bytesReadTotal * 1_000_000_000.0 / elapsed;
        }
    }

    private final Metrics metrics = new Metrics();
    private static final byte[] SIGNATURE = {
        'P', 'G', 'C', 'O', 'P', 'Y', '\n', (byte) 0xFF, '\r', '\n', 0
    };

    private static final LocalDate EPOCH_DATE = LocalDate.of(2000, 1, 1);
    private static final LocalDateTime EPOCH_DATETIME = LocalDateTime.of(2000, 1, 1, 0, 0);

    private static final int DEFAULT_BUFFER_SIZE = 1 * 1024 * 1024;

    private static int BUFFER_SIZE;
    private static int MAX_BUFFER_SIZE; // upper bound to prevent unbounded growth
    private ByteBuffer buffer;

    private final InputStream stream;
    private final SeaTunnelRowType rowType;
    private final SeaTunnelDataType<?>[] fieldTypes;

    // parsed rows waiting to be consumed by upper layer
    private final Deque<SeaTunnelRow> queue = new ArrayDeque<>();

    // state for an in-progress row when data spans multiple fills
    private int pendingFields = -1; // -1 means no active row
    private Object[] pendingValues; // holds field values for the active row
    private int pendingIndex = 0; // next field index to parse
    private int pendingFieldLen = -1; // current field length; -1 means length not read yet

    private boolean headerParsed = false;
    private boolean eof = false;

    /**
     * Constructs a PostgreSQL COPY (binary) reader. Initializes schema-derived row type and field
     * types, configures the parsing buffer with a power-of-two capacity based on the provided
     * pgCopyBufferSize.
     */
    public PgCopyBinaryReader(InputStream stream, TableSchema schema, Integer pgCopyBufferSize) {
        this.stream = stream;
        this.rowType = schema.toPhysicalRowDataType();
        this.fieldTypes = rowType.getFieldTypes();
        BUFFER_SIZE =
                pgCopyBufferSize == null
                        ? DEFAULT_BUFFER_SIZE
                        : 1
                                << (32
                                        - Integer.numberOfLeadingZeros(
                                                pgCopyBufferSize
                                                        - 1)); // Smallest power of two greater than
        // or equal to pgCopyBufferSize
        MAX_BUFFER_SIZE = BUFFER_SIZE * 1024;
        this.buffer = ByteBuffer.allocate(BUFFER_SIZE).order(ByteOrder.BIG_ENDIAN);
    }

    /**
     * Indicates whether more rows are available to be read. Returns true if the internal queue has
     * parsed rows or if the stream has not reached EOF.
     */
    @Override
    public boolean hasNext() {
        if (!queue.isEmpty()) {
            return true;
        }
        return !eof;
    }

    /**
     * Retrieves the next SeaTunnelRow from the COPY stream. Lazily fills the buffer and parses rows
     * until one is available or EOF is reached. Throws a JdbcConnectorException on I/O errors.
     */
    @Override
    public SeaTunnelRow next() {
        try {
            if (queue.isEmpty() && !eof) {
                fillAndParse();
                while (queue.isEmpty() && !eof) {
                    fillAndParse();
                }
            }
            return queue.poll();
        } catch (IOException e) {
            throw new JdbcConnectorException(
                    CommonErrorCodeDeprecated.SQL_OPERATION_FAILED, "Binary COPY read failed", e);
        }
    }

    /**
     * Reads more bytes into the parsing buffer and advances parsing. Parses the header once, then
     * decodes available rows into the internal queue.
     */
    private void fillAndParse() throws IOException {
        fillBufferBlocking();
        if (!headerParsed) parseHeader();

        if (headerParsed) parseRows();
    }

    /**
     * Reads from the underlying InputStream into the parsing buffer. First invocation uses clear()
     * to reset position/limit; subsequent invocations use compact() to preserve unread bytes and
     * append new data. Flips the buffer for read operations and sets EOF when the stream is
     * exhausted.
     *
     * @throws IOException if an I/O error occurs while reading into the buffer
     */
    private void fillBufferBlocking() throws IOException {
        boolean initial = buffer.position() == 0 && buffer.limit() == buffer.capacity();
        if (initial) {
            buffer.clear();
        } else {
            buffer.compact();
        }

        int pos = buffer.position();
        int len = buffer.capacity() - pos;
        int bytesRead = stream.read(buffer.array(), pos, len);
        if (bytesRead > 0) {
            buffer.position(pos + bytesRead);
            metrics.addBytesRead(bytesRead); // accumulate bytes read for throughput computation
        } else if (bytesRead == -1) {
            eof = true;
        }
        buffer.flip();
    }

    /**
     * Ensures the buffer capacity is sufficient for a single contiguous field payload. Expands the
     * buffer up to MAX_BUFFER_SIZE by doubling capacity while preserving unread bytes. Throws if
     * the required size exceeds MAX_BUFFER_SIZE.
     */
    private void ensureCapacityFor(int required) {
        if (required <= buffer.capacity()) return;
        if (required > MAX_BUFFER_SIZE) {
            throw new JdbcConnectorException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                    "COPY buffer expansion exceeds max limit: required="
                            + required
                            + ", max="
                            + MAX_BUFFER_SIZE);
        }
        int unread = buffer.remaining();
        int newCap = buffer.capacity();
        while (newCap < required && newCap < MAX_BUFFER_SIZE) newCap = newCap << 1;
        if (newCap < required) {
            throw new JdbcConnectorException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                    "Unable to expand buffer to required size: required="
                            + required
                            + ", max="
                            + MAX_BUFFER_SIZE);
        }
        ByteBuffer newBuf = ByteBuffer.allocate(newCap).order(ByteOrder.BIG_ENDIAN);
        newBuf.put(buffer.array(), buffer.position(), unread);
        newBuf.flip();
        buffer = newBuf;
        metrics.incExpansion(); // track buffer expansion count
    }

    /**
     * Parses the COPY binary header. Validates the PG signature, consumes flags and optional
     * extension area. If incomplete, defers parsing by restoring position and returning.
     */
    private void parseHeader() {
        if (buffer.remaining() < SIGNATURE.length + 8) {
            // Insufficient bytes for header; defer parsing until header is fully available
            // Return and let the upper loop refill the buffer
            return; // 11-byte signature + 4-byte flags + 4-byte extension length
        }

        int savedPos = buffer.position();

        for (byte b : SIGNATURE) {
            if (buffer.get() != b) {
                throw new JdbcConnectorException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                        "Invalid COPY header signature");
            }
        }

        buffer.getInt(); // flags
        int extLen = buffer.getInt();
        if (extLen > 0) {
            if (buffer.remaining() < extLen) {
                buffer.position(savedPos);
                return;
            }
            buffer.position(buffer.position() + extLen);
        }
        headerParsed = true;
    }

    /**
     * Incrementally parses rows from the buffer, preserving state across buffer refills. Handles
     * EOF markers, NULL fields, and variable-length payloads. Enqueues complete rows.
     */
    private void parseRows() {
        while (true) {
            // start a new row when there is no active one
            if (pendingFields < 0) {
                if (buffer.remaining() < 2) return; // need row header (short fields)
                short fields = buffer.getShort();
                if (fields == -1) { // EOF marker
                    eof = true;
                    return;
                }
                if (fields != rowType.getTotalFields()) {
                    throw new JdbcConnectorException(
                            CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                            "Column count mismatch: " + fields);
                }
                pendingFields = fields;
                pendingValues = new Object[fields];
                pendingIndex = 0;
                pendingFieldLen = -1;
            }

            // parse fields of the active row; may pause if data is incomplete
            while (pendingIndex < pendingFields) {
                // read the length prefix for the current field
                if (pendingFieldLen < 0) {
                    if (buffer.remaining() < 4) return; // need 4 bytes length
                    pendingFieldLen = buffer.getInt();
                }
                // -1 denotes NULL field
                if (pendingFieldLen == -1) {
                    pendingValues[pendingIndex++] = null;
                    pendingFieldLen = -1;
                    continue;
                }
                // expand buffer if the upcoming field payload exceeds capacity
                ensureCapacityFor(pendingFieldLen);
                // if payload not fully in buffer yet, wait for next fill
                if (buffer.remaining() < pendingFieldLen) return;

                int startPos = buffer.position();

                // Create a duplicate of the underlying buffer (shares backing array, independent
                // position/limit)
                ByteBuffer fieldBuf = buffer.duplicate().order(ByteOrder.BIG_ENDIAN);
                fieldBuf.limit(startPos + pendingFieldLen);
                fieldBuf.position(startPos);

                pendingValues[pendingIndex] =
                        PgCopyUtils.parseBinaryField(
                                fieldBuf, fieldTypes[pendingIndex], EPOCH_DATE, EPOCH_DATETIME);
                buffer.position(startPos + pendingFieldLen);
                pendingIndex++;
                pendingFieldLen = -1;
            }

            // row complete; enqueue and reset state for next row
            queue.add(new SeaTunnelRow(pendingValues));
            metrics.incRowsParsed(); // increment parsed rows counter
            pendingFields = -1;
            pendingValues = null;
            pendingIndex = 0;
            pendingFieldLen = -1;
            if (buffer.remaining() < 2) return; // need at least next row header
        }
    }

    /**
     * Helper to parse a fixed number of fields into the provided values array. Returns false if
     * insufficient bytes are available to complete parsing.
     */
    private boolean parseFields(Object[] values, int fields) {
        for (int i = 0; i < fields; i++) {
            if (buffer.remaining() < 4) return false;
            int len = buffer.getInt();
            if (len == -1) {
                values[i] = null;
                continue;
            }
            if (buffer.remaining() < len) return false;
            int startPos = buffer.position();

            ByteBuffer fieldBuf = buffer.duplicate().order(ByteOrder.BIG_ENDIAN);
            fieldBuf.limit(startPos + len);
            fieldBuf.position(startPos);
            values[i] =
                    PgCopyUtils.parseBinaryField(
                            fieldBuf, fieldTypes[i], EPOCH_DATE, EPOCH_DATETIME);
            buffer.position(startPos + len);
        }
        return true;
    }

    /**
     * Closes the underlying stream and releases internal resources. Ensures parser state is reset
     * to avoid memory retention. Propagates I/O exceptions encountered during close.
     */
    @Override
    public void close() throws IOException {
        IOException closeException = null;
        try {
            if (stream != null) {
                stream.close();
            }
        } catch (IOException e) {
            closeException = e;
        } finally {
            LOG.info(
                    "PG COPY summary: rows={} rows, bytes={} B, expansions={} times, elapsed={} ms, rows_per_second={} rows/s, bytes_per_second={} B/s",
                    metrics.rowsParsedTotal,
                    metrics.bytesReadTotal,
                    metrics.bufferExpansionCount,
                    metrics.getElapsedMillis(),
                    metrics.getRowsPerSecond(),
                    metrics.getBytesPerSecond());
            buffer = null;
            queue.clear();
            pendingValues = null;
            pendingFields = -1;
            pendingIndex = 0;
            pendingFieldLen = -1;
        }
        if (closeException != null) {
            throw new IOException("Failed to close PgCopyBinaryReader", closeException);
        }
    }
}
