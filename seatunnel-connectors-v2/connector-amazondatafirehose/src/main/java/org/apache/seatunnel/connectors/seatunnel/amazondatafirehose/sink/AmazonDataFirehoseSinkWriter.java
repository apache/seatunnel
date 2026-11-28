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

package org.apache.seatunnel.connectors.seatunnel.amazondatafirehose.sink;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonToken;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;

import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.amazondatafirehose.sink.AmazonDataFirehoseSinkOptions.Format;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.firehose.model.Record;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Buffers one INSERT row per Firehose record on the engine-owned writer thread. Only complete
 * acknowledgements clear the buffer; close aborts resources without sending it.
 */
final class AmazonDataFirehoseSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {
    static final int REQUEST_MAX_BYTES = 4 * 1024 * 1024;
    // Covers the JSON object, the maximum-length stream name, and the records array delimiters.
    private static final int REQUEST_OVERHEAD_BYTES = 128;
    // Opaque keys can be long within each bounded record. Shared symbol tables must not retain
    // their aggregate bytes across records after acknowledged batches have already been released.
    private static final JsonFactory JSON =
            JsonFactory.builder()
                    .disable(JsonFactory.Feature.CANONICALIZE_FIELD_NAMES)
                    .disable(JsonFactory.Feature.INTERN_FIELD_NAMES)
                    .streamReadConstraints(
                            StreamReadConstraints.builder().maxNestingDepth(64).build())
                    .build()
                    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);

    private final FirehoseBatchClient client;
    private final Format format;
    private final FirehoseRowSerializationSchema serializer;
    private final int arity;
    private final int batchSize;
    private final int maxRecordBytes;
    // Writer-thread owned; the client receives an immutable list snapshot, not this buffer.
    private final List<Record> pending;
    // Conservative encoded-wire estimate, reset only after the entire pending batch succeeds.
    private int estimatedRequestBytes = REQUEST_OVERHEAD_BYTES;

    AmazonDataFirehoseSinkWriter(
            FirehoseBatchClient client,
            Format format,
            FirehoseRowSerializationSchema serializer,
            int arity,
            int batchSize,
            int maxRecordBytes) {
        this.client = client;
        this.format = format;
        this.serializer = serializer;
        this.arity = arity;
        this.batchSize = batchSize;
        this.maxRecordBytes = maxRecordBytes;
        this.pending = new ArrayList<>(batchSize);
    }

    /**
     * Buffer a validated record and flush before count or encoded-request bounds can be exceeded. A
     * failed flush retains the pending window for task failure/recovery rather than silently
     * dropping it.
     */
    @Override
    public void write(SeaTunnelRow row) throws IOException {
        if (row.getRowKind() != RowKind.INSERT || row.getArity() != arity) {
            throw invalid("requires INSERT rows that match the input schema");
        }
        byte[] payload = serializer == null ? encode(row.getField(0)) : serializeRow(row);
        // Upper-bound base64 length plus quotes, field name, braces, and an array separator.
        int recordBytes = 4 * ((payload.length + 2) / 3) + 16;
        if (estimatedRequestBytes + recordBytes > REQUEST_MAX_BYTES) {
            flush();
        }
        pending.add(Record.builder().data(SdkBytes.fromByteArray(payload)).build());
        estimatedRequestBytes += recordBytes;
        if (pending.size() == batchSize) {
            flush();
        }
    }

    /**
     * Flush through the writer thread before commit or on an engine flush signal. Retain the full
     * pending window if sending fails; accepted records may repeat on engine recovery.
     */
    void flush() throws IOException {
        if (!pending.isEmpty()) {
            client.put(pending);
            pending.clear();
            estimatedRequestBytes = REQUEST_OVERHEAD_BYTES;
        }
    }

    /*
     * A checkpoint/final commit can acknowledge source progress only after buffered records flush.
     * No commit handle or external rollback is available for Firehose acceptance.
     */
    @Override
    public Optional<Void> prepareCommit(long checkpointId) throws IOException {
        flush();
        return Optional.empty();
    }

    /*
     * Abort abandoned work rather than flushing from teardown, including failed or cancelled jobs.
     */
    @Override
    public void close() {
        client.close();
    }

    /*
     * Preserve RAW bytes and the original JSON_LINES representation, including numeric precision.
     * Strict parsing validates the latter without reserializing it or echoing rejected payloads.
     */
    private byte[] encode(Object value) throws IOException {
        if (format == Format.RAW && value instanceof byte[]) {
            return requireRecordSize((byte[]) value);
        }
        if (!(value instanceof String)
                || ((String) value).isEmpty()
                || ((String) value).length() > maxRecordBytes) {
            throw invalid("requires a nonempty payload within max_record_bytes");
        }
        String text = (String) value;
        if (!StandardCharsets.UTF_8.newEncoder().canEncode(text)) {
            throw invalid("payload is not valid UTF-8");
        }
        byte[] encoded = requireRecordSize(text.getBytes(StandardCharsets.UTF_8));
        if (format == Format.RAW) {
            return encoded;
        }
        if (encoded.length == maxRecordBytes) {
            throw invalid("payload and delimiter exceed max_record_bytes");
        }
        if (text.indexOf('\n') >= 0 || text.indexOf('\r') >= 0) {
            throw invalid("JSON_LINES payload must contain one JSON object on one line");
        }
        try (JsonParser parser = JSON.createParser(text)) {
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                throw invalid("JSON_LINES payload must be a single valid JSON object");
            }
            parser.skipChildren();
            if (parser.nextToken() != null) {
                throw invalid("JSON_LINES payload must be a single valid JSON object");
            }
        } catch (IOException e) {
            throw invalid("JSON_LINES payload must be a single valid JSON object with unique keys");
        }
        return appendDelimiter(encoded);
    }

    /*
     * Reject obvious top-level scalar excess early, then apply the complete typed native work guard.
     * Its iterative JSON path and preflighted CSV path sanitize errors without quoting row values.
     */
    private byte[] serializeRow(SeaTunnelRow row) throws IOException {
        long minimumPayloadBytes = 0;
        for (Object value : row.getFields()) {
            if (value instanceof String) {
                String field = (String) value;
                minimumPayloadBytes += field.length();
                if (field.length() < maxRecordBytes
                        && !StandardCharsets.UTF_8.newEncoder().canEncode(field)) {
                    throw invalid("field is not valid UTF-8");
                }
            } else if (value instanceof byte[]) {
                minimumPayloadBytes += ((byte[]) value).length;
            }
            if (minimumPayloadBytes >= maxRecordBytes) {
                throw invalid("fields and delimiter exceed max_record_bytes");
            }
        }
        byte[] bytes;
        try {
            bytes = serializer.serialize(row);
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw invalid("could not serialize the Firehose record");
        }
        return appendDelimiter(bytes);
    }

    /*
     * Count the LF in the record limit and copy rather than mutating caller-owned encoded payload bytes.
     */
    private byte[] appendDelimiter(byte[] bytes) throws IOException {
        if (bytes.length >= maxRecordBytes) {
            throw invalid("payload and delimiter exceed max_record_bytes");
        }
        byte[] line = Arrays.copyOf(bytes, bytes.length + 1);
        line[line.length - 1] = '\n';
        return line;
    }

    private byte[] requireRecordSize(byte[] bytes) throws IOException {
        if (bytes.length == 0 || bytes.length > maxRecordBytes) {
            throw invalid("requires a nonempty payload within max_record_bytes");
        }
        return bytes;
    }

    private static IOException invalid(String message) {
        return new IOException("Amazon Data Firehose Sink " + message);
    }
}
