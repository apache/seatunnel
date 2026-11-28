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

package org.apache.seatunnel.connectors.seatunnel.solr.sink;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonGenerator;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.solr.sink.SolrSinkOptions.CommitMode;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

final class SolrSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {
    private static final int MAX_DOCUMENT_BYTES = 1024 * 1024;
    private static final int MAX_BATCH_BYTES = 4 * 1024 * 1024;
    private static final int ENVELOPE_BYTES = 32;
    private static final JsonFactory JSON = new JsonFactory();

    private final SolrClient client;
    private final SeaTunnelRowType rowType;
    private final String[] fields;
    private final int keyIndex;
    private final boolean enableDelete;
    private final int batchSize;
    private final CommitMode commitMode;
    private final List<byte[]> pending;
    private int batchBytes = ENVELOPE_BYTES;
    private boolean deleting;
    private boolean schemaVerified;
    private boolean dirty;

    SolrSinkWriter(
            SolrClient client,
            SeaTunnelRowType rowType,
            String[] fields,
            int keyIndex,
            boolean enableDelete,
            int batchSize,
            CommitMode commitMode) {
        this.client = client;
        this.rowType = rowType;
        this.fields = fields;
        this.keyIndex = keyIndex;
        this.enableDelete = enableDelete;
        this.batchSize = batchSize;
        this.commitMode = commitMode;
        this.pending = new ArrayList<>(batchSize);
    }

    /** Serialize and queue one operation, preserving the order across operation changes. */
    @Override
    public void write(SeaTunnelRow row) throws IOException {
        RowKind kind = row.getRowKind();
        boolean delete = kind == RowKind.DELETE && enableDelete;
        if (row.getArity() != fields.length
                || (!delete && kind != RowKind.INSERT && kind != RowKind.UPDATE_AFTER)) {
            throw invalid(
                    "requires matching INSERT or UPDATE_AFTER rows, or DELETE with enable_delete");
        }
        Object key = row.getField(keyIndex);
        if (key != null && !rowType.getFieldType(keyIndex).getTypeClass().isInstance(key)) {
            throw invalid("unique key value does not match its input column type");
        }
        if (key == null
                || (key instanceof String
                        && (((String) key).trim().isEmpty() || ((String) key).length() > 8192))) {
            throw invalid("unique key must be nonempty and no longer than 8192 characters");
        }
        byte[] value = encode(row, key, delete);
        if (!pending.isEmpty()
                && (deleting != delete || batchBytes + value.length + 1 > MAX_BATCH_BYTES)) {
            flushUpdates();
        }
        deleting = delete;
        pending.add(value);
        batchBytes += value.length + 1;
        if (pending.size() == batchSize) {
            flushUpdates();
        }
    }

    private byte[] encode(SeaTunnelRow row, Object key, boolean delete) throws IOException {
        DocumentOutput output = new DocumentOutput();
        try (JsonGenerator generator = JSON.createGenerator(output)) {
            if (delete) {
                writeText(generator, key.toString());
            } else {
                generator.writeStartObject();
                for (int i = 0; i < fields.length; i++) {
                    Object value = row.getField(i);
                    if (value != null) {
                        generator.writeFieldName(fields[i]);
                        writeValue(generator, rowType.getFieldType(i), value);
                    }
                }
                generator.writeEndObject();
            }
        } catch (PayloadFailure e) {
            throw e;
        } catch (IOException | RuntimeException e) {
            throw invalid("could not encode the row as a Solr document");
        }
        return output.bytes.toByteArray();
    }

    private static void writeValue(JsonGenerator out, SeaTunnelDataType<?> type, Object value)
            throws IOException {
        switch (type.getSqlType()) {
            case STRING:
                writeText(out, (String) value);
                break;
            case BOOLEAN:
                out.writeBoolean((Boolean) value);
                break;
            case TINYINT:
            case SMALLINT:
            case INT:
                out.writeNumber(((Number) value).intValue());
                break;
            case BIGINT:
                out.writeNumber(((Number) value).longValue());
                break;
            case FLOAT:
                float number = ((Number) value).floatValue();
                if (!Float.isFinite(number)) {
                    throw invalid("floating-point fields must be finite");
                }
                out.writeNumber(number);
                break;
            case DOUBLE:
                double decimal = ((Number) value).doubleValue();
                if (!Double.isFinite(decimal)) {
                    throw invalid("floating-point fields must be finite");
                }
                out.writeNumber(decimal);
                break;
            case DECIMAL:
                BigDecimal exact = (BigDecimal) value;
                if (Math.max((long) exact.precision(), exact.scale())
                                + Math.max(0L, -(long) exact.scale())
                                + 3
                        > MAX_DOCUMENT_BYTES) {
                    throw invalid("decimal field is too large");
                }
                writeText(out, exact.toPlainString());
                break;
            case DATE:
                out.writeString(
                        ((LocalDate) value).atStartOfDay(ZoneOffset.UTC).toInstant().toString());
                break;
            case TIMESTAMP:
                out.writeString(((LocalDateTime) value).toInstant(ZoneOffset.UTC).toString());
                break;
            case TIMESTAMP_TZ:
                out.writeString(((OffsetDateTime) value).toInstant().toString());
                break;
            case TIME:
                out.writeString(value.toString());
                break;
            case BYTES:
                out.writeBinary((byte[]) value);
                break;
            case ARRAY:
                SeaTunnelDataType<?> elementType = ((ArrayType<?, ?>) type).getElementType();
                out.writeStartArray();
                for (int i = 0; i < Array.getLength(value); i++) {
                    Object element = Array.get(value, i);
                    if (element != null) {
                        writeValue(out, elementType, element);
                    }
                }
                out.writeEndArray();
                break;
            default:
                throw invalid("unsupported field value");
        }
    }

    private static void writeText(JsonGenerator out, String value) throws IOException {
        if (value.length() > MAX_DOCUMENT_BYTES
                || !StandardCharsets.UTF_8.newEncoder().canEncode(value)) {
            throw invalid("field is too large or contains invalid Unicode");
        }
        out.writeString(value);
    }

    private void flushUpdates() throws IOException {
        if (pending.isEmpty()) {
            return;
        }
        if (!schemaVerified) {
            client.verifyUniqueKey(fields[keyIndex]);
            schemaVerified = true;
        }
        ByteArrayOutputStream body = new ByteArrayOutputStream(batchBytes);
        body.write((deleting ? "{\"delete\":[" : "[").getBytes(StandardCharsets.US_ASCII));
        for (int i = 0; i < pending.size(); i++) {
            if (i > 0) {
                body.write(',');
            }
            body.write(pending.get(i));
        }
        body.write(']');
        if (deleting) {
            body.write('}');
        }
        client.update(deleting ? "delete" : "index", body.toByteArray());
        pending.clear();
        batchBytes = ENVELOPE_BYTES;
        dirty = true;
    }

    /** Commit only work acknowledged by this writer and only at a successful flush boundary. */
    void flushAndCommit() throws IOException {
        flushUpdates();
        if (dirty && commitMode != CommitMode.NONE) {
            byte[] command =
                    ("{\"commit\":{\"softCommit\":"
                                    + (commitMode == CommitMode.SOFT)
                                    + ",\"waitSearcher\":true}}")
                            .getBytes(StandardCharsets.US_ASCII);
            client.update("commit", command);
        }
        dirty = false;
    }

    @Override
    public Optional<Void> prepareCommit(long checkpointId) throws IOException {
        flushAndCommit();
        return Optional.empty();
    }

    @Override
    public void close() {
        client.close();
    }

    private static PayloadFailure invalid(String reason) {
        return new PayloadFailure("Solr Sink " + reason);
    }

    private static final class PayloadFailure extends IOException {
        private static final long serialVersionUID = 1L;

        private PayloadFailure(String message) {
            super(message);
        }
    }

    private static final class DocumentOutput extends OutputStream {
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        @Override
        public void write(int value) throws IOException {
            requireCapacity(1);
            bytes.write(value);
        }

        @Override
        public void write(byte[] value, int offset, int size) throws IOException {
            requireCapacity(size);
            bytes.write(value, offset, size);
        }

        private void requireCapacity(int size) throws IOException {
            if (size > MAX_DOCUMENT_BYTES - bytes.size()) {
                throw invalid("document exceeds 1 MiB");
            }
        }
    }
}
