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

package org.apache.seatunnel.connectors.seatunnel.couchbase.sink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.couchbase.exception.CouchbaseConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.couchbase.exception.CouchbaseConnectorException;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Writes {@link SeaTunnelRow} records to a Couchbase collection.
 *
 * <p>Records are buffered in memory and flushed to Couchbase when either:
 *
 * <ul>
 *   <li>The buffer reaches {@code buffer-flush.max-rows}, or
 *   <li>The time since the last flush exceeds {@code buffer-flush.interval}.
 * </ul>
 *
 * <p>Each record is converted to a {@link JsonObject}. The document key is derived from the
 * configured {@code primary-key} fields (joined with {@code _}); when no primary key is set a
 * random UUID is used. Upsert mode is enabled via {@code upsert-enable}.
 *
 * <p>Supported row kinds: {@code INSERT}, {@code UPDATE_AFTER}. {@code UPDATE_BEFORE} is silently
 * skipped. {@code DELETE} is explicitly rejected with an exception — CDC delete support is out of
 * scope for this initial implementation.
 */
@Slf4j
public class CouchbaseWriter implements SinkWriter<SeaTunnelRow, Void, Void> {

    private final Cluster cluster;
    private final Collection collection;
    private final CouchbaseWriterOptions options;
    private final SeaTunnelRowType rowType;

    private final List<SeaTunnelRow> buffer;
    private final long bulkActions;
    private final long batchIntervalMs;
    private final int maxRetries;
    private final long retryIntervalMs;
    private volatile long lastSendTime = 0L;

    // TODO: Reserve context for future parallelism/metrics use.
    @SuppressWarnings("unused")
    private final SinkWriter.Context context;

    public CouchbaseWriter(
            CouchbaseWriterOptions options, CatalogTable catalogTable, SinkWriter.Context context) {
        this.options = options;
        this.rowType = catalogTable.getSeaTunnelRowType();
        this.context = context;

        this.bulkActions = options.getFlushSize();
        this.batchIntervalMs = options.getBatchIntervalMs();
        this.maxRetries = options.getRetryMax();
        this.retryIntervalMs = options.getRetryInterval();
        this.buffer = new ArrayList<>();

        // Connect to the cluster and obtain the target collection.
        this.cluster =
                Cluster.connect(
                        options.getConnectionString(),
                        options.getUsername(),
                        options.getPassword());
        cluster.bucket(options.getBucket()).waitUntilReady(Duration.ofSeconds(30));
        this.collection =
                cluster.bucket(options.getBucket())
                        .scope(options.getScope())
                        .collection(options.getCollection());

        this.lastSendTime = System.currentTimeMillis();
    }

    /**
     * Buffers a single row. Pre-image rows ({@code UPDATE_BEFORE}) are silently skipped because
     * only the final value matters for document stores. {@code DELETE} rows are explicitly rejected
     * because CDC delete support is out of scope for this initial implementation.
     *
     * @param row the incoming row
     * @throws CouchbaseConnectorException if the row kind is {@code DELETE}
     */
    @Override
    public void write(SeaTunnelRow row) {
        if (row.getRowKind() == RowKind.UPDATE_BEFORE) {
            return;
        }
        if (row.getRowKind() == RowKind.DELETE) {
            throw new CouchbaseConnectorException(
                    CouchbaseConnectorErrorCode.UNSUPPORTED_ROW_KIND,
                    "RowKind.DELETE is not supported by the Couchbase sink. "
                            + "CDC delete handling is out of scope for the initial implementation.");
        }
        buffer.add(row);
        if (isOverMaxBatchSizeLimit() || isOverMaxBatchIntervalLimit()) {
            doFlush();
        }
    }

    @Override
    public Optional<Void> prepareCommit() {
        doFlush();
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {}

    @Override
    public void close() {
        try {
            doFlush();
        } finally {
            try {
                cluster.disconnect();
            } catch (Exception e) {
                throw new CouchbaseConnectorException(
                        CouchbaseConnectorErrorCode.CLOSE_CLIENT_FAILED,
                        "Failed to disconnect Couchbase cluster",
                        e);
            }
        }
    }

    // ---------------------------------------------------------------------------
    // Internal helpers
    // ---------------------------------------------------------------------------

    /** Converts a {@link SeaTunnelRow} to a {@link JsonObject} using the schema field names. */
    private JsonObject toJsonObject(SeaTunnelRow row) {
        JsonObject doc = JsonObject.create();
        String[] fieldNames = rowType.getFieldNames();
        SeaTunnelDataType<?>[] fieldTypes = rowType.getFieldTypes();
        for (int i = 0; i < fieldNames.length; i++) {
            Object value = row.getField(i);
            if (value == null) {
                doc.putNull(fieldNames[i]);
            } else {
                putValue(doc, fieldNames[i], value, fieldTypes[i]);
            }
        }
        return doc;
    }

    /** Puts a typed field value into the JSON object. */
    private void putValue(JsonObject doc, String key, Object value, SeaTunnelDataType<?> dataType) {
        switch (dataType.getSqlType()) {
            case BOOLEAN:
                doc.put(key, (Boolean) value);
                break;
            case TINYINT:
                doc.put(key, ((Number) value).intValue());
                break;
            case SMALLINT:
                doc.put(key, ((Number) value).intValue());
                break;
            case INT:
                doc.put(key, (Integer) value);
                break;
            case BIGINT:
                doc.put(key, (Long) value);
                break;
            case FLOAT:
                doc.put(key, (Float) value);
                break;
            case DOUBLE:
                doc.put(key, (Double) value);
                break;
            case STRING:
            case DATE:
            case TIME:
            case TIMESTAMP:
                doc.put(key, value.toString());
                break;
            case BYTES:
                // Couchbase JSON does not natively encode byte arrays; store as Base64 string.
                doc.put(key, java.util.Base64.getEncoder().encodeToString((byte[]) value));
                break;
            default:
                doc.put(key, value.toString());
                break;
        }
    }

    /**
     * Builds the Couchbase document key from the primary-key fields. Falls back to a random UUID
     * when no primary key fields are configured.
     */
    private String buildDocumentKey(JsonObject doc) {
        String[] pk = options.getPrimaryKey();
        if (pk != null && pk.length > 0) {
            return java.util.Arrays.stream(pk)
                    .map(field -> String.valueOf(doc.get(field)))
                    .collect(Collectors.joining("_"));
        }
        return UUID.randomUUID().toString();
    }

    /**
     * Writes all buffered documents to Couchbase with retry logic, then clears the buffer.
     * Synchronised to prevent concurrent flushes from overlapping.
     */
    synchronized void doFlush() {
        if (buffer.isEmpty()) {
            return;
        }

        // Take a snapshot so the buffer can be cleared on success regardless of order.
        final List<SeaTunnelRow> snapshot = new ArrayList<>(buffer);

        boolean success =
                IntStream.rangeClosed(0, maxRetries)
                        .anyMatch(
                                attempt -> {
                                    try {
                                        for (SeaTunnelRow row : snapshot) {
                                            JsonObject doc = toJsonObject(row);
                                            String docId = buildDocumentKey(doc);
                                            if (options.isUpsertEnable()) {
                                                collection.upsert(docId, doc);
                                            } else {
                                                collection.insert(docId, doc);
                                            }
                                        }
                                        buffer.clear();
                                        lastSendTime = System.currentTimeMillis();
                                        return true;
                                    } catch (Exception e) {
                                        log.debug(
                                                "Batch write to Couchbase failed, attempt={}",
                                                attempt,
                                                e);
                                        if (attempt >= maxRetries) {
                                            throw new CouchbaseConnectorException(
                                                    CouchbaseConnectorErrorCode
                                                            .WRITE_RECORDS_FAILED,
                                                    "Batch write to Couchbase failed after "
                                                            + maxRetries
                                                            + " retries",
                                                    e);
                                        }
                                        try {
                                            TimeUnit.MILLISECONDS.sleep(
                                                    retryIntervalMs * (attempt + 1));
                                        } catch (InterruptedException ie) {
                                            Thread.currentThread().interrupt();
                                            throw new CouchbaseConnectorException(
                                                    CouchbaseConnectorErrorCode
                                                            .WRITE_RECORDS_FAILED,
                                                    "Interrupted while retrying batch write",
                                                    ie);
                                        }
                                        return false;
                                    }
                                });

        if (!success) {
            throw new CouchbaseConnectorException(
                    CouchbaseConnectorErrorCode.WRITE_RECORDS_FAILED,
                    "Batch write to Couchbase failed after max retries");
        }
    }

    private boolean isOverMaxBatchSizeLimit() {
        return bulkActions != -1 && buffer.size() >= bulkActions;
    }

    private boolean isOverMaxBatchIntervalLimit() {
        return batchIntervalMs != -1
                && (System.currentTimeMillis() - lastSendTime) >= batchIntervalMs;
    }
}
