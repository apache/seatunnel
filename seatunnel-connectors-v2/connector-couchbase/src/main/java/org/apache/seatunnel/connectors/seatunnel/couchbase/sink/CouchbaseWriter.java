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

import com.couchbase.client.core.error.AmbiguousTimeoutException;
import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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

    /** Immutable pair of a pre-resolved document id and its JSON body. */
    private static final class WriteUnit {
        final String docId;
        final JsonObject doc;

        WriteUnit(String docId, JsonObject doc) {
            this.docId = docId;
            this.doc = doc;
        }
    }

    /**
     * Writes all buffered documents to Couchbase with retry logic, then clears the buffer.
     *
     * <p>Document ids are assigned <em>once</em> before the retry loop starts so that every retry
     * attempt replays the exact same {@code (docId, doc)} pairs. A {@code startFrom} cursor
     * advances past rows that were already durably written in a previous (partial) attempt, which
     * prevents silent duplicate documents in the random-UUID insert path and avoids spurious
     * duplicate-key failures in the stable-key insert path.
     *
     * <p>Ambiguous-exception safety on the insert path: the Couchbase SDK can throw {@link
     * AmbiguousTimeoutException} when a write request times out before the client can determine
     * whether the server committed the document or not. In that case the in-flight row index is
     * recorded in {@code ambiguousIndices} and {@code startFrom} is intentionally <em>not</em>
     * advanced, so the next retry attempt replays the same document.
     *
     * <p>On that retry, if the server already committed the document the insert will fail with
     * {@link DocumentExistsException}. The handler treats that as a confirmation of success
     * <em>only</em> for the specific row index that previously hit the ambiguous timeout —
     * preventing a {@link DocumentExistsException} on a <em>different</em> row from being silently
     * swallowed as a false success.
     *
     * <p>A {@link DocumentExistsException} on any row that has no ambiguous-write history is always
     * a genuine key collision and is therefore re-thrown immediately.
     *
     * <p>Synchronised to prevent concurrent flushes from overlapping.
     */
    synchronized void doFlush() {
        if (buffer.isEmpty()) {
            return;
        }

        // Materialise stable (docId, doc) pairs before entering the retry loop.
        // Assigning the document id here means every retry attempt reuses the same key —
        // no new UUIDs are generated on re-attempts, so partial-success retries cannot
        // produce duplicate documents.
        final List<WriteUnit> units = new ArrayList<>(buffer.size());
        for (SeaTunnelRow row : buffer) {
            JsonObject doc = toJsonObject(row);
            units.add(new WriteUnit(buildDocumentKey(doc), doc));
        }

        // startFrom tracks the first unit that has NOT yet been durably written.
        // When a batch attempt fails mid-way at index k, the next attempt resumes
        // from k, skipping the rows [0, k) that already succeeded.
        int startFrom = 0;
        int attempt = 0;

        // Records the unit indices that previously threw AmbiguousTimeoutException.
        // Only these rows are allowed to treat DocumentExistsException as a success signal
        // on the next attempt — for all other rows it remains a genuine key collision.
        final Set<Integer> ambiguousIndices = new HashSet<>();

        while (true) {
            try {
                for (int i = startFrom; i < units.size(); i++) {
                    WriteUnit unit = units.get(i);
                    if (options.isUpsertEnable()) {
                        collection.upsert(unit.docId, unit.doc);
                    } else {
                        try {
                            collection.insert(unit.docId, unit.doc);
                            // Clean up the ambiguous marker now that the insert succeeded
                            // cleanly (no DocumentExistsException), so a future ambiguous
                            // timeout on this same index is tracked fresh.
                            ambiguousIndices.remove(i);
                        } catch (DocumentExistsException dee) {
                            // Accept DocumentExistsException as success only if this exact
                            // row index previously hit an AmbiguousTimeoutException, meaning
                            // the server may have already committed it. For any other row this
                            // is a genuine key collision and must propagate.
                            if (!ambiguousIndices.contains(i)) {
                                throw dee;
                            }
                            log.debug(
                                    "Insert for docId='{}' got DocumentExistsException after"
                                            + " prior ambiguous timeout at index={} — treating as"
                                            + " already committed; skipping.",
                                    unit.docId,
                                    i);
                            ambiguousIndices.remove(i);
                        }
                    }
                    // Advance the cursor only after the write is confirmed (or confirmed-already-
                    // present) so that a failure on the very next row does not skip this one.
                    startFrom = i + 1;
                }
                buffer.clear();
                lastSendTime = System.currentTimeMillis();
                return;
            } catch (AmbiguousTimeoutException ate) {
                // Record the in-flight index so the next attempt can distinguish a
                // "previously committed" DocumentExistsException from a genuine collision.
                // Do NOT advance startFrom — the next attempt replays from the same position.
                ambiguousIndices.add(startFrom);
                log.warn(
                        "Ambiguous timeout on Couchbase write (docId='{}'), attempt={} — "
                                + "will retry from the same position to verify commit status.",
                        units.get(startFrom).docId,
                        attempt,
                        ate);
                if (attempt >= maxRetries) {
                    throw new CouchbaseConnectorException(
                            CouchbaseConnectorErrorCode.WRITE_RECORDS_FAILED,
                            "Batch write to Couchbase failed after " + maxRetries + " retries",
                            ate);
                }
                attempt++;
                try {
                    TimeUnit.MILLISECONDS.sleep(retryIntervalMs * attempt);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new CouchbaseConnectorException(
                            CouchbaseConnectorErrorCode.WRITE_RECORDS_FAILED,
                            "Interrupted while retrying batch write",
                            ie);
                }
            } catch (Exception e) {
                log.debug("Batch write to Couchbase failed, attempt={}", attempt, e);
                if (attempt >= maxRetries) {
                    throw new CouchbaseConnectorException(
                            CouchbaseConnectorErrorCode.WRITE_RECORDS_FAILED,
                            "Batch write to Couchbase failed after " + maxRetries + " retries",
                            e);
                }
                attempt++;
                try {
                    TimeUnit.MILLISECONDS.sleep(retryIntervalMs * attempt);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new CouchbaseConnectorException(
                            CouchbaseConnectorErrorCode.WRITE_RECORDS_FAILED,
                            "Interrupted while retrying batch write",
                            ie);
                }
            }
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
