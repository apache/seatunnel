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
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.MapType;
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
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Writes {@link SeaTunnelRow} records to a Couchbase collection.
 *
 * <p>Records are buffered in memory and flushed to Couchbase when any of these conditions is met:
 *
 * <ul>
 *   <li>The buffer reaches {@code buffer-flush.max-rows}, or
 *   <li>A checkpoint or shutdown is triggered.
 * </ul>
 *
 * <p>Each record is converted to a {@link JsonObject}. The document key is derived from the
 * configured {@code primary-key} fields using a length-prefixed canonical encoding ({@code
 * <len>:<value>} components separated by {@code #}); when no primary key is set a random UUID is
 * used. Upsert mode is enabled via {@code upsert-enable}.
 *
 * <p>Supported row kinds: {@code INSERT}, {@code UPDATE_AFTER}. {@code UPDATE_BEFORE} is silently
 * skipped. {@code DELETE} is explicitly rejected with an exception — CDC delete support is out of
 * scope for this initial implementation.
 *
 * <p><b>Document-id assignment:</b> each row's document id is assigned at {@link #write} time and
 * stored together with the converted JSON body in the buffer as a {@link WriteUnit}. This means
 * that if {@link #doFlush} is invoked more than once on the same not-yet-cleared buffer (e.g. when
 * a periodic-flush retry is interrupted by {@link #close}), every invocation uses the same
 * pre-assigned ids — no new UUIDs are generated on a second pass, so silent duplicate documents and
 * spurious {@link com.couchbase.client.core.error.DocumentExistsException} collisions on
 * already-committed rows are both eliminated.
 */
@Slf4j
public class CouchbaseWriter implements SinkWriter<SeaTunnelRow, Void, Void> {

    private final Cluster cluster;
    private final Collection collection;
    private final CouchbaseWriterOptions options;
    private final SeaTunnelRowType rowType;

    private final List<WriteUnit> buffer;
    private final long bulkActions;
    private final int maxRetries;
    private final long retryIntervalMs;

    private final SinkWriter.Context context;

    public CouchbaseWriter(
            CouchbaseWriterOptions options, CatalogTable catalogTable, SinkWriter.Context context) {
        this.options = options;
        this.rowType = catalogTable.getSeaTunnelRowType();
        this.context = context;

        this.bulkActions = options.getFlushSize();
        this.maxRetries = options.getRetryMax();
        this.retryIntervalMs = options.getRetryInterval();
        this.buffer = new ArrayList<>();

        // Validate that every configured primary-key field exists in the schema.
        // Catching this at construction time gives a clear error message rather than
        // silently producing "null" or "a_null" document keys at write time.
        validatePrimaryKeyFields(options.getPrimaryKey(), this.rowType);

        // Connect to the cluster and obtain the target collection.
        // If any post-connect step fails we must disconnect to avoid leaking SDK threads/resources.
        Cluster connectedCluster =
                Cluster.connect(
                        options.getConnectionString(),
                        options.getUsername(),
                        options.getPassword());
        Collection resolvedCollection;
        try {
            connectedCluster.bucket(options.getBucket()).waitUntilReady(Duration.ofSeconds(30));
            resolvedCollection =
                    connectedCluster
                            .bucket(options.getBucket())
                            .scope(options.getScope())
                            .collection(options.getCollection());
        } catch (Exception e) {
            try {
                connectedCluster.disconnect();
            } catch (Exception disconnectEx) {
                e.addSuppressed(disconnectEx);
            }
            throw e;
        }
        this.cluster = connectedCluster;
        this.collection = resolvedCollection;

        // Opt in to engine-level timer flush. On Zeta the engine invokes this action on the normal
        // Sink input-processing path when a FlushSignal arrives, so there is no connector-owned
        // scheduler thread and no concurrency with write/checkpoint/close. On Spark and Flink the
        // Context does not implement registerFlushAction (it keeps the interface's no-op default),
        // so there is no periodic timer flush there; the buffer is flushed on
        // buffer-flush.max-rows,
        // on checkpoint, and on close(). The null-check is defensive for non-standard/test call
        // sites that may not supply a context.
        if (context != null) {
            context.registerFlushAction(this::doFlush);
        }
    }

    /**
     * Buffers a single row. Pre-image rows ({@code UPDATE_BEFORE}) are silently skipped because
     * only the final value matters for document stores. {@code DELETE} rows are explicitly rejected
     * because CDC delete support is out of scope for this initial implementation.
     *
     * <p>The row's document id is assigned here, at buffer-add time, so that every subsequent flush
     * attempt (including a {@link #close}-triggered re-flush of a buffer that a prior flush left
     * un-cleared) uses the same stable id. This prevents silent duplicate documents when no primary
     * key is configured (UUID path) and prevents spurious {@link
     * com.couchbase.client.core.error.DocumentExistsException} collisions on already-committed rows
     * when a primary key is configured and insert mode is active.
     *
     * @param row the incoming row throws the CouchBaseConnectorException if the row kind is {@code
     *     DELETE}
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
        // Assign the document id at buffer-add time (not inside doFlush) so that if close()
        // triggers a second doFlush() on a not-yet-cleared buffer the same ids are reused.
        JsonObject doc = toJsonObject(row);
        WriteUnit unit = new WriteUnit(buildDocumentKey(doc), doc);
        synchronized (this) {
            buffer.add(unit);
            if (isOverMaxBatchSizeLimit()) {
                doFlush();
            }
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
        Throwable primaryThrowable = null;
        try {
            doFlush();
        } catch (Throwable t) {
            primaryThrowable = t;
        } finally {
            try {
                cluster.disconnect();
            } catch (Exception disconnectEx) {
                if (primaryThrowable != null) {
                    // Issue 2: preserve the flush error as the primary cause; attach the
                    // disconnect failure so it is visible but does not mask the root cause.
                    primaryThrowable.addSuppressed(disconnectEx);
                } else {
                    primaryThrowable =
                            new CouchbaseConnectorException(
                                    CouchbaseConnectorErrorCode.CLOSE_CLIENT_FAILED,
                                    "Failed to disconnect Couchbase cluster",
                                    disconnectEx);
                }
            }
        }
        if (primaryThrowable != null) {
            if (primaryThrowable instanceof CouchbaseConnectorException) {
                throw (CouchbaseConnectorException) primaryThrowable;
            }
            throw new CouchbaseConnectorException(
                    CouchbaseConnectorErrorCode.WRITE_RECORDS_FAILED,
                    "Flush failed during close",
                    primaryThrowable);
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

    /**
     * Puts a typed field value into the JSON object.
     *
     * <p>Supported types and their JSON representations:
     *
     * <ul>
     *   <li>BOOLEAN → Boolean
     *   <li>TINYINT / SMALLINT / INT → Integer
     *   <li>BIGINT → Long
     *   <li>FLOAT → Float
     *   <li>DOUBLE → Double
     *   <li>DECIMAL → String (exact decimal representation, e.g. {@code "123.456"})
     *   <li>STRING / DATE / TIME / TIMESTAMP → String
     *   <li>BYTES → String (Base64-encoded)
     *   <li>ARRAY → {@link JsonArray} (elements recursively converted)
     *   <li>MAP → {@link JsonObject} (keys coerced to String, values recursively converted)
     *   <li>ROW → nested {@link JsonObject}
     * </ul>
     */
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
            case DECIMAL:
                // Store as an exact string to preserve precision; Couchbase JSON numbers are
                // IEEE-754 doubles which would silently lose scale for large DECIMAL values.
                doc.put(key, ((BigDecimal) value).toPlainString());
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
            case ARRAY:
                doc.put(key, toJsonArray((Object[]) value, (ArrayType<?, ?>) dataType));
                break;
            case MAP:
                doc.put(key, toJsonObjectFromMap((Map<?, ?>) value, (MapType<?, ?>) dataType));
                break;
            case ROW:
                doc.put(key, toJsonObject((SeaTunnelRow) value, (SeaTunnelRowType) dataType));
                break;
            default:
                throw new CouchbaseConnectorException(
                        CouchbaseConnectorErrorCode.UNSUPPORTED_TYPE,
                        "Unsupported SeaTunnel type for Couchbase sink: "
                                + dataType.getSqlType()
                                + ". Field: "
                                + key);
        }
    }

    /**
     * Converts an array value to a {@link JsonArray}, recursively converting each element according
     * to the declared element type.
     */
    private JsonArray toJsonArray(Object[] elements, ArrayType<?, ?> arrayType) {
        SeaTunnelDataType<?> elementType = arrayType.getElementType();
        JsonArray arr = JsonArray.create();
        for (Object element : elements) {
            if (element == null) {
                arr.add((Object) null);
            } else {
                arr.add(scalarToJsonValue(element, elementType));
            }
        }
        return arr;
    }

    /**
     * Converts a {@link Map} value to a {@link JsonObject}, coercing keys to String and recursively
     * converting values according to the declared value type.
     */
    private JsonObject toJsonObjectFromMap(Map<?, ?> map, MapType<?, ?> mapType) {
        SeaTunnelDataType<?> valueType = mapType.getValueType();
        JsonObject obj = JsonObject.create();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            String mapKey = String.valueOf(entry.getKey());
            Object mapValue = entry.getValue();
            if (mapValue == null) {
                obj.putNull(mapKey);
            } else {
                obj.put(mapKey, scalarToJsonValue(mapValue, valueType));
            }
        }
        return obj;
    }

    /**
     * Converts a nested {@link SeaTunnelRow} to a {@link JsonObject} using an explicit {@link
     * SeaTunnelRowType} descriptor (used for ROW fields).
     */
    private JsonObject toJsonObject(SeaTunnelRow row, SeaTunnelRowType rowType) {
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

    /**
     * Returns the JSON-compatible boxed representation of a scalar value so it can be inserted into
     * a {@link JsonArray} or nested {@link JsonObject}. This mirrors {@link #putValue} but returns
     * the value instead of calling {@code doc.put}.
     */
    private Object scalarToJsonValue(Object value, SeaTunnelDataType<?> dataType) {
        switch (dataType.getSqlType()) {
            case BOOLEAN:
                return (Boolean) value;
            case TINYINT:
            case SMALLINT:
                return ((Number) value).intValue();
            case INT:
                return (Integer) value;
            case BIGINT:
                return (Long) value;
            case FLOAT:
                return (Float) value;
            case DOUBLE:
                return (Double) value;
            case DECIMAL:
                return ((BigDecimal) value).toPlainString();
            case STRING:
            case DATE:
            case TIME:
            case TIMESTAMP:
                return value.toString();
            case BYTES:
                return java.util.Base64.getEncoder().encodeToString((byte[]) value);
            case ARRAY:
                return toJsonArray((Object[]) value, (ArrayType<?, ?>) dataType);
            case MAP:
                return toJsonObjectFromMap((Map<?, ?>) value, (MapType<?, ?>) dataType);
            case ROW:
                return toJsonObject((SeaTunnelRow) value, (SeaTunnelRowType) dataType);
            default:
                throw new CouchbaseConnectorException(
                        CouchbaseConnectorErrorCode.UNSUPPORTED_TYPE,
                        "Unsupported SeaTunnel type for Couchbase sink: " + dataType.getSqlType());
        }
    }

    /**
     * Validates that every configured primary-key field name exists in the row schema.
     *
     * <p>Called once during writer construction so that a misconfigured or misspelled key name is
     * caught immediately rather than surfacing as a mysterious {@code "null"} document key at write
     * time.
     *
     * @param primaryKey configured key field names (may be null or empty)
     * @param rowType the schema of incoming rows
     * @throws CouchbaseConnectorException if any key field is absent from the schema
     */
    static void validatePrimaryKeyFields(String[] primaryKey, SeaTunnelRowType rowType) {
        if (primaryKey == null || primaryKey.length == 0) {
            return;
        }
        Set<String> schemaFields = new HashSet<>(Arrays.asList(rowType.getFieldNames()));
        for (String keyField : primaryKey) {
            if (!schemaFields.contains(keyField)) {
                throw new CouchbaseConnectorException(
                        CouchbaseConnectorErrorCode.INVALID_PRIMARY_KEY,
                        "Primary-key field '"
                                + keyField
                                + "' is not present in the row schema. "
                                + "Configured schema fields: "
                                + schemaFields);
            }
        }
    }

    /**
     * Builds the Couchbase document key from the configured primary-key fields. Falls back to a
     * random UUID when no primary-key fields are configured.
     *
     * <p>Delegates to {@link #buildDocumentKeyFrom} for the key-assembly logic so that the
     * null-value validation can be exercised in unit tests without a live cluster connection.
     */
    private String buildDocumentKey(JsonObject doc) {
        String[] pk = options.getPrimaryKey();
        return buildDocumentKeyFrom(pk, doc);
    }

    /**
     * Assembles a Couchbase document key from {@code primaryKey} field values in {@code doc} using
     * a <em>length-prefixed canonical encoding</em> that is guaranteed collision-free.
     *
     * <p>Each component is encoded as {@code <len>:<value>} and components are separated by {@code
     * #}. The length prefix makes boundaries unambiguous regardless of the character content of the
     * values, so key pairs like {@code ("a_b","c")} and {@code ("a","b_c")} produce distinct
     * document IDs ({@code "3:a_b#1:c"} vs {@code "1:a#3:b_c"}).
     *
     * <p>Returns a random UUID when {@code primaryKey} is null or empty.
     *
     * <p>Package-visible so that unit tests can exercise the null-value guard directly without
     * needing a live Couchbase cluster.
     *
     * @throws CouchbaseConnectorException if any primary-key field value is null in {@code doc},
     *     which would produce an ambiguous document key and cause silent data loss in upsert mode
     *     or hard-to-diagnose duplicate-key errors in insert mode
     */
    static String buildDocumentKeyFrom(String[] primaryKey, JsonObject doc) {
        if (primaryKey != null && primaryKey.length > 0) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < primaryKey.length; i++) {
                String field = primaryKey[i];
                Object value = doc.get(field);
                if (value == null) {
                    throw new CouchbaseConnectorException(
                            CouchbaseConnectorErrorCode.INVALID_PRIMARY_KEY,
                            "Primary-key field '"
                                    + field
                                    + "' is null for the current row. "
                                    + "Null primary-key values produce ambiguous"
                                    + " document keys and must be rejected.");
                }
                String part = String.valueOf(value);
                if (i > 0) {
                    sb.append('#');
                }
                sb.append(part.length()).append(':').append(part);
            }
            return sb.toString();
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
     * Writes all buffered {@link WriteUnit}s to Couchbase with retry logic, then clears the buffer.
     *
     * <p>Document ids were assigned at {@link #write} time (not here), so every retry attempt and
     * every cross-invocation re-flush of a not-yet-cleared buffer uses the same stable ids — no new
     * UUIDs are generated, and no spurious key-collision exceptions arise for rows that were
     * already committed by an earlier interrupted attempt.
     *
     * <p>A {@code startFrom} cursor advances past rows that were already durably written in a
     * previous (partial) attempt, which prevents silent duplicate documents in the random-UUID
     * insert path and avoids spurious duplicate-key failures in the stable-key insert path.
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
     *
     * <p>The retry delay is <em>linear</em>: attempt {@code n} sleeps {@code retryIntervalMs * n}
     * milliseconds.
     */
    synchronized void doFlush() {
        if (buffer.isEmpty()) {
            return;
        }

        // Document ids were assigned at write() time; use the buffer directly as the units list.
        final List<WriteUnit> units = buffer;

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
}
