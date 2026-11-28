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

package org.apache.seatunnel.connectors.seatunnel.opendatadc;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

/**
 * Scans ascending OBJECTIDs up to the first observed maximum. Each complete page is validated
 * before emission; accepted rows commit individually so checkpoint injectors can run between polls.
 * The upper bound excludes later IDs but does not freeze updates or deletions of existing rows.
 */
final class OpenDataDcReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    private final SingleSplitReaderContext context;
    private final CrashesApi api;
    private final int pageSize;
    private final long maxRows;
    private final String tableId;
    /*
     * Checkpoint field 1: negative before discovery, otherwise the inclusive scan maximum.
     */
    private long upperBound = -1L;
    /*
     * Checkpoint field 2: last accepted OBJECTID; restored queries resume strictly after this key.
     */
    private long lastId;
    /*
     * Checkpoint field 3: emitted rows, retained across restore to enforce max_rows.
     */
    private long rowCount;
    // Poller-owned, bounded page data is transient and never enters the three-long checkpoint.
    private List<SeaTunnelRow> prepared;
    private int preparedIndex;
    private long preparedBound;

    OpenDataDcReader(
            SingleSplitReaderContext context,
            CrashesApi api,
            int pageSize,
            long maxRows,
            String tableId) {
        this.context = context;
        this.api = api;
        this.pageSize = pageSize;
        this.maxRows = maxRows;
        this.tableId = tableId;
    }

    @Override
    public void open() {}

    /*
     * Delegates sticky cancellation and active-request abortion to the owned API client.
     */
    @Override
    public void cancel() {
        api.cancel();
    }

    /*
     * Closes the owned API client, which cancels in-flight requests before closing transport.
     */
    @Override
    public void close() throws IOException {
        api.close();
    }

    /**
     * Fetches a page or accepts one prepared row, with network I/O outside the monitor. A
     * cancellation-induced I/O error is not reported as a failed request or normal completion.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws IOException {
        if (noMoreSplits || api.isCancelled()) {
            return;
        }
        try {
            pollPage(output);
        } catch (IOException e) {
            // Aborting the transport wakes polling with an I/O error. Do not report that as
            // an HTTP failure or a normal end-of-input when the engine is already canceling.
            if (!api.isCancelled()) {
                throw e;
            }
        }
    }

    /**
     * Serializes exactly 24 bytes: big-endian upperBound, lastId and rowCount longs. The engine
     * checkpoint lock coordinates these longs with each accepted row in pollPage.
     */
    @Override
    protected byte[] snapshotStateToBytes(long checkpointId) {
        return ByteBuffer.allocate(3 * Long.BYTES)
                .putLong(upperBound)
                .putLong(lastId)
                .putLong(rowCount)
                .array();
    }

    /**
     * Restores the same three-long layout so resumed pages keep both the scan bound and row cap.
     */
    @Override
    protected void restoreState(byte[] restoredState) {
        ByteBuffer buffer = ByteBuffer.wrap(restoredState);
        upperBound = buffer.getLong();
        lastId = buffer.getLong();
        rowCount = buffer.getLong();
    }

    /**
     * Accepts at most one row and commits its key/count under the collector monitor, then returns
     * so the engine can hand that monitor to an announced checkpoint or savepoint injector.
     */
    private void pollPage(Collector<SeaTunnelRow> output) throws IOException {
        if (prepared == null) {
            preparePage();
        }
        synchronized (output.getCheckpointLock()) {
            if (api.isCancelled()) {
                return;
            }
            if (preparedIndex < prepared.size()) {
                SeaTunnelRow row = prepared.get(preparedIndex);
                long acceptedId = (Long) row.getField(0);
                output.collect(row);
                upperBound = preparedBound;
                lastId = acceptedId;
                rowCount++;
                preparedIndex++;
            } else {
                // Empty pages still commit the discovered bound before signalling completion.
                upperBound = preparedBound;
            }
            if (preparedIndex == prepared.size()) {
                boolean empty = prepared.isEmpty();
                prepared = null;
                if (empty || lastId == upperBound || (maxRows > 0 && rowCount == maxRows)) {
                    noMoreSplits = true;
                    if (!api.isCancelled()) {
                        context.signalNoMoreElement();
                    }
                }
            }
        }
    }

    /**
     * Fetches and validates the entire ordered projected page outside the checkpoint monitor. No
     * accepted progress changes until collection succeeds; restore uses the same saved key
     * predicate.
     */
    private void preparePage() throws IOException {
        final long bound = upperBound < 0 ? api.readUpperBound() : upperBound;
        final int size = maxRows == 0 ? pageSize : (int) Math.min(pageSize, maxRows - rowCount);
        long pageLastId = lastId;
        List<SeaTunnelRow> rows = new ArrayList<>();
        if (size > 0 && lastId < bound) {
            JsonNode response = api.readPage(lastId, bound, size);
            JsonNode features = response.get("features");
            if (features.size() > size
                    || (features.isEmpty() && response.path("exceededTransferLimit").asBoolean())) {
                throw new IOException("Open Data DC returned inconsistent pagination metadata");
            }
            for (JsonNode feature : features) {
                JsonNode attributes = CrashesApi.attributes(feature);
                long id = CrashesApi.objectId(attributes);
                if (id <= pageLastId || id > bound) {
                    throw new IOException(
                            "Open Data DC returned an out-of-order or out-of-range OBJECTID");
                }
                pageLastId = id;
                rows.add(toRow(id, attributes));
            }
        }
        prepared = rows;
        preparedIndex = 0;
        preparedBound = bound;
    }

    /**
     * Requires every projected field, preserving explicit nulls and converting epoch millis to UTC.
     */
    private SeaTunnelRow toRow(long id, JsonNode attributes) throws IOException {
        JsonNode date = field(attributes, "REPORTDATE");
        JsonNode count = field(attributes, "TOTAL_VEHICLES");
        SeaTunnelRow row =
                new SeaTunnelRow(
                        new Object[] {
                            id,
                            text(attributes, "CCN"),
                            date.isNull()
                                    ? null
                                    : LocalDateTime.ofInstant(
                                            Instant.ofEpochMilli(readLong(date, "REPORTDATE")),
                                            ZoneOffset.UTC),
                            text(attributes, "WARD"),
                            count.isNull() ? null : readInt(count, "TOTAL_VEHICLES"),
                            coordinate(attributes, "LATITUDE"),
                            coordinate(attributes, "LONGITUDE")
                        });
        row.setTableId(tableId);
        return row;
    }

    private static JsonNode field(JsonNode attributes, String key) throws IOException {
        JsonNode value = attributes.get(key);
        if (value == null) {
            throw new IOException("Open Data DC response is missing the projected field " + key);
        }
        return value;
    }

    private static String text(JsonNode attributes, String key) throws IOException {
        JsonNode value = field(attributes, key);
        if (value.isNull() || value.isTextual()) {
            return value.textValue();
        }
        throw new IOException("Open Data DC returned a non-text value for " + key);
    }

    private static Double coordinate(JsonNode attributes, String key) throws IOException {
        JsonNode value = field(attributes, key);
        if (value.isNull()) {
            return null;
        }
        if (value.isNumber() && Double.isFinite(value.doubleValue())) {
            return value.doubleValue();
        }
        throw new IOException("Open Data DC returned an invalid coordinate for " + key);
    }

    private static long readLong(JsonNode value, String key) throws IOException {
        if (value.isIntegralNumber() && value.canConvertToLong()) {
            return value.longValue();
        }
        throw new IOException("Open Data DC returned an invalid long value for " + key);
    }

    private static int readInt(JsonNode value, String key) throws IOException {
        if (value.isIntegralNumber() && value.canConvertToInt()) {
            return value.intValue();
        }
        throw new IOException("Open Data DC returned an invalid integer for " + key);
    }
}
