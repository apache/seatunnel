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
 * Scans ascending OBJECTIDs up to the first observed maximum, committing one page at a time. The
 * upper bound excludes later IDs but does not freeze updates or deletions of existing rows.
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
     * Checkpoint field 2: last OBJECTID emitted with a fully committed page.
     */
    private long lastId;
    /*
     * Checkpoint field 3: emitted rows, retained across restore to enforce max_rows.
     */
    private long rowCount;

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
     * Polls a page without holding the checkpoint lock during network I/O. A cancellation-induced
     * I/O error is not reported as a failed request or normal completion.
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
     * checkpoint lock coordinates this state with the page commit in pollPage.
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
     * Fetches and validates the full ordered page before taking the checkpoint lock. Emission,
     * upper bound, cursor and row count commit together under that lock. Short nonempty pages
     * continue by key; only an empty page, bound or row cap ends the scan.
     */
    private void pollPage(Collector<SeaTunnelRow> output) throws IOException {
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
        synchronized (output.getCheckpointLock()) {
            // Do not publish a fetched page if cancellation won before the commit. Once emission
            // starts, keep the page and its cursor update atomic with respect to a checkpoint.
            if (api.isCancelled()) {
                return;
            }
            for (SeaTunnelRow row : rows) {
                output.collect(row);
            }
            upperBound = bound;
            lastId = pageLastId;
            rowCount += rows.size();
            // Short ArcGIS pages are allowed; continue by key until an empty page or the scan
            // bound.
            if (rows.isEmpty() || lastId == bound || (maxRows > 0 && rowCount == maxRows)) {
                noMoreSplits = true;
                if (!api.isCancelled()) {
                    context.signalNoMoreElement();
                }
            }
        }
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
