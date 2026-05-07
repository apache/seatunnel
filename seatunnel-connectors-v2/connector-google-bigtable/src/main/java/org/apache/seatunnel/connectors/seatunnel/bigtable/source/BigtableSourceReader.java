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

package org.apache.seatunnel.connectors.seatunnel.bigtable.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.bigtable.client.BigtableClient;
import org.apache.seatunnel.connectors.seatunnel.bigtable.config.BigtableParameters;
import org.apache.seatunnel.connectors.seatunnel.bigtable.format.BigtableDeserializationFormat;

import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Reads rows from a single {@link BigtableSourceSplit} and emits {@link SeaTunnelRow} records.
 *
 * <p>The column schema is derived from the {@link SeaTunnelRowType}. Each field name must follow
 * the pattern {@code columnFamily:qualifier}, except the special name {@code rowkey} which maps to
 * the Bigtable row key.
 */
@Slf4j
public class BigtableSourceReader implements SourceReader<SeaTunnelRow, BigtableSourceSplit> {

    private static final String ROW_KEY_FIELD = "rowkey";

    private final Deque<BigtableSourceSplit> pendingSplits = new ConcurrentLinkedDeque<>();
    private final Context context;
    private final SeaTunnelRowType rowType;
    private final BigtableParameters parameters;
    private final BigtableDeserializationFormat deserializationFormat;
    private volatile boolean noMoreSplits = false;
    private BigtableClient bigtableClient;

    /** The split currently being read. Persisted in checkpoint to survive failover. */
    private volatile BigtableSourceSplit currentSplit = null;

    /**
     * Set of field names that map to the Bigtable row key. Populated from {@code rowkey_column}
     * config; falls back to the literal field name {@value ROW_KEY_FIELD} when not configured.
     */
    private final java.util.Set<String> rowKeyFieldNames;

    public BigtableSourceReader(
            BigtableParameters parameters, Context context, SeaTunnelRowType rowType) {
        this(parameters, context, rowType, null);
    }

    BigtableSourceReader(
            BigtableParameters parameters,
            Context context,
            SeaTunnelRowType rowType,
            BigtableClient bigtableClient) {
        this.parameters = parameters;
        this.context = context;
        this.rowType = rowType;
        this.bigtableClient = bigtableClient;
        this.deserializationFormat = new BigtableDeserializationFormat();
        if (parameters.getRowkeyColumns() != null && !parameters.getRowkeyColumns().isEmpty()) {
            this.rowKeyFieldNames = new java.util.HashSet<>(parameters.getRowkeyColumns());
        } else {
            this.rowKeyFieldNames = java.util.Collections.singleton(ROW_KEY_FIELD);
        }
    }

    @Override
    public void open() throws Exception {
        if (bigtableClient == null) {
            bigtableClient = BigtableClient.createInstance(parameters);
        }
    }

    @Override
    public void close() throws IOException {
        if (bigtableClient != null) {
            bigtableClient.close();
            bigtableClient = null;
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        final BigtableSourceSplit split = pendingSplits.poll();
        if (Objects.nonNull(split)) {
            // Assign currentSplit before reading so checkpoints taken during readSplit()
            // include it and can re-enqueue it on restore.
            currentSplit = split;
            readSplit(split, output);
            currentSplit = null;
        } else if (noMoreSplits && pendingSplits.isEmpty()) {
            log.info("Closed the bounded Bigtable source");
            context.signalNoMoreElement();
        } else {
            log.warn("Waiting for Bigtable split, sleeping 1s");
            Thread.sleep(1000L);
        }
    }

    private void readSplit(BigtableSourceSplit split, Collector<SeaTunnelRow> output) {
        Query query = buildQuery(split);
        // Stream rows one at a time to avoid buffering the full result in memory.
        // Each collect() acquires the checkpoint lock only for the emit, not for the network read.
        bigtableClient
                .getDataClient()
                .readRows(query)
                .forEach(
                        bigtableRow -> {
                            SeaTunnelRow seaTunnelRow = convertRow(bigtableRow);
                            synchronized (output.getCheckpointLock()) {
                                output.collect(seaTunnelRow);
                            }
                        });
    }

    private Query buildQuery(BigtableSourceSplit split) {
        Query query = Query.create(parameters.getTable());

        String startKey = split.getStartRowKey();
        String endKey = split.getEndRowKey();
        if (!startKey.isEmpty() && !endKey.isEmpty()) {
            query.range(startKey, endKey);
        } else if (!startKey.isEmpty()) {
            query.range(startKey, null);
        } else if (!endKey.isEmpty()) {
            query.range(null, endKey);
        }

        if (parameters.getScanRowLimit() > 0) {
            query.limit(parameters.getScanRowLimit());
        }

        Filters.Filter filter = buildFilter();
        if (filter != null) {
            query.filter(filter);
        }

        return query;
    }

    private Filters.Filter buildFilter() {
        List<Filters.Filter> filters = new ArrayList<>();

        if (parameters.getMaxVersions() > 0) {
            filters.add(Filters.FILTERS.limit().cellsPerColumn(parameters.getMaxVersions()));
        }

        Long startTs = parameters.getStartTimestamp();
        Long endTs = parameters.getEndTimestamp();
        if (startTs != null && endTs != null) {
            filters.add(Filters.FILTERS.timestamp().range().of(startTs, endTs));
        } else if (startTs != null) {
            filters.add(Filters.FILTERS.timestamp().range().startClosed(startTs));
        } else if (endTs != null) {
            filters.add(Filters.FILTERS.timestamp().range().endOpen(endTs));
        }

        if (filters.isEmpty()) {
            return null;
        }
        if (filters.size() == 1) {
            return filters.get(0);
        }
        Filters.ChainFilter chain = Filters.FILTERS.chain();
        for (Filters.Filter f : filters) {
            chain = chain.filter(f);
        }
        return chain;
    }

    /**
     * Converts a Bigtable {@link Row} into a {@link SeaTunnelRow}.
     *
     * <p>Field names drive the mapping:
     *
     * <ul>
     *   <li>Fields listed in {@code rowkey_column} config (or the literal {@value ROW_KEY_FIELD}
     *       when the option is absent) → the row key bytes
     *   <li>{@code familyName:qualifier} → the latest cell value for that column
     * </ul>
     */
    private SeaTunnelRow convertRow(Row bigtableRow) {
        // Build a flat lookup: "family:qualifier" -> latest cell value
        Map<String, ByteString> cellMap = new HashMap<>();
        for (RowCell cell : bigtableRow.getCells()) {
            String key = cell.getFamily() + ":" + cell.getQualifier().toStringUtf8();
            cellMap.putIfAbsent(key, cell.getValue()); // first cell = latest (sorted desc by ts)
        }

        String[] fieldNames = rowType.getFieldNames();
        ByteString[] rawCells = new ByteString[fieldNames.length];
        for (int i = 0; i < fieldNames.length; i++) {
            String fieldName = fieldNames[i];
            if (rowKeyFieldNames.contains(fieldName)) {
                rawCells[i] = bigtableRow.getKey();
            } else {
                rawCells[i] = cellMap.get(fieldName);
            }
        }
        return deserializationFormat.deserialize(rawCells, rowType);
    }

    @Override
    public List<BigtableSourceSplit> snapshotState(long checkpointId) {
        List<BigtableSourceSplit> state = new ArrayList<>();
        // Include the split currently being read so it can be re-enqueued on restore.
        if (currentSplit != null) {
            state.add(currentSplit);
        }
        state.addAll(pendingSplits);
        return state;
    }

    @Override
    public void addSplits(List<BigtableSourceSplit> splits) {
        pendingSplits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        noMoreSplits = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}
}
