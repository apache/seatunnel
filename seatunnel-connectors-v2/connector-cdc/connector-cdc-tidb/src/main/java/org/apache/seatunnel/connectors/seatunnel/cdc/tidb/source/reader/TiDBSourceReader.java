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

package org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.reader;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.config.TiDBSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.config.TiDBSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.deserializer.SeaTunnelRowSnapshotRecordDeserializer;
import org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.deserializer.SeaTunnelRowStreamingRecordDeserializer;
import org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.split.TiDBSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.utils.TableKeyRangeUtils;

import org.tikv.cdc.CDCClient;
import org.tikv.common.TiSession;
import org.tikv.common.key.Key;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.common.region.RegionStoreClient;
import org.tikv.common.region.TiRegion;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.kvproto.Cdcpb;
import org.tikv.kvproto.Coprocessor;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.shade.com.google.protobuf.ByteString;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
public class TiDBSourceReader implements SourceReader<SeaTunnelRow, TiDBSourceSplit> {

    private static final String CDC_DIAG_PREFIX = "[TiDB-CDC-DIAG]";
    private static final long STREAMING_STATS_LOG_INTERVAL_MS = 10_000L;
    private static final long SLOW_STREAMING_BATCH_MS = 1_000L;
    private static final int METADATA_DRAIN_ATTEMPT_MULTIPLIER = 10;
    private static final long EMPTY_POLL_BREAK_THRESHOLD_MS = 50L;

    private final SourceReader.Context context;
    private final TiDBSourceConfig config;
    private final List<TiDBSourceSplit> sourceSplits;

    private final Map<TiDBSourceSplit, CDCClient> cacheCDCClient;

    private final Map<String, SeaTunnelRowSnapshotRecordDeserializer> snapshotDeserializers =
            new HashMap<>();
    private final Map<String, SeaTunnelRowStreamingRecordDeserializer> streamingDeserializers =
            new HashMap<>();

    private transient TiSession session;

    // Transaction assembly buffers are isolated per table: each CDC client only streams rows of
    // its own table's key range, but batches of different tables interleave across pollNext calls,
    // so sharing one buffer would flush one table's rows through another table's deserializer.
    private final Map<String, TreeMap<RowKeyWithTs, Cdcpb.Event.Row>> preWrites = new HashMap<>();
    private final Map<String, TreeMap<RowKeyWithTs, Cdcpb.Event.Row>> commits = new HashMap<>();
    // cdc event will lose if pull cdc event block when region split
    // use queue to separate read and write to ensure pull event unblock.
    // since sink jdbc is slow, 5000W queue size may be safe size.
    private final Map<String, BlockingQueue<Cdcpb.Event.Row>> committedEvents = new HashMap<>();

    private final List<CatalogTable> catalogTables;
    private final Set<String> configuredTables;

    private long lastStreamingStatsLogTime;
    private long totalPolledRows;
    private long totalCommittedRows;
    private long totalEmittedRows;

    public TiDBSourceReader(
            Context context, TiDBSourceConfig config, List<CatalogTable> catalogTables) {
        this.context = context;
        this.config = config;
        this.sourceSplits = new ArrayList<>();

        this.cacheCDCClient = new HashMap<>();
        this.catalogTables = catalogTables;
        this.configuredTables = new HashSet<>();
        for (CatalogTable catalogTable : catalogTables) {
            TablePath tablePath = catalogTable.getTablePath();
            if (tablePath != null) {
                this.configuredTables.add(
                        TiDBSourceOptions.tableFullName(
                                tablePath.getDatabaseName(), tablePath.getTableName()));
            }
        }
    }

    /** Open the source reader. */
    @Override
    public void open() throws Exception {
        this.session = TiSession.create(config.getTiConfiguration());
        for (CatalogTable table : catalogTables) {
            String databaseName = table.getTablePath().getDatabaseName();
            String tableName = table.getTablePath().getTableName();
            TiTableInfo tableInfo = session.getCatalog().getTable(databaseName, tableName);
            String tableKey = TiDBSourceOptions.tableFullName(databaseName, tableName);
            this.snapshotDeserializers.put(
                    tableKey, new SeaTunnelRowSnapshotRecordDeserializer(tableInfo, table));
            this.streamingDeserializers.put(
                    tableKey, new SeaTunnelRowStreamingRecordDeserializer(tableInfo, table));
        }
        log.info(
                "{} Reader opened, tables={}, startupMode={}, batchSize={},"
                        + " scanTimeout={}, requestTimeout={}.",
                CDC_DIAG_PREFIX,
                snapshotDeserializers.keySet(),
                config.getStartupMode(),
                config.getBatchSize(),
                config.getTiConfiguration().getScanTimeout(),
                config.getTiConfiguration().getTimeout());
    }

    /**
     * Called to close the reader, in case it holds on to any resources, like threads or network
     * connections.
     */
    @Override
    public void close() throws IOException {
        if (this.session != null) {
            try {
                this.session.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Generate the next batch of records.
     *
     * @param output output collector.
     * @throws Exception if error occurs.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        // addSplits may append splits concurrently while the engine drives pollNext, so iterate
        // over a snapshot copy; newly added splits are picked up by a later pollNext call.
        List<TiDBSourceSplit> currentSplits = new ArrayList<>(sourceSplits);
        if (config.getStartupMode() == StartupMode.INITIAL) {
            for (TiDBSourceSplit sourceSplit : currentSplits) {
                if (!sourceSplit.isSnapshotCompleted()) {
                    snapshotEvents(sourceSplit, output);
                    sourceSplit.setSnapshotCompleted(true);
                }
            }
        }
        Iterator<TiDBSourceSplit> iterator = currentSplits.iterator();
        while (iterator.hasNext()) {
            TiDBSourceSplit sourceSplit = iterator.next();
            captureStreamingEvents(sourceSplit, output);
        }
    }

    protected void snapshotEvents(TiDBSourceSplit split, Collector<SeaTunnelRow> output)
            throws Exception {
        log.info(String.format("[%s] Snapshot events start.", split.splitId()));
        SeaTunnelRowSnapshotRecordDeserializer snapshotRecordDeserializer =
                requireSnapshotDeserializer(split);
        Coprocessor.KeyRange keyRange = split.getKeyRange();
        // start timestamp
        long startTs = session.getTimestamp().getVersion();
        ByteString start = split.getSnapshotStart();
        Key end = Key.toRawKey(keyRange.getEnd());
        long scannedCount = 0;
        long emittedCount = 0;
        while (Key.toRawKey(start).compareTo(end) < 0) {
            try (RegionStoreClient scanClient =
                    session.getRegionStoreClientBuilder().build(start)) {
                scanClient.setTimeout(config.getTiConfiguration().getScanTimeout());
                TiRegion region = scanClient.getRegion();
                ByteString regionEnd = region.getEndKey();
                final List<Kvrpcpb.KvPair> segment =
                        scanClient.scan(
                                ConcreteBackOffer.newScannerNextMaxBackOff(), start, startTs);
                if (segment == null || segment.isEmpty()) {
                    if (regionEnd.isEmpty() || Key.toRawKey(regionEnd).compareTo(end) >= 0) {
                        break;
                    }
                    start = regionEnd;
                    split.setSnapshotStart(start);
                    continue;
                }

                boolean reachEnd = false;
                ByteString nextStart = null;
                for (Kvrpcpb.KvPair record : segment) {
                    Key recordKey = Key.toRawKey(record.getKey());
                    if (recordKey.compareTo(end) >= 0) {
                        reachEnd = true;
                        break;
                    }
                    scannedCount++;
                    if (TableKeyRangeUtils.isRecordKey(record.getKey().toByteArray())) {
                        snapshotRecordDeserializer.deserialize(record, output);
                        emittedCount++;
                    }
                    nextStart = recordKey.next().toByteString();
                }
                if (reachEnd || nextStart == null) {
                    break;
                }
                start = nextStart;
                // set snapshot offset
                split.setSnapshotStart(start);
            }
        }
        split.setResolvedTs(startTs);
        log.info(
                "[{}] Snapshot events end, scanned kv count: {}, emitted row count: {}.",
                split.splitId(),
                scannedCount,
                emittedCount);
    }

    protected void captureStreamingEvents(TiDBSourceSplit split, Collector<SeaTunnelRow> output)
            throws Exception {
        long batchStartTime = System.currentTimeMillis();
        long pullStartNanos = System.nanoTime();
        String tableKey = deserializerKey(split);
        long resolvedTs = split.getResolvedTs();
        long startResolvedTs = resolvedTs;
        CDCClient cdcClient = getCdcClient(split, resolvedTs);
        int polledRows = 0;
        int ignoredRows = 0;
        int resolvedTsAdvances = 0;
        int metadataEvents = 0;
        int emptyPolls = 0;
        int pollAttempts = 0;
        long currentMaxResolvedTs = cdcClient.getMaxResolvedTs();
        int maxPollAttempts = maxPollAttempts(config.getBatchSize());
        for (int i = 0, attempts = 0;
                i < config.getBatchSize() && attempts < maxPollAttempts;
                attempts++) {
            long beforeGetResolvedTs = currentMaxResolvedTs;
            long singlePollStartNanos = System.nanoTime();
            final Cdcpb.Event.Row row = cdcClient.get();
            pollAttempts++;
            long singlePollCostMs = nanosToMillis(System.nanoTime() - singlePollStartNanos);
            currentMaxResolvedTs = cdcClient.getMaxResolvedTs();
            if (row == null) {
                if (currentMaxResolvedTs != beforeGetResolvedTs) {
                    resolvedTsAdvances++;
                    metadataEvents++;
                    continue;
                }
                if (singlePollCostMs < EMPTY_POLL_BREAK_THRESHOLD_MS) {
                    metadataEvents++;
                    continue;
                }
                emptyPolls++;
                break;
            }
            if (handleRow(row, tableKey)) {
                polledRows++;
            } else {
                ignoredRows++;
            }
            i++;
        }
        long pullCostMs = nanosToMillis(System.nanoTime() - pullStartNanos);
        long flushStartNanos = System.nanoTime();
        // A split is safe to advance only after every TiKV region has reached the timestamp.
        resolvedTs = cdcClient.getMinResolvedTs();
        int pendingCommitsBeforeFlush = commitsFor(tableKey).size();
        int committedEventsBeforeFlush = committedEventsFor(tableKey).size();
        if (pendingCommitsBeforeFlush > 0) {
            resolvedTs = flushRowsAndGetSafeResolvedTs(resolvedTs, tableKey);
        }
        long flushCostMs = nanosToMillis(System.nanoTime() - flushStartNanos);
        long emitStartNanos = System.nanoTime();
        int emittedRows = 0;
        // output data
        if (!committedEventsFor(tableKey).isEmpty()) {
            SeaTunnelRowStreamingRecordDeserializer streamingDeserializer =
                    requireStreamingDeserializer(split);
            BlockingQueue<Cdcpb.Event.Row> committedEvents = committedEventsFor(tableKey);
            while (!committedEvents.isEmpty()) {
                Cdcpb.Event.Row row = committedEvents.take();
                streamingDeserializer.deserialize(row, output);
                emittedRows++;
            }
        }
        long emitCostMs = nanosToMillis(System.nanoTime() - emitStartNanos);
        long batchCostMs = System.currentTimeMillis() - batchStartTime;
        totalPolledRows += polledRows;
        totalEmittedRows += emittedRows;
        logStreamingStats(
                split,
                startResolvedTs,
                resolvedTs,
                polledRows,
                ignoredRows,
                emittedRows,
                resolvedTsAdvances,
                metadataEvents,
                emptyPolls,
                pollAttempts,
                maxPollAttempts,
                pendingCommitsBeforeFlush,
                committedEventsBeforeFlush,
                pullCostMs,
                flushCostMs,
                emitCostMs,
                batchCostMs);
        split.setResolvedTs(resolvedTs);
    }

    private CDCClient getCdcClient(TiDBSourceSplit split, long finalResolvedTs) {
        CDCClient cdcClient =
                cacheCDCClient.computeIfAbsent(
                        split,
                        k -> {
                            CDCClient client = new CDCClient(session, k.getKeyRange());
                            client.start(finalResolvedTs);
                            log.info(
                                    "{} CDC client started, split={}, startResolvedTs={},"
                                            + " startLagMs={}.",
                                    CDC_DIAG_PREFIX,
                                    k.splitId(),
                                    finalResolvedTs,
                                    resolvedLagMs(finalResolvedTs));
                            return client;
                        });
        return cdcClient;
    }

    private SeaTunnelRowSnapshotRecordDeserializer requireSnapshotDeserializer(
            TiDBSourceSplit split) {
        SeaTunnelRowSnapshotRecordDeserializer deserializer =
                snapshotDeserializers.get(deserializerKey(split));
        if (deserializer == null) {
            throw new IllegalStateException(
                    String.format(
                            "Split %s belongs to table %s which is not configured on this reader."
                                    + " Removing tables when restoring a TiDB-CDC job is not"
                                    + " supported.",
                            split.splitId(), deserializerKey(split)));
        }
        return deserializer;
    }

    private SeaTunnelRowStreamingRecordDeserializer requireStreamingDeserializer(
            TiDBSourceSplit split) {
        SeaTunnelRowStreamingRecordDeserializer deserializer =
                streamingDeserializers.get(deserializerKey(split));
        if (deserializer == null) {
            throw new IllegalStateException(
                    String.format(
                            "Split %s belongs to table %s which is not configured on this reader."
                                    + " Removing tables when restoring a TiDB-CDC job is not"
                                    + " supported.",
                            split.splitId(), deserializerKey(split)));
        }
        return deserializer;
    }

    private static String deserializerKey(TiDBSourceSplit split) {
        return split.tableFullName();
    }

    /**
     * Get the current split checkpoint state by checkpointId.
     *
     * <p>If the source is bounded, checkpoint is not triggered.
     *
     * @param checkpointId checkpoint Id.
     * @return split checkpoint state.
     * @throws Exception if error occurs.
     */
    @Override
    public List<TiDBSourceSplit> snapshotState(long checkpointId) throws Exception {
        return new ArrayList<>(sourceSplits);
    }

    /**
     * Add the split checkpoint state to reader.
     *
     * @param splits split checkpoint state.
     */
    @Override
    public void addSplits(List<TiDBSourceSplit> splits) {
        for (TiDBSourceSplit split : splits) {
            if (configuredTables.contains(split.tableFullName())) {
                sourceSplits.add(split);
            } else {
                log.warn(
                        "{} Drop split {} of table {} which is no longer configured on restore;"
                                + " its checkpoint position is discarded and the table will run a"
                                + " fresh snapshot if it is added back later.",
                        CDC_DIAG_PREFIX,
                        split.splitId(),
                        split.tableFullName());
            }
        }
    }

    /**
     * This method is called when the reader is notified that it will not receive any further
     * splits.
     *
     * <p>It is triggered when the enumerator calls {@link
     * SourceSplitEnumerator.Context#signalNoMoreSplits(int)} with the reader's parallel subtask.
     */
    @Override
    public void handleNoMoreSplits() {}

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}

    private boolean handleRow(final Cdcpb.Event.Row row, final String tableKey) {
        if (!TableKeyRangeUtils.isRecordKey(row.getKey().toByteArray())) {
            // Don't handle index key for now
            return false;
        }
        log.debug("binlog record, type: {}, data: {}", row.getType(), row);
        switch (row.getType()) {
            case COMMITTED:
                preWritesFor(tableKey).put(RowKeyWithTs.ofStart(row), row);
                commitsFor(tableKey).put(RowKeyWithTs.ofCommit(row), row);
                break;
            case COMMIT:
                commitsFor(tableKey).put(RowKeyWithTs.ofCommit(row), row);
                break;
            case PREWRITE:
                preWritesFor(tableKey).put(RowKeyWithTs.ofStart(row), row);
                break;
            case ROLLBACK:
                preWritesFor(tableKey).remove(RowKeyWithTs.ofStart(row));
                break;
            default:
                log.warn("Unsupported row type:" + row.getType());
        }
        return true;
    }

    private TreeMap<RowKeyWithTs, Cdcpb.Event.Row> preWritesFor(final String tableKey) {
        return preWrites.computeIfAbsent(tableKey, ignored -> new TreeMap<>());
    }

    private TreeMap<RowKeyWithTs, Cdcpb.Event.Row> commitsFor(final String tableKey) {
        return commits.computeIfAbsent(tableKey, ignored -> new TreeMap<>());
    }

    private BlockingQueue<Cdcpb.Event.Row> committedEventsFor(final String tableKey) {
        return committedEvents.computeIfAbsent(tableKey, ignored -> new LinkedBlockingQueue<>());
    }

    private long flushRowsAndGetSafeResolvedTs(final long resolvedTs, final String tableKey) {
        long safeResolvedTs = resolvedTs;
        TreeMap<RowKeyWithTs, Cdcpb.Event.Row> commits = commitsFor(tableKey);
        while (!commits.isEmpty() && commits.firstKey().getTimestamp() <= resolvedTs) {
            final RowKeyWithTs commitKey = commits.firstKey();
            final Cdcpb.Event.Row commitRow = commits.firstEntry().getValue();
            final Cdcpb.Event.Row prewriteRow =
                    preWritesFor(tableKey).remove(RowKeyWithTs.ofStart(commitRow));
            if (prewriteRow == null) {
                safeResolvedTs = Math.min(safeResolvedTs, commitKey.getTimestamp() - 1);
                log.warn(
                        "Commit row has no matched prewrite row yet, hold it and stop advancing resolvedTs. "
                                + "commitTs: {}, startTs: {}, key: {}",
                        commitRow.getCommitTs(),
                        commitRow.getStartTs(),
                        commitRow.getKey());
                break;
            }
            commits.pollFirstEntry();
            // if pull cdc event block when region split, cdc event will lose.
            committedEventsFor(tableKey).offer(prewriteRow);
            totalCommittedRows++;
        }
        return safeResolvedTs;
    }

    private void logStreamingStats(
            TiDBSourceSplit split,
            long startResolvedTs,
            long endResolvedTs,
            int polledRows,
            int ignoredRows,
            int emittedRows,
            int resolvedTsAdvances,
            int metadataEvents,
            int emptyPolls,
            int pollAttempts,
            int maxPollAttempts,
            int pendingCommitsBeforeFlush,
            int committedEventsBeforeFlush,
            long pullCostMs,
            long flushCostMs,
            long emitCostMs,
            long batchCostMs) {
        long now = System.currentTimeMillis();
        boolean slowBatch = batchCostMs >= SLOW_STREAMING_BATCH_MS;
        boolean shouldLog =
                slowBatch || now - lastStreamingStatsLogTime >= STREAMING_STATS_LOG_INTERVAL_MS;
        if (!shouldLog) {
            return;
        }
        lastStreamingStatsLogTime = now;
        log.info(
                "{} Streaming stats, split={}, startResolvedTs={}, endResolvedTs={},"
                        + " resolvedLagMs={}, polledRows={}, ignoredRows={}, emittedRows={},"
                        + " resolvedTsAdvances={}, metadataEvents={}, emptyPolls={},"
                        + " pollAttempts={}, maxPollAttempts={}, pendingPrewrites={},"
                        + " pendingCommitsBeforeFlush={},"
                        + " committedQueueBeforeEmit={}, committedQueueAfterEmit={},"
                        + " totalPolledRows={}, totalCommittedRows={}, totalEmittedRows={},"
                        + " pullCostMs={}, flushCostMs={}, emitCostMs={}, batchCostMs={}.",
                CDC_DIAG_PREFIX,
                split.splitId(),
                startResolvedTs,
                endResolvedTs,
                resolvedLagMs(endResolvedTs),
                polledRows,
                ignoredRows,
                emittedRows,
                resolvedTsAdvances,
                metadataEvents,
                emptyPolls,
                pollAttempts,
                maxPollAttempts,
                preWrites.size(),
                pendingCommitsBeforeFlush,
                committedEventsBeforeFlush,
                committedEvents.size(),
                totalPolledRows,
                totalCommittedRows,
                totalEmittedRows,
                pullCostMs,
                flushCostMs,
                emitCostMs,
                batchCostMs);
    }

    private int maxPollAttempts(int batchSize) {
        if (batchSize <= 0) {
            return 0;
        }
        if (batchSize > Integer.MAX_VALUE / METADATA_DRAIN_ATTEMPT_MULTIPLIER) {
            return Integer.MAX_VALUE;
        }
        return Math.max(batchSize, batchSize * METADATA_DRAIN_ATTEMPT_MULTIPLIER);
    }

    private long resolvedLagMs(long resolvedTs) {
        if (resolvedTs <= 0) {
            return -1L;
        }
        return Math.max(0L, System.currentTimeMillis() - tsoPhysicalMillis(resolvedTs));
    }

    private long tsoPhysicalMillis(long tso) {
        return tso >> 18;
    }

    private long nanosToMillis(long nanos) {
        return nanos / 1_000_000L;
    }
}
