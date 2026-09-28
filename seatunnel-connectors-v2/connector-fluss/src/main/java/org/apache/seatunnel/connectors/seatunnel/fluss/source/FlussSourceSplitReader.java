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
package org.apache.seatunnel.connectors.seatunnel.fluss.source;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordsBySplits;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordsWithSplitIds;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.splitreader.SplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.splitreader.SplitsAddition;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.splitreader.SplitsChange;

import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.client.table.scanner.log.LogScanner;
import com.alibaba.fluss.client.table.scanner.log.ScanRecords;
import com.alibaba.fluss.exception.FetchException;
import com.alibaba.fluss.exception.LogOffsetOutOfRangeException;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.record.ChangeType;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.RowType;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Reads the configured Fluss table's buckets through a single {@link LogScanner}, created lazily on
 * the first split assignment. This is a single-table connector: every split a reader receives
 * belongs to {@code config.getTablePath()} and is decoded with that table's row type. In bounded
 * mode a bucket is finished once its read position reaches the captured {@code endOffset}.
 *
 * <p>An {@code earliest} fresh split (batch or streaming) carries {@link
 * LogScanner#EARLIEST_OFFSET} (-2), which Fluss re-resolves server-side to the live log start on
 * every poll, so it is always in range and never overruns. A {@code latest} fresh split (tail
 * captured by the enumerator) and a restored split carry a concrete resume position; if retention
 * has discarded it — before it is subscribed or while a slow reader falls behind mid-stream — the
 * next {@link LogScanner#poll} throws an out-of-range {@link FetchException} (Fluss has no
 * client-side auto-reset). The reader resets any bucket now behind its earliest to the current
 * earliest (Kafka's {@code auto.offset.reset=earliest}); best-effort, since retention may advance
 * again before the next poll. If that pass resets nothing the out-of-range is not a recoverable
 * front-side truncation (e.g. a position beyond latest after a table recreate / rollback / dirty
 * offset), so the reader rethrows to fail the job rather than re-poll the same exception forever.
 *
 * <p>A bounded bucket that retention truncates empty is the one case the reset cannot see: the -2
 * sentinel it subscribes at never overruns, so no exception fires, yet the bucket yields no record
 * and its {@code nextOffset} never reaches {@code endOffset}. {@link
 * #completeDrainedSentinelBuckets(TableScan)} covers it by finishing a still-sentinel bounded
 * bucket whose current earliest has reached its end. Empty bounded buckets are also filtered out by
 * the enumerator at discovery.
 */
@Slf4j
public class FlussSourceSplitReader implements SplitReader<FlussRecord, FlussSourceSplit> {

    private final FlussSourceConfig config;
    private final Duration pollTimeout;

    private FlussAdminClient adminClient;

    /** The configured table's scan state, created lazily on the first split assignment. */
    private volatile TableScan tableScan;

    /** Used to detect bounded completion and to spot positions a retention reset has to recover. */
    private final Map<TableBucket, BucketReader> assigned = new HashMap<>();

    private final Set<String> drainedSplits = new HashSet<>();

    public FlussSourceSplitReader(FlussSourceConfig config) {
        this.config = config;
        this.pollTimeout = Duration.ofMillis(config.getPollTimeoutMs());
    }

    FlussAdminClient adminClient() {
        if (adminClient == null) {
            adminClient =
                    new FlussAdminClient(
                            config.buildFlussConfig(), config.getTablePath().getFullName());
        }
        return adminClient;
    }

    @Override
    public RecordsWithSplitIds<FlussRecord> fetch() throws IOException {
        Map<String, Collection<FlussRecord>> recordsBySplit = new HashMap<>();
        Set<String> finishedSplits = new HashSet<>();

        TableScan scan = tableScan;
        if (scan != null) {
            ScanRecords scanRecords = null;
            try {
                scanRecords = scan.logScanner.poll(pollTimeout);
            } catch (FetchException e) {
                if (!(e.getCause() instanceof LogOffsetOutOfRangeException)) {
                    throw e;
                }
                // A subscribed offset was discarded by retention. Reset the bucket(s) now behind
                // their earliest and retry on the next fetch().
                log.warn(
                        "Out-of-range offset while polling table {}; resetting truncated buckets to earliest",
                        scan.tablePath.getFullName(),
                        e);
                if (!resetTruncatedBuckets(scan)) {
                    // Nothing was behind its earliest, so this out-of-range is not a recoverable
                    // front-side retention truncation. Fail with the original cause
                    // instead of re-polling and throwing the same exception forever.
                    throw e;
                }
            }
            if (scanRecords != null) {
                for (TableBucket bucket : scanRecords.buckets()) {
                    BucketReader reader = assigned.get(bucket);
                    if (reader == null || reader.done) {
                        // Unknown bucket, or a bucket already finished but still subscribed.
                        continue;
                    }
                    FlussSourceSplit split = reader.split;
                    Collection<FlussRecord> out = null;
                    for (ScanRecord record : scanRecords.records(bucket)) {
                        long offset = record.logOffset();
                        if (isBounded(split.getEndOffset()) && offset >= split.getEndOffset()) {
                            reader.nextOffset = offset;
                            break;
                        }
                        if (out == null) {
                            out =
                                    recordsBySplit.computeIfAbsent(
                                            split.splitId(), k -> new ArrayList<>());
                        }
                        out.add(new FlussRecord(convert(scan, record), offset));
                        reader.nextOffset = offset + 1;
                    }
                }
            }
            // Finish any bounded bucket retention truncated empty while still on the sentinel
            completeDrainedSentinelBuckets(scan);
        }

        if (!drainedSplits.isEmpty()) {
            finishedSplits.addAll(drainedSplits);
            drainedSplits.clear();
        }

        for (BucketReader reader : assigned.values()) {
            FlussSourceSplit split = reader.split;
            if (!reader.done
                    && isBounded(split.getEndOffset())
                    && reader.nextOffset >= split.getEndOffset()) {
                finishedSplits.add(split.splitId());
                // Mark finished but keep it tracked. Its records are
                // dropped by the done check above, and finishedSplits is only added once.
                reader.done = true;
            }
        }

        return new RecordsBySplits<>(recordsBySplit, finishedSplits);
    }

    static boolean isBounded(long endOffset) {
        return endOffset != Long.MAX_VALUE;
    }

    /**
     * Assignment-time drained check, needing no earliest lookup: a bounded split whose recorded
     * start already reaches its captured end offset has nothing left to read.
     *
     * <p>That a fully consumed ("finished") split can be handed back on restore looks impossible,
     * but {@code SourceReaderBase} removes a finished split's state <em>lazily</em>: the reader
     * reports the split in {@code finishedSplits}, yet the base only drops its state one {@code
     * pollNext} later (in {@code finishCurrentFetch}), after the split's last record has already
     * advanced {@code currentOffset} to the end. A checkpoint landing in that window persists a
     * just-completed split, and restoring it hands us back {@code start == end}. Finishing it here
     * skips a pointless subscribe + poll and avoids a misleading out-of-range warning should
     * retention have meanwhile passed that end.
     *
     * <p>The {@link LogScanner#EARLIEST_OFFSET} (-2) sentinel of a fresh split floors to 0, so a
     * fresh split is only drained when its end offset is 0 — an empty bucket, which the enumerator
     * already filters out, leaving that a defensive fallback.
     */
    static boolean isDrainedAtAssignment(long startOffset, long endOffset) {
        return isBounded(endOffset) && Math.max(startOffset, 0L) >= endOffset;
    }

    @Override
    public void handleSplitsChanges(SplitsChange<FlussSourceSplit> splitsChanges) {
        if (!(splitsChanges instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChanges.getClass()));
        }
        for (FlussSourceSplit split : splitsChanges.splits()) {
            long startOffset = split.getStartOffset();
            long endOffset = split.getEndOffset();
            if (isDrainedAtAssignment(startOffset, endOffset)) {
                drainedSplits.add(split.splitId());
                log.info(
                        "Split {} has nothing to read (startOffset={}, endOffset={}); marking finished without subscribing",
                        split.splitId(),
                        startOffset,
                        endOffset);
                continue;
            }
            if (tableScan == null) {
                tableScan = createTableScan();
            }
            TableBucket bucket = new TableBucket(split.getTableId(), split.getBucketId());
            tableScan.logScanner.subscribe(split.getBucketId(), startOffset);
            assigned.put(bucket, new BucketReader(split, startOffset));
            log.info("Subscribed to {} at offset {}", split, startOffset);
        }
    }

    /**
     * Resets buckets whose subscribed position retention discarded from the front, after a poll
     * threw out-of-range.
     *
     * @return whether any bucket was reset. {@code false} means no subscribed position was behind
     *     its earliest, so the out-of-range was not a recoverable front-side truncation.
     */
    private boolean resetTruncatedBuckets(TableScan scan) {
        List<BucketReader> candidates =
                assigned.values().stream()
                        .filter(reader -> reader.nextOffset >= 0)
                        .collect(Collectors.toList());
        if (candidates.isEmpty()) {
            return false;
        }
        Map<Integer, Long> earliestByBucket = earliestOffsets(scan.tablePath, candidates);
        boolean reset = false;
        for (BucketReader reader : candidates) {
            long earliest = earliestByBucket.get(reader.split.getBucketId());
            if (reader.nextOffset >= earliest) {
                continue;
            }
            scan.logScanner.subscribe(reader.split.getBucketId(), earliest);
            reader.nextOffset = earliest;
            reset = true;
        }
        return reset;
    }

    private void completeDrainedSentinelBuckets(TableScan scan) {
        List<BucketReader> sentinelBounded =
                assigned.values().stream()
                        .filter(
                                reader ->
                                        !reader.done
                                                && reader.nextOffset < 0
                                                && isBounded(reader.split.getEndOffset()))
                        .collect(Collectors.toList());
        if (sentinelBounded.isEmpty()) {
            return;
        }
        Map<Integer, Long> earliestByBucket = earliestOffsets(scan.tablePath, sentinelBounded);
        for (BucketReader reader : sentinelBounded) {
            long earliest = earliestByBucket.get(reader.split.getBucketId());
            if (earliest >= reader.split.getEndOffset()) {
                reader.nextOffset = earliest;
            }
        }
    }

    @SuppressWarnings("resource")
    private Map<Integer, Long> earliestOffsets(
            TablePath tablePath, Collection<BucketReader> readers) {
        List<Integer> buckets =
                readers.stream()
                        .map(r -> r.split.getBucketId())
                        .distinct()
                        .collect(Collectors.toList());
        return adminClient().earliestOffsets(tablePath, buckets);
    }

    @SuppressWarnings("resource")
    TableScan createTableScan() {
        TablePath tablePath = config.getTablePath();
        com.alibaba.fluss.metadata.TablePath flussTablePath =
                com.alibaba.fluss.metadata.TablePath.of(
                        tablePath.getDatabaseName(), tablePath.getTableName());
        Table table = adminClient().connection().getTable(flussTablePath);
        try {
            LogScanner logScanner = table.newScan().createLogScanner();
            return new TableScan(tablePath, table, logScanner, config.getFlussRowType());
        } catch (RuntimeException e) {
            try {
                table.close();
            } catch (Exception closeError) {
                e.addSuppressed(closeError);
            }
            throw e;
        }
    }

    private SeaTunnelRow convert(TableScan tableScan, ScanRecord record) {
        InternalRow internalRow = record.getRow();
        int arity = tableScan.fieldGetters.length;
        Object[] fields = new Object[arity];
        for (int i = 0; i < arity; i++) {
            fields[i] =
                    FlussTypeConverter.toSeaTunnelValue(
                            tableScan.fieldNames[i],
                            tableScan.fieldTypes[i],
                            tableScan.fieldGetters[i].getFieldOrNull(internalRow));
        }
        SeaTunnelRow row = new SeaTunnelRow(fields);
        row.setRowKind(toRowKind(record.getChangeType()));
        return row;
    }

    private static RowKind toRowKind(ChangeType changeType) {
        switch (changeType) {
            case UPDATE_BEFORE:
                return RowKind.UPDATE_BEFORE;
            case UPDATE_AFTER:
                return RowKind.UPDATE_AFTER;
            case DELETE:
                return RowKind.DELETE;
            case APPEND_ONLY:
            case INSERT:
            default:
                return RowKind.INSERT;
        }
    }

    @Override
    public void wakeUp() {
        TableScan scan = tableScan;
        if (scan != null) {
            scan.logScanner.wakeup();
        }
    }

    @Override
    public void close() throws Exception {
        TableScan scan = tableScan;
        tableScan = null;
        try {
            if (scan != null) {
                scan.close();
            }
        } finally {
            if (adminClient != null) {
                adminClient.close();
            }
        }
    }

    /** The mutable per-bucket read cursor. */
    private static final class BucketReader {
        private final FlussSourceSplit split;
        private long nextOffset;

        /**
         * True once this bounded bucket has reached its end offset. The reader stays tracked (not
         * removed) because {@code com.alibaba.fluss:fluss-client:0.7.0} can't per-bucket
         * unsubscribe a non-partitioned table: keeping it lets {@link
         * #resetTruncatedBuckets(TableScan)} clear a later retention truncation instead of spinning
         * on an out-of-range poll no one owns. Records from a done bucket are dropped in {@link
         * #fetch()}.
         */
        private boolean done;

        private BucketReader(FlussSourceSplit split, long nextOffset) {
            this.split = split;
            this.nextOffset = nextOffset;
        }
    }

    /** The configured table's scanning resources. */
    static class TableScan {
        private final TablePath tablePath;
        private final Table table;
        private final LogScanner logScanner;
        private final String[] fieldNames;
        private final DataType[] fieldTypes;
        private final InternalRow.FieldGetter[] fieldGetters;

        TableScan(TablePath tablePath, Table table, LogScanner logScanner, RowType rowType) {
            this.tablePath = tablePath;
            this.table = table;
            this.logScanner = logScanner;
            List<String> names = rowType.getFieldNames();
            int fieldCount = rowType.getFieldCount();
            this.fieldNames = new String[fieldCount];
            this.fieldTypes = new DataType[fieldCount];
            for (int i = 0; i < fieldCount; i++) {
                this.fieldNames[i] = names.get(i);
                this.fieldTypes[i] = rowType.getTypeAt(i);
            }
            this.fieldGetters = InternalRow.createFieldGetters(rowType);
        }

        private void close() throws Exception {
            try {
                logScanner.close();
            } finally {
                table.close();
            }
        }
    }
}
