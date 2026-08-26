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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.source.event.MaxcomputeCompletedSplitsReportEvent;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.util.MaxcomputeUtil;

import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
/**
 * Lazily assigns bounded MaxCompute source splits.
 *
 * <p>Only a small batch is materialized for each reader request. This prevents split metadata from
 * growing with the full source row count.
 */
public class MaxcomputeSourceSplitEnumerator
        implements SourceSplitEnumerator<MaxcomputeSourceSplit, MaxcomputeSourceState> {
    /** Maximum number of split metadata objects sent to one reader for a request. */
    private static final int SPLIT_BATCH_SIZE = 100;

    /** Runtime bridge used to assign split batches and signal source completion. */
    private final Context<MaxcomputeSourceSplit> enumeratorContext;

    /** Reader-returned splits waiting to be reassigned after recovery. */
    private final Map<Integer, Set<MaxcomputeSourceSplit>> pendingSplits;

    /** In-flight split metadata retained only until completion is safely reported. */
    private final Set<MaxcomputeSourceSplit> assignedSplits;

    /** Connector configuration used to open MaxCompute download sessions. */
    private final ReadonlyConfig readonlyConfig;

    /** Stable source-table order used by the persisted lazy split cursor. */
    private final List<SourceTableInfo> orderedSourceTableInfos;

    /** Source row counts resolved once before lazy split generation begins. */
    private final Map<TablePath, Long> tableRecordCounts;

    /** Serializes split assignment, completion events, and state snapshots. */
    private final Object stateLock = new Object();

    /** Next source table cursor for lazy split generation. */
    private int nextTableIndex;

    /** Next row offset within {@link #nextTableIndex}. */
    private long nextRowStart;

    /** Whether source row counts and the lazy generator are ready for reader requests. */
    private boolean splitDiscoveryComplete;

    /**
     * Identifies an eager checkpoint written before lazy assignment existed.
     *
     * <p>Its enumerator state cannot distinguish processed from unprocessed assigned splits, so
     * only reader-returned splits are safe to restore.
     */
    private boolean legacyEagerState;

    public MaxcomputeSourceSplitEnumerator(
            SourceSplitEnumerator.Context<MaxcomputeSourceSplit> enumeratorContext,
            ReadonlyConfig readonlyConfig,
            Map<TablePath, SourceTableInfo> sourceTableInfos) {
        this.enumeratorContext = enumeratorContext;
        this.readonlyConfig = readonlyConfig;
        this.pendingSplits = new HashMap<>();
        this.assignedSplits = new HashSet<>();
        this.orderedSourceTableInfos = new ArrayList<>(sourceTableInfos.values());
        this.orderedSourceTableInfos.sort(
                Comparator.comparing(
                        tableInfo -> tableInfo.getCatalogTable().getTablePath().getFullName()));
        this.tableRecordCounts = new HashMap<>();
    }

    public MaxcomputeSourceSplitEnumerator(
            SourceSplitEnumerator.Context<MaxcomputeSourceSplit> enumeratorContext,
            ReadonlyConfig readonlyConfig,
            Map<TablePath, SourceTableInfo> sourceTableInfos,
            MaxcomputeSourceState sourceState) {
        this(enumeratorContext, readonlyConfig, sourceTableInfos);
        this.legacyEagerState = !sourceState.isLazySplitAssignment();
        if (!legacyEagerState) {
            this.assignedSplits.addAll(sourceState.getAssignedSplit());
            restoreAssignedSplits(sourceState.getAssignedSplit());
        }
        this.nextTableIndex = sourceState.getNextTableIndex();
        this.nextRowStart = sourceState.getNextRowStart();
    }

    @Override
    public void open() {}

    @Override
    public void run() throws Exception {
        synchronized (stateLock) {
            initializeSplitDiscovery();
            for (int readerId : enumeratorContext.registeredReaders()) {
                assignSplitBatch(readerId);
            }
        }
    }

    @Override
    public void close() throws IOException {}

    @Override
    public void addSplitsBack(List<MaxcomputeSourceSplit> splits, int subtaskId) {
        synchronized (stateLock) {
            for (MaxcomputeSourceSplit split : splits) {
                if (!legacyEagerState) {
                    removePendingSplit(split);
                }
                if (split.isFinished()) {
                    assignedSplits.remove(split);
                    continue;
                }
                if (!legacyEagerState) {
                    assignedSplits.add(split);
                }
                pendingSplits
                        .computeIfAbsent(subtaskId, ignored -> new LinkedHashSet<>())
                        .add(split);
            }
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        synchronized (stateLock) {
            return pendingSplits.values().stream().mapToInt(Set::size).sum();
        }
    }

    @Override
    public void registerReader(int subtaskId) {}

    @Override
    public MaxcomputeSourceState snapshotState(long checkpointId) {
        synchronized (stateLock) {
            return new MaxcomputeSourceState(assignedSplits, nextTableIndex, nextRowStart);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    @Override
    public void handleSplitRequest(int subtaskId) {
        synchronized (stateLock) {
            if (!splitDiscoveryComplete) {
                throw new IllegalStateException(
                        "MaxCompute split discovery has not been initialized");
            }
            assignSplitBatch(subtaskId);
        }
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (!(sourceEvent instanceof MaxcomputeCompletedSplitsReportEvent)) {
            return;
        }

        MaxcomputeCompletedSplitsReportEvent reportEvent =
                (MaxcomputeCompletedSplitsReportEvent) sourceEvent;
        if (reportEvent.getCompletedSplits() == null
                || reportEvent.getCompletedSplits().isEmpty()) {
            return;
        }

        Set<String> completedSplitIds = new HashSet<>(reportEvent.getCompletedSplits());
        synchronized (stateLock) {
            assignedSplits.removeIf(split -> completedSplitIds.contains(split.splitId()));
        }
        log.debug(
                "Reader {} reported {} completed MaxCompute splits",
                subtaskId,
                completedSplitIds.size());
    }

    // visible for testing
    static Set<MaxcomputeSourceSplit> computeSplits(
            int numReaders,
            Collection<SourceTableInfo> sourceTableInfos,
            Map<TablePath, Long> tableRecordCounts) {
        Set<MaxcomputeSourceSplit> allSplit = new LinkedHashSet<>();
        int chunkIndex = 0;
        for (SourceTableInfo sourceTableInfo : sourceTableInfos) {
            TablePath tablePath = sourceTableInfo.getCatalogTable().getTablePath();
            long recordCount = tableRecordCounts.get(tablePath);
            int splitRow = MaxcomputeSourceOptions.SPLIT_ROW.defaultValue();
            if (sourceTableInfo.getSplitRow() != null && sourceTableInfo.getSplitRow() > 0) {
                splitRow = sourceTableInfo.getSplitRow();
            }
            for (long num = 0; num < recordCount; num += splitRow) {
                int ownerReader = chunkIndex % numReaders;
                allSplit.add(
                        new MaxcomputeSourceSplit(
                                num,
                                Math.min((long) splitRow, recordCount - num),
                                tablePath,
                                ownerReader));
                chunkIndex++;
            }
        }
        return allSplit;
    }

    /**
     * Resolves source row counts and converts legacy eager state before any split request.
     *
     * <p>Legacy assignment state includes completed splits, so the reader's restored split state is
     * the only safe source of unfinished work.
     */
    private void initializeSplitDiscovery() throws TunnelException {
        if (splitDiscoveryComplete) {
            return;
        }
        for (SourceTableInfo sourceTableInfo : orderedSourceTableInfos) {
            TableTunnel.DownloadSession session =
                    MaxcomputeUtil.getDownloadSession(
                            readonlyConfig,
                            sourceTableInfo.getCatalogTable().getTablePath(),
                            sourceTableInfo.getPartitionSpec());
            tableRecordCounts.put(
                    sourceTableInfo.getCatalogTable().getTablePath(), session.getRecordCount());
        }
        if (legacyEagerState) {
            // Reader state is the only reliable unfinished-split source for legacy checkpoints.
            nextTableIndex = orderedSourceTableInfos.size();
            nextRowStart = 0;
            assignedSplits.clear();
            legacyEagerState = false;
        }
        splitDiscoveryComplete = true;
    }

    /**
     * Assigns at most one bounded split batch to a reader.
     *
     * @param readerId reader subtask requesting work
     */
    private void assignSplitBatch(int readerId) {
        List<MaxcomputeSourceSplit> splits = pollPendingSplits(readerId, SPLIT_BATCH_SIZE);
        assignedSplits.addAll(splits);
        while (splits.size() < SPLIT_BATCH_SIZE) {
            MaxcomputeSourceSplit split = nextSplit(readerId);
            if (split == null) {
                break;
            }
            assignedSplits.add(split);
            splits.add(split);
        }

        if (!splits.isEmpty()) {
            log.debug("Assigning {} MaxCompute splits to reader {}", splits.size(), readerId);
            enumeratorContext.assignSplit(readerId, splits);
            return;
        }
        enumeratorContext.signalNoMoreSplits(readerId);
    }

    /**
     * Returns restored splits for a reader before generating new split metadata.
     *
     * @param readerId reader subtask requesting work
     * @param maxSplits maximum number of splits to return
     * @return pending split batch for the reader
     */
    private List<MaxcomputeSourceSplit> pollPendingSplits(int readerId, int maxSplits) {
        Set<MaxcomputeSourceSplit> pendingForReader = pendingSplits.get(readerId);
        if (pendingForReader == null || pendingForReader.isEmpty()) {
            return new ArrayList<>();
        }

        List<MaxcomputeSourceSplit> splits =
                new ArrayList<>(Math.min(maxSplits, pendingForReader.size()));
        Iterator<MaxcomputeSourceSplit> iterator = pendingForReader.iterator();
        while (iterator.hasNext() && splits.size() < maxSplits) {
            splits.add(iterator.next());
            iterator.remove();
        }
        if (pendingForReader.isEmpty()) {
            pendingSplits.remove(readerId);
        }
        return splits;
    }

    /**
     * Places lazy checkpoint assignments back into bounded pending queues.
     *
     * @param splits in-flight splits from a lazy checkpoint
     */
    private void restoreAssignedSplits(Collection<MaxcomputeSourceSplit> splits) {
        for (MaxcomputeSourceSplit split : splits) {
            int readerId = split.getIndex() % enumeratorContext.currentParallelism();
            pendingSplits.computeIfAbsent(readerId, ignored -> new LinkedHashSet<>()).add(split);
        }
    }

    /**
     * Removes a restored split before replacing it with the reader's authoritative state.
     *
     * @param split split being returned by a restored reader
     */
    private void removePendingSplit(MaxcomputeSourceSplit split) {
        pendingSplits.values().forEach(splits -> splits.remove(split));
    }

    /**
     * Materializes the next split from the persistent table and row cursors.
     *
     * @param readerId reader that will own the newly materialized split
     * @return the next split, or null once all source tables are exhausted
     */
    private MaxcomputeSourceSplit nextSplit(int readerId) {
        while (nextTableIndex < orderedSourceTableInfos.size()) {
            SourceTableInfo sourceTableInfo = orderedSourceTableInfos.get(nextTableIndex);
            TablePath tablePath = sourceTableInfo.getCatalogTable().getTablePath();
            long recordCount = tableRecordCounts.get(tablePath);
            if (nextRowStart >= recordCount) {
                nextTableIndex++;
                nextRowStart = 0;
                continue;
            }

            int splitRow = MaxcomputeSourceOptions.SPLIT_ROW.defaultValue();
            if (sourceTableInfo.getSplitRow() != null && sourceTableInfo.getSplitRow() > 0) {
                splitRow = sourceTableInfo.getSplitRow();
            }
            long rowStart = nextRowStart;
            long rowNum = Math.min((long) splitRow, recordCount - rowStart);
            nextRowStart += rowNum;

            MaxcomputeSourceSplit split =
                    new MaxcomputeSourceSplit(rowStart, rowNum, tablePath, readerId);
            return split;
        }
        return null;
    }
}
