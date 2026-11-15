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

package org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.enumerator;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.config.TiDBSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.split.TiDBSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.utils.TableKeyRangeUtils;

import org.tikv.common.TiSession;
import org.tikv.kvproto.Coprocessor;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class TiDBSourceSplitEnumerator
        implements SourceSplitEnumerator<TiDBSourceSplit, TiDBSourceCheckpointState> {

    private static final String CDC_DIAG_PREFIX = "[TiDB-CDC-DIAG]";

    private final TiDBSourceConfig sourceConfig;
    private final Map<Integer, List<TiDBSourceSplit>> pendingSplit;
    // Persist the round-robin cursor so restored enumerators keep assigning newly discovered
    // splits in the same sequence instead of skewing ownership after a checkpoint restore.
    private final AtomicInteger assignCount = new AtomicInteger(0);
    private final Context<TiDBSourceSplit> context;
    private TiSession tiSession;
    private long tableId;

    private volatile boolean shouldEnumerate;

    private final Object stateLock = new Object();

    public TiDBSourceSplitEnumerator(
            @NonNull Context<TiDBSourceSplit> context, @NonNull TiDBSourceConfig sourceConfig) {
        this(context, sourceConfig, null);
    }

    public TiDBSourceSplitEnumerator(
            @NonNull Context<TiDBSourceSplit> context,
            @NonNull TiDBSourceConfig sourceConfig,
            TiDBSourceCheckpointState restoreState) {
        this.context = context;
        this.sourceConfig = sourceConfig;
        this.pendingSplit = new HashMap<>();
        this.shouldEnumerate = (restoreState == null);
        if (restoreState != null) {
            this.shouldEnumerate = restoreState.isShouldEnumerate();
            this.pendingSplit.putAll(restoreState.getPendingSplit());
            this.assignCount.set(restoreState.getAssignCount());
        }
    }

    @Override
    public void open() {
        this.tiSession = TiSession.create(sourceConfig.getTiConfiguration());
        this.tableId =
                this.tiSession
                        .getCatalog()
                        .getTable(sourceConfig.getDatabaseName(), sourceConfig.getTableName())
                        .getId();
        log.info(
                "{} Enumerator opened, database={}, table={}, tableId={}, startupMode={},"
                        + " parallelism={}.",
                CDC_DIAG_PREFIX,
                sourceConfig.getDatabaseName(),
                sourceConfig.getTableName(),
                tableId,
                sourceConfig.getStartupMode(),
                context.currentParallelism());
    }

    /** The method is executed by the engine only once. */
    @Override
    public void run() throws Exception {
        Set<Integer> readers = context.registeredReaders();
        if (shouldEnumerate) {
            List<TiDBSourceSplit> sourceSplits = getTiDBSourceSplit();
            log.info(
                    "{} Enumerated TiDB CDC splits, database={}, table={}, splitCount={}.",
                    CDC_DIAG_PREFIX,
                    sourceConfig.getDatabaseName(),
                    sourceConfig.getTableName(),
                    sourceSplits.size());
            synchronized (stateLock) {
                addPendingSplit(sourceSplits);
                shouldEnumerate = false;
                assignSplit(readers);
            }
        }
        log.debug(
                "No more splits to assign." + " Sending NoMoreSplitsEvent to reader {}.", readers);
        readers.forEach(context::signalNoMoreSplits);
    }

    private synchronized void addPendingSplit(List<TiDBSourceSplit> splits) {
        splits.stream()
                .sorted(Comparator.comparing(TiDBSourceSplit::splitId))
                .forEach(
                        split ->
                                pendingSplit
                                        .computeIfAbsent(
                                                getSplitOwner(
                                                        assignCount.getAndIncrement(),
                                                        context.currentParallelism()),
                                                ignored -> new ArrayList<>())
                                        .add(split));
    }

    private synchronized void addPendingSplit(List<TiDBSourceSplit> splits, int ownerReader) {
        pendingSplit.computeIfAbsent(ownerReader, ignored -> new ArrayList<>()).addAll(splits);
    }

    private void assignSplit(Collection<Integer> readers) {
        for (Integer reader : readers) {
            final List<TiDBSourceSplit> assignmentForReader = pendingSplit.remove(reader);
            if (assignmentForReader != null && !assignmentForReader.isEmpty()) {
                log.info(
                        "{} Assign split to reader, reader={}, splits={}, remainingPending={}.",
                        CDC_DIAG_PREFIX,
                        reader,
                        assignmentForReader,
                        currentUnassignedSplitSize());
                context.assignSplit(reader, assignmentForReader);
            }
        }
    }

    private static int getSplitOwner(int assignCount, int numReaders) {
        return assignCount % numReaders;
    }

    private List<TiDBSourceSplit> getTiDBSourceSplit() {
        List<TiDBSourceSplit> sourceSplits = new ArrayList<>();
        List<Coprocessor.KeyRange> keyRanges =
                TableKeyRangeUtils.getTableKeyRanges(this.tableId, context.currentParallelism());
        for (Coprocessor.KeyRange keyRange : keyRanges) {
            sourceSplits.add(
                    new TiDBSourceSplit(
                            sourceConfig.getDatabaseName(),
                            sourceConfig.getTableName(),
                            keyRange,
                            sourceConfig.getStartupMode() == StartupMode.INITIAL ? -1 : 0,
                            keyRange.getStart(),
                            false));
        }
        return sourceSplits;
    }

    /**
     * Called to close the enumerator, in case it holds on to any resources, like threads or network
     * connections.
     */
    @Override
    public void close() throws IOException {
        if (this.tiSession != null) {
            try {
                this.tiSession.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Add a split back to the split enumerator. It will only happen when a {@link SourceReader}
     * fails and there are splits assigned to it after the last successful checkpoint.
     *
     * @param splits The split to add back to the enumerator for reassignment.
     * @param subtaskId The id of the subtask to which the returned splits belong.
     */
    @Override
    public void addSplitsBack(List<TiDBSourceSplit> splits, int subtaskId) {
        log.debug("Add back splits {} to TiDBSourceSplitEnumerator.", splits);
        if (!splits.isEmpty()) {
            addPendingSplit(splits, subtaskId);
            if (context.registeredReaders().contains(subtaskId)) {
                assignSplit(Collections.singletonList(subtaskId));
            } else {
                log.warn(
                        "Reader {} is not registered. Pending splits {} are not assigned.",
                        subtaskId,
                        splits);
            }
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplit.values().stream().mapToInt(List::size).sum();
    }

    @Override
    public void handleSplitRequest(int subtaskId) {}

    @Override
    public void registerReader(int subtaskId) {
        log.debug("Register reader {} to TiDBSourceSplitEnumerator.", subtaskId);
        if (!pendingSplit.isEmpty()) {
            assignSplit(Collections.singletonList(subtaskId));
        }
    }

    /**
     * If the source is bounded, checkpoint is not triggered.
     *
     * @param checkpointId
     */
    @Override
    public TiDBSourceCheckpointState snapshotState(long checkpointId) throws Exception {
        synchronized (stateLock) {
            return new TiDBSourceCheckpointState(shouldEnumerate, pendingSplit, assignCount.get());
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // do nothing
    }
}
