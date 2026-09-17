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

package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.utils.HashUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSourceState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * JDBC Source split enumerator.
 *
 * <p>Splits are generated lazily (even arithmetic / uneven index probing) and retained in a bounded
 * pending buffer ({@code split.max-pending-splits}). Assignment is batched ({@code
 * split.assign.batch-size}); readers pull more work through {@link #handleSplitRequest(int)}.
 */
public class JdbcSourceSplitEnumerator
        implements SourceSplitEnumerator<JdbcSourceSplit, JdbcSourceState> {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcSourceSplitEnumerator.class);

    private final Map<TablePath, JdbcSourceTable> tables;
    private final ConcurrentLinkedQueue<TablePath> pendingTables;
    private final Map<Integer, List<JdbcSourceSplit>> pendingSplits;
    private final Set<Integer> noMoreSplitsSignaled;
    private final ChunkSplitter splitter;
    private final Context<JdbcSourceSplit> context;
    private final int assignBatchSize;
    private final int maxPendingSplits;
    private final Object stateLock = new Object();
    private volatile boolean enumerationFinished;
    /** True after {@link #run()} has executed at least once. */
    private volatile boolean runCalled;

    public JdbcSourceSplitEnumerator(
            Context<JdbcSourceSplit> context,
            JdbcSourceConfig jdbcSourceConfig,
            Map<TablePath, JdbcSourceTable> tables,
            JdbcSourceState sourceState) {
        this.context = context;
        this.tables = tables;
        this.splitter = ChunkSplitter.create(jdbcSourceConfig);
        int configuredBatchSize = jdbcSourceConfig.getSplitAssignBatchSize();
        this.assignBatchSize =
                configuredBatchSize > 0
                        ? configuredBatchSize
                        : JdbcSourceOptions.SPLIT_ASSIGN_BATCH_SIZE.defaultValue();
        int configuredMaxPending = jdbcSourceConfig.getSplitMaxPendingSplits();
        this.maxPendingSplits =
                configuredMaxPending > 0
                        ? configuredMaxPending
                        : JdbcSourceOptions.SPLIT_MAX_PENDING_SPLITS.defaultValue();
        this.noMoreSplitsSignaled = new HashSet<>();
        if (sourceState == null) {
            this.pendingTables = new ConcurrentLinkedQueue<>(tables.keySet());
            this.pendingSplits = new HashMap<>();
            this.enumerationFinished = false;
        } else {
            this.pendingTables = new ConcurrentLinkedQueue<>(sourceState.getPendingTables());
            this.pendingSplits = new HashMap<>(sourceState.getPendingSplits());
            if (sourceState.getGeneratorState() != null
                    && sourceState.getGeneratorState().getTablePath() != null) {
                TablePath generatorTable = sourceState.getGeneratorState().getTablePath();
                JdbcSourceTable table = tables.get(generatorTable);
                if (table != null) {
                    splitter.restoreGeneratorState(sourceState.getGeneratorState(), table);
                } else {
                    LOG.warn(
                            "Restored generator state for missing table {}, ignoring cursor.",
                            generatorTable);
                }
            }
            this.enumerationFinished = isGenerationComplete() && totalPendingSplits() == 0;
        }
    }

    @Override
    public void open() {}

    @Override
    public void run() throws Exception {
        LOG.info(
                "Starting split enumerator with assign batch size {} and max pending splits {}.",
                assignBatchSize,
                maxPendingSplits);

        Set<Integer> readers = context.registeredReaders();
        synchronized (stateLock) {
            runCalled = true;
            refillPendingUpToWatermark();
            assignAndMaybeFinish(readers);
        }
        LOG.info("Initial split enumeration pass finished for readers {}.", readers);
    }

    @Override
    public void close() throws IOException {
        splitter.close();
    }

    @Override
    public void addSplitsBack(List<JdbcSourceSplit> splits, int subtaskId) {
        if (!splits.isEmpty()) {
            synchronized (stateLock) {
                addPendingSplit(splits, subtaskId);
                if (context.registeredReaders().contains(subtaskId)) {
                    assignSplit(Collections.singletonList(subtaskId));
                    maybeSignalNoMoreSplits(Collections.singletonList(subtaskId));
                } else {
                    LOG.warn(
                            "Reader {} is not registered. Pending splits {} are not assigned.",
                            subtaskId,
                            splits.size());
                }
            }
        }
        LOG.info("Add back splits {} to JdbcSourceSplitEnumerator.", splits.size());
    }

    @Override
    public int currentUnassignedSplitSize() {
        synchronized (stateLock) {
            int unassigned = totalPendingSplits();
            int remainingTables = pendingTables.size();
            int activeGenerator = splitter.hasNext() ? 1 : 0;
            return unassigned + remainingTables + activeGenerator;
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        synchronized (stateLock) {
            if (!context.registeredReaders().contains(subtaskId)) {
                LOG.warn(
                        "Reader {} is not registered. Split request is ignored; pending splits are retained.",
                        subtaskId);
                return;
            }
            try {
                refillPendingUpToWatermark();
            } catch (Exception e) {
                throw new RuntimeException("Failed to refill pending jdbc splits", e);
            }
            // Refill buckets splits by global hash ownership. Assign to every registered reader so
            // a request from one reader cannot leave another reader's pending splits stranded
            // while that reader is waiting on an in-flight split request.
            assignAndMaybeFinish(context.registeredReaders());
        }
    }

    @Override
    public void registerReader(int subtaskId) {
        LOG.info("Register reader {} to JdbcSourceSplitEnumerator.", subtaskId);
        synchronized (stateLock) {
            try {
                refillPendingUpToWatermark();
            } catch (Exception e) {
                throw new RuntimeException("Failed to refill pending jdbc splits", e);
            }
            assignAndMaybeFinish(context.registeredReaders());
        }
    }

    /**
     * Assigns pending batches to the given readers and, once generation is complete, signals {@code
     * NoMoreSplits} where appropriate.
     */
    private void assignAndMaybeFinish(Collection<Integer> readers) {
        assignSplit(readers);
        if (runCalled && isGenerationComplete()) {
            enumerationFinished = true;
        }
        maybeSignalNoMoreSplits(readers);
    }

    @Override
    public JdbcSourceState snapshotState(long checkpointId) throws Exception {
        synchronized (stateLock) {
            return new JdbcSourceState(
                    new ArrayList<>(pendingTables),
                    new HashMap<>(pendingSplits),
                    splitter.snapshotGeneratorState());
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}

    /**
     * Lazily generates splits until the global pending buffer reaches {@code maxPendingSplits} or
     * all tables are exhausted.
     */
    private void refillPendingUpToWatermark() throws Exception {
        while (totalPendingSplits() < maxPendingSplits) {
            if (!splitter.hasNext()) {
                TablePath tablePath = pendingTables.poll();
                if (tablePath == null) {
                    return;
                }
                LOG.info("Opening lazy split generator for table {}.", tablePath);
                splitter.open(tables.get(tablePath));
                if (!splitter.hasNext()) {
                    continue;
                }
            }
            JdbcSourceSplit split = splitter.nextSplit();
            addPendingSplit(Collections.singletonList(split));
        }
    }

    private boolean isGenerationComplete() {
        return pendingTables.isEmpty() && !splitter.hasNext();
    }

    private int totalPendingSplits() {
        return pendingSplits.values().stream().mapToInt(List::size).sum();
    }

    private void assignSplit(Collection<Integer> readers) {
        for (int reader : readers) {
            List<JdbcSourceSplit> ownedPending =
                    pendingSplits.computeIfAbsent(reader, r -> new ArrayList<>());
            if (ownedPending.isEmpty()) {
                continue;
            }

            int batchCount = Math.min(assignBatchSize, ownedPending.size());
            List<JdbcSourceSplit> assignment = new ArrayList<>(ownedPending.subList(0, batchCount));
            ownedPending.subList(0, batchCount).clear();
            if (ownedPending.isEmpty()) {
                pendingSplits.remove(reader);
            }

            LOG.debug(
                    "Assign {} splits (batch size {}) to reader {}; {} still pending.",
                    assignment.size(),
                    assignBatchSize,
                    reader,
                    ownedPending.size());
            context.assignSplit(reader, assignment);
        }
    }

    /**
     * Signals {@code NoMoreSplits} to readers that have no remaining enumerator-side backlog.
     *
     * <p>Invariant: signals at most once per reader, and only after enumeration has finished
     * ({@code enumerationFinished == true}) and that reader's {@code pendingSplits} queue is empty.
     */
    private void maybeSignalNoMoreSplits(Collection<Integer> readers) {
        if (!enumerationFinished) {
            return;
        }
        for (int reader : readers) {
            List<JdbcSourceSplit> ownedPending = pendingSplits.get(reader);
            boolean hasPending = ownedPending != null && !ownedPending.isEmpty();
            if (!hasPending && noMoreSplitsSignaled.add(reader)) {
                LOG.info("Sending NoMoreSplitsEvent to reader {}.", reader);
                context.signalNoMoreSplits(reader);
            }
        }
        if (enumerationFinished && totalPendingSplits() == 0 && isGenerationComplete()) {
            splitter.close();
        }
    }

    private void addPendingSplit(Collection<JdbcSourceSplit> splits) {
        int readerCount = context.currentParallelism();
        for (JdbcSourceSplit split : splits) {
            int ownerReader = getSplitOwner(split.splitId(), readerCount);
            LOG.debug("Assigning {} to {} reader.", split, ownerReader);
            pendingSplits.computeIfAbsent(ownerReader, r -> new ArrayList<>()).add(split);
        }
    }

    private void addPendingSplit(Collection<JdbcSourceSplit> splits, int ownerReader) {
        pendingSplits.computeIfAbsent(ownerReader, r -> new ArrayList<>()).addAll(splits);
    }

    private static int getSplitOwner(String tp, int numReaders) {
        return HashUtils.bucketIndex(tp.hashCode(), numReaders);
    }
}
