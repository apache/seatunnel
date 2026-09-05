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
 * <p>Split generation keeps the existing chunking predicates and split IDs. Assignment is batched
 * so each reader only receives up to {@code split.assign.batch-size} splits per handoff. Readers
 * pull the next batch through {@link #handleSplitRequest(int)} until enumeration is finished and
 * that reader's pending queue is empty, then {@code NoMoreSplits} is signaled.
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
    private final Object stateLock = new Object();
    private volatile boolean enumerationFinished;

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
        this.noMoreSplitsSignaled = new HashSet<>();
        if (sourceState == null) {
            this.pendingTables = new ConcurrentLinkedQueue<>(tables.keySet());
            this.pendingSplits = new HashMap<>();
            this.enumerationFinished = false;
        } else {
            this.pendingTables = new ConcurrentLinkedQueue<>(sourceState.getPendingTables());
            this.pendingSplits = new HashMap<>(sourceState.getPendingSplits());
            this.enumerationFinished = this.pendingTables.isEmpty();
        }
    }

    @Override
    public void open() {}

    @Override
    public void run() throws Exception {
        LOG.info("Starting split enumerator with assign batch size {}.", assignBatchSize);

        Set<Integer> readers = context.registeredReaders();
        while (!pendingTables.isEmpty()) {
            synchronized (stateLock) {
                TablePath tablePath = pendingTables.poll();
                LOG.info("Splitting table {}.", tablePath);

                Collection<JdbcSourceSplit> splits = splitter.generateSplits(tables.get(tablePath));
                LOG.info("Split table {} into {} splits.", tablePath, splits.size());

                addPendingSplit(splits);
            }

            synchronized (stateLock) {
                assignSplit(readers);
            }
        }

        splitter.close();

        synchronized (stateLock) {
            enumerationFinished = true;
            assignSplit(readers);
            maybeSignalNoMoreSplits(readers);
        }
        LOG.info("Split enumeration finished for readers {}.", readers);
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
            if (!pendingTables.isEmpty()) {
                return 1;
            }
            for (List<JdbcSourceSplit> splits : pendingSplits.values()) {
                if (splits != null && !splits.isEmpty()) {
                    return splits.size();
                }
            }
            return 0;
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        synchronized (stateLock) {
            assignSplit(Collections.singletonList(subtaskId));
            maybeSignalNoMoreSplits(Collections.singletonList(subtaskId));
        }
    }

    @Override
    public void registerReader(int subtaskId) {
        LOG.info("Register reader {} to JdbcSourceSplitEnumerator.", subtaskId);
        synchronized (stateLock) {
            assignSplit(Collections.singletonList(subtaskId));
            maybeSignalNoMoreSplits(Collections.singletonList(subtaskId));
        }
    }

    @Override
    public JdbcSourceState snapshotState(long checkpointId) throws Exception {
        synchronized (stateLock) {
            return new JdbcSourceState(
                    new ArrayList<>(pendingTables), new HashMap<>(pendingSplits));
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}

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
