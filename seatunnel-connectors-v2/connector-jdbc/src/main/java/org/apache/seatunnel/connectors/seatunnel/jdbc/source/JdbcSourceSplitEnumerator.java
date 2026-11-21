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

import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.common.utils.HashUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.event.JdbcSplitFinishedEvent;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.event.JdbcTableCloseStatus;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.event.JdbcTableFinishedEvent;
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

public class JdbcSourceSplitEnumerator
        implements SourceSplitEnumerator<JdbcSourceSplit, JdbcSourceState> {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcSourceSplitEnumerator.class);

    private final Map<TablePath, JdbcSourceTable> tables;
    private final ConcurrentLinkedQueue<TablePath> pendingTables;
    private final Map<Integer, List<JdbcSourceSplit>> pendingSplits;
    private final Map<TablePath, Integer> unfinishedSplitsPerTable;
    private final Map<TablePath, Set<String>> reportedSplitIds;
    /** Readers that have been assigned at least one split for a given table. */
    private final Map<TablePath, Set<Integer>> readersPerTable;
    /**
     * True when the restored checkpoint predates table-level tracking fields, so pre-run returned
     * reader splits must rebuild the missing table state.
     */
    private final boolean legacyTableStateRestore;

    private final boolean earlyCloseSuppressed;
    /** Counts reader-restored splits only during the old-checkpoint bootstrap phase. */
    private boolean countReturnedSplitsAsNew;

    private final ChunkSplitter splitter;
    private final Context<JdbcSourceSplit> context;
    private final Object stateLock = new Object();

    public JdbcSourceSplitEnumerator(
            Context<JdbcSourceSplit> context,
            JdbcSourceConfig jdbcSourceConfig,
            Map<TablePath, JdbcSourceTable> tables,
            JdbcSourceState sourceState) {
        this.context = context;
        this.tables = tables;
        this.splitter = ChunkSplitter.create(jdbcSourceConfig);
        this.reportedSplitIds =
                sourceState == null
                        ? new HashMap<>()
                        : copySets(sourceState.getReportedSplitIdsOrEmpty());
        if (sourceState == null) {
            this.pendingTables = new ConcurrentLinkedQueue<>(tables.keySet());
            this.pendingSplits = new HashMap<>();
            this.unfinishedSplitsPerTable = new HashMap<>();
            this.readersPerTable = new HashMap<>();
            this.legacyTableStateRestore = false;
        } else {
            this.pendingTables = new ConcurrentLinkedQueue<>(sourceState.getPendingTables());
            this.pendingSplits = new HashMap<>(sourceState.getPendingSplits());
            this.unfinishedSplitsPerTable =
                    new HashMap<>(sourceState.getUnfinishedSplitsPerTableOrEmpty());
            this.readersPerTable = copySets(sourceState.getReadersPerTableOrEmpty());
            this.legacyTableStateRestore = sourceState.isLegacyTableState();
        }
        // Zeta snapshots the enumerator before the readers. A count in a restored checkpoint
        // can include a split no restored reader still owns. Never use such counts as a quorum.
        this.earlyCloseSuppressed = sourceState != null;
        this.countReturnedSplitsAsNew = legacyTableStateRestore;
        if (unfinishedSplitsPerTable.isEmpty() && readersPerTable.isEmpty()) {
            rebuildTableStateFromPendingSplits();
        }
    }

    @Override
    public void open() {}

    @Override
    public void run() throws Exception {
        LOG.info("Starting split enumerator.");

        Set<Integer> readers = context.registeredReaders();
        synchronized (stateLock) {
            countReturnedSplitsAsNew = false;
        }
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

        LOG.info("No more splits to assign." + " Sending NoMoreSplitsEvent to reader {}.", readers);
        readers.forEach(context::signalNoMoreSplits);
    }

    @Override
    public void close() throws IOException {
        splitter.close();
    }

    @Override
    public void addSplitsBack(List<JdbcSourceSplit> splits, int subtaskId) {
        synchronized (stateLock) {
            if (!splits.isEmpty()) {
                addReturnedSplits(splits, subtaskId);
                if (context.registeredReaders().contains(subtaskId)) {
                    assignSplit(Collections.singletonList(subtaskId));
                } else {
                    LOG.warn(
                            "Reader {} is not registered. Pending splits {} are not assigned.",
                            subtaskId,
                            splits);
                }
            }
            LOG.info("Add back splits {} to JdbcSourceSplitEnumerator.", splits.size());
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        synchronized (stateLock) {
            return pendingTables.isEmpty() && pendingSplits.isEmpty() ? 0 : 1;
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        throw new JdbcConnectorException(
                CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                String.format("Unsupported handleSplitRequest: %d", subtaskId));
    }

    @Override
    public void registerReader(int subtaskId) {
        LOG.info("Register reader {} to JdbcSourceSplitEnumerator.", subtaskId);
        synchronized (stateLock) {
            if (!pendingSplits.isEmpty()) {
                assignSplit(Collections.singletonList(subtaskId));
            }
            resendFinishedTableEvent(subtaskId);
        }
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (sourceEvent instanceof JdbcTableCloseStatus.Request) {
            JdbcTableCloseStatus.Request request = (JdbcTableCloseStatus.Request) sourceEvent;
            Map<TablePath, Integer> finished = new HashMap<>();
            synchronized (stateLock) {
                for (TablePath table : request.getTables()) {
                    if (earlyCloseSuppressed) {
                        finished.put(table, 0);
                        continue;
                    }
                    Set<Integer> participants = readersPerTable.get(table);
                    if (Integer.valueOf(0).equals(unfinishedSplitsPerTable.get(table))
                            && participants != null
                            && participants.contains(subtaskId)) {
                        finished.put(table, participants.size());
                    }
                }
            }
            context.sendEventToSourceReader(
                    subtaskId,
                    new JdbcTableCloseStatus.Reply(request.getReaderAttempt(), finished));
            return;
        }
        if (!(sourceEvent instanceof JdbcSplitFinishedEvent)) {
            return;
        }
        JdbcSplitFinishedEvent splitFinishedEvent = (JdbcSplitFinishedEvent) sourceEvent;
        synchronized (stateLock) {
            TablePath tablePath = splitFinishedEvent.getTablePath();
            Integer remain = unfinishedSplitsPerTable.get(tablePath);
            if (remain == null) {
                if (legacyTableStateRestore) {
                    sendLegacyTableFinishedFallback(tablePath, subtaskId);
                    return;
                }
                LOG.debug(
                        "Ignore split finished event for unknown table {} from reader {}",
                        tablePath,
                        subtaskId);
                return;
            }
            if (remain == 0) {
                LOG.debug(
                        "Ignore duplicate split finished event for completed table {} from reader {}",
                        tablePath,
                        subtaskId);
                return;
            }
            String splitId = splitFinishedEvent.getSplitId();
            if (splitId != null
                    && !reportedSplitIds
                            .computeIfAbsent(tablePath, k -> new HashSet<>())
                            .add(splitId)) {
                return;
            }
            if (remain <= 1) {
                reportedSplitIds.remove(tablePath);
                unfinishedSplitsPerTable.put(tablePath, 0);
                sendTableFinishedEvent(tablePath, subtaskId);
            } else {
                unfinishedSplitsPerTable.put(splitFinishedEvent.getTablePath(), remain - 1);
            }
        }
    }

    @Override
    public JdbcSourceState snapshotState(long checkpointId) throws Exception {
        synchronized (stateLock) {
            return new JdbcSourceState(
                    new ArrayList<>(pendingTables),
                    new HashMap<>(pendingSplits),
                    new HashMap<>(unfinishedSplitsPerTable),
                    copySets(readersPerTable),
                    copySets(reportedSplitIds));
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}

    private void assignSplit(Collection<Integer> readers) {
        LOG.debug("Assign pendingSplits to readers {}", readers);

        for (int reader : readers) {
            List<JdbcSourceSplit> assignmentForReader = pendingSplits.remove(reader);
            if (assignmentForReader != null && !assignmentForReader.isEmpty()) {
                LOG.debug("Assign splits {} to reader {}", assignmentForReader, reader);
                context.assignSplit(reader, assignmentForReader);
            }
        }
    }

    private void addPendingSplit(Collection<JdbcSourceSplit> splits) {
        int readerCount = context.currentParallelism();
        for (JdbcSourceSplit split : splits) {
            int ownerReader = getSplitOwner(split.splitId(), readerCount);
            LOG.debug("Assigning {} to {} reader.", split, ownerReader);
            addPendingSplit(split, ownerReader, true);
        }
    }

    private void addReturnedSplits(Collection<JdbcSourceSplit> splits, int ownerReader) {
        for (JdbcSourceSplit split : splits) {
            addPendingSplit(split, ownerReader, countReturnedSplitsAsNew);
        }
    }

    private static int getSplitOwner(String tp, int numReaders) {
        return HashUtils.bucketIndex(tp.hashCode(), numReaders);
    }

    private void rebuildTableStateFromPendingSplits() {
        pendingSplits.forEach(
                (reader, splits) -> {
                    if (splits == null) {
                        return;
                    }
                    for (JdbcSourceSplit split : splits) {
                        trackPendingSplitState(split, reader, true);
                    }
                });
    }

    private void addPendingSplit(JdbcSourceSplit split, int ownerReader, boolean countAsNewSplit) {
        trackPendingSplitState(split, ownerReader, countAsNewSplit);
        pendingSplits.computeIfAbsent(ownerReader, r -> new ArrayList<>()).add(split);
    }

    private void trackPendingSplitState(
            JdbcSourceSplit split, int ownerReader, boolean countAsNewSplit) {
        // Reader-state markers are not work and will never generate a split-finished report.
        if (countAsNewSplit && !split.isCloseTableMarker()) {
            unfinishedSplitsPerTable.merge(split.getTablePath(), 1, Integer::sum);
        }
        readersPerTable
                .computeIfAbsent(split.getTablePath(), key -> new HashSet<>())
                .add(ownerReader);
    }

    private void resendFinishedTableEvent(int subtaskId) {
        readersPerTable.forEach(
                (tablePath, readers) -> {
                    Integer remain = unfinishedSplitsPerTable.get(tablePath);
                    if (remain != null && remain == 0 && readers.contains(subtaskId)) {
                        context.sendEventToSourceReader(
                                subtaskId,
                                new JdbcTableFinishedEvent(
                                        tablePath, earlyCloseSuppressed ? 0 : readers.size()));
                    }
                });
    }

    /**
     * Sends the table-finished event to every reader that may still need to emit its close-table
     * marker downstream.
     */
    private void sendTableFinishedEvent(TablePath tablePath, int fallbackReader) {
        Set<Integer> readers = readersPerTable.get(tablePath);
        if (readers == null || readers.isEmpty()) {
            readers = Collections.singleton(fallbackReader);
            readersPerTable.put(tablePath, new HashSet<>(readers));
        }
        int expectedCloseEventCount = earlyCloseSuppressed ? 0 : readers.size();
        for (Integer reader : readers) {
            context.sendEventToSourceReader(
                    reader, new JdbcTableFinishedEvent(tablePath, expectedCloseEventCount));
        }
    }

    /**
     * Degrades old checkpoints without table tracking to a registered-reader broadcast so restored
     * readers do not wait forever for a close signal that the old enumerator state could not store.
     */
    private void sendLegacyTableFinishedFallback(TablePath tablePath, int reportingReader) {
        Set<Integer> readers = readersPerTable.computeIfAbsent(tablePath, key -> new HashSet<>());
        readers.add(reportingReader);
        readers.addAll(context.registeredReaders());
        unfinishedSplitsPerTable.put(tablePath, 0);
        LOG.info(
                "Broadcast restored legacy table {} completion to readers {} because old"
                        + " checkpoint state did not contain per-table split tracking.",
                tablePath,
                readers);
        sendTableFinishedEvent(tablePath, reportingReader);
    }

    private static <T> Map<TablePath, Set<T>> copySets(Map<TablePath, Set<T>> source) {
        Map<TablePath, Set<T>> copied = new HashMap<>();
        source.forEach((tablePath, readers) -> copied.put(tablePath, new HashSet<>(readers)));
        return copied;
    }
}
