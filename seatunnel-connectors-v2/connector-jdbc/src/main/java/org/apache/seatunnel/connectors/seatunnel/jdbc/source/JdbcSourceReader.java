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

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.event.CloseTableEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcInputFormat;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.event.JdbcSplitFinishedEvent;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.event.JdbcTableCloseStatus;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.event.JdbcTableFinishedEvent;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

@Slf4j
public class JdbcSourceReader implements SourceReader<SeaTunnelRow, JdbcSourceSplit> {
    private static final int SPLIT_PROGRESS_LOG_INTERVAL = 50;

    static final int MAX_UNANSWERED_STATUS_REQUESTS = 12;
    static final long STATUS_RETRY_NANOS = TimeUnit.SECONDS.toNanos(5);
    private static final long IDLE_POLL_MILLIS = 1000;
    private final Object stateLock = new Object();
    private final LongSupplier nanoTime;
    private int unansweredRequests;
    private long lastRequestTime;
    private boolean statusRequested;
    private final Set<TablePath> receivedFinishedTables = new HashSet<>();
    private final Context context;
    private final JdbcInputFormat inputFormat;
    private final Deque<JdbcSourceSplit> splits = new ArrayDeque<>();
    private final Deque<CloseTableEvent> pendingCloseTableEvents = new ArrayDeque<>();
    /** Tables this reader has seen locally but has not yet received the global close signal for. */
    private final Set<TablePath> pendingGlobalCloseTables = new HashSet<>();

    private boolean noMoreSplit;
    private final AtomicInteger assignedSplitCount = new AtomicInteger();
    private final AtomicInteger processedSplitCount = new AtomicInteger();

    public JdbcSourceReader(
            Context context, JdbcSourceConfig config, Map<TablePath, CatalogTable> tables) {
        this(context, new JdbcInputFormat(config, tables));
    }

    JdbcSourceReader(Context context, JdbcInputFormat inputFormat) {
        this(context, inputFormat, System::nanoTime);
    }

    JdbcSourceReader(Context context, JdbcInputFormat inputFormat, LongSupplier nanoTime) {
        this.nanoTime = nanoTime;
        this.context = context;
        this.inputFormat = inputFormat;
    }

    @Override
    public void open() throws Exception {
        inputFormat.openInputFormat();
    }

    @Override
    public void cancel() {
        inputFormat.cancel();
    }

    @Override
    public void close() throws IOException {
        inputFormat.closeInputFormat();
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        JdbcTableCloseStatus.Request statusRequest = null;
        synchronized (output.getCheckpointLock()) {
            CloseTableEvent closeTableEvent;
            JdbcSourceSplit split;
            boolean finished;
            synchronized (stateLock) {
                closeTableEvent = pendingCloseTableEvents.poll();
                split = closeTableEvent == null ? splits.poll() : null;
                finished = noMoreSplit && pendingGlobalCloseTables.isEmpty();
                if (closeTableEvent == null && split == null && noMoreSplit && !finished) {
                    long now = nanoTime.getAsLong();
                    if (!statusRequested || now - lastRequestTime >= STATUS_RETRY_NANOS) {
                        if (unansweredRequests >= MAX_UNANSWERED_STATUS_REQUESTS) {
                            throw new IOException(
                                    "No table-close status reply after "
                                            + unansweredRequests
                                            + " requests; still waiting for "
                                            + pendingGlobalCloseTables);
                        }
                        statusRequested = true;
                        unansweredRequests++;
                        lastRequestTime = now;
                        statusRequest = new JdbcTableCloseStatus.Request(pendingGlobalCloseTables);
                    }
                }
            }
            if (closeTableEvent != null) {
                output.collect(closeTableEvent);
                return;
            }
            if (split != null) {
                try {
                    inputFormat.open(split);
                    while (!inputFormat.reachedEnd()) {
                        output.collect(inputFormat.nextRecord());
                    }
                } finally {
                    inputFormat.close();
                }
                // A failed or partially consumed split must not count as table progress.
                context.sendSourceEventToEnumerator(
                        new JdbcSplitFinishedEvent(split.getTablePath(), split.splitId()));
                int processedCount = processedSplitCount.incrementAndGet();
                if (processedCount % SPLIT_PROGRESS_LOG_INTERVAL == 0) {
                    log.info(
                            "Processed {} of {} assigned jdbc source splits",
                            processedCount,
                            assignedSplitCount.get());
                }
                return;
            }
            if (finished) {
                log.info("Closed the bounded jdbc source");
                context.signalNoMoreElement();
                return;
            }
        }
        // Neither idle waiting nor a reverse-event round trip should hold the checkpoint lock or
        // the reader state lock. A reply with no finished tables is a valid heartbeat.
        if (statusRequest != null) {
            context.sendSourceEventToEnumerator(statusRequest);
        } else {
            Thread.sleep(IDLE_POLL_MILLIS);
        }
    }

    @Override
    public List<JdbcSourceSplit> snapshotState(long checkpointId) throws Exception {
        synchronized (stateLock) {
            List<JdbcSourceSplit> snapshot = new ArrayList<>(splits);
            Set<TablePath> tablesWithSnapshotState = new HashSet<>();
            for (JdbcSourceSplit split : snapshot) {
                tablesWithSnapshotState.add(split.getTablePath());
            }
            for (CloseTableEvent closeTableEvent : pendingCloseTableEvents) {
                snapshot.add(
                        JdbcSourceSplit.forCloseTableState(
                                closeTableEvent.getTablePath(),
                                closeTableEvent.getExpectedSourceEventCount()));
                tablesWithSnapshotState.add(closeTableEvent.getTablePath());
            }
            for (TablePath tablePath : pendingGlobalCloseTables) {
                if (!tablesWithSnapshotState.contains(tablePath)) {
                    snapshot.add(JdbcSourceSplit.forCloseTableState(tablePath, 0));
                }
            }
            return snapshot;
        }
    }

    @Override
    public void addSplits(List<JdbcSourceSplit> splits) {
        synchronized (stateLock) {
            for (JdbcSourceSplit split : splits) {
                if (split.isCloseTableMarker()) {
                    restoreCloseTableState(split);
                    continue;
                }
                if (!receivedFinishedTables.contains(split.getTablePath())) {
                    pendingGlobalCloseTables.add(split.getTablePath());
                }
                this.splits.add(split);
                // Progress logging counts only splits that are queued for reading; close-table
                // markers only restore state and never reach splits.poll().
                this.assignedSplitCount.incrementAndGet();
            }
        }
    }

    @Override
    public void handleNoMoreSplits() {
        synchronized (stateLock) {
            noMoreSplit = true;
        }
    }

    @Override
    public void handleSourceEvent(SourceEvent sourceEvent) {
        if (!(sourceEvent instanceof JdbcTableFinishedEvent)
                && !(sourceEvent instanceof JdbcTableCloseStatus.Reply)) {
            return;
        }
        int readerId = context.getIndexOfSubtask();
        synchronized (stateLock) {
            if (sourceEvent instanceof JdbcTableFinishedEvent) {
                JdbcTableFinishedEvent event = (JdbcTableFinishedEvent) sourceEvent;
                enqueueFinished(event.getTablePath(), readerId, event.getExpectedCloseEventCount());
            } else {
                unansweredRequests = 0;
                ((JdbcTableCloseStatus.Reply) sourceEvent)
                        .getFinishedTables()
                        .forEach((table, count) -> enqueueFinished(table, readerId, count));
            }
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}

    private void restoreCloseTableState(JdbcSourceSplit split) {
        if (split.isPendingCloseTableEvent()) {
            enqueueFinished(
                    split.getTablePath(),
                    context.getIndexOfSubtask(),
                    split.getExpectedCloseEventCount());
        } else if (!receivedFinishedTables.contains(split.getTablePath())) {
            pendingGlobalCloseTables.add(split.getTablePath());
        }
    }

    /** Called only under stateLock: the terminal decision must never see half this transition. */
    private void enqueueFinished(TablePath table, int readerId, int expectedCount) {
        if (receivedFinishedTables.add(table) && expectedCount > 0) {
            pendingCloseTableEvents.add(new CloseTableEvent(table, readerId, expectedCount));
        }
        pendingGlobalCloseTables.remove(table);
    }
}
