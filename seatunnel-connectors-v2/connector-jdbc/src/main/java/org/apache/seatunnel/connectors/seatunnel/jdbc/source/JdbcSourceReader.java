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
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.event.JdbcTableFinishedEvent;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

@Slf4j
public class JdbcSourceReader implements SourceReader<SeaTunnelRow, JdbcSourceSplit> {
    private final Context context;
    private final JdbcInputFormat inputFormat;
    private final Deque<JdbcSourceSplit> splits = new ConcurrentLinkedDeque<>();
    private final Deque<CloseTableEvent> pendingCloseTableEvents = new ConcurrentLinkedDeque<>();
    /** Tables this reader has seen locally but has not yet received the global close signal for. */
    private final Set<TablePath> pendingGlobalCloseTables = ConcurrentHashMap.newKeySet();

    private volatile boolean noMoreSplit;

    public JdbcSourceReader(
            Context context, JdbcSourceConfig config, Map<TablePath, CatalogTable> tables) {
        this(context, new JdbcInputFormat(config, tables));
    }

    JdbcSourceReader(Context context, JdbcInputFormat inputFormat) {
        this.context = context;
        this.inputFormat = inputFormat;
    }

    @Override
    public void open() throws Exception {
        inputFormat.openInputFormat();
    }

    @Override
    public void close() throws IOException {
        inputFormat.closeInputFormat();
    }

    @Override
    @SuppressWarnings("magicnumber")
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            CloseTableEvent closeTableEvent = pendingCloseTableEvents.poll();
            if (closeTableEvent != null) {
                output.collect(closeTableEvent);
                return;
            }
            JdbcSourceSplit split = splits.poll();
            if (null != split) {
                try {
                    inputFormat.open(split);
                    while (!inputFormat.reachedEnd()) {
                        SeaTunnelRow seaTunnelRow = inputFormat.nextRecord();
                        output.collect(seaTunnelRow);
                    }
                } finally {
                    inputFormat.close();
                    context.sendSourceEventToEnumerator(
                            new JdbcSplitFinishedEvent(split.getTablePath()));
                }
            } else if (noMoreSplit && splits.isEmpty() && pendingGlobalCloseTables.isEmpty()) {
                // signal to the source that we have reached the end of the data.
                log.info("Closed the bounded jdbc source");
                context.signalNoMoreElement();
            } else {
                Thread.sleep(1000L);
            }
        }
    }

    @Override
    public List<JdbcSourceSplit> snapshotState(long checkpointId) throws Exception {
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

    @Override
    public void addSplits(List<JdbcSourceSplit> splits) {
        for (JdbcSourceSplit split : splits) {
            if (split.isCloseTableMarker()) {
                restoreCloseTableState(split);
                continue;
            }
            pendingGlobalCloseTables.add(split.getTablePath());
            this.splits.add(split);
        }
    }

    @Override
    public void handleNoMoreSplits() {
        noMoreSplit = true;
    }

    @Override
    public void handleSourceEvent(SourceEvent sourceEvent) {
        if (sourceEvent instanceof JdbcTableFinishedEvent) {
            JdbcTableFinishedEvent tableFinishedEvent = (JdbcTableFinishedEvent) sourceEvent;
            pendingGlobalCloseTables.remove(tableFinishedEvent.getTablePath());
            pendingCloseTableEvents.add(
                    new CloseTableEvent(
                            tableFinishedEvent.getTablePath(),
                            context.getIndexOfSubtask(),
                            tableFinishedEvent.getExpectedCloseEventCount()));
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}

    private void restoreCloseTableState(JdbcSourceSplit split) {
        if (split.isPendingCloseTableEvent()) {
            pendingGlobalCloseTables.remove(split.getTablePath());
            pendingCloseTableEvents.add(
                    new CloseTableEvent(
                            split.getTablePath(),
                            context.getIndexOfSubtask(),
                            split.getExpectedCloseEventCount()));
            return;
        }
        pendingGlobalCloseTables.add(split.getTablePath());
    }
}
