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

package org.apache.seatunnel.connectors.cdc.base.source.enumerator;

import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.PendingSplitsState;
import org.apache.seatunnel.connectors.cdc.base.source.event.CompletedSnapshotSplitsAckEvent;
import org.apache.seatunnel.connectors.cdc.base.source.event.CompletedSnapshotSplitsReportEvent;
import org.apache.seatunnel.connectors.cdc.base.source.event.SnapshotSplitWatermark;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Incremental source enumerator that enumerates receive the split request and assign the split to
 * source readers.
 */

public class IncrementalSourceEnumerator
        implements SourceSplitEnumerator<SourceSplitBase, PendingSplitsState> {
    private static final Logger LOG = LoggerFactory.getLogger(IncrementalSourceEnumerator.class);

    private final SourceSplitEnumerator.Context<SourceSplitBase> context;
    private final SplitAssigner splitAssigner;

    /**
     * using TreeSet to prefer assigning incremental split to task-0 for easier debug
     */
    private final TreeSet<Integer> readersAwaitingSplit;

    public IncrementalSourceEnumerator(
            SourceSplitEnumerator.Context<SourceSplitBase> context,
            SplitAssigner splitAssigner) {
        this.context = context;
        this.splitAssigner = splitAssigner;
        this.readersAwaitingSplit = new TreeSet<>();
    }

    @Override
    public void open() {
        splitAssigner.open();
    }

    @Override
    public void run() throws Exception {

    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        if (!context.registeredReaders().contains(subtaskId)) {
            // reader failed between sending the request and now. skip this request.
            return;
        }

        readersAwaitingSplit.add(subtaskId);
        assignSplits();
    }

    @Override
    public void addSplitsBack(List<SourceSplitBase> splits, int subtaskId) {
        LOG.debug("Incremental Source Enumerator adds splits back: {}", splits);
        splitAssigner.addSplits(splits);
    }

    @Override
    public int currentUnassignedSplitSize() {
        return 0;
    }

    @Override
    public void registerReader(int subtaskId) {
        // do nothing
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (sourceEvent instanceof CompletedSnapshotSplitsReportEvent) {
            LOG.info(
                    "The enumerator receives completed split watermarks(log offset) {} from subtask {}.",
                    sourceEvent,
                    subtaskId);
            CompletedSnapshotSplitsReportEvent reportEvent =
                    (CompletedSnapshotSplitsReportEvent) sourceEvent;
            List<SnapshotSplitWatermark> completedSplitWatermarks = reportEvent.getCompletedSnapshotSplitWatermarks();
            splitAssigner.onCompletedSplits(completedSplitWatermarks);

            // send acknowledge event
            CompletedSnapshotSplitsAckEvent ackEvent =
                    new CompletedSnapshotSplitsAckEvent(completedSplitWatermarks.stream()
                        .map(SnapshotSplitWatermark::getSplitId)
                        .collect(Collectors.toList()));
            context.sendEventToSourceReader(subtaskId, ackEvent);
        }
    }

    @Override
    public PendingSplitsState snapshotState(long checkpointId) {
        return splitAssigner.snapshotState(checkpointId);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        splitAssigner.notifyCheckpointComplete(checkpointId);
        // incremental split may be available after checkpoint complete
        assignSplits();
    }

    @Override
    public void close() {
        LOG.info("Closing enumerator...");
        splitAssigner.close();
    }

    // ------------------------------------------------------------------------------------------

    private void assignSplits() {
        final Iterator<Integer> awaitingReader = readersAwaitingSplit.iterator();

        while (awaitingReader.hasNext()) {
            int nextAwaiting = awaitingReader.next();
            // if the reader that requested another split has failed in the meantime, remove
            // it from the list of waiting readers
            if (!context.registeredReaders().contains(nextAwaiting)) {
                awaitingReader.remove();
                continue;
            }

            Optional<SourceSplitBase> split = splitAssigner.getNext();
            if (split.isPresent()) {
                final SourceSplitBase sourceSplit = split.get();
                context.assignSplit(nextAwaiting, sourceSplit);
                awaitingReader.remove();
                LOG.info("Assign split {} to subtask {}", sourceSplit, nextAwaiting);
            } else {
                // there is no available splits by now, skip assigning
                break;
            }
        }
    }
}
