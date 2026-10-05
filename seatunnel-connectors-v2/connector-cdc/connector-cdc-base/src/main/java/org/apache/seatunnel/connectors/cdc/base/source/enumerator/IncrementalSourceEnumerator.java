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
import org.apache.seatunnel.connectors.cdc.base.source.event.CompletedSnapshotPhaseEvent;
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
 *
 * <p>This enumerator tolerates out-of-order or late {@link #addSplitsBack(List, int)} calls, e.g.
 * checkpoint-restored splits returned after a reader already requested a split: returned splits are
 * always queued in the {@link SplitAssigner}, and they are dispatched to waiting readers either on
 * the first {@link #run()} assignment pass (when the enumerator was not running yet) or on the next
 * assignment cycle triggered by {@link #addSplitsBack(List, int)} (when the enumerator is already
 * running).
 */
public class IncrementalSourceEnumerator
        implements SourceSplitEnumerator<SourceSplitBase, PendingSplitsState> {
    private static final Logger LOG = LoggerFactory.getLogger(IncrementalSourceEnumerator.class);

    private final SourceSplitEnumerator.Context<SourceSplitBase> context;
    private final SplitAssigner splitAssigner;

    /** using TreeSet to prefer assigning incremental split to task-0 for easier debug */
    private final TreeSet<Integer> readersAwaitingSplit;

    private volatile boolean running;

    public IncrementalSourceEnumerator(
            SourceSplitEnumerator.Context<SourceSplitBase> context, SplitAssigner splitAssigner) {
        this.context = context;
        this.splitAssigner = splitAssigner;
        this.readersAwaitingSplit = new TreeSet<>();
        this.running = false;
    }

    @Override
    public void open() {
        splitAssigner.open();
    }

    @Override
    public synchronized void run() throws Exception {
        this.running = true;
        // Dispatch the splits that addSplitsBack() returned before run() executed: the engine
        // hands the checkpoint-restored splits of a restarting reader to the enumerator and only
        // invokes run() once every reader has registered, so in a restart the restored splits are
        // normally queued in the assigner while running is still false and addSplitsBack() skipped
        // its assignment pass. They are not lost: any split request that arrived in the meantime
        // is already queued in readersAwaitingSplit by handleSplitRequest(), so this first
        // assignment pass hands out the returned splits to those waiting readers.
        assignSplits();
    }

    @Override
    public synchronized void handleSplitRequest(int subtaskId) {
        if (!context.registeredReaders().contains(subtaskId)) {
            // reader failed between sending the request and now. skip this request.
            return;
        }

        readersAwaitingSplit.add(subtaskId);
        if (running) {
            assignSplits();
        }
    }

    @Override
    public synchronized void addSplitsBack(List<SourceSplitBase> splits, int subtaskId) {
        LOG.debug("Incremental Source Enumerator adds splits back: {}", splits);
        splitAssigner.addSplits(splits);
        // running == false: the engine delivers the checkpoint-restored splits of a restarting
        // reader before it invokes run() (run() waits for every reader to register), so the
        // returned splits are still queued in the assigner and get dispatched by run()'s first
        // assignment pass - skipping the assignment here does not lose them.
        // running == true: a restored split may arrive after the reader already sent its split
        // request and is parked in readersAwaitingSplit; re-run the assignment loop so the
        // waiting reader receives the restored split immediately instead of stalling until
        // another trigger.
        if (running) {
            assignSplits();
        }
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
            LOG.debug(
                    "The enumerator receives completed split watermarks(log offset) {} from subtask {}.",
                    sourceEvent,
                    subtaskId);
            CompletedSnapshotSplitsReportEvent reportEvent =
                    (CompletedSnapshotSplitsReportEvent) sourceEvent;
            List<SnapshotSplitWatermark> completedSplitWatermarks =
                    reportEvent.getCompletedSnapshotSplitWatermarks();
            synchronized (context) {
                splitAssigner.onCompletedSplits(completedSplitWatermarks);
            }

            // send acknowledge event
            CompletedSnapshotSplitsAckEvent ackEvent =
                    new CompletedSnapshotSplitsAckEvent(
                            completedSplitWatermarks.stream()
                                    .map(SnapshotSplitWatermark::getSplitId)
                                    .collect(Collectors.toList()));
            context.sendEventToSourceReader(subtaskId, ackEvent);
        } else if (sourceEvent instanceof CompletedSnapshotPhaseEvent) {
            LOG.debug(
                    "The enumerator receives completed snapshot phase event {} from subtask {}.",
                    sourceEvent,
                    subtaskId);
            CompletedSnapshotPhaseEvent event = (CompletedSnapshotPhaseEvent) sourceEvent;
            if (splitAssigner instanceof HybridSplitAssigner) {
                ((HybridSplitAssigner) splitAssigner).completedSnapshotPhase(event.getTableIds());
                LOG.info(
                        "Clean the SnapshotSplitAssigner#assignedSplits/splitCompletedOffsets to empty.");
            }
        }
    }

    @Override
    public PendingSplitsState snapshotState(long checkpointId) {
        return splitAssigner.snapshotState(checkpointId);
    }

    @Override
    public synchronized void notifyCheckpointComplete(long checkpointId) {
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

            Optional<SourceSplitBase> split;
            synchronized (context) {
                split = splitAssigner.getNext();
            }
            if (split.isPresent()) {
                final SourceSplitBase sourceSplit = split.get();
                context.assignSplit(nextAwaiting, sourceSplit);
                awaitingReader.remove();
                LOG.debug("Assign split {} to subtask {}", sourceSplit, nextAwaiting);
            } else {
                if (splitAssigner.waitingForCompletedSplits()) {
                    // there is no available splits by now, skip assigning
                    break;
                } else {
                    LOG.info(
                            "No more splits available, signal no more splits to subtask {}",
                            nextAwaiting);
                    context.signalNoMoreSplits(nextAwaiting);
                    awaitingReader.remove();
                }
            }
        }
    }
}
