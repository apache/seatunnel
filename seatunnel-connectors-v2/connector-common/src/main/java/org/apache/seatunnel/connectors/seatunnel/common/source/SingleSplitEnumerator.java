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

package org.apache.seatunnel.connectors.seatunnel.common.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Assigns the one split of a source to the first registered reader.
 *
 * <p>{@link #run()}, {@link #addSplitsBack} and {@link #registerReader} are synchronized: on Zeta
 * they are called under different engine locks, and each of them checks whether the split is
 * assigned before it assigns it, so that check and the assignment must not interleave.
 */
public class SingleSplitEnumerator
        implements SourceSplitEnumerator<SingleSplit, SingleSplitEnumeratorState> {
    protected final SourceSplitEnumerator.Context<SingleSplit> context;
    protected SingleSplit pendingSplit;
    protected volatile boolean assigned = false;

    public SingleSplitEnumerator(SourceSplitEnumerator.Context<SingleSplit> context) {
        this.context = context;
    }

    @Override
    public void open() {
        // nothing
    }

    @Override
    public synchronized void run() throws Exception {
        if (assigned || pendingSplit != null) {
            return;
        }

        pendingSplit = new SingleSplit(null);
        assignSplit();
    }

    @Override
    public void close() throws IOException {
        // nothing
    }

    /**
     * Takes back the split of a failed reader, so that it is assigned again when a reader
     * registers. Flink passes only the splits assigned since the last completed checkpoint, which
     * is an empty list when nothing was assigned since then. Ignoring an empty list is safe because
     * the readers this enumerator serves always include their split in their checkpoint state, so a
     * reader restored from that checkpoint gets its split back from its own state.
     *
     * @param splits the splits to take back, possibly empty
     * @param subtaskId the subtask of the failed reader
     */
    @Override
    public synchronized void addSplitsBack(List<SingleSplit> splits, int subtaskId) {
        if (splits.isEmpty()) {
            return;
        }
        pendingSplit = splits.get(0);
        assigned = false;
        assignSplit();
    }

    protected synchronized void assignSplit() {
        if (assigned || pendingSplit == null) {
            return;
        }
        Set<Integer> readers = context.registeredReaders();
        if (!readers.isEmpty()) {
            context.assignSplit(readers.stream().findFirst().get(), pendingSplit);
            assigned = true;
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        return 0;
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        // nothing
    }

    @Override
    public synchronized void registerReader(int subtaskId) {
        assignSplit();
    }

    @Override
    public SingleSplitEnumeratorState snapshotState(long checkpointId) throws Exception {
        return new SingleSplitEnumeratorState();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // nothing
    }
}
