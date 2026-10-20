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

package org.apache.seatunnel.e2e.source.checkpointable;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class CheckpointableSequenceSplitEnumerator
        implements SourceSplitEnumerator<CheckpointableSequenceSplit, CheckpointableSequenceState> {

    private final Context<CheckpointableSequenceSplit> context;
    private final Deque<CheckpointableSequenceSplit> pendingSplits;
    private final Set<Integer> registeredReaders = ConcurrentHashMap.newKeySet();
    private volatile boolean awaitingRestoredSplits;

    public CheckpointableSequenceSplitEnumerator(
            Context<CheckpointableSequenceSplit> context,
            List<CheckpointableSequenceSplit> pendingSplits,
            boolean awaitingRestoredSplits) {
        this.context = context;
        this.pendingSplits = new ArrayDeque<>(pendingSplits);
        this.awaitingRestoredSplits = awaitingRestoredSplits;
    }

    @Override
    public void open() {}

    @Override
    public void run() {}

    @Override
    public void close() throws IOException {}

    @Override
    public void addSplitsBack(List<CheckpointableSequenceSplit> splits, int subtaskId) {
        awaitingRestoredSplits = false;
        for (CheckpointableSequenceSplit split : splits) {
            pendingSplits.addFirst(split);
        }
        assignPendingSplits(subtaskId);
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplits.size();
    }

    @Override
    public void registerReader(int subtaskId) {
        registeredReaders.add(subtaskId);
        assignPendingSplits(subtaskId);
    }

    @Override
    public CheckpointableSequenceState snapshotState(long checkpointId) {
        return new CheckpointableSequenceState(new ArrayList<>(pendingSplits));
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    @Override
    public void handleSplitRequest(int subtaskId) {
        assignPendingSplits(subtaskId);
    }

    private void assignPendingSplits(int subtaskId) {
        if (!registeredReaders.contains(subtaskId)) {
            return;
        }
        while (!pendingSplits.isEmpty()) {
            context.assignSplit(subtaskId, pendingSplits.pollFirst());
        }
        if (!awaitingRestoredSplits) {
            context.signalNoMoreSplits(subtaskId);
        }
    }
}
