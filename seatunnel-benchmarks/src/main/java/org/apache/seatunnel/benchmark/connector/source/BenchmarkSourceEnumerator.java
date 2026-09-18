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

package org.apache.seatunnel.benchmark.connector.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Assigns one deterministic interleaved sequence split to every source subtask. */
public final class BenchmarkSourceEnumerator
        implements SourceSplitEnumerator<BenchmarkSourceSplit, BenchmarkSourceState> {

    private final Context<BenchmarkSourceSplit> context;
    private final long totalRows;
    private final long ratePerSecond;
    private final int payloadSize;
    private final int emitBatchSize;
    private final long startEpochMillis;
    private final Set<Integer> assignedSubtasks;
    private final Map<Integer, BenchmarkSourceSplit> returnedSplits = new HashMap<>();
    private boolean started;

    public BenchmarkSourceEnumerator(
            Context<BenchmarkSourceSplit> context,
            long totalRows,
            long ratePerSecond,
            int payloadSize,
            int emitBatchSize,
            long startEpochMillis,
            Set<Integer> assignedSubtasks) {
        this.context = context;
        this.totalRows = totalRows;
        this.ratePerSecond = ratePerSecond;
        this.payloadSize = payloadSize;
        this.emitBatchSize = emitBatchSize;
        this.startEpochMillis = startEpochMillis;
        this.assignedSubtasks = new HashSet<>(assignedSubtasks);
    }

    @Override
    public void open() {}

    @Override
    public synchronized void run() {
        started = true;
        for (int subtaskId : context.registeredReaders()) {
            assignSplit(subtaskId);
        }
    }

    @Override
    public void close() throws IOException {}

    @Override
    public synchronized void addSplitsBack(List<BenchmarkSourceSplit> splits, int subtaskId) {
        if (!splits.isEmpty()) {
            // Keep the reader-checkpointed nextSequence instead of recreating the split at zero.
            returnedSplits.put(subtaskId, splits.get(0));
            assignedSubtasks.remove(subtaskId);
        }
        if (started) {
            assignSplit(subtaskId);
        }
    }

    @Override
    public synchronized int currentUnassignedSplitSize() {
        return returnedSplits.size()
                + Math.max(0, context.currentParallelism() - assignedSubtasks.size());
    }

    @Override
    public synchronized void handleSplitRequest(int subtaskId) {
        if (started) {
            assignSplit(subtaskId);
        }
    }

    @Override
    public synchronized void registerReader(int subtaskId) {
        if (started) {
            assignSplit(subtaskId);
        }
    }

    @Override
    public synchronized BenchmarkSourceState snapshotState(long checkpointId) {
        return new BenchmarkSourceState(startEpochMillis, assignedSubtasks);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    private void assignSplit(int subtaskId) {
        BenchmarkSourceSplit returned = returnedSplits.remove(subtaskId);
        if (returned != null) {
            context.assignSplit(subtaskId, returned);
            assignedSubtasks.add(subtaskId);
        } else if (!assignedSubtasks.contains(subtaskId)) {
            int parallelism = context.currentParallelism();
            context.assignSplit(
                    subtaskId,
                    new BenchmarkSourceSplit(
                            subtaskId,
                            parallelism,
                            totalRows,
                            startEpochMillis,
                            ratePerSecond,
                            payloadSize,
                            emitBatchSize,
                            subtaskId));
            assignedSubtasks.add(subtaskId);
        }
        context.signalNoMoreSplits(subtaskId);
    }
}
