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

package org.apache.seatunnel.connectors.seatunnel.sevenshifts;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import java.io.Serializable;
import java.util.List;

/**
 * Owns the single unassigned traversal and accepts reader-owned restored positions after startup.
 * Assignment is restricted to subtask zero and acknowledged before pending ownership is cleared.
 */
final class SevenShiftsEnumerator
        implements SourceSplitEnumerator<SevenShiftsSplit, SevenShiftsEnumerator.State> {
    private final Context<SevenShiftsSplit> context;
    private final String checkpointKey;
    private SevenShiftsSplit pending;
    private boolean running;

    SevenShiftsEnumerator(
            Context<SevenShiftsSplit> context, SevenShiftsConfig config, State state) {
        this.context = context;
        checkpointKey = config.checkpointKey();
        if (context.currentParallelism() != 1) {
            throw SevenShiftsConfig.invalid("source parallelism must be 1");
        }
        if (state == null) {
            pending = new SevenShiftsSplit(config);
        } else {
            validate(state.checkpointKey);
            pending = state.pending == null ? null : state.pending.copy();
        }
    }

    @Override
    public void open() {}

    @Override
    public synchronized void run() {
        running = true;
        assign();
    }

    @Override
    public synchronized void close() {
        running = false;
    }

    @Override
    public synchronized void addSplitsBack(List<SevenShiftsSplit> splits, int subtaskId) {
        if (splits.size() > 1) {
            throw SevenShiftsConfig.invalid("checkpoint returned more than one scan split");
        }
        if (!splits.isEmpty()) {
            SevenShiftsSplit split = splits.get(0);
            validate(split.checkpointKey);
            pending = split.copy();
        }
        assign();
    }

    @Override
    public synchronized int currentUnassignedSplitSize() {
        return pending == null ? 0 : 1;
    }

    @Override
    public synchronized void handleSplitRequest(int subtaskId) {
        assign();
    }

    @Override
    public synchronized void registerReader(int subtaskId) {
        assign();
    }

    /**
     * Saves only coordinator-owned work. The assigned reader independently snapshots its accepted
     * row prefix, so an empty pending position must not recreate a fresh traversal on restore.
     */
    @Override
    public synchronized State snapshotState(long checkpointId) {
        return new State(checkpointKey, pending == null ? null : pending.copy());
    }

    private void assign() {
        if (running && pending != null && context.registeredReaders().contains(0)) {
            context.assignSplit(0, pending);
            pending = null;
            context.signalNoMoreSplits(0);
        }
    }

    private void validate(String savedKey) {
        if (!checkpointKey.equals(savedKey)) {
            throw SevenShiftsConfig.invalid(
                    "resource, base_url, api_version, start_date and page_size must match the checkpoint");
        }
    }

    /**
     * Detached coordinator checkpoint containing an optional unassigned scan. The fingerprint
     * prevents a saved traversal from being assigned to a different export or state version.
     */
    static final class State implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String checkpointKey;
        private final SevenShiftsSplit pending;

        private State(String checkpointKey, SevenShiftsSplit pending) {
            this.checkpointKey = checkpointKey;
            this.pending = pending;
        }
    }
}
