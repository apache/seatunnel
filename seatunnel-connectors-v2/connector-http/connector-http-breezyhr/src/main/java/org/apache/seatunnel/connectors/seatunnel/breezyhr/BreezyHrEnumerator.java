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
package org.apache.seatunnel.connectors.seatunnel.breezyhr;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import java.io.Serializable;
import java.util.List;

/**
 * Assigns the sole Breezy HR scan to its single source reader.
 *
 * <p>After assignment, only the reader advances the pending resource plan.
 */
final class BreezyHrEnumerator
        implements SourceSplitEnumerator<BreezyHrSplit, BreezyHrEnumerator.Snapshot> {
    private final Context<BreezyHrSplit> context;
    private final String scope;
    // Kept until ownership is successfully transferred to the registered worker.
    private BreezyHrSplit pending;
    private boolean running;
    // Final assignment is independent of whether the assigned split was restored or new.
    private boolean finalAssignmentSent;

    BreezyHrEnumerator(Context<BreezyHrSplit> context, BreezyHrConfig config, Snapshot saved) {
        this.context = context;
        scope = config.scope;
        if (context.currentParallelism() != 1) {
            throw BreezyHrConfig.invalid("parallelism", "Breezy HR requires source parallelism 1");
        }
        if (saved != null && !scope.equals(saved.scope)) {
            throw BreezyHrConfig.invalid("checkpoint", "read scope changed");
        }
        pending = saved == null ? new BreezyHrSplit(config) : saved.pending;
    }

    @Override
    public void open() {}

    /**
     * Enables assignment after the coordinator is running.
     *
     * <p>The reader may register before or after this hook.
     */
    @Override
    public synchronized void run() {
        running = true;
        offer();
    }

    /**
     * Stops future split assignment in this coordinator.
     *
     * <p>Reader cancellation is handled separately by the source lifecycle.
     */
    @Override
    public synchronized void close() {
        running = false;
    }

    @Override
    public synchronized void handleSplitRequest(int subtaskId) {
        offer();
    }

    @Override
    public synchronized void registerReader(int subtaskId) {
        offer();
    }

    @Override
    public synchronized int currentUnassignedSplitSize() {
        return pending == null ? 0 : 1;
    }

    @Override
    public synchronized void addSplitsBack(List<BreezyHrSplit> splits, int subtaskId) {
        if (splits.isEmpty()) {
            return;
        }
        if (splits.size() != 1 || pending != null || !scope.equals(splits.get(0).scope)) {
            throw BreezyHrConfig.invalid("checkpoint", "could not return the Breezy HR split");
        }
        pending = splits.get(0);
        offer();
    }

    /**
     * Saves only pre-assignment coordinator state once the worker owns the split.
     *
     * <p>The ownership marker avoids reconstructing a scan after an assigned or completed job.
     */
    @Override
    public synchronized Snapshot snapshotState(long checkpointId) {
        return new Snapshot(scope, pending);
    }

    private void offer() {
        if (!running || !context.registeredReaders().contains(0)) {
            return;
        }
        if (pending != null) {
            context.assignSplit(0, pending);
            pending = null;
        }
        if (!finalAssignmentSent) {
            // A restored coordinator with no split has no new provider work to assign.
            context.signalNoMoreSplits(0);
            finalAssignmentSent = true;
        }
    }

    /**
     * Serialized coordinator state with the optional unassigned split.
     *
     * <p>The scope fingerprint guards against switching Breezy HR identity or resource selection on
     * restore.
     */
    static final class Snapshot implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String scope;
        private final BreezyHrSplit pending;

        private Snapshot(String scope, BreezyHrSplit pending) {
            this.scope = scope;
            this.pending = pending;
        }
    }
}
