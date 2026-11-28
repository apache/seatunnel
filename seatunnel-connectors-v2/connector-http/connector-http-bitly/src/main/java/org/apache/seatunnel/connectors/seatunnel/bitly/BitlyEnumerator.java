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
package org.apache.seatunnel.connectors.seatunnel.bitly;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import java.io.Serializable;
import java.util.List;

/**
 * Assigns the sole Bitly scan to its single source reader.
 *
 * <p>After assignment, only the reader advances the pending resource plan.
 */
final class BitlyEnumerator implements SourceSplitEnumerator<BitlySplit, BitlyEnumerator.Snapshot> {
    private final Context<BitlySplit> context;
    private final String scope;
    // Kept until ownership is successfully transferred to the registered worker.
    private BitlySplit pending;
    private boolean running;

    BitlyEnumerator(Context<BitlySplit> context, BitlyConfig config, Snapshot saved) {
        this.context = context;
        scope = config.scope;
        if (context.currentParallelism() != 1) {
            throw BitlyConfig.invalid("parallelism", "Bitly requires source parallelism 1");
        }
        if (saved != null && !scope.equals(saved.scope)) {
            throw BitlyConfig.invalid("checkpoint", "read scope changed");
        }
        pending = saved == null ? new BitlySplit(config) : saved.pending;
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
    public synchronized void addSplitsBack(List<BitlySplit> splits, int subtaskId) {
        if (splits.isEmpty()) {
            return;
        }
        if (splits.size() != 1 || pending != null || !scope.equals(splits.get(0).scope)) {
            throw BitlyConfig.invalid("checkpoint", "could not return the Bitly split");
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
        if (running && pending != null && context.registeredReaders().contains(0)) {
            context.assignSplit(0, pending);
            pending = null;
            context.signalNoMoreSplits(0);
        }
    }

    /**
     * Serialized coordinator state with the optional unassigned split.
     *
     * <p>The scope fingerprint guards against switching tenant or resource selection on restore.
     */
    static final class Snapshot implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String scope;
        private final BitlySplit pending;

        private Snapshot(String scope, BitlySplit pending) {
            this.scope = scope;
            this.pending = pending;
        }
    }
}
