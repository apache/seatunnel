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
package org.apache.seatunnel.connectors.seatunnel.beamer;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import java.io.Serializable;
import java.util.List;

/**
 * Assigns the one ordered NPS scan to its registered reader.
 *
 * <p>Before assignment only the enumerator owns the split. After assignment the reader checkpoints
 * its published progress, including the fully completed cursor.
 */
final class BeamerEnumerator
        implements SourceSplitEnumerator<BeamerSplit, BeamerEnumerator.Snapshot> {
    private final Context<BeamerSplit> context;
    private final String scope;
    // The worker owns all subsequent progress once this field has been offered.
    private BeamerSplit unassigned;
    private boolean running;

    BeamerEnumerator(Context<BeamerSplit> context, BeamerConfig config, Snapshot checkpoint) {
        this.context = context;
        this.scope = config.scope;
        if (context.currentParallelism() != 1) {
            throw BeamerConfig.invalid("parallelism", "NPS scans require parallelism 1");
        }
        if (checkpoint != null && !scope.equals(checkpoint.scope)) {
            throw BeamerConfig.invalid("checkpoint", "read scope changed");
        }
        unassigned = checkpoint == null ? new BeamerSplit(config) : checkpoint.unassigned;
    }

    @Override
    public void open() {}

    /**
     * Enables assignment once both the coordinator and reader are ready.
     *
     * <p>Either callback can come first.
     */
    @Override
    public synchronized void run() {
        running = true;
        offerSplit();
    }

    @Override
    public synchronized void close() {
        running = false;
    }

    @Override
    public synchronized void handleSplitRequest(int subtaskId) {
        offerSplit();
    }

    @Override
    public synchronized void registerReader(int subtaskId) {
        offerSplit();
    }

    @Override
    public synchronized int currentUnassignedSplitSize() {
        return unassigned == null ? 0 : 1;
    }

    @Override
    public synchronized void addSplitsBack(List<BeamerSplit> splits, int subtaskId) {
        if (splits.isEmpty()) {
            return;
        }
        if (unassigned != null || splits.size() != 1 || !scope.equals(splits.get(0).scope)) {
            throw BeamerConfig.invalid("checkpoint", "cannot return the NPS split");
        }
        unassigned = splits.get(0);
        offerSplit();
    }

    /**
     * Preserves the initial position if a checkpoint occurs before assignment.
     *
     * <p>An empty coordinator position after assignment keeps recovery with the reader's snapshot.
     */
    @Override
    public synchronized Snapshot snapshotState(long checkpointId) {
        return new Snapshot(scope, unassigned);
    }

    private void offerSplit() {
        if (running && unassigned != null && context.registeredReaders().contains(0)) {
            context.assignSplit(0, unassigned);
            unassigned = null;
            context.signalNoMoreSplits(0);
        }
    }

    /**
     * Checkpoint of source ownership before and after first assignment.
     *
     * <p>The scope guards against reconstructing a split for a different ordered NPS scan.
     */
    static final class Snapshot implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String scope;
        private final BeamerSplit unassigned;

        private Snapshot(String scope, BeamerSplit unassigned) {
            this.scope = scope;
            this.unassigned = unassigned;
        }
    }
}
