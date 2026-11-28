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
package org.apache.seatunnel.connectors.seatunnel.babelforce;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import java.io.Serializable;
import java.util.List;

/**
 * Coordinates the sole ordered simple report split.
 *
 * <p>The coordinator freezes the end of the report window before assigning it to the reader.
 */
final class BabelforceEnumerator
        implements SourceSplitEnumerator<BabelforceCheckpoint, BabelforceEnumerator.State> {
    private final Context<BabelforceCheckpoint> context;
    private final String fingerprint;
    // The position transfers to the reader when the single subtask registers.
    private BabelforceCheckpoint outstanding;
    private boolean running;

    BabelforceEnumerator(
            Context<BabelforceCheckpoint> context, BabelforceOptions options, State state) {
        this.context = context;
        fingerprint = options.fingerprint;
        if (context.currentParallelism() != 1) {
            throw BabelforceOptions.invalid("parallelism", "requires one ordered calls split");
        }
        if (state != null && !fingerprint.equals(state.fingerprint)) {
            throw BabelforceOptions.invalid("checkpoint", "read scope changed");
        }
        outstanding = state == null ? new BabelforceCheckpoint(options) : state.outstanding;
    }

    @Override
    public void open() {}

    /**
     * Allows dispatch when the coordinator and downstream worker are both ready.
     *
     * <p>Source registration can precede this call on recovery.
     */
    @Override
    public synchronized void run() {
        running = true;
        dispatch();
    }

    @Override
    public synchronized void close() {
        running = false;
    }

    @Override
    public synchronized int currentUnassignedSplitSize() {
        return outstanding == null ? 0 : 1;
    }

    @Override
    public synchronized void handleSplitRequest(int subtaskId) {
        dispatch();
    }

    @Override
    public synchronized void registerReader(int subtaskId) {
        dispatch();
    }

    @Override
    public synchronized void addSplitsBack(List<BabelforceCheckpoint> splits, int subtaskId) {
        if (splits.isEmpty()) {
            return;
        }
        if (outstanding != null
                || splits.size() != 1
                || !fingerprint.equals(splits.get(0).fingerprint)) {
            throw BabelforceOptions.invalid("checkpoint", "unexpected calls split");
        }
        outstanding = splits.get(0);
        dispatch();
    }

    /**
     * Stores the pending scan if the coordinator has not sent it yet.
     *
     * <p>The assigned worker owns every page position thereafter.
     */
    @Override
    public synchronized State snapshotState(long checkpointId) {
        return new State(fingerprint, outstanding);
    }

    private void dispatch() {
        if (running && outstanding != null && context.registeredReaders().contains(0)) {
            context.assignSplit(0, outstanding);
            outstanding = null;
            context.signalNoMoreSplits(0);
        }
    }

    /**
     * Enumerator state preserving the initial window and scope before assignment.
     *
     * <p>The fingerprint remains present even if only the worker owns the actual split.
     */
    static final class State implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String fingerprint;
        private final BabelforceCheckpoint outstanding;

        private State(String fingerprint, BabelforceCheckpoint outstanding) {
            this.fingerprint = fingerprint;
            this.outstanding = outstanding;
        }
    }
}
