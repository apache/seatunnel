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

package org.apache.seatunnel.connectors.seatunnel.ashby;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import java.io.Serializable;
import java.util.List;

/**
 * Coordinates the single owner of an Ashby list and its continuation.
 *
 * <p>A split remains on the coordinator until its worker is registered. Both sides verify the
 * restored scope before delegating or reading.
 */
final class AshbyEnumerator implements SourceSplitEnumerator<AshbySplit, AshbyEnumerator.State> {
    private final Context<AshbySplit> context;
    private final String fingerprint;
    // The coordinator owns the split until assignment or return from a failed task.
    private AshbySplit pending;
    // Registration and run can arrive in either order. Only a running enumerator can dispatch.
    private boolean started;

    AshbyEnumerator(Context<AshbySplit> context, AshbySettings settings, State saved) {
        if (context.currentParallelism() != 1) {
            throw AshbySettings.invalid("parallelism", "requires one ordered source split");
        }
        this.context = context;
        fingerprint = settings.fingerprint;
        if (saved == null) {
            pending = new AshbySplit(settings);
        } else {
            sameScope(saved.fingerprint);
            pending = saved.pending;
        }
    }

    @Override
    public void open() {}

    /**
     * Allows assignment after the coordinator starts and its sole reader is registered.
     *
     * <p>This handles readers registering either before or after the enumerator starts.
     */
    @Override
    public synchronized void run() {
        started = true;
        dispatch();
    }

    @Override
    public synchronized void close() {
        started = false;
    }

    @Override
    public synchronized void addSplitsBack(List<AshbySplit> splits, int subtask) {
        if (splits.isEmpty()) {
            return;
        }
        if (splits.size() != 1 || pending != null) {
            throw AshbySettings.invalid("checkpoint", "more than one split was returned");
        }
        sameScope(splits.get(0).fingerprint);
        pending = splits.get(0);
        dispatch();
    }

    @Override
    public synchronized int currentUnassignedSplitSize() {
        return pending == null ? 0 : 1;
    }

    @Override
    public synchronized void handleSplitRequest(int subtask) {
        dispatch();
    }

    @Override
    public synchronized void registerReader(int subtask) {
        dispatch();
    }

    /**
     * Saves the scope identity and the split when it has yet to be assigned.
     *
     * <p>An assigned split's progress is owned by the reader's checkpoint.
     */
    @Override
    public synchronized State snapshotState(long checkpoint) {
        return new State(fingerprint, pending);
    }

    private void dispatch() {
        if (started && pending != null && context.registeredReaders().contains(0)) {
            context.assignSplit(0, pending);
            pending = null;
            context.signalNoMoreSplits(0);
        }
    }

    private void sameScope(String saved) {
        if (!fingerprint.equals(saved)) {
            throw AshbySettings.invalid("checkpoint", "resource or read settings changed");
        }
    }

    /**
     * Coordinator checkpoint for the immutable scope and pre-assignment state.
     *
     * <p>This state retains an initial split even if the job stops before any provider request.
     */
    static final class State implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String fingerprint;
        private final AshbySplit pending;

        private State(String fingerprint, AshbySplit pending) {
            this.fingerprint = fingerprint;
            this.pending = pending;
        }
    }
}
