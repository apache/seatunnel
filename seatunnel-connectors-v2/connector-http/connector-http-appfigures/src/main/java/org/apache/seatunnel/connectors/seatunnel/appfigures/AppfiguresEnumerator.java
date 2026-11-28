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

package org.apache.seatunnel.connectors.seatunnel.appfigures;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;

/**
 * Assigns the single split that owns ordered Appfigures pagination.
 *
 * <p>The default report date bounds are created once, and stay the same after restore.
 */
final class AppfiguresEnumerator
        implements SourceSplitEnumerator<AppfiguresSplit, AppfiguresEnumerator.State> {
    private final Context<AppfiguresSplit> context;
    private final String fingerprint;
    private AppfiguresSplit pending;
    private boolean started;

    AppfiguresEnumerator(
            Context<AppfiguresSplit> context, AppfiguresSettings settings, State restored) {
        if (context.currentParallelism() != 1) {
            throw AppfiguresSettings.invalid("parallelism", "requires one ordered source split");
        }
        this.context = context;
        fingerprint = settings.fingerprint;
        if (restored == null) {
            pending = new AppfiguresSplit(settings, LocalDate.now(ZoneOffset.UTC));
        } else {
            requireFingerprint(restored.fingerprint);
            pending = restored.pending;
        }
    }

    @Override
    public void open() {}

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
    public synchronized void addSplitsBack(List<AppfiguresSplit> splits, int subtaskId) {
        if (splits.isEmpty()) {
            return;
        }
        if (splits.size() != 1 || pending != null) {
            throw AppfiguresSettings.invalid("checkpoint", "received more than one source split");
        }
        requireFingerprint(splits.get(0).fingerprint);
        pending = splits.get(0);
        dispatch();
    }

    @Override
    public synchronized int currentUnassignedSplitSize() {
        return pending == null ? 0 : 1;
    }

    @Override
    public synchronized void handleSplitRequest(int subtaskId) {
        dispatch();
    }

    @Override
    public synchronized void registerReader(int subtaskId) {
        dispatch();
    }

    /**
     * Saves the split if it is still waiting for its worker.
     *
     * <p>The state retains the default reporting period selected at job initialization.
     */
    @Override
    public synchronized State snapshotState(long checkpointId) {
        return new State(fingerprint, pending);
    }

    private void dispatch() {
        if (started && pending != null && context.registeredReaders().contains(0)) {
            context.assignSplit(0, pending);
            pending = null;
            context.signalNoMoreSplits(0);
        }
    }

    private void requireFingerprint(String source) {
        if (!fingerprint.equals(source)) {
            throw AppfiguresSettings.invalid("checkpoint", "resource or read settings changed");
        }
    }

    /**
     * Coordinator checkpoint for the settings identity and any unassigned split.
     *
     * <p>The reader persists progress after the split has been assigned.
     */
    static final class State implements Serializable {
        private static final long serialVersionUID = 1L;
        final String fingerprint;
        final AppfiguresSplit pending;

        private State(String fingerprint, AppfiguresSplit pending) {
            this.fingerprint = fingerprint;
            this.pending = pending;
        }
    }
}
