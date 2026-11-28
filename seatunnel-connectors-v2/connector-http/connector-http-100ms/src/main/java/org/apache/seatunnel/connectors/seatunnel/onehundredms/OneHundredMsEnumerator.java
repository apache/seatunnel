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

package org.apache.seatunnel.connectors.seatunnel.onehundredms;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;

/**
 * Assigns a single bounded crawl. A restored reader owns its saved progress and can return it after
 * the coordinator has started.
 */
final class OneHundredMsEnumerator
        implements SourceSplitEnumerator<OneHundredMsProgress, OneHundredMsEnumerator.State> {
    private final Context<OneHundredMsProgress> context;
    private final String fingerprint;
    private OneHundredMsProgress pending;
    private boolean running;

    OneHundredMsEnumerator(
            Context<OneHundredMsProgress> context, OneHundredMsConfig config, State state) {
        if (context.currentParallelism() != 1) {
            throw OneHundredMsConfig.invalid("source parallelism must be 1");
        }
        this.context = context;
        fingerprint = config.planFingerprint;
        if (state == null) {
            pending = new OneHundredMsProgress(fingerprint, Instant.now());
        } else {
            verify(state.fingerprint);
            pending = state.pending;
        }
    }

    @Override
    public void open() {}

    /**
     * Assigns the pending crawl once reader zero is registered; restored readers retain their
     * split.
     */
    @Override
    public synchronized void run() {
        running = true;
        assign();
    }

    @Override
    public synchronized void close() {
        running = false;
    }

    /**
     * Reassigns the detached returned crawl, including its current page and accepted prefix. The
     * single reader continues that state instead of creating a fresh upper timestamp or cursor.
     */
    @Override
    public synchronized void addSplitsBack(List<OneHundredMsProgress> splits, int subtaskId) {
        if (splits.isEmpty()) {
            return;
        }
        if (splits.size() != 1 || subtaskId != 0) {
            throw OneHundredMsConfig.invalid("restore expects one crawl split");
        }
        OneHundredMsProgress received = splits.get(0);
        verify(received.fingerprint);
        pending = received;
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
     * Saves only a still-unassigned crawl; assigned progress is owned by the reader checkpoint. The
     * copy prevents later assignment or cursor changes from mutating a coordinator snapshot.
     */
    @Override
    public synchronized State snapshotState(long checkpointId) {
        return new State(fingerprint, pending == null ? null : new OneHundredMsProgress(pending));
    }

    private void assign() {
        if (running && pending != null && context.registeredReaders().contains(0)) {
            context.assignSplit(0, pending);
            pending = null;
            context.signalNoMoreSplits(0);
        }
    }

    private void verify(String saved) {
        if (!fingerprint.equals(saved)) {
            throw OneHundredMsConfig.invalid(
                    "endpoint, resources, page size or start_date changed during restore");
        }
    }

    /**
     * Coordinator state keeps the original scrape boundary until it reaches the reader. An assigned
     * crawl is excluded because reader snapshots then own its accepted-prefix progress.
     */
    static final class State implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String fingerprint;
        private final OneHundredMsProgress pending;

        State(String fingerprint, OneHundredMsProgress pending) {
            this.fingerprint = fingerprint;
            this.pending = pending;
        }
    }
}
