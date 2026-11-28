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

package org.apache.seatunnel.connectors.seatunnel.confluence;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import java.io.Serializable;
import java.util.List;

/**
 * Assigns one independent pagination sequence to the sole reader.
 *
 * @see ConfluenceSplit
 */
final class ConfluenceEnumerator
        implements SourceSplitEnumerator<ConfluenceSplit, ConfluenceEnumerator.State> {
    private final Context<ConfluenceSplit> context;
    private final ConfluenceSplit initial;
    // A restored null pending value waits for the reader checkpoint returned by addSplitsBack.
    // All mutable assignment state is guarded by the enumeration context monitor.
    private ConfluenceSplit pending;
    private boolean running;
    private boolean assigned;

    ConfluenceEnumerator(
            Context<ConfluenceSplit> context, ConfluenceSplit initial, State restored) {
        if (context.currentParallelism() != 1) {
            throw ConfluenceConfig.invalid("source parallelism must be 1");
        }
        this.context = context;
        this.initial = initial;
        this.pending = restored == null ? initial : restored.pending;
    }

    @Override
    public void open() {}

    @Override
    public void run() {
        synchronized (context) {
            running = true;
            assign();
        }
    }

    @Override
    public void close() {}

    /**
     * Restores reader-owned progress. An explicit empty restore reissues the initial split only
     * when the enumerator has neither pending nor assigned work.
     */
    @Override
    public void addSplitsBack(List<ConfluenceSplit> splits, int subtaskId) {
        synchronized (context) {
            if (splits.isEmpty()) {
                if (!assigned && pending == null) {
                    pending = initial;
                    assign();
                }
                return;
            }
            if (splits.size() != 1 || subtaskId != 0) {
                throw ConfluenceConfig.invalid("expected one returned pagination split");
            }
            pending = splits.get(0);
            assigned = false;
            assign();
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        synchronized (context) {
            return pending == null ? 0 : 1;
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        synchronized (context) {
            assign();
        }
    }

    @Override
    public void registerReader(int subtaskId) {
        handleSplitRequest(subtaskId);
    }

    /*
     * Saves only unassigned work; assigned progress is captured by the reader checkpoint.
     */
    @Override
    public State snapshotState(long checkpointId) {
        synchronized (context) {
            return new State(pending);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    /*
     * Transfers pending work once the sole reader is registered and enumeration is running.
     */
    private void assign() {
        if (!running || assigned || pending == null || !context.registeredReaders().contains(0)) {
            return;
        }
        ConfluenceSplit split = pending;
        context.assignSplit(0, split);
        assigned = true;
        pending = null;
    }

    /**
     * Keeps unassigned work across restore without duplicating the reader-owned committed position.
     */
    static final class State implements Serializable {
        private static final long serialVersionUID = 1L;
        private final ConfluenceSplit pending;

        private State(ConfluenceSplit pending) {
            this.pending = pending;
        }
    }
}
