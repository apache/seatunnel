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
package org.apache.seatunnel.connectors.seatunnel.awinadvertiser;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import java.io.Serializable;
import java.util.List;

/**
 * Assigns one bounded Awin report plan to a single reader.
 *
 * <p>The coordinator owns the initial time cutoff until the registered worker receives its split.
 */
final class AwinEnumerator implements SourceSplitEnumerator<AwinCursor, AwinEnumerator.State> {
    private final Context<AwinCursor> context;
    private final String scope;
    // The reader owns progression immediately after the split leaves this slot.
    private AwinCursor waiting;
    private boolean running;

    AwinEnumerator(Context<AwinCursor> context, AwinSettings settings, State restored) {
        this.context = context;
        scope = settings.scope;
        if (context.currentParallelism() != 1) {
            throw AwinSettings.invalid("parallelism", "requires one ordered report scan");
        }
        if (restored != null && !scope.equals(restored.scope)) {
            throw AwinSettings.invalid("checkpoint", "read scope changed");
        }
        waiting = restored == null ? new AwinCursor(settings) : restored.waiting;
    }

    @Override
    public void open() {}

    /**
     * Enables the coordinator to hand off a split after its worker registers.
     *
     * <p>Registration can happen either before or after this lifecycle callback.
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
        return waiting == null ? 0 : 1;
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
    public synchronized void addSplitsBack(List<AwinCursor> splits, int subtaskId) {
        if (splits.isEmpty()) {
            return;
        }
        if (waiting != null || splits.size() != 1 || !scope.equals(splits.get(0).scope)) {
            throw AwinSettings.invalid(
                    "checkpoint", "unexpected split returned to the coordinator");
        }
        waiting = splits.get(0);
        dispatch();
    }

    /**
     * Retains the cutoff and starting report position if assignment has not yet happened.
     *
     * <p>After dispatch the worker's checkpoint alone records the active scan.
     */
    @Override
    public synchronized State snapshotState(long checkpointId) {
        return new State(scope, waiting);
    }

    private void dispatch() {
        if (running && waiting != null && context.registeredReaders().contains(0)) {
            context.assignSplit(0, waiting);
            waiting = null;
            context.signalNoMoreSplits(0);
        }
    }

    /**
     * Coordinator checkpoint covering the period before first assignment.
     *
     * <p>The scope also protects restored jobs where the worker had already received the split.
     */
    static final class State implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String scope;
        private final AwinCursor waiting;

        private State(String scope, AwinCursor waiting) {
            this.scope = scope;
            this.waiting = waiting;
        }
    }
}
