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

package org.apache.seatunnel.connectors.seatunnel.agilecrm;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import java.io.Serializable;
import java.util.List;

/** Assigns one complete resource traversal and retains unassigned work in coordinator state. */
final class AgileCrmEnumerator
        implements SourceSplitEnumerator<AgileCrmTraversal, AgileCrmEnumerator.State> {
    private final Context<AgileCrmTraversal> context;
    private final String stateKey;
    private AgileCrmTraversal waiting;
    private boolean active;

    AgileCrmEnumerator(
            Context<AgileCrmTraversal> context, AgileCrmSettings config, State restored) {
        if (context.currentParallelism() != 1) {
            throw AgileCrmSettings.invalid("source parallelism must be 1");
        }
        this.context = context;
        stateKey = config.stateKey();
        if (restored == null) {
            waiting = new AgileCrmTraversal(config);
        } else {
            verify(restored.stateKey);
            waiting = restored.waiting == null ? null : restored.waiting.snapshot();
        }
    }

    @Override
    public void open() {}

    @Override
    public synchronized void run() {
        active = true;
        handoff();
    }

    @Override
    public synchronized void close() {
        active = false;
    }

    @Override
    public synchronized void addSplitsBack(List<AgileCrmTraversal> restored, int subtask) {
        if (restored.size() > 1) {
            throw AgileCrmSettings.invalid("restored more than one scan split");
        }
        if (!restored.isEmpty()) {
            verify(restored.get(0).stateKey);
            waiting = restored.get(0).snapshot();
        }
        handoff();
    }

    @Override
    public synchronized int currentUnassignedSplitSize() {
        return waiting == null ? 0 : 1;
    }

    @Override
    public synchronized void handleSplitRequest(int subtask) {
        handoff();
    }

    @Override
    public synchronized void registerReader(int subtask) {
        handoff();
    }

    @Override
    public synchronized State snapshotState(long checkpointId) {
        return new State(stateKey, waiting == null ? null : waiting.snapshot());
    }

    private void handoff() {
        if (active && waiting != null && context.registeredReaders().contains(0)) {
            context.assignSplit(0, waiting);
            waiting = null;
            context.signalNoMoreSplits(0);
        }
    }

    private void verify(String restoredKey) {
        if (!stateKey.equals(restoredKey)) {
            throw AgileCrmSettings.invalid(
                    "checkpoint resource, origin, page size or event range does not match");
        }
    }

    static final class State implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String stateKey;
        private final AgileCrmTraversal waiting;

        private State(String stateKey, AgileCrmTraversal waiting) {
            this.stateKey = stateKey;
            this.waiting = waiting;
        }
    }
}
