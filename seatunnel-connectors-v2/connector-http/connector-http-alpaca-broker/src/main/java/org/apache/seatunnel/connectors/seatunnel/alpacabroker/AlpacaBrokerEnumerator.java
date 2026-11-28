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

package org.apache.seatunnel.connectors.seatunnel.alpacabroker;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import java.io.Serializable;
import java.util.List;

/**
 * Assigns one serial cursor per table and preserves pre-assignment and returned splits.
 *
 * <p>A single in-flight page avoids concurrent changes to a provider pagination boundary.
 */
final class AlpacaBrokerEnumerator
        implements SourceSplitEnumerator<AlpacaBrokerPosition, AlpacaBrokerEnumerator.State> {
    private final Context<AlpacaBrokerPosition> context;
    private final String stateKey;
    private AlpacaBrokerPosition unassigned;
    private boolean running;

    AlpacaBrokerEnumerator(
            Context<AlpacaBrokerPosition> context, AlpacaBrokerSettings settings, State restored) {
        if (context.currentParallelism() != 1) {
            throw AlpacaBrokerSettings.invalid(
                    "parallelism", "each AlpacaBroker source requires 1");
        }
        this.context = context;
        stateKey = settings.stateKey;
        if (restored == null) {
            unassigned = AlpacaBrokerPosition.initial(settings);
        } else {
            verifyKey(restored.stateKey);
            unassigned = restored.unassigned;
        }
    }

    /**
     * The coordinator has no network resources. Assignment waits for run and registration.
     *
     * <p>A reader creates its own HTTP client after the split is available.
     */
    @Override
    public void open() {}

    /**
     * Assign the initial or restored split once the engine has opened its readers.
     *
     * <p>The engine may register readers before starting this enumerator.
     */
    @Override
    public synchronized void run() {
        running = true;
        assignWhenPossible();
    }

    /**
     * Prevent late assignment after this coordinator closes.
     *
     * <p>An unassigned split remains in state until the engine takes ownership of it.
     */
    @Override
    public synchronized void close() {
        running = false;
    }

    /**
     * Accept engine recovery of a reader-owned split, including a terminal position.
     *
     * <p>A returned split preserves the saved cursor rather than building a new scan window.
     */
    @Override
    public synchronized void addSplitsBack(List<AlpacaBrokerPosition> splits, int subtaskId) {
        if (splits.size() > 1 || (unassigned != null && !splits.isEmpty())) {
            throw AlpacaBrokerSettings.invalid("checkpoint", "received more than one split");
        }
        if (!splits.isEmpty()) {
            verifyKey(splits.get(0).stateKey);
            unassigned = splits.get(0);
            assignWhenPossible();
        }
    }

    @Override
    public synchronized int currentUnassignedSplitSize() {
        return unassigned == null ? 0 : 1;
    }

    @Override
    public synchronized void handleSplitRequest(int subtaskId) {
        assignWhenPossible();
    }

    @Override
    public synchronized void registerReader(int subtaskId) {
        assignWhenPossible();
    }

    /**
     * A null pending split means the reader already owns its checkpoint state.
     *
     * <p>The coordinator stores a full position only while it is responsible for assignment.
     */
    @Override
    public synchronized State snapshotState(long checkpointId) {
        return new State(stateKey, unassigned);
    }

    private void assignWhenPossible() {
        if (running && unassigned != null && context.registeredReaders().contains(0)) {
            context.assignSplit(0, unassigned);
            unassigned = null;
            context.signalNoMoreSplits(0);
        }
    }

    private void verifyKey(String other) {
        if (!stateKey.equals(other)) {
            throw AlpacaBrokerSettings.invalid(
                    "checkpoint", "resource, endpoint or query settings differ");
        }
    }

    /**
     * Serializable coordinator snapshot carrying any split not yet owned by a reader.
     *
     * <p>The settings identity guards restore even when the reader holds the position.
     */
    static final class State implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String stateKey;
        private final AlpacaBrokerPosition unassigned;

        private State(String stateKey, AlpacaBrokerPosition unassigned) {
            this.stateKey = stateKey;
            this.unassigned = unassigned;
        }
    }
}
