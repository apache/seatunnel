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

package org.apache.seatunnel.connectors.seatunnel.opcua;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import java.io.Serializable;
import java.util.List;

/** The single endpoint has one coordinator-owned split until the reader takes it. */
final class OpcUaEnumerator implements SourceSplitEnumerator<OpcUaSplit, OpcUaEnumerator.State> {
    private final Context<OpcUaSplit> context;
    private final String key;
    private OpcUaSplit pending;
    private boolean started;

    OpcUaEnumerator(Context<OpcUaSplit> context, String key, State restored) {
        if (context.currentParallelism() != 1) {
            throw OpcUaSettings.invalid("source parallelism must be 1");
        }
        this.context = context;
        this.key = key;
        if (restored == null) {
            pending = new OpcUaSplit(key);
        } else {
            validate(restored.key);
            pending = restored.pending;
        }
    }

    @Override
    public void open() {}

    @Override
    public synchronized void run() {
        started = true;
        assign();
    }

    @Override
    public synchronized void close() {
        started = false;
    }

    @Override
    public synchronized void addSplitsBack(List<OpcUaSplit> splits, int subtask) {
        if (splits.size() > 1) {
            throw OpcUaSettings.invalid("checkpoint has more than one OPC UA split");
        }
        if (!splits.isEmpty()) {
            OpcUaSplit stored = splits.get(0);
            validate(stored.stateKey);
            pending = stored;
        }
        assign();
    }

    @Override
    public synchronized int currentUnassignedSplitSize() {
        return pending == null ? 0 : 1;
    }

    @Override
    public synchronized void handleSplitRequest(int subtask) {
        assign();
    }

    @Override
    public synchronized void registerReader(int subtask) {
        assign();
    }

    @Override
    public synchronized State snapshotState(long checkpointId) {
        return new State(key, pending);
    }

    private void assign() {
        if (started && pending != null && context.registeredReaders().contains(0)) {
            context.assignSplit(0, pending);
            pending = null;
            context.signalNoMoreSplits(0);
        }
    }

    private void validate(String other) {
        if (!key.equals(other)) {
            throw OpcUaSettings.invalid(
                    "checkpoint OPC UA endpoint, security or node settings do not match");
        }
    }

    static final class State implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String key;
        private final OpcUaSplit pending;

        private State(String key, OpcUaSplit pending) {
            this.key = key;
            this.pending = pending;
        }
    }
}
