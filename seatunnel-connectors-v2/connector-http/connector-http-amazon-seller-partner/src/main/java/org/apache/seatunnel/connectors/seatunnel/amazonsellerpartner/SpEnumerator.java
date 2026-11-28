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

package org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;

/**
 * Owns the frozen resource window until the engine has assigned its single reader.
 *
 * <p>Only the unassigned split lives in the coordinator checkpoint. An assigned reader checkpoints
 * its own position through the source reader API.
 */
final class SpEnumerator implements SourceSplitEnumerator<SpPosition, SpEnumerator.State> {
    private final Context<SpPosition> context;
    private final String fingerprint;
    private SpPosition pending;
    private boolean started;

    SpEnumerator(Context<SpPosition> context, SpSettings settings, State recovered) {
        if (context.currentParallelism() != 1) {
            throw SpSettings.invalid(
                    "parallelism", "the Selling Partner source uses one serial split");
        }
        this.context = context;
        fingerprint = settings.fingerprint;
        if (recovered == null) {
            pending = SpWindows.initial(settings, Instant.now());
        } else {
            requireSame(recovered.fingerprint);
            pending = recovered.pending;
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
    public synchronized void addSplitsBack(List<SpPosition> splits, int subtaskId) {
        if (splits.size() > 1 || (pending != null && !splits.isEmpty())) {
            throw SpSettings.invalid("checkpoint", "source received more than one split");
        }
        if (!splits.isEmpty()) {
            requireSame(splits.get(0).fingerprint);
            pending = splits.get(0);
            dispatch();
        }
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

    private void requireSame(String recovered) {
        if (!fingerprint.equals(recovered)) {
            throw SpSettings.invalid(
                    "checkpoint", "resource, credentials, marketplace or window options differ");
        }
    }

    /**
     * Persisted coordinator identity and any split that still needs assignment.
     *
     * <p>Restore preserves the original cutoff even if no provider call has been made.
     */
    static final class State implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String fingerprint;
        private final SpPosition pending;

        private State(String fingerprint, SpPosition pending) {
            this.fingerprint = fingerprint;
            this.pending = pending;
        }
    }
}
