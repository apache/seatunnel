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

package org.apache.seatunnel.connectors.seatunnel.acuityscheduling;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import java.io.Serializable;
import java.util.List;

/** Assigns one bounded crawl and accepts the reader's exact split on recovery. */
final class AcuityEnumerator
        implements SourceSplitEnumerator<AcuityProgress, AcuityEnumerator.State> {
    private final Context<AcuityProgress> context;
    private final String fingerprint;
    private AcuityProgress pending;
    private boolean running;

    AcuityEnumerator(Context<AcuityProgress> context, AcuityConfig config, State restored) {
        this.context = context;
        fingerprint = config.fingerprint;
        if (context.currentParallelism() != 1) {
            throw AcuityConfig.invalid("source parallelism must be 1");
        }
        if (restored == null) {
            pending = new AcuityProgress(fingerprint, config.start, config.frozenEnd());
        } else {
            verify(restored.fingerprint);
            pending = restored.pending;
        }
    }

    @Override
    public void open() {}

    @Override
    public synchronized void run() {
        running = true;
        assign();
    }

    @Override
    public synchronized void close() {
        running = false;
    }

    @Override
    public synchronized void addSplitsBack(List<AcuityProgress> splits, int worker) {
        if (splits.isEmpty()) {
            return;
        }
        if (splits.size() != 1 || worker != 0) {
            throw AcuityConfig.invalid("expected a single restored crawl split");
        }
        AcuityProgress restored = splits.get(0);
        verify(restored.fingerprint);
        pending = restored;
        assign();
    }

    @Override
    public synchronized int currentUnassignedSplitSize() {
        return pending == null ? 0 : 1;
    }

    @Override
    public synchronized void handleSplitRequest(int worker) {
        assign();
    }

    @Override
    public synchronized void registerReader(int worker) {
        assign();
    }

    /**
     * A checkpoint taken before assignment preserves the original date bounds in the coordinator.
     */
    @Override
    public synchronized State snapshotState(long checkpointId) {
        return new State(fingerprint, pending == null ? null : new AcuityProgress(pending));
    }

    private void assign() {
        if (running && pending != null && context.registeredReaders().contains(0)) {
            context.assignSplit(0, pending);
            pending = null;
            context.signalNoMoreSplits(0);
        }
    }

    private void verify(String restoredFingerprint) {
        if (!fingerprint.equals(restoredFingerprint)) {
            throw AcuityConfig.invalid(
                    "account, endpoint, resources or date query options changed during restore");
        }
    }

    /**
     * The coordinator retains an unassigned split, while assigned progress lives in reader state.
     */
    static final class State implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String fingerprint;
        private final AcuityProgress pending;

        State(String fingerprint, AcuityProgress pending) {
            this.fingerprint = fingerprint;
            this.pending = pending;
        }
    }
}
