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

package org.apache.seatunnel.connectors.seatunnel.aha;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import java.io.Serializable;
import java.util.List;

/** Transfers one complete crawl, including a split returned after coordinator startup. */
final class AhaEnumerator implements SourceSplitEnumerator<AhaProgress, AhaEnumerator.State> {
    private final Context<AhaProgress> context;
    private final String fingerprint;
    private AhaProgress pending;
    private boolean started;

    AhaEnumerator(Context<AhaProgress> context, AhaConfig config, State saved) {
        if (context.currentParallelism() != 1) {
            throw AhaConfig.invalid("source parallelism must be 1");
        }
        this.context = context;
        this.fingerprint = config.fingerprint;
        if (saved == null) {
            pending = new AhaProgress(config.fingerprint);
        } else {
            checkFingerprint(saved.fingerprint);
            pending = saved.pending;
        }
    }

    @Override
    public void open() {}

    @Override
    public synchronized void run() {
        started = true;
        assignIfReady();
    }

    @Override
    public synchronized void close() {
        started = false;
    }

    @Override
    public synchronized void addSplitsBack(List<AhaProgress> returned, int reader) {
        if (returned.isEmpty()) {
            return;
        }
        if (returned.size() != 1 || reader != 0) {
            throw AhaConfig.invalid("expected one returned crawl split");
        }
        AhaProgress split = returned.get(0);
        checkFingerprint(split.fingerprint);
        pending = split;
        assignIfReady();
    }

    @Override
    public synchronized int currentUnassignedSplitSize() {
        return pending == null ? 0 : 1;
    }

    @Override
    public synchronized void handleSplitRequest(int reader) {
        assignIfReady();
    }

    @Override
    public synchronized void registerReader(int reader) {
        assignIfReady();
    }

    /** Persists initial ownership even if no reader has received the split yet. */
    @Override
    public synchronized State snapshotState(long checkpointId) {
        return new State(fingerprint, pending == null ? null : new AhaProgress(pending));
    }

    private void assignIfReady() {
        if (started && pending != null && context.registeredReaders().contains(0)) {
            context.assignSplit(0, pending);
            pending = null;
            context.signalNoMoreSplits(0);
        }
    }

    private void checkFingerprint(String saved) {
        if (!fingerprint.equals(saved)) {
            throw AhaConfig.invalid(
                    "incompatible checkpoint parameters: tenant, resource order or page_size changed");
        }
    }

    /** Stores either unassigned progress or the marker that the reader owns its checkpoint. */
    static final class State implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String fingerprint;
        private final AhaProgress pending;

        State(String fingerprint, AhaProgress pending) {
            this.fingerprint = fingerprint;
            this.pending = pending;
        }
    }
}
