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

package org.apache.seatunnel.connectors.seatunnel.adjust;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import java.io.Serializable;
import java.util.List;

/** Owns the anchored calendar window until its sole split is assigned. */
final class AdjustEnumerator implements SourceSplitEnumerator<AdjustScan, AdjustEnumerator.State> {
    private final Context<AdjustScan> context;
    private final String signature;
    private AdjustScan pending;
    private boolean running;

    AdjustEnumerator(Context<AdjustScan> context, AdjustReportPlan plan, State restored) {
        if (context.currentParallelism() != 1) {
            throw AdjustReportPlan.invalid("source parallelism must be 1");
        }
        this.context = context;
        signature = plan.signature();
        if (restored == null) {
            pending = new AdjustScan(signature, plan.start, plan.windowEnd());
        } else {
            check(restored.signature);
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
    public synchronized void addSplitsBack(List<AdjustScan> splits, int subtask) {
        if (splits.size() > 1) {
            throw AdjustReportPlan.invalid("checkpoint contains more than one scan");
        }
        if (!splits.isEmpty()) {
            AdjustScan restored = splits.get(0);
            check(restored.signature);
            pending = restored;
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
        return new State(signature, pending);
    }

    private void assign() {
        if (running && pending != null && context.registeredReaders().contains(0)) {
            context.assignSplit(0, pending);
            pending = null;
            context.signalNoMoreSplits(0);
        }
    }

    private void check(String saved) {
        if (!signature.equals(saved)) {
            throw AdjustReportPlan.invalid("checkpoint report parameters do not match this job");
        }
    }

    static final class State implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String signature;
        private final AdjustScan pending;

        private State(String signature, AdjustScan pending) {
            this.signature = signature;
            this.pending = pending;
        }
    }
}
