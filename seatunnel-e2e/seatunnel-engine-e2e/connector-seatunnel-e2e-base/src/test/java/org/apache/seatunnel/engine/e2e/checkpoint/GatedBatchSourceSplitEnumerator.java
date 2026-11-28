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

package org.apache.seatunnel.engine.e2e.checkpoint;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import java.io.IOException;
import java.util.List;

/**
 * Enumerator of {@link GatedBatchSource}.
 *
 * <p>There is nothing to enumerate: the reader produces rows from {@link BatchSourceGate}, so every
 * registered reader is told immediately that no split will arrive. The reader keeps emitting until
 * the test releases the gate and only then signals that it has no more elements.
 */
public class GatedBatchSourceSplitEnumerator
        implements SourceSplitEnumerator<GatedBatchSourceSplit, GatedBatchSourceState> {

    private final Context<GatedBatchSourceSplit> context;

    public GatedBatchSourceSplitEnumerator(Context<GatedBatchSourceSplit> context) {
        this.context = context;
    }

    @Override
    public void open() {}

    @Override
    public void run() {}

    @Override
    public void close() throws IOException {}

    @Override
    public void addSplitsBack(List<GatedBatchSourceSplit> splits, int subtaskId) {
        // The source owns no splits, so a returning reader has nothing to give back.
    }

    @Override
    public int currentUnassignedSplitSize() {
        return 0;
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        context.signalNoMoreSplits(subtaskId);
    }

    @Override
    public void registerReader(int subtaskId) {
        context.signalNoMoreSplits(subtaskId);
    }

    @Override
    public GatedBatchSourceState snapshotState(long checkpointId) {
        return new GatedBatchSourceState();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}
}
