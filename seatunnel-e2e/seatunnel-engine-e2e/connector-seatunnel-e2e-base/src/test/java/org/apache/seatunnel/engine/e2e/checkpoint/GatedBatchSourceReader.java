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

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;

/**
 * Reader of {@link GatedBatchSource}.
 *
 * <p>While the test holds the gate open, every poll emits exactly one row, so the job keeps making
 * observable progress for as long as the test needs it to stay RUNNING. Once the gate is released
 * the reader reports "no more element" and the bounded job runs into its normal close path.
 *
 * <p>The gate wait happens outside the checkpoint lock and {@code Collector#getCheckpointLock()}
 * only guards the single {@link Collector#collect} call, so holding the job open never delays
 * barrier injection or state snapshots.
 */
@Slf4j
public class GatedBatchSourceReader implements SourceReader<SeaTunnelRow, GatedBatchSourceSplit> {

    private final Context context;
    private final String holdKey;
    private final long emitIntervalMs;

    private long emittedRows;
    private boolean noMoreElement;

    public GatedBatchSourceReader(Context context, String holdKey, long emitIntervalMs) {
        this.context = context;
        this.holdKey = holdKey;
        this.emitIntervalMs = emitIntervalMs;
    }

    @Override
    public void open() throws Exception {}

    @Override
    public void close() {}

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        if (noMoreElement) {
            return;
        }
        if (BatchSourceGate.awaitRelease(holdKey, emitIntervalMs)) {
            noMoreElement = true;
            // WARN on purpose: the module's test logging keeps the root logger at WARN.
            log.warn(
                    "Gated source reader left the hold on key {} after {} rows",
                    holdKey,
                    emittedRows);
            if (!Thread.currentThread().isInterrupted()) {
                context.signalNoMoreElement();
            }
            return;
        }
        long rowId = emittedRows++;
        synchronized (output.getCheckpointLock()) {
            output.collect(new SeaTunnelRow(new Object[] {rowId}));
        }
        BatchSourceGate.recordEmitted(holdKey);
    }

    @Override
    public List<GatedBatchSourceSplit> snapshotState(long checkpointId) throws Exception {
        return Collections.emptyList();
    }

    @Override
    public void addSplits(List<GatedBatchSourceSplit> splits) {
        // The source owns no splits, so there is nothing to add.
    }

    @Override
    public void handleNoMoreSplits() {
        // The reader stays alive until the test releases the gate, independent of split
        // assignment.
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
