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

package org.apache.seatunnel.e2e.source.checkpointable;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

public class CheckpointableSequenceSourceReader
        implements SourceReader<SeaTunnelRow, CheckpointableSequenceSplit> {

    private final Deque<CheckpointableSequenceSplit> activeSplits = new ConcurrentLinkedDeque<>();
    private final Context context;
    private final int recordsPerPoll;
    private final long emitIntervalMs;
    private volatile boolean noMoreSplits;

    public CheckpointableSequenceSourceReader(
            Context context, int recordsPerPoll, long emitIntervalMs) {
        this.context = context;
        this.recordsPerPoll = recordsPerPoll;
        this.emitIntervalMs = emitIntervalMs;
    }

    @Override
    public void open() throws Exception {}

    @Override
    public void close() {}

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            CheckpointableSequenceSplit split = activeSplits.poll();
            if (split == null) {
                if (noMoreSplits) {
                    context.signalNoMoreElement();
                } else {
                    Thread.sleep(Math.max(emitIntervalMs, 10L));
                }
                return;
            }

            int emitted = 0;
            while (emitted < recordsPerPoll && split.hasRemaining()) {
                output.collect(new SeaTunnelRow(new Object[] {split.advance()}));
                emitted++;
            }

            if (split.hasRemaining()) {
                activeSplits.addLast(split);
            }

            if (emitIntervalMs > 0L) {
                Thread.sleep(emitIntervalMs);
            }
        }
    }

    @Override
    public List<CheckpointableSequenceSplit> snapshotState(long checkpointId) throws Exception {
        return new ArrayList<>(activeSplits);
    }

    @Override
    public void addSplits(List<CheckpointableSequenceSplit> splits) {
        activeSplits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        noMoreSplits = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
