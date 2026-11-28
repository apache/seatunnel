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

import java.nio.file.Files;
import java.nio.file.Paths;
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
    /** Offsets a split may advance before the reader parks; negative keeps the historical mode. */
    private final long pauseAfterOffsetsPerSplit;
    /** File whose existence opens the progress gate; empty means the gate is never closed. */
    private final String resumeFile;

    private volatile boolean noMoreSplits;

    public CheckpointableSequenceSourceReader(
            Context context, int recordsPerPoll, long emitIntervalMs) {
        this(context, recordsPerPoll, emitIntervalMs, -1L, "");
    }

    public CheckpointableSequenceSourceReader(
            Context context,
            int recordsPerPoll,
            long emitIntervalMs,
            long pauseAfterOffsetsPerSplit,
            String resumeFile) {
        this.context = context;
        this.recordsPerPoll = recordsPerPoll;
        this.emitIntervalMs = emitIntervalMs;
        this.pauseAfterOffsetsPerSplit = pauseAfterOffsetsPerSplit;
        this.resumeFile = resumeFile == null ? "" : resumeFile;
    }

    @Override
    public void open() throws Exception {}

    @Override
    public void close() {}

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        if (progressGateClosed()) {
            // The gate is checked and honoured outside the checkpoint lock on purpose. A paused
            // reader must keep returning from pollNext without emitting: blocking inside the
            // checkpoint lock would prevent the barrier injector from snapshotting this reader's
            // parked split state, so the savepoint that the pause is meant to make partial could
            // never complete. Sleeping one poll interval also keeps cancellation responsive.
            Thread.sleep(Math.max(emitIntervalMs, 10L));
            return;
        }
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

    /**
     * Tells whether this reader has advanced its current split to the configured pause boundary and
     * the resume file that releases the gate does not exist yet.
     *
     * <p>The gate is owned by the test fixture, so the pause point is a deterministic offset of
     * every split instead of a wall-clock guess about how far a reader happened to get. A split
     * that has no data left keeps the gate open so a misconfigured pause point can never park a
     * reader forever, and an already created resume file keeps it open across a restore.
     */
    private boolean progressGateClosed() {
        if (pauseAfterOffsetsPerSplit < 0L) {
            return false;
        }
        CheckpointableSequenceSplit split = activeSplits.peekFirst();
        if (split == null || !split.hasRemaining()) {
            return false;
        }
        if (split.getNextOffset() - split.getStartOffset() < pauseAfterOffsetsPerSplit) {
            return false;
        }
        return resumeFile.isEmpty() || !Files.exists(Paths.get(resumeFile));
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
