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

package org.apache.seatunnel.benchmark.connector.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/** Emits records according to an absolute open-loop schedule instead of pacing on completions. */
public final class BenchmarkSourceReader
        implements SourceReader<SeaTunnelRow, BenchmarkSourceSplit> {

    private final Context context;
    private BenchmarkSourceSplit split;
    private String payload;
    private boolean noMoreSplits;
    private boolean finished;

    public BenchmarkSourceReader(Context context) {
        this.context = context;
    }

    @Override
    public void open() {}

    @Override
    public void close() throws IOException {}

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws InterruptedException {
        if (finished) {
            return;
        }
        if (split == null) {
            if (noMoreSplits) {
                finish();
            }
            return;
        }
        if (split.getNextSequence() >= split.getTotalRows()) {
            finish();
            return;
        }

        long nextScheduledMillis = scheduledMillis(split, split.getNextSequence());
        long nowMillis = System.currentTimeMillis();
        if (nextScheduledMillis > nowMillis) {
            Thread.sleep(Math.min(nextScheduledMillis - nowMillis, 10L));
            return;
        }

        int emitted = 0;
        synchronized (output.getCheckpointLock()) {
            while (emitted < split.getEmitBatchSize()
                    && split.getNextSequence() < split.getTotalRows()) {
                long sequence = split.getNextSequence();
                long scheduledAtMillis = scheduledMillis(split, sequence);
                if (split.getRatePerSecond() > 0
                        && scheduledAtMillis > System.currentTimeMillis()) {
                    break;
                }
                SeaTunnelRow row =
                        new SeaTunnelRow(new Object[] {sequence, scheduledAtMillis, payload, 0L});
                row.setTableId("benchmark.benchmark.events");
                output.collect(row);
                split.advance();
                emitted++;
            }
        }

        if (split.getNextSequence() >= split.getTotalRows()) {
            finish();
        }
    }

    @Override
    public List<BenchmarkSourceSplit> snapshotState(long checkpointId) {
        return split == null ? Collections.emptyList() : Collections.singletonList(split.copy());
    }

    @Override
    public void addSplits(List<BenchmarkSourceSplit> splits) {
        if (splits.isEmpty()) {
            return;
        }
        if (split != null) {
            throw new IllegalStateException("Benchmark source reader only supports one split");
        }
        split = splits.get(0);
        payload = createPayload(split.getPayloadSize());
    }

    @Override
    public void handleNoMoreSplits() {
        noMoreSplits = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // The synthetic source has no external offsets to commit after coordinator completion.
    }

    static long scheduledMillis(BenchmarkSourceSplit split, long sequence) {
        if (split.getRatePerSecond() == 0) {
            return System.currentTimeMillis();
        }
        long rate = split.getRatePerSecond();
        long wholeSeconds = sequence / rate;
        long remainder = sequence % rate;
        return split.getStartEpochMillis() + wholeSeconds * 1_000L + remainder * 1_000L / rate;
    }

    private static String createPayload(int size) {
        char[] chars = new char[size];
        for (int index = 0; index < chars.length; index++) {
            chars[index] = (char) ('a' + index % 26);
        }
        return new String(chars);
    }

    private void finish() {
        if (!finished) {
            finished = true;
            context.signalNoMoreElement();
        }
    }
}
