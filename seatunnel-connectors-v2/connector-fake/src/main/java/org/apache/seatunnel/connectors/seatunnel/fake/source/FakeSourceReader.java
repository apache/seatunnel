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

package org.apache.seatunnel.connectors.seatunnel.fake.source;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.fake.config.FakeConfig;
import org.apache.seatunnel.connectors.seatunnel.fake.config.MultipleTableFakeSourceConfig;

import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Collectors;

@Slf4j
public class FakeSourceReader implements SourceReader<SeaTunnelRow, FakeSourceSplit> {

    /**
     * Upper bound on the number of rows emitted per {@link #pollNext(Collector)} call. Rows are
     * emitted while holding the checkpoint lock, so emitting an entire split in a single call keeps
     * the lock held for the whole split. For large splits that starves checkpoint/savepoint barrier
     * injection, which needs the same lock, and makes stop-with-savepoint hang in DOING_SAVEPOINT
     * until the checkpoint times out.
     */
    private static final int MAX_ROWS_PER_POLL = 4096;

    private final SourceReader.Context context;
    private final Deque<FakeSourceSplit> splits = new ConcurrentLinkedDeque<>();

    private final MultipleTableFakeSourceConfig multipleTableFakeSourceConfig;
    // TableFullName to FakeDataGenerator
    private final Map<String, FakeDataGenerator> fakeDataGeneratorMap;
    private volatile boolean noMoreSplit;
    private final long minSplitReadInterval;
    private volatile long latestTimestamp = 0;
    // True while the head split has been partially emitted; continuation batches of the same
    // split bypass the split read interval and the idle sleep.
    private boolean splitInProgress = false;

    public FakeSourceReader(
            Context context,
            MultipleTableFakeSourceConfig multipleTableFakeSourceConfig,
            String jobId) {
        this.context = context;
        this.multipleTableFakeSourceConfig = multipleTableFakeSourceConfig;
        this.fakeDataGeneratorMap =
                multipleTableFakeSourceConfig.getFakeConfigs().stream()
                        .collect(
                                Collectors.toMap(
                                        fakeConfig ->
                                                fakeConfig
                                                        .getCatalogTable()
                                                        .getTableId()
                                                        .toTablePath()
                                                        .toString(),
                                        fakeConfig -> new FakeDataGenerator(fakeConfig, jobId)));
        this.minSplitReadInterval =
                multipleTableFakeSourceConfig.getFakeConfigs().stream()
                        .map(FakeConfig::getSplitReadInterval)
                        .min(Integer::compareTo)
                        .get();
    }

    @Override
    public void open() {}

    @Override
    public void close() {}

    @Override
    @SuppressWarnings("MagicNumber")
    public void pollNext(Collector<SeaTunnelRow> output) throws InterruptedException {
        long currentTimestamp = Instant.now().toEpochMilli();
        if (!splitInProgress && currentTimestamp <= latestTimestamp + minSplitReadInterval) {
            return;
        }
        latestTimestamp = currentTimestamp;
        synchronized (output.getCheckpointLock()) {
            splitInProgress = false;
            FakeSourceSplit split = splits.poll();
            if (null != split) {
                FakeDataGenerator fakeDataGenerator = fakeDataGeneratorMap.get(split.getTableId());
                if (fakeDataGenerator.hasCustomRowData()) {
                    int customRowStartIndex = decodeCustomRowStartIndex(split.getRowNum());
                    int customRowCount = fakeDataGenerator.getCustomRowCount();
                    int batchRowNum =
                            Math.min(
                                    Math.max(customRowCount - customRowStartIndex, 0),
                                    MAX_ROWS_PER_POLL);
                    long rowCount =
                            fakeDataGenerator.generateCustomRows(
                                    customRowStartIndex, batchRowNum, output::collect);
                    int nextCustomRowStartIndex = customRowStartIndex + batchRowNum;
                    if (nextCustomRowStartIndex < customRowCount) {
                        // Store custom-row progress in the requeued split so a checkpoint or
                        // savepoint taken between batches snapshots the not-yet-emitted rows.
                        splits.addFirst(
                                new FakeSourceSplit(
                                        split.getTableId(),
                                        split.getSplitId(),
                                        encodeCustomRowStartIndex(nextCustomRowStartIndex)));
                        splitInProgress = true;
                    } else {
                        log.info(
                                "{} rows of custom data have been generated in the last batch of split({}) for table {}. Generation time: {}",
                                rowCount,
                                split.splitId(),
                                split.getTableId(),
                                latestTimestamp);
                    }
                } else {
                    // Randomly generated data are sent directly to the downstream operator.
                    // Emit at most MAX_ROWS_PER_POLL rows per call and requeue the remainder of
                    // the split so the checkpoint lock is released between batches, allowing
                    // checkpoint/savepoint barriers to be injected while a large split is being
                    // generated.
                    int batchRowNum = Math.min(split.getRowNum(), MAX_ROWS_PER_POLL);
                    long rowCount =
                            fakeDataGenerator.generateFakedRows(batchRowNum, output::collect);
                    int remainingRowNum = split.getRowNum() - batchRowNum;
                    if (remainingRowNum > 0) {
                        // The requeued split carries the remaining row count, so a checkpoint or
                        // savepoint taken between batches snapshots the not-yet-emitted rows.
                        splits.addFirst(
                                new FakeSourceSplit(
                                        split.getTableId(), split.getSplitId(), remainingRowNum));
                        splitInProgress = true;
                    } else {
                        log.info(
                                "{} rows of data have been generated in the last batch of split({}) for table {}. Generation time: {}",
                                rowCount,
                                split.splitId(),
                                split.getTableId(),
                                latestTimestamp);
                    }
                }
            } else {
                if (!noMoreSplit) {
                    log.info("wait split!");
                }
            }
        }
        if (noMoreSplit
                && splits.isEmpty()
                && Boundedness.BOUNDED.equals(context.getBoundedness())) {
            // signal to the source that we have reached the end of the data.
            log.info("Closed the bounded fake source");
            context.signalNoMoreElement();
        }
        if (!splitInProgress) {
            Thread.sleep(1000L);
        }
    }

    private static int encodeCustomRowStartIndex(int startIndex) {
        return -startIndex - 1;
    }

    private static int decodeCustomRowStartIndex(int rowNum) {
        return rowNum < 0 ? -rowNum - 1 : 0;
    }

    @Override
    public List<FakeSourceSplit> snapshotState(long checkpointId) throws Exception {
        return new ArrayList<>(splits);
    }

    @Override
    public void addSplits(List<FakeSourceSplit> splits) {
        log.debug("reader {} add splits {}", context.getIndexOfSubtask(), splits);
        this.splits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        noMoreSplit = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
