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

    private final SourceReader.Context context;
    private final Deque<FakeSourceSplit> splits = new ConcurrentLinkedDeque<>();

    private final MultipleTableFakeSourceConfig multipleTableFakeSourceConfig;
    // TableFullName to FakeDataGenerator
    private final Map<String, FakeDataGenerator> fakeDataGeneratorMap;
    private volatile boolean noMoreSplit;
    private final long minSplitReadInterval;
    private volatile long latestTimestamp = 0;

    public FakeSourceReader(
            SourceReader.Context context,
            MultipleTableFakeSourceConfig multipleTableFakeSourceConfig) {
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
                                        FakeDataGenerator::new));
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
        if (currentTimestamp <= latestTimestamp + minSplitReadInterval) {
            return;
        }
        latestTimestamp = currentTimestamp;
        synchronized (output.getCheckpointLock()) {
            FakeSourceSplit split = splits.poll();
            if (null != split) {
                FakeDataGenerator fakeDataGenerator = fakeDataGeneratorMap.get(split.getTableId());
                // Randomly generated data are sent directly to the downstream operator
                List<SeaTunnelRow> seaTunnelRows =
                        fakeDataGenerator.generateFakedRows(split.getRowNum());
                seaTunnelRows.forEach(output::collect);
                log.info(
                        "{} rows of data have been generated in split({}) for table {}. Generation time: {}",
                        seaTunnelRows.size(),
                        split.splitId(),
                        split.getTableId(),
                        latestTimestamp);
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
        Thread.sleep(1000L);
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
