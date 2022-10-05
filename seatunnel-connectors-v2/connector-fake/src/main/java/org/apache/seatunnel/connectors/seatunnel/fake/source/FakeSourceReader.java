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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

public class FakeSourceReader implements SourceReader<SeaTunnelRow, FakeSourceSplit> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FakeSourceReader.class);

    private final SourceReader.Context context;
    private final Deque<FakeSourceSplit> splits = new LinkedList<>();
    private final FakeDataGenerator fakeDataGenerator;
    boolean noMoreSplit;

    public FakeSourceReader(SourceReader.Context context, FakeDataGenerator fakeDataGenerator) {
        this.context = context;
        this.fakeDataGenerator = fakeDataGenerator;
    }

    @Override
    public void open() {
        // nothing
    }

    @Override
    public void close() {
        // nothing
    }

    @Override
    @SuppressWarnings("magicnumber")
    public void pollNext(Collector<SeaTunnelRow> output) throws InterruptedException {
        synchronized (output.getCheckpointLock()) {
            FakeSourceSplit split = splits.poll();
            if (null != split) {
                // Generate a random number of rows to emit.
                List<SeaTunnelRow> seaTunnelRows = fakeDataGenerator.generateFakedRows();
                for (SeaTunnelRow seaTunnelRow : seaTunnelRows) {
                    output.collect(seaTunnelRow);
                }
                if (Boundedness.BOUNDED.equals(context.getBoundedness())) {
                    // signal to the source that we have reached the end of the data.
                    LOGGER.info("Closed the bounded fake source");
                    context.signalNoMoreElement();
                }
            } else if (noMoreSplit) {
                // signal to the source that we have reached the end of the data.
                LOGGER.info("Closed the bounded fake source");
                context.signalNoMoreElement();
            } else {
                Thread.sleep(1000L);
            }

        }
    }

    @Override
    public List<FakeSourceSplit> snapshotState(long checkpointId) throws Exception {
        return new ArrayList<>(splits);
    }

    @Override
    public void addSplits(List<FakeSourceSplit> splits) {
        this.splits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        noMoreSplit = true;

    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {

    }
}
