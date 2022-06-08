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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class FakeSourceReader implements SourceReader<SeaTunnelRow, FakeSourceSplit> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FakeSourceReader.class);

    private final SourceReader.Context context;

    private final String[] names = {"Wenjun", "Fanjia", "Zongwen", "CalvinKirs"};
    private final int[] ages = {11, 22, 33, 44};
    private final Random random = ThreadLocalRandom.current();

    public FakeSourceReader(SourceReader.Context context) {
        this.context = context;
    }

    @Override
    public void open() {

    }

    @Override
    public void close() {

    }

    @Override
    @SuppressWarnings("magicnumber")
    public void pollNext(Collector<SeaTunnelRow> output) throws InterruptedException {
        // Generate a random number of rows to emit.
        int size = random.nextInt(10);
        for (int i = 0; i < size; i++) {
            int randomIndex = random.nextInt(names.length);
            Map<String, Object> fieldMap = new HashMap<>(4);
            fieldMap.put("name", names[randomIndex]);
            fieldMap.put("age", ages[randomIndex]);
            fieldMap.put("timestamp", System.currentTimeMillis());
            SeaTunnelRow seaTunnelRow = new SeaTunnelRow(new Object[]{names[randomIndex], ages[randomIndex], System.currentTimeMillis()}, fieldMap);
            output.collect(seaTunnelRow);
        }
        if (Boundedness.BOUNDED.equals(context.getBoundedness())) {
            // signal to the source that we have reached the end of the data.
            LOGGER.info("Closed the bounded fake source");
            context.signalNoMoreElement();
        }
        Thread.sleep(1000L);
    }

    @Override
    public List<FakeSourceSplit> snapshotState(long checkpointId) {
        return null;
    }

    @Override
    public void addSplits(List<FakeSourceSplit> splits) {

    }

    @Override
    public void handleNoMoreSplits() {

    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {

    }
}
