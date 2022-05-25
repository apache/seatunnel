/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.api.example;

import org.apache.seatunnel.engine.api.source.InputStatus;
import org.apache.seatunnel.engine.api.source.SourceReader;
import org.apache.seatunnel.engine.api.type.Row;
import org.apache.seatunnel.engine.api.type.SeaTunnelRow;
import org.apache.seatunnel.engine.utils.Collector;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class SimpleSourceReader implements SourceReader {
    private final Random random = ThreadLocalRandom.current();
    private final String[] names = {"Wenjun", "Fanjia", "Zongwen", "CalvinKirs"};
    private final int[] ages = {11, 22, 33, 44};
    private int pollNumber = 2;
    private final int sleepTime = 1000;

    @Override
    public void open() {

    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public InputStatus pullNext(Collector<Row> output) throws Exception {
        final int bound = 10;
        for (int i = 0; i < random.nextInt(bound); i++) {
            int randomIndex = random.nextInt(names.length);
            Map<String, Object> fieldMap = new HashMap<>();
            fieldMap.put("name", names[randomIndex]);
            fieldMap.put("age", ages[randomIndex]);
            fieldMap.put("timestamp", System.currentTimeMillis());
            SeaTunnelRow seaTunnelRow = new SeaTunnelRow(new Object[]{names[randomIndex], ages[randomIndex], System.currentTimeMillis()}, fieldMap);
            output.collect(seaTunnelRow);
        }

        Thread.sleep(sleepTime);
        return pollNumber-- > 0 ? InputStatus.MORE_AVAILABLE : InputStatus.END_OF_INPUT;
    }

    @Override
    public byte[] snapshotState(int checkpointId) {
        return new byte[0];
    }

    @Override
    public void notifyCheckpointComplete(int checkpointId) {

    }
}
