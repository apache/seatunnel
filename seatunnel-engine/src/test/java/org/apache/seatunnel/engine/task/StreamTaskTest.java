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

package org.apache.seatunnel.engine.task;

import org.apache.seatunnel.engine.api.checkpoint.MemoryStateBackend;
import org.apache.seatunnel.engine.api.common.JobID;
import org.apache.seatunnel.engine.api.sink.SinkWriter;
import org.apache.seatunnel.engine.api.source.Boundedness;
import org.apache.seatunnel.engine.api.source.InputStatus;
import org.apache.seatunnel.engine.api.source.SourceReader;
import org.apache.seatunnel.engine.api.type.Row;
import org.apache.seatunnel.engine.api.type.SeaTunnelRow;
import org.apache.seatunnel.engine.config.Configuration;
import org.apache.seatunnel.engine.executionplan.ExecutionId;
import org.apache.seatunnel.engine.executionplan.JobInformation;
import org.apache.seatunnel.engine.utils.Collector;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class StreamTaskTest {

    private final Random random = ThreadLocalRandom.current();
    private final String[] names = {"Z3", "L4", "W5", "Mz"};
    private final int[] ages = {11, 22, 33, 44};

    @Test
    public void testStreamTask() throws InterruptedException {

        long transferSize = 10000;

        Queue<Row> seaTunnelRows = buildTestData(transferSize);

        Queue<Row> receiveRows = new LinkedList<>();


        SourceReader sourceReader = new TestStreamSourceReader(new LinkedList<>(seaTunnelRows));
        SinkWriter sinkWriter = new TestSinkWriter(receiveRows);

        StreamTask streamTask = buildStreamTask(sourceReader, sinkWriter);

        streamTask.beforeRun();

        Thread thread = new Thread(streamTask);


        thread.start();


        Thread.sleep(500);

        Assert.assertEquals(transferSize, receiveRows.size());


    }


    public StreamTask buildStreamTask(SourceReader sourceReader, SinkWriter sinkWriter) {
        JobInformation jobInformation = new JobInformation(new JobID(), "test", new Configuration(), Boundedness.BOUNDED);
        return new StreamTask(
                jobInformation,
                new ExecutionId(),
                1,
                sourceReader,
                new ArrayList<>(),
                sinkWriter,
                new MemoryStateBackend());
    }

    public Queue<Row> buildTestData(long size) {
        LinkedList<Row> queue = new LinkedList<Row>();
        while (size-- > 0) {
            int randomIndex = random.nextInt(names.length);
            Map<String, Object> fieldMap = new HashMap<>(4);
            fieldMap.put("name", names[randomIndex]);
            fieldMap.put("age", ages[randomIndex]);
            fieldMap.put("timestamp", System.currentTimeMillis());
            SeaTunnelRow seaTunnelRow = new SeaTunnelRow(new Object[]{names[randomIndex], ages[randomIndex], System.currentTimeMillis()}, fieldMap);
            queue.add(seaTunnelRow);
        }
        return queue;
    }

    static class TestSinkWriter implements SinkWriter {
        Queue<Row> receiveData;

        public TestSinkWriter(Queue<Row> receiveData) {
            this.receiveData = receiveData;
        }

        @Override
        public void open() {

        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public void write(Row element) throws IOException {
            receiveData.add(element);
        }

        @Override
        public byte[] snapshotState(int checkpointId) {
            return new byte[0];
        }

        @Override
        public void notifyCheckpointComplete(int checkpointId) {

        }
    }


    static class TestStreamSourceReader implements SourceReader {

        Queue<Row> sendData;
        private final Random random = ThreadLocalRandom.current();

        public TestStreamSourceReader(Queue<Row> sendData) {
            this.sendData = sendData;
        }

        @Override
        public void open() {

        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public InputStatus pullNext(Collector<Row> output) throws Exception {
            for (int i = 0; i < random.nextInt(10); i++) {
                Row poll = sendData.poll();
                if (null != poll) {
                    output.collect(poll);
                } else {
                    return InputStatus.NOTHING_AVAILABLE;
                }
            }
            return InputStatus.MORE_AVAILABLE;
        }

        @Override
        public byte[] snapshotState(int checkpointId) {
            return new byte[0];
        }

        @Override
        public void notifyCheckpointComplete(int checkpointId) {

        }
    }
}