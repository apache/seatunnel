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

import org.apache.seatunnel.engine.api.sink.SinkWriter;
import org.apache.seatunnel.engine.api.source.InputStatus;
import org.apache.seatunnel.engine.api.source.SourceReader;
import org.apache.seatunnel.engine.api.transform.AbstractTransformation;
import org.apache.seatunnel.engine.api.type.Row;
import org.apache.seatunnel.engine.api.type.TerminateRow;
import org.apache.seatunnel.engine.executionplan.ExecutionId;
import org.apache.seatunnel.engine.executionplan.JobInformation;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class BatchTask implements Task {

    private JobInformation jobInformation;
    private ExecutionId executionID;
    private int taskId;
    private SourceReader sourceReader;
    private List<AbstractTransformation> transformations;
    private SinkWriter sinkWriter;
    private Channel channel;
    private BatchTaskSourceCollector collector;

    private PushHandler pushHandler;
    private PollHandler pollHandler;

    private ExecutorService executorService;

    public BatchTask(JobInformation jobInformation, ExecutionId executionID, int taskId, SourceReader sourceReader, List<AbstractTransformation> transformations, SinkWriter sinkWriter) {
        this.jobInformation = jobInformation;
        this.executionID = executionID;
        this.taskId = taskId;
        this.sourceReader = sourceReader;
        this.transformations = transformations;
        this.sinkWriter = sinkWriter;
        this.channel = new MemoryBufferedChannel(jobInformation);

        this.executorService = Executors.newFixedThreadPool(2);
    }

    @Override
    public void run() {

        executorService.submit(pushHandler);
        executorService.submit(pollHandler);
        executorService.shutdown();

        final int timeOut = 3650;
        try {
            if (!executorService.awaitTermination(timeOut, TimeUnit.DAYS)) {
                throw new RuntimeException("The execution time exceeds " + timeOut + " days");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    public void beforeRun() {
        //build StreamCollector
        collector = new BatchTaskSourceCollector(transformations, channel);
        //build worker Handler

        pushHandler = new PushHandler(sourceReader, collector, channel);
        pollHandler = new PollHandler(channel, sinkWriter);

    }

    public static class PushHandler implements Runnable {
        SourceReader sourceReader;
        BatchTaskSourceCollector collector;
        Channel channel;

        public PushHandler(SourceReader sourceReader, BatchTaskSourceCollector collector, Channel channel) {
            this.sourceReader = sourceReader;
            this.collector = collector;
            this.channel = channel;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    InputStatus inputStatus = sourceReader.pullNext(collector);
                    if (inputStatus == InputStatus.END_OF_INPUT) {
                        channel.push(TerminateRow.get());
                        channel.flush();
                        break;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static class PollHandler implements Runnable {
        Channel channel;
        SinkWriter sinkWriter;

        public PollHandler(Channel channel, SinkWriter sinkWriter) {
            this.channel = channel;
            this.sinkWriter = sinkWriter;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    Row poll = channel.pull();
                    if (poll == null) {
                        break;
                    }
                    sinkWriter.write(poll);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
