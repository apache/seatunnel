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
import org.apache.seatunnel.engine.api.transform.Transformation;
import org.apache.seatunnel.engine.api.type.Row;
import org.apache.seatunnel.engine.api.type.TerminateRow;
import org.apache.seatunnel.engine.executionplan.ExecutionId;
import org.apache.seatunnel.engine.executionplan.JobInformation;

import java.io.IOException;
import java.util.List;

public class BatchTask implements Task {

    private JobInformation jobInformation;
    private ExecutionId executionID;
    private int taskId;
    private SourceReader sourceReader;
    private List<Transformation> transformations;
    private SinkWriter sinkWriter;
    private Channel channel;
    private BatchTaskSourceCollector collector;

    //Two worker threads
    private Thread pushThread;
    private Thread pollThread;
    private boolean inputIsEnd;

    public BatchTask(JobInformation jobInformation, ExecutionId executionID, int taskId, SourceReader sourceReader, List<Transformation> transformations, SinkWriter sinkWriter) {
        this.jobInformation = jobInformation;
        this.executionID = executionID;
        this.taskId = taskId;
        this.sourceReader = sourceReader;
        this.transformations = transformations;
        this.sinkWriter = sinkWriter;
        this.channel = new MemoryBufferedChannel(jobInformation);
        //this.channel = new MemoryChannel();
    }

    @Override
    public void run() {
        pushThread.start();
        pollThread.start();

        try {

            pushThread.join();
            pollThread.join();

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void beforeRun() {
        //build StreamCollector
        collector = new BatchTaskSourceCollector(transformations, channel);
        //build worker threads
        pushThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        InputStatus inputStatus = sourceReader.pullNext(collector);
                        if (inputStatus == InputStatus.END_OF_INPUT) {
                            channel.push(TerminateRow.get());
                            channel.flush();
                            inputIsEnd = true;
                            break;
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        });

        pollThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Row poll = channel.pull();
                        if (poll == null && inputIsEnd) {
                            break;
                        }
                        sinkWriter.write(poll);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
    }

}
