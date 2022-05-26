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

import org.apache.seatunnel.engine.api.checkpoint.StateBackend;
import org.apache.seatunnel.engine.api.sink.SinkWriter;
import org.apache.seatunnel.engine.api.source.InputStatus;
import org.apache.seatunnel.engine.api.source.SourceReader;
import org.apache.seatunnel.engine.api.transform.AbstractTransformation;
import org.apache.seatunnel.engine.executionplan.ExecutionId;
import org.apache.seatunnel.engine.executionplan.JobInformation;

import java.io.IOException;
import java.util.List;

public class StreamTask implements Task {
    private JobInformation jobInformation;
    private ExecutionId executionID;
    private int taskId;
    private SourceReader sourceReader;
    private List<AbstractTransformation> transformations;
    private SinkWriter sinkWriter;
    private StateBackend stateBackend;

    private StreamTaskSourceCollector collector;
    private boolean needCheckpoint;
    private int checkpointId;

    private boolean isRunning;
    private final int sleepTimeMs = 500;

    public StreamTask(JobInformation jobInformation, ExecutionId executionID, int taskId, SourceReader sourceReader, List<AbstractTransformation> transformations, SinkWriter sinkWriter, StateBackend stateBackend) {
        this.jobInformation = jobInformation;
        this.executionID = executionID;
        this.taskId = taskId;
        this.sourceReader = sourceReader;
        this.transformations = transformations;
        this.sinkWriter = sinkWriter;
        this.stateBackend = stateBackend;
    }

    public void checkpoint() throws IOException {
        if (needCheckpoint) {
            byte[] sourceReaderState = sourceReader.snapshotState(checkpointId);
            byte[] sinkWriterState = sinkWriter.snapshotState(checkpointId);

            //build TaskState
            TaskState taskState = new TaskState(checkpointId, sourceReaderState, sinkWriterState);

            //update state
            stateBackend.set(jobInformation.getJobId(), taskId, taskState.serialize(taskState));

            //commit
            sinkWriter.notifyCheckpointComplete(checkpointId);
        }
    }

    @Override
    public void run() {
        try {
            while (isRunning) {
                InputStatus inputStatus = sourceReader.pullNext(collector);

                //run checkpoint
                checkpoint();

                //Indicator that no data is currently available, but more data will be available in the future again.
                if (inputStatus == InputStatus.NOTHING_AVAILABLE) {
                    Thread.sleep(sleepTimeMs);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void beforeRun() {
        //build collector..
        collector = new StreamTaskSourceCollector(transformations, sinkWriter);
        isRunning = true;

    }
}
