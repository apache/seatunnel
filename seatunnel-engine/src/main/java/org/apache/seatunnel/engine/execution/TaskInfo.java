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

package org.apache.seatunnel.engine.execution;

import org.apache.seatunnel.engine.api.serialization.Serializer;
import org.apache.seatunnel.engine.api.sink.SinkWriter;
import org.apache.seatunnel.engine.api.source.Boundedness;
import org.apache.seatunnel.engine.api.source.SourceReader;
import org.apache.seatunnel.engine.api.transform.AbstractTransformation;
import org.apache.seatunnel.engine.executionplan.ExecutionId;
import org.apache.seatunnel.engine.executionplan.JobInformation;

import java.io.IOException;
import java.util.List;

public class TaskInfo implements Serializer<TaskInfo> {

    public final JobInformation jobInformation;
    public final ExecutionId executionID;
    public final int taskId;
    public final Boundedness boundedness;
    public final SourceReader sourceReader;
    public final List<AbstractTransformation> transformations;
    public final SinkWriter sinkWriter;

    public TaskInfo(JobInformation jobInformation, ExecutionId executionID, int taskId, Boundedness boundedness, SourceReader sourceReader, List<AbstractTransformation> transformations, SinkWriter sinkWriter) {
        this.jobInformation = jobInformation;
        this.executionID = executionID;
        this.taskId = taskId;
        this.boundedness = boundedness;
        this.sourceReader = sourceReader;
        this.transformations = transformations;
        this.sinkWriter = sinkWriter;
    }

    @Override
    public byte[] serialize(TaskInfo obj) throws IOException {
        throw new RuntimeException("not support now");
    }

    @Override
    public TaskInfo deserialize(byte[] serialized) throws IOException {
        throw new RuntimeException("not support now");
    }
}
