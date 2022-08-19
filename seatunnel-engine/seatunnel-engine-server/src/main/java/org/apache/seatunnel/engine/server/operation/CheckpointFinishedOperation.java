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

package org.apache.seatunnel.engine.server.operation;

import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.server.serializable.OperationDataSerializerHook;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class CheckpointFinishedOperation extends AsyncOperation {
    private long jobId;

    private long pipelineId;

    private long checkpointId;

    public CheckpointFinishedOperation() {
    }

    public CheckpointFinishedOperation(long jobId,
                                       long pipelineId,
                                       long checkpointId) {
        this.jobId = jobId;
        this.pipelineId = pipelineId;
        this.checkpointId = checkpointId;
    }

    @Override
    public int getClassId() {
        return OperationDataSerializerHook.SUBMIT_OPERATOR;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeLong(jobId);
        out.writeLong(pipelineId);
        out.writeLong(checkpointId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        jobId = in.readLong();
        pipelineId = in.readLong();
        checkpointId = in.readLong();
    }

    @Override
    protected PassiveCompletableFuture<?> doRun() throws Exception {
        // TODO: Notifies all tasks of the pipeline about the status of the checkpoint
        return null;
    }
}
