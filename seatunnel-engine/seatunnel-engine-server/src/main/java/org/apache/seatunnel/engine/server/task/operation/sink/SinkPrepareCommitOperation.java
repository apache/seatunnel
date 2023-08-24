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

package org.apache.seatunnel.engine.server.task.operation.sink;

import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.TaskExecutionService;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.serializable.TaskDataSerializerHook;
import org.apache.seatunnel.engine.server.task.SinkAggregatedCommitterTask;
import org.apache.seatunnel.engine.server.task.operation.checkpoint.BarrierFlowOperation;
import org.apache.seatunnel.engine.server.task.record.Barrier;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import lombok.NoArgsConstructor;

import java.io.IOException;

@NoArgsConstructor
public class SinkPrepareCommitOperation<CommitInfoT> extends BarrierFlowOperation {
    private byte[] commitInfos;

    public SinkPrepareCommitOperation(
            Barrier checkpointBarrier, TaskLocation taskLocation, byte[] commitInfos) {
        super(checkpointBarrier, taskLocation);
        this.commitInfos = commitInfos;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeByteArray(commitInfos);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        commitInfos = in.readByteArray();
    }

    @Override
    public String getServiceName() {
        return SeaTunnelServer.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return TaskDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return TaskDataSerializerHook.SINK_PREPARE_COMMIT_TYPE;
    }

    @Override
    public void run() throws Exception {
        TaskExecutionService taskExecutionService =
                ((SeaTunnelServer) getService()).getTaskExecutionService();
        SinkAggregatedCommitterTask<CommitInfoT, ?> committerTask =
                taskExecutionService.getTask(taskLocation);
        ClassLoader taskClassLoader =
                taskExecutionService
                        .getExecutionContext(taskLocation.getTaskGroupLocation())
                        .getClassLoader();
        ClassLoader mainClassLoader = Thread.currentThread().getContextClassLoader();

        if (commitInfos != null) {
            CommitInfoT deserializeCommitInfo = null;
            try {
                Thread.currentThread().setContextClassLoader(taskClassLoader);
                deserializeCommitInfo =
                        committerTask.getCommitInfoSerializer().deserialize(commitInfos);
            } finally {
                Thread.currentThread().setContextClassLoader(mainClassLoader);
            }
            committerTask.receivedWriterCommitInfo(barrier.getId(), deserializeCommitInfo);
        }
        committerTask.triggerBarrier(barrier);
    }
}
