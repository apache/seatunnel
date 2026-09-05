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

package org.apache.seatunnel.engine.server.checkpoint.operation;

import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.common.utils.RetryUtils;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.exception.TaskGroupContextNotFoundException;
import org.apache.seatunnel.engine.server.execution.Task;
import org.apache.seatunnel.engine.server.execution.TaskGroupContext;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.serializable.CheckpointDataSerializerHook;
import org.apache.seatunnel.engine.server.task.operation.TaskOperation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.IOException;

@Getter
@NoArgsConstructor
public class CheckpointFinishedOperation extends TaskOperation {

    private long checkpointId;

    private boolean successful;

    /**
     * Type of the completed or aborted checkpoint.
     *
     * <p>Sink-side schema-change guards need this type after failover because the restored runtime
     * may not have observed the original checkpoint barrier.
     */
    private CheckpointType checkpointType;

    public CheckpointFinishedOperation(
            TaskLocation taskLocation,
            long checkpointId,
            boolean successful,
            CheckpointType checkpointType) {
        super(taskLocation);
        this.checkpointId = checkpointId;
        this.successful = successful;
        this.checkpointType = checkpointType;
    }

    @Override
    public int getFactoryId() {
        return CheckpointDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return CheckpointDataSerializerHook.CHECKPOINT_FINISHED_OPERATOR;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(checkpointId);
        out.writeBoolean(successful);
        out.writeString(checkpointType.getName());
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        checkpointId = in.readLong();
        successful = in.readBoolean();
        checkpointType = CheckpointType.fromName(in.readString());
    }

    @Override
    public void runInternal() throws Exception {
        SeaTunnelServer server = getService();
        RetryUtils.retryWithException(
                () -> {
                    try {
                        TaskGroupContext groupContext =
                                server.getTaskExecutionService()
                                        .getExecutionContext(taskLocation.getTaskGroupLocation());
                        Task task = groupContext.getTaskGroup().getTask(taskLocation.getTaskID());
                        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
                        Thread.currentThread()
                                .setContextClassLoader(
                                        groupContext.getClassLoader(taskLocation.getTaskID()));
                        if (successful) {
                            task.notifyCheckpointComplete(checkpointId, checkpointType);
                        } else {
                            task.notifyCheckpointAborted(checkpointId, checkpointType);
                        }
                        Thread.currentThread().setContextClassLoader(classLoader);
                    } catch (Exception e) {
                        throw new SeaTunnelEngineException(ExceptionUtils.getMessage(e));
                    }
                    return null;
                },
                new RetryUtils.RetryMaterial(
                        Constant.OPERATION_RETRY_TIME,
                        true,
                        exception ->
                                exception instanceof TaskGroupContextNotFoundException
                                        && !server.taskIsEnded(taskLocation.getTaskGroupLocation()),
                        Constant.OPERATION_RETRY_SLEEP));
    }
}
