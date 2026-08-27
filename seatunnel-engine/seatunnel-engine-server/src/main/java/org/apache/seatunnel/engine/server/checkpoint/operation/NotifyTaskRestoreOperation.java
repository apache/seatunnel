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

import org.apache.seatunnel.common.utils.RetryUtils;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.checkpoint.ActionSubtaskState;
import org.apache.seatunnel.engine.server.exception.TaskGroupContextNotFoundException;
import org.apache.seatunnel.engine.server.execution.Task;
import org.apache.seatunnel.engine.server.execution.TaskGroupContext;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.serializable.CheckpointDataSerializerHook;
import org.apache.seatunnel.engine.server.task.operation.TaskOperation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@NoArgsConstructor
@Slf4j
public class NotifyTaskRestoreOperation extends TaskOperation {

    private List<ActionSubtaskState> restoredState;

    public NotifyTaskRestoreOperation(
            TaskLocation taskLocation, List<ActionSubtaskState> restoredState) {
        super(taskLocation);
        this.restoredState = restoredState;
    }

    @Override
    public int getFactoryId() {
        return CheckpointDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return CheckpointDataSerializerHook.NOTIFY_TASK_RESTORE_OPERATOR;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(restoredState.size());
        for (ActionSubtaskState state : restoredState) {
            out.writeObject(state);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        this.restoredState = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            restoredState.add(in.readObject());
        }
    }

    @Override
    public void runInternal() throws Exception {
        SeaTunnelServer server = getService();
        RetryUtils.retryWithException(
                () -> {
                    log.debug("NotifyTaskRestoreOperation " + taskLocation);
                    TaskGroupContext groupContext =
                            server.getTaskExecutionService()
                                    .getExecutionContext(taskLocation.getTaskGroupLocation());
                    Task task = groupContext.getTaskGroup().getTask(taskLocation.getTaskID());
                    try {
                        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
                        task.getExecutionContext()
                                .getTaskExecutionService()
                                .asyncExecuteFunction(
                                        taskLocation.getTaskGroupLocation(),
                                        () -> {
                                            Thread.currentThread()
                                                    .setContextClassLoader(
                                                            groupContext.getClassLoader(
                                                                    task.getTaskID()));
                                            try {
                                                log.debug(
                                                        "NotifyTaskRestoreOperation.restoreState "
                                                                + restoredState);
                                                task.restoreState(restoredState);
                                                log.debug(
                                                        "NotifyTaskRestoreOperation.finished "
                                                                + restoredState);
                                            } catch (Throwable e) {
                                                task.getExecutionContext()
                                                        .sendToMaster(
                                                                new CheckpointErrorReportOperation(
                                                                        taskLocation, e));
                                            } finally {
                                                Thread.currentThread()
                                                        .setContextClassLoader(classLoader);
                                            }
                                        });

                    } catch (Exception e) {
                        throw new SeaTunnelException(e);
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
