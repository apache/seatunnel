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

import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.execution.Task;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.serializable.CheckpointDataSerializerHook;
import org.apache.seatunnel.engine.server.task.operation.TaskOperation;
import org.apache.seatunnel.engine.server.task.record.Barrier;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@NoArgsConstructor
@Slf4j
public class CheckpointBarrierTriggerOperation extends TaskOperation {
    protected Barrier barrier;

    public CheckpointBarrierTriggerOperation(Barrier barrier, TaskLocation taskLocation) {
        super(taskLocation);
        this.barrier = barrier;
    }

    @Override
    public int getFactoryId() {
        return CheckpointDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return CheckpointDataSerializerHook.CHECKPOINT_BARRIER_TRIGGER_OPERATOR;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(barrier);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        // TODO: support another barrier
        barrier = in.readObject();
    }

    /**
     * Hands the barrier to the target task on this worker.
     *
     * <p>No task of a pipeline starts before every task has reported that it is ready, so when a
     * barrier exists, its target task group is already registered on this worker. If it cannot be
     * found, that deployment has ended. The barrier is not retried: a pipeline restore deploys a
     * new task group at the same location, and a retried barrier would reach that deployment, which
     * belongs to a new checkpoint coordinator and never completes this checkpoint.
     */
    @Override
    public void runInternal() throws Exception {
        SeaTunnelServer server = getService();
        Task task =
                server.getTaskExecutionService()
                        .getExecutionContext(taskLocation.getTaskGroupLocation())
                        .getTaskGroup()
                        .getTask(taskLocation.getTaskID());
        task.getExecutionContext()
                .getTaskExecutionService()
                .asyncExecuteFunction(
                        taskLocation.getTaskGroupLocation(),
                        () -> {
                            try {
                                log.debug("CheckpointBarrierTriggerOperation [{}]", taskLocation);
                                task.triggerBarrier(barrier);
                            } catch (Exception e) {
                                task.getExecutionContext()
                                        .sendToMaster(
                                                new CheckpointErrorReportOperation(
                                                        taskLocation, e));
                            }
                        });
    }
}
