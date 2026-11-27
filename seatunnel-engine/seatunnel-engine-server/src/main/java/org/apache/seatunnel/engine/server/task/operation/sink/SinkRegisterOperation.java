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

import org.apache.seatunnel.common.utils.RetryUtils;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.TaskExecutionService;
import org.apache.seatunnel.engine.server.exception.TaskGroupContextNotFoundException;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.serializable.TaskDataSerializerHook;
import org.apache.seatunnel.engine.server.task.SinkAggregatedCommitterTask;
import org.apache.seatunnel.engine.server.task.operation.TracingOperation;

import com.hazelcast.cluster.Address;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

public class SinkRegisterOperation extends TracingOperation implements IdentifiedDataSerializable {

    private static final ILogger LOGGER = Logger.getLogger(SinkRegisterOperation.class);
    private TaskLocation writerTaskID;
    private TaskLocation committerTaskID;

    public SinkRegisterOperation() {}

    public SinkRegisterOperation(TaskLocation writerTaskID, TaskLocation committerTaskID) {
        this.writerTaskID = writerTaskID;
        this.committerTaskID = committerTaskID;
    }

    /**
     * Registers the calling writer with the aggregated committer on this node. Retries while the
     * committer is not deployed yet, and fails at once when its task group has already ended.
     */
    @Override
    public void runInternal() throws Exception {
        SeaTunnelServer server = getService();
        Address readerAddress = getCallerAddress();
        TaskExecutionService taskExecutionService = server.getTaskExecutionService();
        RetryUtils.retryWithException(
                () -> {
                    SinkAggregatedCommitterTask<?, ?> task = getCommitterTask(taskExecutionService);
                    task.receivedWriterRegister(writerTaskID, readerAddress);
                    return null;
                },
                new RetryUtils.RetryMaterial(
                        Constant.OPERATION_RETRY_TIME,
                        true,
                        e ->
                                e instanceof TaskGroupContextNotFoundException
                                        || e instanceof NullPointerException,
                        Constant.OPERATION_RETRY_SLEEP));
    }

    /**
     * Looks up the committer task. While its task group is not active, for example while a restored
     * pipeline is still being deployed, the lookup fails with the retryable {@link
     * TaskGroupContextNotFoundException}. If the task group has ended on this node, for example
     * because the job is being cancelled, retrying cannot succeed, so this fails at once with an
     * exception that is not retried. A pipeline restore cleans the finished context before it
     * deploys the committer again; if that best-effort cleanup failed, a restored writer that
     * registers before the new committer is active fails the same way, and the restore attempt
     * fails instead of waiting.
     */
    private SinkAggregatedCommitterTask<?, ?> getCommitterTask(
            TaskExecutionService taskExecutionService) {
        try {
            return taskExecutionService.getTask(committerTaskID);
        } catch (TaskGroupContextNotFoundException e) {
            if (taskExecutionService.isTaskGroupEnded(committerTaskID.getTaskGroupLocation())) {
                throw new SeaTunnelEngineException(
                        String.format(
                                "Writer %s cannot register with the aggregated committer %s"
                                        + " because the committer's task group has already ended",
                                writerTaskID, committerTaskID),
                        e);
            }
            throw e;
        }
    }

    @Override
    public String getServiceName() {
        return SeaTunnelServer.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(writerTaskID);
        out.writeObject(committerTaskID);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        writerTaskID = in.readObject();
        committerTaskID = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return TaskDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return TaskDataSerializerHook.SINK_REGISTER_TYPE;
    }
}
