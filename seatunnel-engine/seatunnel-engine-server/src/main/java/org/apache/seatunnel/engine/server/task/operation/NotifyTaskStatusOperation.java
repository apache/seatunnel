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

package org.apache.seatunnel.engine.server.task.operation;

import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.execution.TaskExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.serializable.TaskDataSerializerHook;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

public class NotifyTaskStatusOperation extends TracingOperation
        implements IdentifiedDataSerializable {

    private TaskGroupLocation taskGroupLocation;
    private TaskExecutionState taskExecutionState;

    public NotifyTaskStatusOperation() {}

    public NotifyTaskStatusOperation(
            TaskGroupLocation taskGroupLocation, TaskExecutionState taskExecutionState) {
        super();
        this.taskGroupLocation = taskGroupLocation;
        this.taskExecutionState = taskExecutionState;
    }

    @Override
    public final int getFactoryId() {
        return TaskDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return TaskDataSerializerHook.NOTIFY_TASK_STATUS_OPERATOR;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(taskGroupLocation);
        out.writeObject(taskExecutionState);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        taskGroupLocation = in.readObject();
        taskExecutionState = in.readObject();
    }

    @Override
    public void runInternal() throws Exception {
        SeaTunnelServer server = getService();
        server.getCoordinatorService().updateTaskExecutionState(taskExecutionState);
    }

    @Override
    public Object getResponse() {
        return super.getResponse();
    }
}
