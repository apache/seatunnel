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
import org.apache.seatunnel.engine.server.execution.TaskDeployState;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.serializable.TaskDataSerializerHook;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import lombok.NonNull;

import java.io.IOException;

public class DeployTaskOperation extends TracingOperation implements IdentifiedDataSerializable {
    private Data taskImmutableInformation;
    private SlotProfile slotProfile;

    private TaskDeployState state;

    public DeployTaskOperation() {}

    public DeployTaskOperation(
            @NonNull SlotProfile slotProfile, @NonNull Data taskImmutableInformation) {
        this.taskImmutableInformation = taskImmutableInformation;
        this.slotProfile = slotProfile;
    }

    @Override
    public void runInternal() throws Exception {
        SeaTunnelServer server = getService();
        state =
                server.getSlotService()
                        .getSlotContext(slotProfile)
                        .getTaskExecutionService()
                        .deployTask(taskImmutableInformation);
    }

    @Override
    public int getFactoryId() {
        return TaskDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return TaskDataSerializerHook.DEPLOY_TASK_OPERATOR;
    }

    @Override
    public Object getResponse() {
        return state;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        IOUtil.writeData(out, taskImmutableInformation);
        out.writeObject(slotProfile);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        taskImmutableInformation = IOUtil.readData(in);
        slotProfile = in.readObject();
    }
}
