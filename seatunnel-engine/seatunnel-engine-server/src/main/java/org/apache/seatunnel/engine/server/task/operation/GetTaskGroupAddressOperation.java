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

import org.apache.seatunnel.common.utils.RetryUtils;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.serializable.TaskDataSerializerHook;

import com.hazelcast.cluster.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Objects;

public class GetTaskGroupAddressOperation extends TracingOperation
        implements IdentifiedDataSerializable {

    private TaskLocation taskLocation;

    private Address response;

    public GetTaskGroupAddressOperation() {}

    public GetTaskGroupAddressOperation(TaskLocation taskLocation) {
        this.taskLocation = taskLocation;
    }

    @Override
    public void runInternal() throws Exception {
        SeaTunnelServer server = getService();
        response =
                RetryUtils.retryWithException(
                        () ->
                                server.getCoordinatorService()
                                        .getJobMaster(taskLocation.getJobId())
                                        .queryTaskGroupAddress(taskLocation.getTaskGroupLocation()),
                        new RetryUtils.RetryMaterial(
                                Constant.OPERATION_RETRY_TIME,
                                true,
                                Objects::nonNull,
                                Constant.OPERATION_RETRY_SLEEP));
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public String getServiceName() {
        return SeaTunnelServer.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(taskLocation);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        taskLocation = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return TaskDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return TaskDataSerializerHook.GET_TASKGROUP_ADDRESS_TYPE;
    }
}
