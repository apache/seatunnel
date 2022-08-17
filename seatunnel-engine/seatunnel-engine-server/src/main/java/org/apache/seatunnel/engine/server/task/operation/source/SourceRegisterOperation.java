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

package org.apache.seatunnel.engine.server.task.operation.source;

import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.execution.TaskInfo;
import org.apache.seatunnel.engine.server.serializable.TaskDataSerializerHook;
import org.apache.seatunnel.engine.server.task.SourceSplitEnumeratorTask;

import com.hazelcast.cluster.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;

/**
 * For {@link org.apache.seatunnel.api.source.SourceReader} to register with
 * the {@link org.apache.seatunnel.api.source.SourceSplitEnumerator}
 */
public class SourceRegisterOperation extends Operation implements IdentifiedDataSerializable {

    private TaskInfo readerTaskInfo;
    private TaskInfo enumeratorTaskInfo;

    public SourceRegisterOperation() {
    }

    public SourceRegisterOperation(TaskInfo readerTaskInfo, TaskInfo enumeratorTaskInfo) {
        this.readerTaskInfo = readerTaskInfo;
        this.enumeratorTaskInfo = enumeratorTaskInfo;
    }

    @Override
    public void run() throws Exception {
        SeaTunnelServer server = getService();
        Address readerAddress = getCallerAddress();
        SourceSplitEnumeratorTask<?> task =
                server.getTaskExecutionService().getExecutionContext(enumeratorTaskInfo.getTaskGroupId()).getTaskGroup().getTask(enumeratorTaskInfo.getTaskInfo());
        task.receivedReader(readerTaskInfo, readerAddress);
    }

    @Override
    public String getServiceName() {
        return SeaTunnelServer.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        readerTaskInfo.writeData(out);
        enumeratorTaskInfo.writeData(out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        readerTaskInfo.readData(in);
        enumeratorTaskInfo.readData(in);
    }

    @Override
    public int getFactoryId() {
        return TaskDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return TaskDataSerializerHook.SOURCE_REGISTER_TYPE;
    }
}
