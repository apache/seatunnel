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
import org.apache.seatunnel.engine.server.execution.TaskExecutionContext;
import org.apache.seatunnel.engine.server.serializable.TaskDataSerializerHook;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.UUID;

public class SinkRegisterOperation extends Operation implements IdentifiedDataSerializable {

    private long writerTaskID;
    private long committerTaskID;

    public SinkRegisterOperation() {
    }

    public SinkRegisterOperation(long writerTaskID, long committerTaskID) {
        this.writerTaskID = writerTaskID;
        this.committerTaskID = committerTaskID;
    }

    @Override
    public void run() throws Exception {
        SeaTunnelServer server = getService();
        UUID readerUUID = getCallerUuid();
        TaskExecutionContext executionContext =
                server.getTaskExecutionService().getExecutionContext(committerTaskID);
        // TODO register writer to committer
    }

    @Override
    public String getServiceName() {
        return SeaTunnelServer.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(writerTaskID);
        out.writeLong(committerTaskID);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        writerTaskID = in.readLong();
        committerTaskID = in.readLong();
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
