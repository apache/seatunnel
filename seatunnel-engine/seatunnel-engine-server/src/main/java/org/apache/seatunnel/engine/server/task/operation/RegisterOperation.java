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
import org.apache.seatunnel.engine.server.execution.TaskExecutionContext;
import org.apache.seatunnel.engine.server.task.TaskFactory;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.UUID;

/**
 * For {@link org.apache.seatunnel.api.source.SourceReader} to register with
 * the {@link org.apache.seatunnel.api.source.SourceSplitEnumerator}
 */
public class RegisterOperation extends Operation implements IdentifiedDataSerializable {

    private long readerTaskID;
    private long enumeratorTaskID;

    public RegisterOperation() {
    }

    public RegisterOperation(long readerTaskID, long enumeratorTaskID) {
        this.readerTaskID = readerTaskID;
        this.enumeratorTaskID = enumeratorTaskID;
    }

    @Override
    public void run() throws Exception {
        SeaTunnelServer server = getService();
        UUID readerUUID = getCallerUuid();
        TaskExecutionContext executionContext =
                server.getTaskExecutionService().getExecutionContext(enumeratorTaskID);
        // TODO register reader to enumerator
    }

    @Override
    public String getServiceName() {
        return SeaTunnelServer.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(readerTaskID);
        out.writeLong(enumeratorTaskID);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        readerTaskID = in.readLong();
        enumeratorTaskID = in.readLong();
    }

    @Override
    public int getFactoryId() {
        return TaskFactory.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return TaskFactory.REGISTER_TYPE;
    }
}
