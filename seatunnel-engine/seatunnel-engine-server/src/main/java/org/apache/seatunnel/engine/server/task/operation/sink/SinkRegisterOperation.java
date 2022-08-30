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
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.execution.WorkerTaskLocation;
import org.apache.seatunnel.engine.server.serializable.TaskDataSerializerHook;
import org.apache.seatunnel.engine.server.task.SinkAggregatedCommitterTask;
import org.apache.seatunnel.engine.server.task.TaskRuntimeException;

import com.hazelcast.cluster.Address;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;

public class SinkRegisterOperation extends Operation implements IdentifiedDataSerializable {

    private static final ILogger LOGGER = Logger.getLogger(SinkRegisterOperation.class);
    private static final int RETRY_TIME = 5;
    private static final int RETRY_INTERVAL = 2000;
    private WorkerTaskLocation writerTaskID;
    private TaskLocation committerTaskID;

    public SinkRegisterOperation() {
    }

    public SinkRegisterOperation(WorkerTaskLocation writerTaskID, TaskLocation committerTaskID) {
        this.writerTaskID = writerTaskID;
        this.committerTaskID = committerTaskID;
    }

    @Override
    public void run() throws Exception {
        SeaTunnelServer server = getService();
        Address readerAddress = getCallerAddress();
        SinkAggregatedCommitterTask<?> task = null;
        for (int i = 0; i < RETRY_TIME; i++) {
            try {
                task = server.getTaskExecutionService().getExecutionContext(committerTaskID.getTaskGroupLocation())
                        .getTaskGroup().getTask(committerTaskID.getTaskID());
                break;
            } catch (NullPointerException e) {
                LOGGER.warning("can't get committer task , waiting task started");
                Thread.sleep(RETRY_INTERVAL);
            }
        }
        if (task == null) {
            LOGGER.severe("can't connect with committer task");
            throw new TaskRuntimeException("can't connect with committer task");
        }
        task.receivedWriterRegister(writerTaskID, readerAddress);
    }

    @Override
    public String getServiceName() {
        return SeaTunnelServer.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        writerTaskID.writeData(out);
        committerTaskID.writeData(out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        writerTaskID.readData(in);
        committerTaskID.readData(in);
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
