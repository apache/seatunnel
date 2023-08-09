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

import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.common.utils.RetryUtils;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.exception.TaskGroupContextNotFoundException;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.serializable.TaskDataSerializerHook;
import org.apache.seatunnel.engine.server.task.SourceSeaTunnelTask;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AssignSplitOperation<SplitT extends SourceSplit> extends Operation
        implements IdentifiedDataSerializable {

    private List<byte[]> splits;
    private TaskLocation taskID;

    public AssignSplitOperation() {}

    public AssignSplitOperation(TaskLocation taskID, List<byte[]> splits) {
        this.taskID = taskID;
        this.splits = splits;
    }

    @Override
    public void run() throws Exception {
        SeaTunnelServer server = getService();
        RetryUtils.retryWithException(
                () -> {
                    SourceSeaTunnelTask<?, SplitT> task =
                            server.getTaskExecutionService().getTask(taskID);
                    ClassLoader taskClassLoader =
                            server.getTaskExecutionService()
                                    .getExecutionContext(taskID.getTaskGroupLocation())
                                    .getClassLoader();
                    ClassLoader mainClassLoader = Thread.currentThread().getContextClassLoader();
                    List<SplitT> deserializeSplits = new ArrayList<>();
                    try {
                        Thread.currentThread().setContextClassLoader(taskClassLoader);
                        for (byte[] split : this.splits) {
                            deserializeSplits.add(task.getSplitSerializer().deserialize(split));
                        }
                    } finally {
                        Thread.currentThread().setContextClassLoader(mainClassLoader);
                    }

                    task.receivedSourceSplit(deserializeSplits);
                    return null;
                },
                new RetryUtils.RetryMaterial(
                        Constant.OPERATION_RETRY_TIME,
                        true,
                        exception ->
                                exception instanceof TaskGroupContextNotFoundException
                                        && !server.taskIsEnded(taskID.getTaskGroupLocation()),
                        Constant.OPERATION_RETRY_SLEEP));
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(splits.size());
        for (byte[] split : splits) {
            out.writeByteArray(split);
        }
        out.writeObject(taskID);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        int splitCount = in.readInt();
        splits = new ArrayList<>(splitCount);
        for (int i = 0; i < splitCount; i++) {
            splits.add(in.readByteArray());
        }
        taskID = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return TaskDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return TaskDataSerializerHook.ASSIGN_SPLIT_TYPE;
    }
}
