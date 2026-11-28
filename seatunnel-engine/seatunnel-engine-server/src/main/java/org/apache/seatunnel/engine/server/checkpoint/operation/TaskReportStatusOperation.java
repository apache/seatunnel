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

import org.apache.seatunnel.common.utils.RetryUtils;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.serializable.CheckpointDataSerializerHook;
import org.apache.seatunnel.engine.server.serializable.TaskDataSerializerHook;
import org.apache.seatunnel.engine.server.task.operation.TracingOperation;
import org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState;

import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.IOException;

import static com.hazelcast.internal.serialization.impl.SerializationConstants.CONSTANT_TYPE_DATA_SERIALIZABLE;
import static com.hazelcast.internal.serialization.impl.SerializationConstants.CONSTANT_TYPE_NULL;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class TaskReportStatusOperation extends TracingOperation
        implements IdentifiedDataSerializable {

    private TaskLocation location;
    private SeaTunnelTaskState status;

    @Override
    public int getFactoryId() {
        return CheckpointDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return CheckpointDataSerializerHook.TASK_REPORT_STATUS_OPERATOR;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(location);
        out.writeObject(status);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        if (!(in instanceof BufferObjectDataInput)) {
            throw new IOException(
                    getClass().getName()
                            + " requires a buffered input to validate "
                            + TaskLocation.class.getName()
                            + "; actual input is "
                            + in.getClass().getName());
        }
        BufferObjectDataInput buffer = (BufferObjectDataInput) in;
        int position = buffer.position();
        // Validate only the top-level location identity, not its nested objects or status.
        // Absolute reads leave the cursor unchanged for the constrained typed read below.
        int serializerType = buffer.readInt(position);
        if (serializerType != CONSTANT_TYPE_DATA_SERIALIZABLE) {
            throw new IOException(
                    getClass().getName()
                            + " expects "
                            + TaskLocation.class.getName()
                            + " but wire serializer type is "
                            + (serializerType == CONSTANT_TYPE_NULL ? "null" : serializerType));
        }
        // Bit zero is Hazelcast DataSerializableSerializer's IDS_FLAG.
        int header = Byte.toUnsignedInt(buffer.readByte(position + Integer.BYTES));
        if ((header & 1) == 0) {
            throw new IOException(
                    getClass().getName()
                            + " expects an identified "
                            + TaskLocation.class.getName()
                            + " but wire header is "
                            + header);
        }
        int factoryId = buffer.readInt(position + Integer.BYTES + Byte.BYTES);
        int classId = buffer.readInt(position + 2 * Integer.BYTES + Byte.BYTES);
        if (factoryId != TaskDataSerializerHook.FACTORY_ID
                || classId != TaskDataSerializerHook.TASK_LOCATION_TYPE) {
            throw new IOException(
                    getClass().getName()
                            + " expects "
                            + TaskLocation.class.getName()
                            + " but wire factory ID is "
                            + factoryId
                            + " and class ID is "
                            + classId);
        }
        location = in.readObject(TaskLocation.class);
        status = in.readObject();
        if (status == null) {
            throw new IOException(getClass().getName() + " received null status for " + location);
        }
    }

    @Override
    public void runInternal() throws Exception {
        CoordinatorService coordinatorService =
                ((SeaTunnelServer) getService()).getCoordinatorService();
        RetryUtils.retryWithException(
                () -> {
                    coordinatorService
                            .getJobMaster(location.getJobId())
                            .getCheckpointManager()
                            .reportedTask(this);
                    return null;
                },
                new RetryUtils.RetryMaterial(
                        Constant.OPERATION_RETRY_TIME,
                        true,
                        e -> true,
                        Constant.OPERATION_RETRY_SLEEP));
    }
}
