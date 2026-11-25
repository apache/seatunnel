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
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.serializable.TaskDataSerializerHook;

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
public abstract class TaskOperation extends TracingOperation implements IdentifiedDataSerializable {

    protected TaskLocation taskLocation;

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
        if (!(in instanceof BufferObjectDataInput)) {
            throw new IOException(
                    "Task operation requires a buffered input to validate "
                            + TaskLocation.class.getName());
        }
        BufferObjectDataInput buffer = (BufferObjectDataInput) in;
        int position = buffer.position();
        // Peek at Hazelcast's object/IDS header before selecting any serializer or factory.
        // Absolute reads preserve the cursor for the constrained readObject(Class) below.
        int serializerType = buffer.readInt(position);
        if (serializerType != CONSTANT_TYPE_DATA_SERIALIZABLE) {
            throw new IOException(
                    "Task operation expects "
                            + TaskLocation.class.getName()
                            + " but wire serializer type is "
                            + (serializerType == CONSTANT_TYPE_NULL ? "null" : serializerType));
        }
        // Bit zero is Hazelcast DataSerializableSerializer's IDS_FLAG.
        if ((buffer.readByte(position + Integer.BYTES) & 1) == 0) {
            throw new IOException(
                    "Task operation expects an identified " + TaskLocation.class.getName());
        }
        int factoryId = buffer.readInt(position + Integer.BYTES + Byte.BYTES);
        int classId = buffer.readInt(position + 2 * Integer.BYTES + Byte.BYTES);
        if (factoryId != TaskDataSerializerHook.FACTORY_ID
                || classId != TaskDataSerializerHook.TASK_LOCATION_TYPE) {
            throw new IOException(
                    "Task operation expects "
                            + TaskLocation.class.getName()
                            + " but wire factory ID is "
                            + factoryId
                            + " and class ID is "
                            + classId);
        }
        taskLocation = in.readObject(TaskLocation.class);
    }
}
