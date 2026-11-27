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

package org.apache.seatunnel.engine.server.execution;

import org.apache.seatunnel.shade.org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.seatunnel.shade.org.apache.commons.lang3.builder.HashCodeBuilder;

import org.apache.seatunnel.engine.server.serializable.TaskDataSerializerHook;

import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import lombok.ToString;

import java.io.IOException;
import java.io.Serializable;

import static com.hazelcast.internal.serialization.impl.SerializationConstants.CONSTANT_TYPE_DATA_SERIALIZABLE;
import static com.hazelcast.internal.serialization.impl.SerializationConstants.CONSTANT_TYPE_NULL;

@ToString
public class TaskLocation implements IdentifiedDataSerializable, Serializable {

    private TaskGroupLocation taskGroupLocation;
    private long taskID;
    private int index;

    private static final long SUB_PIPELINE_ID_FACTORY = 10000L * 10000L * 10000L;
    private static final long GROUP_ID_FACTOR = 10000L * 10000L;
    private static final long TASK_GROUP_FACTOR = 10000L;

    public TaskLocation() {}

    /**
     * Reads an operation's location after checking its top-level Hazelcast wire identity. Absolute
     * header reads preserve the cursor and configured byte order. This does not validate nested
     * objects decoded by {@link #readData(ObjectDataInput)}.
     *
     * @param in the operation input positioned at the location field
     * @param operation the concrete operation name for decode diagnostics
     * @return the decoded location
     * @throws IOException if the input cannot prove the expected top-level identity
     */
    public static TaskLocation readFrom(ObjectDataInput in, String operation) throws IOException {
        if (!(in instanceof BufferObjectDataInput)) {
            throw new IOException(
                    operation
                            + " requires a buffered input to validate "
                            + TaskLocation.class.getName()
                            + "; actual input is "
                            + in.getClass().getName());
        }
        BufferObjectDataInput buffer = (BufferObjectDataInput) in;
        int position = buffer.position();
        int serializerType = buffer.readInt(position);
        if (serializerType != CONSTANT_TYPE_DATA_SERIALIZABLE) {
            throw new IOException(
                    operation
                            + " expects "
                            + TaskLocation.class.getName()
                            + " but wire serializer type is "
                            + (serializerType == CONSTANT_TYPE_NULL ? "null" : serializerType));
        }
        // Bit zero is Hazelcast DataSerializableSerializer's IDS_FLAG.
        int header = Byte.toUnsignedInt(buffer.readByte(position + Integer.BYTES));
        if ((header & 1) == 0) {
            throw new IOException(
                    operation
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
                    operation
                            + " expects "
                            + TaskLocation.class.getName()
                            + " but wire factory ID is "
                            + factoryId
                            + " and class ID is "
                            + classId);
        }
        return in.readObject(TaskLocation.class);
    }

    public TaskLocation(
            TaskGroupLocation taskGroupLocation, long taskInGroupIndex, int taskParallelismIndex) {
        this.taskGroupLocation = taskGroupLocation;
        this.taskID =
                taskGroupLocation.getPipelineId() * SUB_PIPELINE_ID_FACTORY
                        + taskGroupLocation.getTaskGroupId() * GROUP_ID_FACTOR
                        + taskInGroupIndex * TASK_GROUP_FACTOR
                        + taskParallelismIndex;
        this.index = taskParallelismIndex;
    }

    public TaskGroupLocation getTaskGroupLocation() {
        return taskGroupLocation;
    }

    public long getJobId() {
        return taskGroupLocation.getJobId();
    }

    public int getPipelineId() {
        return taskGroupLocation.getPipelineId();
    }

    public long getTaskID() {
        return taskID;
    }

    public long getTaskVertexId() {
        return taskID;
    }

    public int getTaskIndex() {
        return index;
    }

    public void setTaskGroupLocation(TaskGroupLocation taskGroupLocation) {
        this.taskGroupLocation = taskGroupLocation;
    }

    public void setTaskID(long taskID) {
        this.taskID = taskID;
    }

    @Override
    public int getFactoryId() {
        return TaskDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return TaskDataSerializerHook.TASK_LOCATION_TYPE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(taskGroupLocation);
        out.writeLong(taskID);
        out.writeInt(index);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        taskGroupLocation = in.readObject();
        taskID = in.readLong();
        index = in.readInt();
    }

    @Override
    public String toString() {
        return "TaskLocation{"
                + "taskGroupLocation="
                + taskGroupLocation
                + ", taskID="
                + taskID
                + ", index="
                + index
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TaskLocation that = (TaskLocation) o;
        return new EqualsBuilder()
                .append(taskID, that.taskID)
                .append(taskGroupLocation, that.taskGroupLocation)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(taskGroupLocation).append(taskID).toHashCode();
    }
}
