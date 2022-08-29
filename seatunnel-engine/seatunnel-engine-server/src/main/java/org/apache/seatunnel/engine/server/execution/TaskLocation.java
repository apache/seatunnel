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

import org.apache.seatunnel.engine.server.serializable.TaskDataSerializerHook;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.IOException;
import java.io.Serializable;

public class TaskLocation implements IdentifiedDataSerializable, Serializable {

    private TaskGroupInfo taskGroupInfo;
    private long taskID;

    public TaskLocation() {
    }

    public TaskLocation(TaskGroupInfo taskGroupInfo, long taskID) {
        this.taskGroupInfo = taskGroupInfo;
        this.taskID = taskID;
    }

    public TaskGroupInfo getTaskGroupInfo() {
        return taskGroupInfo;
    }

    public void setTaskGroupInfo(TaskGroupInfo taskGroupInfo) {
        this.taskGroupInfo = taskGroupInfo;
    }

    public long getTaskID() {
        return taskID;
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
        out.writeObject(taskGroupInfo);
        out.writeLong(taskID);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        taskGroupInfo = in.readObject();
        taskID = in.readLong();
    }

    @Override
    public String toString() {
        return "TaskLocation{" +
            "taskGroupInfo=" + taskGroupInfo +
            ", taskID=" + taskID +
            '}';
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
        return new EqualsBuilder().append(taskID, that.taskID).append(taskGroupInfo, that.taskGroupInfo).isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(taskGroupInfo).append(taskID).toHashCode();
    }
}
