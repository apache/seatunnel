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

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

public class TaskInfo implements IdentifiedDataSerializable, Serializable {
    public static final Integer COORDINATOR_INDEX = -1;

    private long taskGroupId;

    private long jobId;

    private long pipelineId;

    private long vertexId;

    private int index;

    public TaskInfo() {
    }

    public TaskInfo(long taskGroupId, long jobId, long pipelineId, long vertexId, int index) {
        this.taskGroupId = taskGroupId;
        this.jobId = jobId;
        this.pipelineId = pipelineId;
        this.vertexId = vertexId;
        this.index = index;
    }

    public long getTaskGroupId() {
        return taskGroupId;
    }

    public long getJobId() {
        return jobId;
    }

    public long getPipelineId() {
        return pipelineId;
    }

    public long getVertexId() {
        return vertexId;
    }

    public int getIndex() {
        return index;
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
        out.writeLong(taskGroupId);
        out.writeLong(jobId);
        out.writeLong(pipelineId);
        out.writeLong(vertexId);
        out.writeInt(index);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        taskGroupId = in.readLong();
        jobId = in.readLong();
        pipelineId = in.readLong();
        vertexId = in.readLong();
        index = in.readInt();
    }

    public String toBasicLog() {
        return String.format("task info, job: %s, pipeline: %s, task: %s, index: %s", jobId, pipelineId, vertexId, index);
    }

    @Override
    public String toString() {
        return "TaskInfo{" +
            "taskGroupId=" + taskGroupId +
            ", jobId=" + jobId +
            ", pipelineId=" + pipelineId +
            ", vertexId=" + vertexId +
            ", index=" + index +
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
        TaskInfo that = (TaskInfo) o;
        return taskGroupId == that.taskGroupId
            && jobId == that.jobId
            && pipelineId == that.pipelineId
            && vertexId == that.vertexId
            && index == that.index;
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskGroupId, jobId, pipelineId, vertexId, index);
    }
}
