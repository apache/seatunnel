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

package org.apache.seatunnel.engine.server.master.cleanup;

import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.serializable.ResourceDataSerializerHook;

import com.hazelcast.cluster.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PipelineCleanupRecord implements IdentifiedDataSerializable {

    private PipelineLocation pipelineLocation;
    private PipelineStatus finalStatus;
    private boolean savepointEnd;

    private Map<TaskGroupLocation, Address> taskGroups = new HashMap<>();
    private Set<TaskGroupLocation> cleanedTaskGroups = new HashSet<>();
    private boolean metricsImapCleaned;

    private long createTimeMillis;
    private long lastAttemptTimeMillis;
    private int attemptCount;

    @Override
    public int getFactoryId() {
        return ResourceDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return ResourceDataSerializerHook.PIPELINE_CLEANUP_RECORD_TYPE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(pipelineLocation);
        out.writeString(finalStatus == null ? null : finalStatus.name());
        out.writeBoolean(savepointEnd);

        if (taskGroups == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(taskGroups.size());
            for (Map.Entry<TaskGroupLocation, Address> entry : taskGroups.entrySet()) {
                out.writeObject(entry.getKey());
                out.writeObject(entry.getValue());
            }
        }

        if (cleanedTaskGroups == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(cleanedTaskGroups.size());
            for (TaskGroupLocation taskGroupLocation : cleanedTaskGroups) {
                out.writeObject(taskGroupLocation);
            }
        }

        out.writeBoolean(metricsImapCleaned);
        out.writeLong(createTimeMillis);
        out.writeLong(lastAttemptTimeMillis);
        out.writeInt(attemptCount);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        pipelineLocation = in.readObject();
        String statusName = in.readString();
        finalStatus = statusName == null ? null : PipelineStatus.valueOf(statusName);
        savepointEnd = in.readBoolean();

        int taskGroupsSize = in.readInt();
        if (taskGroupsSize >= 0) {
            taskGroups = new HashMap<>(taskGroupsSize);
            for (int i = 0; i < taskGroupsSize; i++) {
                TaskGroupLocation taskGroupLocation = in.readObject();
                Address address = in.readObject();
                taskGroups.put(taskGroupLocation, address);
            }
        } else {
            taskGroups = null;
        }

        int cleanedTaskGroupsSize = in.readInt();
        if (cleanedTaskGroupsSize >= 0) {
            cleanedTaskGroups = new HashSet<>(cleanedTaskGroupsSize);
            for (int i = 0; i < cleanedTaskGroupsSize; i++) {
                cleanedTaskGroups.add(in.readObject());
            }
        } else {
            cleanedTaskGroups = null;
        }

        metricsImapCleaned = in.readBoolean();
        createTimeMillis = in.readLong();
        lastAttemptTimeMillis = in.readLong();
        attemptCount = in.readInt();
    }

    public boolean isCleaned() {
        return metricsImapCleaned
                && taskGroups != null
                && cleanedTaskGroups != null
                && cleanedTaskGroups.containsAll(taskGroups.keySet());
    }

    public PipelineCleanupRecord mergeFrom(PipelineCleanupRecord other) {
        if (other == null) {
            return this;
        }
        Map<TaskGroupLocation, Address> mergedTaskGroups = new HashMap<>();
        if (this.taskGroups != null) {
            mergedTaskGroups.putAll(this.taskGroups);
        }
        if (other.taskGroups != null) {
            mergedTaskGroups.putAll(other.taskGroups);
        }

        Set<TaskGroupLocation> mergedCleaned = new HashSet<>();
        if (this.cleanedTaskGroups != null) {
            mergedCleaned.addAll(this.cleanedTaskGroups);
        }
        if (other.cleanedTaskGroups != null) {
            mergedCleaned.addAll(other.cleanedTaskGroups);
        }

        PipelineCleanupRecord merged =
                new PipelineCleanupRecord(
                        this.pipelineLocation != null
                                ? this.pipelineLocation
                                : other.pipelineLocation,
                        this.finalStatus != null ? this.finalStatus : other.finalStatus,
                        this.savepointEnd || other.savepointEnd,
                        mergedTaskGroups,
                        mergedCleaned,
                        this.metricsImapCleaned || other.metricsImapCleaned,
                        this.createTimeMillis != 0 ? this.createTimeMillis : other.createTimeMillis,
                        Math.max(this.lastAttemptTimeMillis, other.lastAttemptTimeMillis),
                        Math.max(this.attemptCount, other.attemptCount));
        return merged;
    }
}
