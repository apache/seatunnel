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

import com.hazelcast.cluster.Address;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PipelineCleanupRecord implements Serializable {
    private static final long serialVersionUID = 4176046941412667025L;

    private PipelineLocation pipelineLocation;
    private PipelineStatus finalStatus;
    private boolean savepointEnd;

    private Map<TaskGroupLocation, Address> taskGroups = new HashMap<>();
    private Set<TaskGroupLocation> cleanedTaskGroups = new HashSet<>();
    private boolean metricsImapCleaned;

    private long createTimeMillis;
    private long lastAttemptTimeMillis;
    private int attemptCount;

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
