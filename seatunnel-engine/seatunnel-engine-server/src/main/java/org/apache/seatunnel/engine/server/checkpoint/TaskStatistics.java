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

package org.apache.seatunnel.engine.server.checkpoint;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class TaskStatistics {
    /** ID of the task the statistics belong to. */
    private final Long jobVertexId;

    private final SubtaskStatistics[] subtaskStats;

    private int numAcknowledgedSubtasks;

    private SubtaskStatistics latestAckedSubtaskStatistics;

    TaskStatistics(Long jobVertexId, int parallelism) {
        this.jobVertexId = checkNotNull(jobVertexId, "JobVertexID");
        checkArgument(parallelism > 0, "the parallelism of task <= 0");
        this.subtaskStats = new SubtaskStatistics[parallelism];
    }

    boolean reportSubtaskStats(SubtaskStatistics subtask) {
        checkNotNull(subtask, "Subtask stats");
        int subtaskIndex = subtask.getSubtaskIndex();

        if (subtaskIndex < 0 || subtaskIndex >= subtaskStats.length) {
            return false;
        }

        if (subtaskStats[subtaskIndex] == null) {
            subtaskStats[subtaskIndex] = subtask;
            numAcknowledgedSubtasks++;
            return true;
        } else {
            return false;
        }
    }

    /**
     * @return The latest acknowledged subtask stats or <code>null</code> if none was acknowledged
     *     yet.
     */
    public SubtaskStatistics getLatestAcknowledgedSubtaskStatistics() {
        return latestAckedSubtaskStatistics;
    }

    /**
     * @return Ack timestamp of the latest acknowledged subtask or <code>-1</code> if none was
     *     acknowledged yet..
     */
    public long getLatestAckTimestamp() {
        return latestAckedSubtaskStatistics != null ?
            latestAckedSubtaskStatistics.getAckTimestamp() :
            -1;
    }

    public Long getJobVertexId() {
        return jobVertexId;
    }

    public SubtaskStatistics[] getSubtaskStats() {
        return subtaskStats;
    }
}
