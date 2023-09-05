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

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;
import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

public class TaskStatistics implements Serializable {
    /** ID of the task the statistics belong to. */
    private final Long jobVertexId;

    private final List<SubtaskStatistics> subtaskStats;

    /** Marks whether a subtask is complete; */
    private final boolean[] subtaskCompleted;

    private int numAcknowledgedSubtasks = 0;

    private SubtaskStatistics latestAckedSubtaskStatistics;

    TaskStatistics(Long jobVertexId, int parallelism) {
        this.jobVertexId = checkNotNull(jobVertexId, "JobVertexID");
        checkArgument(parallelism > 0, "the parallelism of task <= 0");
        this.subtaskStats = Arrays.asList(new SubtaskStatistics[parallelism]);
        this.subtaskCompleted = new boolean[parallelism];
    }

    boolean reportSubtaskStatistics(SubtaskStatistics subtask) {
        checkNotNull(subtask, "Subtask stats");
        int subtaskIndex = subtask.getSubtaskIndex();

        if (subtaskIndex < 0 || subtaskIndex >= subtaskStats.size()) {
            return false;
        }

        if (subtaskStats.get(subtaskIndex) == null) {
            subtaskStats.set(subtaskIndex, subtask);
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
        return latestAckedSubtaskStatistics != null
                ? latestAckedSubtaskStatistics.getAckTimestamp()
                : -1;
    }

    public Long getJobVertexId() {
        return jobVertexId;
    }

    public List<SubtaskStatistics> getSubtaskStats() {
        return subtaskStats;
    }

    public void completed(int subtaskIndex) {
        subtaskCompleted[subtaskIndex] = true;
    }

    public boolean isCompleted() {
        for (boolean completed : subtaskCompleted) {
            if (!completed) {
                return false;
            }
        }
        return true;
    }
}
