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

import org.apache.seatunnel.shade.com.google.common.base.Objects;

import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.task.record.Barrier;

import lombok.Getter;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

@Getter
public class CheckpointBarrier implements Barrier, Serializable {
    private final long id;
    private final long timestamp;
    private final CheckpointType checkpointType;
    private final Set<TaskLocation> prepareCloseTasks;
    private final Set<TaskLocation> closedTasks;

    public CheckpointBarrier(long id, long timestamp, CheckpointType checkpointType) {
        this(id, timestamp, checkpointType, Collections.emptySet(), Collections.emptySet());
    }

    public CheckpointBarrier(
            long id,
            long timestamp,
            CheckpointType checkpointType,
            Set<TaskLocation> prepareCloseTasks,
            Set<TaskLocation> closedTasks) {
        this.id = id;
        this.timestamp = timestamp;
        this.checkpointType = checkNotNull(checkpointType);
        this.prepareCloseTasks = prepareCloseTasks;
        this.closedTasks = closedTasks;
        if (new HashSet(prepareCloseTasks).removeAll(closedTasks)) {
            throw new IllegalArgumentException(
                    "The prepareCloseTasks collection should not contain elements of the closedTasks collection");
        }
    }

    @Override
    public boolean snapshot() {
        return true;
    }

    @Override
    public boolean prepareClose() {
        return checkpointType.isFinalCheckpoint();
    }

    @Override
    public boolean prepareClose(TaskLocation task) {
        if (prepareClose()) {
            return true;
        }
        return prepareCloseTasks.contains(task);
    }

    @Override
    public Set<TaskLocation> closedTasks() {
        return Collections.unmodifiableSet(closedTasks);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id, timestamp, checkpointType);
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        } else if (other == null || other.getClass() != CheckpointBarrier.class) {
            return false;
        } else {
            CheckpointBarrier that = (CheckpointBarrier) other;
            return that.id == this.id
                    && that.timestamp == this.timestamp
                    && this.checkpointType.equals(that.checkpointType);
        }
    }

    @Override
    public String toString() {
        return String.format(
                "CheckpointBarrier %d @ %d type: %s, prepareClose: %s, closed: %s",
                id, timestamp, checkpointType, prepareCloseTasks, closedTasks);
    }

    public boolean isAuto() {
        return checkpointType.isAuto();
    }
}
