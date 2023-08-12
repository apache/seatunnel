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

import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.server.task.record.Barrier;

import com.google.common.base.Objects;

import java.io.Serializable;

import static com.google.common.base.Preconditions.checkNotNull;

public class CheckpointBarrier implements Barrier, Serializable {
    private final long id;
    private final long timestamp;
    private final CheckpointType checkpointType;

    public CheckpointBarrier(long id, long timestamp, CheckpointType checkpointType) {
        this.id = id;
        this.timestamp = timestamp;
        this.checkpointType = checkNotNull(checkpointType);
    }

    public long getId() {
        return id;
    }

    @Override
    public boolean snapshot() {
        return true;
    }

    @Override
    public boolean prepareClose() {
        return checkpointType.isFinalCheckpoint();
    }

    public long getTimestamp() {
        return timestamp;
    }

    public CheckpointType getCheckpointType() {
        return checkpointType;
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
                "CheckpointBarrier %d @ %d Options: %s", id, timestamp, checkpointType);
    }

    public boolean isAuto() {
        return checkpointType.isAuto();
    }
}
