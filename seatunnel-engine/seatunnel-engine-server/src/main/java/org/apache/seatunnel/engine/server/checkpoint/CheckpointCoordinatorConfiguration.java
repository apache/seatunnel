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

import java.io.Serializable;
import java.util.Objects;

public class CheckpointCoordinatorConfiguration implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final long MINIMAL_CHECKPOINT_TIME = 10;

    private final long checkpointInterval;

    private final long checkpointTimeout;

    private final int maxConcurrentCheckpoints;

    private final int tolerableFailureCheckpoints;

    private CheckpointCoordinatorConfiguration(long checkpointInterval,
                                               long checkpointTimeout,
                                               int maxConcurrentCheckpoints,
                                               int tolerableFailureCheckpoints) {
        this.checkpointInterval = checkpointInterval;
        this.checkpointTimeout = checkpointTimeout;
        this.maxConcurrentCheckpoints = maxConcurrentCheckpoints;
        this.tolerableFailureCheckpoints = tolerableFailureCheckpoints;
    }

    public long getCheckpointInterval() {
        return checkpointInterval;
    }

    public long getCheckpointTimeout() {
        return checkpointTimeout;
    }

    public int getMaxConcurrentCheckpoints() {
        return maxConcurrentCheckpoints;
    }

    public int getTolerableFailureCheckpoints() {
        return tolerableFailureCheckpoints;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CheckpointCoordinatorConfiguration that = (CheckpointCoordinatorConfiguration) o;
        return checkpointInterval == that.checkpointInterval
            && checkpointTimeout == that.checkpointTimeout
            && maxConcurrentCheckpoints == that.maxConcurrentCheckpoints
            && tolerableFailureCheckpoints == that.tolerableFailureCheckpoints;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            checkpointInterval,
            checkpointTimeout,
            maxConcurrentCheckpoints,
            tolerableFailureCheckpoints);
    }

    @Override
    public String toString() {
        return "CheckpointCoordinatorConfiguration{" +
            "checkpointInterval=" + checkpointInterval +
            ", checkpointTimeout=" + checkpointTimeout +
            ", maxConcurrentCheckpoints=" + maxConcurrentCheckpoints +
            ", tolerableFailureCheckpoints=" + tolerableFailureCheckpoints +
            '}';
    }

    public static CheckpointCoordinatorConfiguration.Builder builder() {
        return new Builder();
    }

    @SuppressWarnings("MagicNumber")
    public static final class Builder {
        private long checkpointInterval = 300000;
        private long checkpointTimeout = 300000;
        private int maxConcurrentCheckpoints = 1;
        private int tolerableFailureCheckpoints = 0;

        private Builder() {
        }

        public Builder checkpointInterval(long checkpointInterval) {
            checkArgument(checkpointInterval < MINIMAL_CHECKPOINT_TIME, "The minimum checkpoint interval is 10 mills.");
            this.checkpointInterval = checkpointInterval;
            return this;
        }

        public Builder checkpointTimeout(long checkpointTimeout) {
            checkArgument(checkpointTimeout < MINIMAL_CHECKPOINT_TIME, "The minimum checkpoint timeout is 10 mills.");
            this.checkpointTimeout = checkpointTimeout;
            return this;
        }

        public Builder maxConcurrentCheckpoints(int maxConcurrentCheckpoints) {
            checkArgument(maxConcurrentCheckpoints < 1, "The minimum number of concurrent checkpoints is 1.");
            this.maxConcurrentCheckpoints = maxConcurrentCheckpoints;
            return this;
        }

        public Builder tolerableFailureCheckpoints(int tolerableFailureCheckpoints) {
            checkArgument(maxConcurrentCheckpoints < 0, "The number of tolerance failed checkpoints must be a natural number.");
            this.tolerableFailureCheckpoints = tolerableFailureCheckpoints;
            return this;
        }

        public CheckpointCoordinatorConfiguration build() {
            return new CheckpointCoordinatorConfiguration(
                checkpointInterval,
                checkpointTimeout,
                maxConcurrentCheckpoints,
                tolerableFailureCheckpoints);
        }
    }
}
