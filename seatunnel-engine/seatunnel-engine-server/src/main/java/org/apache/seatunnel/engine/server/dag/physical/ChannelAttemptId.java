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

package org.apache.seatunnel.engine.server.dag.physical;

import java.io.Serializable;
import java.util.Objects;

/**
 * Attempt-scoped channel identity used by dynamic lookup transport fencing.
 *
 * <p>The job execution epoch is part of the identity so a full job restart can fence stale messages
 * even when task deployment attempt counters are reused.
 */
public final class ChannelAttemptId implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Global job execution epoch used to fence messages from an earlier job restart. */
    private final long jobExecutionEpoch;

    /** Stable logical channel identity shared across attempts. */
    private final LogicalChannelKey channelKey;

    /** Source task deployment attempt. */
    private final long sourceDeploymentAttempt;

    /** Target task deployment attempt. */
    private final long targetDeploymentAttempt;

    /** Reconnect epoch within the same pair of task deployment attempts. */
    private final long connectionEpoch;

    /**
     * Creates a fully fenced attempt identity for a logical channel.
     *
     * @param jobExecutionEpoch global job execution epoch
     * @param channelKey stable logical channel identity
     * @param sourceDeploymentAttempt source task deployment attempt
     * @param targetDeploymentAttempt target task deployment attempt
     * @param connectionEpoch reconnect epoch for the same attempt pair
     */
    public ChannelAttemptId(
            long jobExecutionEpoch,
            LogicalChannelKey channelKey,
            long sourceDeploymentAttempt,
            long targetDeploymentAttempt,
            long connectionEpoch) {
        if (jobExecutionEpoch < 0
                || sourceDeploymentAttempt < 0
                || targetDeploymentAttempt < 0
                || connectionEpoch < 0) {
            throw new IllegalArgumentException(
                    "Channel attempt epochs and deployment attempts must be non-negative");
        }
        this.jobExecutionEpoch = jobExecutionEpoch;
        this.channelKey = Objects.requireNonNull(channelKey, "channelKey");
        this.sourceDeploymentAttempt = sourceDeploymentAttempt;
        this.targetDeploymentAttempt = targetDeploymentAttempt;
        this.connectionEpoch = connectionEpoch;
    }

    public long getJobExecutionEpoch() {
        return jobExecutionEpoch;
    }

    public LogicalChannelKey getChannelKey() {
        return channelKey;
    }

    public long getSourceDeploymentAttempt() {
        return sourceDeploymentAttempt;
    }

    public long getTargetDeploymentAttempt() {
        return targetDeploymentAttempt;
    }

    public long getConnectionEpoch() {
        return connectionEpoch;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ChannelAttemptId)) {
            return false;
        }
        ChannelAttemptId that = (ChannelAttemptId) other;
        return jobExecutionEpoch == that.jobExecutionEpoch
                && sourceDeploymentAttempt == that.sourceDeploymentAttempt
                && targetDeploymentAttempt == that.targetDeploymentAttempt
                && connectionEpoch == that.connectionEpoch
                && channelKey.equals(that.channelKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                jobExecutionEpoch,
                channelKey,
                sourceDeploymentAttempt,
                targetDeploymentAttempt,
                connectionEpoch);
    }
}
