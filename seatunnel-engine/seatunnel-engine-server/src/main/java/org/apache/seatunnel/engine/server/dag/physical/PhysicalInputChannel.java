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

import org.apache.seatunnel.engine.core.dag.logical.ExchangeDescriptor;
import org.apache.seatunnel.engine.server.execution.TaskLocation;

import java.io.Serializable;
import java.util.Objects;

/** Physical endpoint declaration for one logical input channel. */
public final class PhysicalInputChannel implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Stable channel identity independent of deployments and reconnects. */
    private final LogicalChannelKey logicalChannelKey;

    /** Current physical source endpoint assigned by planning. */
    private final TaskLocation sourceTaskLocation;

    /** Current physical target endpoint assigned by planning. */
    private final TaskLocation targetTaskLocation;

    /** Versioned routing declaration for the channel. */
    private final ExchangeDescriptor exchangeDescriptor;

    /**
     * Creates an immutable physical channel declaration.
     *
     * @param logicalChannelKey stable channel identity
     * @param sourceTaskLocation current upstream task location
     * @param targetTaskLocation current downstream task location
     * @param exchangeDescriptor versioned routing declaration
     */
    public PhysicalInputChannel(
            LogicalChannelKey logicalChannelKey,
            TaskLocation sourceTaskLocation,
            TaskLocation targetTaskLocation,
            ExchangeDescriptor exchangeDescriptor) {
        this.logicalChannelKey = Objects.requireNonNull(logicalChannelKey, "logicalChannelKey");
        this.sourceTaskLocation = Objects.requireNonNull(sourceTaskLocation, "sourceTaskLocation");
        this.targetTaskLocation = Objects.requireNonNull(targetTaskLocation, "targetTaskLocation");
        this.exchangeDescriptor = Objects.requireNonNull(exchangeDescriptor, "exchangeDescriptor");
    }

    public LogicalChannelKey getLogicalChannelKey() {
        return logicalChannelKey;
    }

    public TaskLocation getSourceTaskLocation() {
        return sourceTaskLocation;
    }

    public TaskLocation getTargetTaskLocation() {
        return targetTaskLocation;
    }

    public ExchangeDescriptor getExchangeDescriptor() {
        return exchangeDescriptor;
    }

    /** Binds this stable logical channel to one concrete pair of deployment attempts. */
    public ChannelAttemptId bindAttempts(
            long jobExecutionEpoch,
            long sourceDeploymentAttempt,
            long targetDeploymentAttempt,
            long connectionEpoch) {
        return new ChannelAttemptId(
                jobExecutionEpoch,
                logicalChannelKey,
                sourceDeploymentAttempt,
                targetDeploymentAttempt,
                connectionEpoch);
    }
}
