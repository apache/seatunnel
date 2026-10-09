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

package org.apache.seatunnel.engine.server.task.source;

import org.apache.seatunnel.engine.server.execution.TaskLocation;

import com.hazelcast.cluster.Address;

/** Attempt-aware Reader registration admitted to a managed Source coordinator event loop. */
public final class ManagedSourceRegistration {
    private final TaskLocation readerLocation;
    private final Address readerAddress;
    /** Monotonic worker deployment identity that fences delayed Reader attempts. */
    private final long readerExecutionId;

    private final String readerAttemptId;
    private final int runtimeProtocolVersion;
    private final String capabilityDigest;
    private final long firstReaderCommandSequence;
    private final long restoredAppliedWatermark;
    /** Last no-more-splits generation included in the Reader's completed checkpoint. */
    private final long restoredNoMoreSplitsGeneration;

    public ManagedSourceRegistration(
            TaskLocation readerLocation,
            Address readerAddress,
            long readerExecutionId,
            String readerAttemptId,
            int runtimeProtocolVersion,
            String capabilityDigest,
            long firstReaderCommandSequence,
            long restoredAppliedWatermark,
            long restoredNoMoreSplitsGeneration) {
        this.readerLocation = readerLocation;
        this.readerAddress = readerAddress;
        this.readerExecutionId = readerExecutionId;
        this.readerAttemptId = readerAttemptId;
        this.runtimeProtocolVersion = runtimeProtocolVersion;
        this.capabilityDigest = capabilityDigest;
        this.firstReaderCommandSequence = firstReaderCommandSequence;
        this.restoredAppliedWatermark = restoredAppliedWatermark;
        this.restoredNoMoreSplitsGeneration = restoredNoMoreSplitsGeneration;
    }

    public TaskLocation getReaderLocation() {
        return readerLocation;
    }

    public Address getReaderAddress() {
        return readerAddress;
    }

    /** Returns the immutable deployment identity of the registering Reader. */
    public long getReaderExecutionId() {
        return readerExecutionId;
    }

    public String getReaderAttemptId() {
        return readerAttemptId;
    }

    public int getRuntimeProtocolVersion() {
        return runtimeProtocolVersion;
    }

    public String getCapabilityDigest() {
        return capabilityDigest;
    }

    public long getFirstReaderCommandSequence() {
        return firstReaderCommandSequence;
    }

    public long getRestoredAppliedWatermark() {
        return restoredAppliedWatermark;
    }

    /** Returns the durable no-more-splits generation restored by this Reader. */
    public long getRestoredNoMoreSplitsGeneration() {
        return restoredNoMoreSplitsGeneration;
    }
}
