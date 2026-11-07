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

import lombok.Getter;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/** Durable checkpoint intent stored inside the completed checkpoint metadata. */
@Getter
public final class CheckpointIntent implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String PURPOSE_NORMAL = "NORMAL";
    public static final String PURPOSE_DYNAMIC_LOOKUP_FACT_POSITION_ANCHOR =
            "DYNAMIC_LOOKUP_FACT_POSITION_ANCHOR";
    public static final String PHASE_NONE = "NONE";
    public static final String PHASE_FACT_POSITIONS_DURABLE = "FACT_POSITIONS_DURABLE";

    private final int version;
    private final String checkpointPurpose;
    private final String checkpointIntentId;
    private final String targetDurablePhase;
    private final byte[] anchoredPositionDigest;

    public CheckpointIntent(
            int version,
            String checkpointPurpose,
            String checkpointIntentId,
            String targetDurablePhase,
            byte[] anchoredPositionDigest) {
        this.version = version;
        this.checkpointPurpose = Objects.requireNonNull(checkpointPurpose, "checkpointPurpose");
        this.checkpointIntentId = Objects.requireNonNull(checkpointIntentId, "checkpointIntentId");
        this.targetDurablePhase = Objects.requireNonNull(targetDurablePhase, "targetDurablePhase");
        this.anchoredPositionDigest =
                anchoredPositionDigest == null
                        ? null
                        : Arrays.copyOf(anchoredPositionDigest, anchoredPositionDigest.length);
    }

    public static CheckpointIntent normal(long jobId, int pipelineId, long checkpointId) {
        return new CheckpointIntent(
                1, PURPOSE_NORMAL, intentId(jobId, pipelineId, checkpointId), PHASE_NONE, null);
    }

    public static CheckpointIntent dynamicLookupFactPositionAnchor(
            long jobId, int pipelineId, long checkpointId, byte[] anchoredPositionDigest) {
        return new CheckpointIntent(
                1,
                PURPOSE_DYNAMIC_LOOKUP_FACT_POSITION_ANCHOR,
                intentId(jobId, pipelineId, checkpointId),
                PHASE_FACT_POSITIONS_DURABLE,
                anchoredPositionDigest);
    }

    public byte[] getAnchoredPositionDigest() {
        return anchoredPositionDigest == null
                ? null
                : Arrays.copyOf(anchoredPositionDigest, anchoredPositionDigest.length);
    }

    /** Returns whether this checkpoint keeps the legacy checkpoint persistence format. */
    public boolean isNormalCheckpoint() {
        return PURPOSE_NORMAL.equals(checkpointPurpose);
    }

    private static String intentId(long jobId, int pipelineId, long checkpointId) {
        return jobId + "/" + pipelineId + "/" + checkpointId;
    }
}
