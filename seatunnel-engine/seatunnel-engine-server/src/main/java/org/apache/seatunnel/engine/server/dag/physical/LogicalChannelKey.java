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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 * Stable identity of one logical upstream-to-downstream channel.
 *
 * <p>Deployment attempts are deliberately excluded so the identity remains stable across
 * redeployments and can be referenced by plans and checkpoints.
 */
public final class LogicalChannelKey implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Stable job identity shared by every deployment attempt of the channel. */
    private final String jobId;

    /** Stable lookup operator identity used by checkpoint and coordinator state. */
    private final String operatorUid;

    /** Planner-provided source identity; runtime action IDs are deliberately excluded. */
    private final String sourceActionUid;

    /** Stable logical edge identity. */
    private final long edgeId;

    /** Target fact or dimension input port. */
    private final int targetInputPort;

    /** Logical upstream subtask index. */
    private final int upstreamSubtask;

    /** Logical downstream subtask index. */
    private final int downstreamSubtask;

    /**
     * Creates the deployment-independent identity of a physical input channel.
     *
     * @param jobId stable job identity
     * @param operatorUid stable lookup operator identity
     * @param sourceActionUid stable source identity
     * @param edgeId stable logical edge identity
     * @param targetInputPort downstream input port
     * @param upstreamSubtask upstream subtask index
     * @param downstreamSubtask downstream subtask index
     */
    public LogicalChannelKey(
            String jobId,
            String operatorUid,
            String sourceActionUid,
            long edgeId,
            int targetInputPort,
            int upstreamSubtask,
            int downstreamSubtask) {
        if (jobId == null || jobId.trim().isEmpty()) {
            throw new IllegalArgumentException("jobId must not be blank");
        }
        if (operatorUid == null || operatorUid.trim().isEmpty()) {
            throw new IllegalArgumentException("operatorUid must not be blank");
        }
        if (sourceActionUid == null || sourceActionUid.trim().isEmpty()) {
            throw new IllegalArgumentException("sourceActionUid must not be blank");
        }
        if (targetInputPort < 0 || upstreamSubtask < 0 || downstreamSubtask < 0) {
            throw new IllegalArgumentException(
                    "Port and subtask indexes must be non-negative: port="
                            + targetInputPort
                            + ", upstream="
                            + upstreamSubtask
                            + ", downstream="
                            + downstreamSubtask);
        }
        this.jobId = jobId;
        this.operatorUid = operatorUid;
        this.sourceActionUid = sourceActionUid;
        this.edgeId = edgeId;
        this.targetInputPort = targetInputPort;
        this.upstreamSubtask = upstreamSubtask;
        this.downstreamSubtask = downstreamSubtask;
    }

    public String getJobId() {
        return jobId;
    }

    public String getOperatorUid() {
        return operatorUid;
    }

    public String getSourceActionUid() {
        return sourceActionUid;
    }

    public long getEdgeId() {
        return edgeId;
    }

    public int getTargetInputPort() {
        return targetInputPort;
    }

    public int getUpstreamSubtask() {
        return upstreamSubtask;
    }

    public int getDownstreamSubtask() {
        return downstreamSubtask;
    }

    /** Returns canonical bytes for stable hashing and envelope validation. */
    public byte[] toCanonicalBytes() {
        byte[] jobBytes = jobId.getBytes(StandardCharsets.UTF_8);
        byte[] operatorBytes = operatorUid.getBytes(StandardCharsets.UTF_8);
        byte[] sourceBytes = sourceActionUid.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocate(
                        Integer.BYTES
                                + jobBytes.length
                                + Integer.BYTES
                                + operatorBytes.length
                                + Integer.BYTES
                                + sourceBytes.length
                                + Long.BYTES
                                + Integer.BYTES * 3)
                .putInt(jobBytes.length)
                .put(jobBytes)
                .putInt(operatorBytes.length)
                .put(operatorBytes)
                .putInt(sourceBytes.length)
                .put(sourceBytes)
                .putLong(edgeId)
                .putInt(targetInputPort)
                .putInt(upstreamSubtask)
                .putInt(downstreamSubtask)
                .array();
    }

    /** Returns SHA-256 over the canonical channel identity. */
    public byte[] canonicalDigest() {
        try {
            return MessageDigest.getInstance("SHA-256").digest(toCanonicalBytes());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by the Java runtime", e);
        }
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof LogicalChannelKey)) {
            return false;
        }
        LogicalChannelKey that = (LogicalChannelKey) other;
        return jobId.equals(that.jobId)
                && operatorUid.equals(that.operatorUid)
                && sourceActionUid.equals(that.sourceActionUid)
                && edgeId == that.edgeId
                && targetInputPort == that.targetInputPort
                && upstreamSubtask == that.upstreamSubtask
                && downstreamSubtask == that.downstreamSubtask;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(toCanonicalBytes());
    }

    @Override
    public String toString() {
        return "LogicalChannelKey{"
                + "jobId='"
                + jobId
                + '\''
                + ", operatorUid='"
                + operatorUid
                + '\''
                + ", sourceActionUid='"
                + sourceActionUid
                + '\''
                + ", edgeId="
                + edgeId
                + ", targetInputPort="
                + targetInputPort
                + ", upstreamSubtask="
                + upstreamSubtask
                + ", downstreamSubtask="
                + downstreamSubtask
                + '}';
    }
}
