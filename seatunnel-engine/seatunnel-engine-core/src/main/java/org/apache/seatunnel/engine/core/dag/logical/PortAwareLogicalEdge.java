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

package org.apache.seatunnel.engine.core.dag.logical;

import org.apache.seatunnel.engine.core.serializable.JobDataSerializerHook;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Logical edge with a stable identity and explicit target input port.
 *
 * <p>This class has a dedicated Hazelcast class ID. The legacy {@link LogicalEdge} class ID and its
 * two-long wire layout remain unchanged.
 */
public final class PortAwareLogicalEdge extends LogicalEdge {

    /** Tagged payload version for the dedicated port-aware Hazelcast class ID. */
    public static final int CURRENT_FORMAT_VERSION = 1;

    /** Version of the tagged edge payload, separate from the legacy edge format. */
    private int edgeFormatVersion;

    /** Stable logical identity propagated through all planning layers. */
    private long edgeId;

    /** Target input port declared by the planner. */
    private int targetInputPort;

    /** Versioned routing declaration for this edge. */
    private ExchangeDescriptor exchangeDescriptor;

    /** Creates an empty instance for Hazelcast identified-data deserialization. */
    public PortAwareLogicalEdge() {}

    /**
     * Creates a versioned port-aware logical edge.
     *
     * @param edgeId stable logical edge identity
     * @param inputVertexId upstream logical vertex
     * @param targetVertexId downstream logical vertex
     * @param targetInputPort downstream input port
     * @param exchangeDescriptor versioned routing declaration
     */
    public PortAwareLogicalEdge(
            long edgeId,
            long inputVertexId,
            long targetVertexId,
            int targetInputPort,
            ExchangeDescriptor exchangeDescriptor) {
        super(inputVertexId, targetVertexId);
        if (targetInputPort < 0) {
            throw new IllegalArgumentException(
                    "targetInputPort must be non-negative: " + targetInputPort);
        }
        this.edgeFormatVersion = CURRENT_FORMAT_VERSION;
        this.edgeId = edgeId;
        this.targetInputPort = targetInputPort;
        this.exchangeDescriptor = Objects.requireNonNull(exchangeDescriptor, "exchangeDescriptor");
    }

    public int getEdgeFormatVersion() {
        return edgeFormatVersion;
    }

    public long getEdgeId() {
        return edgeId;
    }

    public int getTargetInputPort() {
        return targetInputPort;
    }

    public ExchangeDescriptor getExchangeDescriptor() {
        return exchangeDescriptor;
    }

    @Override
    public int getClassId() {
        return JobDataSerializerHook.PORT_AWARE_LOGICAL_EDGE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(edgeFormatVersion);
        out.writeLong(getInputVertexId());
        out.writeLong(getTargetVertexId());
        out.writeLong(edgeId);
        out.writeInt(targetInputPort);
        exchangeDescriptor.writeTo(out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        edgeFormatVersion = in.readInt();
        if (edgeFormatVersion != CURRENT_FORMAT_VERSION) {
            throw new IOException(
                    "Unsupported port-aware logical edge version: " + edgeFormatVersion);
        }
        setInputVertexId(in.readLong());
        setTargetVertexId(in.readLong());
        edgeId = in.readLong();
        targetInputPort = in.readInt();
        if (targetInputPort < 0) {
            throw new IOException("Invalid target input port: " + targetInputPort);
        }
        exchangeDescriptor = ExchangeDescriptor.readFrom(in);
    }

    @Override
    protected boolean canEqual(Object other) {
        return other instanceof PortAwareLogicalEdge;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof PortAwareLogicalEdge)) {
            return false;
        }
        PortAwareLogicalEdge that = (PortAwareLogicalEdge) other;
        return that.canEqual(this)
                && edgeId == that.edgeId
                && targetInputPort == that.targetInputPort
                && Objects.equals(getInputVertexId(), that.getInputVertexId())
                && Objects.equals(getTargetVertexId(), that.getTargetVertexId())
                && Arrays.equals(
                        exchangeDescriptor.toCanonicalBytes(),
                        that.exchangeDescriptor.toCanonicalBytes());
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(edgeId);
        result = 31 * result + Objects.hashCode(getInputVertexId());
        result = 31 * result + Objects.hashCode(getTargetVertexId());
        result = 31 * result + Integer.hashCode(targetInputPort);
        result = 31 * result + Arrays.hashCode(exchangeDescriptor.toCanonicalBytes());
        return result;
    }

    @Override
    public String toString() {
        return "PortAwareLogicalEdge{"
                + "edgeId="
                + edgeId
                + ", inputVertexId="
                + getInputVertexId()
                + ", targetVertexId="
                + getTargetVertexId()
                + ", targetInputPort="
                + targetInputPort
                + ", exchangeDescriptor="
                + exchangeDescriptor
                + '}';
    }
}
