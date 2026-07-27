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

package org.apache.seatunnel.engine.server.dag.execution;

import org.apache.seatunnel.engine.core.dag.logical.ExchangeDescriptor;
import org.apache.seatunnel.engine.core.dag.logical.PortAwareLogicalEdge;

import java.util.Arrays;
import java.util.Objects;

/** Execution edge that preserves the identity and target port of a port-aware logical edge. */
public final class PortAwareExecutionEdge extends ExecutionEdge {

    /** Stable logical edge identity copied without regeneration. */
    private final long edgeId;

    /** Explicit downstream input port. */
    private final int targetInputPort;

    /** Versioned routing declaration copied from the logical edge. */
    private final ExchangeDescriptor exchangeDescriptor;

    /**
     * Creates an execution edge while preserving all port-aware metadata.
     *
     * @param leftVertex upstream execution vertex
     * @param rightVertex downstream execution vertex
     * @param edgeId stable logical edge identity
     * @param targetInputPort downstream input port
     * @param exchangeDescriptor versioned routing declaration
     */
    public PortAwareExecutionEdge(
            ExecutionVertex leftVertex,
            ExecutionVertex rightVertex,
            long edgeId,
            int targetInputPort,
            ExchangeDescriptor exchangeDescriptor) {
        super(leftVertex, rightVertex);
        this.edgeId = edgeId;
        this.targetInputPort = targetInputPort;
        this.exchangeDescriptor = Objects.requireNonNull(exchangeDescriptor, "exchangeDescriptor");
    }

    /** Converts a logical port-aware edge without deriving or rewriting any edge metadata. */
    public static PortAwareExecutionEdge fromLogicalEdge(
            ExecutionVertex leftVertex,
            ExecutionVertex rightVertex,
            PortAwareLogicalEdge logicalEdge) {
        return new PortAwareExecutionEdge(
                leftVertex,
                rightVertex,
                logicalEdge.getEdgeId(),
                logicalEdge.getTargetInputPort(),
                logicalEdge.getExchangeDescriptor());
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
    public ExecutionEdge withVertices(
            ExecutionVertex replacementLeft, ExecutionVertex replacementRight) {
        return new PortAwareExecutionEdge(
                replacementLeft, replacementRight, edgeId, targetInputPort, exchangeDescriptor);
    }

    @Override
    protected boolean canEqual(Object other) {
        return other instanceof PortAwareExecutionEdge;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof PortAwareExecutionEdge)) {
            return false;
        }
        PortAwareExecutionEdge that = (PortAwareExecutionEdge) other;
        return that.canEqual(this)
                && edgeId == that.edgeId
                && targetInputPort == that.targetInputPort
                && Objects.equals(getLeftVertex(), that.getLeftVertex())
                && Objects.equals(getRightVertex(), that.getRightVertex())
                && Objects.equals(getLeftVertexId(), that.getLeftVertexId())
                && Objects.equals(getRightVertexId(), that.getRightVertexId())
                && Arrays.equals(
                        exchangeDescriptor.toCanonicalBytes(),
                        that.exchangeDescriptor.toCanonicalBytes());
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(edgeId);
        result = 31 * result + Objects.hashCode(getLeftVertexId());
        result = 31 * result + Objects.hashCode(getRightVertexId());
        result = 31 * result + Integer.hashCode(targetInputPort);
        result = 31 * result + Arrays.hashCode(exchangeDescriptor.toCanonicalBytes());
        return result;
    }

    @Override
    public String toString() {
        return "PortAwareExecutionEdge{"
                + "edgeId="
                + edgeId
                + ", leftVertexId="
                + getLeftVertexId()
                + ", rightVertexId="
                + getRightVertexId()
                + ", targetInputPort="
                + targetInputPort
                + ", exchangeDescriptor="
                + exchangeDescriptor
                + '}';
    }
}
