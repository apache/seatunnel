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

package org.apache.seatunnel.engine.core.dag.actions;

import org.apache.seatunnel.engine.core.dag.logical.ExchangeDescriptor;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/** Explicit binding between one upstream action and one target input port. */
public final class InputPortBinding implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Domain separator that prevents the edge digest from being reused as another identity type.
     */
    private static final byte[] EDGE_ID_DOMAIN =
            "seatunnel-port-edge-v1".getBytes(StandardCharsets.UTF_8);

    /** Logical upstream action ID captured before execution-plan IDs are regenerated. */
    private final long upstreamActionId;

    /** Stable edge identity carried unchanged through every planning layer. */
    private final long edgeId;

    /** Target input port assigned by the planner, never inferred from list order. */
    private final int targetInputPort;

    /** Versioned routing declaration retained for the later exchange implementation. */
    private final ExchangeDescriptor exchangeDescriptor;

    /**
     * Creates an immutable planner binding.
     *
     * @param upstreamActionId stable logical upstream action ID
     * @param edgeId stable logical edge identity
     * @param targetInputPort explicit downstream port
     * @param exchangeDescriptor versioned routing declaration
     */
    public InputPortBinding(
            long upstreamActionId,
            long edgeId,
            int targetInputPort,
            ExchangeDescriptor exchangeDescriptor) {
        if (targetInputPort < 0) {
            throw new IllegalArgumentException(
                    "targetInputPort must be non-negative: " + targetInputPort);
        }
        this.upstreamActionId = upstreamActionId;
        this.edgeId = edgeId;
        this.targetInputPort = targetInputPort;
        this.exchangeDescriptor = Objects.requireNonNull(exchangeDescriptor, "exchangeDescriptor");
    }

    /**
     * Creates a binding with a deterministic edge ID derived from both endpoints and the target
     * port.
     */
    public static InputPortBinding forward(
            long upstreamActionId, long targetActionId, int targetInputPort) {
        return new InputPortBinding(
                upstreamActionId,
                stableEdgeId(upstreamActionId, targetActionId, targetInputPort),
                targetInputPort,
                ExchangeDescriptor.forward());
    }

    public long getUpstreamActionId() {
        return upstreamActionId;
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

    private static long stableEdgeId(
            long upstreamActionId, long targetActionId, int targetInputPort) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(EDGE_ID_DOMAIN);
            digest.update(
                    ByteBuffer.allocate(Long.BYTES * 2 + Integer.BYTES)
                            .putLong(upstreamActionId)
                            .putLong(targetActionId)
                            .putInt(targetInputPort)
                            .array());
            return ByteBuffer.wrap(digest.digest()).getLong();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by the Java runtime", e);
        }
    }
}
