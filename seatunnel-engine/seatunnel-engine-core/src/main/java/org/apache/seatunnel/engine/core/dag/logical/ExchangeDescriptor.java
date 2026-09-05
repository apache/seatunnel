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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/**
 * Versioned exchange envelope attached to a {@link PortAwareLogicalEdge}.
 *
 * <p>The current runtime supports only {@link DistributionType#FORWARD}. Later exchange work can
 * add a new descriptor version without changing the legacy {@link LogicalEdge} wire layout or
 * guessing a version from trailing bytes.
 */
public final class ExchangeDescriptor implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Defensive upper bound for a future versioned canonical descriptor payload. */
    private static final int MAX_CANONICAL_BYTES = 1024;

    /** Canonical descriptor version implemented by the current dynamic lookup runtime. */
    public static final int CURRENT_VERSION = 1;

    /** Canonical descriptor format version written independently of Java serialization. */
    private final int protocolVersion;

    /** Stable routing mode encoded through an explicit wire code. */
    private final DistributionType distributionType;

    /**
     * Creates a validated exchange descriptor for a known protocol version.
     *
     * @param protocolVersion canonical descriptor version
     * @param distributionType explicit routing mode
     */
    public ExchangeDescriptor(int protocolVersion, DistributionType distributionType) {
        if (protocolVersion != CURRENT_VERSION) {
            throw new IllegalArgumentException(
                    "Unsupported exchange descriptor version: " + protocolVersion);
        }
        this.protocolVersion = protocolVersion;
        this.distributionType = Objects.requireNonNull(distributionType, "distributionType");
    }

    /** Returns the only routing declaration supported by the current dynamic lookup runtime. */
    public static ExchangeDescriptor forward() {
        return new ExchangeDescriptor(CURRENT_VERSION, DistributionType.FORWARD);
    }

    public int getProtocolVersion() {
        return protocolVersion;
    }

    public DistributionType getDistributionType() {
        return distributionType;
    }

    /**
     * Returns the canonical, JVM-independent bytes used by edge equality, hashing, and wire
     * serialization.
     */
    public byte[] toCanonicalBytes() {
        return ByteBuffer.allocate(Integer.BYTES * 2)
                .putInt(protocolVersion)
                .putInt(distributionType.getWireCode())
                .array();
    }

    /** Writes a length-framed canonical descriptor. */
    public void writeTo(ObjectDataOutput out) throws IOException {
        byte[] canonicalBytes = toCanonicalBytes();
        out.writeInt(canonicalBytes.length);
        out.write(canonicalBytes);
    }

    /** Reads and validates a length-framed canonical descriptor. */
    public static ExchangeDescriptor readFrom(ObjectDataInput in) throws IOException {
        int payloadLength = in.readInt();
        if (payloadLength <= 0 || payloadLength > MAX_CANONICAL_BYTES) {
            throw new IOException("Invalid exchange descriptor payload length: " + payloadLength);
        }
        byte[] payload = new byte[payloadLength];
        in.readFully(payload);
        return fromCanonicalBytes(payload);
    }

    /** Decodes a canonical descriptor and rejects unknown or malformed versions. */
    public static ExchangeDescriptor fromCanonicalBytes(byte[] payload) throws IOException {
        if (payload == null || payload.length != Integer.BYTES * 2) {
            throw new IOException(
                    "Invalid exchange descriptor canonical length: "
                            + (payload == null ? -1 : payload.length));
        }
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        int version = buffer.getInt();
        int distributionWireCode = buffer.getInt();
        if (version != CURRENT_VERSION) {
            throw new IOException("Unsupported exchange descriptor version: " + version);
        }
        try {
            return new ExchangeDescriptor(
                    version, DistributionType.fromWireCode(distributionWireCode));
        } catch (IllegalArgumentException e) {
            throw new IOException(
                    "Unknown exchange distribution wire code: " + distributionWireCode, e);
        }
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ExchangeDescriptor)) {
            return false;
        }
        ExchangeDescriptor that = (ExchangeDescriptor) other;
        return Arrays.equals(toCanonicalBytes(), that.toCanonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(toCanonicalBytes());
    }

    @Override
    public String toString() {
        return "ExchangeDescriptor{"
                + "protocolVersion="
                + protocolVersion
                + ", distributionType="
                + distributionType
                + '}';
    }
}
