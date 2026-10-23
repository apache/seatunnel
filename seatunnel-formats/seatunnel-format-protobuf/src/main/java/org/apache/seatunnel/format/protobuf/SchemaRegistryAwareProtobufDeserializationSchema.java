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

package org.apache.seatunnel.format.protobuf;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * A Protobuf deserialization schema that is aware of Confluent Schema Registry's wire format.
 *
 * <p>This schema will try to strip the Schema Registry header (magic byte, schema id and message
 * indexes) before delegating to {@link ProtobufDeserializationSchema}. If stripping fails, it falls
 * back to using the original payload, so it can safely be enabled for both plain and Schema
 * Registry encoded messages.
 */
public class SchemaRegistryAwareProtobufDeserializationSchema
        implements DeserializationSchema<SeaTunnelRow> {

    private static final long serialVersionUID = -2134049729306615854L;

    private static final int SCHEMA_REGISTRY_PREFIX_LENGTH = 5;
    private static final int MAX_VARINT_BYTES = 5;
    private static final long INVALID_VARINT = -1L;

    /**
     * Maximum number of offsets retained for the legacy best-effort probe. Structurally valid
     * Confluent headers do not use this limit; it only preserves non-empty messages accepted before
     * the header parser was introduced.
     */
    private static final int MAX_ADDITIONAL_HEADER_BYTES = 16;

    private static final Logger LOG =
            LoggerFactory.getLogger(SchemaRegistryAwareProtobufDeserializationSchema.class);

    private final ProtobufDeserializationSchema inner;
    private final SeaTunnelRowType rowType;

    public SchemaRegistryAwareProtobufDeserializationSchema(CatalogTable catalogTable) {
        this.inner = new ProtobufDeserializationSchema(catalogTable);
        this.rowType = catalogTable.getSeaTunnelRowType();
    }

    @Override
    public SeaTunnelRow deserialize(byte[] message) throws IOException {
        if (message == null || message.length == 0) {
            return inner.deserialize(message);
        }

        int length = message.length;

        // Confluent Schema Registry Protobuf wire format:
        // 1 byte magic (0), 4 bytes schema id, N bytes message indexes (varints), then protobuf.
        if (length >= 6 && message[0] == 0) {
            int payloadOffset = findPayloadOffset(message);
            if (payloadOffset >= 0) {
                SeaTunnelRow result = tryDeserialize(message, payloadOffset, length, true);
                if (result != null) {
                    return result;
                }
            }

            // Preserve compatibility with messages accepted by the previous best-effort probe.
            // Try candidateStart = 6 first (common case: single message index)
            if (payloadOffset != 6) {
                SeaTunnelRow result = tryDeserialize(message, 6, length, false);
                if (result != null) {
                    return result;
                }
            }

            // Probe other offsets (5 to 5 + MAX_ADDITIONAL_HEADER_BYTES)
            int maxProbeStart = Math.min(5 + MAX_ADDITIONAL_HEADER_BYTES, length - 1);
            for (int start = 5; start <= maxProbeStart; start++) {
                if (start == 6 || start == payloadOffset) {
                    continue; // Already tried
                }
                SeaTunnelRow result = tryDeserialize(message, start, length, false);
                if (result != null) {
                    return result;
                }
            }
        }

        // Fallback: try original message (no Schema Registry header)
        return inner.deserialize(message);
    }

    /** Returns the payload offset for a structurally valid Confluent Protobuf header. */
    private static int findPayloadOffset(byte[] message) {
        long lengthVarInt = readUnsignedVarInt(message, SCHEMA_REGISTRY_PREFIX_LENGTH);
        if (lengthVarInt == INVALID_VARINT) {
            return -1;
        }

        int offset = (int) (lengthVarInt >>> 32);
        long encodedLength = lengthVarInt & 0xFFFFFFFFL;
        if (encodedLength == 0) {
            // Confluent encodes the common message-index path [0] as a single zero byte.
            return offset;
        }

        int indexCount = decodeZigZagInt(encodedLength);
        if (indexCount <= 0 || indexCount > message.length - offset) {
            return -1;
        }

        for (int index = 0; index < indexCount; index++) {
            long indexVarInt = readUnsignedVarInt(message, offset);
            if (indexVarInt == INVALID_VARINT || decodeZigZagInt(indexVarInt & 0xFFFFFFFFL) < 0) {
                return -1;
            }
            offset = (int) (indexVarInt >>> 32);
        }
        return offset;
    }

    /**
     * Reads one unsigned 32-bit varint without allocating a holder object. The upper 32 bits
     * contain the next offset and the lower 32 bits contain the decoded value.
     */
    private static long readUnsignedVarInt(byte[] message, int offset) {
        int value = 0;
        for (int byteIndex = 0; byteIndex < MAX_VARINT_BYTES; byteIndex++) {
            if (offset >= message.length) {
                return INVALID_VARINT;
            }

            int current = message[offset++] & 0xFF;
            if (byteIndex == MAX_VARINT_BYTES - 1 && (current & 0xF0) != 0) {
                return INVALID_VARINT;
            }
            value |= (current & 0x7F) << (byteIndex * 7);
            if ((current & 0x80) == 0) {
                return ((long) offset << 32) | (value & 0xFFFFFFFFL);
            }
        }
        return INVALID_VARINT;
    }

    private static int decodeZigZagInt(long value) {
        return (int) ((value >>> 1) ^ -(value & 1L));
    }

    /**
     * Try to deserialize message starting from the given offset. Uses ByteArrayInputStream to avoid
     * copying the byte array.
     *
     * @param message the original message byte array
     * @param offset the starting offset in the array
     * @param length the total length of the array
     * @param allowEmptyPayload whether a structurally validated header may have a zero-byte payload
     * @return deserialized SeaTunnelRow, or null if parsing fails
     */
    private SeaTunnelRow tryDeserialize(
            byte[] message, int offset, int length, boolean allowEmptyPayload) {
        int remaining = length - offset;
        if (remaining < (allowEmptyPayload ? 0 : 2)) {
            return null;
        }

        try (ByteArrayInputStream inputStream =
                new ByteArrayInputStream(message, offset, remaining)) {
            return inner.deserialize(inputStream);
        } catch (IOException | RuntimeException e) {
            LOG.warn(
                    "Protobuf message not recognized at candidate offset {}, falling back",
                    offset,
                    e);
            return null;
        }
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return this.rowType;
    }
}
