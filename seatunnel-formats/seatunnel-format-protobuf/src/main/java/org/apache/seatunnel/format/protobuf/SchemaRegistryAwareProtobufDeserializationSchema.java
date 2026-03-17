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

    /**
     * Maximum number of additional header bytes (beyond the 5 bytes magic + schema id) to probe
     * when trying to locate the actual Protobuf message. This covers the variable-length "message
     * indexes" part used by Schema Registry for Protobuf.
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
            // Try candidateStart = 6 first (common case: single message index)
            SeaTunnelRow result = tryDeserialize(message, 6, length);
            if (result != null) {
                return result;
            }

            // Probe other offsets (5 to 5 + MAX_ADDITIONAL_HEADER_BYTES)
            int maxProbeStart = Math.min(5 + MAX_ADDITIONAL_HEADER_BYTES, length - 1);
            for (int start = 5; start <= maxProbeStart; start++) {
                if (start == 6) {
                    continue; // Already tried
                }
                result = tryDeserialize(message, start, length);
                if (result != null) {
                    return result;
                }
            }
        }

        // Fallback: try original message (no Schema Registry header)
        return inner.deserialize(message);
    }

    /**
     * Try to deserialize message starting from the given offset. Uses ByteArrayInputStream to avoid
     * copying the byte array.
     *
     * @param message the original message byte array
     * @param offset the starting offset in the array
     * @param length the total length of the array
     * @return deserialized SeaTunnelRow, or null if parsing fails
     */
    private SeaTunnelRow tryDeserialize(byte[] message, int offset, int length) {
        int remaining = length - offset;
        // A valid protobuf message must have at least 2 bytes (tag + value for a small field)
        if (remaining < 2) {
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
