/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.format.json;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;

import lombok.Getter;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

public class JsonSerializationSchema implements SerializationSchema {

    public static final String FORMAT = "Common";
    /** RowType to generate the runtime converter. */
    private final SeaTunnelRowType rowType;

    /** Reusable object node. */
    private transient ObjectNode node;

    /** Object mapper that is used to create output JSON objects. */
    @Getter private final ObjectMapper mapper = new ObjectMapper();

    private final Charset charset;

    private final RowToJsonConverters.RowToJsonConverter runtimeConverter;

    public JsonSerializationSchema(SeaTunnelRowType rowType) {
        this(rowType, StandardCharsets.UTF_8);
    }

    public JsonSerializationSchema(SeaTunnelRowType rowType, Charset charset) {
        this.rowType = rowType;
        this.runtimeConverter = new RowToJsonConverters().createConverter(checkNotNull(rowType));
        this.charset = charset;
    }

    public JsonSerializationSchema(SeaTunnelRowType rowType, String nullValue) {
        this.rowType = rowType;
        this.runtimeConverter =
                new RowToJsonConverters().createConverter(checkNotNull(rowType), nullValue);
        this.charset = StandardCharsets.UTF_8;
    }

    public JsonSerializationSchema(SeaTunnelRowType rowType, boolean serializeTimestampTzAsLocal) {
        this(rowType, serializeTimestampTzAsLocal, null);
    }

    /**
     * Construct a {@link JsonSerializationSchema} with an explicit target zone for wall-clock
     * {@code TIMESTAMP_TZ} serialization. Use this overload when the caller knows the target
     * session zone (for example the Doris sink session timezone) so the JVM default is not silently
     * relied on.
     *
     * @param rowType the row type to serialize
     * @param serializeTimestampTzAsLocal whether to drop the offset on {@code TIMESTAMP_TZ}
     * @param timestampTzZoneId the target zone to convert {@code TIMESTAMP_TZ} values to before
     *     dropping the offset; if {@code null}, {@link ZoneId#systemDefault()} is used.
     */
    public JsonSerializationSchema(
            SeaTunnelRowType rowType,
            boolean serializeTimestampTzAsLocal,
            ZoneId timestampTzZoneId) {
        this.rowType = rowType;
        this.runtimeConverter =
                new RowToJsonConverters(serializeTimestampTzAsLocal, timestampTzZoneId)
                        .createConverter(checkNotNull(rowType));
        this.charset = StandardCharsets.UTF_8;
    }

    {
        mapper.configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);
    }

    @Override
    public byte[] serialize(SeaTunnelRow row) {
        if (node == null) {
            node = mapper.createObjectNode();
        }

        try {
            runtimeConverter.convert(mapper, node, row);
            return mapper.writeValueAsString(node).getBytes(charset);
        } catch (Throwable t) {
            throw CommonError.jsonOperationError(FORMAT, row.toString(), t);
        }
    }

    public JsonNode convert(SeaTunnelRow row) {
        if (node == null) {
            node = mapper.createObjectNode();
        }

        try {
            return runtimeConverter.convert(mapper, node, row);
        } catch (Exception e) {
            throw CommonError.jsonOperationError(FORMAT, row.toString(), e);
        }
    }
}
