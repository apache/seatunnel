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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;

import lombok.Getter;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static com.google.common.base.Preconditions.checkNotNull;

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
}
