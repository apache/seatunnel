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

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.format.json.exception.SeaTunnelJsonFormatException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.Getter;

public class JsonSerializationSchema implements SerializationSchema {

    /**
     * RowType to generate the runtime converter.
     */
    private final SeaTunnelRowType rowType;

    /** Reusable object node. */
    private transient ObjectNode node;

    /** Object mapper that is used to create output JSON objects. */
    @Getter
    private final ObjectMapper mapper = new ObjectMapper();

    private final RowToJsonConverters.RowToJsonConverter runtimeConverter;

    public JsonSerializationSchema(SeaTunnelRowType rowType) {
        this.rowType = rowType;
        this.runtimeConverter = new RowToJsonConverters()
                .createConverter(checkNotNull(rowType));
    }

    @Override
    public byte[] serialize(SeaTunnelRow row) {
        if (node == null) {
            node = mapper.createObjectNode();
        }

        try {
            runtimeConverter.convert(mapper, node, row);
            return mapper.writeValueAsBytes(node);
        } catch (Throwable e) {
            throw new SeaTunnelJsonFormatException(CommonErrorCode.JSON_OPERATION_FAILED,
                    String.format("Failed to deserialize JSON '%s'.", row), e);
        }
    }
}
