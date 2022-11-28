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

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.CompositeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.format.json.exception.SeaTunnelJsonFormatException;

import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.io.IOException;

public class JsonDeserializationSchema implements DeserializationSchema<SeaTunnelRow> {
    private static final long serialVersionUID = 1L;

    /**
     * Flag indicating whether to fail if a field is missing.
     */
    private final boolean failOnMissingField;

    /**
     * Flag indicating whether to ignore invalid fields/rows (default: throw an exception).
     */
    private final boolean ignoreParseErrors;

    /**
     * The row type of the produced {@link SeaTunnelRow}.
     */
    private final SeaTunnelRowType rowType;

    /**
     * Runtime converter that converts {@link JsonNode}s into objects of internal data
     * structures.
     */
    private final JsonToRowConverters.JsonToRowConverter runtimeConverter;

    /**
     * Object mapper for parsing the JSON.
     */
    private final ObjectMapper objectMapper = new ObjectMapper();

    public JsonDeserializationSchema(boolean failOnMissingField,
                                     boolean ignoreParseErrors,
                                     SeaTunnelRowType rowType) {
        if (ignoreParseErrors && failOnMissingField) {
            throw new SeaTunnelJsonFormatException(CommonErrorCode.ILLEGAL_ARGUMENT,
                "JSON format doesn't support failOnMissingField and ignoreParseErrors are both enabled.");
        }
        this.rowType = checkNotNull(rowType);
        this.failOnMissingField = failOnMissingField;
        this.ignoreParseErrors = ignoreParseErrors;
        this.runtimeConverter =
            new JsonToRowConverters(failOnMissingField, ignoreParseErrors)
                .createConverter(checkNotNull(rowType));

        if (hasDecimalType(rowType)) {
            objectMapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
        }
        objectMapper.configure(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature(), true);
    }

    private static boolean hasDecimalType(SeaTunnelDataType<?> dataType) {
        if (dataType.getSqlType() == SqlType.DECIMAL) {
            return true;
        }
        if (dataType instanceof CompositeType) {
            CompositeType<?> compositeType = (CompositeType<?>) dataType;
            for (SeaTunnelDataType<?> child : compositeType.getChildren()) {
                if (hasDecimalType(child)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public SeaTunnelRow deserialize(byte[] message) throws IOException {
        if (message == null) {
            return null;
        }
        return convertJsonNode(convertBytes(message));
    }

    public void collect(byte[] message, Collector<SeaTunnelRow> out) throws IOException {
        JsonNode jsonNode = convertBytes(message);
        if (jsonNode.isArray()) {
            ArrayNode arrayNode = (ArrayNode) jsonNode;
            for (int i = 0; i < arrayNode.size(); i++) {
                SeaTunnelRow deserialize = convertJsonNode(arrayNode.get(i));
                out.collect(deserialize);
            }
        }
    }

    private SeaTunnelRow convertJsonNode(JsonNode jsonNode) throws IOException {
        if (jsonNode == null) {
            return null;
        }
        try {
            return (SeaTunnelRow) runtimeConverter.convert(jsonNode);
        } catch (Throwable t) {
            if (ignoreParseErrors) {
                return null;
            }
            throw new SeaTunnelJsonFormatException(CommonErrorCode.JSON_OPERATION_FAILED,
                    String.format("Failed to deserialize JSON '%s'.", jsonNode.asText()), t);
        }
    }

    private JsonNode convertBytes(byte[] message) throws IOException {
        try {
            return objectMapper.readTree(message);
        } catch (Throwable t) {
            if (ignoreParseErrors) {
                return null;
            }
            throw new SeaTunnelJsonFormatException(CommonErrorCode.JSON_OPERATION_FAILED,
                    String.format("Failed to deserialize JSON '%s'.", new String(message)), t);
        }
    }

    @Override
    public SeaTunnelRowType getProducedType() {
        return this.rowType;
    }
}
