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

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.json.JsonReadFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.NullNode;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.CompositeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.format.json.exception.SeaTunnelJsonFormatException;

import java.io.IOException;
import java.util.Optional;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

public class JsonDeserializationSchema implements DeserializationSchema<SeaTunnelRow> {
    private static final long serialVersionUID = 1L;

    private static final String FORMAT = "Common";

    /** Flag indicating whether to fail if a field is missing. */
    private final boolean failOnMissingField;

    /** Flag indicating whether to ignore invalid fields/rows (default: throw an exception). */
    private final boolean ignoreParseErrors;

    /** The row type of the produced {@link SeaTunnelRow}. */
    private final SeaTunnelRowType rowType;

    /**
     * Runtime converter that converts {@link JsonNode}s into objects of internal data structures.
     */
    private JsonToRowConverters.JsonToObjectConverter runtimeConverter;

    /** Object mapper for parsing the JSON. */
    private final ObjectMapper objectMapper = new ObjectMapper();

    private CatalogTable catalogTable;

    public JsonDeserializationSchema(
            boolean failOnMissingField, boolean ignoreParseErrors, SeaTunnelRowType rowType) {
        if (ignoreParseErrors && failOnMissingField) {
            throw new SeaTunnelJsonFormatException(
                    CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                    "JSON format doesn't support failOnMissingField and ignoreParseErrors are both enabled.");
        }
        this.rowType = checkNotNull(rowType);
        this.failOnMissingField = failOnMissingField;
        this.ignoreParseErrors = ignoreParseErrors;
        this.runtimeConverter =
                new JsonToRowConverters(failOnMissingField, ignoreParseErrors)
                        .createRowConverter(checkNotNull(rowType));

        if (hasDecimalType(rowType)) {
            objectMapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
        }
        objectMapper.configure(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature(), true);
    }

    public JsonDeserializationSchema(
            CatalogTable catalogTable, boolean failOnMissingField, boolean ignoreParseErrors) {
        if (ignoreParseErrors && failOnMissingField) {
            throw new SeaTunnelJsonFormatException(
                    CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                    "JSON format doesn't support failOnMissingField and ignoreParseErrors are both enabled.");
        }
        this.catalogTable = catalogTable;
        this.rowType = checkNotNull(catalogTable.getSeaTunnelRowType());
        this.failOnMissingField = failOnMissingField;
        this.ignoreParseErrors = ignoreParseErrors;
        this.runtimeConverter =
                new JsonToRowConverters(failOnMissingField, ignoreParseErrors)
                        .createRowConverter(checkNotNull(rowType));

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

    public SeaTunnelRow deserialize(String message) throws IOException {
        if (message == null) {
            return null;
        }
        return convertJsonNode(convert(message));
    }

    public void collect(byte[] message, Collector<SeaTunnelRow> out) throws IOException {
        JsonNode jsonNode = convertBytes(message);
        if (jsonNode.isArray()) {
            ArrayNode arrayNode = (ArrayNode) jsonNode;
            for (int i = 0; i < arrayNode.size(); i++) {
                SeaTunnelRow deserialize = convertJsonNode(arrayNode.get(i));
                setCollectorTablePath(deserialize, catalogTable);
                out.collect(deserialize);
            }
        } else {
            SeaTunnelRow deserialize = convertJsonNode(jsonNode);
            setCollectorTablePath(deserialize, catalogTable);
            out.collect(deserialize);
        }
    }

    public void setCollectorTablePath(SeaTunnelRow deserialize, CatalogTable catalogTable) {
        Optional<TablePath> tablePath =
                Optional.ofNullable(catalogTable).map(CatalogTable::getTablePath);
        if (tablePath.isPresent()) {
            deserialize.setTableId(tablePath.toString());
        }
    }

    private SeaTunnelRow convertJsonNode(JsonNode jsonNode) {
        if (jsonNode.isNull()) {
            return null;
        }
        try {
            return (SeaTunnelRow) runtimeConverter.convert(jsonNode, null);
        } catch (RuntimeException e) {
            if (ignoreParseErrors) {
                return null;
            }
            throw CommonError.jsonOperationError(FORMAT, jsonNode.toString(), e);
        }
    }

    public JsonNode deserializeToJsonNode(byte[] message) throws IOException {
        return objectMapper.readTree(message);
    }

    public SeaTunnelRow convertToRowData(JsonNode message) {
        return (SeaTunnelRow) runtimeConverter.convert(message, null);
    }

    private JsonNode convertBytes(byte[] message) {
        try {
            return objectMapper.readTree(message);
        } catch (IOException | RuntimeException e) {
            if (ignoreParseErrors) {
                return NullNode.getInstance();
            }
            throw CommonError.jsonOperationError(FORMAT, new String(message), e);
        }
    }

    private JsonNode convert(String message) {
        try {
            return objectMapper.readTree(message);
        } catch (JsonProcessingException | RuntimeException e) {
            if (ignoreParseErrors) {
                return NullNode.getInstance();
            }
            throw CommonError.jsonOperationError(FORMAT, new String(message), e);
        }
    }

    @Override
    public SeaTunnelRowType getProducedType() {
        return this.rowType;
    }
}
