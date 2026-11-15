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

package org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.serialize;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class CosmosItemDeserializer {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final SeaTunnelRowType rowType;

    public CosmosItemDeserializer(SeaTunnelRowType rowType) {
        this.rowType = rowType;
    }

    public SeaTunnelRow deserialize(Object item) {
        JsonNode root = OBJECT_MAPPER.valueToTree(item);
        SeaTunnelDataType<?>[] fieldTypes = rowType.getFieldTypes();
        String[] fieldNames = rowType.getFieldNames();
        Object[] fields = new Object[fieldNames.length];

        for (int i = 0; i < fieldNames.length; i++) {
            fields[i] = convert(fieldNames[i], fieldTypes[i], root.get(fieldNames[i]));
        }
        return new SeaTunnelRow(fields);
    }

    private Object convert(String field, SeaTunnelDataType<?> type, JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }

        switch (type.getSqlType()) {
            case BOOLEAN:
                return node.asBoolean();
            case TINYINT:
                return (byte) node.asInt();
            case SMALLINT:
                return (short) node.asInt();
            case INT:
                return node.asInt();
            case BIGINT:
                return node.asLong();
            case FLOAT:
                return (float) node.asDouble();
            case DOUBLE:
                return node.asDouble();
            case DECIMAL:
                return new BigDecimal(node.asText());
            case STRING:
                return node.isTextual() ? node.asText() : node.toString();
            case DATE:
                return LocalDate.parse(node.asText());
            case TIME:
                return LocalTime.parse(node.asText());
            case TIMESTAMP:
                return LocalDateTime.parse(node.asText());
            case BYTES:
                try {
                    return node.binaryValue();
                } catch (Exception e) {
                    throw CommonError.convertToSeaTunnelTypeError(
                            "AzureCosmosDB", type.getSqlType().toString(), field);
                }
            case MAP:
                return convertMap(field, (MapType<?, ?>) type, node);
            case ARRAY:
                return convertArray(field, (ArrayType<?, ?>) type, node);
            case ROW:
                return convertRow(field, (SeaTunnelRowType) type, node);
            default:
                throw CommonError.convertToSeaTunnelTypeError(
                        "AzureCosmosDB", type.getSqlType().toString(), field);
        }
    }

    private Map<Object, Object> convertMap(String field, MapType<?, ?> mapType, JsonNode node) {
        if (!node.isObject()) {
            throw CommonError.convertToSeaTunnelTypeError(
                    "AzureCosmosDB", mapType.getSqlType().toString(), field);
        }

        Map<Object, Object> values = new HashMap<>();
        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            Object key =
                    convert(field, mapType.getKeyType(), OBJECT_MAPPER.valueToTree(entry.getKey()));
            Object value = convert(field, mapType.getValueType(), entry.getValue());
            values.put(key, value);
        }
        return values;
    }

    private Object convertArray(String field, ArrayType<?, ?> arrayType, JsonNode node) {
        if (!node.isArray()) {
            throw CommonError.convertToSeaTunnelTypeError(
                    "AzureCosmosDB", arrayType.getSqlType().toString(), field);
        }

        Object array = Array.newInstance(arrayType.getElementType().getTypeClass(), node.size());
        for (int i = 0; i < node.size(); i++) {
            Array.set(array, i, convert(field, arrayType.getElementType(), node.get(i)));
        }
        return array;
    }

    private SeaTunnelRow convertRow(String field, SeaTunnelRowType rowType, JsonNode node) {
        if (!node.isObject()) {
            throw CommonError.convertToSeaTunnelTypeError(
                    "AzureCosmosDB", rowType.getSqlType().toString(), field);
        }

        Object[] fields = new Object[rowType.getTotalFields()];
        for (int i = 0; i < rowType.getTotalFields(); i++) {
            String fieldName = rowType.getFieldName(i);
            fields[i] = convert(fieldName, rowType.getFieldType(i), node.get(fieldName));
        }
        return new SeaTunnelRow(fields);
    }
}
