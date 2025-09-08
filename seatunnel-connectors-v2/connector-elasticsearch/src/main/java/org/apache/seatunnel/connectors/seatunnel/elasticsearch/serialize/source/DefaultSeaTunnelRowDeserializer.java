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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.serialize.source;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.NullNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.TextNode;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.exception.ElasticsearchConnectorException;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.seatunnel.api.table.type.BasicType.BOOLEAN_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.BYTE_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.DOUBLE_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.FLOAT_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.INT_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.LONG_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.SHORT_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.VOID_TYPE;

public class DefaultSeaTunnelRowDeserializer implements SeaTunnelRowDeserializer {

    private final SeaTunnelRowType rowTypeInfo;

    private final ObjectMapper mapper = new ObjectMapper();

    private final String nullDefault = "null";

    private final Map<Integer, DateTimeFormatter> dateTimeFormatterMap =
            new HashMap<Integer, DateTimeFormatter>() {
                {
                    put("yyyy-MM-dd HH".length(), DateTimeFormatter.ofPattern("yyyy-MM-dd HH"));
                    put(
                            "yyyy-MM-dd HH:mm".length(),
                            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"));
                    put(
                            "yyyyMMdd HH:mm:ss".length(),
                            DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss"));
                    put(
                            "yyyy-MM-dd HH:mm:ss".length(),
                            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                    put(
                            "yyyy-MM-dd HH:mm:ss.S".length(),
                            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S"));
                    put(
                            "yyyy-MM-dd HH:mm:ss.SS".length(),
                            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SS"));
                    put(
                            "yyyy-MM-dd HH:mm:ss.SSS".length(),
                            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
                    put(
                            "yyyy-MM-dd HH:mm:ss.SSSS".length(),
                            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSS"));
                    put(
                            "yyyy-MM-dd HH:mm:ss.SSSSSS".length(),
                            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS"));
                    put(
                            "yyyy-MM-dd HH:mm:ss.SSSSSSSSS".length(),
                            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS"));
                }
            };

    public DefaultSeaTunnelRowDeserializer(SeaTunnelRowType rowTypeInfo) {
        this.rowTypeInfo = rowTypeInfo;
    }

    @Override
    public SeaTunnelRow deserialize(ElasticsearchRecord rowRecord) {
        return convert(rowRecord);
    }

    SeaTunnelRow convert(ElasticsearchRecord rowRecord) {
        Object[] seaTunnelFields = new Object[rowTypeInfo.getTotalFields()];
        String fieldName = null;
        Object value = null;
        SeaTunnelDataType seaTunnelDataType = null;
        try {
            for (int i = 0; i < rowTypeInfo.getTotalFields(); i++) {
                fieldName = rowTypeInfo.getFieldName(i);
                value = recursiveGet(rowRecord.getDoc(), fieldName);
                if (value != null) {
                    seaTunnelDataType = rowTypeInfo.getFieldType(i);
                    if (value instanceof NullNode) {
                        seaTunnelFields[i] = null;
                    } else if (value instanceof TextNode) {
                        seaTunnelFields[i] =
                                convertValue(seaTunnelDataType, ((TextNode) value).textValue());
                    } else {
                        seaTunnelFields[i] = convertValue(seaTunnelDataType, value.toString());
                    }
                }
            }
        } catch (Exception ex) {
            throw new ElasticsearchConnectorException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                    String.format(
                            "error fieldName=%s,fieldValue=%s,seaTunnelDataType=%s,rowRecord=%s",
                            fieldName, value, seaTunnelDataType, JsonUtils.toJsonString(rowRecord)),
                    ex);
        }
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(seaTunnelFields);
        seaTunnelRow.setTableId(rowRecord.getTableId());
        return seaTunnelRow;
    }

    Object convertValue(SeaTunnelDataType<?> fieldType, String fieldValue)
            throws JsonProcessingException {
        if (STRING_TYPE.equals(fieldType)) {
            return fieldValue;
        } else {
            if (nullDefault.equals(fieldValue)) {
                return null;
            }
            if (BOOLEAN_TYPE.equals(fieldType)) {
                return Boolean.parseBoolean(fieldValue);
            } else if (BYTE_TYPE.equals(fieldType)) {
                return Byte.valueOf(fieldValue);
            } else if (SHORT_TYPE.equals(fieldType)) {
                return Short.parseShort(fieldValue);
            } else if (INT_TYPE.equals(fieldType)) {
                return Integer.parseInt(fieldValue);
            } else if (LONG_TYPE.equals(fieldType)) {
                return Long.parseLong(fieldValue);
            } else if (FLOAT_TYPE.equals(fieldType)) {
                return Float.parseFloat(fieldValue);
            } else if (DOUBLE_TYPE.equals(fieldType)) {
                return Double.parseDouble(fieldValue);
            } else if (LocalTimeType.LOCAL_DATE_TYPE.equals(fieldType)) {
                LocalDateTime localDateTime = parseDate(fieldValue);
                return localDateTime.toLocalDate();
            } else if (LocalTimeType.LOCAL_TIME_TYPE.equals(fieldType)) {
                LocalDateTime localDateTime = parseDate(fieldValue);
                return localDateTime.toLocalTime();
            } else if (LocalTimeType.LOCAL_DATE_TIME_TYPE.equals(fieldType)) {
                return parseDate(fieldValue);
            } else if (fieldType instanceof DecimalType) {
                return new BigDecimal(fieldValue);
            } else if (fieldType instanceof ArrayType) {
                ArrayType<?, ?> arrayType = (ArrayType<?, ?>) fieldType;
                SeaTunnelDataType<?> elementType = arrayType.getElementType();
                List<String> stringList = new ArrayList<>();
                if (elementType instanceof MapType) {
                    stringList =
                            JsonUtils.isJsonArray(fieldValue)
                                    ? JsonUtils.toList(fieldValue, Map.class).stream()
                                            .map(JsonUtils::toJsonString)
                                            .collect(Collectors.toList())
                                    : Collections.singletonList(fieldValue);
                } else {
                    stringList = JsonUtils.toList(fieldValue, String.class);
                }
                Object arr = Array.newInstance(elementType.getTypeClass(), stringList.size());
                for (int i = 0; i < stringList.size(); i++) {
                    Object convertValue = convertValue(elementType, stringList.get(i));
                    Array.set(arr, i, convertValue);
                }
                return arr;
            } else if (fieldType instanceof MapType) {
                MapType<?, ?> mapType = (MapType<?, ?>) fieldType;
                SeaTunnelDataType<?> keyType = mapType.getKeyType();

                SeaTunnelDataType<?> valueType = mapType.getValueType();
                Map<String, String> stringMap =
                        mapper.readValue(
                                fieldValue, new TypeReference<HashMap<String, String>>() {});
                Map<Object, Object> convertMap = new HashMap<Object, Object>();
                for (Map.Entry<String, String> entry : stringMap.entrySet()) {
                    Object convertKey = convertValue(keyType, entry.getKey());
                    Object convertValue = convertValue(valueType, entry.getValue());
                    convertMap.put(convertKey, convertValue);
                }
                return convertMap;
            } else if (fieldType instanceof SeaTunnelRowType) {
                SeaTunnelRowType rowType = (SeaTunnelRowType) fieldType;
                Map<String, Object> collect =
                        mapper.readValue(fieldValue, new TypeReference<Map<String, Object>>() {});
                Object[] seaTunnelFields = new Object[rowType.getTotalFields()];
                for (int i = 0; i < rowType.getTotalFields(); i++) {
                    String fieldName = rowType.getFieldName(i);
                    SeaTunnelDataType<?> fieldDataType = rowType.getFieldType(i);
                    Object value = collect.get(fieldName);
                    if (value != null) {
                        seaTunnelFields[i] =
                                convertValue(
                                        fieldDataType,
                                        (value instanceof List || value instanceof Map)
                                                ? mapper.writeValueAsString(value)
                                                : value.toString());
                    }
                }
                return new SeaTunnelRow(seaTunnelFields);
            } else if (fieldType instanceof PrimitiveByteArrayType) {
                return Base64.getDecoder().decode(fieldValue);
            } else if (VOID_TYPE.equals(fieldType) || fieldType == null) {
                return null;
            } else {
                throw new ElasticsearchConnectorException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                        "Unexpected value: " + fieldType);
            }
        }
    }

    private LocalDateTime parseDate(String fieldValue) {
        // handle strings of timestamp type
        try {
            long ts = Long.parseLong(fieldValue);
            return LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneId.systemDefault());
        } catch (NumberFormatException e) {
            // no op
        }
        String formatDate = fieldValue.replace("T", " ").replace("Z", "");
        if (fieldValue.length() == "yyyyMMdd".length()
                || fieldValue.length() == "yyyy-MM-dd".length()) {
            formatDate = fieldValue + " 00:00:00";
        }
        DateTimeFormatter dateTimeFormatter = dateTimeFormatterMap.get(formatDate.length());
        if (dateTimeFormatter == null) {
            throw new ElasticsearchConnectorException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION, "unsupported date format");
        }
        return LocalDateTime.parse(formatDate, dateTimeFormatter);
    }

    Object recursiveGet(Map<String, Object> collect, String keyWithRecursive) {
        Object value = null;
        boolean isFirst = true;
        for (String key : keyWithRecursive.split("\\.")) {
            if (isFirst) {
                value = collect.get(key);
                isFirst = false;
            } else if (value instanceof ObjectNode) {
                value = ((ObjectNode) value).get(key);
            }
        }
        return value;
    }
}
