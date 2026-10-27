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

package org.apache.seatunnel.format.json.debezium;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.utils.DateTimeParseHelper;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class DebeziumRowConverter implements DateTimeParseHelper, Serializable {
    private static final String DECIMAL_SCALE_KEY = "scale";
    private static final String DECIMAL_VALUE_KEY = "value";

    private final Map<String, DateTimeFormatter> fieldFormatterMap =
            new ConcurrentHashMap<String, DateTimeFormatter>() {};
    private final SeaTunnelRowType rowType;
    private final String[] rootFieldNames;
    private final SeaTunnelDataType<?>[] rootFieldTypes;

    public DebeziumRowConverter(SeaTunnelRowType rowType) {
        this.rowType = rowType;
        this.rootFieldNames = rowType.getFieldNames();
        this.rootFieldTypes = rowType.getFieldTypes();
    }

    public SeaTunnelRow parse(JsonNode node) throws IOException {
        return (SeaTunnelRow) getValue(null, rowType, node);
    }

    private Object getValue(String fieldName, SeaTunnelDataType<?> dataType, JsonNode value)
            throws IOException {
        SqlType sqlType = dataType.getSqlType();
        if (value == null || value.isNull()) {
            return null;
        }
        switch (sqlType) {
            case BOOLEAN:
                return value.asBoolean();
            case TINYINT:
                return (byte) value.asInt();
            case SMALLINT:
                return (short) value.asInt();
            case INT:
                return value.asInt();
            case BIGINT:
                return value.asLong();
            case FLOAT:
                return value.floatValue();
            case DOUBLE:
                return value.doubleValue();
            case DECIMAL:
                if (value.isNumber()) {
                    return value.decimalValue();
                }
                if (value.isBinary() || value.isTextual()) {
                    try {
                        return new BigDecimal(
                                new BigInteger(value.binaryValue()),
                                ((DecimalType) dataType).getScale());
                    } catch (Exception e) {
                        throw new RuntimeException("Invalid bytes for Decimal field", e);
                    }
                }
                if (value.has(DECIMAL_SCALE_KEY)) {
                    return new BigDecimal(
                            new BigInteger(value.get(DECIMAL_VALUE_KEY).binaryValue()),
                            value.get(DECIMAL_SCALE_KEY).intValue());
                }
                return new BigDecimal(value.asText());
            case STRING:
                return value.asText();
            case BYTES:
                try {
                    return value.binaryValue();
                } catch (IOException e) {
                    throw new RuntimeException("Invalid bytes field", e);
                }
            case DATE:
                String dateStr = value.asText();
                if (value.canConvertToLong()) {
                    return LocalDate.ofEpochDay(Long.parseLong(dateStr));
                }
                return parseDate(dateStr, fieldName, null, fieldFormatterMap);
            case TIME:
                String timeStr = value.asText();
                if (value.canConvertToLong()) {
                    long time = Long.parseLong(timeStr);
                    if (timeStr.length() == 8) {
                        time = TimeUnit.SECONDS.toMicros(time);
                    } else if (timeStr.length() == 11) {
                        time = TimeUnit.MILLISECONDS.toMicros(time);
                    }
                    return LocalTime.ofNanoOfDay(time);
                }
                return parseTime(timeStr, fieldName, null, fieldFormatterMap);
            case TIMESTAMP:
                String timestampStr = value.asText();
                if (value.canConvertToLong()) {
                    long timestamp = value.asLong();
                    if (timestampStr.length() > 16) {
                        timestamp = TimeUnit.NANOSECONDS.toMillis(timestamp);
                    } else if (timestampStr.length() > 13) {
                        timestamp = TimeUnit.MICROSECONDS.toMillis(timestamp);
                    } else if (timestampStr.length() > 10) {
                        // already in milliseconds
                    } else {
                        timestamp = TimeUnit.SECONDS.toMillis(timestamp);
                    }
                    return LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneOffset.UTC);
                }
                return parseTimestamp(timestampStr, fieldName, null, fieldFormatterMap);
            case ARRAY:
                {
                    List<Object> arrayValue = new ArrayList<>(value.size());
                    SeaTunnelDataType<?> elementType = ((ArrayType) dataType).getElementType();
                    for (JsonNode o : value) {
                        arrayValue.add(getValue(fieldName, elementType, o));
                    }
                    return arrayValue;
                }
            case MAP:
                Map<Object, Object> mapValue = new LinkedHashMap<>();
                SeaTunnelDataType<?> mapValueType = ((MapType) dataType).getValueType();
                for (Iterator<Map.Entry<String, JsonNode>> it = value.fields(); it.hasNext(); ) {
                    Map.Entry<String, JsonNode> entry = it.next();
                    mapValue.put(
                            entry.getKey(),
                            getValue(
                                    fieldName == null ? null : fieldName + ".value",
                                    mapValueType,
                                    entry.getValue()));
                }
                return mapValue;
            case ROW:
                {
                    SeaTunnelRowType nestedRowType = (SeaTunnelRowType) dataType;
                    String[] fieldNames =
                            nestedRowType == rowType
                                    ? rootFieldNames
                                    : nestedRowType.getFieldNames();
                    SeaTunnelDataType<?>[] fieldTypes =
                            nestedRowType == rowType
                                    ? rootFieldTypes
                                    : nestedRowType.getFieldTypes();
                    int arity = fieldNames.length;
                    Object[] fields = new Object[arity];
                    for (int i = 0; i < arity; i++) {
                        String name = fieldNames[i];
                        fields[i] = getValue(name, fieldTypes[i], value.get(name));
                    }
                    return new SeaTunnelRow(fields);
                }
            default:
                throw new UnsupportedOperationException("Unsupported type: " + sqlType);
        }
    }
}
