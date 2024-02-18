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

public class DebeziumRowConverter implements Serializable {

    private final SeaTunnelRowType rowType;

    public DebeziumRowConverter(SeaTunnelRowType rowType) {
        this.rowType = rowType;
    }

    public SeaTunnelRow serializeValue(JsonNode node) {
        return (SeaTunnelRow) getValue(rowType, node);
    }

    private Object getValue(SeaTunnelDataType<?> dataType, JsonNode value) {
        SqlType sqlType = dataType.getSqlType();
        if (value == null) {
            return null;
        }
        switch (sqlType) {
            case BOOLEAN:
                return value.booleanValue();
            case TINYINT:
                return (byte) value.intValue();
            case SMALLINT:
                return (short) value.intValue();
            case INT:
                return value.intValue();
            case BIGINT:
                return value.longValue();
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
            case STRING:
                return value.textValue();
            case BYTES:
                try {
                    return value.binaryValue();
                } catch (IOException e) {
                    throw new RuntimeException("Invalid bytes field", e);
                }
            case DATE:
                try {
                    int d = Integer.parseInt(value.toString());
                    return LocalDate.ofEpochDay(d);
                } catch (NumberFormatException e) {
                    return LocalDate.parse(
                            value.textValue(), DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                }
            case TIME:
                try {
                    long t = Long.parseLong(value.toString());
                    return LocalTime.ofNanoOfDay(t * 1000L);
                } catch (NumberFormatException e) {
                    return LocalTime.parse(value.textValue());
                }
            case TIMESTAMP:
                try {
                    long timestamp = Long.parseLong(value.toString());
                    return LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneOffset.UTC);
                } catch (NumberFormatException e) {
                    return LocalDateTime.parse(
                            value.textValue(),
                            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"));
                }
            case ARRAY:
                List<Object> arrayValue = new ArrayList<>();
                for (JsonNode o : value) {
                    arrayValue.add(getValue(((ArrayType) dataType).getElementType(), o));
                }
                return arrayValue;
            case MAP:
                Map<Object, Object> mapValue = new LinkedHashMap<>();
                for (Iterator<Map.Entry<String, JsonNode>> it = value.fields(); it.hasNext(); ) {
                    Map.Entry<String, JsonNode> entry = it.next();
                    mapValue.put(
                            entry.getKey(),
                            getValue(((MapType) dataType).getValueType(), entry.getValue()));
                }
                return mapValue;
            case ROW:
                SeaTunnelRowType rowType = (SeaTunnelRowType) dataType;
                SeaTunnelRow row = new SeaTunnelRow(rowType.getTotalFields());
                for (int i = 0; i < rowType.getTotalFields(); i++) {
                    row.setField(
                            i,
                            getValue(rowType.getFieldType(i), value.get(rowType.getFieldName(i))));
                }
                return row;
            default:
                throw new UnsupportedOperationException("Unsupported type: " + sqlType);
        }
    }
}
