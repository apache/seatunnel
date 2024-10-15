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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.format.json.exception.SeaTunnelJsonFormatException;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;

/**
 * Tool class used to convert from {@link JsonNode} to {@link
 * org.apache.seatunnel.api.table.type.SeaTunnelRow}. *
 */
public class JsonToRowConverters implements Serializable {

    private static final long serialVersionUID = 1L;

    @SuppressWarnings("MagicNumber")
    public static final DateTimeFormatter TIME_FORMAT =
            new DateTimeFormatterBuilder()
                    .appendPattern("HH:mm:ss")
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                    .toFormatter();

    public static final String FORMAT = "Common";

    /** Flag indicating whether to fail if a field is missing. */
    private final boolean failOnMissingField;

    /** Flag indicating whether to ignore invalid fields/rows (default: throw an exception). */
    private final boolean ignoreParseErrors;

    public Map<String, DateTimeFormatter> fieldFormatterMap = new HashMap<>();

    public JsonToRowConverters(boolean failOnMissingField, boolean ignoreParseErrors) {
        this.failOnMissingField = failOnMissingField;
        this.ignoreParseErrors = ignoreParseErrors;
    }

    /** Creates a runtime converter which is null safe. */
    public JsonToObjectConverter createConverter(SeaTunnelDataType<?> type) {
        return wrapIntoNullableConverter(createNotNullConverter(type));
    }

    /** Creates a runtime converter which assuming input object is not null. */
    private JsonToObjectConverter createNotNullConverter(SeaTunnelDataType<?> type) {
        SqlType sqlType = type.getSqlType();
        switch (sqlType) {
            case NULL:
                return new JsonToObjectConverter() {
                    @Override
                    public Object convert(JsonNode jsonNode, String fieldName) {
                        return null;
                    }
                };
            case BOOLEAN:
                return new JsonToObjectConverter() {
                    @Override
                    public Object convert(JsonNode jsonNode, String fieldName) {
                        return convertToBoolean(jsonNode);
                    }
                };
            case TINYINT:
                return new JsonToObjectConverter() {
                    @Override
                    public Object convert(JsonNode jsonNode, String fieldName) {
                        return Byte.parseByte(jsonNode.asText().trim());
                    }
                };
            case SMALLINT:
                return new JsonToObjectConverter() {
                    @Override
                    public Object convert(JsonNode jsonNode, String fieldName) {
                        return Short.parseShort(jsonNode.asText().trim());
                    }
                };
            case INT:
                return new JsonToObjectConverter() {
                    @Override
                    public Object convert(JsonNode jsonNode, String fieldName) {
                        return convertToInt(jsonNode);
                    }
                };
            case BIGINT:
                return new JsonToObjectConverter() {
                    @Override
                    public Object convert(JsonNode jsonNode, String fieldName) {
                        return convertToLong(jsonNode);
                    }
                };
            case DATE:
                return new JsonToObjectConverter() {
                    @Override
                    public Object convert(JsonNode jsonNode, String fieldName) {
                        return convertToLocalDate(jsonNode, fieldName);
                    }
                };
            case TIME:
                return new JsonToObjectConverter() {
                    @Override
                    public Object convert(JsonNode jsonNode, String fieldName) {
                        return convertToLocalTime(jsonNode);
                    }
                };
            case TIMESTAMP:
                return new JsonToObjectConverter() {
                    @Override
                    public Object convert(JsonNode jsonNode, String fieldName) {
                        return convertToLocalDateTime(jsonNode, fieldName);
                    }
                };
            case FLOAT:
                return new JsonToObjectConverter() {
                    @Override
                    public Object convert(JsonNode jsonNode, String fieldName) {
                        return convertToFloat(jsonNode);
                    }
                };
            case DOUBLE:
                return new JsonToObjectConverter() {
                    @Override
                    public Object convert(JsonNode jsonNode, String fieldName) {
                        return convertToDouble(jsonNode);
                    }
                };
            case STRING:
                return new JsonToObjectConverter() {
                    @Override
                    public Object convert(JsonNode jsonNode, String fieldName) {
                        return convertToString(jsonNode);
                    }
                };
            case BYTES:
                return new JsonToObjectConverter() {
                    @Override
                    public Object convert(JsonNode jsonNode, String fieldName) {
                        return convertToBytes(jsonNode);
                    }
                };
            case DECIMAL:
                return new JsonToObjectConverter() {
                    @Override
                    public Object convert(JsonNode jsonNode, String fieldName) {
                        return convertToBigDecimal(jsonNode);
                    }
                };
            case ARRAY:
                return createArrayConverter((ArrayType<?, ?>) type);
            case MAP:
                return createMapConverter((MapType<?, ?>) type);
            case ROW:
                return createRowConverter((SeaTunnelRowType) type);
            default:
                throw new SeaTunnelJsonFormatException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                        "Unsupported type: " + type);
        }
    }

    private boolean convertToBoolean(JsonNode jsonNode) {
        if (jsonNode.isBoolean()) {
            // avoid redundant toString and parseBoolean, for better performance
            return jsonNode.asBoolean();
        } else {
            return Boolean.parseBoolean(jsonNode.asText().trim());
        }
    }

    private int convertToInt(JsonNode jsonNode) {
        if (jsonNode.canConvertToInt()) {
            // avoid redundant toString and parseInt, for better performance
            return jsonNode.asInt();
        } else {
            return Integer.parseInt(jsonNode.asText().trim());
        }
    }

    private long convertToLong(JsonNode jsonNode) {
        if (jsonNode.canConvertToLong()) {
            // avoid redundant toString and parseLong, for better performance
            return jsonNode.asLong();
        } else {
            return Long.parseLong(jsonNode.asText().trim());
        }
    }

    private double convertToDouble(JsonNode jsonNode) {
        if (jsonNode.isDouble()) {
            // avoid redundant toString and parseDouble, for better performance
            return jsonNode.asDouble();
        } else {
            return Double.parseDouble(jsonNode.asText().trim());
        }
    }

    private float convertToFloat(JsonNode jsonNode) {
        if (jsonNode.isDouble()) {
            // avoid redundant toString and parseDouble, for better performance
            return (float) jsonNode.asDouble();
        } else {
            return Float.parseFloat(jsonNode.asText().trim());
        }
    }

    private LocalDate convertToLocalDate(JsonNode jsonNode, String fieldName) {
        if ("".equals(jsonNode.asText())) {
            return null;
        }
        String dateStr = jsonNode.asText();
        DateTimeFormatter dateFormatter = fieldFormatterMap.get(fieldName);
        if (dateFormatter == null) {
            dateFormatter = DateUtils.matchDateFormatter(dateStr);
            fieldFormatterMap.put(fieldName, dateFormatter);
        }
        if (dateFormatter == null) {
            throw CommonError.formatDateError(dateStr, fieldName);
        }

        return dateFormatter.parse(jsonNode.asText()).query(TemporalQueries.localDate());
    }

    private LocalTime convertToLocalTime(JsonNode jsonNode) {
        if ("".equals(jsonNode.asText())) {
            return null;
        }
        TemporalAccessor parsedTime = TIME_FORMAT.parse(jsonNode.asText());
        return parsedTime.query(TemporalQueries.localTime());
    }

    private LocalDateTime convertToLocalDateTime(JsonNode jsonNode, String fieldName) {
        if ("".equals(jsonNode.asText())) {
            return null;
        }

        String datetimeStr = jsonNode.asText();
        DateTimeFormatter dateTimeFormatter = fieldFormatterMap.get(fieldName);
        if (dateTimeFormatter == null) {
            dateTimeFormatter = DateTimeUtils.matchDateTimeFormatter(datetimeStr);
            fieldFormatterMap.put(fieldName, dateTimeFormatter);
        }
        if (dateTimeFormatter == null) {
            throw CommonError.formatDateTimeError(datetimeStr, fieldName);
        }

        TemporalAccessor parsedTimestamp = dateTimeFormatter.parse(datetimeStr);
        LocalTime localTime = parsedTimestamp.query(TemporalQueries.localTime());
        LocalDate localDate = parsedTimestamp.query(TemporalQueries.localDate());
        return LocalDateTime.of(localDate, localTime);
    }

    private String convertToString(JsonNode jsonNode) {
        if (jsonNode.isContainerNode()) {
            return jsonNode.toString();
        } else {
            return jsonNode.asText();
        }
    }

    private byte[] convertToBytes(JsonNode jsonNode) {
        try {
            return jsonNode.binaryValue();
        } catch (IOException e) {
            throw CommonError.jsonOperationError(FORMAT, jsonNode.toString(), e);
        }
    }

    private BigDecimal convertToBigDecimal(JsonNode jsonNode) {
        BigDecimal bigDecimal;
        if (jsonNode.isBigDecimal()) {
            bigDecimal = jsonNode.decimalValue();
        } else {
            bigDecimal = new BigDecimal(jsonNode.asText());
        }

        return bigDecimal;
    }

    public JsonToObjectConverter createRowConverter(SeaTunnelRowType rowType) {
        final JsonToObjectConverter[] fieldConverters =
                Arrays.stream(rowType.getFieldTypes())
                        .map(
                                new Function<SeaTunnelDataType<?>, Object>() {
                                    @Override
                                    public Object apply(SeaTunnelDataType<?> seaTunnelDataType) {
                                        return createConverter(seaTunnelDataType);
                                    }
                                })
                        .toArray(
                                new IntFunction<JsonToObjectConverter[]>() {
                                    @Override
                                    public JsonToObjectConverter[] apply(int value) {
                                        return new JsonToObjectConverter[value];
                                    }
                                });
        final String[] fieldNames = rowType.getFieldNames();

        return new JsonToObjectConverter() {
            @Override
            public SeaTunnelRow convert(JsonNode jsonNode, String rowFieldName) {
                if (jsonNode == null || jsonNode.isNull() || jsonNode.isMissingNode()) {
                    return null;
                }
                int arity = fieldNames.length;
                SeaTunnelRow row = new SeaTunnelRow(arity);
                for (int i = 0; i < arity; i++) {
                    String fieldName = fieldNames[i];
                    JsonNode field;
                    if (jsonNode.isArray()) {
                        field = jsonNode.get(i);
                    } else {
                        field = jsonNode.get(fieldName);
                    }
                    try {
                        if (StringUtils.isNotBlank(rowFieldName)) {
                            fieldName = rowFieldName + "." + fieldName;
                        }
                        Object convertedField = convertField(fieldConverters[i], fieldName, field);
                        row.setField(i, convertedField);
                    } catch (Throwable t) {
                        throw CommonError.jsonOperationError(
                                FORMAT,
                                String.format("Field $.%s in %s", fieldName, jsonNode.toString()),
                                t);
                    }
                }
                return row;
            }
        };
    }

    private JsonToObjectConverter createArrayConverter(ArrayType<?, ?> type) {
        JsonToObjectConverter valueConverter = createConverter(type.getElementType());
        return new JsonToObjectConverter() {
            @Override
            public Object convert(JsonNode jsonNode, String fieldName) {
                Object arr =
                        Array.newInstance(type.getElementType().getTypeClass(), jsonNode.size());
                for (int i = 0; i < jsonNode.size(); i++) {
                    Array.set(arr, i, valueConverter.convert(jsonNode.get(i), fieldName));
                }
                return arr;
            }
        };
    }

    private JsonToObjectConverter createMapConverter(MapType<?, ?> type) {
        JsonToObjectConverter keyConverter = createConverter(type.getKeyType());
        JsonToObjectConverter valueConverter = createConverter(type.getValueType());
        return new JsonToObjectConverter() {
            @Override
            public Object convert(JsonNode jsonNode, String fieldName) {
                Map<Object, Object> value = new HashMap<>();
                jsonNode.fields()
                        .forEachRemaining(
                                new Consumer<Map.Entry<String, JsonNode>>() {
                                    @Override
                                    public void accept(Map.Entry<String, JsonNode> entry) {
                                        JsonNode keyNode;
                                        try {
                                            keyNode =
                                                    JsonUtils.stringToJsonNode(
                                                            JsonUtils.toJsonString(entry.getKey()));
                                        } catch (Exception e) {
                                            throw CommonError.jsonOperationError(
                                                    FORMAT, entry.getKey(), e);
                                        }
                                        value.put(
                                                keyConverter.convert(keyNode, fieldName + ".key"),
                                                valueConverter.convert(
                                                        entry.getValue(), fieldName + ".value"));
                                    }
                                });
                return value;
            }
        };
    }

    private Object convertField(
            JsonToObjectConverter fieldConverter, String fieldName, JsonNode field) {
        if (field == null) {
            if (failOnMissingField) {
                throw new IllegalArgumentException(
                        String.format("Could not find field with name %s .", fieldName));
            } else {
                return null;
            }
        } else {
            return fieldConverter.convert(field, fieldName);
        }
    }

    private JsonToObjectConverter wrapIntoNullableConverter(JsonToObjectConverter converter) {
        return new JsonToObjectConverter() {
            @Override
            public Object convert(JsonNode jsonNode, String fieldName) {
                if (jsonNode == null || jsonNode.isNull() || jsonNode.isMissingNode()) {
                    return null;
                }
                try {
                    return converter.convert(jsonNode, fieldName);
                } catch (RuntimeException e) {
                    if (!ignoreParseErrors) {
                        throw e;
                    }
                    return null;
                }
            }
        };
    }

    /**
     * Runtime converter that converts {@link JsonNode}s into objects of internal data structures.
     */
    public interface JsonToObjectConverter extends Serializable {
        Object convert(JsonNode jsonNode, String fieldName);
    }

    /** Exception which refers to parse errors in converters. */
    private static final class JsonParseException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public JsonParseException(String message) {
            super(message);
        }

        public JsonParseException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
