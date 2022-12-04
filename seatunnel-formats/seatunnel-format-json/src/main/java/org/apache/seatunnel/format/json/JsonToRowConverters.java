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

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.format.json.exception.SeaTunnelJsonFormatException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

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

/** Tool class used to convert from {@link JsonNode} to {@link org.apache.seatunnel.api.table.type.SeaTunnelRow}. * */
public class JsonToRowConverters implements Serializable {

    private static final long serialVersionUID = 1L;

    @SuppressWarnings("MagicNumber")
    public static final DateTimeFormatter TIME_FORMAT =
        new DateTimeFormatterBuilder()
            .appendPattern("HH:mm:ss")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .toFormatter();

    /** Flag indicating whether to fail if a field is missing. */
    private final boolean failOnMissingField;

    /** Flag indicating whether to ignore invalid fields/rows (default: throw an exception). */
    private final boolean ignoreParseErrors;

    public JsonToRowConverters(
        boolean failOnMissingField,
        boolean ignoreParseErrors) {
        this.failOnMissingField = failOnMissingField;
        this.ignoreParseErrors = ignoreParseErrors;
    }

    /** Creates a runtime converter which is null safe. */
    public JsonToRowConverter createConverter(SeaTunnelDataType<?> type) {
        return wrapIntoNullableConverter(createNotNullConverter(type));
    }

    /** Creates a runtime converter which assuming input object is not null. */
    private JsonToRowConverter createNotNullConverter(SeaTunnelDataType<?> type) {
        SqlType sqlType = type.getSqlType();
        switch (sqlType) {
            case ROW:
                return createRowConverter((SeaTunnelRowType) type);
            case NULL:
                return new JsonToRowConverter() {
                    @Override
                    public Object convert(JsonNode jsonNode) {
                        return null;
                    }
                };
            case BOOLEAN:
                return new JsonToRowConverter() {
                    @Override
                    public Object convert(JsonNode jsonNode) {
                        return convertToBoolean(jsonNode);
                    }
                };
            case TINYINT:
                return new JsonToRowConverter() {
                    @Override
                    public Object convert(JsonNode jsonNode) {
                        return Byte.parseByte(jsonNode.asText().trim());
                    }
                };
            case SMALLINT:
                return new JsonToRowConverter() {
                    @Override
                    public Object convert(JsonNode jsonNode) {
                        return Short.parseShort(jsonNode.asText().trim());
                    }
                };
            case INT:
                return new JsonToRowConverter() {
                    @Override
                    public Object convert(JsonNode jsonNode) {
                        return convertToInt(jsonNode);
                    }
                };
            case BIGINT:
                return new JsonToRowConverter() {
                    @Override
                    public Object convert(JsonNode jsonNode) {
                        return convertToLong(jsonNode);
                    }
                };
            case DATE:
                return new JsonToRowConverter() {
                    @Override
                    public Object convert(JsonNode jsonNode) {
                        return convertToLocalDate(jsonNode);
                    }
                };
            case TIME:
                return new JsonToRowConverter() {
                    @Override
                    public Object convert(JsonNode jsonNode) {
                        return convertToLocalTime(jsonNode);
                    }
                };
            case TIMESTAMP:
                return new JsonToRowConverter() {
                    @Override
                    public Object convert(JsonNode jsonNode) {
                        return convertToLocalDateTime(jsonNode);
                    }
                };
            case FLOAT:
                return new JsonToRowConverter() {
                    @Override
                    public Object convert(JsonNode jsonNode) {
                        return convertToFloat(jsonNode);
                    }
                };
            case DOUBLE:
                return new JsonToRowConverter() {
                    @Override
                    public Object convert(JsonNode jsonNode) {
                        return convertToDouble(jsonNode);
                    }
                };
            case STRING:
                return new JsonToRowConverter() {
                    @Override
                    public Object convert(JsonNode jsonNode) {
                        return convertToString(jsonNode);
                    }
                };
            case BYTES:
                return new JsonToRowConverter() {
                    @Override
                    public Object convert(JsonNode jsonNode) {
                        return convertToBytes(jsonNode);
                    }
                };
            case DECIMAL:
                return new JsonToRowConverter() {
                    @Override
                    public Object convert(JsonNode jsonNode) {
                        return convertToBigDecimal(jsonNode);
                    }
                };
            case ARRAY:
                return createArrayConverter((ArrayType<?, ?>) type);
            case MAP:
                return createMapConverter((MapType<?, ?>) type);
            default:
                throw new SeaTunnelJsonFormatException(CommonErrorCode.UNSUPPORTED_DATA_TYPE,
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

    private LocalDate convertToLocalDate(JsonNode jsonNode) {
        return ISO_LOCAL_DATE.parse(jsonNode.asText()).query(TemporalQueries.localDate());
    }

    private LocalTime convertToLocalTime(JsonNode jsonNode) {
        TemporalAccessor parsedTime = TIME_FORMAT.parse(jsonNode.asText());
        return parsedTime.query(TemporalQueries.localTime());
    }

    private LocalDateTime convertToLocalDateTime(JsonNode jsonNode) {
        TemporalAccessor parsedTimestamp = DateTimeFormatter.ISO_LOCAL_DATE_TIME.parse(jsonNode.asText());
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
            throw new SeaTunnelJsonFormatException(CommonErrorCode.JSON_OPERATION_FAILED,
                    "Unable to deserialize byte array.", e);
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

    private JsonToRowConverter createRowConverter(SeaTunnelRowType rowType) {
        final JsonToRowConverter[] fieldConverters =
            Arrays.stream(rowType.getFieldTypes())
                .map(new Function<SeaTunnelDataType<?>, Object>() {
                    @Override
                    public Object apply(SeaTunnelDataType<?> seaTunnelDataType) {
                        return createConverter(seaTunnelDataType);
                    }
                })
                .toArray(new IntFunction<JsonToRowConverter[]>() {
                    @Override
                    public JsonToRowConverter[] apply(int value) {
                        return new JsonToRowConverter[value];
                    }
                });
        final String[] fieldNames = rowType.getFieldNames();

        return new JsonToRowConverter() {
            @Override
            public Object convert(JsonNode jsonNode) {
                ObjectNode node = (ObjectNode) jsonNode;
                int arity = fieldNames.length;
                SeaTunnelRow row = new SeaTunnelRow(arity);
                for (int i = 0; i < arity; i++) {
                    String fieldName = fieldNames[i];
                    JsonNode field = node.get(fieldName);
                    try {
                        Object convertedField = convertField(fieldConverters[i], fieldName, field);
                        row.setField(i, convertedField);
                    } catch (Throwable t) {
                        throw new SeaTunnelJsonFormatException(CommonErrorCode.JSON_OPERATION_FAILED,
                                String.format("Fail to deserialize at field: %s.", fieldName), t);
                    }
                }
                return row;
            }
        };
    }

    private JsonToRowConverter createArrayConverter(ArrayType<?, ?> type) {
        JsonToRowConverter valueConverter = createConverter(type.getElementType());
        return new JsonToRowConverter() {
            @Override
            public Object convert(JsonNode jsonNode) {
                Object arr = Array.newInstance(type.getElementType().getTypeClass(), jsonNode.size());
                for (int i = 0; i < jsonNode.size(); i++) {
                    Array.set(arr, i, valueConverter.convert(jsonNode.get(i)));
                }
                return arr;
            }
        };
    }

    private JsonToRowConverter createMapConverter(MapType<?, ?> type) {
        JsonToRowConverter valueConverter = createConverter(type.getValueType());
        return new JsonToRowConverter() {
            @Override
            public Object convert(JsonNode jsonNode) {
                Map<Object, Object> value = new HashMap<>();
                jsonNode.fields().forEachRemaining(new Consumer<Map.Entry<String, JsonNode>>() {
                    @Override
                    public void accept(Map.Entry<String, JsonNode> entry) {
                        value.put(entry.getKey(), valueConverter.convert(entry.getValue()));
                    }
                });
                return value;
            }
        };
    }

    private Object convertField(
        JsonToRowConverter fieldConverter, String fieldName, JsonNode field) {
        if (field == null) {
            if (failOnMissingField) {
                throw new SeaTunnelJsonFormatException(CommonErrorCode.JSON_OPERATION_FAILED,
                        String.format("Could not find field with name %s .", fieldName));
            } else {
                return null;
            }
        } else {
            return fieldConverter.convert(field);
        }
    }

    private JsonToRowConverter wrapIntoNullableConverter(JsonToRowConverter converter) {
        return new JsonToRowConverter() {
            @Override
            public Object convert(JsonNode jsonNode) {
                if (jsonNode == null || jsonNode.isNull() || jsonNode.isMissingNode()) {
                    return null;
                }
                try {
                    return converter.convert(jsonNode);
                } catch (Throwable t) {
                    if (!ignoreParseErrors) {
                        throw t;
                    }
                    return null;
                }
            }
        };
    }

    /**
     * Runtime converter that converts {@link JsonNode}s into objects of internal
     * data structures.
     */
    @FunctionalInterface
    public interface JsonToRowConverter extends Serializable {
        Object convert(JsonNode jsonNode);
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
