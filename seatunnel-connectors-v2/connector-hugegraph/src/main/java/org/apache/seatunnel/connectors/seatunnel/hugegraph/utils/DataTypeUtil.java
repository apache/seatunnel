/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.hugegraph.utils;

import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.ListFormat;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorException;

import org.apache.hugegraph.structure.constant.Cardinality;
import org.apache.hugegraph.structure.constant.DataType;
import org.apache.hugegraph.structure.schema.PropertyKey;

import java.lang.reflect.Array;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public final class DataTypeUtil {

    private static final Set<String> ACCEPTABLE_TRUE;

    static {
        ACCEPTABLE_TRUE = new HashSet<>();
        ACCEPTABLE_TRUE.add("true");
        ACCEPTABLE_TRUE.add("1");
        ACCEPTABLE_TRUE.add("yes");
        ACCEPTABLE_TRUE.add("y");
    }

    private static final Set<String> ACCEPTABLE_FALSE;

    static {
        ACCEPTABLE_FALSE = new HashSet<>();
        ACCEPTABLE_FALSE.add("false");
        ACCEPTABLE_FALSE.add("0");
        ACCEPTABLE_FALSE.add("no");
        ACCEPTABLE_FALSE.add("n");
    }

    public static Object convert(
            Object value, PropertyKey propertyKey, String dateFormat, String timeZone) {
        return convert(
                value,
                propertyKey,
                dateFormat,
                timeZone,
                Collections.emptyList(),
                new ListFormat());
    }

    public static Object convert(
            Object value,
            PropertyKey propertyKey,
            String dateFormat,
            String timeZone,
            ListFormat listFormat) {
        return convert(
                value, propertyKey, dateFormat, timeZone, Collections.emptyList(), listFormat);
    }

    public static Object convert(
            Object value,
            PropertyKey propertyKey,
            String dateFormat,
            String timeZone,
            List<String> extraDateFormats,
            ListFormat listFormat) {
        E.checkArgumentNotNull(value, "The value to be converted can't be null");

        String key = propertyKey.name();
        DataType dataType = propertyKey.dataType();
        Cardinality cardinality = propertyKey.cardinality();
        switch (cardinality) {
            case SINGLE:
                return parseSingleValue(
                        key, value, dataType, dateFormat, timeZone, extraDateFormats);
            case SET:
            case LIST:
                return parseMultiValues(
                        key,
                        value,
                        dataType,
                        cardinality,
                        dateFormat,
                        timeZone,
                        extraDateFormats,
                        listFormat);
            default:
                throw new HugeGraphConnectorException(
                        HugeGraphConnectorErrorCode.INVALID_GRAPH_SCHEMA,
                        String.format("Unsupported cardinality: '%s'", cardinality));
        }
    }

    /**
     * collection format: "obj1,obj2,...,obj_n" or "[obj1,obj2,...,obj_n]" ..etc TODO: After parsing
     * to json, the order of the collection changed in some cases (such as list<date>)
     */
    private static Object parseMultiValues(
            String key,
            Object values,
            DataType dataType,
            Cardinality cardinality,
            String dateFormat,
            String timeZone,
            List<String> extraDateFormats,
            ListFormat listFormat) {
        Collection<?> sourceValues;
        if (values instanceof Collection) {
            sourceValues = (Collection<?>) values;
        } else if (values.getClass().isArray()) {
            List<Object> arrayValues = new ArrayList<>(Array.getLength(values));
            for (int i = 0; i < Array.getLength(values); i++) {
                arrayValues.add(Array.get(values, i));
            }
            sourceValues = arrayValues;
        } else {
            E.checkState(
                    values instanceof String,
                    "The value(key='%s') must be a Collection, array, or String type, "
                            + "but got '%s'(%s)",
                    key,
                    values,
                    values.getClass());
            sourceValues = split(key, (String) values, listFormat);
        }

        Collection<Object> results =
                cardinality == Cardinality.LIST ? new ArrayList<>() : new LinkedHashSet<>();
        sourceValues.forEach(
                value -> {
                    results.add(
                            parseSingleValue(
                                    key, value, dataType, dateFormat, timeZone, extraDateFormats));
                });
        E.checkArgument(
                checkCollectionDataType(key, results, dataType),
                "Not all collection elems %s match with data type %s",
                results,
                dataType);
        return results;
    }

    @SuppressWarnings("unchecked")
    public static List<Object> splitField(String key, Object rawColumnValue) {
        E.checkArgument(rawColumnValue != null, "The value to be split can't be null");
        if (rawColumnValue instanceof Collection) {
            Collection<?> collection = (Collection<?>) rawColumnValue;
            return new ArrayList<>(collection);
        }
        if (rawColumnValue.getClass().isArray()) {
            List<Object> values = new ArrayList<>(Array.getLength(rawColumnValue));
            for (int i = 0; i < Array.getLength(rawColumnValue); i++) {
                values.add(Array.get(rawColumnValue, i));
            }
            return values;
        }
        String rawValue = rawColumnValue.toString();
        return split(key, rawValue, new ListFormat());
    }

    public static UUID parseUUID(String key, Object rawValue) {
        if (rawValue instanceof UUID) {
            return (UUID) rawValue;
        } else if (rawValue instanceof String) {
            String value = ((String) rawValue).trim();
            if (value.contains("-")) {
                return UUID.fromString(value);
            }
            // UUID represented by hex string
            E.checkArgument(value.length() == 32, "Invalid UUID value(key='%s') '%s'", key, value);
            String high = value.substring(0, 16);
            String low = value.substring(16);
            return new UUID(Long.parseUnsignedLong(high, 16), Long.parseUnsignedLong(low, 16));
        }
        throw new HugeGraphConnectorException(
                HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                String.format(
                        "Failed to convert value(key='%s') " + "'%s'(%s) to UUID",
                        key, rawValue, rawValue.getClass()));
    }

    private static Object parseSingleValue(
            String key,
            Object rawValue,
            DataType dataType,
            String dateFormat,
            String timeZone,
            List<String> extraDateFormats) {
        Object value = trimString(rawValue);
        if (value == null) {
            return null;
        }

        if (dataType.isNumber()) {
            return parseNumber(key, value, dataType);
        }

        switch (dataType) {
            case TEXT:
                return value.toString();
            case BOOLEAN:
                return parseBoolean(key, value);
            case DATE:
                return parseDate(key, value, dateFormat, timeZone, extraDateFormats);
            case UUID:
                return parseUUID(key, value);
            default:
                E.checkArgument(
                        checkDataType(key, value, dataType),
                        "The value(key='%s') '%s'(%s) is not match with data type %s and "
                                + "can't convert to it",
                        key,
                        value,
                        value.getClass(),
                        dataType);
        }
        return value;
    }

    private static Object trimString(Object rawValue) {
        if (rawValue instanceof String) {
            return ((String) rawValue).trim();
        }
        return rawValue;
    }

    private static Boolean parseBoolean(String key, Object rawValue) {
        if (rawValue instanceof Boolean) {
            return (Boolean) rawValue;
        }
        if (rawValue instanceof String) {
            String value = ((String) rawValue).toLowerCase();
            if (ACCEPTABLE_TRUE.contains(value)) {
                return true;
            } else if (ACCEPTABLE_FALSE.contains(value)) {
                return false;
            } else {
                throw new HugeGraphConnectorException(
                        HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                        String.format(
                                "Failed to convert '%s'(key='%s') to Boolean, "
                                        + "the acceptable boolean strings are %s or %s",
                                key, rawValue, ACCEPTABLE_TRUE, ACCEPTABLE_FALSE));
            }
        }
        throw new HugeGraphConnectorException(
                HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                String.format(
                        "Failed to convert value(key='%s') " + "'%s'(%s) to Boolean",
                        key, rawValue, rawValue.getClass()));
    }

    private static Number parseNumber(String key, Object value, DataType dataType) {
        E.checkState(dataType.isNumber(), "The target data type must be number");
        try {
            switch (dataType) {
                case BYTE:
                    return Byte.parseByte(value.toString());
                case INT:
                    return Integer.parseInt(value.toString());
                case LONG:
                    return parseLong(value.toString());
                case FLOAT:
                    return Float.parseFloat(value.toString());
                case DOUBLE:
                    return Double.parseDouble(value.toString());
                default:
                    throw new HugeGraphConnectorException(
                            HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                            String.format(
                                    "Number type only contains Byte, "
                                            + "Integer, Long, Float, Double, "
                                            + "but got %s",
                                    dataType.clazz()));
            }
        } catch (NumberFormatException e) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                    String.format(
                            "Failed to convert value(key=%s) " + "'%s'(%s) to Number",
                            key, value, value.getClass()),
                    e);
        }
    }

    private static long parseLong(String rawValue) {
        if (rawValue.startsWith("-")) {
            return Long.parseLong(rawValue);
        } else {
            return Long.parseUnsignedLong(rawValue);
        }
    }

    private static Date parseDate(String key, Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Date) {
            return (Date) value;
        }

        if (value instanceof LocalDateTime) {
            return Date.from(((LocalDateTime) value).atZone(ZoneId.systemDefault()).toInstant());
        }

        if (value instanceof java.time.LocalDate) {
            return Date.from(
                    ((java.time.LocalDate) value).atStartOfDay(ZoneId.systemDefault()).toInstant());
        }

        if (value instanceof Number) {
            return new Date(((Number) value).longValue());
        }

        if (value instanceof String) {
            String s = ((String) value).trim();
            if (s.isEmpty()) {
                return null;
            }
            // 1. Try to parse as long timestamp
            try {
                return new Date(Long.parseLong(s));
            } catch (NumberFormatException e) {
                // Not a timestamp, proceed to parse as date string
            }

            try {
                return org.apache.hugegraph.util.DateUtil.parse(s);
            } catch (Exception e) {
                throw new HugeGraphConnectorException(
                        HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                        String.format(
                                "Failed to convert string value(key='%s') '%s' to Date "
                                        + "using HugeGraph DateUtil.",
                                key, value),
                        e);
            }
        }
        throw new HugeGraphConnectorException(
                HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                String.format(
                        "Failed to convert value(key='%s') " + "'%s'(%s) to Date",
                        key, value, value.getClass()));
    }

    private static Date parseDate(
            String key,
            Object value,
            String dateFormat,
            String timeZone,
            List<String> extraDateFormats) {
        if (value instanceof Date) {
            return (Date) value;
        }

        ZoneId zoneId;
        try {
            if (timeZone != null && !timeZone.isEmpty()) {
                zoneId = ZoneId.of(timeZone);
            } else {
                zoneId = ZoneId.systemDefault();
            }
        } catch (Exception e) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                    String.format("Invalid timeZone string provided: '%s'", timeZone),
                    e);
        }

        if (value instanceof LocalDateTime) {
            return Date.from(((LocalDateTime) value).atZone(zoneId).toInstant());
        }

        if (value instanceof java.time.LocalDate) {
            return Date.from(((java.time.LocalDate) value).atStartOfDay(zoneId).toInstant());
        }

        if (value instanceof Number) {
            return new Date(((Number) value).longValue());

        } else if (value instanceof String) {
            String strValue = ((String) value).trim();
            if ("timestamp".equals(dateFormat)) {
                try {
                    return new Date(Long.parseLong(strValue));
                } catch (NumberFormatException e) {
                    throw new HugeGraphConnectorException(
                            HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                            String.format("Invalid timestamp value '%s'", value),
                            e);
                }
            }

            // Candidate patterns, primary first then the extras — tried in order (deterministic,
            // unlike the loader which uses a HashSet), first successful parse wins.
            List<String> formats = new ArrayList<>();
            if (dateFormat != null && !dateFormat.isEmpty()) {
                formats.add(dateFormat);
            }
            if (extraDateFormats != null) {
                for (String extra : extraDateFormats) {
                    if (extra != null && !extra.isEmpty()) {
                        formats.add(extra);
                    }
                }
            }

            if (formats.isEmpty()) {
                // Fallback for when no format is provided.
                try {
                    return new Date(Long.parseLong(strValue));
                } catch (NumberFormatException e) {
                    throw new HugeGraphConnectorException(
                            HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                            "Date format must be provided to parse a date string that is not a timestamp.",
                            e);
                }
            }

            Exception lastFailure = null;
            for (String format : formats) {
                try {
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
                    LocalDateTime ldt;
                    try {
                        ldt = LocalDateTime.parse(strValue, formatter);
                    } catch (java.time.format.DateTimeParseException dateTimeFailure) {
                        ldt = LocalDate.parse(strValue, formatter).atStartOfDay();
                    }
                    return Date.from(ldt.atZone(zoneId).toInstant());
                } catch (Exception e) {
                    lastFailure = e;
                }
            }
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                    String.format(
                            "Failed to parse date string '%s' with any of the formats %s",
                            value, formats),
                    lastFailure);
        }
        throw new HugeGraphConnectorException(
                HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                String.format(
                        "Failed to convert value(key='%s') " + "'%s'(%s) to Date",
                        key, value, value.getClass()));
    }

    private static List<Object> split(String key, String rawValue, ListFormat listFormat) {
        List<Object> valueColl = new ArrayList<>();
        if (rawValue == null || rawValue.isEmpty()) {
            return valueColl;
        }

        String value = rawValue.trim();
        String startSymbol = listFormat.getStartSymbol();
        String endSymbol = listFormat.getEndSymbol();
        if (startSymbol != null
                && !startSymbol.isEmpty()
                && endSymbol != null
                && !endSymbol.isEmpty()
                && value.startsWith(startSymbol)
                && value.endsWith(endSymbol)) {
            value = value.substring(startSymbol.length(), value.length() - endSymbol.length());
        }

        Set<String> ignoredElems = new HashSet<>(listFormat.getIgnoredElems());
        com.google.common.base.Splitter.on(listFormat.getElemDelimiter())
                .trimResults()
                .omitEmptyStrings()
                .split(value)
                .forEach(
                        elem -> {
                            if (!ignoredElems.contains(elem)) {
                                valueColl.add(elem);
                            }
                        });
        return valueColl;
    }

    /** Check the type of the value valid */
    private static boolean checkDataType(String key, Object value, DataType dataType) {
        if (value instanceof Number && dataType.isNumber()) {
            return parseNumber(key, value, dataType) != null;
        }
        return dataType.clazz().isInstance(value);
    }

    /** Check the type of all the values (maybe some list properties) valid */
    private static boolean checkCollectionDataType(
            String key, Collection<?> values, DataType dataType) {
        for (Object value : values) {
            if (!checkDataType(key, value, dataType)) {
                return false;
            }
        }
        return true;
    }
}
