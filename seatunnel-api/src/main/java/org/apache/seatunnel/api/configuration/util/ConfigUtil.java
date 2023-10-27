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

package org.apache.seatunnel.api.configuration.util;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.configuration.Option;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.ParameterizedType;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

@Slf4j
public class ConfigUtil {

    private static final ObjectMapper JACKSON_MAPPER = new ObjectMapper();

    @SuppressWarnings("unchecked")
    public static <T> T convertValue(Object rawValue, Option<T> option) {
        TypeReference<T> typeReference = option.typeReference();
        if (typeReference.getType() instanceof Class) {
            // simple type
            Class<T> clazz = (Class<T>) typeReference.getType();
            if (clazz.equals(rawValue.getClass())) {
                return (T) rawValue;
            }
            try {
                return convertValue(rawValue, clazz);
            } catch (IllegalArgumentException e) {
                // Continue with Jackson parsing
            }
        }
        try {
            // complex type && untreated type
            return JACKSON_MAPPER.readValue(convertToJsonString(rawValue), typeReference);
        } catch (JsonProcessingException e) {
            if (typeReference.getType() instanceof ParameterizedType
                    && List.class.equals(
                            ((ParameterizedType) typeReference.getType()).getRawType())) {
                try {
                    log.warn(
                            "Option '{}' is a List, and it is recommended to configure it as [\"string1\",\"string2\"]; we will only use ',' to split the String into a list.",
                            option.key());
                    return (T)
                            convertToList(
                                    rawValue,
                                    (Class<T>)
                                            ((ParameterizedType) typeReference.getType())
                                                    .getActualTypeArguments()[0]);
                } catch (Exception ignore) {
                    // nothing
                }
            }
            throw new IllegalArgumentException(
                    String.format(
                            "Json parsing exception, value '%s', and expected type '%s'",
                            rawValue, typeReference.getType().getTypeName()),
                    e);
        }
    }

    static <T> List<T> convertToList(Object rawValue, Class<T> clazz) {
        if (rawValue instanceof List) {
            return ((List<?>) rawValue)
                    .stream()
                            .map(value -> convertValue(convertToJsonString(value), clazz))
                            .collect(Collectors.toList());
        }
        return Arrays.stream(rawValue.toString().split(","))
                .map(String::trim)
                .map(value -> convertValue(value, clazz))
                .collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    static <T> T convertValue(Object rawValue, Class<T> clazz) {
        if (Boolean.class.equals(clazz)) {
            return (T) convertToBoolean(rawValue);
        } else if (clazz.isEnum()) {
            return (T) convertToEnum(rawValue, (Class<? extends Enum<?>>) clazz);
        } else if (String.class.equals(clazz)) {
            return (T) convertToJsonString(rawValue);
        } else if (Integer.class.equals(clazz)) {
            return (T) convertToInt(rawValue);
        } else if (Long.class.equals(clazz)) {
            return (T) convertToLong(rawValue);
        } else if (Float.class.equals(clazz)) {
            return (T) convertToFloat(rawValue);
        } else if (Double.class.equals(clazz)) {
            return (T) convertToDouble(rawValue);
        }
        throw new IllegalArgumentException("Unsupported type: " + clazz);
    }

    static Integer convertToInt(Object o) {
        if (o.getClass() == Integer.class) {
            return (Integer) o;
        } else if (o.getClass() == Long.class) {
            long value = (Long) o;
            if (value <= Integer.MAX_VALUE && value >= Integer.MIN_VALUE) {
                return (int) value;
            } else {
                throw new IllegalArgumentException(
                        String.format(
                                "Configuration value %s overflows/underflows the integer type.",
                                value));
            }
        }

        return Integer.parseInt(o.toString());
    }

    static Long convertToLong(Object o) {
        if (o.getClass() == Long.class) {
            return (Long) o;
        } else if (o.getClass() == Integer.class) {
            return ((Integer) o).longValue();
        }

        return Long.parseLong(o.toString());
    }

    static Float convertToFloat(Object o) {
        if (o.getClass() == Float.class) {
            return (Float) o;
        } else if (o.getClass() == Double.class) {
            double value = ((Double) o);
            if (value == 0.0
                    || (value >= Float.MIN_VALUE && value <= Float.MAX_VALUE)
                    || (value >= -Float.MAX_VALUE && value <= -Float.MIN_VALUE)) {
                return (float) value;
            } else {
                throw new IllegalArgumentException(
                        String.format(
                                "Configuration value %s overflows/underflows the float type.",
                                value));
            }
        }

        return Float.parseFloat(o.toString());
    }

    static Double convertToDouble(Object o) {
        if (o.getClass() == Double.class) {
            return (Double) o;
        } else if (o.getClass() == Float.class) {
            return ((Float) o).doubleValue();
        }

        return Double.parseDouble(o.toString());
    }

    static Boolean convertToBoolean(Object o) {
        switch (o.toString().toUpperCase()) {
            case "TRUE":
                return true;
            case "FALSE":
                return false;
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "Unrecognized option for boolean: %s. Expected either true or false(case insensitive)",
                                o));
        }
    }

    static <E extends Enum<?>> E convertToEnum(Object o, Class<E> clazz) {
        return Arrays.stream(clazz.getEnumConstants())
                .filter(
                        e ->
                                e.toString()
                                        .toUpperCase(Locale.ROOT)
                                        .equals(o.toString().toUpperCase(Locale.ROOT)))
                .findAny()
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        String.format(
                                                "Could not parse value for enum %s. Expected one of: [%s]",
                                                clazz, Arrays.toString(clazz.getEnumConstants()))));
    }

    public static String convertToJsonString(Object o) {
        if (o == null) {
            return null;
        }
        if (o instanceof String) {
            return (String) o;
        }
        try {
            return JACKSON_MAPPER.writeValueAsString(o);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(String.format("Could not parse json, value: %s", o));
        }
    }

    public static String convertToJsonString(Config config) {
        return convertToJsonString(config.root().unwrapped());
    }

    public static Config convertToConfig(String configJson) {
        return ConfigFactory.parseString(configJson);
    }
}
