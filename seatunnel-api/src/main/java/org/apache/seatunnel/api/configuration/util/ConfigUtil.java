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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsMapper;

import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class ConfigUtil {
    private static final JavaPropsMapper PROPERTIES_MAPPER = new JavaPropsMapper();
    private static final ObjectMapper JACKSON_MAPPER = new ObjectMapper();

    /**
     * <pre>
     * poll.timeout = 1000
     *                      ==>>  poll : {timeout = 1000, interval = 500}
     * poll.interval = 500
     * </pre>
     */
    public static Map<String, Object> treeMap(Object rawMap) {
        try {
            return PROPERTIES_MAPPER.readValue(PROPERTIES_MAPPER.writeValueAsString(rawMap), new TypeReference<Map<String, Object>>() {
            });
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Json parsing exception.");
        }
    }

    /**
     * <pre>
     *                                                  poll.timeout = 1000
     * poll : {timeout = 1000, interval = 500}  ==>>
     *                                                  poll.interval = 500
     * </pre>
     */
    public static Map<String, Object> flatteningMap(Map<String, Object> treeMap) {
        Map<String, Object> flatteningMap = new HashMap<>();
        List<String> keys = new ArrayList<>();
        backtracking(flatteningMap, treeMap, keys);
        return flatteningMap;
    }

    @SuppressWarnings("unchecked")
    private static void backtracking(Map<String, Object> flatteningMap, Object rawValue, List<String> keys) {
        if (rawValue == null) {
            return;
        }
        if (!(rawValue instanceof Map)) {
            flatteningMap.put(String.join(".", keys), rawValue);
            return;
        }
        ((Map<String, Object>) rawValue).forEach((key, value) -> {
            keys.add(key);
            backtracking(flatteningMap, value, keys);
            keys.remove(keys.size() - 1);
        });
    }

    @SuppressWarnings("unchecked")
    public static <T> T convertValue(Object rawValue, TypeReference<T> typeReference) {
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
        } else {
            // complex type
            ParameterizedType type = (ParameterizedType) typeReference.getType();
            if (Map.class.equals(type.getRawType()) && rawValue instanceof Map) {
                rawValue = ConfigUtil.flatteningMap(((Map<String, Object>) rawValue));
            }
        }
        try {
            String str = rawValue instanceof String ? ((String) rawValue) : JACKSON_MAPPER.writeValueAsString(rawValue);
            return JACKSON_MAPPER.readValue(str, typeReference);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(String.format("Json parsing exception, value '%s', and expected type '%s'", rawValue, typeReference.getType().getTypeName()), e);
        }
    }

    @SuppressWarnings("unchecked")
    static <T> T convertValue(Object rawValue, Class<T> clazz) {
        if (Boolean.class.equals(clazz)) {
            return (T) convertToBoolean(rawValue);
        } else if (clazz.isEnum()) {
            return (T) convertToEnum(rawValue, (Class<? extends Enum<?>>) clazz);
        } else if (String.class.equals(clazz)) {
            return (T) convertToJsonString(rawValue);
        }
        throw new IllegalArgumentException("Unsupported type: " + clazz);
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
            .filter(e -> e.toString()
                .toUpperCase(Locale.ROOT)
                .equals(o.toString().toUpperCase(Locale.ROOT)))
            .findAny()
            .orElseThrow(() -> new IllegalArgumentException(String.format(
                "Could not parse value for enum %s. Expected one of: [%s]",
                clazz, Arrays.toString(clazz.getEnumConstants()))));
    }

    public static String convertToJsonString(Object o) {
        if (o instanceof String) {
            return (String) o;
        }
        try {
            return JACKSON_MAPPER.writeValueAsString(o);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(String.format("Could not parse json, value: %s", o));
        }
    }
}
