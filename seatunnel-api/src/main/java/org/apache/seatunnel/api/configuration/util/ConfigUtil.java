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

    @SuppressWarnings("unchecked")
    static Object flatteningMap(Object rawValue, Map<String, Object> newMap, List<String> keys, boolean nestedMap) {
        if (rawValue == null) {
            return null;
        }
        if (!(rawValue instanceof List) && !(rawValue instanceof Map)) {
            if (newMap == null) {
                return rawValue;
            }
            newMap.put(String.join(".", keys), rawValue);
            return newMap;
        }

        if (rawValue instanceof List) {
            List<Object> rawList = (List<Object>) rawValue;
            rawList.replaceAll(value -> flatteningMap(value, null, null, false));
            if (newMap != null) {
                newMap.put(String.join(".", keys), rawList);
                return newMap;
            }
            return rawList;
        } else {
            Map<String, Object> rawMap = (Map<String, Object>) rawValue;
            if (!nestedMap) {
                keys = new ArrayList<>();
                newMap = new HashMap<>(rawMap.size());
            }
            for (Map.Entry<String, Object> entry : rawMap.entrySet()) {
                keys.add(entry.getKey());
                flatteningMap(entry.getValue(), newMap, keys, true);
                keys.remove(keys.size() - 1);
            }
            return newMap;
        }
    }

    /**
     * <pre>
     *                                                  poll.timeout = 1000
     * poll : {timeout = 1000, interval = 500}  ==>>
     *                                                  poll.interval = 500
     * </pre>
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> flatteningMap(Map<String, Object> treeMap) {
        return (Map<String, Object>) flatteningMapWithObject(treeMap);
    }

    static Object flatteningMapWithObject(Object rawValue) {
        return flatteningMap(rawValue, null, null, false);
    }

    @SuppressWarnings("unchecked")
    public static <T> T convertValue(Object rawValue, TypeReference<T> typeReference) {
        rawValue = flatteningMapWithObject(rawValue);
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
