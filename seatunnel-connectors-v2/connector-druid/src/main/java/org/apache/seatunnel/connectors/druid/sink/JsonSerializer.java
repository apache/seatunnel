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

package org.apache.seatunnel.connectors.druid.sink;

import org.apache.commons.lang.StringUtils;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.databind.type.MapType;
import com.google.common.collect.Lists;
import lombok.Getter;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class JsonSerializer implements Serializable {

    private static final long serialVersionUID = -7498603065258697303L;
    @Getter private static ObjectMapper mapper;

    static {
        mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    public static <T> T deserialize(byte[] bytes, Class<T> clazz) {
        try {
            return mapper.readValue(bytes, clazz);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static String serialize(Object object) {
        try {
            return mapper.writeValueAsString(object);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static byte[] serializeAsBytes(Object object) {
        try {
            return mapper.writeValueAsBytes(object);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static <T> List<T> parseToList(String str, Class<T> clazz) {
        if (StringUtils.isEmpty(str)) {
            return Lists.newArrayList();
        }
        try {
            CollectionType collectionType =
                    mapper.getTypeFactory().constructCollectionType(List.class, clazz);
            return mapper.readValue(str, collectionType);
        } catch (Exception e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    public static <K, V> List<Map<K, V>> parseToListMap(
            String str, Class<K> keyClass, Class<V> valueClass) {
        try {
            CollectionType collectionType = getCollectorType(keyClass, valueClass);
            return mapper.readValue(str, collectionType);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static Map<String, String> parseToMap(String str) {
        try {
            return mapper.readValue(str, Map.class);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static Map<String, Object> parseToMapObject(Object object) {
        try {
            return mapper.convertValue(object, new TypeReference<Map<String, Object>>() {});
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static JsonNode parse(String str) {
        try {
            return mapper.readTree(str);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static <K, V> CollectionType getCollectorType(Class<K> keyClass, Class<V> valueClass) {
        MapType mapType = mapper.getTypeFactory().constructMapType(Map.class, keyClass, valueClass);
        return mapper.getTypeFactory().constructCollectionType(List.class, mapType);
    }
}
