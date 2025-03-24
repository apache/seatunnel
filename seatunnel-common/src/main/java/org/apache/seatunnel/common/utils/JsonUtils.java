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

package org.apache.seatunnel.common.utils;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonSerializer;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeType;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.TextNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.type.CollectionType;
import org.apache.seatunnel.shade.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT;
import static org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL;
import static org.apache.seatunnel.shade.com.fasterxml.jackson.databind.MapperFeature.REQUIRE_SETTERS_FOR_GETTERS;

public class JsonUtils {

    /** can use static singleton, inject: just make sure to reuse! */
    private static final ObjectMapper OBJECT_MAPPER =
            new ObjectMapper()
                    .configure(FAIL_ON_UNKNOWN_PROPERTIES, false)
                    .configure(ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT, true)
                    .configure(READ_UNKNOWN_ENUM_VALUES_AS_NULL, true)
                    .configure(REQUIRE_SETTERS_FOR_GETTERS, true)
                    .setTimeZone(TimeZone.getDefault())
                    // support java8 time api
                    .registerModule(new JavaTimeModule());

    private static final ObjectMapper DEFAULT_OBJECT_MAPPER = new ObjectMapper();

    private JsonUtils() {
        throw new UnsupportedOperationException("Construct JSONUtils");
    }

    public static ArrayNode createArrayNode() {
        return OBJECT_MAPPER.createArrayNode();
    }

    public static ObjectNode createObjectNode() {
        return OBJECT_MAPPER.createObjectNode();
    }

    public static JsonNode toJsonNode(Object obj) {
        return OBJECT_MAPPER.valueToTree(obj);
    }

    public static JsonNode stringToJsonNode(String obj) throws JsonProcessingException {
        return OBJECT_MAPPER.readTree(obj);
    }

    public static JsonNode readTree(byte[] obj) throws IOException {
        return OBJECT_MAPPER.readTree(obj);
    }

    /**
     * json representation of object
     *
     * @param object object
     * @param feature feature
     * @return object to json string
     */
    public static String toJsonString(Object object, SerializationFeature feature) {
        try {
            ObjectWriter writer = OBJECT_MAPPER.writer(feature);
            return writer.writeValueAsString(object);
        } catch (Exception e) {
            throw new RuntimeException("Object to json exception!", e);
        }
    }

    /**
     * This method deserializes the specified Json into an object of the specified class. It is not
     * suitable to use if the specified class is a generic type since it will not have the generic
     * type information because of the Type Erasure feature of Java. Therefore, this method should
     * not be used if the desired type is a generic type. Note that this method works fine if the
     * any of the fields of the specified object are generics, just the object itself should not be
     * a generic type.
     *
     * @param json the string from which the object is to be deserialized
     * @param clazz the class of T
     * @param <T> T
     * @return an object of type T from the string classOfT
     */
    public static <T> T parseObject(String json, Class<T> clazz) {
        if (StringUtils.isEmpty(json)) {
            return null;
        }

        try {
            return OBJECT_MAPPER.readValue(json, clazz);
        } catch (Exception e) {
            throw new RuntimeException("Json parse object exception!", e);
        }
    }

    /**
     * json to list
     *
     * @param json json string
     * @param clazz class
     * @param <T> T
     * @return list
     */
    public static <T> List<T> toList(String json, Class<T> clazz) {
        if (StringUtils.isEmpty(json)) {
            return Collections.emptyList();
        }

        try {
            CollectionType listType =
                    OBJECT_MAPPER.getTypeFactory().constructCollectionType(ArrayList.class, clazz);
            return OBJECT_MAPPER.readValue(json, listType);
        } catch (Exception e) {
            throw new RuntimeException("Json parse list exception!", e);
        }
    }

    /**
     * Method for finding a JSON Object field with specified name in this node or its child nodes,
     * and returning value it has. If no matching field is found in this node or its descendants,
     * returns null.
     *
     * @param jsonNode json node
     * @param fieldName Name of field to look for
     * @return Value of first matching node found, if any; null if none
     */
    public static String findValue(JsonNode jsonNode, String fieldName) {
        JsonNode node = jsonNode.findValue(fieldName);

        if (node == null) {
            return null;
        }

        return node.asText();
    }

    /**
     * json to map {@link #toMap(String, Class, Class)}
     *
     * @param json json
     * @return json to map
     */
    public static Map<String, String> toMap(String json) {
        return parseObject(json, new TypeReference<Map<String, String>>() {});
    }

    public static Map<String, Object> toMap(JsonNode jsonNode) {
        return DEFAULT_OBJECT_MAPPER.convertValue(
                jsonNode, new TypeReference<Map<String, Object>>() {});
    }

    public static Map<String, String> toStringMap(JsonNode jsonNode) {
        Map<String, String> fieldsMap = new LinkedHashMap<>();
        jsonNode.fields()
                .forEachRemaining(
                        field -> {
                            String key = field.getKey();
                            JsonNode value = field.getValue();
                            if (value.getNodeType() == JsonNodeType.OBJECT) {
                                fieldsMap.put(key, value.toString());
                            } else {
                                fieldsMap.put(key, value.textValue());
                            }
                        });
        return fieldsMap;
    }

    /**
     * json to map
     *
     * @param json json
     * @param classK classK
     * @param classV classV
     * @param <K> K
     * @param <V> V
     * @return to map
     */
    public static <K, V> Map<K, V> toMap(String json, Class<K> classK, Class<V> classV) {
        if (StringUtils.isEmpty(json)) {
            return Collections.emptyMap();
        }

        try {
            return OBJECT_MAPPER.readValue(json, new TypeReference<Map<K, V>>() {});
        } catch (Exception e) {
            throw new RuntimeException("json to map exception!", e);
        }
    }

    /**
     * json to object
     *
     * @param json json string
     * @param type type reference
     * @param <T> type
     * @return return parse object
     */
    public static <T> T parseObject(String json, TypeReference<T> type) {
        if (StringUtils.isEmpty(json)) {
            return null;
        }

        try {
            return OBJECT_MAPPER.readValue(json, type);
        } catch (Exception e) {
            throw new RuntimeException("Json parse object exception.", e);
        }
    }

    /**
     * object to json string
     *
     * @param object object
     * @return json string
     */
    public static String toJsonString(Object object) {
        try {
            return OBJECT_MAPPER.writeValueAsString(object);
        } catch (Exception e) {
            throw new RuntimeException("Object json deserialization exception.", e);
        }
    }

    public static ObjectNode parseObject(String text) {
        return parseObject(text.getBytes());
    }

    public static ObjectNode parseObject(byte[] content) {
        try {
            return (ObjectNode) OBJECT_MAPPER.readTree(content);
        } catch (IOException e) {
            throw new RuntimeException(
                    "String json deserialization exception." + new String(content), e);
        }
    }

    public static ArrayNode parseArray(String text) {
        try {
            return (ArrayNode) OBJECT_MAPPER.readTree(text);
        } catch (Exception e) {
            throw new RuntimeException("Json deserialization exception.", e);
        }
    }

    /** json serializer */
    public static class JsonDataSerializer extends JsonSerializer<String> {

        @Override
        public void serialize(String value, JsonGenerator gen, SerializerProvider provider)
                throws IOException {
            gen.writeRawValue(value);
        }
    }

    /** json data deserializer */
    public static class JsonDataDeserializer extends JsonDeserializer<String> {

        @Override
        public String deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            JsonNode node = p.getCodec().readTree(p);
            if (node instanceof TextNode) {
                return node.asText();
            } else {
                return node.toString();
            }
        }
    }

    public static boolean isJsonArray(String jsonString) {
        try {
            JsonNode jsonNode = OBJECT_MAPPER.readTree(jsonString);
            return jsonNode.isArray();
        } catch (Exception e) {
            return false;
        }
    }
}
