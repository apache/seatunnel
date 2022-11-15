/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.seatunnel.engine.imap.storage.api.common;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Todo: move to common module
 */
@Slf4j
public class ProtoStuffSerializer implements Serializer {

    /**
     * At the moment it looks like we only have one Schema.
     */
    private static final Map<Class<?>, Schema<?>> SCHEMA_CACHE = new ConcurrentHashMap<>();

    @SuppressWarnings("unchecked")
    private static <T> Schema<T> getSchema(Class<T> clazz) {
        return (Schema<T>) SCHEMA_CACHE.computeIfAbsent(clazz, RuntimeSchema::createFrom);
    }

    private static final Set<Class<?>> WRAPPERS = new HashSet<>();

    private static final Class<SerializerDeserializerWrapper> WRAPPER_CLASS = SerializerDeserializerWrapper.class;

    private static final Schema<SerializerDeserializerWrapper> WRAPPER_SCHEMA = getSchema(WRAPPER_CLASS);

    static {
        WRAPPERS.add(Boolean.class);
        WRAPPERS.add(Byte.class);
        WRAPPERS.add(Character.class);
        WRAPPERS.add(Short.class);
        WRAPPERS.add(Integer.class);
        WRAPPERS.add(Long.class);
        WRAPPERS.add(Float.class);
        WRAPPERS.add(Double.class);
        WRAPPERS.add(String.class);
        WRAPPERS.add(Void.class);
        WRAPPERS.add(List.class);
        WRAPPERS.add(ArrayList.class);
        WRAPPERS.add(Map.class);
        WRAPPERS.add(HashMap.class);
        WRAPPERS.add(TreeMap.class);
        WRAPPERS.add(Hashtable.class);
        WRAPPERS.add(SortedMap.class);
    }

    @Override
    public <T> byte[] serialize(T obj) {
        Class<T> clazz = (Class<T>) obj.getClass();
        LinkedBuffer buffer = LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE);
        Schema schema = WRAPPER_SCHEMA;
        if (WRAPPERS.contains(clazz)) {
            obj = (T) SerializerDeserializerWrapper.of(obj);
        } else {
            schema = getSchema(clazz);
        }

        byte[] data;
        try {
            data = ProtostuffIOUtil.toByteArray(obj, schema, buffer);
        } finally {
            buffer.clear();
        }
        return data;
    }

    @Override
    public <T> T deserialize(byte[] data, Class<T> clz) {

        if (!WRAPPERS.contains(clz)) {
            Schema<T> schema = getSchema(clz);
            T message = schema.newMessage();
            ProtostuffIOUtil.mergeFrom(data, message, schema);
            return message;
        }
        SerializerDeserializerWrapper<T> wrapper = new SerializerDeserializerWrapper<>();
        ProtostuffIOUtil.mergeFrom(data, wrapper, WRAPPER_SCHEMA);
        return wrapper.getObj();
    }

    public static class SerializerDeserializerWrapper<T> {
        private T obj;

        public static <T> SerializerDeserializerWrapper<T> of(T obj) {
            SerializerDeserializerWrapper<T> wrapper = new SerializerDeserializerWrapper<>();
            wrapper.setObj(obj);
            return wrapper;
        }

        public T getObj() {
            return obj;
        }

        public void setObj(T obj) {
            this.obj = obj;
        }
    }

}
