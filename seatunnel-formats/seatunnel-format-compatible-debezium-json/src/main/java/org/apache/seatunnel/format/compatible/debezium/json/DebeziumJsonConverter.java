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

package org.apache.seatunnel.format.compatible.debezium.json;

import org.apache.seatunnel.common.utils.ReflectionUtils;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.DecimalFormat;
import org.apache.kafka.connect.source.SourceRecord;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Converts a Debezium {@link SourceRecord} to the JSON representation used by the
 * COMPATIBLE_DEBEZIUM_JSON format, delegating the Struct→JSON conversion to Kafka Connect's local
 * {@link JsonConverter}.
 *
 * <p>The local {@link JsonConverter} is a complete copy of Kafka Connect's official JSON converter
 * under the SeaTunnel package, with the upstream null/default behavior exposed through {@code
 * replace.null.with.default}. Keeping it under the SeaTunnel package avoids shadowing the official
 * Kafka class in other connectors.
 */
public class DebeziumJsonConverter implements Serializable {
    /** Keep the stream identity of the pre-ST-3742 converter. */
    private static final long serialVersionUID = -4309014023723437706L;

    private static final String INCLUDE_SCHEMA_METHOD = "convertToJsonWithEnvelope";
    private static final String EXCLUDE_SCHEMA_METHOD = "convertToJsonWithoutEnvelope";
    private static final String BEFORE_FIELD = "before";
    private static final String AFTER_FIELD = "after";
    private static final String SOURCE_FIELD = "source";

    private final boolean keySchemaEnable;
    private final boolean valueSchemaEnable;
    private final boolean keyReplaceNullWithDefault;
    private final boolean valueReplaceNullWithDefault;
    private transient volatile JsonConverter keyConverter;
    private transient volatile JsonConverter valueConverter;
    private transient Method keyConverterMethod;
    private transient Method valueConverterMethod;

    public DebeziumJsonConverter(boolean keySchemaEnable, boolean valueSchemaEnable) {
        this(keySchemaEnable, valueSchemaEnable, false, false);
    }

    public DebeziumJsonConverter(
            boolean keySchemaEnable, boolean valueSchemaEnable, boolean replaceNullWithDefault) {
        this(keySchemaEnable, valueSchemaEnable, replaceNullWithDefault, replaceNullWithDefault);
    }

    public DebeziumJsonConverter(
            boolean keySchemaEnable,
            boolean valueSchemaEnable,
            boolean keyReplaceNullWithDefault,
            boolean valueReplaceNullWithDefault) {
        this.keySchemaEnable = keySchemaEnable;
        this.valueSchemaEnable = valueSchemaEnable;
        this.keyReplaceNullWithDefault = keyReplaceNullWithDefault;
        this.valueReplaceNullWithDefault = valueReplaceNullWithDefault;
    }

    public String serializeKey(SourceRecord record)
            throws InvocationTargetException, IllegalAccessException {
        tryInit();
        JsonNode jsonNode =
                (JsonNode)
                        keyConverterMethod.invoke(keyConverter, record.keySchema(), record.key());
        /*
         If Record key and keySchema is null keyConverterMethod invoke method get jsonNode is null
         toString method occur nullPointException, So add a judge
        */
        if (Objects.isNull(jsonNode)) {
            return null;
        }
        return jsonNode.toString();
    }

    public String serializeValue(SourceRecord record)
            throws InvocationTargetException, IllegalAccessException {
        tryInit();
        Object value = record.value();
        if (!valueReplaceNullWithDefault) {
            value = preserveProtocolDefaults(record.valueSchema(), value);
        }
        JsonNode jsonNode =
                (JsonNode) valueConverterMethod.invoke(valueConverter, record.valueSchema(), value);
        // Mirrors JsonConverter#fromConnectData: a null schema with a null value yields null.
        if (Objects.isNull(jsonNode)) {
            return null;
        }
        return jsonNode.toString();
    }

    /**
     * Preserve Debezium protocol defaults while keeping explicit NULLs in the business payload.
     *
     * <p>The upstream converter receives one envelope struct containing both the business rows
     * ({@code before}/{@code after}) and protocol metadata ({@code source}, {@code snapshot}, and
     * similar fields). The ST-3742 NULL policy applies only to the business rows. Before passing
     * the value to the official converter copy, materialize schema defaults in the metadata part of
     * the envelope so that {@code source.snapshot="false"} remains compatible with Debezium's
     * protocol contract.
     */
    private static Object preserveProtocolDefaults(Schema schema, Object value) {
        if (!(value instanceof Struct)
                || schema == null
                || schema.type() != Schema.Type.STRUCT
                || schema.field(SOURCE_FIELD) == null
                || (schema.field(BEFORE_FIELD) == null && schema.field(AFTER_FIELD) == null)) {
            return value;
        }

        Struct envelope = (Struct) value;
        Struct prepared = new Struct(schema);
        for (Field field : schema.fields()) {
            Object fieldValue = envelope.getWithoutDefault(field.name());
            if (BEFORE_FIELD.equals(field.name()) || AFTER_FIELD.equals(field.name())) {
                prepared.put(field.name(), fieldValue);
            } else {
                prepared.put(field.name(), materializeProtocolDefaults(field.schema(), fieldValue));
            }
        }
        return prepared;
    }

    /** Recursively materialize defaults in Debezium protocol metadata structs. */
    private static Object materializeProtocolDefaults(Schema schema, Object value) {
        if (value == null) {
            return schema == null ? null : schema.defaultValue();
        }
        if (schema == null) {
            return value;
        }

        switch (schema.type()) {
            case STRUCT:
                Struct struct = (Struct) value;
                Struct prepared = new Struct(schema);
                for (Field field : schema.fields()) {
                    prepared.put(
                            field.name(),
                            materializeProtocolDefaults(
                                    field.schema(), struct.getWithoutDefault(field.name())));
                }
                return prepared;
            case ARRAY:
                Collection<?> collection = (Collection<?>) value;
                List<Object> preparedCollection = new ArrayList<>(collection.size());
                for (Object element : collection) {
                    preparedCollection.add(
                            materializeProtocolDefaults(schema.valueSchema(), element));
                }
                return preparedCollection;
            case MAP:
                Map<?, ?> map = (Map<?, ?>) value;
                Map<Object, Object> preparedMap = new LinkedHashMap<>(map.size());
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    preparedMap.put(
                            materializeProtocolDefaults(schema.keySchema(), entry.getKey()),
                            materializeProtocolDefaults(schema.valueSchema(), entry.getValue()));
                }
                return preparedMap;
            default:
                return value;
        }
    }

    private void tryInit() {
        if (keyConverter == null) {
            synchronized (this) {
                if (keyConverter == null) {
                    keyConverter = new JsonConverter();
                    Map<String, Object> configs = new HashMap<>();
                    configs.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, keySchemaEnable);
                    configs.put(
                            JsonConverterConfig.DECIMAL_FORMAT_CONFIG,
                            DecimalFormat.NUMERIC.name());
                    configs.put(
                            JsonConverterConfig.REPLACE_NULL_WITH_DEFAULT_CONFIG,
                            keyReplaceNullWithDefault);
                    keyConverter.configure(configs, true);
                    keyConverterMethod =
                            ReflectionUtils.getDeclaredMethod(
                                            JsonConverter.class,
                                            keySchemaEnable
                                                    ? INCLUDE_SCHEMA_METHOD
                                                    : EXCLUDE_SCHEMA_METHOD,
                                            Schema.class,
                                            Object.class)
                                    .get();
                }
            }
        }
        if (valueConverter == null) {
            synchronized (this) {
                if (valueConverter == null) {
                    valueConverter = new JsonConverter();
                    Map<String, Object> configs = new HashMap<>();
                    configs.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, valueSchemaEnable);
                    configs.put(
                            JsonConverterConfig.DECIMAL_FORMAT_CONFIG,
                            DecimalFormat.NUMERIC.name());
                    configs.put(
                            JsonConverterConfig.REPLACE_NULL_WITH_DEFAULT_CONFIG,
                            valueReplaceNullWithDefault);
                    valueConverter.configure(configs, false);
                    valueConverterMethod =
                            ReflectionUtils.getDeclaredMethod(
                                            JsonConverter.class,
                                            valueSchemaEnable
                                                    ? INCLUDE_SCHEMA_METHOD
                                                    : EXCLUDE_SCHEMA_METHOD,
                                            Schema.class,
                                            Object.class)
                                    .get();
                }
            }
        }
    }
}
