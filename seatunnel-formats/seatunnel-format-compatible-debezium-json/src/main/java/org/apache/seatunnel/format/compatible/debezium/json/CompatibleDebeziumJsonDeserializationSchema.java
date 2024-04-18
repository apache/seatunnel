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

package org.apache.seatunnel.format.compatible.debezium.json;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;

public class CompatibleDebeziumJsonDeserializationSchema
        implements DeserializationSchema<SeaTunnelRow> {
    public static final String IDENTIFIER = "compatible_debezium_json";
    public static final String FIELD_TOPIC = "topic";
    public static final String FIELD_KEY = "key";
    public static final String FIELD_VALUE = "value";
    public static final SeaTunnelRowType DEBEZIUM_DATA_ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {FIELD_TOPIC, FIELD_KEY, FIELD_VALUE},
                    new SeaTunnelDataType[] {
                        BasicType.STRING_TYPE, BasicType.STRING_TYPE, BasicType.STRING_TYPE
                    });

    private final DebeziumJsonConverter debeziumJsonConverter;

    public CompatibleDebeziumJsonDeserializationSchema(
            boolean keySchemaEnable, boolean valueSchemaEnable) {
        this.debeziumJsonConverter = new DebeziumJsonConverter(keySchemaEnable, valueSchemaEnable);
    }

    @Override
    public SeaTunnelRow deserialize(byte[] message) throws IOException {
        throw new UnsupportedEncodingException();
    }

    public SeaTunnelRow deserialize(SourceRecord record)
            throws InvocationTargetException, IllegalAccessException {
        String key = debeziumJsonConverter.serializeKey(record);
        String value = debeziumJsonConverter.serializeValue(record);
        Object[] fields = new Object[] {record.topic(), key, value};
        return new SeaTunnelRow(fields);
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return DEBEZIUM_DATA_ROW_TYPE;
    }
}
