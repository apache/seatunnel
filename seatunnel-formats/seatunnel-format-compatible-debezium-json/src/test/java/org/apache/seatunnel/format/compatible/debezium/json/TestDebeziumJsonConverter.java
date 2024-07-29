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

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.util.Collections;

public class TestDebeziumJsonConverter {

    @Test
    public void testSerializeDecimalToNumber()
            throws InvocationTargetException, IllegalAccessException, JsonProcessingException {
        String key = "k";
        String value = "v";
        Struct keyStruct =
                new Struct(SchemaBuilder.struct().field(key, Decimal.builder(2).build()).build());
        keyStruct.put(key, BigDecimal.valueOf(1101, 2));
        Struct valueStruct =
                new Struct(SchemaBuilder.struct().field(value, Decimal.builder(2).build()).build());
        valueStruct.put(value, BigDecimal.valueOf(1101, 2));

        SourceRecord sourceRecord =
                new SourceRecord(
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        null,
                        keyStruct.schema(),
                        keyStruct,
                        valueStruct.schema(),
                        valueStruct);

        DebeziumJsonConverter converter = new DebeziumJsonConverter(false, false);
        Assertions.assertEquals("{\"k\":11.01}", converter.serializeKey(sourceRecord));
        Assertions.assertEquals("{\"v\":11.01}", converter.serializeValue(sourceRecord));
    }

    @Test
    public void testDebeziumSerializeKeyIsNull()
            throws InvocationTargetException, IllegalAccessException, JsonProcessingException {
        String value = "v";
        Struct valueStruct = new Struct(SchemaBuilder.struct().field(value, Schema.STRING_SCHEMA));
        valueStruct.put(value, "DebeziumTest");

        SourceRecord sourceRecord =
                new SourceRecord(
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        null,
                        null,
                        null,
                        valueStruct.schema(),
                        valueStruct);

        DebeziumJsonConverter converter = new DebeziumJsonConverter(false, false);
        Assertions.assertEquals(null, converter.serializeKey(sourceRecord));
        Assertions.assertEquals("{\"v\":\"DebeziumTest\"}", converter.serializeValue(sourceRecord));
    }
}
