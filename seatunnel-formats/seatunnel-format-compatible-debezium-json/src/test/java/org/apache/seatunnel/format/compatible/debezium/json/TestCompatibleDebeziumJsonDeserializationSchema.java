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

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;

public class TestCompatibleDebeziumJsonDeserializationSchema {

    @Test
    public void testDebeziumDeserializationSchema()
            throws InvocationTargetException, IllegalAccessException {
        SchemaBuilder schemaBuilder =
                SchemaBuilder.struct()
                        .name("test")
                        .field("field", SchemaBuilder.string().optional().build());
        Struct struct = new Struct(schemaBuilder.build()).put("field", "value");
        SourceRecord record =
                new SourceRecord(
                        null,
                        null,
                        "test",
                        schemaBuilder.build(),
                        struct,
                        schemaBuilder.build(),
                        struct);

        CompatibleDebeziumJsonDeserializationSchema compatibleDebeziumJsonDeserializationSchema =
                new CompatibleDebeziumJsonDeserializationSchema(true, true);
        SeaTunnelRow deserialize = compatibleDebeziumJsonDeserializationSchema.deserialize(record);
        Assertions.assertNotNull(deserialize);
        Assertions.assertEquals(TablePath.DEFAULT.getFullName(), deserialize.getTableId());
        Assertions.assertIterableEquals(
                Lists.newArrayList(
                        "test",
                        "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":true,\"field\":\"field\"}],\"optional\":false,\"name\":\"test\"},\"payload\":{\"field\":\"value\"}}",
                        "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":true,\"field\":\"field\"}],\"optional\":false,\"name\":\"test\"},\"payload\":{\"field\":\"value\"}}"),
                Arrays.asList(deserialize.getFields()));
    }
}
