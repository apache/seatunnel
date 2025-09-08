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

package org.apache.seatunnel.connectors.seatunnel.kafka.serialize;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.MessageFormat;
import org.apache.seatunnel.format.compatible.debezium.json.CompatibleDebeziumJsonDeserializationSchema;

import org.apache.kafka.clients.producer.ProducerRecord;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

public class DefaultSeaTunnelRowSerializerTest {

    @Test
    public void testCustomTopic() {
        String topic = null;
        SeaTunnelRowType rowType =
                CompatibleDebeziumJsonDeserializationSchema.DEBEZIUM_DATA_ROW_TYPE;
        MessageFormat format = MessageFormat.COMPATIBLE_DEBEZIUM_JSON;
        String delimiter = null;
        ReadonlyConfig pluginConfig = ReadonlyConfig.fromMap(Collections.emptyMap());

        DefaultSeaTunnelRowSerializer serializer =
                DefaultSeaTunnelRowSerializer.create(
                        topic, rowType, format, delimiter, pluginConfig);
        ProducerRecord<byte[], byte[]> record =
                serializer.serializeRow(
                        new SeaTunnelRow(new Object[] {"test.database1.table1", "key1", "value1"}));

        Assertions.assertEquals("test.database1.table1", record.topic());
        Assertions.assertEquals("key1", new String(record.key()));
        Assertions.assertEquals("value1", new String(record.value()));

        topic = "test_topic";
        serializer =
                DefaultSeaTunnelRowSerializer.create(
                        topic, rowType, format, delimiter, pluginConfig);
        record =
                serializer.serializeRow(
                        new SeaTunnelRow(new Object[] {"test.database1.table1", "key1", "value1"}));

        Assertions.assertEquals("test_topic", record.topic());
        Assertions.assertEquals("key1", new String(record.key()));
        Assertions.assertEquals("value1", new String(record.value()));
    }
}
