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
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.MessageFormat;
import org.apache.seatunnel.format.compatible.debezium.json.CompatibleDebeziumJsonDeserializationSchema;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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

    @Test
    public void testKafkaHeaders() {
        String topic = "test_topic";
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name", "source", "traceId"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                            BasicType.INT_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE
                        });
        MessageFormat format = MessageFormat.JSON;
        String delimiter = ",";
        Map<String, Object> configMap = new HashMap<>();
        ReadonlyConfig pluginConfig = ReadonlyConfig.fromMap(configMap);

        // Test with header fields
        DefaultSeaTunnelRowSerializer serializer =
                DefaultSeaTunnelRowSerializer.create(
                        topic,
                        Arrays.asList("id"),
                        Arrays.asList("source", "traceId"),
                        rowType,
                        format,
                        delimiter,
                        pluginConfig);

        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, "test", "web", "trace-123"});
        ProducerRecord<byte[], byte[]> record = serializer.serializeRow(row);

        Assertions.assertEquals("test_topic", record.topic());
        Assertions.assertNotNull(record.headers());

        Header sourceHeader = record.headers().lastHeader("source");
        Assertions.assertNotNull(sourceHeader);
        Assertions.assertEquals("web", new String(sourceHeader.value(), StandardCharsets.UTF_8));

        Header traceIdHeader = record.headers().lastHeader("traceId");
        Assertions.assertNotNull(traceIdHeader);
        Assertions.assertEquals(
                "trace-123", new String(traceIdHeader.value(), StandardCharsets.UTF_8));
    }

    @Test
    public void testKafkaHeadersWithNullValue() {
        String topic = "test_topic";
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name", "source", "traceId"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                            BasicType.INT_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE
                        });
        MessageFormat format = MessageFormat.JSON;
        String delimiter = ",";
        Map<String, Object> configMap = new HashMap<>();
        ReadonlyConfig pluginConfig = ReadonlyConfig.fromMap(configMap);

        DefaultSeaTunnelRowSerializer serializer =
                DefaultSeaTunnelRowSerializer.create(
                        topic,
                        Arrays.asList("id"),
                        Arrays.asList("source", "traceId"),
                        rowType,
                        format,
                        delimiter,
                        pluginConfig);

        // Test with null header value
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, "test", "web", null});
        ProducerRecord<byte[], byte[]> record = serializer.serializeRow(row);

        Assertions.assertEquals("test_topic", record.topic());
        Assertions.assertNotNull(record.headers());

        Header sourceHeader = record.headers().lastHeader("source");
        Assertions.assertNotNull(sourceHeader);
        Assertions.assertEquals("web", new String(sourceHeader.value(), StandardCharsets.UTF_8));

        // Null value should be written as null in headers
        Header traceIdHeader = record.headers().lastHeader("traceId");
        Assertions.assertNotNull(traceIdHeader);
        Assertions.assertNull(traceIdHeader.value());
    }

    @Test
    public void testBackwardCompatibilityWithKeyFields() {
        // Test that the 6-parameter create method (without headerFields) still works
        String topic = "test_topic";
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name", "age"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                            BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.INT_TYPE
                        });
        MessageFormat format = MessageFormat.JSON;
        String delimiter = ",";
        Map<String, Object> configMap = new HashMap<>();
        ReadonlyConfig pluginConfig = ReadonlyConfig.fromMap(configMap);

        // Test with keyFields but no headerFields (backward compatibility)
        DefaultSeaTunnelRowSerializer serializer =
                DefaultSeaTunnelRowSerializer.create(
                        topic, Arrays.asList("id"), rowType, format, delimiter, pluginConfig);

        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, "John", 25});
        ProducerRecord<byte[], byte[]> record = serializer.serializeRow(row);

        Assertions.assertEquals("test_topic", record.topic());
        Assertions.assertNotNull(record.value());

        // Value should contain all fields
        String valueString = new String(record.value(), StandardCharsets.UTF_8);
        Assertions.assertTrue(valueString.contains("\"id\""));
        Assertions.assertTrue(valueString.contains("\"name\""));
        Assertions.assertTrue(valueString.contains("\"age\""));
    }

    @Test
    public void testBackwardCompatibilityWithPartition() {
        // Test that the 6-parameter create method with partition (without headerFields) still works
        String topic = "test_topic";
        Integer partition = 0;
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name", "age"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                            BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.INT_TYPE
                        });
        MessageFormat format = MessageFormat.JSON;
        String delimiter = ",";
        Map<String, Object> configMap = new HashMap<>();
        ReadonlyConfig pluginConfig = ReadonlyConfig.fromMap(configMap);

        // Test with partition but no headerFields (backward compatibility)
        DefaultSeaTunnelRowSerializer serializer =
                DefaultSeaTunnelRowSerializer.create(
                        topic, partition, rowType, format, delimiter, pluginConfig);

        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, "John", 25});
        ProducerRecord<byte[], byte[]> record = serializer.serializeRow(row);

        Assertions.assertEquals("test_topic", record.topic());
        Assertions.assertEquals(partition, record.partition());
        Assertions.assertNotNull(record.value());

        // Value should contain all fields
        String valueString = new String(record.value(), StandardCharsets.UTF_8);
        Assertions.assertTrue(valueString.contains("\"id\""));
        Assertions.assertTrue(valueString.contains("\"name\""));
        Assertions.assertTrue(valueString.contains("\"age\""));
    }

    @Test
    public void testHeaderFieldsExcludedFromValue() {
        String topic = "test_topic";
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name", "source", "traceId"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                            BasicType.INT_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE
                        });
        MessageFormat format = MessageFormat.JSON;
        String delimiter = ",";
        Map<String, Object> configMap = new HashMap<>();
        ReadonlyConfig pluginConfig = ReadonlyConfig.fromMap(configMap);

        // Test with header fields
        DefaultSeaTunnelRowSerializer serializer =
                DefaultSeaTunnelRowSerializer.create(
                        topic,
                        Arrays.asList("id"),
                        Arrays.asList("source", "traceId"),
                        rowType,
                        format,
                        delimiter,
                        pluginConfig);

        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, "test", "web", "trace-123"});
        ProducerRecord<byte[], byte[]> record = serializer.serializeRow(row);

        Assertions.assertEquals("test_topic", record.topic());

        // Verify headers contain the expected fields
        Header sourceHeader = record.headers().lastHeader("source");
        Assertions.assertNotNull(sourceHeader);
        Assertions.assertEquals("web", new String(sourceHeader.value(), StandardCharsets.UTF_8));

        Header traceIdHeader = record.headers().lastHeader("traceId");
        Assertions.assertNotNull(traceIdHeader);
        Assertions.assertEquals(
                "trace-123", new String(traceIdHeader.value(), StandardCharsets.UTF_8));

        // Verify value does NOT contain header fields (source and traceId)
        // Header fields are only in Kafka headers, not in the message value
        String valueString = new String(record.value(), StandardCharsets.UTF_8);
        // The value should only contain id and name fields
        Assertions.assertTrue(valueString.contains("\"id\""));
        Assertions.assertTrue(valueString.contains("\"name\""));
        // Header fields should NOT be in the value
        Assertions.assertFalse(valueString.contains("\"source\""));
        Assertions.assertFalse(valueString.contains("\"traceId\""));
    }

    @Test
    public void testKafkaHeadersWithNullValueExcludedFromValue() {
        // Test that null header values are written as "null" string in headers
        // (consistent with partition_key_fields behavior)
        // and header fields are excluded from the message value
        String topic = "test_topic";
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name", "source", "traceId"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                            BasicType.INT_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE
                        });
        MessageFormat format = MessageFormat.JSON;
        String delimiter = ",";
        Map<String, Object> configMap = new HashMap<>();
        ReadonlyConfig pluginConfig = ReadonlyConfig.fromMap(configMap);

        DefaultSeaTunnelRowSerializer serializer =
                DefaultSeaTunnelRowSerializer.create(
                        topic,
                        Arrays.asList("id"),
                        Arrays.asList("source", "traceId"),
                        rowType,
                        format,
                        delimiter,
                        pluginConfig);

        // Test with null header value
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, "test", "web", null});
        ProducerRecord<byte[], byte[]> record = serializer.serializeRow(row);

        Assertions.assertEquals("test_topic", record.topic());
        Assertions.assertNotNull(record.headers());

        Header sourceHeader = record.headers().lastHeader("source");
        Assertions.assertNotNull(sourceHeader);
        Assertions.assertEquals("web", new String(sourceHeader.value(), StandardCharsets.UTF_8));

        // Null value should be written as null in headers
        Header traceIdHeader = record.headers().lastHeader("traceId");
        Assertions.assertNotNull(traceIdHeader);
        Assertions.assertNull(traceIdHeader.value());

        // Header fields should NOT be in the message value
        String valueString = new String(record.value(), StandardCharsets.UTF_8);
        Assertions.assertTrue(valueString.contains("\"id\""));
        Assertions.assertTrue(valueString.contains("\"name\""));
        Assertions.assertFalse(valueString.contains("\"source\""));
        Assertions.assertFalse(valueString.contains("\"traceId\""));
    }
}
