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

package org.apache.seatunnel.connectors.seatunnel.mqtt.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

class MqttSourceConfigTest {

    @Test
    void testDefaultValues() {
        MqttSourceConfig sourceConfig = new MqttSourceConfig(ReadonlyConfig.fromMap(baseConfig()));

        Assertions.assertEquals("tcp://localhost:1883", sourceConfig.getUrl());
        Assertions.assertEquals("users", sourceConfig.getTopic());
        Assertions.assertNull(sourceConfig.getUsername());
        Assertions.assertNull(sourceConfig.getPassword());
        Assertions.assertEquals(1, sourceConfig.getQos());
        Assertions.assertEquals("json", sourceConfig.getFormat());
        Assertions.assertEquals(",", sourceConfig.getFieldDelimiter());
        Assertions.assertTrue(sourceConfig.isCleanSession());
        Assertions.assertEquals(30, sourceConfig.getConnectionTimeout());
        Assertions.assertEquals(60, sourceConfig.getKeepAliveInterval());
        Assertions.assertEquals(120, sourceConfig.getReconnectTimeout());
        Assertions.assertEquals(1000, sourceConfig.getMaxQueueSize());
    }

    @Test
    void testExplicitValues() {
        Map<String, Object> config = baseConfig();
        config.put("username", "user");
        config.put("password", "pass");
        config.put("qos", 0);
        config.put("format", "text");
        config.put("field_delimiter", "|");
        config.put("client_id", "mqtt-source-client");
        config.put("clean_session", false);
        config.put("connection_timeout", 10);
        config.put("keep_alive_interval", 20);
        config.put("reconnect_timeout", 30);
        config.put("max_queue_size", 200);

        MqttSourceConfig sourceConfig = new MqttSourceConfig(ReadonlyConfig.fromMap(config));

        Assertions.assertEquals("user", sourceConfig.getUsername());
        Assertions.assertEquals("pass", sourceConfig.getPassword());
        Assertions.assertEquals(0, sourceConfig.getQos());
        Assertions.assertEquals("text", sourceConfig.getFormat());
        Assertions.assertEquals("|", sourceConfig.getFieldDelimiter());
        Assertions.assertEquals("mqtt-source-client", sourceConfig.getClientId());
        Assertions.assertFalse(sourceConfig.isCleanSession());
        Assertions.assertEquals(10, sourceConfig.getConnectionTimeout());
        Assertions.assertEquals(20, sourceConfig.getKeepAliveInterval());
        Assertions.assertEquals(30, sourceConfig.getReconnectTimeout());
        Assertions.assertEquals(200, sourceConfig.getMaxQueueSize());
    }

    @Test
    void testInvalidQosFails() {
        Map<String, Object> config = baseConfig();
        config.put("qos", 2);

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new MqttSourceConfig(ReadonlyConfig.fromMap(config)));
    }

    @Test
    void testNegativeQosFails() {
        Map<String, Object> config = baseConfig();
        config.put("qos", -1);

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new MqttSourceConfig(ReadonlyConfig.fromMap(config)));
    }

    @Test
    void testNonPositiveReconnectTimeoutFails() {
        Map<String, Object> config = baseConfig();
        config.put("reconnect_timeout", 0);

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new MqttSourceConfig(ReadonlyConfig.fromMap(config)));
    }

    @Test
    void testNonPositiveMaxQueueSizeFails() {
        Map<String, Object> config = baseConfig();
        config.put("max_queue_size", 0);

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new MqttSourceConfig(ReadonlyConfig.fromMap(config)));
    }

    @Test
    void testUnsupportedFormatFails() {
        Map<String, Object> config = baseConfig();
        config.put("format", "avro");

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new MqttSourceConfig(ReadonlyConfig.fromMap(config)));
    }

    @Test
    void testPersistentSessionRequiresClientId() {
        Map<String, Object> config = baseConfig();
        config.put("clean_session", false);

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new MqttSourceConfig(ReadonlyConfig.fromMap(config)));
    }

    @Test
    void testPersistentSessionRequiresNonBlankClientId() {
        Map<String, Object> config = baseConfig();
        config.put("clean_session", false);
        config.put("client_id", "   ");

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new MqttSourceConfig(ReadonlyConfig.fromMap(config)));
    }

    @Test
    void testDefaultClientIdIsGeneratedForCleanSession() {
        MqttSourceConfig sourceConfig = new MqttSourceConfig(ReadonlyConfig.fromMap(baseConfig()));

        Assertions.assertTrue(sourceConfig.getClientId().startsWith("seatunnel_mqtt_source_"));
    }

    @Test
    void testConfigSurvivesJavaSerialization() throws Exception {
        Map<String, Object> config = baseConfig();
        config.put("username", "admin");
        // Deliberately non-default values, so the assertions below prove the values round
        // tripped rather than that defaults were rebuilt. The default qos is 1 and validate()
        // limits it to 0 or 1, so 0 is the only non-default that is legal; the default format
        // is json.
        config.put("qos", 0);
        config.put("format", "text");
        MqttSourceConfig sourceConfig = new MqttSourceConfig(ReadonlyConfig.fromMap(config));

        byte[] serialized;
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(sourceConfig);
            out.flush();
            serialized = bytes.toByteArray();
        }

        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(serialized))) {
            MqttSourceConfig restored = (MqttSourceConfig) in.readObject();
            Assertions.assertEquals(sourceConfig.getUrl(), restored.getUrl());
            Assertions.assertEquals(sourceConfig.getTopic(), restored.getTopic());
            Assertions.assertEquals(sourceConfig.getUsername(), restored.getUsername());
            Assertions.assertEquals(sourceConfig.getQos(), restored.getQos());
            Assertions.assertEquals(sourceConfig.getClientId(), restored.getClientId());
        }
    }

    private static Map<String, Object> baseConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("url", "tcp://localhost:1883");
        config.put("topic", "users");
        return config;
    }
}
