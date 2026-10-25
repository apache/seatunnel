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

package org.apache.seatunnel.connectors.seatunnel.mqtt.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.common.utils.SerializationUtils;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Covers the Java serialization contract of {@link MqttSink}.
 *
 * <p>{@code SeaTunnelSink} extends {@link java.io.Serializable}, so the sink crosses the same
 * logical DAG boundary that {@link
 * org.apache.seatunnel.connectors.seatunnel.mqtt.source.MqttSource} does. That boundary is where a
 * missing field stops a job being submitted at all, which is the defect #12405 reported and #12406
 * fixed on the source side. The source now has round trip coverage; this is the matching sink half,
 * recorded as Issue 2 on the #12416 review.
 *
 * <p>There is no existing test class that owns {@code MqttSink} itself: {@code MqttSinkFactoryTest}
 * covers the factory's option rule and {@code MqttSinkWriterTest} covers the writer. This mirrors
 * {@code MqttSourceTest} on the source side rather than bolting sink-level assertions onto a
 * writer-level class.
 */
class MqttSinkTest {

    @Test
    void testSinkIsJavaSerializable() {
        MqttSink sink = newSink(baseConfig());

        MqttSink restored = SerializationUtils.deserialize(SerializationUtils.serialize(sink));

        // Assert restored state rather than a constant: getPluginName() returns "MQTT" literally
        // and would still pass with every field lost.
        Assertions.assertEquals(
                sink.getWriteCatalogTable().orElseThrow(AssertionError::new).getTableId(),
                restored.getWriteCatalogTable().orElseThrow(AssertionError::new).getTableId());
        Assertions.assertEquals(
                sink.getWriteCatalogTable().orElseThrow(AssertionError::new).getTableSchema(),
                restored.getWriteCatalogTable().orElseThrow(AssertionError::new).getTableSchema());

        // pluginConfig and seaTunnelRowType have no getters, so building a writer is the only way
        // to observe them: MqttSinkWriter's constructor reads topic, qos, batch_size and url from
        // the config and derives its serializer from the row type. The MqttClient construction is
        // mocked because that constructor also connects, which would otherwise reach out to
        // tcp://localhost:1883.
        try (MockedConstruction<MqttClient> ignored = Mockito.mockConstruction(MqttClient.class)) {
            Assertions.assertDoesNotThrow(
                    () -> restored.createWriter(Mockito.mock(SinkWriter.Context.class)));
        }
    }

    @Test
    void testSinkConfigValuesSurviveJavaSerialization() {
        // qos 2 is not a legal MQTT QoS for this connector and MqttSinkWriter rejects it, but
        // MqttSink itself does not validate, so the sink builds fine and only a writer surfaces
        // it. That makes it an observable, non-default value: MqttSinkOptions.QOS defaults to 1,
        // so a pluginConfig that came back empty would yield a legal qos and the writer would
        // build without complaint. The rejection below only happens if the 2 travelled.
        Map<String, Object> config = baseConfig();
        config.put("qos", 2);
        MqttSink sink = newSink(config);

        MqttSink restored = SerializationUtils.deserialize(SerializationUtils.serialize(sink));

        try (MockedConstruction<MqttClient> ignored = Mockito.mockConstruction(MqttClient.class)) {
            IllegalArgumentException exception =
                    Assertions.assertThrows(
                            IllegalArgumentException.class,
                            () -> restored.createWriter(Mockito.mock(SinkWriter.Context.class)));
            Assertions.assertTrue(
                    exception.getMessage().contains("got: 2"),
                    "expected the restored qos in the message, got: " + exception.getMessage());
        }
    }

    private static MqttSink newSink(Map<String, Object> config) {
        ReadonlyConfig pluginConfig = ReadonlyConfig.fromMap(config);
        return new MqttSink(pluginConfig, CatalogTableUtil.buildWithConfig(pluginConfig));
    }

    private static Map<String, Object> baseConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("url", "tcp://localhost:1883");
        config.put("topic", "users");
        config.put("schema", schemaConfig());
        return config;
    }

    private static Map<String, Object> schemaConfig() {
        Map<String, Object> schema = new HashMap<>();
        schema.put("fields", Collections.singletonMap("id", "int"));
        return schema;
    }
}
