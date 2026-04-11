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

package org.apache.seatunnel.connectors.seatunnel.pulsar.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.pulsar.exception.PulsarConnectorException;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PulsarSinkWriterTest {

    private PulsarSinkWriter createWriter(ReadonlyConfig config) {
        return new PulsarSinkWriter(
                new SeaTunnelRowType(new String[] {}, new SeaTunnelDataType[] {}),
                config,
                java.util.Collections.emptyList(),
                null,
                topic -> createProducerProxy());
    }

    @Test
    public void testResolveTopicWithTableId() {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {});
        row.setTableId("persistent://tenant/ns/topic1");

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("topic", "fallback-topic");
        configMap.put("format", "json");
        configMap.put("field_delimiter", ",");
        configMap.put("transaction_timeout", 1000);
        configMap.put("semantics", "NON");
        configMap.put("message.routing.mode", "RoundRobinPartition");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        PulsarSinkWriter writer = createWriter(config);

        String topic = writer.resolveTopic(row);

        assertEquals("persistent://tenant/ns/topic1", topic);
    }

    @Test
    public void testResolveTopicWithoutTableId() {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {});
        // no tableId set

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("topic", "fallback-topic");
        configMap.put("format", "json");
        configMap.put("field_delimiter", ",");
        configMap.put("transaction_timeout", 1000);
        configMap.put("semantics", "NON");
        configMap.put("message.routing.mode", "RoundRobinPartition");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        PulsarSinkWriter writer = createWriter(config);

        String topic = writer.resolveTopic(row);

        assertEquals("fallback-topic", topic);
    }

    @Test
    public void testResolveTopicWithoutTableIdAndWithoutTopic() {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {});

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("format", "json");
        configMap.put("field_delimiter", ",");
        configMap.put("transaction_timeout", 1000);
        configMap.put("semantics", "NON");
        configMap.put("message.routing.mode", "RoundRobinPartition");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        PulsarSinkWriter writer = createWriter(config);

        assertThrows(IllegalArgumentException.class, () -> writer.resolveTopic(row));
    }

    @Test
    public void testGetOrCreateProducerCachesProducerByTopic() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("format", "json");
        configMap.put("field_delimiter", ",");
        configMap.put("transaction_timeout", 1000);
        configMap.put("semantics", "NON");
        configMap.put("message.routing.mode", "RoundRobinPartition");

        AtomicInteger createCount = new AtomicInteger();
        PulsarSinkWriter writer =
                new PulsarSinkWriter(
                        new SeaTunnelRowType(new String[] {}, new SeaTunnelDataType[] {}),
                        ReadonlyConfig.fromMap(configMap),
                        java.util.Collections.emptyList(),
                        null,
                        topic -> {
                            createCount.incrementAndGet();
                            return createProducerProxy();
                        });

        Producer<byte[]> first = writer.getOrCreateProducer("topic-a");
        Producer<byte[]> second = writer.getOrCreateProducer("topic-a");
        Producer<byte[]> third = writer.getOrCreateProducer("topic-b");

        assertSame(first, second);
        assertTrue(first != third);
        assertEquals(2, createCount.get());
    }

    @Test
    public void testCloseReleasesResourcesWhenAsyncSendFailed() throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("format", "json");
        configMap.put("field_delimiter", ",");
        configMap.put("transaction_timeout", 1000);
        configMap.put("semantics", "NON");
        configMap.put("message.routing.mode", "RoundRobinPartition");

        AtomicBoolean producerClosed = new AtomicBoolean(false);
        AtomicBoolean clientClosed = new AtomicBoolean(false);

        PulsarSinkWriter writer =
                new PulsarSinkWriter(
                        new SeaTunnelRowType(new String[] {}, new SeaTunnelDataType[] {}),
                        ReadonlyConfig.fromMap(configMap),
                        java.util.Collections.emptyList(),
                        createPulsarClientProxy(clientClosed),
                        topic -> createProducerProxy(producerClosed));
        writer.getOrCreateProducer("topic-a");

        Field sendFailureField = PulsarSinkWriter.class.getDeclaredField("sendMessageException");
        sendFailureField.setAccessible(true);
        @SuppressWarnings("unchecked")
        AtomicReference<Throwable> sendFailure =
                (AtomicReference<Throwable>) sendFailureField.get(writer);
        sendFailure.set(new RuntimeException("send failed"));

        assertThrows(PulsarConnectorException.class, writer::close);
        assertTrue(producerClosed.get());
        assertTrue(clientClosed.get());
    }

    @SuppressWarnings("unchecked")
    private static Producer<byte[]> createProducerProxy() {
        return createProducerProxy(new AtomicBoolean(false));
    }

    @SuppressWarnings("unchecked")
    private static Producer<byte[]> createProducerProxy(AtomicBoolean closed) {
        return (Producer<byte[]>)
                Proxy.newProxyInstance(
                        Producer.class.getClassLoader(),
                        new Class[] {Producer.class},
                        (proxy, method, args) -> {
                            if ("close".equals(method.getName())) {
                                closed.set(true);
                                return null;
                            }
                            Class<?> returnType = method.getReturnType();
                            if (returnType.equals(boolean.class)) {
                                return false;
                            }
                            if (returnType.equals(int.class)) {
                                return 0;
                            }
                            if (returnType.equals(long.class)) {
                                return 0L;
                            }
                            if (returnType.equals(float.class)) {
                                return 0F;
                            }
                            if (returnType.equals(double.class)) {
                                return 0D;
                            }
                            return null;
                        });
    }

    @SuppressWarnings("unchecked")
    private static PulsarClient createPulsarClientProxy(AtomicBoolean closed) {
        return (PulsarClient)
                Proxy.newProxyInstance(
                        PulsarClient.class.getClassLoader(),
                        new Class[] {PulsarClient.class},
                        (proxy, method, args) -> {
                            if ("close".equals(method.getName())) {
                                closed.set(true);
                                return null;
                            }
                            Class<?> returnType = method.getReturnType();
                            if (returnType.equals(boolean.class)) {
                                return false;
                            }
                            if (returnType.equals(int.class)) {
                                return 0;
                            }
                            if (returnType.equals(long.class)) {
                                return 0L;
                            }
                            return null;
                        });
    }
}
