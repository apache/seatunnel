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

package org.apache.seatunnel.connectors.seatunnel.natsjetstream.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.natsjetstream.config.NatsJetStreamSinkOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.Nats;
import io.nats.client.PublishOptions;
import io.nats.client.impl.Headers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

class NatsJetStreamSinkWriterTest {

    private static final SeaTunnelRowType JSON_ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {"id", "name"},
                    new SeaTunnelDataType<?>[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});

    @Test
    void writePublishesJsonPayload() throws Exception {
        TestContext context = new TestContext();
        try (TestWriterResources resources = TestWriterResources.open(context)) {
            NatsJetStreamSinkWriter writer =
                    new NatsJetStreamSinkWriter(
                            context,
                            JSON_ROW_TYPE,
                            ReadonlyConfig.fromMap(validJsonConfig()),
                            TestWriterResources.catalogTable(JSON_ROW_TYPE));

            writer.write(new SeaTunnelRow(new Object[] {1, "alice"}));

            ArgumentCaptor<byte[]> payloadCaptor = ArgumentCaptor.forClass(byte[].class);
            ArgumentCaptor<PublishOptions> optionsCaptor =
                    ArgumentCaptor.forClass(PublishOptions.class);
            Mockito.verify(resources.getJetStream())
                    .publish(
                            Mockito.eq("orders.events"),
                            Mockito.isNull(),
                            payloadCaptor.capture(),
                            optionsCaptor.capture());
            Assertions.assertEquals(
                    "{\"id\":1,\"name\":\"alice\"}",
                    new String(payloadCaptor.getValue(), StandardCharsets.UTF_8));
            Assertions.assertNull(optionsCaptor.getValue().getMessageId());
        }
    }

    @Test
    void writeTranslatesJetStreamPublishFailure() throws Exception {
        TestContext context = new TestContext();
        try (TestWriterResources resources = TestWriterResources.open(context)) {
            Mockito.doThrow(Mockito.mock(JetStreamApiException.class))
                    .when(resources.getJetStream())
                    .publish(
                            Mockito.anyString(),
                            Mockito.any(),
                            Mockito.any(byte[].class),
                            Mockito.any(PublishOptions.class));

            NatsJetStreamSinkWriter writer =
                    new NatsJetStreamSinkWriter(
                            context,
                            JSON_ROW_TYPE,
                            ReadonlyConfig.fromMap(validJsonConfig()),
                            TestWriterResources.catalogTable(JSON_ROW_TYPE));

            IOException exception =
                    Assertions.assertThrows(
                            IOException.class,
                            () -> writer.write(new SeaTunnelRow(new Object[] {1, "alice"})));

            Assertions.assertTrue(
                    exception.getMessage().contains("Failed to publish NATS JetStream message"));
            Assertions.assertTrue(exception.getMessage().contains("orders.events"));
        }
    }

    @Test
    void closeTranslatesInterruptedExceptionAndPreservesInterrupt() throws Exception {
        TestContext context = new TestContext();
        try (TestWriterResources resources = TestWriterResources.open(context)) {
            Mockito.doThrow(new InterruptedException("stop"))
                    .when(resources.getConnection())
                    .close();

            NatsJetStreamSinkWriter writer =
                    new NatsJetStreamSinkWriter(
                            context,
                            JSON_ROW_TYPE,
                            ReadonlyConfig.fromMap(validJsonConfig()),
                            TestWriterResources.catalogTable(JSON_ROW_TYPE));

            IOException exception = Assertions.assertThrows(IOException.class, writer::close);

            Assertions.assertTrue(
                    exception.getMessage().contains("Interrupted while closing NATS connection"));
            Assertions.assertTrue(Thread.currentThread().isInterrupted());
            Thread.interrupted();
        }
    }

    @Test
    void writePublishesDeleteRowKindAsRegularMessage() throws Exception {
        TestContext context = new TestContext();
        try (TestWriterResources resources = TestWriterResources.open(context)) {
            NatsJetStreamSinkWriter writer =
                    new NatsJetStreamSinkWriter(
                            context,
                            JSON_ROW_TYPE,
                            ReadonlyConfig.fromMap(validJsonConfig()),
                            TestWriterResources.catalogTable(JSON_ROW_TYPE));
            SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, "alice"});
            row.setRowKind(org.apache.seatunnel.api.table.type.RowKind.DELETE);

            writer.write(row);

            Mockito.verify(resources.getJetStream())
                    .publish(
                            Mockito.eq("orders.events"),
                            Mockito.isNull(),
                            Mockito.any(byte[].class),
                            Mockito.any(PublishOptions.class));
        }
    }

    @Test
    void writePublishesNativeDataOnlyMessageUsingConfiguredSubjectFallback() throws Exception {
        SeaTunnelRowType dataOnlyRowType =
                new SeaTunnelRowType(
                        new String[] {"data"},
                        new SeaTunnelDataType<?>[] {PrimitiveByteArrayType.INSTANCE});
        TestContext context = new TestContext();
        try (TestWriterResources resources = TestWriterResources.open(context)) {
            NatsJetStreamSinkWriter writer =
                    new NatsJetStreamSinkWriter(
                            context,
                            dataOnlyRowType,
                            ReadonlyConfig.fromMap(validNativeFallbackConfig()),
                            TestWriterResources.catalogTable(dataOnlyRowType));
            byte[] payload = "payload".getBytes(StandardCharsets.UTF_8);

            writer.write(new SeaTunnelRow(new Object[] {payload}));

            ArgumentCaptor<byte[]> payloadCaptor = ArgumentCaptor.forClass(byte[].class);
            ArgumentCaptor<Headers> headersCaptor = ArgumentCaptor.forClass(Headers.class);
            Mockito.verify(resources.getJetStream())
                    .publish(
                            Mockito.eq("orders.events"),
                            headersCaptor.capture(),
                            payloadCaptor.capture(),
                            Mockito.any(PublishOptions.class));
            Assertions.assertArrayEquals(payload, payloadCaptor.getValue());
            Assertions.assertEquals(
                    "INSERT",
                    headersCaptor
                            .getValue()
                            .getFirst(NatsJetStreamRequestSerializer.ROW_KIND_HEADER));
        }
    }

    @Test
    void writePublishesNativeMessageIdAndHeaders() throws Exception {
        SeaTunnelRowType nativeRowType = NatsJetStreamRequestSerializerTest.NATIVE_ROW_TYPE;
        TestContext context = new TestContext();
        try (TestWriterResources resources = TestWriterResources.open(context)) {
            NatsJetStreamSinkWriter writer =
                    new NatsJetStreamSinkWriter(
                            context,
                            nativeRowType,
                            ReadonlyConfig.fromMap(validNativeConfig()),
                            TestWriterResources.catalogTable(nativeRowType));

            Map<String, String> headers = new HashMap<>();
            headers.put("tenant", "acme");
            headers.put("trace", "t-1");
            writer.write(
                    new SeaTunnelRow(
                            new Object[] {
                                "payments.created",
                                "message-9",
                                headers,
                                "payload".getBytes(StandardCharsets.UTF_8)
                            }));

            ArgumentCaptor<Headers> headersCaptor = ArgumentCaptor.forClass(Headers.class);
            ArgumentCaptor<PublishOptions> optionsCaptor =
                    ArgumentCaptor.forClass(PublishOptions.class);
            Mockito.verify(resources.getJetStream())
                    .publish(
                            Mockito.eq("payments.created"),
                            headersCaptor.capture(),
                            Mockito.any(byte[].class),
                            optionsCaptor.capture());
            Assertions.assertEquals("acme", headersCaptor.getValue().getFirst("tenant"));
            Assertions.assertEquals("message-9", optionsCaptor.getValue().getMessageId());
            Assertions.assertEquals(
                    "INSERT",
                    headersCaptor
                            .getValue()
                            .getFirst(NatsJetStreamRequestSerializer.ROW_KIND_HEADER));
        }
    }

    @Test
    void writeSkipsRowKindHeaderWhenDisabled() throws Exception {
        SeaTunnelRowType nativeRowType = NatsJetStreamRequestSerializerTest.NATIVE_ROW_TYPE;
        TestContext context = new TestContext();
        try (TestWriterResources resources = TestWriterResources.open(context)) {
            Map<String, Object> config = validNativeConfig();
            config.put(NatsJetStreamSinkOptions.INCLUDE_ROW_KIND_HEADER.key(), false);
            NatsJetStreamSinkWriter writer =
                    new NatsJetStreamSinkWriter(
                            context,
                            nativeRowType,
                            ReadonlyConfig.fromMap(config),
                            TestWriterResources.catalogTable(nativeRowType));

            writer.write(
                    new SeaTunnelRow(
                            new Object[] {
                                "payments.created",
                                "message-9",
                                null,
                                "payload".getBytes(StandardCharsets.UTF_8)
                            }));

            Mockito.verify(resources.getJetStream())
                    .publish(
                            Mockito.eq("payments.created"),
                            Mockito.isNull(),
                            Mockito.any(byte[].class),
                            Mockito.any(PublishOptions.class));
        }
    }

    @Test
    void constructorClosesConnectionWhenJetStreamAcquisitionFails() throws Exception {
        TestContext context = new TestContext();
        Connection connection = Mockito.mock(Connection.class);
        Mockito.when(connection.jetStream())
                .thenThrow(new RuntimeException("jetStream unavailable"));
        try (MockedStatic<Nats> mockedNats = Mockito.mockStatic(Nats.class)) {
            mockedNats
                    .when(() -> Nats.connect(Mockito.any(io.nats.client.Options.class)))
                    .thenReturn(connection);

            IOException exception =
                    Assertions.assertThrows(
                            IOException.class,
                            () ->
                                    new NatsJetStreamSinkWriter(
                                            context,
                                            JSON_ROW_TYPE,
                                            ReadonlyConfig.fromMap(validJsonConfig()),
                                            TestWriterResources.catalogTable(JSON_ROW_TYPE)));

            Assertions.assertTrue(
                    exception
                            .getMessage()
                            .contains("Failed to connect NATS JetStream sink writer"));
        }
        Mockito.verify(connection, Mockito.times(1)).close();
    }

    private Map<String, Object> validJsonConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(NatsJetStreamSinkOptions.URL.key(), "nats://127.0.0.1:4222");
        config.put(NatsJetStreamSinkOptions.SUBJECT.key(), "orders.events");
        return config;
    }

    private Map<String, Object> validNativeFallbackConfig() {
        Map<String, Object> config = validJsonConfig();
        config.put(NatsJetStreamSinkOptions.FORMAT.key(), "NATIVE");
        return config;
    }

    private Map<String, Object> validNativeConfig() {
        Map<String, Object> config = validJsonConfig();
        config.put(NatsJetStreamSinkOptions.FORMAT.key(), "NATIVE");
        Map<String, String> mappings = new HashMap<>();
        mappings.put(NatsJetStreamSinkOptions.NATIVE_MAPPING_SUBJECT, "dynamic_subject");
        mappings.put(NatsJetStreamSinkOptions.NATIVE_MAPPING_ID, "message_id");
        mappings.put(NatsJetStreamSinkOptions.NATIVE_MAPPING_HEADERS, "attributes");
        mappings.put(NatsJetStreamSinkOptions.NATIVE_MAPPING_DATA, "payload");
        config.put(NatsJetStreamSinkOptions.NATIVE_FIELDS.key(), mappings);
        return config;
    }
}
