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
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.natsjetstream.config.NatsJetStreamMessageFormat;
import org.apache.seatunnel.connectors.seatunnel.natsjetstream.config.NatsJetStreamSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.natsjetstream.exception.NatsJetStreamConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

class NatsJetStreamRequestSerializerTest {

    private static final SeaTunnelRowType JSON_ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {"id", "name", "active"},
                    new SeaTunnelDataType<?>[] {
                        BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.BOOLEAN_TYPE
                    });

    static final SeaTunnelRowType NATIVE_ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {"dynamic_subject", "message_id", "attributes", "payload"},
                    new SeaTunnelDataType<?>[] {
                        BasicType.STRING_TYPE,
                        BasicType.STRING_TYPE,
                        new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE),
                        PrimitiveByteArrayType.INSTANCE
                    });

    @Test
    void createJsonSerializerGeneratesRepresentativeJsonPayload() {
        NatsJetStreamRequestSerializer serializer =
                NatsJetStreamSinkWriter.createRequestSerializer(
                        JSON_ROW_TYPE, ReadonlyConfig.fromMap(validJsonConfig()));

        NatsJetStreamSinkWriter.PublishRequest request =
                serializer.serialize(new SeaTunnelRow(new Object[] {7, "alice", true}));

        Assertions.assertEquals("orders.events", request.getSubject());
        Assertions.assertNull(request.getHeaders());
        Assertions.assertNull(request.getMessageId());
        Assertions.assertEquals(
                "{\"id\":7,\"name\":\"alice\",\"active\":true}",
                new String(request.getPayload(), StandardCharsets.UTF_8));
    }

    @Test
    void createNativeSerializerBuildsSubjectHeadersPayloadAndMessageId() {
        NatsJetStreamRequestSerializer serializer =
                NatsJetStreamSinkWriter.createRequestSerializer(
                        NATIVE_ROW_TYPE, ReadonlyConfig.fromMap(validNativeConfig()));

        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("tenant", "acme");
        headers.put("trace", "t-1");
        byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
        SeaTunnelRow row =
                new SeaTunnelRow(new Object[] {"payments.created", "msg-1", headers, payload});

        NatsJetStreamSinkWriter.PublishRequest request = serializer.serialize(row);

        Assertions.assertEquals("payments.created", request.getSubject());
        Assertions.assertEquals("msg-1", request.getMessageId());
        Assertions.assertArrayEquals(payload, request.getPayload());
        Assertions.assertNotNull(request.getHeaders());
        Assertions.assertEquals("acme", request.getHeaders().getFirst("tenant"));
        Assertions.assertEquals("t-1", request.getHeaders().getFirst("trace"));
        Assertions.assertEquals(
                "INSERT",
                request.getHeaders().getFirst(NatsJetStreamRequestSerializer.ROW_KIND_HEADER));
    }

    @Test
    void createNativeSerializerFallsBackToConfiguredSubjectAndOptionalFields() {
        Map<String, Object> config = validNativeConfig();
        Map<String, String> mappings = new HashMap<>();
        mappings.put(NatsJetStreamSinkOptions.NATIVE_MAPPING_DATA, "payload");
        config.put(NatsJetStreamSinkOptions.NATIVE_FIELDS.key(), mappings);

        NatsJetStreamRequestSerializer serializer =
                NatsJetStreamSinkWriter.createRequestSerializer(
                        NATIVE_ROW_TYPE, ReadonlyConfig.fromMap(config));

        byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
        NatsJetStreamSinkWriter.PublishRequest request =
                serializer.serialize(new SeaTunnelRow(new Object[] {null, null, null, payload}));

        Assertions.assertEquals("orders.events", request.getSubject());
        Assertions.assertNull(request.getMessageId());
        Assertions.assertNotNull(request.getHeaders());
        Assertions.assertEquals(
                "INSERT",
                request.getHeaders().getFirst(NatsJetStreamRequestSerializer.ROW_KIND_HEADER));
        Assertions.assertArrayEquals(payload, request.getPayload());
    }

    @Test
    void createNativeSerializerSkipsRowKindHeaderWhenDisabled() {
        Map<String, Object> config = validNativeConfig();
        config.put(NatsJetStreamSinkOptions.INCLUDE_ROW_KIND_HEADER.key(), false);

        NatsJetStreamRequestSerializer serializer =
                NatsJetStreamSinkWriter.createRequestSerializer(
                        NATIVE_ROW_TYPE, ReadonlyConfig.fromMap(config));

        NatsJetStreamSinkWriter.PublishRequest request =
                serializer.serialize(
                        new SeaTunnelRow(
                                new Object[] {"payments.created", null, null, bytes("hello")}));

        Assertions.assertNull(request.getHeaders());
    }

    @Test
    void createNativeSerializerRejectsBlankSubjectWithoutFallback() {
        Map<String, Object> config = validNativeConfig();
        config.remove(NatsJetStreamSinkOptions.SUBJECT.key());

        NatsJetStreamRequestSerializer serializer =
                NatsJetStreamSinkWriter.createRequestSerializer(
                        NATIVE_ROW_TYPE, ReadonlyConfig.fromMap(config));

        NatsJetStreamConnectorException exception =
                Assertions.assertThrows(
                        NatsJetStreamConnectorException.class,
                        () ->
                                serializer.serialize(
                                        new SeaTunnelRow(
                                                new Object[] {"   ", null, null, bytes("x")})));

        Assertions.assertTrue(exception.getMessage().contains("field `subject`"));
        Assertions.assertTrue(exception.getMessage().contains("must not be null or blank"));
    }

    @Test
    void createNativeSerializerRejectsWrongSubjectType() {
        NatsJetStreamRequestSerializer serializer =
                NatsJetStreamSinkWriter.createRequestSerializer(
                        NATIVE_ROW_TYPE, ReadonlyConfig.fromMap(validNativeConfig()));

        NatsJetStreamConnectorException exception =
                Assertions.assertThrows(
                        NatsJetStreamConnectorException.class,
                        () ->
                                serializer.serialize(
                                        new SeaTunnelRow(
                                                new Object[] {1, null, null, bytes("x")})));

        Assertions.assertTrue(exception.getMessage().contains("field `subject`"));
        Assertions.assertTrue(exception.getMessage().contains("STRING value"));
    }

    @Test
    void createNativeSerializerRejectsWrongMessageIdType() {
        NatsJetStreamRequestSerializer serializer =
                NatsJetStreamSinkWriter.createRequestSerializer(
                        NATIVE_ROW_TYPE, ReadonlyConfig.fromMap(validNativeConfig()));

        NatsJetStreamConnectorException exception =
                Assertions.assertThrows(
                        NatsJetStreamConnectorException.class,
                        () ->
                                serializer.serialize(
                                        new SeaTunnelRow(
                                                new Object[] {
                                                    "payments.created", 12, null, bytes("x")
                                                })));

        Assertions.assertTrue(exception.getMessage().contains("field `id`"));
        Assertions.assertTrue(exception.getMessage().contains("STRING value"));
    }

    @Test
    void createNativeSerializerRejectsWrongHeadersValueType() {
        NatsJetStreamRequestSerializer serializer =
                NatsJetStreamSinkWriter.createRequestSerializer(
                        NATIVE_ROW_TYPE, ReadonlyConfig.fromMap(validNativeConfig()));

        Map<String, Object> headers = new LinkedHashMap<>();
        headers.put("tenant", 1);

        NatsJetStreamConnectorException exception =
                Assertions.assertThrows(
                        NatsJetStreamConnectorException.class,
                        () ->
                                serializer.serialize(
                                        new SeaTunnelRow(
                                                new Object[] {
                                                    "payments.created", "msg-1", headers, bytes("x")
                                                })));

        Assertions.assertTrue(exception.getMessage().contains("field `headers`"));
        Assertions.assertTrue(exception.getMessage().contains("non-string value"));
    }

    @Test
    void createNativeSerializerRejectsWrongPayloadType() {
        NatsJetStreamRequestSerializer serializer =
                NatsJetStreamSinkWriter.createRequestSerializer(
                        NATIVE_ROW_TYPE, ReadonlyConfig.fromMap(validNativeConfig()));

        NatsJetStreamConnectorException exception =
                Assertions.assertThrows(
                        NatsJetStreamConnectorException.class,
                        () ->
                                serializer.serialize(
                                        new SeaTunnelRow(
                                                new Object[] {
                                                    "payments.created", "msg-1", null, "text"
                                                })));

        Assertions.assertTrue(exception.getMessage().contains("field `data`"));
        Assertions.assertTrue(exception.getMessage().contains("non-null BYTES value"));
    }

    private Map<String, Object> validJsonConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(NatsJetStreamSinkOptions.URL.key(), "nats://127.0.0.1:4222");
        config.put(NatsJetStreamSinkOptions.SUBJECT.key(), "orders.events");
        config.put(NatsJetStreamSinkOptions.FORMAT.key(), NatsJetStreamMessageFormat.JSON.name());
        return config;
    }

    private Map<String, Object> validNativeConfig() {
        Map<String, Object> config = validJsonConfig();
        config.put(NatsJetStreamSinkOptions.FORMAT.key(), NatsJetStreamMessageFormat.NATIVE.name());
        Map<String, String> mappings = new HashMap<>();
        mappings.put(NatsJetStreamSinkOptions.NATIVE_MAPPING_SUBJECT, "dynamic_subject");
        mappings.put(NatsJetStreamSinkOptions.NATIVE_MAPPING_ID, "message_id");
        mappings.put(NatsJetStreamSinkOptions.NATIVE_MAPPING_HEADERS, "attributes");
        mappings.put(NatsJetStreamSinkOptions.NATIVE_MAPPING_DATA, "payload");
        config.put(NatsJetStreamSinkOptions.NATIVE_FIELDS.key(), mappings);
        return config;
    }

    private byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
