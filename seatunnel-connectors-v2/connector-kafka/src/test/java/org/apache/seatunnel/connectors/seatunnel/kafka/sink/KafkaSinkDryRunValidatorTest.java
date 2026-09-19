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

package org.apache.seatunnel.connectors.seatunnel.kafka.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class KafkaSinkDryRunValidatorTest {
    private final SeaTunnelRowType rowType =
            new SeaTunnelRowType(
                    new String[] {"id", "route"},
                    new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});

    @Test
    void testCompatibleFormatUsesUpstreamSchemaAndConfiguredPartition() throws Exception {
        SeaTunnelRowType compatibleType =
                org.apache.seatunnel.format.compatible.debezium.json
                        .CompatibleDebeziumJsonDeserializationSchema.DEBEZIUM_DATA_ROW_TYPE;
        Map<String, Object> options = options("orders");
        options.put("format", "COMPATIBLE_DEBEZIUM_JSON");
        options.put("partition", 1);
        AdminClient admin = mock(AdminClient.class);
        describe(admin, KafkaFuture.completedFuture(Collections.singletonMap("orders", topic())));
        try (MockedStatic<AdminClient> clients = client(admin, new Properties())) {
            validate(options, compatibleType);
            org.apache.kafka.clients.producer.ProducerRecord<byte[], byte[]> record =
                    KafkaSinkSerializer.create(ReadonlyConfig.fromMap(options), compatibleType)
                            .serializeRow(
                                    new SeaTunnelRow(
                                            new Object[] {"record-topic", "key", "value"}));
            assertEquals("orders", record.topic());
            assertEquals(Integer.valueOf(1), record.partition());
        }
    }

    @Test
    void testStaticTopicOnlyUsesMetadataAndPreservesConfiguration() throws Exception {
        AdminClient admin = mock(AdminClient.class);
        describe(admin, KafkaFuture.completedFuture(Collections.singletonMap("orders", topic())));
        Map<String, Object> options = options("orders");
        options.put("partition", 1);
        options.put("semantics", "EXACTLY_ONCE");
        Map<String, Object> clientConfig = new HashMap<>();
        clientConfig.put("bootstrap.servers", "ignored:9092");
        clientConfig.put("sasl.jaas.config", "synthetic-secret");
        clientConfig.put("security.protocol", "SASL_SSL");
        clientConfig.put("sasl.mechanism", "PLAIN");
        clientConfig.put("request.timeout.ms", "60000");
        clientConfig.put("default.api.timeout.ms", "120000");
        options.put("kafka.config", clientConfig);
        Properties captured = new Properties();
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        ClassLoader caller = new ClassLoader(original) {};
        Thread.currentThread().setContextClassLoader(caller);
        try (MockedStatic<AdminClient> clients = client(admin, captured)) {
            validate(options, rowType);
            assertSame(caller, Thread.currentThread().getContextClassLoader());
            assertEquals("localhost:9092", captured.get("bootstrap.servers"));
            assertEquals("synthetic-secret", captured.get("sasl.jaas.config"));
            assertEquals("SASL_SSL", captured.get("security.protocol"));
            assertEquals(30000, captured.get("default.api.timeout.ms"));
            assertEquals(30000, captured.get("request.timeout.ms"));
            assertEquals("120000", clientConfig.get("default.api.timeout.ms"));
            ArgumentCaptor<DescribeTopicsOptions> request =
                    ArgumentCaptor.forClass(DescribeTopicsOptions.class);
            verify(admin).describeTopics(eq(Collections.singleton("orders")), request.capture());
            assertTrue(
                    request.getValue().timeoutMs() > 0 && request.getValue().timeoutMs() <= 30000);
            verify(admin).close(Duration.ofMillis(1));
            verifyNoMoreInteractions(admin);
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    @Test
    void testDynamicTopicChecksBrokerWithoutInventingTarget() throws Exception {
        AdminClient admin = mock(AdminClient.class);
        DescribeClusterResult result = mock(DescribeClusterResult.class);
        when(admin.describeCluster(any(DescribeClusterOptions.class))).thenReturn(result);
        when(result.nodes()).thenReturn(KafkaFuture.completedFuture(Collections.emptyList()));
        try (MockedStatic<AdminClient> clients = client(admin, new Properties())) {
            validate(options("prefix-${route}-${missing}"), rowType);
            verify(admin).describeCluster(any(DescribeClusterOptions.class));
            verify(admin).close(Duration.ofMillis(1));
            verifyNoMoreInteractions(admin);
        }
    }

    @Test
    void testStaticPartitionBounds() throws Exception {
        for (int partition : new int[] {2, 99}) {
            AdminClient admin = mock(AdminClient.class);
            describe(
                    admin,
                    KafkaFuture.completedFuture(Collections.singletonMap("orders", topic())));
            Map<String, Object> options = options("orders");
            options.put("partition", partition);
            try (MockedStatic<AdminClient> clients = client(admin, new Properties())) {
                IllegalArgumentException failure =
                        assertThrows(
                                IllegalArgumentException.class, () -> validate(options, rowType));
                assertTrue(failure.getMessage().contains("partition"));
                verify(admin).close(Duration.ofMillis(1));
            }
        }
    }

    @Test
    void testNegativePartitionRejectedForStaticAndDynamicTopicsWithoutClient() {
        for (String topic : Arrays.asList("orders", "${route}")) {
            Map<String, Object> options = options(topic);
            options.put("partition", -1);
            try (MockedStatic<AdminClient> clients = mockStatic(AdminClient.class)) {
                IllegalArgumentException failure =
                        assertThrows(
                                IllegalArgumentException.class, () -> validate(options, rowType));
                assertTrue(failure.getMessage().contains("partition must not be negative"));
                clients.verifyNoInteractions();
            }
        }
    }

    @Test
    void testNativeIgnoresTopLevelPartitionAndSerializesRecordPartition() throws Exception {
        SeaTunnelRowType nativeType =
                new SeaTunnelRowType(
                        new String[] {"headers", "key", "partition", "timestamp", "value"},
                        new SeaTunnelDataType[] {
                            new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE),
                            PrimitiveByteArrayType.INSTANCE,
                            BasicType.INT_TYPE,
                            BasicType.LONG_TYPE,
                            PrimitiveByteArrayType.INSTANCE
                        });
        Map<String, Object> options = options("orders");
        options.put("format", "NATIVE");
        options.put("partition", 999);
        AdminClient admin = mock(AdminClient.class);
        describe(admin, KafkaFuture.completedFuture(Collections.singletonMap("orders", topic())));
        try (MockedStatic<AdminClient> clients = client(admin, new Properties())) {
            validate(options, nativeType);
            options.put("partition", "ignored-by-native");
            validate(options, nativeType);
            assertEquals(
                    Integer.valueOf(1),
                    KafkaSinkSerializer.create(ReadonlyConfig.fromMap(options), nativeType)
                            .serializeRow(
                                    new SeaTunnelRow(
                                            new Object[] {
                                                Collections.emptyMap(),
                                                new byte[0],
                                                1,
                                                123L,
                                                new byte[0]
                                            }))
                            .partition());
        }
    }

    @Test
    void testLocalSchemaErrorsNeverCreateClient() {
        for (Map<String, Object> options :
                Arrays.asList(
                        options("${unknown}"),
                        withOption("kafka_headers_fields", Collections.singletonList("unknown")),
                        withOption("partition_key_fields", Collections.singletonList("unknown")),
                        withOption(
                                "kafka_message_value_fields", Collections.singletonList("unknown")),
                        withOption("format", "NATIVE"))) {
            try (MockedStatic<AdminClient> clients = mockStatic(AdminClient.class)) {
                IllegalArgumentException failure =
                        assertThrows(
                                IllegalArgumentException.class, () -> validate(options, rowType));
                assertTrue(failure.getMessage().contains("upstream schema"));
                assertNull(failure.getCause());
                clients.verifyNoInteractions();
            }
        }
    }

    @Test
    void testSerializerPreservesFirstFieldTopicAndExplicitPartition() {
        Map<String, Object> options = options("prefix-${route}-${unknown}");
        options.put("partition", 1);
        org.apache.kafka.clients.producer.ProducerRecord<byte[], byte[]> record =
                KafkaSinkSerializer.create(ReadonlyConfig.fromMap(options), rowType)
                        .serializeRow(new SeaTunnelRow(new Object[] {7, "actual-target"}));
        assertEquals("actual-target", record.topic());
        assertEquals(Integer.valueOf(1), record.partition());
        assertTrue(
                new String(record.value(), java.nio.charset.StandardCharsets.UTF_8).contains("7"));
    }

    @Test
    void testRemoteFailuresAreSanitizedAndCloseClient() {
        for (RuntimeException cause :
                Arrays.asList(
                        new AuthenticationException("password=synthetic-secret"),
                        new TopicAuthorizationException("password=synthetic-secret"),
                        new UnknownTopicOrPartitionException("password=synthetic-secret"))) {
            AdminClient admin = mock(AdminClient.class);
            KafkaFutureImpl<Map<String, TopicDescription>> future = new KafkaFutureImpl<>();
            future.completeExceptionally(cause);
            describe(admin, future);
            try (MockedStatic<AdminClient> clients = client(admin, new Properties())) {
                IllegalArgumentException failure =
                        assertThrows(
                                IllegalArgumentException.class,
                                () -> validate(options("orders"), rowType));
                assertFalse(failure.getMessage().contains("synthetic-secret"));
                assertNull(failure.getCause());
                assertEquals(0, failure.getSuppressed().length);
                verify(admin).close(Duration.ofMillis(1));
            }
        }
    }

    @Test
    void testClientCreationFailureIsSanitizedAndRestoresClassLoader() {
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        try (MockedStatic<AdminClient> clients = mockStatic(AdminClient.class)) {
            clients.when(() -> AdminClient.create(any(Properties.class)))
                    .thenThrow(new IllegalArgumentException("password=synthetic-secret"));
            IllegalArgumentException failure =
                    assertThrows(
                            IllegalArgumentException.class,
                            () -> validate(options("orders"), rowType));
            assertFalse(failure.getMessage().contains("synthetic-secret"));
            assertNull(failure.getCause());
            assertSame(original, Thread.currentThread().getContextClassLoader());
        }
    }

    @Test
    void testTimeoutIsBoundedAndClientClosed() throws Exception {
        AdminClient admin = mock(AdminClient.class);
        KafkaFuture<Map<String, TopicDescription>> future = mock(KafkaFuture.class);
        when(future.get(anyLong(), eq(TimeUnit.MILLISECONDS)))
                .thenThrow(new TimeoutException("synthetic-secret"));
        describe(admin, future);
        Map<String, Object> options = options("orders");
        Map<String, Object> clientConfig = new HashMap<>();
        clientConfig.put("request.timeout.ms", " 5000 ");
        clientConfig.put("default.api.timeout.ms", " 5000 ");
        options.put("kafka.config", clientConfig);
        Properties captured = new Properties();
        try (MockedStatic<AdminClient> clients = client(admin, captured)) {
            IllegalArgumentException failure =
                    assertThrows(IllegalArgumentException.class, () -> validate(options, rowType));
            assertTrue(failure.getMessage().contains("timed out"));
            assertNull(failure.getCause());
            ArgumentCaptor<Long> timeout = ArgumentCaptor.forClass(Long.class);
            verify(future).get(timeout.capture(), eq(TimeUnit.MILLISECONDS));
            assertTrue(timeout.getValue() > 0 && timeout.getValue() <= 5000);
            assertEquals(5000, captured.get("default.api.timeout.ms"));
            verify(admin).close(Duration.ofMillis(1));
        }
    }

    @Test
    void testInvalidClientTimeoutsDoNotCreateClient() {
        for (Object value : Arrays.asList(-1, "synthetic-secret", 5000)) {
            Map<String, Object> options = options("orders");
            options.put("kafka.config", Collections.singletonMap("default.api.timeout.ms", value));
            try (MockedStatic<AdminClient> clients = mockStatic(AdminClient.class)) {
                IllegalArgumentException failure =
                        assertThrows(
                                IllegalArgumentException.class, () -> validate(options, rowType));
                assertTrue(failure.getMessage().contains("invalid Kafka client configuration"));
                assertNull(failure.getCause());
                clients.verifyNoInteractions();
            }
        }
    }

    @Test
    void testInterruptionIsPreservedAndClientClosed() throws Exception {
        AdminClient admin = mock(AdminClient.class);
        KafkaFuture<Map<String, TopicDescription>> future = mock(KafkaFuture.class);
        when(future.get(anyLong(), eq(TimeUnit.MILLISECONDS)))
                .thenThrow(new InterruptedException("synthetic-secret"));
        describe(admin, future);
        try (MockedStatic<AdminClient> clients = client(admin, new Properties())) {
            try {
                assertThrows(
                        InterruptedException.class, () -> validate(options("orders"), rowType));
                assertTrue(Thread.currentThread().isInterrupted());
                verify(admin).close(Duration.ofMillis(1));
            } finally {
                Thread.interrupted();
            }
        }
    }

    @Test
    void testAlreadyInterruptedDoesNotCreateClient() {
        try (MockedStatic<AdminClient> clients = mockStatic(AdminClient.class)) {
            Thread.currentThread().interrupt();
            try {
                assertThrows(
                        InterruptedException.class, () -> validate(options("orders"), rowType));
                assertTrue(Thread.currentThread().isInterrupted());
                clients.verifyNoInteractions();
            } finally {
                Thread.interrupted();
            }
        }
    }

    private Map<String, Object> options(String topic) {
        Map<String, Object> options = new HashMap<>();
        options.put("bootstrap.servers", "localhost:9092");
        options.put("topic", topic);
        return options;
    }

    private Map<String, Object> withOption(String key, Object value) {
        Map<String, Object> options = options("orders");
        options.put(key, value);
        return options;
    }

    private void validate(Map<String, Object> options, SeaTunnelRowType type) throws Exception {
        TableSchema.Builder schema = TableSchema.builder();
        for (int i = 0; i < type.getTotalFields(); i++) {
            schema.column(
                    PhysicalColumn.of(
                            type.getFieldName(i), type.getFieldType(i), 0, true, null, null));
        }
        CatalogTable table =
                CatalogTable.of(
                        TableIdentifier.of("test", "db", "input"),
                        schema.build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "");
        new KafkaSinkFactory()
                .validateConnectionForDryRun(
                        new TableSinkFactoryContext(
                                table,
                                ReadonlyConfig.fromMap(options),
                                getClass().getClassLoader()));
    }

    private TopicDescription topic() {
        return new TopicDescription(
                "orders",
                false,
                Arrays.asList(
                        new TopicPartitionInfo(
                                0, null, Collections.emptyList(), Collections.emptyList()),
                        new TopicPartitionInfo(
                                1, null, Collections.emptyList(), Collections.emptyList())));
    }

    private void describe(AdminClient admin, KafkaFuture<Map<String, TopicDescription>> future) {
        DescribeTopicsResult result = mock(DescribeTopicsResult.class);
        when(admin.describeTopics(anySet(), any(DescribeTopicsOptions.class))).thenReturn(result);
        when(result.allTopicNames()).thenReturn(future);
    }

    private MockedStatic<AdminClient> client(AdminClient admin, Properties captured) {
        MockedStatic<AdminClient> clients = mockStatic(AdminClient.class);
        clients.when(() -> AdminClient.create(any(Properties.class)))
                .thenAnswer(
                        invocation -> {
                            assertSame(
                                    KafkaSinkFactory.class.getClassLoader(),
                                    Thread.currentThread().getContextClassLoader());
                            captured.putAll(invocation.getArgument(0));
                            return admin;
                        });
        return clients;
    }
}
