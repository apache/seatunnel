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

package org.apache.seatunnel.connectors.seatunnel.kafka.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.PatternSyntaxException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class KafkaSourceDryRunValidatorTest {

    private final KafkaSourceFactory factory = new KafkaSourceFactory();

    @Test
    void testLiteralTopicsUseOnlyMetadataAndPreserveClientConfiguration() throws Exception {
        Map<String, Object> options = options("orders,customers");
        Map<String, Object> kafkaConfig = new HashMap<>();
        kafkaConfig.put("bootstrap.servers", "ignored:9092");
        kafkaConfig.put("security.protocol", "SASL_SSL");
        kafkaConfig.put("sasl.mechanism", "PLAIN");
        kafkaConfig.put("sasl.jaas.config", "synthetic-jaas-config");
        kafkaConfig.put("client.id", "configured-client");
        kafkaConfig.put("default.api.timeout.ms", "120000");
        kafkaConfig.put("request.timeout.ms", "60000");
        options.put("kafka.config", kafkaConfig);
        AdminClient admin = mock(AdminClient.class);
        describe(admin, KafkaFuture.completedFuture(Collections.emptyMap()));
        Properties properties = new Properties();
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        ClassLoader caller = new ClassLoader(original) {};
        Thread.currentThread().setContextClassLoader(caller);
        try (MockedStatic<AdminClient> clients = createClient(admin, properties)) {
            doAnswer(
                            invocation -> {
                                assertSame(
                                        KafkaSourceFactory.class.getClassLoader(),
                                        Thread.currentThread().getContextClassLoader());
                                return null;
                            })
                    .when(admin)
                    .close(any(Duration.class));
            validate(options);
            assertSame(caller, Thread.currentThread().getContextClassLoader());
            assertEquals("localhost:9092", properties.get("bootstrap.servers"));
            for (String key :
                    Arrays.asList(
                            "security.protocol",
                            "sasl.mechanism",
                            "sasl.jaas.config",
                            "client.id")) {
                assertEquals(kafkaConfig.get(key), properties.get(key));
            }
            assertEquals(30000, properties.get("default.api.timeout.ms"));
            assertEquals(30000, properties.get("request.timeout.ms"));
            assertEquals("120000", kafkaConfig.get("default.api.timeout.ms"));
            assertEquals("ignored:9092", kafkaConfig.get("bootstrap.servers"));
            ArgumentCaptor<DescribeTopicsOptions> request =
                    ArgumentCaptor.forClass(DescribeTopicsOptions.class);
            verify(admin)
                    .describeTopics(
                            eq(new HashSet<>(Arrays.asList("orders", "customers"))),
                            request.capture());
            assertTrue(request.getValue().timeoutMs() > 0);
            assertTrue(request.getValue().timeoutMs() <= 30000);
            verify(admin).close(Duration.ofMillis(1));
            verifyNoMoreInteractions(admin);
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"tables_configs", "table_list"})
    void testMixedTableTopicsAndPatternsUseRuntimeMatching(String tableOption) throws Exception {
        Map<String, Object> options = options("unused");
        options.remove("topic");
        Map<String, Object> pattern = new HashMap<>();
        pattern.put("topic", "orders-[0-9]+");
        pattern.put("pattern", true);
        options.put(
                tableOption,
                Arrays.asList(Collections.singletonMap("topic", "fixed,orders-1"), pattern));
        AdminClient admin = mock(AdminClient.class);
        list(admin, new HashSet<>(Arrays.asList("orders-1", "orders-2", "prefix-orders-3")));
        describe(admin, KafkaFuture.completedFuture(Collections.emptyMap()));
        try (MockedStatic<AdminClient> clients = createClient(admin, new Properties())) {
            validate(options);
            ArgumentCaptor<ListTopicsOptions> request =
                    ArgumentCaptor.forClass(ListTopicsOptions.class);
            verify(admin).listTopics(request.capture());
            assertFalse(request.getValue().shouldListInternal());
            verify(admin)
                    .describeTopics(
                            eq(new HashSet<>(Arrays.asList("fixed", "orders-1", "orders-2"))),
                            any(DescribeTopicsOptions.class));
            verify(admin).close(Duration.ofMillis(1));
            verifyNoMoreInteractions(admin);
        }
    }

    @Test
    void testPatternWithoutCurrentMatchesStillChecksConnectivity() throws Exception {
        Map<String, Object> options = options("future-.*");
        options.put("pattern", true);
        AdminClient admin = mock(AdminClient.class);
        list(admin, Collections.singleton("other"));
        try (MockedStatic<AdminClient> clients = createClient(admin, new Properties())) {
            validate(options);
            verify(admin).listTopics(any(ListTopicsOptions.class));
            verify(admin).close(Duration.ofMillis(1));
            verifyNoMoreInteractions(admin);
        }
    }

    @Test
    void testMetadataFailuresPropagateAndCloseClient() {
        for (RuntimeException failure :
                Arrays.asList(
                        new AuthenticationException("authentication failed"),
                        new TopicAuthorizationException(Collections.singleton("orders")),
                        new UnknownTopicOrPartitionException("missing topic"))) {
            AdminClient admin = mock(AdminClient.class);
            KafkaFutureImpl<Map<String, TopicDescription>> future = new KafkaFutureImpl<>();
            future.completeExceptionally(failure);
            describe(admin, future);
            try (MockedStatic<AdminClient> clients = createClient(admin, new Properties())) {
                ExecutionException exception =
                        assertThrows(ExecutionException.class, () -> validate(options("orders")));
                assertSame(failure, exception.getCause());
                verify(admin).close(Duration.ofMillis(1));
            }
        }
    }

    @Test
    void testPatternListingFailureDoesNotDescribeTopics() {
        AdminClient admin = mock(AdminClient.class);
        ListTopicsResult result = mock(ListTopicsResult.class);
        KafkaFutureImpl<Set<String>> future = new KafkaFutureImpl<>();
        future.completeExceptionally(new AuthenticationException("authentication failed"));
        when(admin.listTopics(any(ListTopicsOptions.class))).thenReturn(result);
        when(result.names()).thenReturn(future);
        Map<String, Object> options = options("orders-.*");
        options.put("pattern", true);
        try (MockedStatic<AdminClient> clients = createClient(admin, new Properties())) {
            assertThrows(ExecutionException.class, () -> validate(options));
            verify(admin, never()).describeTopics(anySet(), any(DescribeTopicsOptions.class));
            verify(admin).close(Duration.ofMillis(1));
        }
    }

    @Test
    void testMetadataWaitHonorsSmallerConfiguredDeadline() throws Exception {
        AdminClient admin = mock(AdminClient.class);
        KafkaFuture<Map<String, TopicDescription>> future = mock(KafkaFuture.class);
        when(future.get(anyLong(), eq(TimeUnit.MILLISECONDS)))
                .thenThrow(new TimeoutException("metadata wait timed out"));
        describe(admin, future);
        Properties properties = new Properties();
        Map<String, Object> options = options("orders");
        Map<String, Object> kafkaConfig = new HashMap<>();
        kafkaConfig.put("default.api.timeout.ms", 5000);
        kafkaConfig.put("request.timeout.ms", 5000);
        options.put("kafka.config", kafkaConfig);
        try (MockedStatic<AdminClient> clients = createClient(admin, properties)) {
            TimeoutException exception =
                    assertThrows(TimeoutException.class, () -> validate(options));
            assertEquals("metadata wait timed out", exception.getMessage());
            ArgumentCaptor<Long> timeout = ArgumentCaptor.forClass(Long.class);
            verify(future).get(timeout.capture(), eq(TimeUnit.MILLISECONDS));
            assertTrue(timeout.getValue() > 0);
            assertTrue(timeout.getValue() <= 5000);
            assertEquals(5000, properties.get("default.api.timeout.ms"));
            assertEquals(5000, properties.get("request.timeout.ms"));
            verify(admin).close(Duration.ofMillis(1));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"default.api.timeout.ms", "request.timeout.ms"})
    void testTimeoutWhitespaceMatchesKafkaConfigurationParsing(String key) throws Exception {
        Map<String, Object> options = options("orders");
        Map<String, Object> kafkaConfig = new HashMap<>();
        kafkaConfig.put("request.timeout.ms", 5000);
        kafkaConfig.put(key, " 5000 ");
        options.put("kafka.config", kafkaConfig);
        AdminClient admin = mock(AdminClient.class);
        describe(admin, KafkaFuture.completedFuture(Collections.emptyMap()));
        Properties properties = new Properties();
        try (MockedStatic<AdminClient> clients = createClient(admin, properties)) {
            validate(options);
            assertEquals(5000, properties.get(key));
            verify(admin).close(Duration.ofMillis(1));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"default.api.timeout.ms", "request.timeout.ms"})
    void testInvalidTimeoutDoesNotCreateClient(String key) {
        for (String value : Arrays.asList("-1", "invalid", "9999999999999")) {
            Map<String, Object> options = options("orders");
            options.put("kafka.config", Collections.singletonMap(key, value));
            try (MockedStatic<AdminClient> clients = mockStatic(AdminClient.class)) {
                ConfigException exception =
                        assertThrows(ConfigException.class, () -> validate(options));
                assertTrue(exception.getMessage().contains(key));
                clients.verifyNoInteractions();
            }
        }
    }

    @Test
    void testExplicitApiTimeoutBelowOriginalRequestTimeoutFailsBeforeClientCreation() {
        for (Integer requestTimeout : Arrays.asList(null, 10000)) {
            Map<String, Object> kafkaConfig = new HashMap<>();
            kafkaConfig.put("default.api.timeout.ms", 5000);
            if (requestTimeout != null) {
                kafkaConfig.put("request.timeout.ms", requestTimeout);
            }
            Map<String, Object> options = options("orders");
            options.put("kafka.config", kafkaConfig);
            try (MockedStatic<AdminClient> clients = mockStatic(AdminClient.class)) {
                ConfigException exception =
                        assertThrows(ConfigException.class, () -> validate(options));
                assertTrue(exception.getMessage().contains("default.api.timeout.ms"));
                assertTrue(exception.getMessage().contains("request.timeout.ms"));
                clients.verifyNoInteractions();
            }
        }
    }

    @Test
    void testUnspecifiedApiTimeoutAllowsLongerRequestTimeoutBeforeCapping() throws Exception {
        Map<String, Object> options = options("orders");
        options.put("kafka.config", Collections.singletonMap("request.timeout.ms", 120000));
        AdminClient admin = mock(AdminClient.class);
        describe(admin, KafkaFuture.completedFuture(Collections.emptyMap()));
        Properties properties = new Properties();
        try (MockedStatic<AdminClient> clients = createClient(admin, properties)) {
            validate(options);
            assertEquals(30000, properties.get("default.api.timeout.ms"));
            assertEquals(30000, properties.get("request.timeout.ms"));
        }
    }

    @Test
    void testMetadataFailureRestoresCallerClassLoader() {
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        ClassLoader caller = new ClassLoader(original) {};
        AdminClient admin = mock(AdminClient.class);
        KafkaFutureImpl<Map<String, TopicDescription>> future = new KafkaFutureImpl<>();
        future.completeExceptionally(new UnknownTopicOrPartitionException("missing topic"));
        describe(admin, future);
        Thread.currentThread().setContextClassLoader(caller);
        try (MockedStatic<AdminClient> clients = createClient(admin, new Properties())) {
            assertThrows(ExecutionException.class, () -> validate(options("orders")));
            assertSame(caller, Thread.currentThread().getContextClassLoader());
            verify(admin).close(Duration.ofMillis(1));
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    @Test
    void testInterruptionIsRestoredAndClientClosed() throws Exception {
        AdminClient admin = mock(AdminClient.class);
        KafkaFuture<Map<String, TopicDescription>> future = mock(KafkaFuture.class);
        when(future.get(anyLong(), eq(TimeUnit.MILLISECONDS)))
                .thenThrow(new InterruptedException());
        describe(admin, future);
        try (MockedStatic<AdminClient> clients = createClient(admin, new Properties())) {
            try {
                assertThrows(InterruptedException.class, () -> validate(options("orders")));
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
                assertThrows(InterruptedException.class, () -> validate(options("orders")));
                assertTrue(Thread.currentThread().isInterrupted());
                clients.verifyNoInteractions();
            } finally {
                Thread.interrupted();
            }
        }
    }

    @Test
    void testInvalidPatternAndEmptyTopicSetDoNotCreateClient() {
        try (MockedStatic<AdminClient> clients = mockStatic(AdminClient.class)) {
            Map<String, Object> options = options("[");
            options.put("pattern", true);
            assertThrows(PatternSyntaxException.class, () -> validate(options));
            assertThrows(IllegalArgumentException.class, () -> validate(options(",,")));
            clients.verifyNoInteractions();
        }
    }

    @Test
    void testClientCreationFailureRestoresContextClassLoader() {
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        try (MockedStatic<AdminClient> clients = mockStatic(AdminClient.class)) {
            clients.when(() -> AdminClient.create(any(Properties.class)))
                    .thenThrow(new IllegalArgumentException("invalid client configuration"));
            assertThrows(IllegalArgumentException.class, () -> validate(options("orders")));
            assertSame(original, Thread.currentThread().getContextClassLoader());
        }
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "json",
                "text",
                "native",
                "canal_json",
                "debezium_json",
                "maxwell_json",
                "ogg_json",
                "compatible_kafka_connect_json"
            })
    void testInferredSchemaMatchesRuntimeIncludingHeadersAndEventTime(String format) {
        Map<String, Object> options = options("orders");
        options.put("format", format);
        options.put(
                "schema",
                Collections.singletonMap("fields", Collections.singletonMap("id", "int")));
        options.put("kafka_headers_fields", Collections.singletonList("trace_id"));
        assertSchemaParity(options);
    }

    @Test
    void testDefaultSchemaIsRealRuntimeContentSchema() {
        List<CatalogTable> tables = assertSchemaParity(options("orders"));
        assertEquals("content", tables.get(0).getSeaTunnelRowType().getFieldName(0));
        assertEquals(BasicType.STRING_TYPE, tables.get(0).getSeaTunnelRowType().getFieldType(0));
    }

    @Test
    void testAvroSchemaMatchesRuntime() {
        Map<String, Object> options = options("orders");
        options.put("format", "avro");
        options.put(
                "schema",
                Collections.singletonMap("fields", Collections.singletonMap("id", "int")));
        options.put(
                "avro_schema",
                "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}");
        options.put("strip_schema_registry_header", true);
        assertSchemaParity(options);
    }

    @Test
    void testProtobufSchemaMatchesRuntime() {
        Map<String, Object> options = options("orders");
        options.put("format", "protobuf");
        options.put(
                "schema",
                Collections.singletonMap("fields", Collections.singletonMap("id", "int")));
        options.put("protobuf_message_name", "Order");
        options.put("protobuf_schema", "syntax = \"proto3\"; message Order { int32 id = 1; }");
        assertSchemaParity(options);
    }

    @ParameterizedTest
    @ValueSource(strings = {"tables_configs", "table_list"})
    void testInferredMultiTableSchemasMatchRuntime(String tableOption) {
        Map<String, Object> options = options("unused");
        options.remove("topic");
        Map<String, Object> second = new HashMap<>();
        second.put("topic", "second");
        second.put("format", "native");
        options.put(tableOption, Arrays.asList(Collections.singletonMap("topic", "first"), second));
        assertEquals(2, assertSchemaParity(options).size());
    }

    private List<CatalogTable> assertSchemaParity(Map<String, Object> options) {
        ReadonlyConfig config = ReadonlyConfig.fromMap(options);
        try (MockedStatic<AdminClient> clients = mockStatic(AdminClient.class)) {
            List<CatalogTable> inferred =
                    factory.inferSchemaForDryRun(
                            new TableSourceFactoryContext(config, getClass().getClassLoader()));
            List<CatalogTable> runtime = new KafkaSource(config).getProducedCatalogTables();
            assertEquals(runtime.size(), inferred.size());
            for (int i = 0; i < runtime.size(); i++) {
                CatalogTable expected = runtime.get(i);
                CatalogTable actual = inferred.get(i);
                assertEquals(expected.getTableId(), actual.getTableId());
                assertEquals(expected.getTableSchema(), actual.getTableSchema());
                assertEquals(expected.getSeaTunnelRowType(), actual.getSeaTunnelRowType());
                assertEquals(expected.getMetadataSchema(), actual.getMetadataSchema());
                assertEquals(expected.getOptions(), actual.getOptions());
                assertEquals(expected.getPartitionKeys(), actual.getPartitionKeys());
            }
            clients.verifyNoInteractions();
            return inferred;
        }
    }

    private void validate(Map<String, Object> options) throws Exception {
        TableSourceFactoryContext context =
                new TableSourceFactoryContext(
                        ReadonlyConfig.fromMap(options), getClass().getClassLoader());
        factory.validateConnectionForDryRun(context, factory.inferSchemaForDryRun(context));
    }

    private Map<String, Object> options(String topic) {
        Map<String, Object> options = new HashMap<>();
        options.put("bootstrap.servers", "localhost:9092");
        options.put("topic", topic);
        return options;
    }

    private MockedStatic<AdminClient> createClient(AdminClient admin, Properties captured) {
        MockedStatic<AdminClient> clients = mockStatic(AdminClient.class);
        clients.when(() -> AdminClient.create(any(Properties.class)))
                .thenAnswer(
                        invocation -> {
                            assertSame(
                                    KafkaSourceFactory.class.getClassLoader(),
                                    Thread.currentThread().getContextClassLoader());
                            captured.putAll(invocation.getArgument(0));
                            return admin;
                        });
        return clients;
    }

    private void describe(AdminClient admin, KafkaFuture<Map<String, TopicDescription>> future) {
        DescribeTopicsResult result = mock(DescribeTopicsResult.class);
        when(admin.describeTopics(anySet(), any(DescribeTopicsOptions.class))).thenReturn(result);
        when(result.allTopicNames()).thenReturn(future);
    }

    private void list(AdminClient admin, Set<String> topics) {
        ListTopicsResult result = mock(ListTopicsResult.class);
        when(admin.listTopics(any(ListTopicsOptions.class))).thenReturn(result);
        when(result.names()).thenReturn(KafkaFuture.completedFuture(topics));
    }
}
