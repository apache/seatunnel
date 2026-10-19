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

package org.apache.seatunnel.connectors.seatunnel.clickhouse;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.table.factory.SupportSinkDryRunValidation;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.exception.ClickhouseConnectorException;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.client.ClickhouseSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.file.ClickhouseFileSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.source.ClickhouseSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseRecord;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseValue;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ClickhouseFactoryTest {

    @Test
    public void testSinkSupportsConnectDryRun() {
        Assertions.assertTrue(new ClickhouseSinkFactory() instanceof SupportSinkDryRunValidation);
    }

    @Test
    public void testSinkDryRunUsesMetadataAndClosesResources() throws Exception {
        Map<String, Object> config = createValidSinkConfig();
        config.put("table", "quoted'\\table");
        Map<String, String> properties = new HashMap<>();
        properties.put("connect_timeout", "250");
        properties.put("socket_timeout", "0");
        properties.put("retry", "100");
        properties.put("max_execution_time", "2");
        properties.put("failover", "100");
        properties.put("ssl", "true");
        properties.put("sslrootcert", "/test-only/ca.pem");
        properties.put("session_id", "existing-session");
        config.put("clickhouse.config", properties);
        ClickHouseClient client = mock(ClickHouseClient.class);
        ClickHouseRequest<?> request = mock(ClickHouseRequest.class, Mockito.RETURNS_SELF);
        ClickHouseResponse response = metadataResponse(1);
        when(client.connect(any(ClickHouseNode.class))).thenAnswer(invocation -> request);
        when(request.executeAndWait()).thenReturn(response);
        try (MockedStatic<ClickHouseClient> clients = Mockito.mockStatic(ClickHouseClient.class)) {
            clients.when(() -> ClickHouseClient.newInstance(any())).thenReturn(client);
            validateSinkDryRun(config);
        }
        ArgumentCaptor<ClickHouseNode> node = ArgumentCaptor.forClass(ClickHouseNode.class);
        verify(client).connect(node.capture());
        Assertions.assertEquals("localhost", node.getValue().getHost());
        Assertions.assertEquals(8123, node.getValue().getPort());
        Assertions.assertEquals("250", node.getValue().getOptions().get("connect_timeout"));
        Assertions.assertEquals("10000", node.getValue().getOptions().get("socket_timeout"));
        Assertions.assertEquals("0", node.getValue().getOptions().get("retry"));
        Assertions.assertEquals("2", node.getValue().getOptions().get("max_execution_time"));
        Assertions.assertEquals("0", node.getValue().getOptions().get("failover"));
        Assertions.assertEquals("", node.getValue().getOptions().get("session_id"));
        Assertions.assertEquals("true", node.getValue().getOptions().get("ssl"));
        Assertions.assertEquals(
                "/test-only/ca.pem", node.getValue().getOptions().get("sslrootcert"));
        Assertions.assertEquals("100", properties.get("retry"));
        verify(request)
                .query(
                        "SELECT count() FROM system.tables WHERE database = :database AND name = :table");
        verify(request).params(new Object[] {"default", "quoted'\\table"});
        verify(response).close();
        verify(client).close();
    }

    @Test
    public void testSinkDryRunRespectsSchemaSaveModesWithoutCreatingTables() throws Exception {
        for (SchemaSaveMode mode : SchemaSaveMode.values()) {
            Map<String, Object> config = createValidSinkConfig();
            config.put("schema_save_mode", mode.name());
            config.put("data_save_mode", "CUSTOM_PROCESSING");
            config.put("custom_sql", "DROP TABLE must_not_execute");
            ClickHouseClient client = mock(ClickHouseClient.class);
            ClickHouseRequest<?> request = mock(ClickHouseRequest.class, Mockito.RETURNS_SELF);
            ClickHouseResponse response = metadataResponse(0);
            when(client.connect(any(ClickHouseNode.class))).thenAnswer(invocation -> request);
            when(request.executeAndWait()).thenReturn(response);
            try (MockedStatic<ClickHouseClient> clients =
                    Mockito.mockStatic(ClickHouseClient.class)) {
                clients.when(() -> ClickHouseClient.newInstance(any())).thenReturn(client);
                if (mode == SchemaSaveMode.ERROR_WHEN_SCHEMA_NOT_EXIST) {
                    ClickhouseConnectorException error =
                            Assertions.assertThrows(
                                    ClickhouseConnectorException.class,
                                    () -> validateSinkDryRun(config));
                    Assertions.assertTrue(
                            error.getMessage().contains("ERROR_WHEN_SCHEMA_NOT_EXIST"));
                } else {
                    validateSinkDryRun(config);
                }
            }
            verify(request, never()).query("DROP TABLE must_not_execute");
            verify(response).close();
            verify(client).close();
        }
    }

    @Test
    public void testSinkDryRunSanitizesQueryAndCloseFailures() throws Exception {
        for (boolean failQuery : new boolean[] {true, false}) {
            ClickHouseClient client = mock(ClickHouseClient.class);
            ClickHouseRequest<?> request = mock(ClickHouseRequest.class, Mockito.RETURNS_SELF);
            ClickHouseResponse response = metadataResponse(0);
            when(client.connect(any(ClickHouseNode.class))).thenAnswer(invocation -> request);
            if (failQuery) {
                when(request.executeAndWait())
                        .thenThrow(new IllegalStateException("private-password"));
            } else {
                when(request.executeAndWait()).thenReturn(response);
                Mockito.doThrow(new IllegalStateException("private-password"))
                        .when(response)
                        .close();
            }
            Mockito.doThrow(new IllegalStateException("private-token")).when(client).close();
            try (MockedStatic<ClickHouseClient> clients =
                    Mockito.mockStatic(ClickHouseClient.class)) {
                clients.when(() -> ClickHouseClient.newInstance(any())).thenReturn(client);
                ClickhouseConnectorException error =
                        Assertions.assertThrows(
                                ClickhouseConnectorException.class,
                                () -> validateSinkDryRun(createValidSinkConfig()));
                StringWriter trace = new StringWriter();
                error.printStackTrace(new PrintWriter(trace));
                Assertions.assertFalse(trace.toString().contains("private-password"));
                Assertions.assertFalse(trace.toString().contains("private-token"));
                Assertions.assertTrue(error.getMessage().contains("access to system.tables"));
            }
            verify(client).close();
        }
    }

    @Test
    public void testSinkDryRunRejectsUnsafeOptionsAndUnresolvedCredentialsBeforeConnecting() {
        try (MockedStatic<ClickHouseClient> clients = Mockito.mockStatic(ClickHouseClient.class)) {
            Map<String, Object> config = createValidSinkConfig();
            config.put(
                    "clickhouse.config",
                    Collections.singletonMap("custom_http_params", "query=SELECT 1"));
            Assertions.assertThrows(
                    ClickhouseConnectorException.class, () -> validateSinkDryRun(config));
            config.put(
                    "clickhouse.config",
                    Collections.singletonMap(
                            "custom_http_headers", "x-clickhouse-query-id=existing-query"));
            Assertions.assertThrows(
                    ClickhouseConnectorException.class, () -> validateSinkDryRun(config));
            config.remove("clickhouse.config");
            config.put("username", "non-default-user");
            config.put("password", "");
            Assertions.assertThrows(
                    ClickhouseConnectorException.class, () -> validateSinkDryRun(config));
            config.put("username", "");
            config.put("password", "password");
            Assertions.assertThrows(
                    ClickhouseConnectorException.class, () -> validateSinkDryRun(config));
            clients.verifyNoInteractions();
        }
    }

    @Test
    public void testSinkDryRunPreservesInterruption() throws Exception {
        try (MockedStatic<ClickHouseClient> clients = Mockito.mockStatic(ClickHouseClient.class)) {
            Thread.currentThread().interrupt();
            try {
                Assertions.assertThrows(
                        ClickhouseConnectorException.class,
                        () -> validateSinkDryRun(createValidSinkConfig()));
                Assertions.assertTrue(Thread.currentThread().isInterrupted());
                clients.verifyNoInteractions();
            } finally {
                Thread.interrupted();
            }
            ClickHouseClient client = mock(ClickHouseClient.class, Mockito.CALLS_REAL_METHODS);
            when(client.getConfig()).thenReturn(new ClickHouseConfig());
            CompletableFuture<ClickHouseResponse> response = mock(CompletableFuture.class);
            when(response.get(anyLong(), any(TimeUnit.class)))
                    .thenThrow(new InterruptedException("interrupted"));
            when(client.execute(any(ClickHouseRequest.class))).thenReturn(response);
            clients.when(() -> ClickHouseClient.newInstance(any())).thenReturn(client);
            try {
                Assertions.assertThrows(
                        ClickhouseConnectorException.class,
                        () -> validateSinkDryRun(createValidSinkConfig()));
                Assertions.assertTrue(Thread.currentThread().isInterrupted());
            } finally {
                Thread.interrupted();
            }
            verify(client).close();
        }
    }

    private ClickHouseResponse metadataResponse(int count) {
        ClickHouseResponse response = mock(ClickHouseResponse.class);
        ClickHouseRecord record = mock(ClickHouseRecord.class);
        ClickHouseValue value = mock(ClickHouseValue.class);
        when(response.firstRecord()).thenReturn(record);
        when(record.getValue(0)).thenReturn(value);
        when(value.asInteger()).thenReturn(count);
        return response;
    }

    private void validateSinkDryRun(Map<String, Object> config) {
        new ClickhouseSinkFactory()
                .validateConnectionForDryRun(
                        new TableSinkFactoryContext(
                                null, ReadonlyConfig.fromMap(config), getClass().getClassLoader()));
    }

    private Map<String, Object> createValidSinkConfig() {
        Map<String, Object> config = createValidSourceConfig();
        config.put("database", "default");
        config.put("table", "target");
        return config;
    }

    private void validateSource(Map<String, Object> configMap) {
        ClickhouseSourceFactory factory = new ClickhouseSourceFactory();
        ConfigValidator.of(ReadonlyConfig.fromMap(configMap)).validate(factory.optionRule());
    }

    private Map<String, Object> createValidSourceConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(ClickhouseBaseOptions.HOST.key(), "localhost:8123");
        config.put(ClickhouseBaseOptions.USERNAME.key(), "default");
        config.put(ClickhouseBaseOptions.PASSWORD.key(), "password");
        return config;
    }

    @Test
    public void testSourceHostValidation() {
        Map<String, Object> validConfig = createValidSourceConfig();
        Assertions.assertDoesNotThrow(() -> validateSource(validConfig));

        Map<String, Object> missingHost = createValidSourceConfig();
        missingHost.remove(ClickhouseBaseOptions.HOST.key());
        Assertions.assertThrows(OptionValidationException.class, () -> validateSource(missingHost));

        Map<String, Object> emptyHost = createValidSourceConfig();
        emptyHost.put(ClickhouseBaseOptions.HOST.key(), "");
        Assertions.assertThrows(OptionValidationException.class, () -> validateSource(emptyHost));

        Map<String, Object> whitespaceHost = createValidSourceConfig();
        whitespaceHost.put(ClickhouseBaseOptions.HOST.key(), "   ");
        Assertions.assertThrows(
                OptionValidationException.class, () -> validateSource(whitespaceHost));

        Map<String, Object> paddedHost = createValidSourceConfig();
        paddedHost.put(ClickhouseBaseOptions.HOST.key(), "  localhost:8123  ");
        Assertions.assertDoesNotThrow(() -> validateSource(paddedHost));
    }

    @Test
    public void testOptionRule() {
        Assertions.assertNotNull((new ClickhouseSourceFactory()).optionRule());
        Assertions.assertNotNull((new ClickhouseSinkFactory()).optionRule());
        Assertions.assertNotNull((new ClickhouseFileSinkFactory()).optionRule());
    }
}
