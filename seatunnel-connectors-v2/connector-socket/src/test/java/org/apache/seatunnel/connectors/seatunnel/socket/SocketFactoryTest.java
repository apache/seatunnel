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

package org.apache.seatunnel.connectors.seatunnel.socket;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.socket.config.SocketConfig;
import org.apache.seatunnel.connectors.seatunnel.socket.config.SocketSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.socket.exception.SocketConnectorException;
import org.apache.seatunnel.connectors.seatunnel.socket.sink.SocketClient;
import org.apache.seatunnel.connectors.seatunnel.socket.sink.SocketSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.socket.source.SocketSourceFactory;
import org.apache.seatunnel.format.json.JsonSerializationSchema;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

class SocketFactoryTest {

    private OptionRule sinkRule;

    @BeforeEach
    void setUp() {
        sinkRule = new SocketSinkFactory().optionRule();
    }

    private Map<String, Object> baseSinkConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(SocketSinkOptions.HOST.key(), "localhost");
        cfg.put(SocketSinkOptions.PORT.key(), 8080);
        return cfg;
    }

    private void validateSink(Map<String, Object> cfg) {
        ConfigValidator.of(ReadonlyConfig.fromMap(cfg)).validate(sinkRule);
    }

    @Test
    void optionRule() {
        Assertions.assertNotNull((new SocketSourceFactory()).optionRule());
        Assertions.assertNotNull(sinkRule);
    }

    @Test
    void testSinkOptionRuleWithValidPortAndMaxRetries() {
        Map<String, Object> cfg = baseSinkConfig();
        cfg.put(SocketSinkOptions.MAX_RETRIES.key(), 5);
        Assertions.assertDoesNotThrow(() -> validateSink(cfg));
    }

    @Test
    void testSinkOptionRuleWithZeroPortFails() {
        Map<String, Object> cfg = baseSinkConfig();
        cfg.put(SocketSinkOptions.PORT.key(), 0);
        Assertions.assertThrows(OptionValidationException.class, () -> validateSink(cfg));
    }

    @Test
    void testSinkOptionRuleWithNegativePortFails() {
        Map<String, Object> cfg = baseSinkConfig();
        cfg.put(SocketSinkOptions.PORT.key(), -1);
        Assertions.assertThrows(OptionValidationException.class, () -> validateSink(cfg));
    }

    @Test
    void testSinkOptionRuleWithNegativeMaxRetriesFails() {
        Map<String, Object> cfg = baseSinkConfig();
        cfg.put(SocketSinkOptions.MAX_RETRIES.key(), -1);
        Assertions.assertThrows(OptionValidationException.class, () -> validateSink(cfg));
    }

    @Test
    void testSinkOptionRuleWithNegativeConnectTimeoutFails() {
        Map<String, Object> cfg = baseSinkConfig();
        cfg.put(SocketSinkOptions.CONNECT_TIMEOUT_MS.key(), -1);
        Assertions.assertThrows(OptionValidationException.class, () -> validateSink(cfg));
    }

    @Test
    void testSinkOptionRuleAcceptsZeroConnectTimeout() {
        Map<String, Object> cfg = baseSinkConfig();
        cfg.put(SocketSinkOptions.CONNECT_TIMEOUT_MS.key(), 0);
        Assertions.assertDoesNotThrow(() -> validateSink(cfg));
    }

    @Test
    void testSinkDoesNotExposeReadTimeout() {
        // The sink only writes, so a read timeout would be a no-op there and must not be offered.
        Assertions.assertFalse(
                sinkRule.getOptionalOptions().stream()
                        .anyMatch(o -> "read_timeout_ms".equals(o.key())),
                "read_timeout_ms must not be part of the sink option rule");
    }

    @Test
    void testSourceOptionRuleWithNegativeConnectTimeoutFails() {
        OptionRule sourceRule = new SocketSourceFactory().optionRule();
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(SocketSinkOptions.HOST.key(), "localhost");
        cfg.put(SocketSinkOptions.PORT.key(), 8080);
        cfg.put(SocketSinkOptions.CONNECT_TIMEOUT_MS.key(), -1);
        Assertions.assertThrows(
                OptionValidationException.class,
                () -> ConfigValidator.of(ReadonlyConfig.fromMap(cfg)).validate(sourceRule));
    }

    @Test
    void testSinkClientPassesConnectTimeoutToSocket() throws Exception {
        Map<String, Object> cfg = baseSinkConfig();
        cfg.put(SocketSinkOptions.CONNECT_TIMEOUT_MS.key(), 6543);
        SocketConfig socketConfig = new SocketConfig(ReadonlyConfig.fromMap(cfg));

        try (MockedConstruction<Socket> mocked =
                Mockito.mockConstruction(
                        Socket.class,
                        (socket, ctx) ->
                                Mockito.when(socket.getOutputStream())
                                        .thenReturn(new ByteArrayOutputStream()))) {
            SocketClient client =
                    new SocketClient(
                            socketConfig,
                            new JsonSerializationSchema(
                                    new SeaTunnelRowType(
                                            new String[] {"id"},
                                            new SeaTunnelDataType<?>[] {BasicType.INT_TYPE})));
            client.open();

            Socket created = mocked.constructed().get(0);
            Mockito.verify(created).connect(Mockito.any(InetSocketAddress.class), Mockito.eq(6543));
        }
    }

    @Test
    void testSinkClientClosesSocketWhenConnectFails() throws Exception {
        SocketConfig socketConfig = new SocketConfig(ReadonlyConfig.fromMap(baseSinkConfig()));

        try (MockedConstruction<Socket> mocked =
                Mockito.mockConstruction(
                        Socket.class,
                        (socket, ctx) ->
                                Mockito.doThrow(new java.net.ConnectException("refused"))
                                        .when(socket)
                                        .connect(
                                                Mockito.any(InetSocketAddress.class),
                                                Mockito.anyInt()))) {
            SocketClient client =
                    new SocketClient(
                            socketConfig,
                            new JsonSerializationSchema(
                                    new SeaTunnelRowType(
                                            new String[] {"id"},
                                            new SeaTunnelDataType<?>[] {BasicType.INT_TYPE})));
            Assertions.assertThrows(SocketConnectorException.class, client::open);
            Mockito.verify(mocked.constructed().get(0)).close();
        }
    }
}
