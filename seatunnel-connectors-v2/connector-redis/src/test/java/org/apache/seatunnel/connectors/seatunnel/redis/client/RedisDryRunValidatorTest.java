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

package org.apache.seatunnel.connectors.seatunnel.redis.client;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.redis.config.JedisWrapper;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisParameters;
import org.apache.seatunnel.connectors.seatunnel.redis.exception.RedisConnectorException;
import org.apache.seatunnel.connectors.seatunnel.redis.exception.RedisErrorCode;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.MockedConstruction;

import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.exceptions.JedisClusterOperationException;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class RedisDryRunValidatorTest {

    private static final String USER = "synthetic-user";
    private static final String PASSWORD = "synthetic-secret";
    private static final String SINGLE_TARGET = "localhost:6379";
    private static final String CLUSTER_TARGET = "[127.0.0.1:7000, 127.0.0.1:7001]";

    @Test
    void singleNodeAuthenticatesNamedUserSelectsPingsAndCloses() {
        try (MockedConstruction<Jedis> clients = mockConstruction(Jedis.class)) {
            RedisDryRunValidator.validate(singleParameters(USER, PASSWORD));

            assertEquals(1, clients.constructed().size());
            Jedis jedis = clients.constructed().get(0);
            InOrder order = inOrder(jedis);
            order.verify(jedis).auth(USER, PASSWORD);
            order.verify(jedis).select(2);
            order.verify(jedis).ping();
            order.verify(jedis).close();
            // Proves that no key or ACL command is issued.
            verifyNoMoreInteractions(jedis);
        }
    }

    @Test
    void singleNodeAuthenticatesPasswordOnly() {
        try (MockedConstruction<Jedis> clients = mockConstruction(Jedis.class)) {
            RedisDryRunValidator.validate(singleParameters(null, PASSWORD));

            Jedis jedis = clients.constructed().get(0);
            InOrder order = inOrder(jedis);
            order.verify(jedis).auth(PASSWORD);
            order.verify(jedis).select(2);
            order.verify(jedis).ping();
            order.verify(jedis).close();
            verifyNoMoreInteractions(jedis);
        }
    }

    @Test
    void singleNodeUsesRuntimeConnectionSetup() {
        try (MockedConstruction<Jedis> clients =
                mockConstruction(
                        Jedis.class,
                        (jedis, context) ->
                                assertEquals(
                                        Arrays.asList("localhost", 6379), context.arguments()))) {
            RedisDryRunValidator.validate(singleParameters(null, null));

            assertEquals(1, clients.constructed().size());
        }
    }

    @Test
    void singleNodeWithoutAuthDoesNotAuthenticate() {
        try (MockedConstruction<Jedis> clients = mockConstruction(Jedis.class)) {
            RedisDryRunValidator.validate(singleParameters(null, null));

            Jedis jedis = clients.constructed().get(0);
            verify(jedis, never()).auth(anyString());
            verify(jedis, never()).auth(anyString(), anyString());
            verify(jedis).select(2);
            verify(jedis).ping();
            verify(jedis).close();
            verifyNoMoreInteractions(jedis);
        }
    }

    @Test
    void singleNodeAuthenticationFailureIsReportedAndClientClosed() {
        JedisDataException cause =
                new JedisDataException("WRONGPASS invalid username-password pair");
        try (MockedConstruction<Jedis> clients =
                mockConstruction(
                        Jedis.class,
                        (jedis, context) -> when(jedis.auth(USER, PASSWORD)).thenThrow(cause))) {
            RedisConnectorException exception =
                    assertThrows(
                            RedisConnectorException.class,
                            () -> RedisDryRunValidator.validate(singleParameters(USER, PASSWORD)));

            assertConnectionError(exception, SINGLE_TARGET, cause);
            Jedis jedis = clients.constructed().get(0);
            verify(jedis, never()).select(anyInt());
            verify(jedis, never()).ping();
            verify(jedis).close();
        }
    }

    @Test
    void singleNodeConnectionFailureIsReportedAndClientClosed() {
        JedisConnectionException cause =
                new JedisConnectionException("java.net.ConnectException: Connection refused");
        try (MockedConstruction<Jedis> clients =
                mockConstruction(
                        Jedis.class, (jedis, context) -> when(jedis.select(2)).thenThrow(cause))) {
            RedisConnectorException exception =
                    assertThrows(
                            RedisConnectorException.class,
                            () -> RedisDryRunValidator.validate(singleParameters(null, null)));

            assertConnectionError(exception, SINGLE_TARGET, cause);
            verify(clients.constructed().get(0)).close();
        }
    }

    @Test
    void singleNodePingFailureIsReportedAndClientClosed() {
        JedisConnectionException cause = new JedisConnectionException("Unexpected end of stream.");
        try (MockedConstruction<Jedis> clients =
                mockConstruction(
                        Jedis.class, (jedis, context) -> when(jedis.ping()).thenThrow(cause))) {
            RedisConnectorException exception =
                    assertThrows(
                            RedisConnectorException.class,
                            () -> RedisDryRunValidator.validate(singleParameters(null, PASSWORD)));

            assertConnectionError(exception, SINGLE_TARGET, cause);
            verify(clients.constructed().get(0)).close();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void clusterAuthenticatesNamedUserReadsInfoAndCloses() {
        try (MockedConstruction<JedisCluster> clusters =
                        mockConstruction(
                                JedisCluster.class,
                                (cluster, context) -> {
                                    List<?> arguments = context.arguments();
                                    assertClusterNodes((Set<HostAndPort>) arguments.get(0));
                                    DefaultJedisClientConfig config =
                                            (DefaultJedisClientConfig) arguments.get(1);
                                    assertEquals(USER, config.getUser());
                                    assertEquals(PASSWORD, config.getPassword());
                                });
                MockedConstruction<JedisWrapper> wrappers =
                        mockConstruction(
                                JedisWrapper.class,
                                (wrapper, context) ->
                                        when(wrapper.info()).thenReturn("redis_version:7.0.0"))) {
            RedisDryRunValidator.validate(clusterParameters(USER, PASSWORD));

            assertEquals(1, clusters.constructed().size());
            JedisWrapper wrapper = wrappers.constructed().get(0);
            InOrder order = inOrder(wrapper);
            order.verify(wrapper).info();
            order.verify(wrapper).close();
            verifyNoMoreInteractions(wrapper);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void clusterAuthenticatesPasswordOnly() {
        try (MockedConstruction<JedisCluster> clusters =
                        mockConstruction(
                                JedisCluster.class,
                                (cluster, context) -> {
                                    List<?> arguments = context.arguments();
                                    assertClusterNodes((Set<HostAndPort>) arguments.get(0));
                                    assertEquals(PASSWORD, arguments.get(4));
                                });
                MockedConstruction<JedisWrapper> wrappers = mockConstruction(JedisWrapper.class)) {
            RedisDryRunValidator.validate(clusterParameters(null, PASSWORD));

            assertEquals(1, clusters.constructed().size());
            JedisWrapper wrapper = wrappers.constructed().get(0);
            verify(wrapper).info();
            verify(wrapper).close();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void clusterWithoutAuthPassesNoCredentials() {
        try (MockedConstruction<JedisCluster> clusters =
                        mockConstruction(
                                JedisCluster.class,
                                (cluster, context) -> {
                                    List<?> arguments = context.arguments();
                                    assertEquals(1, arguments.size());
                                    assertClusterNodes((Set<HostAndPort>) arguments.get(0));
                                });
                MockedConstruction<JedisWrapper> wrappers = mockConstruction(JedisWrapper.class)) {
            RedisDryRunValidator.validate(clusterParameters(null, null));

            assertEquals(1, clusters.constructed().size());
            verify(wrappers.constructed().get(0)).close();
        }
    }

    @Test
    void clusterWithNoReachableNodeIsReported() {
        JedisClusterOperationException cause =
                new JedisClusterOperationException("Could not initialize cluster slots cache.");
        try (MockedConstruction<JedisCluster> clusters =
                        mockConstruction(
                                JedisCluster.class,
                                (cluster, context) -> {
                                    throw cause;
                                });
                MockedConstruction<JedisWrapper> wrappers = mockConstruction(JedisWrapper.class)) {
            RedisConnectorException exception =
                    assertThrows(
                            RedisConnectorException.class,
                            () -> RedisDryRunValidator.validate(clusterParameters(USER, PASSWORD)));

            assertConnectionError(exception, CLUSTER_TARGET, cause);
            assertTrue(wrappers.constructed().isEmpty());
        }
    }

    @Test
    void clusterInfoFailureIsReportedAndClientClosed() {
        RedisConnectorException cause =
                new RedisConnectorException(
                        RedisErrorCode.GET_REDIS_INFO_ERROR,
                        "Failed to get redis info from all node in cluster");
        try (MockedConstruction<JedisCluster> clusters = mockConstruction(JedisCluster.class);
                MockedConstruction<JedisWrapper> wrappers =
                        mockConstruction(
                                JedisWrapper.class,
                                (wrapper, context) -> when(wrapper.info()).thenThrow(cause))) {
            RedisConnectorException exception =
                    assertThrows(
                            RedisConnectorException.class,
                            () -> RedisDryRunValidator.validate(clusterParameters(null, PASSWORD)));

            assertConnectionError(exception, CLUSTER_TARGET, cause);
            verify(wrappers.constructed().get(0)).close();
        }
    }

    @Test
    void clusterMalformedNodeIsReportedAsConnectionError() {
        Map<String, Object> config = new HashMap<>();
        config.put("mode", "CLUSTER");
        config.put("nodes", Arrays.asList("127.0.0.1:7000", "host-without-port"));
        RedisParameters parameters = parameters(config);

        RedisConnectorException exception =
                assertThrows(
                        RedisConnectorException.class,
                        () -> RedisDryRunValidator.validate(parameters));

        assertEquals(RedisErrorCode.REDIS_CONNECTION_ERROR, exception.getSeaTunnelErrorCode());
        assertTrue(exception.getMessage().contains("host-without-port"), exception.getMessage());
    }

    private static void assertClusterNodes(Set<HostAndPort> nodes) {
        assertEquals(2, nodes.size());
        assertTrue(nodes.contains(new HostAndPort("127.0.0.1", 7000)));
        assertTrue(nodes.contains(new HostAndPort("127.0.0.1", 7001)));
    }

    private static void assertConnectionError(
            RedisConnectorException exception, String target, Throwable cause) {
        assertEquals(RedisErrorCode.REDIS_CONNECTION_ERROR, exception.getSeaTunnelErrorCode());
        assertTrue(exception.getMessage().contains(target), exception.getMessage());
        assertFalse(exception.getMessage().contains(PASSWORD), exception.getMessage());
        // Mockito wraps exceptions thrown from a mocked constructor, so search the cause chain.
        Throwable current = exception.getCause();
        while (current != null && current != cause) {
            current = current.getCause();
        }
        assertSame(cause, current);
    }

    private static RedisParameters singleParameters(String user, String auth) {
        Map<String, Object> config = new HashMap<>();
        config.put("mode", "SINGLE");
        config.put("host", "localhost");
        config.put("port", 6379);
        config.put("db_num", 2);
        return parameters(withCredentials(config, user, auth));
    }

    private static RedisParameters clusterParameters(String user, String auth) {
        Map<String, Object> config = new HashMap<>();
        config.put("mode", "CLUSTER");
        config.put("nodes", Arrays.asList("127.0.0.1:7000", "127.0.0.1:7001"));
        return parameters(withCredentials(config, user, auth));
    }

    private static Map<String, Object> withCredentials(
            Map<String, Object> config, String user, String auth) {
        if (user != null) {
            config.put("user", user);
        }
        if (auth != null) {
            config.put("auth", auth);
        }
        return config;
    }

    private static RedisParameters parameters(Map<String, Object> config) {
        RedisParameters parameters = new RedisParameters();
        parameters.buildConnectionConfig(ReadonlyConfig.fromMap(config));
        return parameters;
    }
}
