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

package org.apache.seatunnel.connectors.seatunnel.rabbitmq.client;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqConfig;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.exception.RabbitmqConnectorException;

import org.junit.jupiter.api.Test;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class RabbitmqClientTest {

    @Test
    void dryRunOnlyPassivelyChecksAllQueuesAndClosesConnection() throws Exception {
        ConnectionFactory factory = mock(ConnectionFactory.class);
        Connection connection = mock(Connection.class);
        Channel channel = mock(Channel.class);
        when(factory.newConnection()).thenReturn(connection);
        when(connection.createChannel()).thenReturn(channel);
        Map<String, Object> options = dryRunOptions();
        options.remove("queue_name");
        options.put(
                "tables_configs",
                Arrays.asList(
                        Collections.singletonMap("queue_name", "first"),
                        Collections.singletonMap("queue_name", "second")));

        RabbitmqSourceDryRunValidator.validate(ReadonlyConfig.fromMap(options), ignored -> factory);

        verify(channel).queueDeclarePassive("first");
        verify(channel).queueDeclarePassive("second");
        verifyNoMoreInteractions(channel);
        verify(connection).createChannel();
        verify(connection).abort(1000);
        verifyNoMoreInteractions(connection);
        verify(factory).setConnectionTimeout(10000);
        verify(factory).setHandshakeTimeout(10000);
        verify(factory).setChannelRpcTimeout(10000);
        verify(factory).setAutomaticRecoveryEnabled(false);
        verify(factory).setTopologyRecoveryEnabled(false);
        assertFalse((Boolean) options.get("passive"));
    }

    @Test
    void dryRunClosesConnectionWhenChannelCreationFailsWithoutExposingCause() throws Exception {
        ConnectionFactory factory = mock(ConnectionFactory.class);
        Connection connection = mock(Connection.class);
        when(factory.newConnection()).thenReturn(connection);
        when(connection.createChannel()).thenThrow(new IOException("amqp://user:secret@host"));
        IOException failure =
                assertThrows(
                        IOException.class,
                        () ->
                                RabbitmqSourceDryRunValidator.validate(
                                        ReadonlyConfig.fromMap(dryRunOptions()),
                                        ignored -> factory));
        assertTrue(failure.getMessage().contains("channel creation"));
        assertFalse(failure.getMessage().contains("secret"));
        assertNull(failure.getCause());
        assertEquals(0, failure.getSuppressed().length);
        verify(connection).abort(1000);
    }

    @Test
    void dryRunMissingQueueFailsWithoutCreatingItAndClosesConnection() throws Exception {
        ConnectionFactory factory = mock(ConnectionFactory.class);
        Connection connection = mock(Connection.class);
        Channel channel = mock(Channel.class);
        when(factory.newConnection()).thenReturn(connection);
        when(connection.createChannel()).thenReturn(channel);
        when(channel.queueDeclarePassive("existing"))
                .thenThrow(new IOException("private broker text"));
        IOException failure =
                assertThrows(
                        IOException.class,
                        () ->
                                RabbitmqSourceDryRunValidator.validate(
                                        ReadonlyConfig.fromMap(dryRunOptions()),
                                        ignored -> factory));
        assertTrue(failure.getMessage().contains("pre-create"));
        assertNull(failure.getCause());
        verify(channel).queueDeclarePassive("existing");
        verifyNoMoreInteractions(channel);
        verify(connection).abort(1000);
    }

    @Test
    void dryRunHonorsSmallerTimeoutAndRejectsPreInterruptedThread() throws Exception {
        ConnectionFactory factory = mock(ConnectionFactory.class);
        when(factory.getConnectionTimeout()).thenReturn(250);
        when(factory.newConnection()).thenThrow(new IOException("unavailable"));
        assertThrows(
                IOException.class,
                () ->
                        RabbitmqSourceDryRunValidator.validate(
                                ReadonlyConfig.fromMap(dryRunOptions()), ignored -> factory));
        verify(factory).setConnectionTimeout(250);
        verify(factory).setHandshakeTimeout(250);
        verify(factory).setChannelRpcTimeout(250);

        ConnectionFactory unused = mock(ConnectionFactory.class);
        Thread.currentThread().interrupt();
        try {
            assertThrows(
                    InterruptedException.class,
                    () ->
                            RabbitmqSourceDryRunValidator.validate(
                                    ReadonlyConfig.fromMap(dryRunOptions()), ignored -> unused));
            assertTrue(Thread.currentThread().isInterrupted());
            verifyNoInteractions(unused);
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void sharedFactoryPreservesUriPrecedenceAndRecoverySettings() {
        Map<String, Object> options = dryRunOptions();
        options.put("url", "amqp://uri-user:uri-password@uri-host:1234/my-vhost");
        options.put("AUTOMATIC_RECOVERY_ENABLED", true);
        RabbitmqConfig config = new RabbitmqConfig(ReadonlyConfig.fromMap(options));
        ConnectionFactory factory = RabbitmqClient.createConnectionFactory(config);
        assertEquals("uri-host", factory.getHost());
        assertEquals(1234, factory.getPort());
        assertEquals("uri-user", factory.getUsername());
        assertEquals("uri-password", factory.getPassword());
        assertEquals("my-vhost", factory.getVirtualHost());
        assertTrue(factory.isAutomaticRecoveryEnabled());
    }

    private static Map<String, Object> dryRunOptions() {
        Map<String, Object> options = new HashMap<>();
        options.put("host", "localhost");
        options.put("port", 5672);
        options.put("username", "guest");
        options.put("password", "guest");
        options.put("queue_name", "existing");
        options.put("passive", false);
        return options;
    }

    @Test
    void dryRunCleanupFailureIsSanitizedAndCannotPass() throws Exception {
        ConnectionFactory factory = mock(ConnectionFactory.class);
        Connection connection = mock(Connection.class);
        Channel channel = mock(Channel.class);
        when(factory.newConnection()).thenReturn(connection);
        when(connection.createChannel()).thenReturn(channel);
        doThrow(new IllegalStateException("private-secret")).when(connection).abort(1000);
        IOException failure =
                assertThrows(
                        IOException.class,
                        () ->
                                RabbitmqSourceDryRunValidator.validate(
                                        ReadonlyConfig.fromMap(dryRunOptions()),
                                        ignored -> factory));
        assertTrue(failure.getMessage().contains("cleanup"));
        assertFalse(failure.getMessage().contains("private-secret"));
        assertNull(failure.getCause());
    }

    @Test
    void dryRunRestoresInterruptAndClosesAfterQueueCheck() throws Exception {
        ConnectionFactory factory = mock(ConnectionFactory.class);
        Connection connection = mock(Connection.class);
        Channel channel = mock(Channel.class);
        when(factory.newConnection()).thenReturn(connection);
        when(connection.createChannel()).thenReturn(channel);
        when(channel.queueDeclarePassive("existing"))
                .thenAnswer(
                        ignored -> {
                            Thread.currentThread().interrupt();
                            return null;
                        });
        try {
            assertThrows(
                    InterruptedException.class,
                    () ->
                            RabbitmqSourceDryRunValidator.validate(
                                    ReadonlyConfig.fromMap(dryRunOptions()), ignored -> factory));
            assertTrue(Thread.currentThread().isInterrupted());
            verify(connection).abort(1000);
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void declaresExistingQueuePassivelyWhenConfigured() throws Exception {
        Channel channel = mock(Channel.class);
        RabbitmqConfig config = mock(RabbitmqConfig.class);
        when(config.isPassive()).thenReturn(true);

        RabbitmqClient.declareQueue(channel, config, "existing-queue");

        verify(channel).queueDeclarePassive("existing-queue");
        verify(channel, never()).queueDeclare("existing-queue", true, false, false, null);
    }

    @Test
    void declaresQueueWithConfiguredPropertiesByDefault() throws Exception {
        Channel channel = mock(Channel.class);
        RabbitmqConfig config = mock(RabbitmqConfig.class);
        when(config.isPassive()).thenReturn(false);
        when(config.getDurable()).thenReturn(true);
        when(config.getExclusive()).thenReturn(false);
        when(config.getAutoDelete()).thenReturn(false);

        RabbitmqClient.declareQueue(channel, config, "new-queue");

        verify(channel).queueDeclare("new-queue", true, false, false, null);
        verify(channel, never()).queueDeclarePassive("new-queue");
    }

    @Test
    void explainsPassiveQueueDeclarationFailure() throws Exception {
        Channel channel = mock(Channel.class);
        RabbitmqConfig config = mock(RabbitmqConfig.class);
        when(config.isPassive()).thenReturn(true);
        doThrow(new IOException("queue not found"))
                .when(channel)
                .queueDeclarePassive("missing-queue");

        RabbitmqConnectorException exception =
                assertThrows(
                        RabbitmqConnectorException.class,
                        () -> RabbitmqClient.declareQueue(channel, config, "missing-queue"));

        assertTrue(exception.getMessage().contains("missing-queue"));
        assertTrue(exception.getMessage().contains("passive=false"));
    }
}
