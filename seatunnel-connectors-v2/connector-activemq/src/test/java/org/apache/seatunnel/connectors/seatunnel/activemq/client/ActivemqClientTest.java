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

package org.apache.seatunnel.connectors.seatunnel.activemq.client;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions;

import org.apache.activemq.ActiveMQConnectionFactory;

import org.junit.jupiter.api.Test;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ActivemqClientTest {

    @Test
    void reusesAndClosesSessionAndProducer() throws Exception {
        ReadonlyConfig config = config();
        Connection connection = mock(Connection.class);
        Session session = mock(Session.class);
        Queue destination = mock(Queue.class);
        MessageProducer producer = mock(MessageProducer.class);
        TextMessage message = mock(TextMessage.class);
        when(connection.createSession(false, Session.AUTO_ACKNOWLEDGE)).thenReturn(session);
        when(session.createQueue("test-queue")).thenReturn(destination);
        when(session.createProducer(destination)).thenReturn(producer);
        when(session.createTextMessage(anyString())).thenReturn(message);

        ActivemqClient client =
                new ActivemqClient(config, mock(ActiveMQConnectionFactory.class), connection);
        client.write("first".getBytes());
        client.write("second".getBytes());
        client.close();

        verify(connection, times(1)).start();
        verify(connection, times(1)).createSession(false, Session.AUTO_ACKNOWLEDGE);
        verify(session, times(1)).createProducer(destination);
        verify(producer).setDeliveryMode(2);
        verify(producer).setTimeToLive(0L);
        verify(producer).setPriority(4);
        verify(producer, times(2)).send(message);
        verify(producer).close();
        verify(session).close();
        verify(connection).close();
    }

    @Test
    void appliesConnectionAndProducerOptions() throws Exception {
        Map<String, Object> values = new HashMap<>();
        values.put(ActivemqSinkOptions.URI.key(), "tcp://localhost:61616");
        values.put(ActivemqSinkOptions.QUEUE_NAME.key(), "test-queue");
        values.put(ActivemqSinkOptions.MAX_THREAD_POOL_SIZE.key(), 17);
        values.put(ActivemqSinkOptions.SEND_TIMEOUT.key(), 18);
        values.put(ActivemqSinkOptions.USE_COMPRESSION.key(), true);
        values.put(ActivemqSinkOptions.CONNECT_RESPONSE_TIMEOUT.key(), 19);
        values.put(ActivemqSinkOptions.PRODUCER_WINDOW_SIZE.key(), 20);
        values.put(ActivemqSinkOptions.USE_ASYNC_SEND.key(), true);
        values.put(ActivemqSinkOptions.DELIVERY_MODE.key(), 1);
        values.put(ActivemqSinkOptions.TIME_TO_LIVE.key(), 21);
        values.put(ActivemqSinkOptions.PRIORITY.key(), 7);

        ReadonlyConfig config = ReadonlyConfig.fromMap(values);
        ActiveMQConnectionFactory factory = ActivemqClient.createConnectionFactory(config);
        assertEquals(17, factory.getMaxThreadPoolSize());
        assertEquals(18, factory.getSendTimeout());
        assertEquals(true, factory.isUseCompression());
        assertEquals(19, factory.getConnectResponseTimeout());
        assertEquals(20, factory.getProducerWindowSize());
        assertEquals(true, factory.isUseAsyncSend());

        Connection connection = mock(Connection.class);
        Session session = mock(Session.class);
        Queue destination = mock(Queue.class);
        MessageProducer producer = mock(MessageProducer.class);
        when(connection.createSession(false, Session.AUTO_ACKNOWLEDGE)).thenReturn(session);
        when(session.createQueue("test-queue")).thenReturn(destination);
        when(session.createProducer(destination)).thenReturn(producer);
        ActivemqClient client = new ActivemqClient(config, factory, connection);

        verify(producer).setDeliveryMode(1);
        verify(producer).setTimeToLive(21L);
        verify(producer).setPriority(7);
        client.close();
    }

    @Test
    void closesOpenedResourcesWhenProducerInitializationFails() throws Exception {
        ReadonlyConfig config = config();
        Connection connection = mock(Connection.class);
        Session session = mock(Session.class);
        Queue destination = mock(Queue.class);
        when(connection.createSession(false, Session.AUTO_ACKNOWLEDGE)).thenReturn(session);
        when(session.createQueue("test-queue")).thenReturn(destination);
        when(session.createProducer(destination)).thenThrow(new JMSException("producer failure"));

        assertThrows(
                RuntimeException.class,
                () ->
                        new ActivemqClient(
                                config, mock(ActiveMQConnectionFactory.class), connection));

        verify(session).close();
        verify(connection).close();
    }

    private ReadonlyConfig config() {
        Map<String, Object> values = new HashMap<>();
        values.put(ActivemqSinkOptions.URI.key(), "tcp://localhost:61616");
        values.put(ActivemqSinkOptions.QUEUE_NAME.key(), "test-queue");
        return ReadonlyConfig.fromMap(values);
    }
}
