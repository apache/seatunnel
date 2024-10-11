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

package org.apache.seatunnel.e2e.connector.activemq;

import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.apache.activemq.ActiveMQConnectionFactory;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import java.io.IOException;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ActivemqIT extends TestSuiteBase {

    private static final String ACTIVEMQ_CONTAINER_HOST = "activemq-host";
    public GenericContainer<?> activeMQContainer =
            new GenericContainer<>(DockerImageName.parse("rmohr/activemq"))
                    .withExposedPorts(61616)
                    .withNetworkAliases(ACTIVEMQ_CONTAINER_HOST)
                    .withNetwork(NETWORK);

    private Connection connection;
    private Session session;
    private MessageProducer producer;
    private MessageConsumer consumer;

    @BeforeAll
    public void setup() throws JMSException, InterruptedException {
        activeMQContainer
                .withNetwork(NETWORK)
                .waitingFor(new HostPortWaitStrategy().withStartupTimeout(Duration.ofMinutes(2)));
        activeMQContainer.start();
        String brokerUrl = "tcp://127.0.0.1:" + activeMQContainer.getMappedPort(61616);
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerUrl);
        connection = connectionFactory.createConnection();
        connection.start();

        // Creating session for sending messages
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // Getting the queue
        Queue queue = session.createQueue("testQueue");

        // Creating the producer & consumer
        producer = session.createProducer(queue);
        consumer = session.createConsumer(queue);
    }

    @AfterAll
    public void tearDown() throws JMSException {
        // Cleaning up resources
        if (producer != null) producer.close();
        if (session != null) session.close();
        if (connection != null) connection.close();
    }

    @Test
    public void testSendMessage() throws JMSException {
        String dummyPayload = "Dummy payload";

        // Sending a text message to the queue
        TextMessage message = session.createTextMessage(dummyPayload);
        producer.send(message);

        // Receiving the message from the queue
        TextMessage receivedMessage = (TextMessage) consumer.receive(5000);

        assertEquals(dummyPayload, receivedMessage.getText());
    }

    @TestTemplate
    public void testSinkApacheActivemq(TestContainer container)
            throws IOException, InterruptedException, JMSException {
        Container.ExecResult execResult = container.executeJob("/fake_source_to_sink.conf");
        TextMessage textMessage = (TextMessage) consumer.receive();
        Assertions.assertTrue(textMessage.getText().contains("map"));
        Assertions.assertTrue(textMessage.getText().contains("c_boolean"));
        Assertions.assertTrue(textMessage.getText().contains("c_tinyint"));
        Assertions.assertTrue(textMessage.getText().contains("c_timestamp"));
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }
}
