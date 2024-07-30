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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import java.util.ArrayList;

public class ActivemqIT extends TestSuiteBase {

    public GenericContainer<?> activeMQContainer =
            new GenericContainer<>(DockerImageName.parse("rmohr/activemq"))
                    .withExposedPorts(61616)
                    .withCopyFileToContainer(
                            MountableFile.forClasspathResource("e2e.json"), "e2e.json");
    private Connection connection;
    private Session session;

    @BeforeEach
    public void setup() throws JMSException {
        activeMQContainer.start();
        String brokerUrl = "tcp://localhost:" + activeMQContainer.getMappedPort(61616);
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerUrl);
        connection = connectionFactory.createConnection();
        connection.start();

        // Creating session for sending messages
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @AfterEach
    public void tearDown() throws JMSException {
        // Cleaning up resources
        connection.close();
        activeMQContainer.close();
    }

    @TestTemplate
    public void testActivemqFakeSource(TestContainer container) throws Exception {
        Container.ExecResult execResult = container.executeJob("/fake_source_to_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        Queue queue = session.createQueue("fakesource");

        // Creating the producer & consumer
        MessageConsumer consumer = session.createConsumer(queue);

        ArrayList<String> messageList = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            TextMessage textMessage = (TextMessage) consumer.receive();
            if (textMessage.getText() != null) {
                messageList.add(textMessage.getText());
            }
        }
        Assertions.assertFalse(messageList.isEmpty());
        Assertions.assertEquals(5, messageList.size());
        Assertions.assertTrue(messageList.get(0).contains("c_map"));
        Assertions.assertTrue(messageList.get(0).contains("c_array"));
        Assertions.assertTrue(messageList.get(0).contains("c_string"));
        Assertions.assertTrue(messageList.get(0).contains("c_timestamp"));
    }

    @TestTemplate
    public void testActivemqLocalFileSource(TestContainer container) throws Exception {
        Container.ExecResult execResult = container.executeJob("/localfile_source_to_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }
}
