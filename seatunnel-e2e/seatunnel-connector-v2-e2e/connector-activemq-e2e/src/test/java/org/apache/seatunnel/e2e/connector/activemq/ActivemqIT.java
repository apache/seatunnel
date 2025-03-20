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

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.format.json.JsonSerializationSchema;

import org.apache.activemq.ActiveMQConnectionFactory;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.shaded.org.apache.commons.lang3.tuple.Pair;
import org.testcontainers.utility.DockerLoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ActivemqIT extends TestSuiteBase implements TestResource {
    private static final String DOCKER_IMAGE = "apache/activemq-classic:5.18.4";
    private static final String ACTIVEMQ_CONTAINER_HOST = "activemq-host";
    private static final int PORT = 61616;
    private static final int PORT1 = 8161;
    private static final String SOURCE_QUEUE = "sourceQueue";
    private static final String SINK_QUEUE = "sinkQueue";

    private static final Pair<SeaTunnelRowType, List<SeaTunnelRow>> TEST_DATASET =
            generateTestDataSet();
    private static final JsonSerializationSchema JSON_SERIALIZATION_SCHEMA =
            new JsonSerializationSchema(TEST_DATASET.getKey());
    private GenericContainer<?> activeMQContainer;
    private Connection connection;
    private Session session;
    private MessageProducer producer;
    private MessageConsumer consumer;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        activeMQContainer =
                new GenericContainer<>(DOCKER_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(ACTIVEMQ_CONTAINER_HOST)
                        .withExposedPorts(PORT, PORT1)
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(DOCKER_IMAGE)))
                        .waitingFor(
                                new HostPortWaitStrategy()
                                        .withStartupTimeout(Duration.ofMinutes(2)));

        activeMQContainer.start();
        String brokerUrl =
                "tcp://"
                        + activeMQContainer.getHost()
                        + ":"
                        + activeMQContainer.getMappedPort(61616);
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
    @Override
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

    @TestTemplate
    public void testActiveMQ(TestContainer container) throws Exception {
        // send data to source queue before executeJob start in every testContainer
        initSourceData();
        Set<String> resultSet = new HashSet<>();
        // assert execute Job code
        Container.ExecResult execResult = container.executeJob("/activemq_to_activemq.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        // consume data when every  testContainer finished
        // try to poll five times
        MessageConsumer messageConsumer = session.createConsumer(session.createQueue(SINK_QUEUE));
        for (int i = 0; i < 5; i++) {
            Message message = messageConsumer.receive();
            if (message != null && message instanceof TextMessage) {
                TextMessage textMessage = (TextMessage) message;
                byte[] body = textMessage.getText().getBytes();
                resultSet.add(new String(body));
            }
        }
        // assert source and sink data
        Assertions.assertTrue(resultSet.size() > 0);
        Assertions.assertTrue(
                resultSet.stream()
                        .findAny()
                        .get()
                        .equals(
                                new String(
                                        JSON_SERIALIZATION_SCHEMA.serialize(
                                                TEST_DATASET.getValue().get(1)))));
        if (messageConsumer != null) {
            messageConsumer.close();
        }
    }

    @TestTemplate
    public void testSourceActiveMQJsonToConsole(TestContainer container) throws Exception {
        initSourceData();
        Container.ExecResult execResult = container.executeJob("/activemq-source_to_console.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    private static Pair<SeaTunnelRowType, List<SeaTunnelRow>> generateTestDataSet() {

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {
                            "id",
                            "c_map",
                            "c_array",
                            "c_string",
                            "c_boolean",
                            "c_tinyint",
                            "c_smallint",
                            "c_int",
                            "c_bigint",
                            "c_float",
                            "c_double",
                            "c_decimal",
                            "c_bytes",
                            "c_date",
                            "c_timestamp"
                        },
                        new SeaTunnelDataType[] {
                            BasicType.LONG_TYPE,
                            new MapType(BasicType.STRING_TYPE, BasicType.SHORT_TYPE),
                            ArrayType.BYTE_ARRAY_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.BOOLEAN_TYPE,
                            BasicType.BYTE_TYPE,
                            BasicType.SHORT_TYPE,
                            BasicType.INT_TYPE,
                            BasicType.LONG_TYPE,
                            BasicType.FLOAT_TYPE,
                            BasicType.DOUBLE_TYPE,
                            new DecimalType(2, 1),
                            PrimitiveByteArrayType.INSTANCE,
                            LocalTimeType.LOCAL_DATE_TYPE,
                            LocalTimeType.LOCAL_DATE_TIME_TYPE
                        });

        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                Long.valueOf(1),
                                Collections.singletonMap("key", Short.parseShort("1")),
                                new Byte[] {Byte.parseByte("1")},
                                "string",
                                Boolean.FALSE,
                                Byte.parseByte("1"),
                                Short.parseShort("1"),
                                Integer.parseInt("1"),
                                Long.parseLong("1"),
                                Float.parseFloat("1.1"),
                                Double.parseDouble("1.1"),
                                BigDecimal.valueOf(11, 1),
                                "test".getBytes(),
                                LocalDate.now(),
                                LocalDateTime.now()
                            });
            rows.add(row);
        }
        return Pair.of(rowType, rows);
    }

    private void initSourceData() throws IOException, InterruptedException, JMSException {
        Queue queue = session.createQueue(SOURCE_QUEUE);
        MessageProducer producer = session.createProducer(queue);
        List<SeaTunnelRow> rows = TEST_DATASET.getValue();
        for (int i = 0; i < rows.size(); i++) {
            TextMessage message =
                    session.createTextMessage(
                            new String(JSON_SERIALIZATION_SCHEMA.serialize(rows.get(1))));
            producer.send(message);
        }
    }
}
