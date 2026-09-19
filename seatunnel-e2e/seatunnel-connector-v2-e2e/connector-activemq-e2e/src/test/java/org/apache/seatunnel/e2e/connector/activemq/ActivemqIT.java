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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.activemq.source.ActivemqSource;
import org.apache.seatunnel.connectors.seatunnel.activemq.source.ActivemqSourceEnumerator.Split;
import org.apache.seatunnel.connectors.seatunnel.activemq.source.ActivemqSourceFactory;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;

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

import lombok.extern.slf4j.Slf4j;

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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Slf4j
public class ActivemqIT extends TestSuiteBase implements TestResource {

    private static final String ACTIVEMQ_CONTAINER_HOST = "activemq-host";
    public GenericContainer<?> activeMQContainer =
            new GenericContainer<>(DockerImageName.parse("apache/activemq-classic:5.18.7"))
                    .withExposedPorts(61616)
                    .withNetworkAliases(ACTIVEMQ_CONTAINER_HOST)
                    .withNetwork(NETWORK);

    private Connection connection;
    private Session session;
    private MessageProducer producer;
    private MessageConsumer consumer;

    @BeforeAll
    @Override
    public void startUp() throws JMSException, InterruptedException {
        activeMQContainer
                .withNetwork(NETWORK)
                .waitingFor(new HostPortWaitStrategy().withStartupTimeout(Duration.ofMinutes(2)));
        activeMQContainer.start();
        String brokerUrl = brokerUrl();
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
        try {
            if (connection != null) {
                // Closing a connection closes its sessions, consumers and producers as well.
                connection.close();
            }
        } finally {
            activeMQContainer.close();
        }
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
        TextMessage textMessage = (TextMessage) consumer.receive(10000);
        Assertions.assertNotNull(textMessage, "Sink did not publish a message");
        Assertions.assertTrue(textMessage.getText().contains("map"));
        Assertions.assertTrue(textMessage.getText().contains("c_boolean"));
        Assertions.assertTrue(textMessage.getText().contains("c_tinyint"));
        Assertions.assertTrue(textMessage.getText().contains("c_timestamp"));
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    @Test
    public void testSourceCheckpointDoesNotAcknowledgeLaterMessages() throws Exception {
        String queue = newQueue();
        publish(queue, "{\"value\":\"before\"}");
        RowCollector rows = new RowCollector();
        try (SourceReader<SeaTunnelRow, Split> reader = createReader(queue, "JSON")) {
            awaitRows(reader, rows, 1);
            reader.snapshotState(1);
            publish(queue, "{\"value\":\"after\"}");
            awaitRows(reader, rows, 2);
            reader.notifyCheckpointComplete(1);
        }
        assertEquals("{\"value\":\"after\"}", receive(queue));
        assertNull(receive(queue), "The completed-checkpoint message was not acknowledged");
    }

    @Test
    public void testSourceAbortCloseAndRestoreRedelivers() throws Exception {
        String queue = newQueue();
        publish(queue, "{\"value\":\"replay\"}");
        List<Split> state;
        try (SourceReader<SeaTunnelRow, Split> reader = createReader(queue, "JSON")) {
            awaitRows(reader, new RowCollector(), 1);
            state = reader.snapshotState(1);
            reader.notifyCheckpointAborted(1);
        }
        try (SourceReader<SeaTunnelRow, Split> restored = createReader(queue, "JSON")) {
            restored.addSplits(state);
            RowCollector rows = new RowCollector();
            awaitRows(restored, rows, 1);
            assertEquals("replay", rows.rows.get(0).getField(0));
            restored.snapshotState(2);
            restored.notifyCheckpointComplete(2);
        }
        assertNull(receive(queue));
    }

    @Test
    public void testSourceParallelConsumersAndTextDeserialization() throws Exception {
        String queue = newQueue();
        try (SourceReader<SeaTunnelRow, Split> first = createReader(queue, "TEXT", 1);
                SourceReader<SeaTunnelRow, Split> second = createReader(queue, "TEXT", 1)) {
            RowCollector firstRows = new RowCollector();
            RowCollector secondRows = new RowCollector();
            // Register both consumers before publishing; otherwise the first consumer can
            // legitimately prefetch the entire queue before its competitor connects.
            first.pollNext(firstRows);
            second.pollNext(secondRows);
            publish(queue, "first");
            publish(queue, "second");
            awaitRows(first, firstRows, 1);
            awaitRows(second, secondRows, 1);
            Assertions.assertNotEquals(
                    firstRows.rows.get(0).getField(0), secondRows.rows.get(0).getField(0));
            first.snapshotState(1);
            second.snapshotState(1);
            first.notifyCheckpointComplete(1);
            second.notifyCheckpointComplete(1);
        }
        assertNull(receive(queue));
    }

    @Test
    public void testSourceMalformedMessageRemainsOnQueue() throws Exception {
        String queue = newQueue();
        publish(queue, "not-json");
        try (SourceReader<SeaTunnelRow, Split> reader = createReader(queue, "JSON")) {
            assertThrows(
                    Exception.class,
                    () -> {
                        RowCollector rows = new RowCollector();
                        awaitRows(reader, rows, 1);
                    });
        }
        assertEquals("not-json", receive(queue));
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK},
            disabledReason =
                    "This streaming lifecycle test uses Zeta job status and checkpoint REST APIs")
    public void testSourceStreamingJob(TestContainer container) throws Exception {
        String jobId = String.valueOf(JobIdGenerator.newJobId());
        CompletableFuture<Container.ExecResult> job =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return container.executeJob(
                                        "/activemq_source_to_console.conf", jobId);
                            } catch (Exception e) {
                                throw new java.util.concurrent.CompletionException(e);
                            }
                        });
        try {
            await().atMost(120, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                assertStreaming(job);
                                assertEquals("RUNNING", container.getJobStatus(jobId));
                            });
            String value = "activemq-source-" + UUID.randomUUID();
            publish("source-events", "{\"value\":\"" + value + "\"}");
            await().atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                assertStreaming(job);
                                Assertions.assertTrue(container.getServerLogs().contains(value));
                            });
            // Count from AFTER observing output so the next completed checkpoint covers the row.
            long completed = container.getCompletedCheckpointCount(jobId);
            await().atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                assertStreaming(job);
                                Assertions.assertTrue(
                                        container.getCompletedCheckpointCount(jobId) > completed);
                            });
        } finally {
            if (!job.isDone()) {
                Container.ExecResult cancelled = container.cancelJob(jobId);
                assertEquals(0, cancelled.getExitCode(), cancelled.getStderr());
            }
            Container.ExecResult result = job.get(120, TimeUnit.SECONDS);
            assertEquals(0, result.getExitCode(), result.getStderr());
        }
        assertNull(receive("source-events"), "The source did not acknowledge the checkpointed row");
    }

    private void assertStreaming(CompletableFuture<Container.ExecResult> job) throws Exception {
        if (job.isDone()) {
            Assertions.fail("Streaming job ended before cancellation: " + job.get().getStderr());
        }
    }

    private SourceReader<SeaTunnelRow, Split> createReader(String queue, String format)
            throws Exception {
        return createReader(queue, format, 10);
    }

    private SourceReader<SeaTunnelRow, Split> createReader(
            String queue, String format, int inFlight) throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put("uri", brokerUrl());
        options.put("queue_name", queue);
        options.put("format", format);
        options.put("max_in_flight_messages", inFlight);
        options.put(
                "schema",
                Collections.singletonMap("fields", Collections.singletonMap("value", "string")));
        ActivemqSource source =
                (ActivemqSource)
                        new ActivemqSourceFactory()
                                .<SeaTunnelRow, Split,
                                        org.apache.seatunnel.connectors.seatunnel.activemq.source
                                                .ActivemqSourceEnumerator.State>
                                        createSource(
                                                new TableSourceFactoryContext(
                                                        ReadonlyConfig.fromMap(options),
                                                        getClass().getClassLoader()))
                                .createSource();
        SourceReader<SeaTunnelRow, Split> reader = source.createReader(null);
        reader.open();
        reader.addSplits(Collections.singletonList(new Split(0)));
        return reader;
    }

    private void awaitRows(SourceReader<SeaTunnelRow, Split> reader, RowCollector rows, int count)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (rows.rows.size() < count && System.nanoTime() < deadline) {
            reader.pollNext(rows);
        }
        assertEquals(count, rows.rows.size());
    }

    private String brokerUrl() {
        return "tcp://"
                + activeMQContainer.getHost()
                + ":"
                + activeMQContainer.getMappedPort(61616);
    }

    private String newQueue() {
        return "source-" + UUID.randomUUID();
    }

    private void publish(String queue, String text) throws JMSException {
        MessageProducer sender = session.createProducer(session.createQueue(queue));
        try {
            sender.send(session.createTextMessage(text));
        } finally {
            sender.close();
        }
    }

    private String receive(String queue) throws JMSException {
        MessageConsumer receiver = session.createConsumer(session.createQueue(queue));
        try {
            Message message = receiver.receive(1000);
            return message == null ? null : ((TextMessage) message).getText();
        } finally {
            receiver.close();
        }
    }

    private static final class RowCollector implements Collector<SeaTunnelRow> {
        private final List<SeaTunnelRow> rows = new ArrayList<>();
        private final Object lock = new Object();

        @Override
        public void collect(SeaTunnelRow row) {
            rows.add(row);
        }

        @Override
        public Object getCheckpointLock() {
            return lock;
        }
    }
}
