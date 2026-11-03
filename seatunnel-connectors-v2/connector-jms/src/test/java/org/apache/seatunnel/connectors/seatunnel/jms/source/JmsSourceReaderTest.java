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

package org.apache.seatunnel.connectors.seatunnel.jms.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jms.JmsTestBrokers;
import org.apache.seatunnel.connectors.seatunnel.jms.JmsTestBrokers.Provider;
import org.apache.seatunnel.connectors.seatunnel.jms.exception.JmsConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jms.exception.JmsConnectorException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.TextMessage;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs the source reader against real brokers embedded in the test JVM. Every test that depends on
 * provider behaviour runs against each provider in {@link JmsTestBrokers#providers()}.
 */
class JmsSourceReaderTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    /** How long a drain waits for the first message when the queue is expected to be empty. */
    private static final long EMPTY_QUEUE_WAIT_MILLIS = 3000;
    /** How long a drain waits for the first message when messages are expected back. */
    private static final long REDELIVERY_WAIT_MILLIS = 15000;

    private final List<JmsSourceReader> readers = new ArrayList<>();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private String queue;

    @BeforeAll
    static void startBrokers() throws Exception {
        JmsTestBrokers.start();
    }

    @AfterAll
    static void stopBrokers() throws Exception {
        JmsTestBrokers.stop();
    }

    @BeforeEach
    void newQueue() {
        queue = "jms-source-" + UUID.randomUUID();
    }

    @AfterEach
    void closeReaders() throws Exception {
        for (JmsSourceReader reader : readers) {
            try {
                reader.close();
            } catch (Exception ignored) {
                // A failed reader may fail to close; the test has already checked what it needs.
            }
        }
        executor.shutdownNow();
    }

    static Stream<Provider> providers() {
        return JmsTestBrokers.providers().stream();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("providers")
    void uncommittedMessagesAreRedeliveredWhenTheReaderCloses(Provider provider) throws Exception {
        provider.sendText(queue, json(1), json(2));
        JmsSourceReader reader = reader(provider);
        RecordingCollector output = new RecordingCollector();

        pollUntil(reader, output, 2);
        reader.snapshotState(1);
        reader.close();

        assertEquals(
                sorted(json(1), json(2)), sorted(provider.drain(queue, REDELIVERY_WAIT_MILLIS)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("providers")
    void completedCheckpointCommitsOnlyMessagesReceivedBeforeIt(Provider provider)
            throws Exception {
        JmsSourceReader reader = reader(provider);
        RecordingCollector output = new RecordingCollector();
        provider.sendText(queue, json(1));
        pollUntil(reader, output, 1);
        reader.snapshotState(1);
        provider.sendText(queue, json(2));

        if (provider.releasesPrefetchOnConsumerClose()) {
            // Message 2 is received in the next epoch before checkpoint 1 completes.
            pollUntil(reader, output, 2);
            reader.notifyCheckpointComplete(1);
        } else {
            // The closed consumer of epoch 1 holds message 2 until its session is committed.
            reader.notifyCheckpointComplete(1);
            pollUntil(reader, output, 2);
        }
        reader.close();

        assertEquals(Arrays.asList(1, 2), output.ids());
        assertEquals(
                Collections.singletonList(json(2)), provider.drain(queue, REDELIVERY_WAIT_MILLIS));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("providers")
    void abortedCheckpointIsCommittedByTheNextCompletedCheckpoint(Provider provider)
            throws Exception {
        JmsSourceReader reader = reader(provider);
        RecordingCollector output = new RecordingCollector();
        provider.sendText(queue, json(1));
        pollUntil(reader, output, 1);
        reader.snapshotState(1);
        reader.notifyCheckpointAborted(1);
        provider.sendText(queue, json(2));

        if (provider.releasesPrefetchOnConsumerClose()) {
            pollUntil(reader, output, 2);
            reader.snapshotState(2);
            reader.notifyCheckpointComplete(2);
        } else {
            // Checkpoint 2 seals nothing new but still commits the epoch of aborted checkpoint 1.
            reader.snapshotState(2);
            reader.notifyCheckpointComplete(2);
            pollUntil(reader, output, 2);
            reader.snapshotState(3);
            reader.notifyCheckpointComplete(3);
        }
        reader.close();

        assertEquals(Arrays.asList(1, 2), output.ids());
        assertEquals(Collections.emptyList(), provider.drain(queue, EMPTY_QUEUE_WAIT_MILLIS));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("providers")
    void finalCheckpointIsCommittedFromAnotherThreadAfterPollingStopped(Provider provider)
            throws Exception {
        provider.sendText(queue, json(1), json(2));
        JmsSourceReader reader = reader(provider);
        RecordingCollector output = new RecordingCollector();
        pollUntil(reader, output, 2);
        reader.snapshotState(1);

        // No pollNext follows: the engine stops polling after a closing barrier, but still
        // delivers its completion, from a thread other than the task thread.
        executor.submit(
                        () -> {
                            reader.notifyCheckpointComplete(1);
                            return null;
                        })
                .get(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
        reader.close();

        assertEquals(Collections.emptyList(), provider.drain(queue, EMPTY_QUEUE_WAIT_MILLIS));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("providers")
    void repeatedCompletionNotificationIsHarmless(Provider provider) throws Exception {
        JmsSourceReader reader = reader(provider);
        RecordingCollector output = new RecordingCollector();
        provider.sendText(queue, json(1));
        pollUntil(reader, output, 1);
        reader.snapshotState(1);
        reader.notifyCheckpointComplete(1);
        reader.notifyCheckpointComplete(1);

        provider.sendText(queue, json(2));
        pollUntil(reader, output, 2);
        reader.snapshotState(2);
        reader.notifyCheckpointComplete(2);
        reader.notifyCheckpointComplete(1);
        reader.notifyCheckpointComplete(2);
        reader.close();

        assertEquals(Arrays.asList(1, 2), output.ids());
        assertEquals(Collections.emptyList(), provider.drain(queue, EMPTY_QUEUE_WAIT_MILLIS));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("providers")
    void failedCommitFailsTheReaderAndTheMessagesAreRedelivered(Provider provider)
            throws Exception {
        AtomicBoolean failCommits = new AtomicBoolean();
        ConnectionFactory factory = commitFailingFactory(provider.factory(), failCommits);
        provider.sendText(queue, json(1), json(2));
        JmsSourceReader reader = reader(provider, Collections.emptyMap(), 0, factory);
        RecordingCollector output = new RecordingCollector();
        pollUntil(reader, output, 2);
        reader.snapshotState(1);

        failCommits.set(true);
        JmsConnectorException error =
                assertThrows(JmsConnectorException.class, () -> reader.notifyCheckpointComplete(1));
        assertEquals(JmsConnectorErrorCode.COMMIT_FAILED, error.getSeaTunnelErrorCode());
        assertThrows(JmsConnectorException.class, () -> reader.pollNext(output));
        reader.close();

        assertEquals(
                sorted(json(1), json(2)), sorted(provider.drain(queue, REDELIVERY_WAIT_MILLIS)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("providers")
    void brokerSideDropFailsTheReaderAndNoMessageIsLost(Provider provider) throws Exception {
        JmsSourceReader reader = reader(provider);
        RecordingCollector output = new RecordingCollector();
        provider.sendText(queue, json(1));
        pollUntil(reader, output, 1);
        reader.snapshotState(1);
        provider.sendText(queue, json(2));
        if (provider.releasesPrefetchOnConsumerClose()) {
            // Epoch 1 is sealed and uncommitted, epoch 2 is open and holds message 2.
            pollUntil(reader, output, 2);
        } else {
            // Opens epoch 2. The closed consumer of epoch 1 holds message 2 until its session
            // ends, so epoch 2 receives nothing.
            reader.pollNext(output);
            assertEquals(1, output.ids().size());
        }

        provider.dropConnections();
        // Some providers return null from receiveNoWait() after the drop; the exception listener
        // must still fail the reader.
        await().atMost(TIMEOUT).until(() -> pollFails(reader, output));
        assertThrows(JmsConnectorException.class, () -> reader.notifyCheckpointComplete(1));
        reader.close();

        // Duplicates are allowed after a failure, loss is not.
        assertEquals(
                new HashSet<>(Arrays.asList(json(1), json(2))),
                new HashSet<>(provider.drain(queue, REDELIVERY_WAIT_MILLIS)));
    }

    /**
     * A provider may report a lost connection only to the exception listener while {@code
     * receiveNoWait()} keeps returning null. The reader must fail rather than idle.
     */
    @Test
    void connectionLossReportedOnlyToTheExceptionListenerFailsTheReader() throws Exception {
        Provider provider = JmsTestBrokers.artemisCore();
        ConnectionFactory real = provider.factory();
        AtomicBoolean lost = new AtomicBoolean();
        javax.jms.ExceptionListener[] listener = new javax.jms.ExceptionListener[1];
        ConnectionFactory factory =
                proxy(
                        ConnectionFactory.class,
                        (factoryProxy, factoryMethod, factoryArgs) -> {
                            Connection connection =
                                    (Connection) invoke(factoryMethod, real, factoryArgs);
                            return proxy(
                                    Connection.class,
                                    (connectionProxy, connectionMethod, connectionArgs) -> {
                                        if (connectionMethod
                                                .getName()
                                                .equals("setExceptionListener")) {
                                            listener[0] =
                                                    (javax.jms.ExceptionListener) connectionArgs[0];
                                        }
                                        Object created =
                                                invoke(
                                                        connectionMethod,
                                                        connection,
                                                        connectionArgs);
                                        if (!(created instanceof Session)) {
                                            return created;
                                        }
                                        return nullAfterLossSession((Session) created, lost);
                                    });
                        });
        provider.sendText(queue, json(1));
        JmsSourceReader reader = reader(provider, Collections.emptyMap(), 0, factory);
        RecordingCollector output = new RecordingCollector();
        pollUntil(reader, output, 1);

        lost.set(true);
        provider.sendText(queue, json(2));
        JMSException connectionLoss = new JMSException("connection lost");
        assertNotNull(listener[0], "the reader registers no exception listener");
        listener[0].onException(connectionLoss);

        JmsConnectorException error =
                assertThrows(JmsConnectorException.class, () -> reader.pollNext(output));
        assertEquals(connectionLoss, error.getCause());
        assertThrows(JmsConnectorException.class, () -> reader.snapshotState(1));
        reader.close();

        assertEquals(Collections.singletonList(1), output.ids());
        assertEquals(
                sorted(json(1), json(2)), sorted(provider.drain(queue, REDELIVERY_WAIT_MILLIS)));
    }

    private static Session nullAfterLossSession(Session session, AtomicBoolean lost) {
        return proxy(
                Session.class,
                (sessionProxy, sessionMethod, sessionArgs) -> {
                    Object created = invoke(sessionMethod, session, sessionArgs);
                    if (!(created instanceof javax.jms.MessageConsumer)) {
                        return created;
                    }
                    javax.jms.MessageConsumer consumer = (javax.jms.MessageConsumer) created;
                    return proxy(
                            javax.jms.MessageConsumer.class,
                            (consumerProxy, consumerMethod, consumerArgs) ->
                                    consumerMethod.getName().equals("receiveNoWait") && lost.get()
                                            ? null
                                            : invoke(consumerMethod, consumer, consumerArgs));
                });
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("providers")
    void rotationsOverABacklogReadEveryMessageOnce(Provider provider) throws Exception {
        int messages = 30;
        provider.sendText(queue, jsons(1, messages));
        JmsSourceReader reader =
                reader(provider, Collections.singletonMap("max_in_flight_messages", 2), 0, null);
        RecordingCollector output = new RecordingCollector();

        for (int checkpoint = 1; checkpoint <= messages / 2; checkpoint++) {
            pollUntil(reader, output, checkpoint * 2);
            reader.snapshotState(checkpoint);
            reader.notifyCheckpointComplete(checkpoint);
        }
        reader.close();

        assertEquals(ids(1, messages), sortedIds(output.ids()));
        assertEquals(Collections.emptyList(), provider.drain(queue, EMPTY_QUEUE_WAIT_MILLIS));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("providers")
    void inFlightBoundPausesReceivingUntilACheckpointCompletes(Provider provider) throws Exception {
        provider.sendText(queue, jsons(1, 5));
        JmsSourceReader reader =
                reader(provider, Collections.singletonMap("max_in_flight_messages", 2), 0, null);
        RecordingCollector output = new RecordingCollector();

        pollUntil(reader, output, 2);
        for (int i = 0; i < 5; i++) {
            reader.pollNext(output);
        }
        assertEquals(2, output.ids().size());

        reader.snapshotState(1);
        for (int i = 0; i < 5; i++) {
            reader.pollNext(output);
        }
        // Sealed but uncommitted messages still count against the bound.
        assertEquals(2, output.ids().size());

        reader.notifyCheckpointComplete(1);
        pollUntil(reader, output, 4);
        reader.snapshotState(2);
        reader.notifyCheckpointComplete(2);
        pollUntil(reader, output, 5);
        reader.snapshotState(3);
        reader.notifyCheckpointComplete(3);
        reader.close();

        assertEquals(ids(1, 5), sortedIds(output.ids()));
        assertEquals(Collections.emptyList(), provider.drain(queue, EMPTY_QUEUE_WAIT_MILLIS));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("providers")
    void parallelReadersReadEveryMessageOnce(Provider provider) throws Exception {
        int messages = 20;
        provider.sendText(queue, jsons(1, messages));
        JmsSourceReader first = reader(provider, Collections.emptyMap(), 0, null);
        JmsSourceReader second = reader(provider, Collections.emptyMap(), 1, null);
        RecordingCollector firstOutput = new RecordingCollector();
        RecordingCollector secondOutput = new RecordingCollector();
        long[] checkpoint = {0};

        await().atMost(TIMEOUT)
                .pollInterval(Duration.ofMillis(20))
                .until(
                        () -> {
                            first.pollNext(firstOutput);
                            second.pollNext(secondOutput);
                            checkpoint[0]++;
                            first.snapshotState(checkpoint[0]);
                            second.snapshotState(checkpoint[0]);
                            first.notifyCheckpointComplete(checkpoint[0]);
                            second.notifyCheckpointComplete(checkpoint[0]);
                            return firstOutput.ids().size() + secondOutput.ids().size() >= messages;
                        });
        first.close();
        second.close();

        List<Integer> all = new ArrayList<>(firstOutput.ids());
        all.addAll(secondOutput.ids());
        assertEquals(ids(1, messages), sortedIds(all));
        assertEquals(Collections.emptyList(), provider.drain(queue, EMPTY_QUEUE_WAIT_MILLIS));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("providers")
    void bytesMessagesAndMessageSelectorAreSupported(Provider provider) throws Exception {
        provider.send(
                queue,
                session -> {
                    BytesMessage keep = session.createBytesMessage();
                    keep.writeBytes(json(1).getBytes(StandardCharsets.UTF_8));
                    keep.setStringProperty("kind", "keep");
                    TextMessage skip = session.createTextMessage(json(2));
                    skip.setStringProperty("kind", "skip");
                    TextMessage keepText = session.createTextMessage(json(3));
                    keepText.setStringProperty("kind", "keep");
                    return Arrays.asList(keep, skip, keepText);
                });
        JmsSourceReader reader =
                reader(
                        provider,
                        Collections.singletonMap("message_selector", "kind = 'keep'"),
                        0,
                        null);
        RecordingCollector output = new RecordingCollector();

        pollUntil(reader, output, 2);
        reader.snapshotState(1);
        reader.notifyCheckpointComplete(1);
        reader.close();

        assertEquals(Arrays.asList(1, 3), sortedIds(output.ids()));
        assertEquals(
                Collections.singletonList(json(2)), provider.drain(queue, REDELIVERY_WAIT_MILLIS));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("providers")
    void unsupportedMessageFailsTheReaderAndIsNotCommitted(Provider provider) throws Exception {
        provider.send(
                queue,
                session ->
                        Collections.singletonList(
                                session.createObjectMessage(new HashMap<String, String>())));
        JmsSourceReader reader = reader(provider);
        RecordingCollector output = new RecordingCollector();

        JmsConnectorException error = awaitPollFailure(reader, output);
        assertEquals(JmsConnectorErrorCode.UNSUPPORTED_MESSAGE, error.getSeaTunnelErrorCode());
        assertTrue(error.getMessage().contains("ObjectMessage"), error.getMessage());
        assertThrows(JmsConnectorException.class, () -> reader.snapshotState(1));
        reader.close();

        assertEquals(1, provider.drain(queue, REDELIVERY_WAIT_MILLIS).size());
    }

    @Test
    void emptyMessageBodyFailsTheReader() throws Exception {
        Provider provider = JmsTestBrokers.artemisCore();
        provider.sendText(queue, "");
        JmsSourceReader reader = reader(provider);
        RecordingCollector output = new RecordingCollector();

        JmsConnectorException error = awaitPollFailure(reader, output);
        assertEquals(JmsConnectorErrorCode.UNSUPPORTED_MESSAGE, error.getSeaTunnelErrorCode());
        reader.close();

        assertEquals(Collections.singletonList(""), provider.drain(queue, REDELIVERY_WAIT_MILLIS));
    }

    @Test
    void conversionErrorDoesNotExposeThePayload() throws Exception {
        Provider provider = JmsTestBrokers.artemisCore();
        provider.sendText(queue, "{\"id\": \"secret-payload\"}");
        JmsSourceReader reader = reader(provider);
        RecordingCollector output = new RecordingCollector();

        JmsConnectorException error = awaitPollFailure(reader, output);
        assertEquals(
                JmsConnectorErrorCode.DESERIALIZE_MESSAGE_FAILED, error.getSeaTunnelErrorCode());
        assertFalse(error.toString().contains("secret-payload"), error.toString());
        assertEquals(null, error.getCause());
    }

    @Test
    void textFormatSplitsFieldsWithTheDelimiter() throws Exception {
        Provider provider = JmsTestBrokers.artemisCore();
        provider.sendText(queue, "7|seven");
        Map<String, Object> options = new HashMap<>();
        options.put("format", "text");
        options.put("field_delimiter", "|");
        JmsSourceReader reader = reader(provider, options, 0, null);
        RecordingCollector output = new RecordingCollector();

        pollUntil(reader, output, 1);

        assertEquals(7, output.rows.get(0).getField(0));
        assertEquals("seven", output.rows.get(0).getField(1));
    }

    @Test
    void jndiConnectionFactoryReadsMessages() throws Exception {
        Provider provider = JmsTestBrokers.artemisCore();
        provider.sendText(queue, json(1));
        Map<String, Object> options = baseOptions(provider);
        options.remove("connection_factory_class");
        options.remove("connection_factory_properties");
        options.put(
                "jndi_initial_context_factory",
                "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
        options.put(
                "jndi_properties",
                Collections.singletonMap(
                        "connectionFactory.SeaTunnelFactory",
                        "tcp://127.0.0.1:" + JmsTestBrokers.artemisCorePort()));
        options.put("jndi_connection_factory_name", "SeaTunnelFactory");
        JmsSourceReader reader = reader(options, 0, null);
        RecordingCollector output = new RecordingCollector();

        pollUntil(reader, output, 1);
        reader.snapshotState(1);
        reader.notifyCheckpointComplete(1);
        reader.close();

        assertEquals(Collections.singletonList(1), output.ids());
        assertEquals(Collections.emptyList(), provider.drain(queue, EMPTY_QUEUE_WAIT_MILLIS));
    }

    @Test
    void cancelWhileConnectingClosesTheNewConnection() throws Exception {
        Provider provider = JmsTestBrokers.artemisCore();
        ConnectionFactory real = provider.factory();
        CountDownLatch connecting = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean closed = new AtomicBoolean();
        ConnectionFactory blocking =
                proxy(
                        ConnectionFactory.class,
                        (proxy, method, args) -> {
                            if (method.getName().equals("createConnection")) {
                                connecting.countDown();
                                assertTrue(release.await(TIMEOUT.getSeconds(), TimeUnit.SECONDS));
                                Connection connection = (Connection) invoke(method, real, args);
                                return proxy(
                                        Connection.class,
                                        (connectionProxy, connectionMethod, connectionArgs) -> {
                                            if (connectionMethod.getName().equals("close")) {
                                                closed.set(true);
                                            }
                                            return invoke(
                                                    connectionMethod, connection, connectionArgs);
                                        });
                            }
                            return invoke(method, real, args);
                        });
        JmsSourceReader reader = reader(provider, Collections.emptyMap(), 0, blocking);
        RecordingCollector output = new RecordingCollector();

        Future<?> poll =
                executor.submit(
                        () -> {
                            reader.pollNext(output);
                            return null;
                        });
        assertTrue(connecting.await(TIMEOUT.getSeconds(), TimeUnit.SECONDS));
        reader.cancel();
        release.countDown();

        Exception error =
                assertThrows(
                        Exception.class, () -> poll.get(TIMEOUT.getSeconds(), TimeUnit.SECONDS));
        assertTrue(error.getCause() instanceof JmsConnectorException, error.toString());
        assertTrue(closed.get());
        assertTrue(output.rows.isEmpty());
    }

    @Test
    void cancelBeforeTheFirstPollIsSticky() throws Exception {
        Provider provider = JmsTestBrokers.artemisCore();
        provider.sendText(queue, json(1));
        JmsSourceReader reader = reader(provider);
        RecordingCollector output = new RecordingCollector();

        reader.cancel();
        reader.pollNext(output);
        reader.close();

        assertTrue(output.rows.isEmpty());
        assertEquals(
                Collections.singletonList(json(1)), provider.drain(queue, REDELIVERY_WAIT_MILLIS));
    }

    private JmsSourceReader reader(Provider provider) {
        return reader(provider, Collections.emptyMap(), 0, null);
    }

    private JmsSourceReader reader(
            Provider provider,
            Map<String, Object> extraOptions,
            int split,
            ConnectionFactory factory) {
        Map<String, Object> options = baseOptions(provider);
        options.putAll(extraOptions);
        return reader(options, split, factory);
    }

    private JmsSourceReader reader(
            Map<String, Object> options, int split, ConnectionFactory factory) {
        ReadonlyConfig config = ReadonlyConfig.fromMap(options);
        JmsSourceFactory.validate(config);
        CatalogTable table = CatalogTableUtil.buildWithConfig(config);
        JmsSourceReader reader =
                new JmsSourceReader(
                        config, JmsSourceFactory.createDeserializer(config, table), factory);
        reader.open();
        reader.addSplits(Collections.singletonList(new JmsSourceSplit(split)));
        readers.add(reader);
        return reader;
    }

    private Map<String, Object> baseOptions(Provider provider) {
        Map<String, Object> fields = new HashMap<>();
        fields.put("id", "int");
        fields.put("text", "string");
        Map<String, Object> options = new HashMap<>();
        options.put("connection_factory_class", provider.factoryClass());
        options.put("connection_factory_properties", provider.factoryProperties());
        options.put("destination", queue);
        options.put("schema", Collections.singletonMap("fields", fields));
        return options;
    }

    private static void pollUntil(JmsSourceReader reader, RecordingCollector output, int rows) {
        await().atMost(TIMEOUT)
                .pollInterval(Duration.ofMillis(20))
                .until(
                        () -> {
                            reader.pollNext(output);
                            return output.ids().size() >= rows;
                        });
        assertEquals(rows, output.ids().size());
    }

    private static boolean pollFails(JmsSourceReader reader, RecordingCollector output) {
        try {
            reader.pollNext(output);
            return false;
        } catch (Exception e) {
            return true;
        }
    }

    private static JmsConnectorException awaitPollFailure(
            JmsSourceReader reader, RecordingCollector output) {
        JmsConnectorException[] failure = new JmsConnectorException[1];
        await().atMost(TIMEOUT)
                .pollInterval(Duration.ofMillis(20))
                .until(
                        () -> {
                            try {
                                reader.pollNext(output);
                                return false;
                            } catch (JmsConnectorException e) {
                                failure[0] = e;
                                return true;
                            }
                        });
        return failure[0];
    }

    /** Wraps a factory so that every Session#commit throws while {@code failCommits} is set. */
    private static ConnectionFactory commitFailingFactory(
            ConnectionFactory real, AtomicBoolean failCommits) {
        return proxy(
                ConnectionFactory.class,
                (factoryProxy, factoryMethod, factoryArgs) -> {
                    Object result = invoke(factoryMethod, real, factoryArgs);
                    if (!(result instanceof Connection)) {
                        return result;
                    }
                    Connection connection = (Connection) result;
                    return proxy(
                            Connection.class,
                            (connectionProxy, connectionMethod, connectionArgs) -> {
                                Object created =
                                        invoke(connectionMethod, connection, connectionArgs);
                                if (!(created instanceof Session)) {
                                    return created;
                                }
                                Session session = (Session) created;
                                return proxy(
                                        Session.class,
                                        (sessionProxy, sessionMethod, sessionArgs) -> {
                                            if (sessionMethod.getName().equals("commit")
                                                    && failCommits.get()) {
                                                throw new JMSException("injected commit failure");
                                            }
                                            return invoke(sessionMethod, session, sessionArgs);
                                        });
                            });
                });
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return (T)
                Proxy.newProxyInstance(
                        JmsSourceReaderTest.class.getClassLoader(), new Class<?>[] {type}, handler);
    }

    private static Object invoke(java.lang.reflect.Method method, Object target, Object[] args)
            throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw e.getTargetException();
        }
    }

    private static String json(int id) {
        return "{\"id\": " + id + ", \"text\": \"m" + id + "\"}";
    }

    private static String[] jsons(int from, int to) {
        return IntStream.rangeClosed(from, to)
                .mapToObj(JmsSourceReaderTest::json)
                .toArray(String[]::new);
    }

    private static List<Integer> ids(int from, int to) {
        return IntStream.rangeClosed(from, to).boxed().collect(Collectors.toList());
    }

    private static List<Integer> sortedIds(List<Integer> ids) {
        List<Integer> sorted = new ArrayList<>(ids);
        Collections.sort(sorted);
        return sorted;
    }

    private static List<String> sorted(String... values) {
        return sorted(Arrays.asList(values));
    }

    private static List<String> sorted(List<String> values) {
        List<String> sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        return sorted;
    }

    /** Collects rows the way the engine does, with a checkpoint lock. */
    private static final class RecordingCollector implements Collector<SeaTunnelRow> {
        private final Object checkpointLock = new Object();
        private final List<SeaTunnelRow> rows = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void collect(SeaTunnelRow record) {
            rows.add(record);
        }

        @Override
        public Object getCheckpointLock() {
            return checkpointLock;
        }

        List<Integer> ids() {
            synchronized (rows) {
                return rows.stream()
                        .map(row -> (Integer) row.getField(0))
                        .collect(Collectors.toList());
            }
        }
    }
}
