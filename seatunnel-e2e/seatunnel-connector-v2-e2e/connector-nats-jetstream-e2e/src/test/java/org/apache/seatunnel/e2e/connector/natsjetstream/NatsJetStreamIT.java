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

package org.apache.seatunnel.e2e.connector.natsjetstream;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.impl.Headers;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class NatsJetStreamIT extends TestSuiteBase implements TestResource {

    private static final String IMAGE = "nats:2.10.14-alpine";
    private static final String NATS_HOST = "nats-jetstream-e2e";
    private static final int NATS_PORT = 4222;
    private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(2);
    private static final Duration TEST_TIMEOUT = Duration.ofSeconds(30);

    private static final String JSON_STREAM = "json_stream";
    private static final String JSON_SUBJECT = "orders.json";
    private static final String NATIVE_STREAM = "native_stream";
    private static final String NATIVE_SUBJECT_ONE = "events.native.alpha";
    private static final String NATIVE_SUBJECT_TWO = "events.native.beta";
    private static final String DEFAULT_STREAM = "default_stream";
    private static final String DEFAULT_SUBJECT = "events.default";

    private static final List<String> EXPECTED_JSON_PAYLOADS =
            Arrays.asList(
                    "{\"id\":101,\"name\":\"alice\",\"score\":9.5}",
                    "{\"id\":102,\"name\":\"bob\",\"score\":7.25}",
                    "{\"id\":103,\"name\":\"carol\",\"score\":8.75}");

    private static final List<NativeExpectedMessage> EXPECTED_NATIVE_MESSAGES =
            Arrays.asList(
                    new NativeExpectedMessage(
                            NATIVE_SUBJECT_ONE,
                            "msg-1",
                            mapOf("tenant", "acme", "trace", "trace-1"),
                            "payload-1".getBytes(StandardCharsets.UTF_8)),
                    new NativeExpectedMessage(
                            NATIVE_SUBJECT_TWO,
                            "msg-2",
                            mapOf("tenant", "beta", "trace", "trace-2"),
                            "payload-2".getBytes(StandardCharsets.UTF_8)),
                    new NativeExpectedMessage(
                            NATIVE_SUBJECT_ONE,
                            "msg-3",
                            mapOf("tenant", "acme", "trace", "trace-3"),
                            "payload-3".getBytes(StandardCharsets.UTF_8)));

    private static final List<String> EXPECTED_DEFAULT_PAYLOADS =
            Arrays.asList(
                    "{\"id\":201,\"status\":\"queued\",\"enabled\":true}",
                    "{\"id\":202,\"status\":\"done\",\"enabled\":false}");

    private GenericContainer<?> natsContainer;
    private Connection adminConnection;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        natsContainer =
                new GenericContainer<>(DockerImageName.parse(IMAGE))
                        .withCommand("--jetstream")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(NATS_HOST)
                        .withExposedPorts(NATS_PORT)
                        .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger(IMAGE)))
                        .waitingFor(new HostPortWaitStrategy().withStartupTimeout(STARTUP_TIMEOUT));
        Startables.deepStart(Stream.of(natsContainer)).join();
        adminConnection = createClientConnection();
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (adminConnection != null) {
            adminConnection.close();
            adminConnection = null;
        }
        if (natsContainer != null) {
            natsContainer.close();
            natsContainer = null;
        }
    }

    @TestTemplate
    public void testJsonSink(TestContainer container) throws Exception {
        recreateStream(JSON_STREAM, JSON_SUBJECT);
        try (VerificationConsumer consumer =
                createVerificationConsumer(JSON_STREAM, JSON_SUBJECT)) {
            Container.ExecResult execResult =
                    container.executeJob("/fake_to_nats_jetstream_json.conf");
            Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

            List<Message> messages = consumer.awaitMessages(EXPECTED_JSON_PAYLOADS.size());
            List<String> actualPayloads =
                    messages.stream()
                            .map(message -> new String(message.getData(), StandardCharsets.UTF_8))
                            .collect(Collectors.toList());
            Assertions.assertEquals(EXPECTED_JSON_PAYLOADS, actualPayloads);
            Assertions.assertTrue(
                    messages.stream()
                            .allMatch(message -> JSON_SUBJECT.equals(message.getSubject())));
        }
    }

    @TestTemplate
    public void testNativeSink(TestContainer container) throws Exception {
        recreateStream(NATIVE_STREAM, NATIVE_SUBJECT_ONE, NATIVE_SUBJECT_TWO);
        try (VerificationConsumer consumer =
                createVerificationConsumer(NATIVE_STREAM, "events.native.*")) {
            Container.ExecResult execResult =
                    container.executeJob("/fake_to_nats_jetstream_native.conf");
            Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

            List<Message> messages = consumer.awaitMessages(EXPECTED_NATIVE_MESSAGES.size());
            List<NativeActualMessage> actualMessages =
                    messages.stream()
                            .map(NativeActualMessage::fromMessage)
                            .collect(Collectors.toList());
            List<NativeActualMessage> expectedMessages =
                    EXPECTED_NATIVE_MESSAGES.stream()
                            .map(NativeExpectedMessage::toActualMessage)
                            .sorted(NativeActualMessage::compareByContent)
                            .collect(Collectors.toList());
            actualMessages.sort(NativeActualMessage::compareByContent);
            Assertions.assertEquals(expectedMessages, actualMessages);
        }
    }

    @TestTemplate
    public void testNativeSinkWithDefaultFieldMapping(TestContainer container) throws Exception {
        recreateStream(DEFAULT_STREAM, DEFAULT_SUBJECT);
        try (VerificationConsumer consumer =
                createVerificationConsumer(DEFAULT_STREAM, DEFAULT_SUBJECT)) {
            Container.ExecResult execResult =
                    container.executeJob("/fake_to_nats_jetstream_native_defaults.conf");
            Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

            List<Message> messages = consumer.awaitMessages(EXPECTED_DEFAULT_PAYLOADS.size());
            List<String> actualPayloads =
                    messages.stream()
                            .map(message -> new String(message.getData(), StandardCharsets.UTF_8))
                            .collect(Collectors.toList());
            Assertions.assertEquals(EXPECTED_DEFAULT_PAYLOADS, actualPayloads);
            Assertions.assertTrue(
                    messages.stream()
                            .allMatch(message -> DEFAULT_SUBJECT.equals(message.getSubject())));
            Assertions.assertTrue(
                    messages.stream()
                            .allMatch(
                                    message -> {
                                        Headers headers = message.getHeaders();
                                        return headers == null || headers.isEmpty();
                                    }));
        }
    }

    private Connection createClientConnection() throws IOException, InterruptedException {
        Options options =
                new Options.Builder()
                        .server(getMappedNatsUrl())
                        .connectionTimeout(Duration.ofSeconds(30))
                        .build();
        return Nats.connect(options);
    }

    private String getMappedNatsUrl() {
        return "nats://" + natsContainer.getHost() + ":" + natsContainer.getMappedPort(NATS_PORT);
    }

    private void recreateStream(String streamName, String... subjects)
            throws IOException, JetStreamApiException {
        JetStreamManagement management = adminConnection.jetStreamManagement();
        try {
            management.deleteStream(streamName);
        } catch (JetStreamApiException e) {
            if (e.getErrorCode() != 404) {
                throw e;
            }
        }
        management.addStream(
                StreamConfiguration.builder()
                        .name(streamName)
                        .storageType(StorageType.Memory)
                        .retentionPolicy(RetentionPolicy.Limits)
                        .subjects(subjects)
                        .build());
    }

    private VerificationConsumer createVerificationConsumer(
            String streamName, String subscriptionSubject)
            throws IOException, InterruptedException, JetStreamApiException {
        Connection connection = createClientConnection();
        PullSubscribeOptions subscribeOptions =
                PullSubscribeOptions.builder().stream(streamName)
                        .durable("consumer-" + UUID.randomUUID())
                        .build();
        JetStreamSubscription subscription =
                connection.jetStream().subscribe(subscriptionSubject, subscribeOptions);
        return new VerificationConsumer(connection, subscription);
    }

    private static Map<String, String> mapOf(String... keyValues) {
        Map<String, String> map = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            map.put(keyValues[i], keyValues[i + 1]);
        }
        return map;
    }

    private static final class VerificationConsumer implements AutoCloseable {
        private final Connection connection;
        private final JetStreamSubscription subscription;

        private VerificationConsumer(Connection connection, JetStreamSubscription subscription) {
            this.connection = connection;
            this.subscription = subscription;
        }

        private List<Message> awaitMessages(int expectedCount) {
            List<Message> receivedMessages = new ArrayList<>(expectedCount);
            Awaitility.await()
                    .atMost(TEST_TIMEOUT)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .until(
                            () -> {
                                subscription.pull(expectedCount - receivedMessages.size());
                                while (receivedMessages.size() < expectedCount) {
                                    Message message =
                                            subscription.nextMessage(Duration.ofMillis(200));
                                    if (message == null) {
                                        break;
                                    }
                                    receivedMessages.add(message);
                                    message.ack();
                                }
                                return receivedMessages.size() >= expectedCount;
                            });
            return receivedMessages;
        }

        @Override
        public void close() throws Exception {
            if (connection != null) {
                connection.close();
            }
        }
    }

    private static final class NativeExpectedMessage {
        private final String subject;
        private final String messageId;
        private final Map<String, String> headers;
        private final byte[] payload;

        private NativeExpectedMessage(
                String subject, String messageId, Map<String, String> headers, byte[] payload) {
            this.subject = subject;
            this.messageId = messageId;
            this.headers = headers;
            this.payload = payload;
        }

        private NativeActualMessage toActualMessage() {
            return new NativeActualMessage(subject, messageId, headers, payload);
        }
    }

    private static final class NativeActualMessage {
        private final String subject;
        private final String messageId;
        private final Map<String, String> headers;
        private final byte[] payload;

        private NativeActualMessage(
                String subject, String messageId, Map<String, String> headers, byte[] payload) {
            this.subject = subject;
            this.messageId = messageId;
            this.headers = headers;
            this.payload = payload;
        }

        private static NativeActualMessage fromMessage(Message message) {
            Headers headers = message.getHeaders();
            Map<String, String> headerMap = new LinkedHashMap<>();
            String messageId = null;
            if (headers != null) {
                for (String key : headers.keySet()) {
                    if (!"Nats-Msg-Id".equals(key)) {
                        headerMap.put(key, headers.getFirst(key));
                    }
                }
                messageId = headers.getFirst("Nats-Msg-Id");
            }
            return new NativeActualMessage(
                    message.getSubject(), messageId, headerMap, message.getData());
        }

        private static int compareByContent(NativeActualMessage left, NativeActualMessage right) {
            int subjectComparison = left.subject.compareTo(right.subject);
            if (subjectComparison != 0) {
                return subjectComparison;
            }
            int messageIdComparison =
                    java.util.Objects.toString(left.messageId, "")
                            .compareTo(java.util.Objects.toString(right.messageId, ""));
            if (messageIdComparison != 0) {
                return messageIdComparison;
            }
            return comparePayloads(left.payload, right.payload);
        }

        private static int comparePayloads(byte[] left, byte[] right) {
            int length = Math.min(left.length, right.length);
            for (int i = 0; i < length; i++) {
                int comparison = Byte.compare(left[i], right[i]);
                if (comparison != 0) {
                    return comparison;
                }
            }
            return Integer.compare(left.length, right.length);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof NativeActualMessage)) {
                return false;
            }
            NativeActualMessage that = (NativeActualMessage) obj;
            return subject.equals(that.subject)
                    && java.util.Objects.equals(messageId, that.messageId)
                    && headers.equals(that.headers)
                    && Arrays.equals(payload, that.payload);
        }

        @Override
        public int hashCode() {
            int result = java.util.Objects.hash(subject, messageId, headers);
            result = 31 * result + Arrays.hashCode(payload);
            return result;
        }
    }
}
