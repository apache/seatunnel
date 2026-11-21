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

package org.apache.seatunnel.e2e.connector.mqtt;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;

/**
 * Covers submitting and running an MQTT source on Zeta end to end.
 *
 * <p>The connector previously had no source IT, only {@link MqttSinkIT}, which is why a source that
 * could not be submitted at all went unnoticed from its introduction until a user reported it: the
 * source configuration was not {@link java.io.Serializable}, so the logical DAG failed to
 * serialize. A unit test pins that specific field, and this covers the submission path it broke.
 *
 * <p><b>Why a probe loop rather than "publish once after RUNNING":</b> the job reaching RUNNING is
 * a master-side state. {@code PhysicalVertex#deployInternal} sets RUNNING as soon as the deploy
 * call returns, while the worker only opens the reader later, when the task walks INIT to
 * WAITING_RESTORE to {@code cycle.open()}, and {@code MqttSourceReader#open} is what actually
 * connects and subscribes. MQTT keeps no backlog for a subscriber that was not connected, so any
 * row published in that gap is dropped by the broker and never replayed. Publishing once on the
 * first poll that sees RUNNING therefore depends on winning a race that the engine does not
 * guarantee. Instead this drives uniquely named probe rows until one comes back on the echo topic,
 * which proves the reader is subscribed and the whole source to sink path is live, and only then
 * publishes the rows it asserts on. {@code FlussSourceIT#testFlussSourceStreamingLatest} uses the
 * same pattern for the same class of lag.
 */
@Slf4j
public class MqttSourceIT extends TestSuiteBase implements TestResource {

    private static final String IMAGE = "eclipse-mosquitto:2.0.15";
    private static final String NETWORK_ALIAS = "mqtt-e2e";
    private static final int MQTT_PORT = 1883;
    private static final String SOURCE_TOPIC = "test/seatunnel/source_in";
    private static final String ECHO_TOPIC = "test/seatunnel/source_out";
    private static final int EXPECTED_ROW_COUNT = 8;

    /**
     * Probe rows use ids far above the asserted range so they can never be mistaken for one of the
     * rows under test, and so a duplicate probe cannot affect the final assertion.
     */
    private static final long PROBE_ID_BASE = 1000L;

    /** Zeta parses the job id as a long over REST, so it has to be numeric. */
    private static final String JOB_ID = "96481357024690";

    private GenericContainer<?> mosquittoContainer;
    private MqttClient publisherClient;
    private MqttClient echoSubscriberClient;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        this.mosquittoContainer =
                new GenericContainer<>(DockerImageName.parse(IMAGE))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(NETWORK_ALIAS)
                        .withExposedPorts(MQTT_PORT)
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("mosquitto.conf"),
                                "/mosquitto/config/mosquitto.conf")
                        .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger(IMAGE)))
                        .waitingFor(
                                new HostPortWaitStrategy()
                                        .withStartupTimeout(Duration.ofMinutes(2)));
        Startables.deepStart(Stream.of(mosquittoContainer)).join();
        log.info(
                "Mosquitto container started on port {}",
                mosquittoContainer.getMappedPort(MQTT_PORT));
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        closeQuietly(publisherClient);
        closeQuietly(echoSubscriberClient);
        if (mosquittoContainer != null) {
            mosquittoContainer.close();
        }
    }

    /**
     * Zeta only: starting the job asynchronously and then cancelling it uses the Zeta-only
     * container APIs, the same reason FlussSourceIT excludes the other engines from its streaming
     * test.
     */
    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK},
            disabledReason = "Uses Zeta-only job status and cancel APIs")
    public void testMqttSourceReadsPublishedMessages(TestContainer container) throws Exception {
        CopyOnWriteArrayList<String> echoed = new CopyOnWriteArrayList<>();
        echoSubscriberClient = connectEchoSubscriber("e2e_source_echo_subscriber", echoed);

        CompletableFuture<Void> jobFuture =
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                container.executeJob("/mqtt_source_e2e.conf", JOB_ID);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        try {
            // assertJobStillRunning is what turns a failed submission, which is exactly the
            // regression this IT exists to catch, into an immediate failure carrying the real
            // cause instead of a three minute wait ending in "expected RUNNING but was null".
            await().atMost(3, TimeUnit.MINUTES)
                    .pollInterval(2, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                assertJobStillRunning(jobFuture);
                                Assertions.assertEquals("RUNNING", container.getJobStatus(JOB_ID));
                            });

            publisherClient = connectClient("e2e_source_publisher");

            AtomicInteger probeSeq = new AtomicInteger();
            await().atMost(3, TimeUnit.MINUTES)
                    .pollInterval(2, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                assertJobStillRunning(jobFuture);
                                int n = probeSeq.incrementAndGet();
                                publish(PROBE_ID_BASE + n, "probe_" + n);
                                Assertions.assertTrue(
                                        parseEchoedRows(echoed).keySet().stream()
                                                .anyMatch(id -> id >= PROBE_ID_BASE),
                                        "No probe row has come back on '"
                                                + ECHO_TOPIC
                                                + "' yet, so the source has not subscribed");
                            });
            log.info("Source is live after {} probe rows", probeSeq.get());

            for (int i = 0; i < EXPECTED_ROW_COUNT; i++) {
                publish(i, "row_" + i);
            }
            log.info("Published {} messages to '{}'", EXPECTED_ROW_COUNT, SOURCE_TOPIC);

            // Asserts the deserialized (id, name) pairs rather than a substring, so a source that
            // dropped or mistyped the id column cannot pass. Membership rather than an exact
            // count, because both hops run at QoS 1 and a redelivery is not a defect.
            await().atMost(3, TimeUnit.MINUTES)
                    .pollInterval(2, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                assertJobStillRunning(jobFuture);
                                Map<Long, String> rows = parseEchoedRows(echoed);
                                for (int i = 0; i < EXPECTED_ROW_COUNT; i++) {
                                    Assertions.assertEquals(
                                            "row_" + i,
                                            rows.get((long) i),
                                            "Missing or wrong row for id " + i + " in: " + rows);
                                }
                            });
        } finally {
            container.cancelJob(JOB_ID);
            awaitTerminalAndJoin(container, jobFuture);
        }
    }

    /**
     * Fails immediately if the async job ended before the test was done with it. Without this the
     * caller only sees the symptom, a status wait that never turns RUNNING, while the submission
     * exception stays buried in a log line.
     */
    private static void assertJobStillRunning(CompletableFuture<Void> jobFuture) {
        if (!jobFuture.isDone()) {
            return;
        }
        // Rethrows the original submission failure wrapped in a CompletionException.
        jobFuture.join();
        throw new IllegalStateException(
                "The MQTT source job exited on its own before the test finished with it");
    }

    /**
     * Waits for the cancel to be observable and then joins the async job. {@code
     * SeaTunnelContainer#doExecuteJob} runs a JVM thread leak check after the job ends, and that
     * check runs on this thread; without the join it races {@code @AfterAll} closing the container
     * and is skipped with a "container is not running" error on an otherwise passing run.
     */
    private void awaitTerminalAndJoin(TestContainer container, CompletableFuture<Void> jobFuture) {
        try {
            await().atMost(2, TimeUnit.MINUTES)
                    .pollInterval(2, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            "CANCELED", container.getJobStatus(JOB_ID)));
        } catch (Exception e) {
            log.warn("Job {} did not report CANCELED before the timeout", JOB_ID, e);
        }
        try {
            jobFuture.get(2, TimeUnit.MINUTES);
        } catch (Exception e) {
            // executeJob reports a non-zero exit once the job is cancelled, which is expected
            // here. The join is for the thread leak check above, not for the exit code.
            log.info("Async job finished after cancel: {}", e.toString());
        }
    }

    /** Ignores payloads that are not the expected row shape rather than failing the poll. */
    private static Map<Long, String> parseEchoedRows(List<String> payloads) {
        ObjectMapper mapper = new ObjectMapper();
        Map<Long, String> rows = new HashMap<>();
        for (String payload : payloads) {
            try {
                ObjectNode node = mapper.readValue(payload, ObjectNode.class);
                if (node.hasNonNull("id") && node.hasNonNull("name")) {
                    rows.put(node.get("id").asLong(), node.get("name").asText());
                }
            } catch (Exception e) {
                log.warn("Ignoring unparseable echoed payload: {}", payload, e);
            }
        }
        return rows;
    }

    private void publish(long id, String name) throws Exception {
        String payload = String.format("{\"id\":%d,\"name\":\"%s\"}", id, name);
        publisherClient.publish(
                SOURCE_TOPIC, new MqttMessage(payload.getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * The callback is installed before connecting and re-subscribes on every completed connect.
     * With automatic reconnect and a clean session the broker drops the subscription when the link
     * drops, so a plain MqttCallback would leave the test waiting out its timeout with nothing
     * arriving after a reconnect.
     */
    private MqttClient connectEchoSubscriber(String clientId, List<String> received)
            throws Exception {
        MqttClient client = newClient(clientId);
        client.setCallback(
                new MqttCallbackExtended() {
                    @Override
                    public void connectComplete(boolean reconnect, String serverUri) {
                        try {
                            client.subscribe(ECHO_TOPIC, 1);
                            log.info(
                                    "Echo subscriber subscribed to '{}' (reconnect={})",
                                    ECHO_TOPIC,
                                    reconnect);
                        } catch (Exception e) {
                            log.error("Failed to subscribe to '{}'", ECHO_TOPIC, e);
                        }
                    }

                    @Override
                    public void connectionLost(Throwable cause) {
                        log.warn("Echo subscriber lost connection", cause);
                    }

                    @Override
                    public void messageArrived(String topic, MqttMessage message) {
                        received.add(new String(message.getPayload(), StandardCharsets.UTF_8));
                    }

                    @Override
                    public void deliveryComplete(IMqttDeliveryToken token) {}
                });
        client.connect(connectOptions());
        return client;
    }

    private MqttClient connectClient(String clientId) throws Exception {
        MqttClient client = newClient(clientId);
        client.connect(connectOptions());
        return client;
    }

    private MqttClient newClient(String clientId) throws Exception {
        String brokerUrl =
                "tcp://"
                        + mosquittoContainer.getHost()
                        + ":"
                        + mosquittoContainer.getMappedPort(MQTT_PORT);
        return new MqttClient(brokerUrl, clientId, new MemoryPersistence());
    }

    private static MqttConnectOptions connectOptions() {
        MqttConnectOptions opts = new MqttConnectOptions();
        opts.setCleanSession(true);
        opts.setAutomaticReconnect(true);
        return opts;
    }

    private void closeQuietly(MqttClient client) {
        if (client == null) {
            return;
        }
        try {
            if (client.isConnected()) {
                client.disconnect();
            }
            client.close();
        } catch (Exception e) {
            log.warn("Failed to close MQTT client", e);
        }
    }
}
