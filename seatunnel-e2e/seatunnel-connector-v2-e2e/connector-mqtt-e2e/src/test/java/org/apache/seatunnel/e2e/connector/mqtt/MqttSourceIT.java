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

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;

/**
 * Covers submitting and running an MQTT source on Zeta end to end.
 *
 * <p>The connector previously had no source IT, only {@link MqttSinkIT}, which is why a source that
 * could not be submitted at all went unnoticed from its introduction until a user reported it: the
 * source configuration was not {@link java.io.Serializable}, so the logical DAG failed to
 * serialize. A unit test pins that specific field, and this covers the submission path it broke.
 */
@Slf4j
public class MqttSourceIT extends TestSuiteBase implements TestResource {

    private static final String IMAGE = "eclipse-mosquitto:2.0.15";
    private static final String NETWORK_ALIAS = "mqtt-e2e";
    private static final int MQTT_PORT = 1883;
    private static final String SOURCE_TOPIC = "test/seatunnel/source_in";
    private static final String ECHO_TOPIC = "test/seatunnel/source_out";
    private static final int EXPECTED_ROW_COUNT = 8;

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
        echoSubscriberClient = connectClient("e2e_source_echo_subscriber");
        echoSubscriberClient.setCallback(
                new MqttCallback() {
                    @Override
                    public void connectionLost(Throwable cause) {
                        log.warn("Echo subscriber lost connection", cause);
                    }

                    @Override
                    public void messageArrived(String topic, MqttMessage message) {
                        echoed.add(new String(message.getPayload(), StandardCharsets.UTF_8));
                    }

                    @Override
                    public void deliveryComplete(IMqttDeliveryToken token) {}
                });
        echoSubscriberClient.subscribe(ECHO_TOPIC, 1);

        CompletableFuture.runAsync(
                () -> {
                    try {
                        container.executeJob("/mqtt_source_e2e.conf", JOB_ID);
                    } catch (Exception e) {
                        log.error("MQTT source job failed", e);
                        throw new RuntimeException(e);
                    }
                });

        // The job must be RUNNING before anything is published. MQTT keeps no backlog for a
        // subscriber that was not connected, so messages sent earlier would simply be dropped
        // rather than replayed once the source subscribes.
        await().atMost(3, TimeUnit.MINUTES)
                .pollInterval(2, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> Assertions.assertEquals("RUNNING", container.getJobStatus(JOB_ID)));

        publisherClient = connectClient("e2e_source_publisher");
        for (int i = 0; i < EXPECTED_ROW_COUNT; i++) {
            String payload = String.format("{\"id\":%d,\"name\":\"row_%d\"}", i, i);
            publisherClient.publish(
                    SOURCE_TOPIC, new MqttMessage(payload.getBytes(StandardCharsets.UTF_8)));
        }
        log.info("Published {} messages to '{}'", EXPECTED_ROW_COUNT, SOURCE_TOPIC);

        try {
            // Waits for every published row to appear rather than for an exact count. Both hops
            // run at QoS 1, which is at-least-once, so a redelivery would make an equality
            // assertion fail for a reason that is not a defect in the source.
            await().atMost(3, TimeUnit.MINUTES)
                    .pollInterval(2, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                for (int i = 0; i < EXPECTED_ROW_COUNT; i++) {
                                    String expected = "row_" + i;
                                    Assertions.assertTrue(
                                            echoed.stream()
                                                    .anyMatch(
                                                            payload -> payload.contains(expected)),
                                            "Missing '" + expected + "' in: " + echoed);
                                }
                            });
        } finally {
            container.cancelJob(JOB_ID);
        }
    }

    private MqttClient connectClient(String clientId) throws Exception {
        String brokerUrl =
                "tcp://"
                        + mosquittoContainer.getHost()
                        + ":"
                        + mosquittoContainer.getMappedPort(MQTT_PORT);
        MqttClient client = new MqttClient(brokerUrl, clientId, new MemoryPersistence());
        MqttConnectOptions opts = new MqttConnectOptions();
        opts.setCleanSession(true);
        opts.setAutomaticReconnect(true);
        client.connect(opts);
        return client;
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
