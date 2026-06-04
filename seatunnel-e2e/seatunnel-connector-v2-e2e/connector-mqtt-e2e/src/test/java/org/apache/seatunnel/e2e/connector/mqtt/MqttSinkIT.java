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
import org.apache.seatunnel.e2e.common.container.TestContainer;

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
import org.testcontainers.containers.Container;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * E2E integration test for the MQTT Sink connector. Spins up an ephemeral Eclipse Mosquitto broker
 * via Testcontainers, executes a SeaTunnel job using FakeSource, and independently verifies the
 * published MQTT messages from a subscriber client.
 */
@Slf4j
public class MqttSinkIT extends TestSuiteBase implements TestResource {

    private static final String IMAGE = "eclipse-mosquitto:2.0.15";
    private static final String NETWORK_ALIAS = "mqtt-e2e";
    private static final int MQTT_PORT = 1883;
    private static final String TEST_TOPIC = "test/seatunnel/sink";
    private static final int EXPECTED_ROW_COUNT = 16;

    private GenericContainer<?> mosquittoContainer;
    private MqttClient subscriberClient;
    private CopyOnWriteArrayList<String> receivedMessages;
    private CountDownLatch messageLatch;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        // Start Mosquitto broker with anonymous access configuration
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
        if (subscriberClient != null && subscriberClient.isConnected()) {
            subscriberClient.disconnect();
            subscriberClient.close();
        }
        if (mosquittoContainer != null) {
            mosquittoContainer.close();
        }
    }

    @TestTemplate
    public void testMqttSink(TestContainer container) throws Exception {
        // Prepare the independent subscriber before the SeaTunnel job runs.
        receivedMessages = new CopyOnWriteArrayList<>();
        messageLatch = new CountDownLatch(EXPECTED_ROW_COUNT);

        String brokerUrl =
                "tcp://"
                        + mosquittoContainer.getHost()
                        + ":"
                        + mosquittoContainer.getMappedPort(MQTT_PORT);
        subscriberClient =
                new MqttClient(brokerUrl, "e2e_test_subscriber", new MemoryPersistence());

        MqttConnectOptions opts = new MqttConnectOptions();
        opts.setCleanSession(true);
        opts.setAutomaticReconnect(true);
        subscriberClient.connect(opts);

        subscriberClient.setCallback(
                new MqttCallback() {
                    @Override
                    public void connectionLost(Throwable cause) {
                        log.warn("E2E subscriber lost connection", cause);
                    }

                    @Override
                    public void messageArrived(String topic, MqttMessage message) {
                        String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
                        receivedMessages.add(payload);
                        messageLatch.countDown();
                    }

                    @Override
                    public void deliveryComplete(IMqttDeliveryToken token) {}
                });
        subscriberClient.subscribe(TEST_TOPIC, 1);
        log.info("E2E subscriber connected and subscribed to topic '{}'", TEST_TOPIC);

        // Execute the SeaTunnel job
        Container.ExecResult execResult = container.executeJob("/mqtt_sink_e2e.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

        // Wait for all messages to arrive (with timeout)
        boolean allReceived = messageLatch.await(30, TimeUnit.SECONDS);
        Assertions.assertTrue(
                allReceived,
                "Expected "
                        + EXPECTED_ROW_COUNT
                        + " messages but received "
                        + receivedMessages.size());

        // Verify each message is valid JSON containing expected schema fields
        Assertions.assertEquals(EXPECTED_ROW_COUNT, receivedMessages.size());
        for (String msg : receivedMessages) {
            Assertions.assertTrue(msg.contains("\"id\""), "Missing 'id' field in: " + msg);
            Assertions.assertTrue(msg.contains("\"name\""), "Missing 'name' field in: " + msg);
            Assertions.assertTrue(msg.contains("\"age\""), "Missing 'age' field in: " + msg);
        }
        log.info(
                "E2E verification passed: received {} valid JSON messages",
                receivedMessages.size());

        // Cleanup subscriber for this test template iteration
        if (subscriberClient.isConnected()) {
            subscriberClient.disconnect();
            subscriberClient.close();
            subscriberClient = null;
        }
    }
}
