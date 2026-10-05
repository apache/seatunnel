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

package org.apache.seatunnel.e2e.connector.pulsar;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static java.time.temporal.ChronoUnit.SECONDS;

@Slf4j
public class PulsarSinkIT extends TestSuiteBase implements TestResource {

    private static final String PULSAR_IMAGE_NAME = "apachepulsar/pulsar:2.3.1";
    public static final String PULSAR_HOST = "pulsar.e2e.sink";
    private static final String TOPIC = "topic_test02";

    /** Expected record count produced by fake_to_pulsar.conf. */
    private static final int EXPECTED_RECORD_COUNT = 10;

    /** Total time budget for draining sink output before failing the test. */
    private static final Duration RECEIVE_TIMEOUT = Duration.ofSeconds(60);

    /** Single receive poll timeout to avoid blocking the CI job forever. */
    private static final int RECEIVE_POLL_TIMEOUT_SECONDS = 5;

    private PulsarContainer pulsarContainer;

    @Override
    @BeforeAll
    public void startUp() throws Exception {
        pulsarContainer =
                PulsarContainerSupport.startPulsarContainer(
                        dockerClient,
                        PULSAR_IMAGE_NAME,
                        NETWORK,
                        PULSAR_HOST,
                        Duration.of(400, SECONDS));
        Awaitility.given()
                .ignoreExceptions()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(180, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertTrue(
                                        pulsarContainer.isRunning(),
                                        "Pulsar container should be running"));
    }

    @Override
    public void tearDown() throws Exception {
        pulsarContainer.close();
    }

    /** Create a dedicated client for the broker exposed by the test container. */
    private PulsarClient createPulsarClient() throws Exception {
        return PulsarClient.builder().serviceUrl(pulsarContainer.getPulsarBrokerUrl()).build();
    }

    /**
     * Subscribe to the sink topic before the batch job starts so the assertion cannot miss records
     * because the producer finished before the consumer began polling.
     */
    private Consumer<byte[]> createPulsarConsumer(PulsarClient client) throws Exception {
        return client.newConsumer()
                .topic(TOPIC)
                .subscriptionName("PulsarSubTest" + new Random().nextInt())
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
    }

    /**
     * Consume the records written by the sink job without leaving the test stuck in a blocking
     * receive call when CI delivery is delayed.
     */
    private List<String> getPulsarConsumerData(Consumer<byte[]> consumer) throws Exception {
        List<String> data = new ArrayList<>(EXPECTED_RECORD_COUNT);
        long deadlineNanos = System.nanoTime() + RECEIVE_TIMEOUT.toNanos();
        while (data.size() < EXPECTED_RECORD_COUNT && System.nanoTime() < deadlineNanos) {
            Message<byte[]> msg = consumer.receive(RECEIVE_POLL_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (msg == null) {
                continue;
            }

            String value = new String(msg.getData(), StandardCharsets.UTF_8);
            data.add(value);
            consumer.acknowledge(msg);
            log.info("value:{}", value);
        }

        if (data.size() < EXPECTED_RECORD_COUNT) {
            log.warn(
                    "Timed out waiting for {} Pulsar sink records, only consumed {}",
                    EXPECTED_RECORD_COUNT,
                    data.size());
        }
        return data;
    }

    @TestTemplate
    public void testSinkPulsar(TestContainer container) throws Exception {
        try (PulsarClient client = createPulsarClient();
                Consumer<byte[]> consumer = createPulsarConsumer(client)) {
            Container.ExecResult execResult = container.executeJob("/fake_to_pulsar.conf");
            Assertions.assertEquals(execResult.getExitCode(), 0);

            List<String> data = getPulsarConsumerData(consumer);
            log.info("data size:{}", data.size());
            Assertions.assertEquals(
                    EXPECTED_RECORD_COUNT,
                    data.size(),
                    String.format(
                            "Expected %d Pulsar records within %d seconds but received %d",
                            EXPECTED_RECORD_COUNT, RECEIVE_TIMEOUT.getSeconds(), data.size()));
            ObjectMapper objectMapper = new ObjectMapper();
            ObjectNode objectNode = objectMapper.readValue(data.get(0), ObjectNode.class);
            Assertions.assertTrue(objectNode.has("c_map"));
            Assertions.assertTrue(objectNode.has("c_array"));
            Assertions.assertTrue(objectNode.has("c_string"));
            Assertions.assertTrue(objectNode.has("c_boolean"));
            Assertions.assertTrue(objectNode.has("c_double"));
        }
    }
}
