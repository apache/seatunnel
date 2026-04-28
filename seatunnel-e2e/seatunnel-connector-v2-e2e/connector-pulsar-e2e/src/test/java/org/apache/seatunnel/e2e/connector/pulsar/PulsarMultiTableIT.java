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

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Slf4j
public class PulsarMultiTableIT extends TestSuiteBase implements TestResource {

    private static final String PULSAR_IMAGE_NAME = "apachepulsar/pulsar:2.3.1";
    public static final String PULSAR_HOST = "pulsar.multitable.e2e";
    private static final String TOPIC_ORDERS = "persistent://public/default/topic-orders";
    private static final String TOPIC_USERS = "persistent://public/default/topic-users-json";

    private PulsarContainer pulsarContainer;
    private PulsarClient client;

    @Override
    @BeforeAll
    public void startUp() throws Exception {
        pulsarContainer =
                new PulsarContainer(DockerImageName.parse(PULSAR_IMAGE_NAME))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(PULSAR_HOST)
                        .withStartupTimeout(Duration.ofMinutes(3))
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(PULSAR_IMAGE_NAME)));
        Startables.deepStart(Stream.of(pulsarContainer)).join();
        Awaitility.given()
                .ignoreExceptions()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(180, TimeUnit.SECONDS)
                .untilAsserted(this::produceTestData);
    }

    @Override
    public void tearDown() throws Exception {
        if (client != null) {
            client.close();
        }
        pulsarContainer.close();
    }

    private void produceTestData() throws PulsarClientException {
        client = PulsarClient.builder().serviceUrl(pulsarContainer.getPulsarBrokerUrl()).build();
        try (Producer<byte[]> ordersProducer =
                        client.newProducer(Schema.BYTES).topic(TOPIC_ORDERS).create();
                Producer<byte[]> usersProducer =
                        client.newProducer(Schema.BYTES).topic(TOPIC_USERS).create()) {
            ordersProducer.send(
                    "{\"order_id\":1,\"amount\":99.9}".getBytes(StandardCharsets.UTF_8));
            usersProducer.send("{\"user_id\":2,\"name\":\"bob\"}".getBytes(StandardCharsets.UTF_8));
        }
    }

    @TestTemplate
    void testMultiTableBatch(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult result = container.executeJob("/multi_table_pulsar_to_assert.conf");
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
    }
}
