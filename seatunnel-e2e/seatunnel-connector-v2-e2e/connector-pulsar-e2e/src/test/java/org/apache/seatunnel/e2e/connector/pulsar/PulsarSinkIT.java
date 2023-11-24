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
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static java.time.temporal.ChronoUnit.SECONDS;

@Slf4j
public class PulsarSinkIT extends TestSuiteBase implements TestResource {

    private static final String PULSAR_IMAGE_NAME = "apachepulsar/pulsar:2.3.1";
    public static final String PULSAR_HOST = "pulsar.e2e.sink";
    public static final String TOPIC = "topic-test02";
    private PulsarContainer pulsarContainer;

    @Override
    @BeforeAll
    public void startUp() throws Exception {
        pulsarContainer =
                new PulsarContainer(DockerImageName.parse(PULSAR_IMAGE_NAME))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(PULSAR_HOST)
                        .withStartupTimeout(Duration.of(400, SECONDS))
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(PULSAR_IMAGE_NAME)));

        Startables.deepStart(Stream.of(pulsarContainer)).join();
        Awaitility.given()
                .ignoreExceptions()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(180, TimeUnit.SECONDS);
    }

    @Override
    public void tearDown() throws Exception {
        pulsarContainer.close();
    }

    private Map<String, String> getPulsarConsumerData() {
        Map<String, String> data = new HashMap<>();
        try {
            PulsarClient client =
                    PulsarClient.builder().serviceUrl(pulsarContainer.getPulsarBrokerUrl()).build();

            Consumer consumer =
                    client.newConsumer()
                            .topic(TOPIC)
                            .subscriptionName("PulsarSubTest01")
                            .subscriptionType(SubscriptionType.Exclusive)
                            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                            .subscribe();
            System.out.println("consumer.getLastMessageId()： " + consumer.getLastMessageId());
            while (true) {
                Message msg = consumer.receive();
                if (msg != null) {
                    data.put(msg.getKey(), new String(msg.getData()));
                    System.out.println("key: " + msg.getKey());
                    System.out.println("data: " + new String(msg.getData()));
                    System.out.println("MessageId: " + msg.getMessageId());
                    consumer.acknowledge(msg.getMessageId());
                }
                if (msg.getMessageId().equals(consumer.getLastMessageId())) {
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return data;
    }

    @TestTemplate
    public void testSinkKafka(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/fake_to_pulsar.conf");
        Assertions.assertEquals(execResult.getExitCode(), 0);

        Map<String, String> data = getPulsarConsumerData();
        ObjectMapper objectMapper = new ObjectMapper();
        String key = data.keySet().iterator().next();
        ObjectNode objectNode = objectMapper.readValue(key, ObjectNode.class);
        Assertions.assertTrue(objectNode.has("c_map"));
        Assertions.assertTrue(objectNode.has("c_string"));
        Assertions.assertEquals(10, data.size());
    }
}
