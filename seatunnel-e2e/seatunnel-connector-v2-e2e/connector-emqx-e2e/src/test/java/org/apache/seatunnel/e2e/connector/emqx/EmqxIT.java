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

package org.apache.seatunnel.e2e.connector.emqx;

import org.apache.seatunnel.connectors.seatunnel.emqx.client.MqttClientUtil;
import org.apache.seatunnel.connectors.seatunnel.emqx.source.ClientMetadata;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;

@Slf4j
public class EmqxIT extends TestSuiteBase implements TestResource {
    private static final String EMQX_IMAGE_NAME = "emqx/emqx:5.5.1";
    private static final String EMQX_HOST = "emqx-broker";
    private static final int TCP_PORT = 1883;
    private static final int HTTP_PORT = 18083;

    private GenericContainer<?> emqxContainer;
    private MqttClient client;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        emqxContainer =
                new GenericContainer<>(DockerImageName.parse(EMQX_IMAGE_NAME))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(EMQX_HOST)
                        .withExposedPorts(TCP_PORT, HTTP_PORT)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(EMQX_IMAGE_NAME)));
        emqxContainer.setPortBindings(
                Lists.newArrayList(
                        String.format("%s:%s", TCP_PORT, TCP_PORT),
                        String.format("%s:%s", HTTP_PORT, HTTP_PORT)));
        Startables.deepStart(Stream.of(emqxContainer)).join();
        log.info("Emqx container started");
        Awaitility.given()
                .await()
                .ignoreExceptions()
                .atMost(180, TimeUnit.SECONDS)
                .untilAsserted(this::initEmqxClient);
    }

    private void insertTestData() throws MqttException {
        String value = "{\"id\":%s,\"name\":\"test\",\"age\":18}";
        for (int i = 0; i < 10; i++) {
            client.publish("testtopic/e2e", String.format(value, i).getBytes(), 0, false);
        }
    }

    private void initEmqxClient() throws MqttException {
        ClientMetadata metadata = new ClientMetadata();
        metadata.setClientId("test_for_ping");
        metadata.setBroker(String.format("tcp://%s:1883", emqxContainer.getHost()));
        metadata.setTopic("testtopic/e2e");
        client = MqttClientUtil.createMqttClient(metadata, "1", 0);
        Assertions.assertTrue(client.isConnected());
    }

    @TestTemplate
    public void testReadFromEmqx(TestContainer container) throws Exception {
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob("/emqx_streaming_to_console.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :{}", e.getMessage());
                        throw new RuntimeException();
                    }
                    return null;
                });
        Thread.sleep(5 * 1000L);
        insertTestData();
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            String logs = container.getServerLogs();
                            for (int i = 0; i < 10; i++) {
                                Assertions.assertTrue(
                                        logs.contains(
                                                "SeaTunnelRow#tableId= SeaTunnelRow#kind=INSERT : "
                                                        + i
                                                        + ", test, 18"));
                            }
                        });
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (emqxContainer != null) {
            emqxContainer.stop();
            log.info("Emqx container stopped");
        }
    }
}
