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

import static java.time.temporal.ChronoUnit.SECONDS;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.format.text.TextSerializationSchema;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;

import java.io.IOException;
import java.time.Duration;
import java.util.UUID;

/**
 * Start / stop a Pulsar cluster.
 */
@Slf4j
public class PulsarIT extends TestSuiteBase implements TestResource {

    private PulsarContainer pulsarService;

    private String serviceUrl;

    private String adminUrl;

    private String zkUrl;

    private ClientConfigurationData clientConfigurationData = new ClientConfigurationData();

    private ConsumerConfigurationData<byte[]> consumerConfigurationData = new ConsumerConfigurationData<>();

    private PulsarAdmin pulsarAdmin;

    private PulsarClient pulsarClient;

    private static final String TOPIC = "persistent://public/default/test";

    @BeforeAll
    @Override
    public void startUp() throws Exception {

        log.info("Starting PulsarService ");
        pulsarService = new PulsarContainer();
        pulsarService.addExposedPort(2181);
        pulsarService.waitingFor(new HttpWaitStrategy()
                .forPort(PulsarContainer.BROKER_HTTP_PORT)
                .forStatusCode(200)
                .forPath("/admin/v2/namespaces/public/default")
                .withStartupTimeout(Duration.of(400, SECONDS)));
        pulsarService.start();
        pulsarService.followOutput(new Slf4jLogConsumer(log));
        serviceUrl = pulsarService.getPulsarBrokerUrl();
        adminUrl = pulsarService.getHttpServiceUrl();
        zkUrl = "localhost:" + pulsarService.getMappedPort(2181);
        clientConfigurationData.setServiceUrl(serviceUrl);
        consumerConfigurationData.setSubscriptionMode(SubscriptionMode.NonDurable);
        consumerConfigurationData.setSubscriptionType(SubscriptionType.Exclusive);
        consumerConfigurationData.setSubscriptionName("flink-" + UUID.randomUUID());
        log.info("serviceUrl:{}", serviceUrl);
        log.info("adminUrl:{}", adminUrl);
        log.info("zkUrl:{}", zkUrl);

        ClientBuilder builder = PulsarClient.builder();
        builder.serviceUrl(serviceUrl);
        pulsarClient = builder.build();

        TextSerializationSchema serializer = TextSerializationSchema.builder()
                .seaTunnelRowType(seaTunnelRowType)
                .delimiter(",")
                .build();
        generateTestData(row -> new String(serializer.serialize(row)), 0, 2);

        log.info("Successfully started PulsarService");
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        log.info("-------------------------------------------------------------------------");
        log.info("    Shut down PulsarService ");
        log.info("-------------------------------------------------------------------------");

        if (pulsarService != null) {
            pulsarService.stop();
        }
        if (pulsarAdmin != null) {
            pulsarAdmin.close();
        }

        log.info("-------------------------------------------------------------------------");
        log.info("    PulsarService finished");
        log.info("-------------------------------------------------------------------------");
    }

    @TestTemplate
    public void testSourcePulsarTextToConsole(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/pulsarsource_text_to_console.conf");
        log.info("execResult.getExitCode:{}", execResult.getExitCode());
        log.info("execResult.getStdout:{}", execResult.getStdout());
        log.info("execResult.getStderr:{}", execResult.getStderr());

        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    private void generateTestData(ProducerRecordConverter converter, int start, int end) throws PulsarClientException {
        try (
                Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                        .topic(TOPIC)
                        .create();
        ) {
            for (int i = start; i < end; i++) {
                SeaTunnelRow row = new SeaTunnelRow(new Object[]{Long.valueOf(i), "pulsarsource-test" + i});
                String producerRecord = converter.convert(row);
                producer.send(producerRecord);
            }
        }
    }

    interface ProducerRecordConverter {
        String convert(SeaTunnelRow row);
    }

    @SuppressWarnings("checkstyle:InnerTypeLast")
    SeaTunnelRowType seaTunnelRowType = new SeaTunnelRowType(
            new String[]{"id", "c_string"},
            new SeaTunnelDataType[]{BasicType.LONG_TYPE, BasicType.STRING_TYPE}
    );
}
