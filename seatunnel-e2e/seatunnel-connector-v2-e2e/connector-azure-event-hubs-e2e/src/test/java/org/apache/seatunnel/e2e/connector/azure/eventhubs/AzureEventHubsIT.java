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
package org.apache.seatunnel.e2e.connector.azure.eventhubs;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.seatunnel.SeaTunnelContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;

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

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import com.azure.messaging.eventhubs.models.SendOptions;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;

@Slf4j
public class AzureEventHubsIT extends TestSuiteBase implements TestResource {

    private static final String EVENT_HUBS_IMAGE =
            "mcr.microsoft.com/azure-messaging/eventhubs-emulator:2.2.1";
    private static final String AZURITE_IMAGE = "mcr.microsoft.com/azure-storage/azurite:3.35.0";
    private static final String EVENT_HUBS_HOST = "eventhubs-emulator";
    private static final String AZURITE_HOST = "azurite";
    private static final int AMQP_PORT = 5672;
    private static final String EVENT_HUB_NAME = "events";
    private static final String JOB_CONFIG = "/eventhubs/azure_event_hubs_to_console.conf";
    private static final String PARTITION_0_EVENT = "eventhubs-partition-0";
    private static final String PARTITION_1_EVENT = "eventhubs-partition-1";
    private static final String SHARED_ACCESS_KEY = "SAS_KEY_VALUE";

    private GenericContainer<?> azurite;
    private GenericContainer<?> emulator;
    private EventHubProducerClient producer;

    @BeforeAll
    @Override
    public void startUp() {
        DockerImageName azuriteImage = DockerImageName.parse(AZURITE_IMAGE);
        azurite =
                new GenericContainer<>(azuriteImage)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(AZURITE_HOST)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(
                                                azuriteImage.asCanonicalNameString())));
        Startables.deepStart(Stream.of(azurite)).join();

        DockerImageName emulatorImage = DockerImageName.parse(EVENT_HUBS_IMAGE);
        emulator =
                new GenericContainer<>(emulatorImage)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(EVENT_HUBS_HOST)
                        .withExposedPorts(AMQP_PORT)
                        .withEnv("BLOB_SERVER", AZURITE_HOST)
                        .withEnv("METADATA_SERVER", AZURITE_HOST)
                        .withEnv("ACCEPT_EULA", "Y")
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("eventhubs/Config.json"),
                                "/Eventhubs_Emulator/ConfigFiles/Config.json")
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(
                                                emulatorImage.asCanonicalNameString())))
                        .waitingFor(
                                new HostPortWaitStrategy()
                                        .withStartupTimeout(Duration.ofMinutes(3)));
        Startables.deepStart(Stream.of(emulator)).join();

        producer =
                new EventHubClientBuilder()
                        .connectionString(hostConnectionString(), EVENT_HUB_NAME)
                        .buildProducerClient();
        await().atMost(60, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .ignoreExceptions()
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        2,
                                        producer.getPartitionIds().stream().count(),
                                        "Event Hubs emulator partitions are not ready"));
        SeaTunnelContainer.enableAzureSdkReactorThreadExemption();
    }

    @AfterAll
    @Override
    public void tearDown() {
        try {
            if (producer != null) {
                producer.close();
            }
            if (emulator != null) {
                emulator.close();
            }
            if (azurite != null) {
                azurite.close();
            }
        } finally {
            SeaTunnelContainer.disableAzureSdkReactorThreadExemption();
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK},
            disabledReason =
                    "The source checkpoint assertion uses the Zeta REST job status and server logs")
    public void testReadsAllPartitionsAndCompletesCheckpoint(TestContainer container)
            throws Exception {
        sendToPartition("0", PARTITION_0_EVENT);
        sendToPartition("1", PARTITION_1_EVENT);

        String jobId = String.valueOf(JobIdGenerator.newJobId());
        CompletableFuture<Container.ExecResult> jobFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return container.executeJob(JOB_CONFIG, jobId);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        try {
            await().atMost(60, TimeUnit.SECONDS)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                assertJobStillRunning(jobFuture);
                                Assertions.assertEquals("RUNNING", container.getJobStatus(jobId));
                            });
            await().atMost(60, TimeUnit.SECONDS)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                assertJobStillRunning(jobFuture);
                                String logs = container.getServerLogs();
                                Assertions.assertTrue(
                                        logs.contains(PARTITION_0_EVENT),
                                        "Partition 0 event was not emitted by the source");
                                Assertions.assertTrue(
                                        logs.contains(PARTITION_1_EVENT),
                                        "Partition 1 event was not emitted by the source");
                            });
            await().atMost(60, TimeUnit.SECONDS)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                assertJobStillRunning(jobFuture);
                                Assertions.assertTrue(
                                        container.getCompletedCheckpointCount(jobId) > 0,
                                        "No checkpoint completed after the events were emitted");
                            });
        } finally {
            if (!jobFuture.isDone()) {
                Container.ExecResult cancelResult = container.cancelJob(jobId);
                Assertions.assertEquals(0, cancelResult.getExitCode(), cancelResult.getStderr());
            }
        }

        Container.ExecResult jobResult = jobFuture.get(120, TimeUnit.SECONDS);
        Assertions.assertEquals(0, jobResult.getExitCode(), jobResult.getStderr());
    }

    private void sendToPartition(String partitionId, String eventId) {
        String body = "{\"event_id\":\"" + eventId + "\",\"event_type\":\"created\"}";
        producer.send(
                Collections.singletonList(new EventData(body)),
                new SendOptions().setPartitionId(partitionId));
    }

    private void assertJobStillRunning(CompletableFuture<Container.ExecResult> jobFuture)
            throws Exception {
        if (jobFuture.isDone()) {
            Container.ExecResult result = jobFuture.get();
            Assertions.fail("Streaming source job terminated early:\n" + result.getStderr());
        }
    }

    private String hostConnectionString() {
        return "Endpoint=sb://"
                + emulator.getHost()
                + ":"
                + emulator.getMappedPort(AMQP_PORT)
                + ";SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey="
                + SHARED_ACCESS_KEY
                + ";UseDevelopmentEmulator=true;";
    }
}
