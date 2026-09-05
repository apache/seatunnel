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

package org.apache.seatunnel.e2e.connector.azure.queue;

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
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import com.azure.storage.queue.QueueClient;
import com.azure.storage.queue.QueueClientBuilder;
import com.azure.storage.queue.models.QueueMessageItem;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;

@Slf4j
public class AzureQueueStorageIT extends TestSuiteBase implements TestResource {

    private static final String AZURITE_IMAGE = "mcr.microsoft.com/azure-storage/azurite:3.35.0";
    private static final String ACCOUNT_NAME = "devstoreaccount1";
    private static final String ACCOUNT_KEY =
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
    private static final String QUEUE_NAME = "events";
    private static final String SOURCE_QUEUE_NAME = "source-events";
    private static final int QUEUE_PORT = 10001;
    private static final String NETWORK_ALIAS = "azure-queue";
    private static final String JOB_CONFIG = "/azurequeue/fake_to_azure_queue.conf";
    private static final String SOURCE_JOB_CONFIG = "/azurequeue/azure_queue_to_console.conf";
    private static final String EXPECTED_MESSAGE = "{\"name\":\"alice\",\"age\":30}";
    private static final String EXPECTED_SOURCE_EVENT_ID = "azure-queue-source-checkpoint-event";
    private static final String SOURCE_MESSAGE =
            "{\"event_id\":\"" + EXPECTED_SOURCE_EVENT_ID + "\",\"event_type\":\"created\"}";

    private GenericContainer<?> azurite;
    private QueueClient queueClient;
    private QueueClient sourceQueueClient;

    @BeforeAll
    @Override
    public void startUp() {
        DockerImageName image = DockerImageName.parse(AZURITE_IMAGE);
        azurite =
                new GenericContainer<>(image)
                        .withCommand(
                                "azurite-queue",
                                "--queueHost",
                                "0.0.0.0",
                                "--queuePort",
                                String.valueOf(QUEUE_PORT),
                                "--skipApiVersionCheck",
                                "--loose")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(NETWORK_ALIAS)
                        .withExposedPorts(QUEUE_PORT)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(
                                                image.asCanonicalNameString())));
        Startables.deepStart(Stream.of(azurite)).join();

        queueClient =
                new QueueClientBuilder()
                        .connectionString(hostConnectionString())
                        .queueName(QUEUE_NAME)
                        .buildClient();
        queueClient.createIfNotExists();
        sourceQueueClient =
                new QueueClientBuilder()
                        .connectionString(hostConnectionString())
                        .queueName(SOURCE_QUEUE_NAME)
                        .buildClient();
        sourceQueueClient.createIfNotExists();
        SeaTunnelContainer.enableAzureQueueReactorThreadExemption();
    }

    @AfterAll
    @Override
    public void tearDown() {
        try {
            if (azurite != null) {
                azurite.close();
            }
        } finally {
            SeaTunnelContainer.disableAzureQueueReactorThreadExemption();
        }
    }

    @TestTemplate
    public void testAzureQueueStorageSink(TestContainer container) throws Exception {
        Container.ExecResult result = container.executeJob(JOB_CONFIG);
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());

        AtomicReference<QueueMessageItem> receivedMessage = new AtomicReference<>();
        await().atMost(30, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(
                        () -> {
                            Iterator<QueueMessageItem> messages =
                                    queueClient.receiveMessages(1).iterator();
                            if (!messages.hasNext()) {
                                return false;
                            }
                            receivedMessage.set(messages.next());
                            return true;
                        });

        Assertions.assertEquals(EXPECTED_MESSAGE, receivedMessage.get().getBody().toString());
        queueClient.deleteMessage(
                receivedMessage.get().getMessageId(), receivedMessage.get().getPopReceipt());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK},
            disabledReason =
                    "The source checkpoint assertion uses the Zeta REST job status and server logs")
    public void testAzureQueueStorageSourceDeletesAfterCheckpoint(TestContainer container)
            throws Exception {
        sourceQueueClient.clearMessages();
        String jobId = String.valueOf(JobIdGenerator.newJobId());
        CompletableFuture<Container.ExecResult> jobFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return container.executeJob(SOURCE_JOB_CONFIG, jobId);
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

            long checkpointCount = container.getCompletedCheckpointCount(jobId);
            sourceQueueClient.sendMessage(SOURCE_MESSAGE);

            await().atMost(60, TimeUnit.SECONDS)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                assertJobStillRunning(jobFuture);
                                Assertions.assertTrue(
                                        container
                                                .getServerLogs()
                                                .contains(EXPECTED_SOURCE_EVENT_ID),
                                        "Azure Queue message was not emitted by the source");
                            });
            await().atMost(60, TimeUnit.SECONDS)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                assertJobStillRunning(jobFuture);
                                Assertions.assertTrue(
                                        container.getCompletedCheckpointCount(jobId)
                                                > checkpointCount,
                                        "No checkpoint completed after the Azure Queue message was emitted");
                                Assertions.assertNull(
                                        sourceQueueClient.peekMessage(),
                                        "Azure Queue message was not deleted after checkpoint completion");
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

    private void assertJobStillRunning(CompletableFuture<Container.ExecResult> jobFuture)
            throws Exception {
        if (jobFuture.isDone()) {
            Container.ExecResult result = jobFuture.get();
            Assertions.fail("Streaming source job terminated early:\n" + result.getStderr());
        }
    }

    private String hostConnectionString() {
        return "DefaultEndpointsProtocol=http;AccountName="
                + ACCOUNT_NAME
                + ";AccountKey="
                + ACCOUNT_KEY
                + ";QueueEndpoint=http://"
                + azurite.getHost()
                + ":"
                + azurite.getMappedPort(QUEUE_PORT)
                + "/"
                + ACCOUNT_NAME
                + ";";
    }
}
