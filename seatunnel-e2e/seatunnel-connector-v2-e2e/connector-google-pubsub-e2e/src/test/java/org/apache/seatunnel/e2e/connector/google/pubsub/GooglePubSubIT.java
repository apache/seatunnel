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

package org.apache.seatunnel.e2e.connector.google.pubsub;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
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

import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;

@Slf4j
public class GooglePubSubIT extends TestSuiteBase implements TestResource {

    private static final String EMULATOR_IMAGE =
            "gcr.io/google.com/cloudsdktool/google-cloud-cli:573.0.0-emulators";
    private static final String PROJECT_ID = "seatunnel-test";
    private static final String TOPIC_ID = "events";
    private static final String SUBSCRIPTION_ID = "events-test";
    private static final String SOURCE_TOPIC_ID = "source-events";
    private static final String SOURCE_SUBSCRIPTION_ID = "source-events-test";
    private static final int EMULATOR_PORT = 8085;
    private static final String EMULATOR_HOST = "pubsub-emulator";
    private static final String JOB_CONFIG = "/pubsub/fake_to_google_pubsub.conf";
    private static final String SOURCE_JOB_CONFIG = "/pubsub/google_pubsub_to_console.conf";
    private static final String EXPECTED_MESSAGE = "{\"name\":\"alice\",\"age\":30}";
    private static final String EXPECTED_SOURCE_EVENT_ID = "pubsub-source-checkpoint-event";
    private static final String SOURCE_MESSAGE =
            "{\"event_id\":\"" + EXPECTED_SOURCE_EVENT_ID + "\",\"event_type\":\"created\"}";

    private GenericContainer<?> emulator;
    private String emulatorEndpoint;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        DockerImageName image = DockerImageName.parse(EMULATOR_IMAGE);
        emulator =
                new GenericContainer<>(image)
                        .withCommand(
                                "gcloud",
                                "beta",
                                "emulators",
                                "pubsub",
                                "start",
                                "--project=" + PROJECT_ID,
                                "--host-port=0.0.0.0:" + EMULATOR_PORT)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(EMULATOR_HOST)
                        .withExposedPorts(EMULATOR_PORT)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(
                                                image.asCanonicalNameString())));
        Startables.deepStart(Stream.of(emulator)).join();

        emulatorEndpoint =
                "http://" + emulator.getHost() + ":" + emulator.getMappedPort(EMULATOR_PORT);
        createResource("/v1/projects/" + PROJECT_ID + "/topics/" + TOPIC_ID, "{}");
        createResource("/v1/projects/" + PROJECT_ID + "/topics/" + SOURCE_TOPIC_ID, "{}");
        createResource(
                "/v1/projects/" + PROJECT_ID + "/subscriptions/" + SUBSCRIPTION_ID,
                "{\"topic\":\"projects/"
                        + PROJECT_ID
                        + "/topics/"
                        + TOPIC_ID
                        + "\",\"ackDeadlineSeconds\":10}");
        createResource(
                "/v1/projects/" + PROJECT_ID + "/subscriptions/" + SOURCE_SUBSCRIPTION_ID,
                "{\"topic\":\"projects/"
                        + PROJECT_ID
                        + "/topics/"
                        + SOURCE_TOPIC_ID
                        + "\",\"ackDeadlineSeconds\":10}");
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (emulator != null) {
            emulator.close();
        }
    }

    @TestTemplate
    public void testGooglePubSubSink(TestContainer container) throws Exception {
        Container.ExecResult result = container.executeJob(JOB_CONFIG);
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());

        AtomicReference<ObjectNode> receivedMessage = new AtomicReference<>();
        await().atMost(30, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(
                        () -> {
                            ObjectNode response =
                                    JsonUtils.parseObject(
                                            request(
                                                    "POST",
                                                    "/v1/projects/"
                                                            + PROJECT_ID
                                                            + "/subscriptions/"
                                                            + SUBSCRIPTION_ID
                                                            + ":pull",
                                                    "{\"maxMessages\":1,\"returnImmediately\":true}"));
                            JsonNode messages = response.get("receivedMessages");
                            if (messages == null || messages.isEmpty()) {
                                return false;
                            }
                            receivedMessage.set((ObjectNode) messages.get(0));
                            return true;
                        });

        ObjectNode message = receivedMessage.get();
        String payload =
                new String(
                        Base64.getDecoder().decode(message.path("message").path("data").asText()),
                        StandardCharsets.UTF_8);
        Assertions.assertEquals(EXPECTED_MESSAGE, payload);
        acknowledge(message.path("ackId").asText());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK},
            disabledReason =
                    "The source checkpoint assertion uses the Zeta REST job status and server logs")
    public void testGooglePubSubSourceAcknowledgesAfterCheckpoint(TestContainer container)
            throws Exception {
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
            publish(SOURCE_TOPIC_ID, SOURCE_MESSAGE);

            await().atMost(60, TimeUnit.SECONDS)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                assertJobStillRunning(jobFuture);
                                Assertions.assertTrue(
                                        container
                                                .getServerLogs()
                                                .contains(EXPECTED_SOURCE_EVENT_ID),
                                        "Published Pub/Sub message was not emitted by the source");
                            });
            await().atMost(60, TimeUnit.SECONDS)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                assertJobStillRunning(jobFuture);
                                Assertions.assertTrue(
                                        container.getCompletedCheckpointCount(jobId)
                                                > checkpointCount,
                                        "No checkpoint completed after the Pub/Sub message was emitted");
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

    private void publish(String topicId, String payload) throws IOException {
        String data = Base64.getEncoder().encodeToString(payload.getBytes(StandardCharsets.UTF_8));
        request(
                "POST",
                "/v1/projects/" + PROJECT_ID + "/topics/" + topicId + ":publish",
                "{\"messages\":[{\"data\":\"" + data + "\"}]}");
    }

    private void assertJobStillRunning(CompletableFuture<Container.ExecResult> jobFuture)
            throws Exception {
        if (jobFuture.isDone()) {
            Container.ExecResult result = jobFuture.get();
            Assertions.fail("Streaming source job terminated early:\n" + result.getStderr());
        }
    }

    private void acknowledge(String ackId) throws IOException {
        request(
                "POST",
                "/v1/projects/" + PROJECT_ID + "/subscriptions/" + SUBSCRIPTION_ID + ":acknowledge",
                "{\"ackIds\":[\"" + ackId + "\"]}");
    }

    private void createResource(String path, String body) {
        await().atMost(30, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .ignoreExceptions()
                .untilAsserted(() -> request("PUT", path, body));
    }

    private String request(String method, String path, String body) throws IOException {
        HttpURLConnection connection =
                (HttpURLConnection) new URL(emulatorEndpoint + path).openConnection();
        try {
            connection.setRequestMethod(method);
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setConnectTimeout(10_000);
            connection.setReadTimeout(10_000);
            connection.setDoOutput(true);
            try (OutputStream output = connection.getOutputStream()) {
                output.write(body.getBytes(StandardCharsets.UTF_8));
            }

            int status = connection.getResponseCode();
            InputStream responseStream =
                    status >= 200 && status < 300
                            ? connection.getInputStream()
                            : connection.getErrorStream();
            String response = responseStream == null ? "" : readResponse(responseStream);
            if (status < 200 || status >= 300) {
                throw new IOException(
                        "Pub/Sub emulator request failed with status " + status + ": " + response);
            }
            return response;
        } finally {
            connection.disconnect();
        }
    }

    private String readResponse(InputStream input) throws IOException {
        try (InputStream response = input;
                ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[4096];
            int read;
            while ((read = response.read(buffer)) != -1) {
                output.write(buffer, 0, read);
            }
            return new String(output.toByteArray(), StandardCharsets.UTF_8);
        }
    }
}
