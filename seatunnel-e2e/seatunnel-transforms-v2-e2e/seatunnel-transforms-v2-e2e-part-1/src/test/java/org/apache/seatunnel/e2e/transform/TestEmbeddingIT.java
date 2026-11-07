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

package org.apache.seatunnel.e2e.transform;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static io.restassured.RestAssured.given;

@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK},
        disabledReason = "Currently SPARK not support adapt")
public class TestEmbeddingIT extends TestSuiteBase implements TestResource {
    private static final String TMP_DIR = "/tmp";
    private GenericContainer<?> mockserverContainer;
    private static final String IMAGE = "mockserver/mockserver:5.14.0";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final DateTimeFormatter MOCKSERVER_TIMESTAMP =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    @BeforeAll
    @Override
    public void startUp() {
        Optional<URL> resource =
                Optional.ofNullable(TestLLMIT.class.getResource("/mock-embedding.json"));
        this.mockserverContainer =
                new GenericContainer<>(DockerImageName.parse(IMAGE))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("mockserver")
                        .withExposedPorts(1080)
                        .withCopyFileToContainer(
                                MountableFile.forHostPath(
                                        new File(
                                                        resource.orElseThrow(
                                                                        () ->
                                                                                new IllegalArgumentException(
                                                                                        "Can not get config file of mockServer"))
                                                                .getPath())
                                                .getAbsolutePath()),
                                TMP_DIR + "/mock-embedding.json")
                        .withEnv(
                                "MOCKSERVER_INITIALIZATION_JSON_PATH",
                                TMP_DIR + "/mock-embedding.json")
                        .withEnv("MOCKSERVER_LOG_LEVEL", "WARN")
                        .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger(IMAGE)))
                        .waitingFor(new HttpWaitStrategy().forPath("/").forStatusCode(404));
        Startables.deepStart(Stream.of(mockserverContainer)).join();
    }

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                ContainerUtil.copyFileIntoContainers(
                        "/binary/cat.png", "/seatunnel/read/binary/cat.png", container);
            };

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (mockserverContainer != null) {
            mockserverContainer.stop();
        }
    }

    @TestTemplate
    public void testEmbedding(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/embedding_transform.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK},
            disabledReason = "Verifies Zeta failure without engine-level job retries")
    public void testRetryAfterExceedingMaxBackoff(TestContainer container)
            throws IOException, InterruptedException {
        for (String provider : Arrays.asList("OPENAI", "DOUBAO", "QIANFAN", "ZHIPU", "CUSTOM")) {
            Container.ExecResult result = executeRetryAfterJob(container, provider, "capped", 100);
            Assertions.assertNotEquals(0, result.getExitCode(), provider + result.getStdout());
            assertInvocationCount("/retry-after/capped/" + provider, 1);
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK},
            disabledReason = "Verifies Zeta failure without engine-level job retries")
    public void testRetryAfterHttpDateExceedingMaxBackoff(TestContainer container)
            throws IOException, InterruptedException {
        String date =
                DateTimeFormatter.RFC_1123_DATE_TIME.format(
                        ZonedDateTime.now(ZoneOffset.UTC).plusMinutes(2));
        given().contentType("application/json")
                .body(
                        MAPPER.writeValueAsString(
                                Map.of(
                                        "priority", 10,
                                        "httpRequest",
                                                Map.of(
                                                        "method",
                                                        "POST",
                                                        "path",
                                                        "/retry-after/date/ZHIPU",
                                                        "body",
                                                        Map.of(
                                                                "type",
                                                                "JSON",
                                                                "json",
                                                                "{\"input\":[\"retry-after-e2e\"]}",
                                                                "matchType",
                                                                "ONLY_MATCHING_FIELDS")),
                                        "httpResponse",
                                                Map.of(
                                                        "statusCode",
                                                        503,
                                                        "headers",
                                                        Map.of("Retry-After", Arrays.asList(date)),
                                                        "body",
                                                        "{}"))))
                .put(
                        "http://"
                                + mockserverContainer.getHost()
                                + ":"
                                + mockserverContainer.getMappedPort(1080)
                                + "/mockserver/expectation")
                .then()
                .statusCode(201);
        Container.ExecResult result = executeRetryAfterJob(container, "ZHIPU", "date", 100);
        Assertions.assertNotEquals(0, result.getExitCode(), result.getStdout());
        assertInvocationCount("/retry-after/date/ZHIPU", 1);
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK},
            disabledReason = "Verifies Zeta provider cooldown without engine-level job retries")
    public void testRetryAfterCooldownWithoutMaxBackoff(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult result = executeRetryAfterJob(container, "ZHIPU", "cooldown", 0);
        Assertions.assertEquals(0, result.getExitCode(), result.getStdout());
        assertInvocationCount("/retry-after/cooldown/ZHIPU", 2);
        // Compare provider-side timestamps, excluding job startup and shutdown time.
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            JsonNode exchanges =
                                    retrieveInvocations(
                                            "/retry-after/cooldown/ZHIPU", "REQUEST_RESPONSES");
                            Assertions.assertEquals(2, exchanges.size());
                            Assertions.assertEquals(
                                    429,
                                    exchanges
                                            .get(0)
                                            .path("httpResponse")
                                            .path("statusCode")
                                            .asInt());
                            Assertions.assertEquals(
                                    200,
                                    exchanges
                                            .get(1)
                                            .path("httpResponse")
                                            .path("statusCode")
                                            .asInt());
                            LocalDateTime first =
                                    LocalDateTime.parse(
                                            exchanges.get(0).path("timestamp").asText(),
                                            MOCKSERVER_TIMESTAMP);
                            LocalDateTime second =
                                    LocalDateTime.parse(
                                            exchanges.get(1).path("timestamp").asText(),
                                            MOCKSERVER_TIMESTAMP);
                            Assertions.assertTrue(
                                    Duration.between(first, second).toMillis() >= 2000,
                                    "The second request must respect Retry-After: 2");
                        });
    }

    private Container.ExecResult executeRetryAfterJob(
            TestContainer container, String provider, String scenario, long maxBackoff)
            throws IOException, InterruptedException {
        return container.executeJob(
                "/embedding_transform_retry_after.conf",
                Arrays.asList(
                        "retry_provider=" + provider,
                        "retry_api_path=http://mockserver:1080/retry-after/"
                                + scenario
                                + "/"
                                + provider,
                        "retry_max_backoff=" + maxBackoff));
    }

    private void assertInvocationCount(String path, int count) {
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        count, retrieveInvocations(path, "REQUESTS").size(), path));
    }

    private JsonNode retrieveInvocations(String path, String type) throws IOException {
        // Dimension probes must not satisfy the assertion about source-row invocations.
        String requestPath = path.endsWith("/QIANFAN") ? path + "/embedding-3" : path;
        String input =
                path.endsWith("/OPENAI")
                        ? "{\"input\":\"retry-after-e2e\"}"
                        : "{\"input\":[\"retry-after-e2e\"]}";
        String matcher =
                MAPPER.writeValueAsString(
                        Map.of(
                                "method",
                                "POST",
                                "path",
                                requestPath,
                                "body",
                                Map.of(
                                        "type", "JSON",
                                        "json", input,
                                        "matchType", "ONLY_MATCHING_FIELDS")));
        String response =
                given().contentType("application/json")
                        .body(matcher)
                        .put(
                                "http://"
                                        + mockserverContainer.getHost()
                                        + ":"
                                        + mockserverContainer.getMappedPort(1080)
                                        + "/mockserver/retrieve?type="
                                        + type)
                        .then()
                        .statusCode(200)
                        .extract()
                        .asString();
        JsonNode invocations = MAPPER.readTree(response);
        Assertions.assertTrue(invocations.isArray(), "Expected a MockServer invocation array");
        return invocations;
    }

    @TestTemplate
    public void testMultimodalEmbedding(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/embedding_transform_multimodal.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    public void testEmbeddingMultiTable(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/embedding_transform_multi_table.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    public void testEmbeddingWithCustomModel(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/embedding_transform_custom.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    public void testBinaryEmbeddingWithCompleteMode(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/embedding_transform_binary_complete_file.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    public void testBinaryEmbedding(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/embedding_transform_binary.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }
}
