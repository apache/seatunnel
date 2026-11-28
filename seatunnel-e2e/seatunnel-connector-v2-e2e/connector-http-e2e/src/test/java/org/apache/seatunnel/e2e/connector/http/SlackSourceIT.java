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

package org.apache.seatunnel.e2e.connector.http;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.DependencyJar;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;
import org.postgresql.Driver;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

/**
 * Executes real engine jobs into PostgreSQL against a deterministic HTTP fixture. This API-contract
 * suite does not certify a live Slack workspace or token.
 */
@Tag("api-contract")
public class SlackSourceIT extends TestSuiteBase {
    private static final String CONFIG = "/slack_history_to_postgresql.conf";
    private static final String RESTORE_CONFIG = "/slack_history_restore.conf";
    private final ObjectMapper mapper = new ObjectMapper();
    private GenericContainer<?> fixture;
    private PostgreSQLContainer<?> postgres;

    @TestContainerExtension
    private final ContainerExtendedFactory driver =
            container ->
                    DependencyJar.of(Driver.class)
                            .copyTo(container, "/tmp/seatunnel/plugins/Jdbc/lib", "postgresql.jar");

    @BeforeAll
    void startFixture() throws Exception {
        fixture =
                new GenericContainer<>(DockerImageName.parse("python:3.11-alpine"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("slack-fixture")
                        .withExposedPorts(1080)
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("slack_history_fixture.py"),
                                "/fixture.py")
                        .withCommand("python", "/fixture.py")
                        .waitingFor(new HttpWaitStrategy().forPath("/ready").forStatusCode(200));
        fixture.start();
        postgres =
                new PostgreSQLContainer<>(DockerImageName.parse("postgres:14-alpine"))
                        .withDatabaseName("test")
                        .withUsername("test")
                        .withPassword("test")
                        .withNetwork(NETWORK)
                        .withNetworkAliases("slack-postgres");
        postgres.start();
        try (Connection connection = connection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "CREATE TABLE slack_history (channel_id TEXT, ts TEXT, thread_ts TEXT, \"user\" TEXT, text TEXT, type TEXT, subtype TEXT, raw_json TEXT)");
        }
    }

    @AfterAll
    void stopFixture() {
        if (fixture != null) {
            fixture.stop();
        }
        if (postgres != null) {
            postgres.stop();
        }
    }

    @TestTemplate
    void boundedHistoryAndFailures(TestContainer container) throws Exception {
        reset();
        Container.ExecResult success = container.executeJob(CONFIG, variables("success"));
        Assertions.assertEquals(0, success.getExitCode(), success.getStderr());
        List<List<String>> rows = rows();
        Assertions.assertEquals(3, rows.size());
        Assertions.assertEquals(
                Arrays.asList("C1", "1700000000.000001", null, "U1", "世界🚀", "message", null),
                rows.get(0).subList(0, 7));
        Assertions.assertEquals(
                Arrays.asList("C1", "1700000000.000002", null, null, null, null, null),
                rows.get(1).subList(0, 7));
        Assertions.assertEquals(
                Arrays.asList(
                        "C2",
                        "1700000000.000003",
                        "1700000000.000001",
                        null,
                        "café",
                        "message",
                        "bot_message"),
                rows.get(2).subList(0, 7));
        Assertions.assertEquals(
                mapper.readTree(
                        "{\"ts\":\"1700000000.000001\",\"user\":\"U1\",\"text\":\"世界🚀\",\"type\":\"message\"}"),
                mapper.readTree(rows.get(0).get(7)));
        Assertions.assertEquals(
                mapper.readTree("{\"ts\":\"1700000000.000002\"}"),
                mapper.readTree(rows.get(1).get(7)));
        Assertions.assertEquals(
                mapper.readTree(
                        "{\"ts\":\"1700000000.000003\",\"thread_ts\":\"1700000000.000001\",\"text\":\"café\",\"type\":\"message\",\"subtype\":\"bot_message\"}"),
                mapper.readTree(rows.get(2).get(7)));
        JsonNode requests = fixture("/requests");
        Assertions.assertEquals(5, requests.size());
        Assertions.assertEquals(Arrays.asList("", "short", "short", "last", ""), cursors(requests));
        ObjectNode throttled = requests.get(1).deepCopy();
        ObjectNode retried = requests.get(2).deepCopy();
        throttled.remove("received_ms");
        retried.remove("received_ms");
        Assertions.assertEquals(throttled, retried, "429 must retry the same nonempty cursor");
        Assertions.assertTrue(
                requests.get(2).path("received_ms").asLong()
                                - requests.get(1).path("received_ms").asLong()
                        >= 1000,
                "Retry-After must be honored");
        for (JsonNode request : requests) {
            Assertions.assertEquals("1700000100.000000", request.path("latest").asText());
            Assertions.assertEquals("1699999999.999999", request.path("oldest").asText());
            Assertions.assertEquals("15", request.path("limit").asText());
            Assertions.assertEquals("false", request.path("inclusive").asText());
        }

        // Scenario, sanitized diagnostic, exact HTTP attempts. Every failure runs a real job.
        String[][] failures = {
            {"auth", "permanent HTTP status 401", "1"},
            {"api-error", "Slack API error invalid_auth", "1"},
            {"api-error-secret", "Slack API error unrecognized_error", "1"},
            {"malformed", "missing messages array", "1"},
            {"has-more", "has_more without next_cursor", "1"},
            {"cycle", "non-progressing pagination cursor", "3"},
            {"exhausted", "HTTP retry budget exhausted", "3"},
            {"deadline-stall", "HTTP transport retry budget exhausted", "3"},
            {"deadline-slow", "HTTP transport retry budget exhausted", "3"},
            {"retry-after", "Retry-After exceeds max_retry_delay_ms", "1"},
            {"oversized", "max_response_bytes exceeded", "1"},
            {"page-limit", "max_pages_per_channel exceeded", "10"},
            {"redirect", "permanent HTTP status 302", "1"}
        };
        for (String[] failure : failures) {
            reset();
            int logStart = container.getServerLogs().length();
            Container.ExecResult result = container.executeJob(CONFIG, variables(failure[0]));
            assertFailure(container, result, logStart, failure[1]);
            Assertions.assertEquals(
                    Integer.parseInt(failure[2]), fixture("/requests").size(), failure[0]);
            Assertions.assertTrue(rows().isEmpty(), failure[0]);
            if (failure[0].startsWith("deadline-")) {
                for (JsonNode request : fixture("/requests")) {
                    Assertions.assertEquals("C1", request.path("channel").asText());
                    Assertions.assertEquals(
                            "",
                            request.path("cursor").asText(),
                            "Timeout must not advance page progress");
                    Assertions.assertEquals("1700000100.000000", request.path("latest").asText());
                }
            }
        }

        reset();
        int logStart = container.getServerLogs().length();
        CompletableFuture<Container.ExecResult> repeated =
                submit(() -> container.executeJob(CONFIG, variables("repeated")));
        try {
            await().atMost(90, TimeUnit.SECONDS)
                    .untilAsserted(() -> Assertions.assertEquals(1, rows().size()));
            fixture("/release");
            assertFailure(
                    container,
                    repeated.get(90, TimeUnit.SECONDS),
                    logStart,
                    "non-progressing pagination cursor");
            Assertions.assertEquals(
                    1, rows().size(), "The repeated-cursor page must never be emitted");
            Assertions.assertEquals(2, fixture("/requests").size());
        } finally {
            fixture("/release");
        }

        // The common harness runs all selected engines; only Zeta exposes savepoint/restore APIs.
        // No engine is skipped: every invocation executes all bounded success/failure jobs above.
        if (container.identifier() == TestContainerId.SEATUNNEL) {
            checkpointRestore(container);
        }
    }

    private void checkpointRestore(TestContainer container) throws Exception {
        reset();
        String jobId = Long.toString(System.currentTimeMillis());
        String[] variables = {"slack_scenario=restore"};
        CompletableFuture<Container.ExecResult> running =
                submit(() -> container.executeJob(RESTORE_CONFIG, jobId, variables));
        try {
            await().atMost(90, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(1, rows().size());
                                Assertions.assertTrue(
                                        cursors(fixture("/requests")).contains("hold"));
                                Assertions.assertTrue(
                                        container.getCompletedCheckpointCount(jobId) > 0,
                                        "Checkpoint must complete while HTTP is blocked");
                            });
            Container.ExecResult savepoint = container.savepointJob(jobId);
            Assertions.assertEquals(0, savepoint.getExitCode(), savepoint.getStderr());
            Assertions.assertEquals(0, running.get(90, TimeUnit.SECONDS).getExitCode());
            JsonNode beforeRestore = fixture("/requests");
            String frozenLatest = beforeRestore.get(0).path("latest").asText();
            Assertions.assertFalse(frozenLatest.isEmpty());
            fixture("/release");
            Container.ExecResult restored = container.restoreJob(RESTORE_CONFIG, jobId, variables);
            Assertions.assertEquals(0, restored.getExitCode(), restored.getStderr());
            List<List<String>> rows = rows();
            Assertions.assertEquals(
                    2,
                    rows.size(),
                    "Restored job must neither skip nor replay the checkpointed first page");
            Assertions.assertEquals("1700000000.000001", rows.get(0).get(1));
            Assertions.assertEquals("1700000000.000002", rows.get(1).get(1));
            JsonNode afterRestore = fixture("/requests");
            Assertions.assertTrue(afterRestore.size() > beforeRestore.size());
            Assertions.assertEquals(
                    "hold", afterRestore.get(beforeRestore.size()).path("cursor").asText());
            for (JsonNode request : afterRestore) {
                Assertions.assertEquals(frozenLatest, request.path("latest").asText());
            }
        } finally {
            fixture("/release");
            String status = container.getJobStatus(jobId);
            if ("RUNNING".equals(status)
                    || "FAILING".equals(status)
                    || "RESTARTING".equals(status)) {
                container.cancelJob(jobId);
            }
        }
    }

    private void assertFailure(
            TestContainer container, Container.ExecResult result, int logStart, String diagnostic) {
        Assertions.assertNotEquals(0, result.getExitCode(), diagnostic);
        String logs = container.getServerLogs();
        String evidence =
                result.getStdout()
                        + result.getStderr()
                        + logs.substring(Math.min(logStart, logs.length()));
        Assertions.assertTrue(evidence.contains(diagnostic), evidence);
        Assertions.assertFalse(
                evidence.contains("fixture-response-secret"), "Raw API response leaked");
    }

    private void reset() throws Exception {
        fixture("/reset");
        try (Connection connection = connection();
                Statement statement = connection.createStatement()) {
            statement.execute("TRUNCATE slack_history");
        }
    }

    private Connection connection() throws Exception {
        return DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }

    private List<List<String>> rows() throws Exception {
        List<List<String>> rows = new ArrayList<>();
        try (Connection connection = connection();
                Statement statement = connection.createStatement();
                ResultSet result =
                        statement.executeQuery(
                                "SELECT channel_id, ts, thread_ts, \"user\", text, type, subtype, raw_json FROM slack_history ORDER BY channel_id, ts")) {
            while (result.next()) {
                List<String> row = new ArrayList<>();
                for (int i = 1; i <= 8; i++) {
                    row.add(result.getString(i));
                }
                rows.add(row);
            }
        }
        return rows;
    }

    private JsonNode fixture(String path) throws Exception {
        HttpURLConnection connection =
                (HttpURLConnection)
                        new URL(
                                        "http://"
                                                + fixture.getHost()
                                                + ":"
                                                + fixture.getMappedPort(1080)
                                                + path)
                                .openConnection();
        connection.setConnectTimeout(5000);
        connection.setReadTimeout(5000);
        try (InputStream input = connection.getInputStream()) {
            return mapper.readTree(input);
        } finally {
            connection.disconnect();
        }
    }

    private static List<String> variables(String scenario) {
        return Arrays.asList(
                "slack_scenario=" + scenario,
                "slack_request_timeout_ms=" + (scenario.startsWith("deadline-") ? 500 : 30000));
    }

    private static List<String> cursors(JsonNode requests) {
        List<String> cursors = new ArrayList<>();
        requests.forEach(request -> cursors.add(request.path("cursor").asText()));
        return cursors;
    }

    private static CompletableFuture<Container.ExecResult> submit(Job job) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return job.run();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    /**
     * Executes one real engine operation on the asynchronous submit path used for held-response
     * jobs.
     */
    @FunctionalInterface
    private interface Job {
        Container.ExecResult run() throws Exception;
    }
}
