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
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;
import org.apache.seatunnel.e2e.common.util.DependencyJar;
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;

import org.awaitility.Awaitility;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
        int successLogStart = container.getServerLogs().length();
        Container.ExecResult success = container.executeJob(CONFIG, variables("success"));
        Assertions.assertEquals(0, success.getExitCode(), success.getStderr());
        String serverLogs = container.getServerLogs();
        String successLogs =
                success.getStdout()
                        + success.getStderr()
                        + serverLogs.substring(Math.min(successLogStart, serverLogs.length()));
        Assertions.assertTrue(
                successLogs.contains("Parsed config file:"),
                "Parsed config must actually be logged");
        Assertions.assertFalse(
                successLogs.contains("fixture-token"), "Source credential leaked at INFO");
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

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void checkpointsDecodedPageAndRejectsChangedAcceptedPrefix(TestContainer container)
            throws Exception {
        for (boolean changed : Arrays.asList(false, true)) {
            PostgreSQLContainer<?> database =
                    new PostgreSQLContainer<>(DockerImageName.parse("postgres:14-alpine"))
                            .withDatabaseName("test")
                            .withUsername("test")
                            .withPassword("test")
                            .withNetwork(NETWORK)
                            .withNetworkAliases("slack-prefix-postgres")
                            .withCommand("postgres -c max_prepared_transactions=100");
            ExecutorService jobs = Executors.newSingleThreadExecutor();
            try {
                container.executeExtraCommands(
                        engine ->
                                DependencyJar.of(Driver.class)
                                        .copyTo(
                                                engine,
                                                "/tmp/seatunnel/plugins/Jdbc/lib",
                                                "postgresql.jar"));
                database.start();
                try (Connection connection = prefixConnection(database);
                        Statement statement = connection.createStatement()) {
                    statement.execute(
                            "CREATE TABLE prefix_rows (\"channel_id\" TEXT, \"ts\" TEXT, \"thread_ts\" TEXT, \"user\" TEXT, \"text\" TEXT, \"type\" TEXT, \"subtype\" TEXT, \"raw_json\" TEXT)");
                }
                fixture("/reset");
                String scenario = changed ? "prefix-changed" : "prefix-stable";
                String initial = Long.toString(JobIdGenerator.newJobId());
                String resumed = Long.toString(JobIdGenerator.newJobId());
                Path file = prefixConfig(scenario);
                String resource = "/" + file.getFileName();
                try {
                    Future<Container.ExecResult> running =
                            jobs.submit(() -> container.executeJob(resource, initial));
                    Awaitility.await()
                            .atMost(90, TimeUnit.SECONDS)
                            .until(() -> fixture("/requests").size() == 1);
                    // The old page-wide critical section drains for about 50 seconds; it cannot
                    // commit this partial transaction within the shorter observation interval.
                    Awaitility.await()
                            .atMost(20, TimeUnit.SECONDS)
                            .untilAsserted(
                                    () -> {
                                        int count = prefixRows(database).size();
                                        Assertions.assertTrue(count >= 5 && count < 100);
                                    });
                    long baseline = container.getCompletedCheckpointCount(initial);
                    Awaitility.await()
                            .atMost(10, TimeUnit.SECONDS)
                            .until(() -> container.getCompletedCheckpointCount(initial) > baseline);
                    Assertions.assertEquals(1, fixture("/requests").size());
                    Assertions.assertTrue(prefixRows(database).size() < 100);
                    container.stopJob(initial);
                    Awaitility.await()
                            .atMost(30, TimeUnit.SECONDS)
                            .until(() -> "CANCELED".equals(container.getJobStatus(initial)));
                    running.get(60, TimeUnit.SECONDS);
                    List<List<String>> committed = prefixRows(database);
                    Assertions.assertTrue(committed.size() >= 5 && committed.size() < 100);
                    String before = container.getServerLogs();
                    Container.ExecResult result =
                            container.restoreJobWithCheckpoint(resource, initial, resumed);
                    if (changed) {
                        String logs =
                                result.getStdout()
                                        + result.getStderr()
                                        + container.getServerLogs().substring(before.length());
                        Assertions.assertNotEquals(0, result.getExitCode(), logs);
                        Assertions.assertTrue(
                                logs.contains("checkpoint page prefix changed or is missing"),
                                logs);
                        Assertions.assertEquals(committed, prefixRows(database));
                    } else {
                        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
                        List<List<String>> rows = prefixRows(database);
                        Assertions.assertEquals(100, rows.size());
                        for (int index = 0; index < 100; index++) {
                            Assertions.assertEquals("C1", rows.get(index).get(0));
                            String timestamp =
                                    String.format(Locale.ROOT, "1700000000.%06d", index + 1);
                            String text = String.format(Locale.ROOT, "row-%03d", index);
                            Assertions.assertEquals(
                                    Arrays.asList(
                                            "C1",
                                            timestamp,
                                            null,
                                            null,
                                            text,
                                            null,
                                            null,
                                            "{\"ts\":\""
                                                    + timestamp
                                                    + "\",\"text\":\""
                                                    + text
                                                    + "\"}"),
                                    rows.get(index));
                        }
                    }
                    Assertions.assertEquals(
                            2,
                            fixture("/requests").size(),
                            "Restore refetches only the same partial page");
                } finally {
                    try {
                        cancelPrefixJob(container, initial);
                    } finally {
                        try {
                            cancelPrefixJob(container, resumed);
                        } finally {
                            Files.deleteIfExists(file);
                        }
                    }
                }
            } finally {
                try {
                    jobs.shutdownNow();
                } finally {
                    database.close();
                }
            }
        }
    }

    /**
     * Transactional append-only JDBC receipts expose committed prefixes without an upsert hiding
     * replay. This configuration uses only the existing HTTP E2E JDBC/driver dependency closure.
     */
    private static Path prefixConfig(String scenario) throws Exception {
        String source =
                "Slack { token=\"fixture-token\"\n channel_ids=[\"C1\"]\n"
                        + " api_root=\"http://slack-fixture:1080/"
                        + scenario
                        + "/\"\n"
                        + " page_size=100\n oldest=\"1699999999.999999\"\n latest=\"1700000100.000000\"\n"
                        + " max_retries=0\n request_timeout_ms=30000\n max_response_bytes=65536\n"
                        + " max_pages_per_channel=10\n}";
        String config =
                "env {\n parallelism=1\n job.mode=BATCH\n job.retry.times=0\n"
                        + " read_limit.rows_per_second=2\n checkpoint.interval=1000\n"
                        + " checkpoint.timeout=30000\n checkpoint.retain-after-job-cancelled=true\n}\n"
                        + "source {\n"
                        + source
                        + "\n}\nsink { Jdbc {\n"
                        + " driver=org.postgresql.Driver\n url=\"jdbc:postgresql://slack-prefix-postgres:5432/test\"\n"
                        + " username=test\n password=test\n batch_size=1\n max_retries=0\n"
                        + " is_exactly_once=true\n xa_data_source_class_name=org.postgresql.xa.PGXADataSource\n"
                        + " transaction_timeout_sec=3600\n query=\"\"\"INSERT INTO prefix_rows (\"channel_id\", \"ts\", \"thread_ts\", \"user\", \"text\", \"type\", \"subtype\", \"raw_json\")"
                        + " VALUES (:channel_id, :ts, :thread_ts, :user, :text, :type, :subtype, :raw_json)\"\"\"\n} }\n";
        Path file =
                Files.createTempFile(
                        ContainerUtil.getCurrentModulePath().resolve("src/test/resources"),
                        "slack-prefix-",
                        ".conf");
        Files.write(file, config.getBytes(StandardCharsets.UTF_8));
        return file;
    }

    private static Connection prefixConnection(PostgreSQLContainer<?> database) throws Exception {
        return DriverManager.getConnection(
                database.getJdbcUrl(), database.getUsername(), database.getPassword());
    }

    /**
     * Reads committed append-only rows in provider order. Duplicate rows remain visible and make
     * the exact row/count assertion fail instead of being hidden by a primary key or upsert.
     */
    private static List<List<String>> prefixRows(PostgreSQLContainer<?> database) throws Exception {
        List<List<String>> rows = new ArrayList<>();
        try (Connection connection = prefixConnection(database);
                Statement statement = connection.createStatement();
                ResultSet values =
                        statement.executeQuery(
                                "SELECT \"channel_id\", \"ts\", \"thread_ts\", \"user\", \"text\", \"type\", \"subtype\", \"raw_json\" FROM prefix_rows ORDER BY ts")) {
            while (values.next()) {
                List<String> row = new ArrayList<>();
                for (int column = 1; column <= 8; column++) {
                    row.add(values.getString(column));
                }
                rows.add(row);
            }
        }
        return rows;
    }

    private static void cancelPrefixJob(TestContainer container, String id) throws Exception {
        String state = container.getJobStatus(id);
        if ("RUNNING".equals(state)
                || "SCHEDULED".equals(state)
                || "FAILING".equals(state)
                || "RESTARTING".equals(state)) {
            container.cancelJob(id);
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
