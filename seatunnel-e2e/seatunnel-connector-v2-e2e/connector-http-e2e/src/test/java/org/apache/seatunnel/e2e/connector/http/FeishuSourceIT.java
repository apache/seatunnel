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

import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.ReuseTestContainers;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;
import org.apache.seatunnel.e2e.common.util.DependencyJar;
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.postgresql.Driver;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Runs real engine jobs against the isolated provider fixture. Transactional append-only receipts
 * additionally distinguish checkpointed accepted prefixes from nontransactional HTTP observations.
 */
@ReuseTestContainers(TestContainerId.SEATUNNEL)
class FeishuSourceIT extends TestSuiteBase implements TestResource {

    private static final String CONFIG = "/feishu_source_to_http.conf";
    private static final int DEADLINE_MS = 1500;
    private static final double MAX_ACTIVE_DEADLINE_SECONDS = 5.0;
    private static final int PORT = 8080;
    private static final String FIRST_CURSOR = "page +/=中&?";
    private static final String END_CURSOR = "end+/=";
    private static final String FIELDS =
            "{\"文本\":\"你好🌊\",\"nested\":[{\"date\":1760000000123,\"empty\":null}],"
                    + "\"decimal\":1234567890.12345678901}";
    private static final String ALPHA =
            "{\"record_id\":\"rec-alpha\",\"id\":\"legacy-001\",\"fields\":"
                    + FIELDS
                    + ",\"created_time\":1760000000456,\"last_modified_time\":1760000000789}";
    private static final String BETA = "{\"record_id\":\"rec-beta\",\"id\":null,\"fields\":null}";

    private final AtomicInteger sequence = new AtomicInteger();
    private GenericContainer<?> provider;

    @BeforeAll
    @Override
    public void startUp() {
        provider =
                new GenericContainer<>(DockerImageName.parse("python:3.12-alpine"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("feishu-source-fixture")
                        .withExposedPorts(PORT)
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("feishu_source_fixture.py"),
                                "/fixture.py")
                        .withCommand("python", "/fixture.py")
                        .waitingFor(Wait.forHttp("/health").forPort(PORT));
        provider.start();
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (provider != null) {
            provider.close();
        }
    }

    @TestTemplate
    void readsFullTableWithExactRowsAndRefreshesExpiredTenantToken(TestContainer container)
            throws Exception {
        String scenario = scenario("happy");
        assertSuccess(
                container.executeJob(CONFIG, variables(scenario, "fixture-secret", 30000, 9)));
        JsonNode state = state(scenario);
        Assertions.assertEquals(2, state.path("auth").intValue());
        Assertions.assertEquals(
                Arrays.asList(null, FIRST_CURSOR, FIRST_CURSOR, END_CURSOR), cursors(state));
        assertBothRows(state.path("rows"));

        String empty = scenario("empty");
        assertSuccess(container.executeJob(CONFIG, variables(empty, "fixture-secret", 30000, 9)));
        JsonNode emptyState = state(empty);
        Assertions.assertEquals(Collections.singletonList(null), cursors(emptyState));
        Assertions.assertTrue(emptyState.path("rows").isEmpty());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void rejectsProviderErrorsMalformedPaginationAndRedirects(TestContainer container)
            throws Exception {
        assertFailed(container, "badauth", "wrong-secret", 30000, 9, "API code 10014", 0);
        assertFailed(container, "permission", "fixture-secret", 30000, 9, "API code 99991672", 1);
        JsonNode expired =
                assertFailed(
                        container,
                        "expiredforever",
                        "fixture-secret",
                        30000,
                        9,
                        "API code 99991663",
                        2);
        Assertions.assertEquals(2, expired.path("auth").intValue());
        assertFailed(
                container,
                "badcursor",
                "fixture-secret",
                30000,
                9,
                "requires a valid Bitable page_token",
                1);
        JsonNode cycle =
                assertFailed(
                        container, "cycle", "fixture-secret", 30000, 9, "repeated a cursor", 3);
        Assertions.assertEquals(Arrays.asList(null, FIRST_CURSOR, END_CURSOR), cursors(cycle));
        assertFailed(container, "limit", "fixture-secret", 30000, 1, "exceeded max_pages", 1);
        assertFailed(
                container,
                "authredirect",
                "fixture-secret",
                30000,
                9,
                "refused credential-bearing redirect with HTTP 307",
                0);
        assertFailed(
                container,
                "redirect",
                "fixture-secret",
                30000,
                9,
                "refused credential-bearing redirect with HTTP 303",
                1);
        assertFailed(container, "oversize", "fixture-secret", 30000, 9, "response exceeds", 1);
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void respectsRateLimitAndServerRetryDelaysAndPageDeadlines(TestContainer container)
            throws Exception {
        String retry = scenario("retry");
        assertSuccess(container.executeJob(CONFIG, variables(retry, "fixture-secret", 30000, 9)));
        JsonNode state = state(retry);
        JsonNode requests = state.path("requests");
        Assertions.assertEquals(4, requests.size());
        for (int i = 1; i < requests.size(); i++) {
            Assertions.assertTrue(
                    requests.get(i).path("time").asDouble()
                                    - requests.get(i - 1).path("time").asDouble()
                            >= 0.95,
                    "Each retry must honor its provider delay");
        }
        Assertions.assertEquals(1, state.path("rows").size());
        assertBeta(state.path("rows").get(0));

        String expiry = scenario("expiry");
        assertSuccess(container.executeJob(CONFIG, variables(expiry, "fixture-secret", 30000, 9)));
        JsonNode expiryState = state(expiry);
        Assertions.assertEquals(2, expiryState.path("auth").intValue());
        Assertions.assertEquals(2, expiryState.path("requests").size());
        Assertions.assertEquals(1, expiryState.path("rows").size());
        assertBeta(expiryState.path("rows").get(0));

        assertFailed(
                container,
                "retryexhaust",
                "fixture-secret",
                30000,
                9,
                "retries exhausted after HTTP 503",
                4);
        assertDeadlineFailure(container, "stall");
        assertDeadlineFailure(container, "slow");
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void cancelsWhilePageIoOrRetryBackoffIsPending(TestContainer container) throws Exception {
        for (String mode : Arrays.asList("stall", "retryhold")) {
            String scenario = scenario(mode);
            String jobId = String.valueOf(JobIdGenerator.newJobId());
            CompletableFuture<Container.ExecResult> job = start(container, scenario, jobId);
            try {
                awaitHeld(scenario);
                assertSuccess(async(() -> container.cancelJob(jobId)).get(25, TimeUnit.SECONDS));
                awaitStatus(container, jobId, "CANCELED", 15);
                job.get(15, TimeUnit.SECONDS);
                JsonNode state = state(scenario);
                Assertions.assertFalse(state.path("released").asBoolean());
                Assertions.assertEquals(1, state.path("requests").size());
                Assertions.assertTrue(state.path("rows").isEmpty());
            } finally {
                control("/release/" + scenario, true);
                if (!job.isDone()) {
                    async(() -> container.cancelJob(jobId)).get(20, TimeUnit.SECONDS);
                }
            }
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void stopsAndRestoresFromCommittedCursorWhilePageIsHeld(TestContainer container)
            throws Exception {
        String scenario = scenario("hold");
        String jobId = String.valueOf(JobIdGenerator.newJobId());
        CompletableFuture<Container.ExecResult> job = start(container, scenario, jobId);
        CompletableFuture<Container.ExecResult> restoration = null;
        try {
            awaitHeld(scenario);
            Awaitility.await()
                    .atMost(Duration.ofSeconds(30))
                    .untilAsserted(
                            () -> {
                                JsonNode state = state(scenario);
                                Assertions.assertEquals(1, state.path("rows").size());
                                assertAlpha(state.path("rows").get(0));
                                Assertions.assertTrue(state.path("held").asBoolean());
                                Assertions.assertFalse(state.path("released").asBoolean());
                            });
            long completedWhileHeld = container.getCompletedCheckpointCount(jobId);
            Awaitility.await()
                    .atMost(Duration.ofSeconds(30))
                    .untilAsserted(
                            () -> {
                                JsonNode held = state(scenario);
                                Assertions.assertTrue(held.path("held").asBoolean());
                                Assertions.assertFalse(held.path("released").asBoolean());
                                Assertions.assertEquals(2, held.path("requests").size());
                                Assertions.assertTrue(
                                        container.getCompletedCheckpointCount(jobId)
                                                > completedWhileHeld,
                                        "A new regular checkpoint must complete after the second page is held");
                            });

            assertSuccess(async(() -> container.savepointJob(jobId)).get(25, TimeUnit.SECONDS));
            awaitStatus(container, jobId, "SAVEPOINT_DONE", 15);
            assertSuccess(job.get(15, TimeUnit.SECONDS));
            JsonNode stopped = state(scenario);
            Assertions.assertFalse(stopped.path("released").asBoolean());
            Assertions.assertEquals(Arrays.asList(null, FIRST_CURSOR), cursors(stopped));
            Assertions.assertEquals(1, stopped.path("rows").size());

            control("/release/" + scenario, true);
            String[] variables =
                    variables(scenario, "fixture-secret", 120000, 9).toArray(new String[0]);
            restoration = async(() -> container.restoreJob(CONFIG, jobId, variables));
            assertSuccess(restoration.get(65, TimeUnit.SECONDS));
            JsonNode restored = state(scenario);
            Assertions.assertEquals(
                    Arrays.asList(null, FIRST_CURSOR, FIRST_CURSOR), cursors(restored));
            Assertions.assertEquals(
                    2,
                    restored.path("auth").intValue(),
                    "Restoration must authenticate separately");
            assertBothRows(restored.path("rows"));
        } finally {
            control("/release/" + scenario, true);
            if (!job.isDone() || (restoration != null && !restoration.isDone())) {
                async(() -> container.cancelJob(jobId)).get(20, TimeUnit.SECONDS);
            }
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
                            .withNetworkAliases("feishu-prefix-postgres")
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
                            "CREATE TABLE prefix_rows (\"record_id\" TEXT, \"id\" TEXT, \"fields\" TEXT, \"record_json\" TEXT)");
                }
                String scenario = changed ? scenario("prefixchanged") : scenario("prefixstable");
                String initial = Long.toString(JobIdGenerator.newJobId());
                String resumed = Long.toString(JobIdGenerator.newJobId());
                Path file = prefixConfig(scenario);
                String resource = "/" + file.getFileName();
                try {
                    Future<Container.ExecResult> running =
                            jobs.submit(() -> container.executeJob(resource, initial));
                    Awaitility.await()
                            .atMost(90, TimeUnit.SECONDS)
                            .until(() -> state(scenario).path("requests").size() == 1);
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
                    Assertions.assertEquals(1, state(scenario).path("requests").size());
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
                            String id = String.format(Locale.ROOT, "rec-%03d", index);
                            String text = String.format(Locale.ROOT, "row-%03d", index);
                            String fields = "{\"text\":\"" + text + "\"}";
                            Assertions.assertEquals(
                                    Arrays.asList(
                                            id,
                                            null,
                                            fields,
                                            "{\"record_id\":\""
                                                    + id
                                                    + "\",\"id\":null,\"fields\":"
                                                    + fields
                                                    + "}"),
                                    rows.get(index));
                        }
                    }
                    Assertions.assertEquals(
                            2,
                            state(scenario).path("requests").size(),
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
                "Feishu { app_id=\""
                        + scenario
                        + "\"\n client_secret=\"fixture-secret\"\n"
                        + " token=\"base-"
                        + scenario
                        + "-credential\"\n table_id=\""
                        + scenario
                        + "\"\n"
                        + " api_base_url=\"http://feishu-source-fixture:8080\"\n allow_insecure_http=true\n"
                        + " page_size=100\n max_pages=9\n page_timeout_ms=30000\n max_retries=0\n}";
        String config =
                "env {\n parallelism=1\n job.mode=BATCH\n job.retry.times=0\n"
                        + " read_limit.rows_per_second=2\n checkpoint.interval=1000\n"
                        + " checkpoint.timeout=30000\n checkpoint.retain-after-job-cancelled=true\n}\n"
                        + "source {\n"
                        + source
                        + "\n}\nsink { Jdbc {\n"
                        + " driver=org.postgresql.Driver\n url=\"jdbc:postgresql://feishu-prefix-postgres:5432/test\"\n"
                        + " username=test\n password=test\n batch_size=1\n max_retries=0\n"
                        + " is_exactly_once=true\n xa_data_source_class_name=org.postgresql.xa.PGXADataSource\n"
                        + " transaction_timeout_sec=3600\n query=\"\"\"INSERT INTO prefix_rows (\"record_id\", \"id\", \"fields\", \"record_json\")"
                        + " VALUES (:record_id, :id, :fields, :record_json)\"\"\"\n} }\n";
        Path file =
                Files.createTempFile(
                        ContainerUtil.getCurrentModulePath().resolve("src/test/resources"),
                        "feishu-prefix-",
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
                                "SELECT \"record_id\", \"id\", \"fields\", \"record_json\" FROM prefix_rows ORDER BY record_id")) {
            while (values.next()) {
                List<String> row = new ArrayList<>();
                for (int column = 1; column <= 4; column++) {
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

    private JsonNode assertFailed(
            TestContainer container,
            String mode,
            String secret,
            int timeout,
            int maxPages,
            String diagnostic,
            int attempts)
            throws Exception {
        String scenario = scenario(mode);
        String secretValue = "badauth".equals(mode) ? secret + "-" + scenario : secret;
        String priorLogs = container.getServerLogs();
        Container.ExecResult result =
                container.executeJob(CONFIG, variables(scenario, secretValue, timeout, maxPages));
        String logs = currentJobLogs(container, priorLogs, result);
        assertFailure(logs, result, diagnostic);
        if ("badauth".equals(mode)) {
            Assertions.assertTrue(logs.contains("Parsed config file:"));
            Assertions.assertTrue(logs.contains(scenario), "This job must be visible in the log");
            Assertions.assertFalse(logs.contains(secretValue), "Client secret leaked");
            Assertions.assertFalse(logs.contains(baseToken(scenario)), "Base token leaked");
        }
        return assertFailedRequestState(scenario, attempts);
    }

    private void assertDeadlineFailure(TestContainer container, String mode) throws Exception {
        String scenario = scenario(mode);
        String jobId = String.valueOf(JobIdGenerator.newJobId());
        String priorLogs = container.getServerLogs();
        String[] variables =
                variables(scenario, "fixture-secret", DEADLINE_MS, 9).toArray(new String[0]);
        CompletableFuture<Container.ExecResult> job =
                async(() -> container.executeJob(CONFIG, jobId, variables));
        try {
            Awaitility.await()
                    .atMost(Duration.ofSeconds(60))
                    .until(() -> state(scenario).path("requests").size() == 1);
            Awaitility.await()
                    .atMost(Duration.ofSeconds(7))
                    .until(() -> state(scenario).has("disconnected_at"));
            Container.ExecResult result = job.get(25, TimeUnit.SECONDS);
            assertFailure(
                    currentJobLogs(container, priorLogs, result), result, "page deadline expired");
            JsonNode state = assertFailedRequestState(scenario, 1);
            double activeSeconds =
                    state.path("disconnected_at").asDouble()
                            - state.path("requests").get(0).path("time").asDouble();
            Assertions.assertTrue(
                    activeSeconds >= 0 && activeSeconds < MAX_ACTIVE_DEADLINE_SECONDS,
                    "Active provider request was not aborted within the page budget plus CI tolerance: "
                            + activeSeconds);
            Assertions.assertFalse(state.path("released").asBoolean());
            if ("slow".equals(mode)) {
                int bytes = state.path("trickle_bytes").intValue();
                Assertions.assertTrue(
                        bytes > 1 && bytes < 100,
                        "Several bytes must arrive but the response must be aborted before EOF");
            }
        } finally {
            control("/release/" + scenario, true);
            if (!job.isDone()) {
                async(() -> container.cancelJob(jobId)).get(20, TimeUnit.SECONDS);
            }
        }
    }

    private static String currentJobLogs(
            TestContainer container, String priorLogs, Container.ExecResult result) {
        String serverLogs = container.getServerLogs();
        Assertions.assertTrue(
                serverLogs.startsWith(priorLogs),
                "Server logs changed before the recorded boundary");
        return result.getStdout() + result.getStderr() + serverLogs.substring(priorLogs.length());
    }

    private static void assertFailure(String logs, Container.ExecResult result, String diagnostic) {
        Assertions.assertNotEquals(0, result.getExitCode());
        Assertions.assertTrue(logs.contains(diagnostic), diagnostic + " missing from job failure");
        Assertions.assertFalse(
                logs.contains("fixture-private-body"), "Provider response text leaked");
    }

    private JsonNode assertFailedRequestState(String scenario, int attempts) throws IOException {
        JsonNode state = state(scenario);
        Assertions.assertEquals(attempts, state.path("requests").size(), scenario);
        Assertions.assertEquals(0, state.path("redirected").intValue());
        Assertions.assertTrue(state.path("rows").isEmpty());
        return state;
    }

    private CompletableFuture<Container.ExecResult> start(
            TestContainer container, String scenario, String jobId) {
        String[] variables =
                variables(scenario, "fixture-secret", 120000, 9).toArray(new String[0]);
        return async(() -> container.executeJob(CONFIG, jobId, variables));
    }

    private static CompletableFuture<Container.ExecResult> async(
            Callable<Container.ExecResult> command) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return command.call();
                    } catch (Exception e) {
                        throw new CompletionException(e);
                    }
                });
    }

    private void awaitHeld(String scenario) {
        Awaitility.await()
                .atMost(Duration.ofSeconds(60))
                .until(() -> state(scenario).path("held").asBoolean());
    }

    private static void awaitStatus(
            TestContainer container, String jobId, String status, int seconds) {
        Awaitility.await()
                .atMost(Duration.ofSeconds(seconds))
                .untilAsserted(
                        () -> Assertions.assertEquals(status, container.getJobStatus(jobId)));
    }

    private String scenario(String mode) {
        return mode + "_" + sequence.incrementAndGet();
    }

    private static List<String> variables(
            String scenario, String secret, int timeout, int maxPages) {
        return Arrays.asList(
                "scenario=" + scenario,
                "secret=" + secret,
                "baseToken=" + baseToken(scenario),
                "pageTimeout=" + timeout,
                "maxPages=" + maxPages);
    }

    private static String baseToken(String scenario) {
        return "base-" + scenario + "-credential";
    }

    private JsonNode state(String scenario) throws IOException {
        return control("/state/" + scenario, false);
    }

    private JsonNode control(String path, boolean post) throws IOException {
        HttpURLConnection connection =
                (HttpURLConnection)
                        new URL("http", provider.getHost(), provider.getMappedPort(PORT), path)
                                .openConnection();
        connection.setConnectTimeout(5000);
        connection.setReadTimeout(5000);
        connection.setRequestMethod(post ? "POST" : "GET");
        try (InputStream response = connection.getInputStream()) {
            return JsonUtils.readTree(response);
        } finally {
            connection.disconnect();
        }
    }

    private static List<String> cursors(JsonNode state) {
        List<String> cursors = new ArrayList<>();
        state.path("requests").forEach(request -> cursors.add(request.path("cursor").textValue()));
        return cursors;
    }

    private static void assertSuccess(Container.ExecResult result) {
        Assertions.assertEquals(0, result.getExitCode(), result.getStdout() + result.getStderr());
    }

    private static void assertBothRows(JsonNode rows) {
        Assertions.assertEquals(2, rows.size());
        assertAlpha(rows.get(0));
        assertBeta(rows.get(1));
    }

    private static void assertAlpha(JsonNode row) {
        Assertions.assertEquals(4, row.size());
        Assertions.assertEquals("rec-alpha", row.path("record_id").textValue());
        Assertions.assertEquals("legacy-001", row.path("id").textValue());
        Assertions.assertEquals(FIELDS, row.path("fields").textValue());
        Assertions.assertEquals(ALPHA, row.path("record_json").textValue());
    }

    private static void assertBeta(JsonNode row) {
        Assertions.assertEquals(4, row.size());
        Assertions.assertEquals("rec-beta", row.path("record_id").textValue());
        Assertions.assertTrue(row.path("id").isNull());
        Assertions.assertTrue(row.path("fields").isNull());
        Assertions.assertEquals(BETA, row.path("record_json").textValue());
    }
}
