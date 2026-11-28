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

package org.apache.seatunnel.e2e.connector.datadog;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.seatunnel.SeaTunnelContainer;
import org.apache.seatunnel.e2e.common.util.DependencyJar;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.postgresql.Driver;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the packaged source, Zeta and a JDBC sink against a verified TLS provider fixture.
 * Exact rows and requests cover pagination, bounded failures and checkpoint recovery.
 */
class DatadogSourceIT extends TestSuiteBase {
    private static final String CONFIG = "/datadog_to_postgresql.conf";
    private static final String START = "2026-01-01T00:00:00.000001Z";
    private static final String BOUNDARY = "2026-01-02T00:00:00.000001Z";
    private static final String END = "2026-01-03T00:00:00.000001Z";
    private static final String QUERY = "service:café +/% = &雪";
    private static final String PASSWORD = "fixture-store";
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final AtomicLong JOB_ID = new AtomicLong(System.currentTimeMillis());

    @TempDir private static Path temp;

    private final String access = UUID.randomUUID().toString();
    private final String secret = UUID.randomUUID().toString();
    private GenericContainer<?> fixture;
    private PostgreSQLContainer<?> postgres;
    private SeaTunnelContainer engine;

    @BeforeAll
    void startEngineAndFixtures() throws Exception {
        createTlsMaterial();
        fixture =
                new GenericContainer<>(DockerImageName.parse("python:3.11-alpine"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(
                                "api.datadoghq.com",
                                "api.ap2.datadoghq.com",
                                "api.ap1.datadoghq.com",
                                "foreign-fixture")
                        .withExposedPorts(8080)
                        .withEnv("FIXTURE_ACCESS", access)
                        .withEnv("FIXTURE_SECRET", secret)
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("datadog_fixture.py"),
                                "/fixture.py")
                        .withCopyFileToContainer(
                                MountableFile.forHostPath(temp.resolve("cert.pem")),
                                "/fixture-cert.pem")
                        .withCopyFileToContainer(
                                MountableFile.forHostPath(temp.resolve("key.pem")),
                                "/fixture-key.pem")
                        .withCommand("python", "/fixture.py")
                        .waitingFor(Wait.forHttp("/ready").forPort(8080).forStatusCode(200));
        fixture.start();

        postgres =
                new PostgreSQLContainer<>(DockerImageName.parse("postgres:14-alpine"))
                        .withDatabaseName("test")
                        .withUsername("test")
                        .withPassword("test")
                        .withNetwork(NETWORK)
                        .withNetworkAliases("datadog-postgres");
        postgres.start();
        try (Connection connection = connection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "CREATE TABLE datadog_result (resource TEXT, id TEXT, \"timestamp\" TEXT, record TEXT)");
        }

        Path driver = DependencyJar.of(Driver.class).path();
        engine =
                new SeaTunnelContainer() {
                    @Override
                    protected String getJavaToolOptions() {
                        return "-Djavax.net.ssl.trustStore=/tmp/datadog-trust.jks -Djavax.net.ssl.trustStorePassword="
                                + PASSWORD;
                    }

                    @Override
                    protected void executeExtraCommands(GenericContainer<?> runtime) {
                        runtime.withCopyFileToContainer(
                                MountableFile.forHostPath(temp.resolve("trust.jks")),
                                "/tmp/datadog-trust.jks");
                        runtime.withCopyFileToContainer(
                                MountableFile.forHostPath(driver),
                                "/tmp/seatunnel/plugins/Jdbc/lib/postgresql.jar");
                    }
                };
        engine.startUp();
    }

    /**
     * A fresh trust store for the isolated engine, with ordinary hostname verification. A second
     * fixture alias deliberately lacks a certificate SAN to test peer rejection.
     */
    private void createTlsMaterial() throws Exception {
        Path server = temp.resolve("server.p12");
        String tool = Paths.get(System.getProperty("java.home"), "bin", "keytool").toString();
        Process keytool =
                new ProcessBuilder(
                                tool,
                                "-genkeypair",
                                "-noprompt",
                                "-alias",
                                "dd",
                                "-keyalg",
                                "RSA",
                                "-keysize",
                                "2048",
                                "-validity",
                                "2",
                                "-dname",
                                "CN=api.datadoghq.com",
                                "-ext",
                                "SAN=dns:api.datadoghq.com,dns:api.ap2.datadoghq.com",
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                server.toString(),
                                "-storepass",
                                PASSWORD)
                        .redirectErrorStream(true)
                        .redirectOutput(temp.resolve("keytool.log").toFile())
                        .start();
        if (!keytool.waitFor(30, TimeUnit.SECONDS)) {
            keytool.destroyForcibly();
            throw new IllegalStateException("Fixture certificate generation timed out");
        }
        assertEquals(0, keytool.exitValue(), "Fixture certificate generation");
        KeyStore privateStore = KeyStore.getInstance("PKCS12");
        try (InputStream stream = Files.newInputStream(server)) {
            privateStore.load(stream, PASSWORD.toCharArray());
        }
        Certificate certificate = privateStore.getCertificate("dd");
        writePem("CERTIFICATE", certificate.getEncoded(), temp.resolve("cert.pem"));
        writePem(
                "PRIVATE KEY",
                privateStore.getKey("dd", PASSWORD.toCharArray()).getEncoded(),
                temp.resolve("key.pem"));

        KeyStore trust = KeyStore.getInstance("JKS");
        trust.load(null, null);
        trust.setCertificateEntry("dd", certificate);
        try (OutputStream out = Files.newOutputStream(temp.resolve("trust.jks"))) {
            trust.store(out, PASSWORD.toCharArray());
        }
    }

    private static void writePem(String type, byte[] value, Path path) throws Exception {
        String encoded = Base64.getMimeEncoder(64, new byte[] {'\n'}).encodeToString(value);
        String pem = "-----BEGIN " + type + "-----\n" + encoded + "\n-----END " + type + "-----\n";
        Files.write(path, pem.getBytes(StandardCharsets.US_ASCII));
    }

    @AfterAll
    void shutdown() throws Exception {
        try {
            if (engine != null) {
                engine.tearDown();
            }
        } finally {
            try {
                if (fixture != null) {
                    fixture.stop();
                }
            } finally {
                if (postgres != null) {
                    postgres.stop();
                }
            }
        }
    }

    @Test
    void readsEveryResourceThroughItsOwnEndpointAndContinuation() throws Exception {
        catalog(
                "dashboards",
                "/api/v1/dashboard",
                "start",
                Arrays.asList("0", "1", "3"),
                "d1",
                "d2",
                "d3");
        run("normal", "teams");
        assertRows(
                "teams",
                row(
                        "t1",
                        null,
                        "{\"id\":\"t1\",\"type\":\"team\",\"attributes\":{\"name\":\"Operations\",\"handle\":\"ops\"}}"),
                row(
                        "t2",
                        null,
                        "{\"id\":\"t2\",\"type\":\"team\",\"attributes\":{\"name\":\"Engineering\",\"handle\":\"eng\"}}"),
                row(
                        "t3",
                        null,
                        "{\"id\":\"t3\",\"type\":\"team\",\"attributes\":{\"name\":\"Support\",\"handle\":\"support\"}}"));
        assertEquals(
                Arrays.asList("0", "1"),
                queryValues(assertRoute("/api/v2/team", "datadoghq.com", 2), "page[number]"));
        catalog(
                "incidents",
                "/api/v2/incidents",
                "page[offset]",
                Arrays.asList("0", "2", "4"),
                "i1",
                "i2");
        catalog(
                "service_level_objectives",
                "/api/v1/slo",
                "offset",
                Arrays.asList("0", "1"),
                "s1",
                "s2",
                "s3");

        run("normal", "downtimes");
        assertRows(
                "downtimes",
                row(
                        "9223372036854775807",
                        null,
                        "{\"id\":9223372036854775807,\"active\":true,\"message\":\"暂停\"}"));
        assertRoute("/api/v1/downtime", "datadoghq.com", 1);

        run("normal", "monitors");
        assertRows(
                "monitors",
                row("9007199254740993", null, "{\"id\":9007199254740993}"),
                row("9007199254740995", null, "{\"id\":9007199254740995}"));
        assertEquals(
                Arrays.asList("0", "9007199254740993", "9007199254740995"),
                queryValues(assertRoute("/api/v1/monitor", "datadoghq.com", 3), "id_offset"));

        run("normal", "synthetic_tests");
        assertRows(
                "synthetic_tests",
                row("p1", null, "{\"public_id\":\"p1\"}"),
                row("p2", null, "{\"public_id\":\"p2\"}"));
        assertEquals(
                Arrays.asList("0", "1", "2"),
                queryValues(
                        assertRoute("/api/v1/synthetics/tests", "datadoghq.com", 3),
                        "page_number"));

        run("normal", "users");
        assertRows(
                "users",
                id("u1"),
                id("u2"),
                row("u3", null, "{\"id\":\"u3\",\"attributes\":{\"disabled\":true}}"));
        assertEquals(
                Arrays.asList("0", "1"),
                queryValues(assertRoute("/api/v2/users", "datadoghq.com", 2), "page[number]"));

        select("normal");
        assertSuccess(
                engine.executeJob(
                        CONFIG, arguments("metrics", "ap2.datadoghq.com", null, null, 30000)));
        assertRows(
                "metrics",
                row("metric.scalar", null, "\"metric.scalar\""),
                row(
                        "metric.exact",
                        null,
                        "{\"id\":\"metric.exact\",\"attributes\":{\"i\":9223372036854775807980,\"d\":0.123456789012345678900,\"n\":null}}"),
                row("metric.tail", null, "\"metric.tail\""));
        JsonNode metricCalls = assertRoute("/api/v2/metrics", "ap2.datadoghq.com", 3);
        assertEquals(
                Arrays.asList(null, "metric+next", "tail+next"),
                queryValues(metricCalls, "page[cursor]"));
        assertEquals(
                Arrays.asList("5184000", "5184000", "5184000"),
                queryValues(metricCalls, "window[seconds]"));
    }

    @Test
    void pagesEventWindowsWithoutLosingSameTimestampOrBoundaryRows() throws Exception {
        for (String resource : Arrays.asList("logs", "audit_logs")) {
            run("normal", resource);
            assertRows(
                    resource,
                    event("first", START),
                    event("same", START),
                    event("previous", "2026-01-01T23:59:59.999999Z"),
                    event("window-boundary", BOUNDARY));
            JsonNode calls =
                    assertRoute(
                            resource.equals("logs")
                                    ? "/api/v2/logs/events"
                                    : "/api/v2/audit/events",
                            "datadoghq.com",
                            4);
            assertEquals(
                    Arrays.asList(START, START, START, BOUNDARY),
                    queryValues(calls, "filter[from]"));
            assertEquals(
                    Arrays.asList(BOUNDARY, BOUNDARY, BOUNDARY, END),
                    queryValues(calls, "filter[to]"));
            assertEquals(
                    Arrays.asList(null, "opaque +/%= &雪", "last", null),
                    queryValues(calls, "page[cursor]"));
            assertEquals(
                    Arrays.asList("timestamp", "timestamp", "timestamp", "timestamp"),
                    queryValues(calls, "sort"));
            assertEquals(
                    resource.equals("logs")
                            ? Collections.nCopies(4, QUERY)
                            : Collections.nCopies(4, null),
                    queryValues(calls, "filter[query]"));
        }
        select("normal");
        assertSuccess(
                engine.executeJob(CONFIG, arguments("logs", "datadoghq.com", START, START, 30000)));
        assertTrue(rows().isEmpty());
        assertEquals(0, requests().size(), "An empty interval cannot produce an API request");
    }

    @Test
    void rejectsIncompleteResponsesAndNeverSendsCredentialsToAnotherOrigin() throws Exception {
        failure("unauthorized", "HTTP 401", 1);
        failure("forbidden", "HTTP 403", 1);
        failure("forbidden-200", "incomplete results", 1);
        failure("partial", "incomplete results", 1);
        failure("malformed", "malformed", 1);
        failure("cycle", "pagination cursor cycle", 3);
        failure("foreign", "pagination origin", 1);
        failure("bad-link", "ambiguous pagination link", 1);
        failure("oversize", "exceeds max_response_bytes", 1);
        select("normal");
        int baseline = engine.getServerLogs().length();
        assertFailure(
                engine.executeJob(
                        CONFIG, arguments("incident_teams", "datadoghq.com", null, null, 30000)),
                baseline,
                "Unsupported Datadog resource");
        assertEquals(0, requests().size(), "A retired resource label cannot open an API request");
        assertTrue(rows().isEmpty());
        baseline = engine.getServerLogs().length();
        Container.ExecResult rejected =
                engine.executeJob(
                        CONFIG, arguments("metrics", "ap1.datadoghq.com", null, null, 30000));
        assertFailure(rejected, baseline, "TLS peer verification");
        assertEquals(
                0, requests().size(), "TLS hostname mismatch must fail before authenticated HTTP");
    }

    @Test
    void boundsRetriesAndHonorsProviderMinimumDelay() throws Exception {
        run("rate", "metrics");
        assertRows("metrics", row("metric.recovered", null, "\"metric.recovered\""));
        JsonNode calls = assertRoute("/api/v2/metrics", "datadoghq.com", 3);
        assertEquals(calls.get(0).path("query"), calls.get(1).path("query"));
        assertEquals(calls.get(1).path("query"), calls.get(2).path("query"));
        assertTrue(elapsed(calls, 0, 1) >= 1.0, "Datadog rate-limit reset was shortened");
        assertTrue(elapsed(calls, 1, 2) >= 0.99, "Retry-After date was shortened");

        run("rate-numeric", "metrics");
        assertRows("metrics", row("metric.recovered", null, "\"metric.recovered\""));
        JsonNode numeric = assertRoute("/api/v2/metrics", "datadoghq.com", 2);
        assertTrue(elapsed(numeric, 0, 1) >= 1.0, "Numeric Retry-After was shortened");

        run("rate-invalid", "metrics");
        assertRows("metrics", row("metric.recovered", null, "\"metric.recovered\""));
        assertRoute("/api/v2/metrics", "datadoghq.com", 2);
        failure("rate-excess", "retry delay exceeds", 1);
        failure("retry-exhaust", "after 3 attempts", 3);
    }

    @Test
    void abortsStalledTricklingAndCanceledRequests() throws Exception {
        for (String scenario : Arrays.asList("held", "trickle")) {
            select(scenario);
            int baseline = engine.getServerLogs().length();
            Container.ExecResult timedOut =
                    engine.executeJob(
                            CONFIG, arguments("metrics", "datadoghq.com", null, null, 1500));
            assertFailure(timedOut, baseline, "page deadline exceeded");
            await().atMost(10, TimeUnit.SECONDS).until(() -> requests().get(0).has("closed_after"));
            JsonNode call = assertRoute("/api/v2/metrics", "datadoghq.com", 1).get(0);
            assertTrue(
                    call.path("closed_after").asDouble() < 4.0, "Fixture saw delayed socket abort");
            assertTrue(rows().isEmpty());
        }

        for (String scenario : Arrays.asList("cancel-io", "cancel-backoff")) {
            select(scenario);
            String jobId = nextJobId();
            List<String> args = arguments("metrics", "datadoghq.com", null, null, 30000);
            CompletableFuture<Container.ExecResult> running = submit(jobId, args);
            try {
                await().atMost(45, TimeUnit.SECONDS)
                        .until(
                                () ->
                                        requests().size() == 1
                                                && (!scenario.equals("cancel-io")
                                                        || requests()
                                                                .get(0)
                                                                .path("held")
                                                                .asBoolean()));
                assertSuccess(engine.cancelJob(jobId));
                assertCanceled(jobId, running);
                if (scenario.equals("cancel-io")) {
                    await().atMost(10, TimeUnit.SECONDS)
                            .until(() -> requests().get(0).has("closed_after"));
                    assertFalse(requests().get(0).path("released").asBoolean());
                } else {
                    // Wait past the provider retry floor, so an unowned retry would reach the
                    // fixture.
                    await().during(13, TimeUnit.SECONDS)
                            .atMost(15, TimeUnit.SECONDS)
                            .until(() -> requests().size() == 1);
                }
                assertEquals(1, requests().size());
                assertTrue(rows().isEmpty());
            } finally {
                control("/release");
                cancelIfRunning(jobId);
            }
        }
    }

    @Test
    void checkpointsAndStopsWithSavepointWhileTheCursorRequestIsStillHeld() throws Exception {
        String start = Instant.now().minus(1, ChronoUnit.HOURS).toString();
        select("restore-cursor");
        String jobId = nextJobId();
        List<String> args = arguments("logs", "datadoghq.com", start, null, 120000);
        CompletableFuture<Container.ExecResult> running = submit(jobId, args);
        try {
            awaitHeldAndCheckpoint(jobId);
            assertSuccess(engine.savepointJob(jobId));
            assertSuccess(running.get(60, TimeUnit.SECONDS));
            assertFalse(requests().get(1).path("released").asBoolean());
            assertRows("logs", event("before", start));
            String frozenEnd = requests().get(0).path("query").path("filter[to]").asText();
            assertFalse(frozenEnd.isEmpty());
            control("/release");
            assertSuccess(engine.restoreJob(CONFIG, jobId, args.toArray(new String[0])));
            assertRows("logs", event("before", start), event("after", start));
            JsonNode calls = assertRoute("/api/v2/logs/events", "datadoghq.com", 3);
            assertEquals(Arrays.asList(null, "held", "held"), queryValues(calls, "page[cursor]"));
            assertEquals(Collections.nCopies(3, frozenEnd), queryValues(calls, "filter[to]"));
        } finally {
            control("/release");
            cancelIfRunning(jobId);
        }
    }

    @Test
    void restoresOffsetFromTheCompletedPeriodicCheckpoint() throws Exception {
        select("restore-offset");
        String jobId = nextJobId();
        String restoredId = nextJobId();
        List<String> args = arguments("dashboards", "datadoghq.com", null, null, 120000);
        CompletableFuture<Container.ExecResult> running = submit(jobId, args);
        try {
            awaitHeldAndCheckpoint(jobId);
            engine.stopJob(jobId);
            assertCanceled(jobId, running);
            assertFalse(requests().get(1).path("released").asBoolean());
            assertRows("dashboards", id("before"));
            control("/release");
            assertSuccess(engine.restoreJobWithCheckpoint(CONFIG, jobId, restoredId, args));
            assertRows("dashboards", id("before"), id("after"));
            assertEquals(
                    Arrays.asList("0", "1", "1", "2"),
                    queryValues(assertRoute("/api/v1/dashboard", "datadoghq.com", 4), "start"));
        } finally {
            control("/release");
            cancelIfRunning(jobId);
            cancelIfRunning(restoredId);
        }
    }

    /**
     * Checkpoints must finish while one decoded page is throttled. A completed stop-with-savepoint
     * then restores its accepted prefix without duplicating rows in the unkeyed JDBC result table.
     */
    @Test
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    void checkpointsThrottledDecodedRowsAndRestoresTheSavepointTail() throws Exception {
        String config = "/datadog_throttled_to_postgresql.conf";
        select("throttled-downtimes");
        String job = nextJobId();
        List<String> args = arguments("downtimes", "datadoghq.com", null, null, 120000);
        List<List<String>> expected = new ArrayList<>();
        for (int i = 1; i <= 64; i++) {
            expected.add(row(Integer.toString(i), null, "{\"id\":" + i + ",\"active\":true}"));
        }
        CompletableFuture<Container.ExecResult> original =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return engine.executeJob(config, job, args.toArray(new String[0]));
                            } catch (Exception e) {
                                throw new IllegalStateException("Fixture job invocation failed", e);
                            }
                        });
        try {
            await().atMost(45, TimeUnit.SECONDS).until(() -> !rows().isEmpty());
            long baseline = engine.getCompletedCheckpointCount(job);
            // Two later completions exclude a checkpoint already injected before the first row.
            await().atMost(15, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                assertTrue(engine.getCompletedCheckpointCount(job) >= baseline + 2);
                                assertTrue(
                                        rows().size() < 64, "The decoded page must remain partial");
                                assertRoute("/api/v1/downtime", "datadoghq.com", 1);
                            });
            assertSuccess(engine.savepointJob(job));
            await().atMost(20, TimeUnit.SECONDS)
                    .until(() -> "SAVEPOINT_DONE".equals(engine.getJobStatus(job)));
            assertSuccess(original.get(40, TimeUnit.SECONDS));
            List<List<String>> prefix = rows();
            assertTrue(!prefix.isEmpty() && prefix.size() < 64);
            List<List<String>> expectedPrefix = new ArrayList<>();
            for (List<String> row : expected.subList(0, prefix.size())) {
                List<String> value = new ArrayList<>();
                value.add("downtimes");
                value.addAll(row);
                expectedPrefix.add(value);
            }
            expectedPrefix.sort(Comparator.comparing(row -> row.get(1)));
            assertEquals(expectedPrefix, prefix);
            assertRoute("/api/v1/downtime", "datadoghq.com", 1);
            assertSuccess(engine.restoreJob(config, job, args.toArray(new String[0])));
            assertRows("downtimes", expected);
            JsonNode calls = assertRoute("/api/v1/downtime", "datadoghq.com", 2);
            assertEquals(calls.get(0).path("query"), calls.get(1).path("query"));
        } finally {
            cancelIfRunning(job);
        }
    }

    /**
     * Sample the counter after the fixture has entered its hold. An old checkpoint cannot pass.
     * Recovery assertions therefore depend on a checkpoint completed while I/O is still blocked.
     */
    private void awaitHeldAndCheckpoint(String jobId) {
        await().atMost(45, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            assertEquals(2, requests().size());
                            assertTrue(requests().get(1).path("held").asBoolean());
                            assertEquals(1, rows().size());
                        });
        long baseline = engine.getCompletedCheckpointCount(jobId);
        await().atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            assertTrue(engine.getCompletedCheckpointCount(jobId) > baseline);
                            assertFalse(requests().get(1).path("released").asBoolean());
                        });
    }

    private void assertCanceled(String jobId, CompletableFuture<Container.ExecResult> running)
            throws Exception {
        await().atMost(30, TimeUnit.SECONDS)
                .until(() -> "CANCELED".equals(engine.getJobStatus(jobId)));
        running.get(45, TimeUnit.SECONDS);
    }

    private void cancelIfRunning(String jobId) throws Exception {
        String state = engine.getJobStatus(jobId);
        if ("RUNNING".equals(state) || "PENDING".equals(state) || "RESTARTING".equals(state)) {
            engine.cancelJob(jobId);
        }
    }

    private static String nextJobId() {
        return Long.toString(JOB_ID.incrementAndGet());
    }

    private CompletableFuture<Container.ExecResult> submit(String jobId, List<String> args) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return engine.executeJob(CONFIG, jobId, args.toArray(new String[0]));
                    } catch (Exception e) {
                        throw new IllegalStateException("Fixture job invocation failed", e);
                    }
                });
    }

    private void catalog(
            String resource, String path, String position, List<String> values, String... ids)
            throws Exception {
        run("normal", resource);
        List<List<String>> expected = new ArrayList<>();
        for (String id : ids) {
            expected.add(id(id));
        }
        assertRows(resource, expected);
        assertEquals(
                values, queryValues(assertRoute(path, "datadoghq.com", values.size()), position));
    }

    private void run(String scenario, String resource) throws Exception {
        select(scenario);
        boolean dated = resource.equals("logs") || resource.equals("audit_logs");
        assertSuccess(
                engine.executeJob(
                        CONFIG,
                        arguments(
                                resource,
                                "datadoghq.com",
                                dated ? START : null,
                                dated ? END : null,
                                30000)));
    }

    private void failure(String scenario, String expected, int attempts) throws Exception {
        select(scenario);
        int baseline = engine.getServerLogs().length();
        Container.ExecResult result =
                engine.executeJob(CONFIG, arguments("metrics", "datadoghq.com", null, null, 30000));
        assertFailure(result, baseline, expected);
        assertRoute("/api/v2/metrics", "datadoghq.com", attempts);
        assertTrue(rows().isEmpty());
    }

    private void assertFailure(Container.ExecResult result, int baseline, String expected) {
        assertNotEquals(0, result.getExitCode(), expected);
        String logs = engine.getServerLogs();
        String tail = logs.substring(Math.min(baseline, logs.length()));
        String relevant = result.getStdout() + result.getStderr() + tail;
        assertFalse(
                relevant.contains(access)
                        || relevant.contains(secret)
                        || relevant.contains("fixture-response-secret"),
                "A safe diagnostic leaked fixture secrets");
        assertTrue(relevant.contains(expected), "Expected Datadog diagnostic: " + expected);
    }

    private static void assertSuccess(Container.ExecResult result) {
        assertEquals(0, result.getExitCode(), "Engine job exit code");
    }

    private void select(String scenario) throws Exception {
        control("/select?scenario=" + scenario);
        try (Connection connection = connection();
                Statement statement = connection.createStatement()) {
            statement.execute("TRUNCATE datadog_result");
        }
    }

    private JsonNode requests() throws Exception {
        return control("/requests");
    }

    private JsonNode assertRoute(String path, String site, int count) throws Exception {
        JsonNode calls = requests();
        assertEquals(count, calls.size());
        for (JsonNode call : calls) {
            assertEquals("GET", call.path("method").asText());
            assertEquals(path, call.path("path").asText());
            assertEquals("api." + site, call.path("host").asText());
            assertTrue(
                    call.path("auth").asBoolean(), "Both read authorization headers are required");
        }
        return calls;
    }

    private static List<String> queryValues(JsonNode calls, String key) {
        List<String> values = new ArrayList<>();
        calls.forEach(call -> values.add(call.path("query").path(key).textValue()));
        return values;
    }

    private static double elapsed(JsonNode calls, int first, int second) {
        return calls.get(second).path("time").asDouble() - calls.get(first).path("time").asDouble();
    }

    private List<String> arguments(
            String resource, String site, String start, String end, int deadline) {
        List<String> args =
                new ArrayList<>(
                        Arrays.asList(
                                "dd_resource=" + resource,
                                "dd_site=" + site,
                                "dd_access=" + access,
                                "dd_secret=" + secret,
                                "dd_deadline=" + deadline));
        args.add("dd_start=" + (start == null ? "" : start));
        args.add("dd_end=" + (end == null ? "" : end));
        args.add("dd_query=" + (resource.equals("logs") ? QUERY : ""));
        // The E2E harness joins arguments into a shell command. Keep each variable one word.
        args.replaceAll(arg -> "'" + arg + "'");
        return args;
    }

    private JsonNode control(String path) throws Exception {
        URL url = new URL("http", fixture.getHost(), fixture.getMappedPort(8080), path);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setConnectTimeout(5000);
        connection.setReadTimeout(5000);
        try (InputStream response = connection.getInputStream()) {
            return JSON.readTree(response);
        } finally {
            connection.disconnect();
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
                                "SELECT resource,id,\"timestamp\",record FROM datadog_result")) {
            while (result.next()) {
                rows.add(
                        Arrays.asList(
                                result.getString(1),
                                result.getString(2),
                                result.getString(3),
                                result.getString(4)));
            }
        }
        rows.sort(Comparator.comparing(row -> row.get(1)));
        return rows;
    }

    @SafeVarargs
    private final void assertRows(String resource, List<String>... rows) throws Exception {
        assertRows(resource, Arrays.asList(rows));
    }

    private void assertRows(String resource, List<List<String>> rows) throws Exception {
        List<List<String>> expected = new ArrayList<>();
        for (List<String> row : rows) {
            List<String> result = new ArrayList<>();
            result.add(resource);
            result.addAll(row);
            expected.add(result);
        }
        expected.sort(Comparator.comparing(row -> row.get(1)));
        assertEquals(expected, rows());
    }

    private static List<String> id(String id) {
        return row(id, null, "{\"id\":\"" + id + "\"}");
    }

    private static List<String> event(String id, String timestamp) {
        return row(
                id,
                timestamp,
                "{\"id\":\""
                        + id
                        + "\",\"attributes\":{\"timestamp\":\""
                        + timestamp
                        + "\",\"value\":{\"empty\":[],\"n\":null}}}");
    }

    private static List<String> row(String id, String timestamp, String json) {
        return Arrays.asList(id, timestamp, json);
    }
}
