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

package org.apache.seatunnel.e2e.connector.activecampaign;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.seatunnel.SeaTunnelContainer;
import org.apache.seatunnel.e2e.common.util.DependencyJar;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exercises the installed plugin through Zeta with an independent HTTPS fixture and JDBC sink. */
class ActiveCampaignSourceIT extends TestSuiteBase {
    private static final String HOST = "tenant.activecampaign.test";
    private static final String ORIGIN = "https://" + HOST + ":8443";
    private static final String CONFIG = "/activecampaign_to_postgresql.conf";
    private static final String TERMINAL = "/activecampaign_terminal_restore.conf";
    private static final String PASSWORD = "fixture-store";
    private static final String BIG_ID = "1234567890123456789012345678901234567890";
    private static final String PRIVATE = "sensitive-provider-diagnostic";
    private static final List<String> RESOURCES =
            Arrays.asList("campaigns", "contacts", "lists", "deals", "segments", "forms");
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final AtomicLong IDS = new AtomicLong(System.currentTimeMillis());

    @TempDir private static Path temp;

    private final String token = UUID.randomUUID().toString();
    private GenericContainer<?> provider;
    private PostgreSQLContainer<?> postgres;
    private FixtureEngine engine;

    @BeforeAll
    void setup() throws Exception {
        tls();
        Path debugConfig = temp.resolve("logging.properties");
        Files.writeString(
                debugConfig,
                "appender.output.type=Console\nappender.output.name=STDOUT\n"
                        + "appender.output.layout.type=PatternLayout\nappender.output.layout.pattern=%p %c - %m%n\n"
                        + "rootLogger.level=info\nrootLogger.appenderRef.output.ref=STDOUT\n"
                        + "logger.http.name=org.apache.http\nlogger.http.level=debug\n");
        provider =
                new GenericContainer<>(DockerImageName.parse("python:3.11-alpine"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(
                                HOST, "wrong.activecampaign.test", "foreign.activecampaign.test")
                        .withExposedPorts(8080)
                        .withEnv("FIXTURE_TOKEN", token)
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("activecampaign_fixture.py"),
                                "/fixture.py")
                        .withCopyFileToContainer(
                                MountableFile.forHostPath(temp.resolve("cert.pem")),
                                "/fixture-cert.pem")
                        .withCopyFileToContainer(
                                MountableFile.forHostPath(temp.resolve("key.pem")),
                                "/fixture-key.pem")
                        .withCommand("python", "/fixture.py")
                        .waitingFor(Wait.forHttp("/ready").forPort(8080));
        provider.start();
        postgres =
                new PostgreSQLContainer<>(DockerImageName.parse("postgres:14-alpine"))
                        .withDatabaseName("test")
                        .withUsername("test")
                        .withPassword("test")
                        .withNetwork(NETWORK)
                        .withNetworkAliases("activecampaign-postgres");
        postgres.start();
        try (Connection sql = sql();
                Statement statement = sql.createStatement()) {
            statement.execute("CREATE TABLE account_rows (resource TEXT, id TEXT, record TEXT)");
        }
        Path jdbc = DependencyJar.of(Driver.class).path();
        engine =
                new FixtureEngine() {
                    @Override
                    protected String getJavaToolOptions() {
                        return "-Djavax.net.ssl.trustStore=/tmp/activecampaign-trust.jks "
                                + "-Djavax.net.ssl.trustStorePassword="
                                + PASSWORD
                                + " -Dlog4j2.configurationFile=/tmp/activecampaign-log4j2.properties";
                    }

                    @Override
                    protected void executeExtraCommands(GenericContainer<?> runtime) {
                        runtime.withCopyFileToContainer(
                                MountableFile.forHostPath(temp.resolve("trust.jks")),
                                "/tmp/activecampaign-trust.jks");
                        runtime.withCopyFileToContainer(
                                MountableFile.forHostPath(debugConfig),
                                "/tmp/activecampaign-log4j2.properties");
                        runtime.withCopyFileToContainer(
                                MountableFile.forHostPath(jdbc),
                                "/tmp/seatunnel/plugins/Jdbc/lib/postgresql.jar");
                    }
                };
        engine.startUp();
    }

    private static void tls() throws Exception {
        Path file = temp.resolve("server.p12");
        Process keytool =
                new ProcessBuilder(
                                Paths.get(System.getProperty("java.home"), "bin", "keytool")
                                        .toString(),
                                "-genkeypair",
                                "-noprompt",
                                "-alias",
                                "fixture",
                                "-keyalg",
                                "RSA",
                                "-keysize",
                                "2048",
                                "-validity",
                                "2",
                                "-dname",
                                "CN=" + HOST,
                                "-ext",
                                "SAN=dns:" + HOST + ",dns:foreign.activecampaign.test",
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                file.toString(),
                                "-storepass",
                                PASSWORD)
                        .redirectErrorStream(true)
                        .redirectOutput(temp.resolve("keytool.log").toFile())
                        .start();
        if (!keytool.waitFor(30, TimeUnit.SECONDS)) {
            keytool.destroyForcibly();
            throw new IllegalStateException("Fixture certificate generation timed out");
        }
        assertEquals(0, keytool.exitValue());
        KeyStore privateStore = KeyStore.getInstance("PKCS12");
        try (InputStream fileStream = Files.newInputStream(file)) {
            privateStore.load(fileStream, PASSWORD.toCharArray());
        }
        Certificate cert = privateStore.getCertificate("fixture");
        pem("CERTIFICATE", cert.getEncoded(), temp.resolve("cert.pem"));
        pem(
                "PRIVATE KEY",
                privateStore.getKey("fixture", PASSWORD.toCharArray()).getEncoded(),
                temp.resolve("key.pem"));
        KeyStore trust = KeyStore.getInstance("JKS");
        trust.load(null, null);
        trust.setCertificateEntry("fixture", cert);
        try (OutputStream out = Files.newOutputStream(temp.resolve("trust.jks"))) {
            trust.store(out, PASSWORD.toCharArray());
        }
    }

    private static void pem(String type, byte[] bytes, Path file) throws Exception {
        String base64 = Base64.getMimeEncoder(64, new byte[] {'\n'}).encodeToString(bytes);
        Files.writeString(
                file, "-----BEGIN " + type + "-----\n" + base64 + "\n-----END " + type + "-----\n");
    }

    @AfterAll
    void teardown() throws Exception {
        try {
            if (engine != null) {
                engine.tearDown();
            }
        } finally {
            try {
                if (provider != null) {
                    provider.stop();
                }
            } finally {
                if (postgres != null) {
                    postgres.stop();
                }
            }
        }
    }

    @Test
    void scansSixEndpointsAndPreservesCompleteRecordsAndNumericPrecision() throws Exception {
        select("normal");
        int baseline = engine.getServerLogs().length();
        assertSuccess(run(CONFIG, arguments(ORIGIN, token, 3, 20, 4, 10000, 65536, 100000)));
        List<String> expectedRows = new ArrayList<>();
        List<String> expectedRequests = new ArrayList<>();
        for (String resource : RESOURCES) {
            expectedRequests.add(resource + ":0");
            expectedRequests.add(resource + ":3");
            if ("contacts".equals(resource)) {
                expectedRows.add(row(resource, 0));
                expectedRows.add(row(resource, 6));
                expectedRequests.add(resource + ":6");
            } else {
                for (int i = 0; i < 4; i++) {
                    expectedRows.add(row(resource, i));
                }
            }
        }
        assertRows(expectedRows);
        assertEquals(expectedRequests, routes());
        JsonNode allRequests = requests();
        for (int i = 0; i < allRequests.size(); i++) {
            JsonNode actual = allRequests.get(i);
            assertEquals(HOST + ":8443", actual.path("host").asText());
            assertTrue(actual.path("authorized").asBoolean());
            assertTrue(actual.path("gzip").asBoolean());
            assertEquals("3", actual.path("query").path("limit").get(0).asText());
            if (i > 0) {
                assertTrue(
                        elapsed(allRequests, i - 1, i) >= 200, "Account requests were not paced");
            }
        }
        assertPrivate(engine.getServerLogs().substring(baseline));
    }

    @Test
    void rejectsInconsistentMetadataOversizedPagesAndNoProgress() throws Exception {
        assertFailure("bad-meta", "invalid meta.total", 1, 20, 4000, 65536);
        assertFailure("contradiction", "meta.total is inconsistent", 1, 20, 4000, 65536);
        assertFailure("oversized-page", "missing or oversized resource array", 1, 20, 4000, 65536);
        assertFailure(
                "empty-loop", "too many consecutive empty continuation pages", 11, 25, 4000, 65536);
        assertFailure("empty-loop", "max_pages exceeded", 2, 2, 4000, 65536);
        assertFailure("cycle", "pagination repeated a previous page", 2, 20, 4000, 65536);
        assertEquals(Arrays.asList("campaigns:0", "campaigns:3"), routes());
    }

    @Test
    void rejectsNonRetryableErrorsRedirectsAndInvalidBodiesWithoutDisclosure() throws Exception {
        assertFailure("unauthorized", "HTTP 401", 1, 20, 4000, 65536);
        assertFailure("payment", "HTTP 402", 1, 20, 4000, 65536);
        assertFailure("missing", "HTTP 404", 1, 20, 4000, 65536);
        assertFailure("invalid", "HTTP 422", 1, 20, 4000, 65536);
        assertFailure("redirect", "HTTP 302", 1, 20, 4000, 65536);
        assertEquals(Collections.singletonList("campaigns:0"), routes());
        assertFailure("oversized", "response exceeds max_response_bytes", 1, 20, 4000, 2048);
        assertFailure("bad-gzip", "invalid gzip response", 1, 20, 4000, 65536);
        assertFailure("bad-json", "malformed or unsupported JSON response", 1, 20, 4000, 65536);
        assertFailure("provider-errors", "invalid collection response", 1, 20, 4000, 65536);
        assertFailure("wrong-root", "missing or oversized resource array", 1, 20, 4000, 65536);

        select("normal");
        int baseline = engine.getServerLogs().length();
        String incorrectToken = UUID.randomUUID().toString();
        checkFailure(
                run(CONFIG, arguments(ORIGIN, incorrectToken, 3, 20, 4, 4000, 65536, 100000)),
                baseline,
                "HTTP 403");
        assertFalse(engine.getServerLogs().substring(baseline).contains(incorrectToken));
        assertEquals(1, requests().size());
        assertFalse(requests().get(0).path("authorized").asBoolean());

        select("normal");
        baseline = engine.getServerLogs().length();
        checkFailure(
                run(
                        CONFIG,
                        arguments(
                                "https://wrong.activecampaign.test:8443",
                                token,
                                3,
                                20,
                                4,
                                4000,
                                65536,
                                100000)),
                baseline,
                "TLS peer verification");
        assertEquals(0, requests().size(), "HTTP request sent after failed hostname verification");
    }

    @Test
    void honorsRetryAfterAndKeepsAttemptsAndDelayBounded() throws Exception {
        select("retry");
        int baseline = engine.getServerLogs().length();
        assertSuccess(run(CONFIG, arguments(ORIGIN, token, 3, 20, 4, 7000, 65536, 100000)));
        assertEquals(8, requests().size());
        assertEquals(
                Arrays.asList("campaigns:0", "campaigns:0", "campaigns:0"), routes().subList(0, 3));
        assertTrue(elapsed(requests(), 0, 1) >= 950, "Retry-After seconds floor was ignored");
        assertRows(
                RESOURCES.stream().map(resource -> row(resource, 0)).collect(Collectors.toList()));
        assertPrivate(engine.getServerLogs().substring(baseline));
        assertFailure(
                "long-retry",
                "request delay exceeds the remaining page deadline",
                1,
                20,
                3000,
                65536);
        assertFailure("busy", "HTTP 503 after 3 attempts", 3, 20, 8000, 65536);
    }

    @Test
    void abortsStalledTricklingAndCanceledResponses() throws Exception {
        assertFailure("stall", "page deadline exceeded", 1, 20, 2500, 65536);
        assertClosedQuickly(0, 10000);
        assertFailure("trickle", "page deadline exceeded", 1, 20, 2500, 65536);
        assertClosedQuickly(0, 10000);

        select("cancel");
        String job = nextJobId();
        CompletableFuture<Container.ExecResult> active =
                submit(CONFIG, job, arguments(ORIGIN, token, 3, 20, 4, 60000, 65536, 100000));
        try {
            waitHeld(0);
            long before = engine.getCompletedCheckpointCount(job);
            await().atMost(30, TimeUnit.SECONDS)
                    .until(() -> engine.getCompletedCheckpointCount(job) > before);
            assertSuccess(engine.cancelJob(job));
            await().atMost(30, TimeUnit.SECONDS)
                    .until(() -> "CANCELED".equals(engine.getJobStatus(job)));
            active.get(45, TimeUnit.SECONDS);
            assertClosedQuickly(0, 45000);
            assertTrue(rows().isEmpty());
        } finally {
            releaseAndCancel(job);
        }
    }

    @Test
    void restoresBeforeFirstPageFromANewerPeriodicCheckpoint() throws Exception {
        select("early");
        String job = nextJobId();
        String restored = nextJobId();
        List<String> args = arguments(ORIGIN, token, 3, 20, 4, 60000, 65536, 100000);
        CompletableFuture<Container.ExecResult> active = submit(CONFIG, job, args);
        try {
            waitHeld(0);
            long before = engine.getCompletedCheckpointCount(job);
            await().atMost(30, TimeUnit.SECONDS)
                    .until(() -> engine.getCompletedCheckpointCount(job) > before);
            engine.stopJob(job);
            await().atMost(30, TimeUnit.SECONDS)
                    .until(() -> "CANCELED".equals(engine.getJobStatus(job)));
            active.get(45, TimeUnit.SECONDS);
            assertTrue(rows().isEmpty());
            assertClosedQuickly(0, 45000);
            control("/release");
            assertSuccess(engine.restoreJobWithCheckpoint(CONFIG, job, restored, args));
            assertEquals(Arrays.asList("campaigns:0", "campaigns:0"), routes().subList(0, 2));
            assertRows(Collections.singletonList(row("campaigns", 0)));
        } finally {
            releaseAndCancel(job, restored);
        }
    }

    @Test
    void resumesTheCommittedOffsetAfterSavepointWhileTheNextPageIsHeld() throws Exception {
        select("boundary");
        String job = nextJobId();
        List<String> args = arguments(ORIGIN, token, 3, 20, 4, 60000, 65536, 100000);
        CompletableFuture<Container.ExecResult> active = submit(CONFIG, job, args);
        try {
            waitHeld(1);
            await().atMost(25, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> assertRows(Collections.singletonList(row("campaigns", 0))));
            long before = engine.getCompletedCheckpointCount(job);
            await().atMost(30, TimeUnit.SECONDS)
                    .until(() -> engine.getCompletedCheckpointCount(job) > before);
            assertSuccess(engine.savepointJob(job));
            assertSuccess(active.get(60, TimeUnit.SECONDS));
            assertClosedQuickly(1, 45000);
            assertFalse(requests().get(1).path("released").asBoolean());
            control("/release");
            assertSuccess(engine.restoreJob(CONFIG, job, args.toArray(new String[0])));
            assertEquals(
                    Arrays.asList("campaigns:0", "campaigns:3", "campaigns:3"),
                    routes().subList(0, 3));
            assertRows(Arrays.asList(row("campaigns", 0), row("campaigns", 3)));
        } finally {
            releaseAndCancel(job);
        }
    }

    @Test
    void savesAndRestoresInsideAFetchedPageWithoutDuplicatingRows() throws Exception {
        select("midpage");
        String job = nextJobId();
        List<String> slow = arguments(ORIGIN, token, 100, 2, 4, 60000, 65536, 4);
        List<String> fast = arguments(ORIGIN, token, 100, 2, 4, 60000, 65536, 100000);
        CompletableFuture<Container.ExecResult> active = submit(CONFIG, job, slow);
        try {
            await().atMost(40, TimeUnit.SECONDS).until(() -> rows().size() >= 4);
            assertSuccess(engine.savepointJob(job));
            assertSuccess(active.get(60, TimeUnit.SECONDS));
            int committed = rows().size();
            assertTrue(
                    committed > 0 && committed < 100, "The checkpoint did not cut inside the page");
            assertEquals(Collections.singletonList("campaigns:0"), routes());
            assertSuccess(engine.restoreJob(CONFIG, job, fast.toArray(new String[0])));
            assertEquals(Arrays.asList("campaigns:0", "campaigns:0"), routes().subList(0, 2));
            assertRows(
                    IntStream.range(0, 100)
                            .mapToObj(index -> row("campaigns", index))
                            .collect(Collectors.toList()));
        } finally {
            releaseAndCancel(job);
        }
    }

    @Test
    void keepsCompletedCollectionTerminalWhenAnotherPipelineSaves() throws Exception {
        select("terminal");
        String job = nextJobId();
        List<String> slow = arguments(ORIGIN, token, 3, 2, 4, 60000, 65536, 20);
        List<String> fast = arguments(ORIGIN, token, 3, 2, 4, 60000, 65536, 100000);
        CompletableFuture<Container.ExecResult> active = submit(TERMINAL, job, slow);
        try {
            await().atMost(45, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                assertRows(Collections.singletonList(row("contacts", 0)));
                                assertEquals("RUNNING", engine.getJobStatus(job));
                            });
            await().atMost(25, TimeUnit.SECONDS)
                    .until(() -> engine.hasFinalPipelineCheckpoint(job));
            assertSuccess(engine.savepointJob(job));
            assertSuccess(active.get(60, TimeUnit.SECONDS));
            assertEquals(Collections.singletonList("contacts:0"), routes());
            assertSuccess(engine.restoreJob(TERMINAL, job, fast.toArray(new String[0])));
            assertEquals(Collections.singletonList("contacts:0"), routes());
            assertRows(Collections.singletonList(row("contacts", 0)));
        } finally {
            releaseAndCancel(job);
        }
    }

    private void assertFailure(
            String scenario, String message, int calls, int pages, int deadline, int byteLimit)
            throws Exception {
        select(scenario);
        int baseline = engine.getServerLogs().length();
        checkFailure(
                run(CONFIG, arguments(ORIGIN, token, 3, pages, 3, deadline, byteLimit, 100000)),
                baseline,
                message);
        assertEquals(calls, requests().size());
    }

    private void checkFailure(Container.ExecResult result, int baseline, String message) {
        assertNotEquals(0, result.getExitCode());
        String logs =
                engine.getServerLogs().substring(baseline)
                        + result.getStdout()
                        + result.getStderr();
        assertTrue(logs.contains(message), "Expected sanitized error " + message);
        assertPrivate(logs);
    }

    private void assertPrivate(String logs) {
        assertFalse(
                logs.contains(token) || logs.contains(PRIVATE),
                "Private request or response appeared in logs");
    }

    private static long elapsed(JsonNode events, int first, int next) {
        return events.get(next).path("started_ms").asLong()
                - events.get(first).path("started_ms").asLong();
    }

    private void waitHeld(int index) {
        await().atMost(45, TimeUnit.SECONDS)
                .until(() -> requests().path(index).path("held").asBoolean());
    }

    private void assertClosedQuickly(int index, int milliseconds) {
        await().atMost(15, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            long closed = requests().path(index).path("closed_ms").asLong(-1);
                            assertTrue(
                                    closed >= 0 && closed <= milliseconds,
                                    "Provider did not observe the transport close");
                        });
    }

    private List<String> routes() throws Exception {
        List<String> found = new ArrayList<>();
        for (JsonNode event : requests()) {
            found.add(
                    event.path("path").asText().substring("/api/3/".length())
                            + ":"
                            + event.path("query").path("offset").path(0).asText());
        }
        return found;
    }

    private static List<String> arguments(
            String origin,
            String token,
            int pageSize,
            int pages,
            int attempts,
            int deadline,
            int byteLimit,
            int rowRate) {
        return Arrays.asList(
                "fixture_origin=" + origin,
                "fixture_token=" + token,
                "fixture_page_size=" + pageSize,
                "fixture_max_pages=" + pages,
                "fixture_attempts=" + attempts,
                "fixture_deadline=" + deadline,
                "fixture_byte_limit=" + byteLimit,
                "fixture_row_rate=" + rowRate);
    }

    private static String nextJobId() {
        return Long.toString(IDS.incrementAndGet());
    }

    private CompletableFuture<Container.ExecResult> submit(
            String config, String job, List<String> args) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return engine.executeJob(config, job, args.toArray(new String[0]));
                    } catch (Exception error) {
                        throw new IllegalStateException("Fixture job invocation failed", error);
                    }
                });
    }

    private Container.ExecResult run(String config, List<String> args) throws Exception {
        return engine.executeJob(config, nextJobId(), args.toArray(new String[0]));
    }

    private static void assertSuccess(Container.ExecResult result) {
        assertEquals(0, result.getExitCode());
    }

    private void releaseAndCancel(String... jobs) throws Exception {
        control("/release");
        for (String job : jobs) {
            String status = engine.getJobStatus(job);
            if ("RUNNING".equals(status)
                    || "PENDING".equals(status)
                    || "RESTARTING".equals(status)) {
                engine.cancelJob(job);
            }
        }
    }

    private void select(String scenario) throws Exception {
        control("/select?mode=" + scenario);
        try (Connection sql = sql();
                Statement query = sql.createStatement()) {
            query.execute("TRUNCATE TABLE account_rows");
        }
    }

    private JsonNode requests() throws Exception {
        return control("/requests");
    }

    private JsonNode control(String path) throws Exception {
        return get(new URL("http", provider.getHost(), provider.getMappedPort(8080), path));
    }

    private static JsonNode get(URL url) throws Exception {
        HttpURLConnection request = (HttpURLConnection) url.openConnection();
        request.setConnectTimeout(5000);
        request.setReadTimeout(5000);
        try (InputStream body = request.getInputStream()) {
            return JSON.readTree(body);
        } finally {
            request.disconnect();
        }
    }

    private static class FixtureEngine extends SeaTunnelContainer {
        boolean hasFinalPipelineCheckpoint(String job) throws Exception {
            URL url =
                    new URL(
                            "http",
                            server.getHost(),
                            server.getMappedPort(8080),
                            "/jobs/checkpoints/" + job);
            for (JsonNode pipeline : get(url).path("pipelines")) {
                if ("completed-point"
                        .equals(pipeline.path("latestCompleted").path("checkpointType").asText())) {
                    return true;
                }
            }
            return false;
        }
    }

    private Connection sql() throws Exception {
        return DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }

    private List<String> rows() throws Exception {
        List<String> found = new ArrayList<>();
        try (Connection sql = sql();
                Statement query = sql.createStatement();
                ResultSet result =
                        query.executeQuery("SELECT resource, id, record FROM account_rows")) {
            while (result.next()) {
                found.add(
                        result.getString(1)
                                + "|"
                                + result.getString(2)
                                + "|"
                                + result.getString(3));
            }
        }
        Collections.sort(found);
        return found;
    }

    private static String row(String resource, int offset) {
        boolean numeric = "campaigns".equals(resource) && offset == 0;
        String id = numeric ? BIG_ID : resource + "-" + offset;
        return resource
                + "|"
                + id
                + "|{\"id\":"
                + (numeric ? id : "\"" + id + "\"")
                + ",\"name\":\"café 雪\",\"nested\":{\"empty\":null,"
                + "\"values\":[true,127,1234567890123456789.123456789012345678900]}}";
    }

    private void assertRows(List<String> expected) throws Exception {
        List<String> sorted = new ArrayList<>(expected);
        Collections.sort(sorted);
        assertEquals(sorted, rows());
    }
}
