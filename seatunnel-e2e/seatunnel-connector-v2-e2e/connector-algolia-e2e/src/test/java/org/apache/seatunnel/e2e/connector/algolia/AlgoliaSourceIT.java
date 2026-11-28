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
package org.apache.seatunnel.e2e.connector.algolia;

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

import java.io.IOException;
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
import java.util.Collections;
import java.util.List;
import java.util.Locale;
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

/**
 * Exercises the packaged Algolia source, exact JDBC outputs, and failure and savepoint boundaries
 * on Zeta.
 */
class AlgoliaSourceIT extends TestSuiteBase {
    private static final String HOST = "tenant.algolia.test";
    private static final String ORIGIN = "https://" + HOST + ":8443";
    private static final String TRUST_PASSWORD = "fixture-trust";
    private static final String PRIVATE = "algolia-fixture-private-query-header";
    private static final String ROOT = "/1/indexes";
    private static final String LOG_ROUTE = "/1/logs";
    private static final String LANG_ROUTE = "/1/dictionaries/*/languages";
    private static final String INDEX = "shops é+";
    private static final String ENCODED_QUERY = ROOT + "/shops%20%C3%A9%2B/query";
    private static final String ENCODED_SETTINGS = ROOT + "/shops%20%C3%A9%2B/settings";
    private static final String START = "2026-11-27T00:00:00Z";
    private static final String END = "2026-11-27T00:05:00Z";
    private static final String MILLIS = "2026-11-27T00:00:00.125Z";
    private static final String QUERY =
            "query=caf%C3%A9+calme&filters=price%3E10&hitsPerPage=2&getRankingInfo=1";
    private static final String CATALOG = "/algolia_catalog.conf";
    private static final String INDICES = "/algolia_indices.conf";
    private static final String LOGS = "/algolia_logs.conf";
    private static final String QUERY_JOB = "/algolia_query.conf";
    private static final String LANGS = "/algolia_languages.conf";
    private static final String TERMINAL = "/algolia_terminal.conf";
    private static final AtomicLong IDS = new AtomicLong(System.currentTimeMillis());
    private static final ObjectMapper JSON = new ObjectMapper();

    @TempDir private static Path temporary;

    private final String applicationId = "APP123";
    private final String token = UUID.randomUUID().toString();
    private GenericContainer<?> fixture;
    private PostgreSQLContainer<?> postgres;
    private Engine engine;

    @BeforeAll
    void startFixtures() throws Exception {
        generateCertificate();
        Path loggers = temporary.resolve("http-debug.properties");
        Files.writeString(
                loggers,
                "appender.console.type=Console\nappender.console.name=OUT\n"
                        + "appender.console.layout.type=PatternLayout\nappender.console.layout.pattern=%p %c %m%n\n"
                        + "rootLogger.level=info\nrootLogger.appenderRef.out.ref=OUT\n"
                        + "logger.http.name=org.apache.http\nlogger.http.level=debug\n");
        fixture =
                new GenericContainer<>(DockerImageName.parse("python:3.11-alpine"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST, "wrong.algolia.test", "redirect.algolia.test")
                        .withExposedPorts(8080)
                        .withEnv("FIXTURE_APP", applicationId)
                        .withEnv("FIXTURE_TOKEN", token)
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("algolia_fixture.py"),
                                "/provider.py")
                        .withCopyFileToContainer(
                                MountableFile.forHostPath(temporary.resolve("provider.crt")),
                                "/algolia-cert.pem")
                        .withCopyFileToContainer(
                                MountableFile.forHostPath(temporary.resolve("provider.key")),
                                "/algolia-key.pem")
                        .withCommand("python", "/provider.py")
                        .waitingFor(Wait.forHttp("/ready").forPort(8080));
        fixture.start();

        postgres =
                new PostgreSQLContainer<>(DockerImageName.parse("postgres:14-alpine"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("algolia-db")
                        .withDatabaseName("test")
                        .withUsername("test")
                        .withPassword("test");
        postgres.start();
        try (Connection sql = sql();
                Statement query = sql.createStatement()) {
            query.execute(
                    "CREATE TABLE algolia_rows (resource TEXT, id TEXT, parent_id TEXT, record TEXT)");
        }
        Path jdbc = DependencyJar.of(Driver.class).path();
        engine =
                new Engine() {
                    @Override
                    protected void executeExtraCommands(GenericContainer<?> container) {
                        container.withCopyFileToContainer(
                                MountableFile.forHostPath(temporary.resolve("trust.jks")),
                                "/tmp/algolia.jks");
                        container.withCopyFileToContainer(
                                MountableFile.forHostPath(loggers), "/tmp/algolia-log.properties");
                        container.withCopyFileToContainer(
                                MountableFile.forHostPath(jdbc),
                                "/tmp/seatunnel/plugins/Jdbc/lib/postgresql.jar");
                    }

                    @Override
                    protected String getJavaToolOptions() {
                        return "-Djavax.net.ssl.trustStore=/tmp/algolia.jks -Djavax.net.ssl.trustStorePassword="
                                + TRUST_PASSWORD
                                + " -Dlog4j2.configurationFile=/tmp/algolia-log.properties";
                    }
                };
        engine.startUp();
    }

    private static void generateCertificate() throws Exception {
        Path store = temporary.resolve("tls.p12");
        Process process =
                new ProcessBuilder(
                                Paths.get(System.getProperty("java.home"), "bin", "keytool")
                                        .toString(),
                                "-genkeypair",
                                "-noprompt",
                                "-alias",
                                "provider",
                                "-keyalg",
                                "RSA",
                                "-keysize",
                                "2048",
                                "-validity",
                                "2",
                                "-dname",
                                "CN=" + HOST,
                                "-ext",
                                "SAN=dns:" + HOST + ",dns:redirect.algolia.test",
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                store.toString(),
                                "-storepass",
                                TRUST_PASSWORD)
                        .redirectErrorStream(true)
                        .redirectOutput(temporary.resolve("keytool.log").toFile())
                        .start();
        if (!process.waitFor(30, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            throw new IllegalStateException("Fixture certificate command timed out");
        }
        assertEquals(0, process.exitValue());
        KeyStore keys = KeyStore.getInstance("PKCS12");
        try (InputStream source = Files.newInputStream(store)) {
            keys.load(source, TRUST_PASSWORD.toCharArray());
        }
        Certificate server = keys.getCertificate("provider");
        pem("CERTIFICATE", server.getEncoded(), temporary.resolve("provider.crt"));
        pem(
                "PRIVATE KEY",
                keys.getKey("provider", TRUST_PASSWORD.toCharArray()).getEncoded(),
                temporary.resolve("provider.key"));
        KeyStore trust = KeyStore.getInstance("JKS");
        trust.load(null, null);
        trust.setCertificateEntry("provider", server);
        try (OutputStream sink = Files.newOutputStream(temporary.resolve("trust.jks"))) {
            trust.store(sink, TRUST_PASSWORD.toCharArray());
        }
    }

    private static void pem(String type, byte[] bytes, Path target) throws Exception {
        String data = Base64.getMimeEncoder(64, new byte[] {'\n'}).encodeToString(bytes);
        Files.writeString(
                target, "-----BEGIN " + type + "-----\n" + data + "\n-----END " + type + "-----\n");
    }

    @AfterAll
    void shutdownFixtures() throws Exception {
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
    void exportsAllFiveResourcesWithExactQueriesAndIndependentParentDiscovery() throws Exception {
        select("all");
        success(run(CATALOG, defaults()));
        List<String> expected = new ArrayList<>();
        expected.add(index(INDEX, START));
        expected.add(index("new", MILLIS));
        expected.add(index("tie", START));
        expected.add(languages(false));
        expected.addAll(windowLogs());
        expected.addAll(hits(INDEX, 1, 4));
        List<String> rest = Arrays.asList("archive", "new", "cutoff", "tie");
        for (int i = 0; i < rest.size(); i++) {
            expected.add(hit(rest.get(i), 10 + i));
            expected.add(settingsRow(rest.get(i)));
        }
        expected.add(settingsRow(INDEX));
        assertRows(expected);
        assertEquals(Arrays.asList("0", "1", "2"), valuesFor(ROOT, "page"));
        assertEquals(Arrays.asList("0", "2", "4"), valuesFor(LOG_ROUTE, "offset"));
        assertEquals(Arrays.asList("0", "1"), valuesFor(ENCODED_QUERY, "page"));
        assertTrue(paths().contains(ENCODED_SETTINGS));
        assertEquals(Arrays.asList(QUERY + "&page=0", QUERY + "&page=1"), sentForms(ENCODED_QUERY));
        for (JsonNode request : calls()) {
            assertTrue(request.path("auth").asBoolean());
            if ("POST".equals(request.path("kind").asText())) {
                assertTrue(request.path("path").asText().endsWith("/query"));
            }
        }
    }

    @Test
    void preservesBothEmptyAndPopulatedLanguageMaps() throws Exception {
        select("empty-languages");
        success(run(LANGS, defaults()));
        assertRows(Collections.singletonList(languages(true)));
        assertEquals(Collections.singletonList(LANG_ROUTE), paths());
        select("all");
        success(run(LANGS, defaults()));
        assertRows(Collections.singletonList(languages(false)));
    }

    @Test
    void distinguishesFullSearchFromOnePageOrOffsetWindow() throws Exception {
        select("explicit-page");
        String requestedPage = "getRankingInfo=1&hitsPerPage=2&page=4&query=caf%C3%A9";
        success(
                run(
                        QUERY_JOB,
                        settings(ORIGIN, token, requestedPage, 2, 2, 10, 10000, 65536, 100000)));
        assertRows(Collections.singletonList(hit(INDEX, 44)));
        assertEquals(Collections.singletonList(requestedPage), sentForms(ENCODED_QUERY));
        select("explicit-offset");
        String requestedOffset = "offset=5&length=2&query=caf%C3%A9";
        success(
                run(
                        QUERY_JOB,
                        settings(ORIGIN, token, requestedOffset, 2, 2, 10, 10000, 65536, 100000)));
        assertRows(Collections.singletonList(hit(INDEX, 95)));
        assertEquals(Collections.singletonList(requestedOffset), sentForms(ENCODED_QUERY));

        for (String invalid :
                Arrays.asList("page=0&offset=1", "hitsPerPage=2&%70age=0&page=1", "length=2")) {
            select("all");
            int mark = engine.getServerLogs().length();
            failure(
                    run(
                            QUERY_JOB,
                            settings(ORIGIN, token, invalid, 2, 2, 10, 10000, 65536, 100000)),
                    mark,
                    "search_query has invalid or conflicting");
            assertTrue(calls().isEmpty());
        }
    }

    @Test
    void failsCompleteSearchOnReportedCapsApproximationOrBadContinuation() throws Exception {
        rejected(QUERY_JOB, "cap", "search results exceed accessible pagination", 2);
        rejected(QUERY_JOB, "approximate", "search hit count is approximate", 2);
        rejected(QUERY_JOB, "wrong-page", "search pagination metadata differs", 2);

        select("final-clipped");
        int mark = engine.getServerLogs().length();
        failure(run(QUERY_JOB, defaults()), mark, "search results exceed accessible pagination");
        assertEquals(Arrays.asList("0", "1", "2"), valuesFor(ENCODED_QUERY, "page"));
        assertRows(hits(INDEX, 1, 5));

        select("search-repeat");
        mark = engine.getServerLogs().length();
        failure(run(QUERY_JOB, defaults()), mark, "pagination repeated a record identity");
        assertRows(hits(INDEX, 1, 3));

        select("zero-hits");
        success(run(QUERY_JOB, defaults()));
        assertRows(Collections.emptyList());
        assertEquals(Arrays.asList(ROOT, ENCODED_QUERY), paths());
    }

    @Test
    void followsAnnouncedIndexPagesAndDetectsRepeatedOrExcessivePages() throws Exception {
        select("empty-index-middle");
        success(run(INDICES, defaults()));
        assertRows(Arrays.asList(index(INDEX, START), index("new", MILLIS)));
        assertEquals(Arrays.asList("0", "1", "2"), valuesFor(ROOT, "page"));

        select("repeat-indices");
        int mark = engine.getServerLogs().length();
        failure(run(INDICES, defaults()), mark, "repeated index page");
        assertRows(Arrays.asList(index(INDEX, START), index("new", MILLIS)));

        select("all");
        mark = engine.getServerLogs().length();
        failure(
                run(INDICES, settings(ORIGIN, token, QUERY, 2, 2, 1, 10000, 65536, 100000)),
                mark,
                "index enumeration exceeds max_pages");
        assertRows(Collections.emptyList());

        select("zero-indices");
        success(run(INDICES, defaults()));
        assertRows(Collections.emptyList());
    }

    @Test
    void readsTheBoundedLogHorizonAndDetectsShiftedOffsets() throws Exception {
        select("retention");
        success(run(LOGS, settings(ORIGIN, token, QUERY, 2, 400, 10, 10000, 1048576, 100000)));
        List<String> retained =
                IntStream.range(0, 1000)
                        .mapToObj(i -> log(String.format(Locale.ROOT, "retained-%04d", i), START))
                        .collect(Collectors.toList());
        assertRows(retained);
        assertEquals(Arrays.asList("0", "400", "800"), valuesFor(LOG_ROUTE, "offset"));
        assertEquals(Arrays.asList("400", "400", "200"), valuesFor(LOG_ROUTE, "length"));

        select("repeat-logs");
        int mark = engine.getServerLogs().length();
        failure(run(LOGS, defaults()), mark, "pagination repeated a record identity");
        assertRows(Collections.singletonList(log("log-millis", MILLIS)));
    }

    @Test
    void confinesCredentialsAndKeepsProviderFailuresOutOfTheDiagnostics() throws Exception {
        rejected(LOGS, "denied", "logs: HTTP 403", 1);
        rejected(INDICES, "redirect", "indices: HTTP 302", 1);
        rejected(LANGS, "oversize", "response exceeds max_response_bytes", 1);
        rejected(INDICES, "bad-json", "invalid JSON response", 1);

        select("all");
        int mark = engine.getServerLogs().length();
        String wrong = UUID.randomUUID().toString();
        Container.ExecResult bad =
                run(INDICES, settings(ORIGIN, wrong, QUERY, 2, 2, 10, 10000, 65536, 100000));
        failure(bad, mark, "indices: HTTP 401");
        assertFalse(
                (engine.getServerLogs().substring(mark) + bad.getStdout() + bad.getStderr())
                        .contains(wrong));
        assertFalse(calls().get(0).path("auth").asBoolean());

        select("all");
        mark = engine.getServerLogs().length();
        failure(
                run(
                        INDICES,
                        settings(
                                "https://wrong.algolia.test:8443",
                                token,
                                QUERY,
                                2,
                                2,
                                10,
                                10000,
                                65536,
                                100000)),
                mark,
                "TLS peer verification");
        assertTrue(calls().isEmpty());
        assertRows(Collections.emptyList());
    }

    @Test
    void honorsRetryAfterAndBoundsAttemptsAndElapsedTime() throws Exception {
        select("retry");
        success(run(LANGS, defaults()));
        assertRows(Collections.singletonList(languages(false)));
        JsonNode wire = calls();
        assertEquals(3, wire.size());
        assertTrue(
                wire.get(1).path("start_ms").asLong() - wire.get(0).path("start_ms").asLong()
                        >= 950);
        rejected(INDICES, "busy", "HTTP 503 after 3 attempts", 3);
        rejected(INDICES, "long-wait", "retry delay exceeds the remaining deadline", 1);
    }

    @Test
    void closesStalledAndTricklingResponsesAndAnEngineCancellation() throws Exception {
        for (String mode : Arrays.asList("stall", "trickle")) {
            select(mode);
            int mark = engine.getServerLogs().length();
            failure(
                    run(INDICES, settings(ORIGIN, token, QUERY, 2, 2, 10, 2700, 65536, 100000)),
                    mark,
                    "page deadline exceeded");
            awaitClose(0, 10000);
            assertRows(Collections.emptyList());
        }
        select("cancel");
        String job = nextJob();
        CompletableFuture<Container.ExecResult> running =
                submit(INDICES, job, recovery(QUERY, 2, 100000));
        try {
            awaitHold(0);
            awaitCheckpoint(job);
            success(engine.cancelJob(job));
            await().atMost(30, TimeUnit.SECONDS)
                    .until(() -> "CANCELED".equals(engine.getJobStatus(job)));
            running.get(45, TimeUnit.SECONDS);
            awaitClose(0, 45000);
            assertRows(Collections.emptyList());
        } finally {
            cleanup(job);
        }
    }

    @Test
    void restoresAnEarlyCheckpointWithNoRowsEmitted() throws Exception {
        select("hold-first");
        String job = nextJob();
        List<String> options = recovery(QUERY, 2, 100000);
        CompletableFuture<Container.ExecResult> running = submit(INDICES, job, options);
        try {
            awaitHold(0);
            awaitCheckpoint(job);
            assertRows(Collections.emptyList());
            success(engine.savepointJob(job));
            success(running.get(60, TimeUnit.SECONDS));
            awaitClose(0, 45000);
            control("/release");
            success(engine.restoreJob(INDICES, job, options.toArray(new String[0])));
            assertRows(Collections.singletonList(index(INDEX, START)));
            assertEquals(Arrays.asList("0", "0"), valuesFor(ROOT, "page"));
        } finally {
            cleanup(job);
        }
    }

    @Test
    void restoresAQueryPageAndRejectsChangedParametersBeforeFetching() throws Exception {
        select("hold-query");
        String job = nextJob();
        String changed = nextJob();
        List<String> options = recovery(QUERY, 2, 100000);
        CompletableFuture<Container.ExecResult> running = submit(QUERY_JOB, job, options);
        try {
            awaitHold(2);
            await().atMost(25, TimeUnit.SECONDS).untilAsserted(() -> assertRows(hits(INDEX, 1, 3)));
            awaitCheckpoint(job);
            success(engine.savepointJob(job));
            success(running.get(60, TimeUnit.SECONDS));
            awaitClose(2, 45000);
            int mark = engine.getServerLogs().length();
            failure(
                    engine.restoreJobWithCheckpoint(
                            QUERY_JOB, job, changed, recovery(QUERY + "&distinct=1", 2, 100000)),
                    mark,
                    "checkpoint does not match the configured source scope");
            assertEquals(3, calls().size());
            control("/release");
            success(engine.restoreJob(QUERY_JOB, job, options.toArray(new String[0])));
            assertRows(hits(INDEX, 1, 4));
            assertEquals(Arrays.asList("0", "1", "1"), valuesFor(ENCODED_QUERY, "page"));
            assertEquals(Arrays.asList("0", "0"), valuesFor(ROOT, "page"));
        } finally {
            cleanup(job, changed);
        }
    }

    @Test
    void restoresLogOffsetsWithEqualTimestamps() throws Exception {
        select("hold-logs");
        String job = nextJob();
        List<String> options = recovery(QUERY, 2, 100000);
        CompletableFuture<Container.ExecResult> running = submit(LOGS, job, options);
        try {
            awaitHold(1);
            await().atMost(25, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> assertRows(Collections.singletonList(log("log-millis", MILLIS))));
            awaitCheckpoint(job);
            success(engine.savepointJob(job));
            success(running.get(60, TimeUnit.SECONDS));
            awaitClose(1, 45000);
            control("/release");
            success(engine.restoreJob(LOGS, job, options.toArray(new String[0])));
            assertRows(windowLogs());
            assertEquals(Arrays.asList("0", "2", "2", "4"), valuesFor(LOG_ROUTE, "offset"));
        } finally {
            cleanup(job);
        }
    }

    @Test
    void rejectsChangedMidPageContentsAndResumesExactRows() throws Exception {
        select("long-index");
        String job = nextJob();
        String changed = nextJob();
        List<String> fast = recovery(QUERY, 50, 100000);
        CompletableFuture<Container.ExecResult> running =
                submit(INDICES, job, recovery(QUERY, 50, 2));
        try {
            await().atMost(40, TimeUnit.SECONDS).until(() -> rows().size() >= 4);
            success(engine.savepointJob(job));
            success(running.get(60, TimeUnit.SECONDS));
            List<String> committed = rows();
            assertTrue(
                    committed.size() > 0 && committed.size() < 50,
                    "Savepoint must fall inside the page");
            control("/change?value=1");
            int mark = engine.getServerLogs().length();
            failure(
                    engine.restoreJobWithCheckpoint(INDICES, job, changed, fast),
                    mark,
                    "saved page has changed");
            assertRows(committed);
            control("/change?value=0");
            success(engine.restoreJob(INDICES, job, fast.toArray(new String[0])));
            assertRows(
                    IntStream.range(0, 50)
                            .mapToObj(
                                    i -> index(String.format(Locale.ROOT, "batch-%02d", i), START))
                            .collect(Collectors.toList()));
            assertEquals(Arrays.asList("0", "0", "0"), valuesFor(ROOT, "page"));
        } finally {
            cleanup(job, changed);
        }
    }

    @Test
    void restoresTheTerminalSourceWithoutRepeatingItsRequest() throws Exception {
        select("empty-languages");
        String job = nextJob();
        CompletableFuture<Container.ExecResult> running =
                submit(TERMINAL, job, recovery(QUERY, 2, 24));
        try {
            await().atMost(40, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                assertRows(Collections.singletonList(languages(true)));
                                assertEquals("RUNNING", engine.getJobStatus(job));
                                assertTrue(engine.completedPipeline(job));
                            });
            success(engine.savepointJob(job));
            success(running.get(60, TimeUnit.SECONDS));
            success(
                    engine.restoreJob(
                            TERMINAL, job, recovery(QUERY, 2, 100000).toArray(new String[0])));
            assertRows(Collections.singletonList(languages(true)));
            assertEquals(Collections.singletonList(LANG_ROUTE), paths());
        } finally {
            cleanup(job);
        }
    }

    private List<String> defaults() {
        return settings(ORIGIN, token, QUERY, 2, 2, 10, 10000, 65536, 100000);
    }

    private List<String> recovery(String form, int size, int rate) {
        return settings(ORIGIN, token, form, size, 2, 10, 60000, 65536, rate);
    }

    private List<String> settings(
            String origin,
            String key,
            String form,
            int size,
            int logSize,
            int maxPages,
            int deadline,
            int maxBytes,
            int rate) {
        return Arrays.asList(
                "fixture_origin=" + origin,
                "fixture_application_id=" + applicationId,
                "fixture_token=" + key,
                "fixture_start=" + START,
                "fixture_end=" + END,
                "fixture_query=" + form,
                "fixture_size=" + size,
                "fixture_log_size=" + logSize,
                "fixture_max_pages=" + maxPages,
                "fixture_deadline=" + deadline,
                "fixture_bytes=" + maxBytes,
                "fixture_rate=" + rate);
    }

    private void rejected(String config, String mode, String message, int expectedCalls)
            throws Exception {
        select(mode);
        int mark = engine.getServerLogs().length();
        failure(run(config, defaults()), mark, message);
        assertEquals(expectedCalls, calls().size());
        assertRows(Collections.emptyList());
    }

    private void failure(Container.ExecResult result, int mark, String message) {
        assertNotEquals(0, result.getExitCode());
        String logs =
                engine.getServerLogs().substring(mark) + result.getStdout() + result.getStderr();
        assertTrue(logs.contains(message), "Expected a sanitized failure containing " + message);
        privateLogs(logs);
    }

    private void privateLogs(String logs) {
        assertFalse(
                logs.contains(token) || logs.contains(PRIVATE),
                "Credentials or provider diagnostics leaked");
    }

    private static void success(Container.ExecResult result) {
        assertEquals(0, result.getExitCode());
    }

    private Container.ExecResult run(String config, List<String> args) throws Exception {
        return engine.executeJob(config, nextJob(), args.toArray(new String[0]));
    }

    private static String nextJob() {
        return Long.toString(IDS.incrementAndGet());
    }

    private CompletableFuture<Container.ExecResult> submit(
            String config, String job, List<String> args) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return engine.executeJob(config, job, args.toArray(new String[0]));
                    } catch (Exception error) {
                        throw new IllegalStateException("Fixture engine command failed", error);
                    }
                });
    }

    private void awaitHold(int index) {
        await().atMost(45, TimeUnit.SECONDS)
                .until(() -> calls().path(index).path("held").asBoolean());
    }

    private void awaitClose(int index, long limit) {
        await().atMost(15, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            long millis = calls().path(index).path("closed_ms").asLong(-1);
                            assertTrue(
                                    millis >= 0 && millis <= limit,
                                    "Provider did not observe the connection close");
                        });
    }

    private void awaitCheckpoint(String job) {
        long previous = engine.getCompletedCheckpointCount(job);
        await().atMost(30, TimeUnit.SECONDS)
                .until(() -> engine.getCompletedCheckpointCount(job) > previous);
    }

    private void cleanup(String... jobs) throws Exception {
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

    private void select(String mode) throws Exception {
        control("/select?mode=" + mode);
        try (Connection sql = sql();
                Statement statement = sql.createStatement()) {
            statement.execute("TRUNCATE TABLE algolia_rows");
        }
    }

    private List<String> paths() throws Exception {
        List<String> paths = new ArrayList<>();
        for (JsonNode call : calls()) {
            paths.add(call.path("path").asText());
        }
        return paths;
    }

    private List<String> valuesFor(String path, String parameter) throws Exception {
        List<String> found = new ArrayList<>();
        for (JsonNode observed : calls()) {
            if (path.equals(observed.path("path").asText())) {
                found.add(param(observed, parameter));
            }
        }
        return found;
    }

    private static String param(JsonNode observed, String parameter) {
        return observed.path("query").path(parameter).path(0).asText("");
    }

    private JsonNode calls() throws Exception {
        return control("/wire");
    }

    private JsonNode control(String path) throws Exception {
        return get(new URL("http", fixture.getHost(), fixture.getMappedPort(8080), path));
    }

    private static JsonNode get(URL address) throws Exception {
        HttpURLConnection connection = (HttpURLConnection) address.openConnection();
        connection.setConnectTimeout(5000);
        connection.setReadTimeout(5000);
        try (InputStream data = connection.getInputStream()) {
            return JSON.readTree(data);
        } finally {
            connection.disconnect();
        }
    }

    private Connection sql() throws Exception {
        return DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }

    private List<String> rows() throws Exception {
        List<String> data = new ArrayList<>();
        try (Connection sql = sql();
                Statement statement = sql.createStatement();
                ResultSet result =
                        statement.executeQuery(
                                "SELECT resource, id, parent_id, record FROM algolia_rows")) {
            while (result.next()) {
                data.add(
                        result.getString(1)
                                + "|"
                                + result.getString(2)
                                + "|"
                                + result.getString(3)
                                + "|"
                                + result.getString(4));
            }
        }
        Collections.sort(data);
        return data;
    }

    private void assertRows(List<String> expected) throws Exception {
        List<String> ordered = new ArrayList<>(expected);
        Collections.sort(ordered);
        assertEquals(ordered, rows());
    }

    private List<String> sentForms(String path) throws Exception {
        List<String> forms = new ArrayList<>();
        for (JsonNode request : calls()) {
            if (path.equals(request.path("path").asText())) {
                forms.add(request.path("params").asText());
            }
        }
        return forms;
    }

    private static String index(String name, String time) {
        return "indices|"
                + name
                + "|null|{\"name\":\""
                + name
                + "\",\"updatedAt\":\""
                + time
                + "\",\"entries\":70}";
    }

    private static String settingsRow(String name) {
        return "indexes_settings|"
                + name
                + "|"
                + name
                + "|{\"paginationLimitedTo\":1000,"
                + "\"customRanking\":[\"desc(score)\"],\"fixture\":\""
                + name
                + "\"}";
    }

    private static List<String> hits(String index, int first, int end) {
        return IntStream.range(first, end)
                .mapToObj(i -> hit(index, i))
                .collect(Collectors.toList());
    }

    private static String hit(String index, int value) {
        String id = "hit-" + value;
        return "indexes_query|"
                + id
                + "|"
                + index
                + "|{\"objectID\":\""
                + id
                + "\",\"_rankingInfo\":{\"nbTypos\":1},"
                + "\"_highlightResult\":{\"title\":{\"value\":\"<em>café</em>\"}},"
                + "\"record\":{\"label\":\"café 雪\",\"nested\":{\"nullable\":null,"
                + "\"number\":1234567890123456789012345678901234567890,"
                + "\"price\":9876543210987654321.123456789012345678900}}}";
    }

    private static String languages(boolean empty) {
        return "available_languages|languages|null|"
                + (empty
                        ? "{}"
                        : "{\"en\":{\"stopwords\":[\"the\"],\"enabled\":true},\"fr\":{\"stopwords\":[\"été\"],\"enabled\":false}}");
    }

    private static String log(String id, String at) {
        return "logs|"
                + id
                + "|null|{\"sha1\":\""
                + id
                + "\",\"timestamp\":\""
                + at
                + "\",\"query_headers\":\""
                + PRIVATE
                + "\",\"answer\":null}";
    }

    private static List<String> windowLogs() {
        return Arrays.asList(
                log("log-millis", MILLIS), log("log-tie-a", START), log("log-tie-b", START));
    }

    /** Reads completed engine checkpoint metadata for the terminal recovery case. */
    private static class Engine extends SeaTunnelContainer {
        @Override
        protected Container.ExecResult executeCommand(
                GenericContainer<?> container, List<String> command)
                throws IOException, InterruptedException {
            // Preserve form separators as literal arguments and avoid logging fixture credentials.
            return container.execInContainer(command.toArray(new String[0]));
        }

        boolean completedPipeline(String job) throws Exception {
            URL rest =
                    new URL(
                            "http",
                            server.getHost(),
                            server.getMappedPort(8080),
                            "/jobs/checkpoints/" + job);
            for (JsonNode item : get(rest).path("pipelines")) {
                if ("completed-point"
                        .equals(item.path("latestCompleted").path("checkpointType").asText())) {
                    return true;
                }
            }
            return false;
        }
    }
}
