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
package org.apache.seatunnel.e2e.connector.aircall;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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

/** Runs the packaged source on Zeta against an independent HTTPS API and real JDBC sink. */
class AircallSourceIT extends TestSuiteBase {
    private static final String HOST = "tenant.aircall.test";
    private static final String ORIGIN = "https://" + HOST + ":8443";
    private static final String TRAP = "https://redirect.aircall.test:8443/trap";
    private static final String TRUST_PASSWORD = "fixture-trust";
    private static final String PRIVATE = "sensitive-provider-fixture-content";
    private static final String LARGE_ID = "1234567890123456789012345678901234567890";
    private static final long START = 1704067200L;
    private static final String CATALOG = "/aircall_catalog.conf";
    private static final String CALLS = "/aircall_calls.conf";
    private static final String TRANSCRIPTS = "/aircall_transcriptions.conf";
    private static final String ENGAGEMENTS = "/aircall_engagements.conf";
    private static final String CAMPAIGNS = "/aircall_campaigns.conf";
    private static final String TERMINAL = "/aircall_terminal.conf";
    private static final AtomicLong IDS = new AtomicLong(System.currentTimeMillis());
    private static final ObjectMapper JSON = new ObjectMapper();

    @TempDir private static Path temporary;

    private final String username = UUID.randomUUID().toString();
    private final String password = UUID.randomUUID().toString();
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
                        .withNetworkAliases(HOST, "wrong.aircall.test", "redirect.aircall.test")
                        .withExposedPorts(8080)
                        .withEnv("FIXTURE_USER", username)
                        .withEnv("FIXTURE_PASSWORD", password)
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("aircall_fixture.py"),
                                "/provider.py")
                        .withCopyFileToContainer(
                                MountableFile.forHostPath(temporary.resolve("provider.crt")),
                                "/aircall-cert.pem")
                        .withCopyFileToContainer(
                                MountableFile.forHostPath(temporary.resolve("provider.key")),
                                "/aircall-key.pem")
                        .withCommand("python", "/provider.py")
                        .waitingFor(Wait.forHttp("/ready").forPort(8080));
        fixture.start();

        postgres =
                new PostgreSQLContainer<>(DockerImageName.parse("postgres:14-alpine"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("aircall-db")
                        .withDatabaseName("test")
                        .withUsername("test")
                        .withPassword("test");
        postgres.start();
        try (Connection sql = sql();
                Statement query = sql.createStatement()) {
            query.execute(
                    "CREATE TABLE aircall_rows (resource TEXT, id TEXT, parent_id TEXT, "
                            + "call_started_at BIGINT, record TEXT)");
        }
        Path jdbc = DependencyJar.of(Driver.class).path();
        engine =
                new Engine() {
                    @Override
                    protected void executeExtraCommands(GenericContainer<?> container) {
                        container.withCopyFileToContainer(
                                MountableFile.forHostPath(temporary.resolve("trust.jks")),
                                "/tmp/aircall.jks");
                        container.withCopyFileToContainer(
                                MountableFile.forHostPath(loggers), "/tmp/aircall-log.properties");
                        container.withCopyFileToContainer(
                                MountableFile.forHostPath(jdbc),
                                "/tmp/seatunnel/plugins/Jdbc/lib/postgresql.jar");
                    }

                    @Override
                    protected String getJavaToolOptions() {
                        return "-Djavax.net.ssl.trustStore=/tmp/aircall.jks -Djavax.net.ssl.trustStorePassword="
                                + TRUST_PASSWORD
                                + " -Dlog4j2.configurationFile=/tmp/aircall-log.properties";
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
                                "SAN=dns:" + HOST + ",dns:redirect.aircall.test",
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
    void retainsExactRowsForAllTwentyFiveResourcesAcrossPaginationAndParents() throws Exception {
        select("catalog");
        int mark = engine.getServerLogs().length();
        success(run(CATALOG, defaults()));
        List<String> result = new ArrayList<>();
        result.addAll(callRows(101, 103));
        result.add(row("company", 200));
        result.addAll(
                Arrays.asList(
                        row("contacts", 210),
                        row("contacts", 211),
                        row("contacts", 212),
                        row("numbers", 301),
                        row("numbers", 302),
                        row("tags", 310),
                        row("user_availability", 320),
                        row("users", 401),
                        row("users", 402),
                        row("teams", 410),
                        row("webhooks", 420),
                        row("conversations", 501),
                        row("conversations", 502),
                        row("campaigns", 520),
                        row("campaigns", 521),
                        row("campaign_outcomes", 530),
                        row("campaign_outcomes", 531),
                        row("trackers", 540),
                        row("sms_templates", 550)));
        result.addAll(
                Arrays.asList(
                        childRow("conversation_engagements", 601, 501, null),
                        childRow("conversation_engagements", 602, 502, null),
                        childRow("whatsapp_templates", 610, 301, null),
                        childRow("whatsapp_templates", 611, 301, null),
                        childRow("user_numbers", 621, 401, null),
                        childRow("user_numbers", 622, 402, null),
                        childRow("dialer_campaigns", 631, 401, null),
                        childRow("dialer_campaign_phone_numbers", 641, 401, null)));
        for (int parent = 101; parent <= 102; parent++) {
            int idx = parent - 100;
            long time = START + parent - 101;
            result.addAll(
                    Arrays.asList(
                            childRow("call_transcriptions", 700 + idx, parent, time),
                            childRow("call_sentiments", 710 + idx, parent, time),
                            childRow("call_topics", 720 + idx, parent, time),
                            childRow("call_action_items", 740 + idx, parent, time),
                            childRow("call_playbook_results", 750 + idx, parent, time)));
            if (parent == 101) {
                result.add(childRow("call_summaries", 731, parent, time));
            }
        }
        assertRows(result);
        assertEquals(25, result.stream().map(row -> row.split("\\|", 2)[0]).distinct().count());

        Set<String> observed = new HashSet<>();
        JsonNode requests = calls();
        long prior = 0;
        int contactPages = 0;
        int whatsappPages = 0;
        for (JsonNode request : requests) {
            String path = request.path("path").asText();
            observed.add(path);
            assertTrue(request.path("auth").asBoolean());
            assertTrue(request.path("gzip").asBoolean());
            assertEquals(HOST + ":8443", request.path("host").asText());
            long started = request.path("start_ms").asLong();
            if (prior != 0) {
                assertTrue(started - prior >= 430, "Request pacing violated");
            }
            prior = started;
            if ("/v1/contacts".equals(path)) {
                contactPages++;
            } else if ("/v1/numbers/301/templates".equals(path)) {
                whatsappPages++;
            } else if ("/v1/sms/templates".equals(path)) {
                assertEquals("100", param(request, "per_page"));
            }
        }
        assertEquals(2, contactPages);
        assertEquals(2, whatsappPages);
        assertTrue(
                observed.containsAll(
                        Arrays.asList(
                                "/v1/calls/102/summary",
                                "/v1/numbers/302/templates",
                                "/v1/users/402/dialer_campaign",
                                "/v1/users/402/dialer_campaign/phone_numbers")));
        assertFalse(observed.contains("/trap"));
        assertTrue(valuesFor("/v1/campaigns", "cursor").contains("opaque+/=value"));
        assertTrue(valuesFor("/v1/campaign_outcomes", "cursor").contains("opaque+/=value"));
        assertEquals(Arrays.asList("", "10"), valuesFor("/v1/numbers/301/templates", "token"));
        privateLogs(engine.getServerLogs().substring(mark));
    }

    @Test
    void partitionsResultCeilingsBeforeEmittingAnyRows() throws Exception {
        select("saturated");
        success(run(CALLS, defaults()));
        assertEquals(
                Arrays.asList(Long.toString(START), Long.toString(START), Long.toString(START + 2)),
                valuesFor("/v1/calls", "from"));
        assertEquals(
                Arrays.asList(
                        Long.toString(START + 3),
                        Long.toString(START + 1),
                        Long.toString(START + 3)),
                valuesFor("/v1/calls", "to"));
        assertRows(callRows(101, 105));

        select("cap-400");
        success(
                run(
                        CALLS,
                        arguments(
                                ORIGIN, password, START, START + 2, 0, 2, 5, 6000, 65536, 100000)));
        assertEquals(
                Arrays.asList(Long.toString(START), Long.toString(START), Long.toString(START + 1)),
                valuesFor("/v1/calls", "from"));
        assertRows(callRows(101, 103));

        select("still-saturated");
        int mark = engine.getServerLogs().length();
        failure(
                run(
                        CALLS,
                        arguments(
                                ORIGIN, password, START, START + 1, 0, 2, 5, 6000, 65536, 100000)),
                mark,
                "10,000-result boundary in one second");
        assertRows(Collections.emptyList());
        assertEquals(1, calls().size());

        select("capped-child");
        mark = engine.getServerLogs().length();
        failure(
                run(ENGAGEMENTS, defaults()),
                mark,
                "10,000-result boundary for a child without a date filter");
        assertRows(Collections.emptyList());
    }

    @Test
    void separatesCreationAcquisitionFromStartedAtAndRereadsTheLookback() throws Exception {
        select("overlap");
        long cut = START + 259200;
        List<String> settings =
                arguments(ORIGIN, password, cut, cut + 4, 3, 3, 5, 6000, 65536, 100000);
        success(run(CALLS, settings));
        List<String> repeated = new ArrayList<>(Arrays.asList(call(101, cut), call(102, cut - 1)));
        assertRows(repeated);
        success(run(CALLS, settings));
        repeated.addAll(Arrays.asList(call(101, cut), call(102, cut - 1)));
        assertRows(repeated);
        assertEquals(Collections.nCopies(2, Long.toString(START)), valuesFor("/v1/calls", "from"));
        assertEquals(Collections.nCopies(2, Long.toString(cut + 3)), valuesFor("/v1/calls", "to"));
    }

    @Test
    void discoversParentsWhenOnlyChildrenAreSelectedAndDoesNotHidePermissionErrors()
            throws Exception {
        select("no-child-filter");
        success(run(ENGAGEMENTS, defaults()));
        assertRows(
                Arrays.asList(
                        childRow("conversation_engagements", 601, 501, null),
                        childRow("conversation_engagements", 602, 502, null)));
        assertEquals(
                Arrays.asList(
                        "/v1/conversations",
                        "/v1/conversations/501/engagements",
                        "/v1/conversations/502/engagements"),
                paths());
        assertFalse(calls().get(1).path("q").has("from"));
        assertFalse(calls().get(2).path("q").has("to"));

        rejected(TRANSCRIPTS, "ai-forbidden", "call_transcriptions HTTP 403", 2, 6000, 65536);
        rejected(TRANSCRIPTS, "ai-unknown", "call_transcriptions HTTP 404", 2, 6000, 65536);
    }

    @Test
    void followsCredibleEmptyPageAndRejectsHostPathAndCursorChanges() throws Exception {
        select("empty-continuation");
        success(run(CALLS, defaults()));
        assertRows(callRows(103, 104));
        assertEquals(Arrays.asList("1", "2"), valuesFor("/v1/calls", "page"));
        rejected(
                CALLS,
                "foreign",
                "continuation violates the configured HTTPS origin or path",
                1,
                6000,
                65536);
        rejected(
                CALLS,
                "wrong-path",
                "continuation violates the configured HTTPS origin or path",
                1,
                6000,
                65536);
        rejected(
                CALLS,
                "wrong-page",
                "continuation changes the query or does not advance",
                1,
                6000,
                65536);
        rejected(CAMPAIGNS, "cursor-cycle", "pagination cursor repeated", 2, 6000, 65536);
    }

    @Test
    void rejectsOutOfScopeTransportAndKeepsCredentialsOutOfLogs() throws Exception {
        rejected(CALLS, "forbidden", "HTTP 403", 1, 4000, 65536);
        rejected(CALLS, "redirect", "HTTP 302", 1, 4000, 65536);
        assertEquals(Collections.singletonList("/v1/calls"), paths());
        rejected(CALLS, "bad-json", "invalid JSON response", 1, 4000, 65536);
        rejected(CALLS, "huge", "response exceeds max_response_bytes", 1, 4000, 2048);

        select("catalog");
        int mark = engine.getServerLogs().length();
        String bad = UUID.randomUUID().toString();
        Container.ExecResult badAuth =
                run(CALLS, arguments(ORIGIN, bad, START, START + 4, 0, 2, 5, 5000, 65536, 100000));
        failure(badAuth, mark, "HTTP 401");
        assertFalse(
                (engine.getServerLogs().substring(mark) + badAuth.getStdout() + badAuth.getStderr())
                        .contains(bad));
        assertFalse(calls().get(0).path("auth").asBoolean());

        select("catalog");
        mark = engine.getServerLogs().length();
        failure(
                run(
                        CALLS,
                        arguments(
                                "https://wrong.aircall.test:8443",
                                password,
                                START,
                                START + 4,
                                0,
                                2,
                                5,
                                5000,
                                65536,
                                100000)),
                mark,
                "TLS peer verification");
        assertTrue(calls().isEmpty());
    }

    @Test
    void honorsResetHeadersAndBoundsRetryAttemptsAndElapsedTime() throws Exception {
        select("retry");
        success(
                run(
                        CALLS,
                        arguments(
                                ORIGIN, password, START, START + 4, 0, 2, 5, 8000, 65536, 100000)));
        assertRows(callRows(101, 103));
        JsonNode observed = calls();
        assertEquals(3, observed.size());
        assertTrue(
                observed.get(1).path("start_ms").asLong()
                                - observed.get(0).path("start_ms").asLong()
                        >= 950,
                "Fixture did not observe the server's delay");
        rejected(CALLS, "busy", "HTTP 503 after 3 attempts", 3, 8000, 65536);
        rejected(CALLS, "long-reset", "retry delay exceeds the remaining deadline", 1, 2500, 65536);
    }

    @Test
    void closesStalledAndTricklingResponsesIncludingCancellation() throws Exception {
        rejected(CALLS, "stall", "page deadline exceeded", 1, 2500, 65536);
        awaitClose(0, 10000);
        rejected(CALLS, "trickle", "page deadline exceeded", 1, 2500, 65536);
        awaitClose(0, 10000);

        select("cancel");
        String job = nextJob();
        CompletableFuture<Container.ExecResult> running =
                submit(
                        CALLS,
                        job,
                        arguments(
                                ORIGIN, password, START, START + 4, 0, 2, 5, 60000, 65536, 100000));
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
    void restoresPagePositionFromANewCheckpointAndRejectsDifferentScope() throws Exception {
        select("hold-offset");
        String job = nextJob();
        String mismatch = nextJob();
        List<String> settings =
                arguments(ORIGIN, password, START, START + 4, 0, 2, 5, 60000, 65536, 100000);
        CompletableFuture<Container.ExecResult> running = submit(CALLS, job, settings);
        try {
            awaitHold(1);
            await().atMost(25, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertRows(callRows(101, 103)));
            awaitCheckpoint(job);
            success(engine.savepointJob(job));
            success(running.get(60, TimeUnit.SECONDS));
            awaitClose(1, 45000);

            int mark = engine.getServerLogs().length();
            failure(
                    engine.restoreJobWithCheckpoint(
                            CALLS,
                            job,
                            mismatch,
                            arguments(
                                    ORIGIN, password, START, START + 4, 0, 3, 5, 60000, 65536,
                                    100000)),
                    mark,
                    "Aircall checkpoint is incompatible");
            assertEquals(2, calls().size());
            control("/release");
            success(engine.restoreJob(CALLS, job, settings.toArray(new String[0])));
            assertRows(callRows(101, 104));
            assertEquals(Arrays.asList("1", "2", "2"), valuesFor("/v1/calls", "page"));
        } finally {
            cleanup(job, mismatch);
        }
    }

    @Test
    void restoresAHeldChildWithoutExportingOrLosingParents() throws Exception {
        select("hold-child");
        String job = nextJob();
        List<String> settings =
                arguments(ORIGIN, password, START, START + 4, 0, 2, 5, 60000, 65536, 100000);
        CompletableFuture<Container.ExecResult> running = submit(TRANSCRIPTS, job, settings);
        try {
            awaitHold(1);
            assertRows(Collections.emptyList());
            awaitCheckpoint(job);
            success(engine.savepointJob(job));
            success(running.get(60, TimeUnit.SECONDS));
            awaitClose(1, 45000);
            assertFalse(calls().get(1).path("released").asBoolean());
            control("/release");
            success(engine.restoreJob(TRANSCRIPTS, job, settings.toArray(new String[0])));
            assertRows(
                    Arrays.asList(
                            childRow("call_transcriptions", 701, 101, START),
                            childRow("call_transcriptions", 702, 102, START + 1)));
            assertEquals(
                    Arrays.asList(
                            "/v1/calls",
                            "/v1/calls/101/transcription",
                            "/v1/calls",
                            "/v1/calls/101/transcription",
                            "/v1/calls/102/transcription"),
                    paths());
        } finally {
            cleanup(job);
        }
    }

    @Test
    void restoresWithinTheSamePageWithoutDuplicatingRows() throws Exception {
        select("long-page");
        String job = nextJob();
        List<String> slow =
                arguments(ORIGIN, password, START, START + 60, 0, 50, 5, 60000, 65536, 2);
        List<String> fast =
                arguments(ORIGIN, password, START, START + 60, 0, 50, 5, 60000, 65536, 100000);
        CompletableFuture<Container.ExecResult> running = submit(CALLS, job, slow);
        try {
            await().atMost(40, TimeUnit.SECONDS).until(() -> rows().size() >= 4);
            success(engine.savepointJob(job));
            success(running.get(60, TimeUnit.SECONDS));
            int committed = rows().size();
            assertTrue(
                    committed > 0 && committed < 50,
                    "Savepoint did not cut inside a response page");
            success(engine.restoreJob(CALLS, job, fast.toArray(new String[0])));
            assertRows(callRows(101, 151));
            assertEquals(Arrays.asList("1", "1"), valuesFor("/v1/calls", "page"));
        } finally {
            cleanup(job);
        }
    }

    @Test
    void restoresAnAlreadyFinishedSourceWithoutAnotherRequest() throws Exception {
        select("catalog");
        String job = nextJob();
        List<String> slow =
                arguments(ORIGIN, password, START, START + 4, 0, 2, 5, 60000, 65536, 24);
        List<String> fast =
                arguments(ORIGIN, password, START, START + 4, 0, 2, 5, 60000, 65536, 100000);
        CompletableFuture<Container.ExecResult> running = submit(TERMINAL, job, slow);
        try {
            await().atMost(40, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                assertRows(Collections.singletonList(row("company", 200)));
                                assertEquals("RUNNING", engine.getJobStatus(job));
                                assertTrue(engine.completedPipeline(job));
                            });
            success(engine.savepointJob(job));
            success(running.get(60, TimeUnit.SECONDS));
            success(engine.restoreJob(TERMINAL, job, fast.toArray(new String[0])));
            assertEquals(Collections.singletonList("/v1/company"), paths());
            assertRows(Collections.singletonList(row("company", 200)));
        } finally {
            cleanup(job);
        }
    }

    private List<String> defaults() {
        return arguments(ORIGIN, password, START, START + 4, 0, 2, 5, 8000, 65536, 100000);
    }

    private List<String> arguments(
            String origin,
            String token,
            long from,
            long to,
            int lookback,
            int size,
            int pages,
            int deadline,
            int bytes,
            int rate) {
        return Arrays.asList(
                "fixture_origin=" + origin,
                "fixture_user=" + username,
                "fixture_password=" + token,
                "fixture_start=" + Instant.ofEpochSecond(from),
                "fixture_end=" + Instant.ofEpochSecond(to),
                "fixture_overlap=" + lookback,
                "fixture_size=" + size,
                "fixture_max_pages=" + pages,
                "fixture_deadline=" + deadline,
                "fixture_bytes=" + bytes,
                "fixture_rate=" + rate);
    }

    private void rejected(
            String config, String mode, String message, int expectedCalls, int timeout, int bytes)
            throws Exception {
        select(mode);
        int mark = engine.getServerLogs().length();
        failure(
                run(
                        config,
                        arguments(
                                ORIGIN, password, START, START + 4, 0, 2, 5, timeout, bytes,
                                100000)),
                mark,
                message);
        assertEquals(expectedCalls, calls().size());
    }

    private void failure(Container.ExecResult result, int mark, String message) {
        assertNotEquals(0, result.getExitCode());
        String logs =
                engine.getServerLogs().substring(mark) + result.getStdout() + result.getStderr();
        assertTrue(logs.contains(message), "Expected a sanitized failure containing " + message);
        privateLogs(logs);
    }

    private void privateLogs(String logs) {
        String encoded =
                Base64.getEncoder()
                        .encodeToString(
                                (username + ":" + password).getBytes(StandardCharsets.US_ASCII));
        assertFalse(
                logs.contains(username)
                        || logs.contains(password)
                        || logs.contains(encoded)
                        || logs.contains(PRIVATE)
                        || logs.contains("opaque+/=value"),
                "Provider data or credentials leaked");
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
            statement.execute("TRUNCATE TABLE aircall_rows");
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
        return observed.path("q").path(parameter).path(0).asText("");
    }

    private JsonNode calls() throws Exception {
        return control("/calls");
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
                                "SELECT resource, id, parent_id, call_started_at, record FROM aircall_rows")) {
            while (result.next()) {
                data.add(
                        result.getString(1)
                                + "|"
                                + result.getString(2)
                                + "|"
                                + result.getString(3)
                                + "|"
                                + result.getString(4)
                                + "|"
                                + result.getString(5));
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

    private static List<String> callRows(int first, int end) {
        return IntStream.range(first, end)
                .mapToObj(id -> call(id, START + id - 101))
                .collect(Collectors.toList());
    }

    private static String call(int id, long started) {
        return row("calls", id, null, started);
    }

    private static String row(String resource, int id) {
        return row(resource, id, null, null);
    }

    private static String childRow(String resource, int id, int parent, Long started) {
        return row(resource, id, parent, started);
    }

    private static String row(String resource, int id, Integer parent, Long started) {
        String key = "webhooks".equals(resource) ? "webhook_id" : "id";
        String identifier =
                "contacts".equals(resource) && id == 210 ? LARGE_ID : Integer.toString(id);
        StringBuilder result =
                new StringBuilder("{\"")
                        .append(key)
                        .append("\":")
                        .append(identifier)
                        .append(",\"name\":\"café 雪\"");
        if ("calls".equals(resource)) {
            result.append(",\"started_at\":")
                    .append(started)
                    .append(",\"recording\":\"")
                    .append(TRAP)
                    .append("\"");
        } else if ("conversations".equals(resource)) {
            String epoch = Instant.ofEpochSecond(START + id - 501).toString();
            result.append(",\"last_engagement_at\":\"")
                    .append(epoch.replace("Z", ".500Z"))
                    .append("\"");
        }
        result.append(
                ",\"nested\":{\"nullable\":null,\"values\":[true,17,1234567890123456789.123456789012345678900]}");
        if ("call_playbook_results".equals(resource)) {
            result.append(",\"call_id\":").append(parent);
        }
        return resource
                + "|"
                + identifier
                + "|"
                + parent
                + "|"
                + started
                + "|"
                + result.append('}');
    }

    private static class Engine extends SeaTunnelContainer {
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
