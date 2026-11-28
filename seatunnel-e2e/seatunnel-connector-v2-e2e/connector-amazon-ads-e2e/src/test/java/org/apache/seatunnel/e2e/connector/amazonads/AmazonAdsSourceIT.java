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
package org.apache.seatunnel.e2e.connector.amazonads;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

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
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises all Ads resources through packaged SeaTunnel Zeta sources and exact JDBC capture.
 * Connection modes, asynchronous provider status and private download behavior are independently
 * observed.
 */
class AmazonAdsSourceIT extends TestSuiteBase {

    private static final String CATALOG = "/amazonads_catalog.conf";
    private static final String ENTITY = "/amazonads_entity.conf";
    private static final String RECOMMENDATIONS = "/amazonads_recommendations.conf";
    private static final String TERMINAL = "/amazonads_terminal.conf";
    private static final String SUMMARY = "sponsored_products_campaigns_report_stream";
    private static final String DAILY = SUMMARY + "_daily";
    private static final String PORTFOLIOS = "portfolios";
    private static final AtomicLong IDS = new AtomicLong(System.currentTimeMillis());
    private static final ObjectMapper JSON = new ObjectMapper();
    @TempDir private static Path temporary;
    private AmazonAdsFixture fixture;
    private PostgreSQLContainer<?> postgres;
    private Engine engine;

    @BeforeAll
    void startFixtures() throws Exception {
        fixture = new AmazonAdsFixture(temporary);
        Path loggers = temporary.resolve("http-debug.properties");
        Files.writeString(
                loggers,
                "appender.console.type=Console\nappender.console.name=OUT\n"
                        + "appender.console.layout.type=PatternLayout\nappender.console.layout.pattern=%p %c %m%n\n"
                        + "rootLogger.level=info\nrootLogger.appenderRef.out.ref=OUT\n"
                        + "logger.http.name=org.apache.http\nlogger.http.level=debug\n");
        postgres =
                new PostgreSQLContainer<>(DockerImageName.parse("postgres:14-alpine"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("amazonads-db")
                        .withDatabaseName("test")
                        .withUsername("test")
                        .withPassword("test");
        postgres.start();
        try (Connection sql = sql();
                Statement statement = sql.createStatement()) {
            statement.execute(
                    "CREATE TABLE amazonads_rows (resource TEXT, id TEXT, profile_id TEXT, "
                            + "parent_id TEXT, window_start TEXT, window_end TEXT, record TEXT)");
        }
        Path jdbc = DependencyJar.of(Driver.class).path();
        engine =
                new Engine() {
                    @Override
                    protected void executeExtraCommands(GenericContainer<?> container) {
                        container.withCopyFileToContainer(
                                MountableFile.forHostPath(fixture.trust),
                                "/tmp/amazonads-trust.jks");
                        container.withCopyFileToContainer(
                                MountableFile.forHostPath(loggers),
                                "/tmp/amazonads-log.properties");
                        container.withCopyFileToContainer(
                                MountableFile.forHostPath(jdbc),
                                "/tmp/seatunnel/plugins/Jdbc/lib/postgresql.jar");
                    }

                    @Override
                    protected String getJavaToolOptions() {
                        return "-Djavax.net.ssl.trustStore=/tmp/amazonads-trust.jks -Djavax.net.ssl.trustStorePassword="
                                + AmazonAdsFixture.STORE_PASSWORD
                                + " -Dlog4j2.configurationFile=/tmp/amazonads-log.properties";
                    }
                };
        engine.startUp();
    }

    @AfterAll
    void stopFixtures() throws Exception {
        try {
            if (engine != null) {
                engine.tearDown();
            }
        } finally {
            try {
                if (postgres != null) {
                    postgres.stop();
                }
            } finally {
                if (fixture != null) {
                    fixture.close();
                }
            }
        }
    }

    @Test
    void exportsAllFiftyNineResourcesAcrossIndependentProfileScopes() throws Exception {
        AmazonAdsFixture.Scenario mode = select("catalog");
        success(run(CATALOG, defaults(PORTFOLIOS, 100000)));
        List<String> expected = profileRows();
        expected.addAll(directRows(null));
        expected.addAll(reportRows(mode, 1));
        assertRows(expected);
        Set<String> resources = new HashSet<>();
        for (String row : rows()) {
            resources.add(row.substring(0, row.indexOf('|')));
        }
        assertEquals(59, resources.size());
        assertEquals(68, mode.creates.get());
        assertEquals(68, mode.downloads.get());
        assertTrue(mode.invalid.isEmpty(), mode.invalid.toString());
        assertTrue(mode.events.stream().allMatch(event -> event.valid));
        assertTrue(
                mode.events.stream()
                        .anyMatch(
                                event ->
                                        event.origin.equals("api") && "222".equals(event.profile)));
        assertTrue(
                mode.events.stream()
                        .filter(e -> e.path.equals(AmazonAdsFixture.PROFILE_PATH))
                        .allMatch(e -> e.profile == null));
        assertTrue(
                mode.events.stream()
                        .filter(e -> e.origin.equals("download"))
                        .allMatch(e -> e.profile == null));
    }

    @Test
    void readsAllAdGroupParentsEvenWhenOnlyRecommendationsAreSelected() throws Exception {
        AmazonAdsFixture.Scenario mode = select("catalog");
        success(run(RECOMMENDATIONS, defaults(PORTFOLIOS, 100000)));
        assertRows(directRows("recommendations"));
        assertTrue(mode.invalid.isEmpty(), mode.invalid.toString());
        assertEquals(
                4, mode.events.stream().filter(e -> e.path.equals("/sp/adGroups/list")).count());
    }

    @Test
    void retainsOpaqueCursorsAndRejectsCyclesOrMissingRows() throws Exception {
        select("catalog");
        success(run(ENTITY, defaults(PORTFOLIOS, 100000)));
        assertRows(directRows(PORTFOLIOS));
        select("cursor-cycle");
        int mark = engine.getServerLogs().length();
        failure(
                run(ENTITY, defaults(PORTFOLIOS, 100000)),
                mark,
                "continuation repeats or exceeds max_pages");
        assertRows(firstPortfolio());
        select("lost-page");
        mark = engine.getServerLogs().length();
        failure(
                run(ENTITY, defaults(PORTFOLIOS, 100000)),
                mark,
                "pagination total changed or indicates missing rows");
        assertRows(firstPortfolio());
    }

    @Test
    void renewsProactivelyAndAfterARead401AndRejectsInvalidGrants() throws Exception {
        for (String mode : List.of("expiry", "reactive")) {
            AmazonAdsFixture.Scenario state = select(mode);
            success(run(ENTITY, defaults(PORTFOLIOS, 100000)));
            assertRows(directRows(PORTFOLIOS));
            assertEquals(2, state.tokens.get());
        }
        AmazonAdsFixture.Scenario invalid = select("bad-token");
        int mark = engine.getServerLogs().length();
        failure(run(ENTITY, defaults(PORTFOLIOS, 100000)), mark, "token request returned HTTP 400");
        assertEquals(1, invalid.events.size());
        assertRows(Collections.emptyList());
    }

    @Test
    void retriesThrottledReadsAndRejectsScopeAndTlsFaults() throws Exception {
        AmazonAdsFixture.Scenario retry = select("retry");
        success(run(ENTITY, defaults(PORTFOLIOS, 100000)));
        assertRows(directRows(PORTFOLIOS));
        assertTrue(
                retry.events.get(2).nanos - retry.events.get(1).nanos
                        >= TimeUnit.MILLISECONDS.toNanos(950));
        denied("denied", PORTFOLIOS, "HTTP 403");
        denied("redirect", PORTFOLIOS, "HTTP 302");
        denied("busy", PORTFOLIOS, "HTTP 503 after 3 attempts");
        denied("long-retry", PORTFOLIOS, "retry delay exceeds the remaining deadline");
        AmazonAdsFixture.Scenario tls = select("catalog");
        int mark = engine.getServerLogs().length();
        failure(
                run(
                        ENTITY,
                        replace(
                                defaults(PORTFOLIOS, 100000),
                                "fixture_api",
                                fixture.mismatchedTlsOrigin())),
                mark,
                "TLS peer verification");
        assertEquals(1, tls.events.size());
        assertEquals("oauth", tls.events.get(0).origin);
    }

    @Test
    void reusesOnlyAnExactDuplicateAndCapsMismatchesAndUnknownCreation() throws Exception {
        AmazonAdsFixture.Scenario exact = select("duplicate");
        success(run(ENTITY, defaults(SUMMARY, 100000)));
        assertRows(reportRows(exact, 1));
        assertEquals(2, exact.creates.get());
        assertEquals(2, exact.downloads.get());
        for (String mode : List.of("mismatch-425", "forbidden-425")) {
            AmazonAdsFixture.Scenario wrong = select(mode);
            int mark = engine.getServerLogs().length();
            failure(
                    run(ENTITY, defaults(DAILY, 100000)),
                    mark,
                    "HTTP 425 did not identify a reusable report");
            assertEquals(3, wrong.creates.get());
            assertEquals(0, wrong.downloads.get());
            assertRows(Collections.emptyList());
        }
        AmazonAdsFixture.Scenario unknown = select("unknown-create");
        int mark = engine.getServerLogs().length();
        failure(
                run(ENTITY, defaults(SUMMARY, 100000)),
                mark,
                "request outcome is unknown after I/O failure");
        assertEquals(1, unknown.creates.get());
        assertRows(Collections.emptyList());
    }

    @Test
    void advancesOnlyKnownReportStatusesAndBoundsProcessingTime() throws Exception {
        AmazonAdsFixture.Scenario moving = select("poll-transition");
        success(run(ENTITY, defaults(DAILY, 100000)));
        assertRows(reportRows(moving, 1));
        assertTrue(moving.reports.values().stream().allMatch(report -> report.lookups.get() == 3));
        denied("failed", SUMMARY, "report generation failed");
        denied("unknown-status", SUMMARY, "unknown status");
        select("poll-timeout");
        int mark = engine.getServerLogs().length();
        failure(
                run(ENTITY, replace(defaults(SUMMARY, 100000), "fixture_report_timeout", "1800")),
                mark,
                "report processing timeout exceeded");
        assertRows(Collections.emptyList());
    }

    @Test
    void verifiesSignedDownloadIsolationAndReacquiresExpiredUrls() throws Exception {
        for (String mode : List.of("expired-url", "download-403")) {
            AmazonAdsFixture.Scenario state = select(mode);
            success(run(ENTITY, defaults(SUMMARY, 100000)));
            assertRows(reportRows(state, 1));
            assertTrue(
                    state.events.stream()
                            .filter(event -> event.origin.equals("download"))
                            .allMatch(event -> event.valid && event.profile == null));
            assertTrue(
                    state.reports.values().stream().anyMatch(report -> report.lookups.get() > 1));
        }
        AmazonAdsFixture.Scenario denied = select("foreign-download");
        int mark = engine.getServerLogs().length();
        failure(
                run(ENTITY, defaults(SUMMARY, 100000)),
                mark,
                "report download origin is outside the allowed HTTPS hosts");
        assertEquals(0, denied.downloads.get());
        denied("download-redirect", SUMMARY, "HTTP 302");
    }

    @Test
    void acceptsDocumentedGzipEncodingsAndRejectsInvalidOrOversizedFiles() throws Exception {
        for (String mode : List.of("catalog", "transport-gzip", "double-gzip", "empty-report")) {
            AmazonAdsFixture.Scenario state = select(mode);
            success(run(ENTITY, defaults(DAILY, 100000)));
            assertRows(reportRows(state, mode.equals("empty-report") ? 0 : 1));
        }
        denied("empty-gzip", DAILY, "report payload must be a JSON array");
        denied("malformed-gzip", DAILY, "invalid gzip response");
        denied("truncated-gzip", DAILY, "truncated gzip report download");
        select("wire-limit");
        int wireMark = engine.getServerLogs().length();
        failure(
                run(ENTITY, replace(defaults(DAILY, 100000), "fixture_wire", "1024")),
                wireMark,
                "compressed report exceeds max_download_bytes");
        assertRows(Collections.emptyList());
        select("gzip-limit");
        int mark = engine.getServerLogs().length();
        failure(
                run(ENTITY, replace(defaults(DAILY, 100000), "fixture_report_bytes", "4096")),
                mark,
                "decoded report exceeds max_report_bytes");
        assertRows(Collections.emptyList());
    }

    @Test
    void cancelsBlockedCreationStatusAndDownloadOutsideCheckpointLocks() throws Exception {
        for (String mode : List.of("hold-create", "hold-poll", "hold-download")) {
            AmazonAdsFixture.Scenario state = select(mode);
            String job = nextJob();
            CompletableFuture<Container.ExecResult> running =
                    submit(ENTITY, job, recovery(SUMMARY, 100000));
            try {
                AmazonAdsFixture.Event event = held(state);
                awaitCheckpoint(job);
                success(engine.cancelJob(job));
                await().atMost(30, TimeUnit.SECONDS)
                        .until(() -> "CANCELED".equals(engine.getJobStatus(job)));
                running.get(45, TimeUnit.SECONDS);
                await().atMost(20, TimeUnit.SECONDS).until(() -> event.closed);
                assertRows(Collections.emptyList());
            } finally {
                cleanup(state, job);
            }
        }
        select("stall");
        int mark = engine.getServerLogs().length();
        failure(
                run(ENTITY, replace(recovery(PORTFOLIOS, 100000), "fixture_request", "2000")),
                mark,
                "deadline exceeded");
        assertRows(Collections.emptyList());
    }

    @Test
    void restoresBeforeFirstOutputAndRejectsIncompatibleResourceScope() throws Exception {
        AmazonAdsFixture.Scenario state = select("hold-oauth");
        String job = nextJob();
        String changed = nextJob();
        List<String> args = recovery(PORTFOLIOS, 100000);
        CompletableFuture<Container.ExecResult> running = submit(ENTITY, job, args);
        try {
            AmazonAdsFixture.Event event = held(state);
            awaitCheckpoint(job);
            assertRows(Collections.emptyList());
            success(engine.savepointJob(job));
            success(running.get(60, TimeUnit.SECONDS));
            await().atMost(20, TimeUnit.SECONDS).until(() -> event.closed);
            int mark = engine.getServerLogs().length();
            failure(
                    engine.restoreJobWithCheckpoint(
                            ENTITY, job, changed, recovery(SUMMARY, 100000)),
                    mark,
                    "checkpoint does not match the configured source scope");
            assertEquals(1, state.events.size());
            state.release = true;
            success(engine.restoreJob(ENTITY, job, args.toArray(new String[0])));
            assertRows(directRows(PORTFOLIOS));
        } finally {
            cleanup(state, job, changed);
        }
    }

    @Test
    void restoresDirectCursorAndItsOriginalProfile() throws Exception {
        AmazonAdsFixture.Scenario state = select("hold-direct");
        String job = nextJob();
        List<String> args = recovery(PORTFOLIOS, 100000);
        CompletableFuture<Container.ExecResult> running = submit(ENTITY, job, args);
        try {
            AmazonAdsFixture.Event event = held(state);
            assertEquals("111", event.profile);
            await().atMost(25, TimeUnit.SECONDS).untilAsserted(() -> assertRows(firstPortfolio()));
            awaitCheckpoint(job);
            success(engine.savepointJob(job));
            success(running.get(60, TimeUnit.SECONDS));
            await().atMost(20, TimeUnit.SECONDS).until(() -> event.closed);
            state.release = true;
            success(engine.restoreJob(ENTITY, job, args.toArray(new String[0])));
            assertRows(directRows(PORTFOLIOS));
        } finally {
            cleanup(state, job);
        }
    }

    @Test
    void restoresCreatePollAndDownloadWithTheCorrectDuplicateAndIdBehavior() throws Exception {
        for (String mode : List.of("hold-create", "hold-poll", "hold-download")) {
            AmazonAdsFixture.Scenario state = select(mode);
            String job = nextJob();
            List<String> args = recovery(SUMMARY, 100000);
            CompletableFuture<Container.ExecResult> running = submit(ENTITY, job, args);
            try {
                AmazonAdsFixture.Event event = held(state);
                awaitCheckpoint(job);
                assertRows(Collections.emptyList());
                success(engine.savepointJob(job));
                success(running.get(60, TimeUnit.SECONDS));
                await().atMost(20, TimeUnit.SECONDS).until(() -> event.closed);
                state.release = true;
                success(engine.restoreJob(ENTITY, job, args.toArray(new String[0])));
                assertRows(reportRows(state, 1));
                assertEquals(mode.equals("hold-create") ? 3 : 2, state.creates.get());
                assertEquals(2, state.reports.size());
            } finally {
                cleanup(state, job);
            }
        }
    }

    @Test
    void savesExactMidReportSpoolOffsetAndRejectsAChangedRefetch() throws Exception {
        AmazonAdsFixture.Scenario state = select("long-report");
        String job = nextJob();
        String changed = nextJob();
        List<String> fast = defaults(DAILY, 100000);
        CompletableFuture<Container.ExecResult> running = submit(ENTITY, job, defaults(DAILY, 2));
        try {
            await().atMost(40, TimeUnit.SECONDS).until(() -> rows().size() >= 4);
            success(engine.savepointJob(job));
            success(running.get(60, TimeUnit.SECONDS));
            List<String> saved = rows();
            assertTrue(
                    saved.size() > 0 && saved.size() < 60, "Savepoint must cut through one report");
            state.changed = true;
            int mark = engine.getServerLogs().length();
            failure(
                    engine.restoreJobWithCheckpoint(ENTITY, job, changed, fast),
                    mark,
                    "saved report content changed");
            assertRows(saved);
            state.changed = false;
            success(engine.restoreJob(ENTITY, job, fast.toArray(new String[0])));
            assertRows(reportRows(state, 60));
            assertEquals(2, state.creates.get());
        } finally {
            cleanup(state, job, changed);
        }
    }

    @Test
    void observesInclusiveReportWindowsAndKeepsTerminalRestoresQuiet() throws Exception {
        AmazonAdsFixture.Scenario windows = select("catalog");
        success(run(ENTITY, replace(defaults(SUMMARY, 100000), "fixture_window", "2")));
        assertRows(reportRows(windows, 1));
        assertEquals(4, windows.reports.size());

        AmazonAdsFixture.Scenario terminal = select("catalog");
        String job = nextJob();
        CompletableFuture<Container.ExecResult> running =
                submit(TERMINAL, job, defaults(PORTFOLIOS, 24));
        try {
            await().atMost(40, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                assertRows(profileRows());
                                assertEquals("RUNNING", engine.getJobStatus(job));
                                assertTrue(engine.completedPipeline(job));
                            });
            success(engine.savepointJob(job));
            success(running.get(60, TimeUnit.SECONDS));
            success(
                    engine.restoreJob(
                            TERMINAL, job, defaults(PORTFOLIOS, 100000).toArray(new String[0])));
            assertRows(profileRows());
            assertEquals(2, terminal.events.size());
        } finally {
            cleanup(terminal, job);
        }
    }

    private List<String> defaults(String resource, int rate) {
        return Arrays.asList(
                "fixture_api=" + fixture.apiOrigin(),
                "fixture_oauth=" + fixture.oauthOrigin(),
                "fixture_client=" + fixture.clientId,
                "fixture_secret=" + fixture.clientSecret,
                "fixture_refresh_token=" + fixture.refresh,
                "fixture_region=NA",
                "fixture_from=" + fixture.lower,
                "fixture_to=" + fixture.upper,
                "fixture_resource=" + resource,
                "fixture_request=6000",
                "fixture_report_timeout=10000",
                "fixture_download_timeout=6000",
                "fixture_window=30",
                "fixture_wire=1048576",
                "fixture_report_bytes=1048576",
                "fixture_rate=" + rate);
    }

    private List<String> recovery(String resource, int rate) {
        List<String> args = replace(defaults(resource, rate), "fixture_request", "60000");
        args = replace(args, "fixture_download_timeout", "60000");
        return replace(args, "fixture_report_timeout", "90000");
    }

    private static List<String> replace(List<String> values, String key, String value) {
        return values.stream()
                .map(entry -> entry.startsWith(key + "=") ? key + "=" + value : entry)
                .collect(Collectors.toList());
    }

    private void denied(String mode, String resource, String reason) throws Exception {
        select(mode);
        int mark = engine.getServerLogs().length();
        failure(run(ENTITY, defaults(resource, 100000)), mark, reason);
        assertRows(Collections.emptyList());
    }

    private void failure(Container.ExecResult result, int mark, String context) {
        assertNotEquals(0, result.getExitCode());
        String logs =
                engine.getServerLogs().substring(mark) + result.getStdout() + result.getStderr();
        assertTrue(logs.contains(context), "Expected sanitized failure containing " + context);
        assertFalse(
                logs.contains(fixture.clientSecret)
                        || logs.contains(fixture.refresh)
                        || logs.contains(fixture.basic)
                        || logs.contains("Atza|private-fixture-access")
                        || logs.contains(AmazonAdsFixture.PRIVATE)
                        || logs.contains(AmazonAdsFixture.SIGNATURE),
                "Provider diagnostic payload, token or signed download capability leaked");
    }

    private static void success(Container.ExecResult result) {
        assertEquals(0, result.getExitCode());
    }

    private Container.ExecResult run(String conf, List<String> args) throws Exception {
        return engine.executeJob(conf, nextJob(), args.toArray(new String[0]));
    }

    private static String nextJob() {
        return Long.toString(IDS.incrementAndGet());
    }

    private CompletableFuture<Container.ExecResult> submit(
            String conf, String job, List<String> args) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return engine.executeJob(conf, job, args.toArray(new String[0]));
                    } catch (Exception ex) {
                        throw new IllegalStateException("Fixture engine command failed", ex);
                    }
                });
    }

    private static AmazonAdsFixture.Event held(AmazonAdsFixture.Scenario mode) {
        await().atMost(45, TimeUnit.SECONDS)
                .until(() -> mode.events.stream().anyMatch(event -> event.held));
        return mode.events.stream().filter(event -> event.held).findFirst().orElseThrow();
    }

    private void awaitCheckpoint(String job) {
        long prior = engine.getCompletedCheckpointCount(job);
        await().atMost(30, TimeUnit.SECONDS)
                .until(() -> engine.getCompletedCheckpointCount(job) > prior);
    }

    private void cleanup(AmazonAdsFixture.Scenario mode, String... jobs) throws Exception {
        mode.release = true;
        for (String job : jobs) {
            String status = engine.getJobStatus(job);
            if ("RUNNING".equals(status)
                    || "PENDING".equals(status)
                    || "RESTARTING".equals(status)) {
                engine.cancelJob(job);
            }
        }
    }

    private AmazonAdsFixture.Scenario select(String name) throws Exception {
        try (Connection sql = sql();
                Statement s = sql.createStatement()) {
            s.execute("TRUNCATE TABLE amazonads_rows");
        }
        return fixture.select(name);
    }

    private Connection sql() throws Exception {
        return DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }

    private List<String> rows() throws Exception {
        List<String> data = new ArrayList<>();
        try (Connection sql = sql();
                Statement query = sql.createStatement();
                ResultSet result =
                        query.executeQuery(
                                "SELECT resource, id, profile_id, parent_id, window_start, window_end, record FROM amazonads_rows")) {
            while (result.next()) {
                List<String> fields = new ArrayList<>();
                for (int i = 1; i <= 7; i++) {
                    fields.add(String.valueOf(result.getString(i)));
                }
                data.add(String.join("|", fields));
            }
        }
        Collections.sort(data);
        return data;
    }

    private void assertRows(List<String> expected) throws Exception {
        List<String> sorted = new ArrayList<>(expected);
        Collections.sort(sorted);
        assertEquals(sorted, rows());
    }

    private static List<String> profileRows() {
        return AmazonAdsFixture.profiles().stream()
                .map(
                        value -> {
                            String id = value.path("profileId").asText();
                            return row("profiles", id, id, null, null, null, value);
                        })
                .collect(Collectors.toList());
    }

    private List<String> firstPortfolio() throws Exception {
        AmazonAdsFixtureCatalog.Operation route = AmazonAdsFixtureCatalog.DIRECT.get(0);
        assertEquals(PORTFOLIOS, route.name);
        ObjectNode input = AmazonAdsFixture.directRow(route, "111", 1);
        return Collections.singletonList(
                row(route.name, input.path(route.key).asText(), "111", null, null, null, input));
    }

    private List<String> directRows(String selected) throws Exception {
        List<String> out = new ArrayList<>();
        for (String profile : List.of("111", "222")) {
            for (AmazonAdsFixtureCatalog.Operation route : AmazonAdsFixtureCatalog.DIRECT) {
                boolean nested = route.mode.equals("BID") || route.mode.equals("SUGGEST");
                if (selected != null
                        && !selected.equals(route.name)
                        && !(selected.equals("recommendations") && nested)) {
                    continue;
                }
                if (nested) {
                    for (int number = 1; number <= 2; number++) {
                        String group = AmazonAdsFixture.groupId(profile, number);
                        ObjectNode record =
                                route.mode.equals("BID")
                                        ? AmazonAdsFixture.bid(group)
                                        : AmazonAdsFixture.suggestion(group);
                        record.put("adGroupId", group)
                                .put("campaignId", AmazonAdsFixture.campaignId(group));
                        String id = route.mode.equals("BID") ? group : digest(record.toString());
                        out.add(row(route.name, id, profile, group, null, null, record));
                    }
                } else {
                    int count = route.mode.equals("OFFSET") ? 3 : 2;
                    for (int index = 1; index <= count; index++) {
                        ObjectNode record = AmazonAdsFixture.directRow(route, profile, index);
                        String id =
                                route.key.isEmpty()
                                        ? digest(record.toString())
                                        : record.path(route.key).asText();
                        boolean attribution = route.mode.startsWith("AT:");
                        out.add(
                                row(
                                        route.name,
                                        id,
                                        profile,
                                        null,
                                        attribution ? fixture.lower.toString() : null,
                                        attribution ? fixture.upper.toString() : null,
                                        record));
                    }
                }
            }
        }
        return out;
    }

    private static List<String> reportRows(AmazonAdsFixture.Scenario state, int count)
            throws Exception {
        List<String> result = new ArrayList<>();
        for (AmazonAdsFixture.Report report : state.reports.values()) {
            String from = report.request.path("startDate").asText();
            String to = report.request.path("endDate").asText();
            String identity =
                    digest(
                            report.template.name,
                            report.profile,
                            from,
                            to,
                            report.request.path("configuration").toString());
            for (int index = 0; index < count; index++) {
                ObjectNode record =
                        AmazonAdsFixture.reportRow(
                                report.template, report.profile, from, index, false);
                record.put("profileId", report.profile).put("reportDate", to);
                result.add(
                        row(
                                report.template.name,
                                identity + ":" + index,
                                report.profile,
                                null,
                                from,
                                to,
                                record));
            }
        }
        return result;
    }

    private static String row(
            String resource,
            String id,
            String profile,
            String parent,
            String from,
            String to,
            JsonNode value) {
        return String.join(
                "|",
                resource,
                id,
                profile,
                String.valueOf(parent),
                String.valueOf(from),
                String.valueOf(to),
                value.toString());
    }

    private static String digest(String... fields) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        for (String value : fields) {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            digest.update(ByteBuffer.allocate(4).putInt(bytes.length).array());
            digest.update(bytes);
        }
        return Base64.getUrlEncoder().withoutPadding().encodeToString(digest.digest());
    }

    /**
     * Keeps fixture credentials and signed handles out of command diagnostics and reads checkpoint
     * events.
     *
     * <p>Docker receives literal argument arrays, including form delimiters and fixture OAuth
     * values.
     */
    private static class Engine extends SeaTunnelContainer {
        @Override
        protected Container.ExecResult executeCommand(
                GenericContainer<?> container, List<String> command)
                throws IOException, InterruptedException {
            return container.execInContainer(command.toArray(new String[0]));
        }

        boolean completedPipeline(String job) throws Exception {
            URL url =
                    new URL(
                            "http",
                            server.getHost(),
                            server.getMappedPort(8080),
                            "/jobs/checkpoints/" + job);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setConnectTimeout(5000);
            connection.setReadTimeout(5000);
            try (InputStream data = connection.getInputStream()) {
                for (JsonNode pipeline : JSON.readTree(data).path("pipelines")) {
                    if ("completed-point"
                            .equals(
                                    pipeline.path("latestCompleted")
                                            .path("checkpointType")
                                            .asText())) {
                        return true;
                    }
                }
                return false;
            } finally {
                connection.disconnect();
            }
        }
    }
}
