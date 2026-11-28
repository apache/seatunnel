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
package org.apache.seatunnel.e2e.connector.appstoreconnect;

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
import java.security.KeyPairGenerator;
import java.security.spec.ECGenParameterSpec;
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
 * Exercises the packaged App Store Connect connector through Zeta and persisted JDBC rows.
 *
 * <p>The HTTPS fixture validates both URL/filter scope and signing independently, and state cases
 * distinguish first output, parent continuation, segment resolution, partial reports and
 * completion.
 */
class AppStoreConnectSourceIT extends TestSuiteBase {
    private static final String APPS = "list_id_apps";
    private static final String SALES = "sales_report";
    private static final String INSTALL_REPORT = "app_store_installations_and_deletions";
    private static final String CATALOG = "/appstoreconnect_catalog.conf";
    private static final String SINGLE = "/appstoreconnect_single.conf";
    private static final String TERMINAL = "/appstoreconnect_terminal.conf";
    private static final ObjectMapper JSON = AppStoreConnectFixture.JSON;
    private static final AtomicLong JOBS = new AtomicLong(System.currentTimeMillis());

    @TempDir private static Path temporary;
    private AppStoreConnectFixture fixture;
    private PostgreSQLContainer<?> postgres;
    private Engine engine;

    @BeforeAll
    void startFixtures() throws Exception {
        fixture = new AppStoreConnectFixture(temporary);
        Path logger = temporary.resolve("appstore-debug.properties");
        Files.writeString(
                logger,
                "appender.console.type=Console\nappender.console.name=OUT\n"
                        + "appender.console.layout.type=PatternLayout\n"
                        + "appender.console.layout.pattern=%p %c %m%n"
                        + "\nrootLogger.level=info\nrootLogger.appenderRef.out.ref=OUT\n"
                        + "logger.http.name=org.apache.http\nlogger.http.level=debug\n");
        postgres =
                new PostgreSQLContainer<>(DockerImageName.parse("postgres:14-alpine"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("appstore-db")
                        .withDatabaseName("test")
                        .withUsername("test")
                        .withPassword("test");
        postgres.start();
        try (Connection connection = db();
                Statement sql = connection.createStatement()) {
            sql.execute(
                    "CREATE TABLE appstore_rows (resource TEXT, id TEXT, app_id TEXT, "
                            + "request_id TEXT, report_id TEXT, instance_id TEXT, segment_id TEXT, "
                            + "report_date TEXT, record TEXT)");
        }
        Path driver = DependencyJar.of(Driver.class).path();
        engine =
                new Engine() {
                    @Override
                    protected void executeExtraCommands(GenericContainer<?> container) {
                        container.withCopyFileToContainer(
                                MountableFile.forHostPath(fixture.trust),
                                "/tmp/appstore-trust.jks");
                        container.withCopyFileToContainer(
                                MountableFile.forHostPath(logger),
                                "/tmp/appstore-debug.properties");
                        container.withCopyFileToContainer(
                                MountableFile.forHostPath(driver),
                                "/tmp/seatunnel/plugins/Jdbc/lib/postgresql.jar");
                    }

                    @Override
                    protected String getJavaToolOptions() {
                        return "-Djavax.net.ssl.trustStore=/tmp/appstore-trust.jks -Djavax.net.ssl.trustStorePassword="
                                + AppStoreConnectFixture.STORE_PASSWORD
                                + " -Dlog4j2.configurationFile=/tmp/appstore-debug.properties";
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
    void readsEveryResourceThroughIndependentlyVerifiedProviderRequests() throws Exception {
        AppStoreConnectFixture.Scenario mode = select("matrix");
        success(run(CATALOG, defaults(APPS, 100000)), mode);
        assertRows(matrixRows(mode));
        Set<String> labels =
                rows().stream()
                        .map(value -> value.substring(0, value.indexOf('|')))
                        .collect(Collectors.toSet());
        assertEquals(29, labels.size());
        assertEquals(4, mode.downloads.get());
        assertEquals(8, mode.details.get());
        assertTrue(mode.invalid.isEmpty(), mode.invalid.toString());
        assertEquals(1, mode.tokens.size());
        assertTrue(
                mode.calls.stream()
                        .filter(call -> call.signed)
                        .allMatch(call -> call.query.keySet().equals(Set.of("signature"))));
        for (String layer :
                List.of("analyticsReportRequests", "/reports", "/instances", "/segments")) {
            assertTrue(
                    mode.calls.stream()
                            .anyMatch(
                                    call ->
                                            call.path.contains(layer)
                                                    && AppStoreConnectFixture.CURSOR.equals(
                                                            call.query.get("cursor"))),
                    layer);
        }
        assertTrue(
                mode.calls.stream()
                        .filter(call -> call.path.endsWith("/segments"))
                        .noneMatch(call -> call.path.contains("/old-o-")));
    }

    @Test
    void distinguishesNoReportAndRetentionFromConfigurationOrPermissionFailures() throws Exception {
        for (String name : List.of("no-report", "gone-report")) {
            AppStoreConnectFixture.Scenario state = select(name);
            success(run(SINGLE, defaults(SALES, 100000)), state);
            assertRows(List.of());
        }
        rejected("bad-vendor", SALES, "dated report request returned HTTP 400");
        rejected("unknown-report-404", SALES, "dated report request returned HTTP 404");
        rejected("forbidden", APPS, "metadata request returned HTTP 403");
    }

    @Test
    void requestsFiscalMonthsAndSkipsSubscriptionDatesWithoutARequest() throws Exception {
        AppStoreConnectFixture.Scenario finance = select("matrix");
        success(
                run(
                        SINGLE,
                        replace(
                                defaults("finance_report", 100000),
                                "fixture_fiscal",
                                fixture.fiscal.minusMonths(1).toString())),
                finance);
        assertEquals(
                List.of(fixture.fiscal.minusMonths(1).toString(), fixture.fiscal.toString()),
                finance.calls.stream()
                        .map(call -> call.query.get("filter[reportDate]"))
                        .collect(Collectors.toList()));
        assertRows(
                List.of(
                        datedRow(
                                "finance_report",
                                "FINANCIAL",
                                fixture.fiscal.minusMonths(1).toString(),
                                1),
                        datedRow("finance_report", "FINANCIAL", fixture.fiscal.toString(), 1)));
        AppStoreConnectFixture.Scenario events = select("matrix");
        List<String> skip =
                replace(
                        defaults("subscription_event_report", 100000),
                        "fixture_event_start",
                        fixture.eventDay.minusDays(1).toString());
        skip = replace(skip, "fixture_event_skip", fixture.eventDay.minusDays(1).toString());
        success(run(SINGLE, skip), events);
        assertEquals(
                List.of(fixture.eventDay.toString()),
                events.calls.stream()
                        .map(call -> call.query.get("filter[reportDate]"))
                        .collect(Collectors.toList()));
        assertRows(
                List.of(
                        datedRow(
                                "subscription_event_report",
                                "SUBSCRIPTION_EVENT",
                                fixture.eventDay.toString(),
                                1)));
    }

    @Test
    void retainsOpaqueCursorsButBoundsMissingChangedOrRepeatedPagination() throws Exception {
        AppStoreConnectFixture.Scenario empty = select("empty-page");
        success(run(SINGLE, defaults(APPS, 100000)), empty);
        assertRows(List.of(appRow("app-b")));
        assertEquals(AppStoreConnectFixture.CURSOR, empty.calls.get(1).query.get("cursor"));

        AppStoreConnectFixture.Scenario cycle = select("cycle");
        int mark = logs().length();
        failure(
                run(SINGLE, defaults(APPS, 100000)),
                mark,
                "pagination repeated a cursor or exceeded max_pages_per_parent",
                cycle);
        assertEquals(2, cycle.calls.size());

        AppStoreConnectFixture.Scenario bounded = select("matrix");
        mark = logs().length();
        failure(
                run(SINGLE, replace(defaults(APPS, 100000), "fixture_pages", "1")),
                mark,
                "pagination repeated a cursor or exceeded max_pages_per_parent",
                bounded);
        assertEquals(1, bounded.calls.size());

        rejected("cross-origin", APPS, "links.next changed the API origin or collection");
        rejected("changed-filter", APPS, "links.next changed the filters or repeated a parameter");
        rejected("bad-json", APPS, "invalid JSON response");
        rejected("bad-error-shape", APPS, "metadata contained provider errors");
    }

    @Test
    void validatesEcSigningResignsOnceAndKeepsCredentialsOutOfHttpDebugOutput() throws Exception {
        AppStoreConnectFixture.Scenario renewed = select("resign");
        success(run(SINGLE, defaults(APPS, 100000)), renewed);
        assertRows(List.of(appRow("app-a"), appRow("app-b")));
        assertEquals(2, renewed.tokens.size());
        assertTrue(renewed.invalid.isEmpty());

        AppStoreConnectFixture.Scenario pem = select("matrix");
        String text =
                "-----BEGIN PRIVATE KEY----- " + fixture.secret + " -----END PRIVATE KEY-----";
        success(run(SINGLE, replace(defaults(APPS, 100000), "fixture_key", text)), pem);
        assertRows(List.of(appRow("app-a"), appRow("app-b")));
        assertEquals(1, pem.tokens.size());

        AppStoreConnectFixture.Scenario denied = select("unauthorized");
        failure(
                run(SINGLE, defaults(APPS, 100000)),
                logs().length(),
                "HTTP 401 after re-signing the API token",
                denied);
        assertEquals(2, denied.calls.size());
        assertEquals(2, denied.tokens.size());
        assertRows(List.of());

        AppStoreConnectFixture.Scenario wrongCurve = select("matrix");
        KeyPairGenerator key = KeyPairGenerator.getInstance("EC");
        key.initialize(new ECGenParameterSpec("secp384r1"));
        String malformed =
                Base64.getEncoder().encodeToString(key.generateKeyPair().getPrivate().getEncoded());
        List<String> args = replace(defaults(APPS, 100000), "fixture_key", malformed);
        int mark = logs().length();
        Container.ExecResult failed = run(SINGLE, args);
        failure(failed, mark, "Invalid AppStoreConnect option secret_key", wrongCurve);
        assertFalse(
                (failed.getStdout() + failed.getStderr() + logs().substring(mark))
                        .contains(malformed));
        assertEquals(0, wrongCurve.calls.size());
    }

    @Test
    void acceptsShortRetryAfterAndFailsFastWhenTheHintExceedsTheBudget() throws Exception {
        AppStoreConnectFixture.Scenario transientResponse = select("retry");
        success(run(SINGLE, defaults(APPS, 100000)), transientResponse);
        assertEquals(3, transientResponse.calls.size());
        assertRows(List.of(appRow("app-a"), appRow("app-b")));

        AppStoreConnectFixture.Scenario delayed = select("retry-budget");
        int mark = logs().length();
        failure(
                run(SINGLE, defaults(APPS, 100000)),
                mark,
                "retry or request pacing exceeds the operation deadline",
                delayed);
        assertEquals(1, delayed.calls.size());
        assertRows(List.of());
    }

    @Test
    void rejectsUntrustedHttpsAndRedirectsWithoutSendingBearerToAnotherOrigin() throws Exception {
        AppStoreConnectFixture.Scenario certificate = select("matrix");
        int mark = logs().length();
        failure(
                run(SINGLE, replace(defaults(APPS, 100000), "fixture_api", fixture.wrongOrigin())),
                mark,
                "HTTPS certificate verification or negotiation failed",
                certificate);
        assertTrue(certificate.calls.isEmpty());
        rejected("redirect", APPS, "metadata request returned HTTP 302");
        rejected("download-redirect", INSTALL_REPORT, "signed download returned HTTP 302");
        rejected(
                "disallowed-host",
                INSTALL_REPORT,
                "signed download HTTPS host is outside the allowed hosts");
    }

    @Test
    void refreshesOnlyTheExpiredUrlAndVerifiesSegmentIdentityAndMd5() throws Exception {
        AppStoreConnectFixture.Scenario expired = select("expired");
        success(run(SINGLE, defaults(INSTALL_REPORT, 100000)), expired);
        assertRows(List.of(analyticsRow(INSTALL_REPORT, "o", "install", 1)));
        assertEquals(2, expired.details.get());
        assertEquals(2, expired.downloads.get());
        assertEquals(
                2,
                expired.calls.stream()
                        .filter(call -> call.signed)
                        .map(call -> call.query.get("signature"))
                        .distinct()
                        .count());
        assertTrue(expired.invalid.isEmpty());
        rejected(
                "checksum",
                INSTALL_REPORT,
                "analytics segment size or MD5 does not match metadata");
        rejected(
                "other-segment",
                "app_download",
                "segment metadata returned a different segment ID");
    }

    @Test
    void readsEveryNonemptySegmentOfAnInstance() throws Exception {
        AppStoreConnectFixture.Scenario state = select("multi-segment");
        success(run(SINGLE, defaults(INSTALL_REPORT, 100000)), state);
        String date = fixture.daily.toString();
        String segment = "seg-o-install-second";
        assertRows(
                List.of(
                        analyticsRow(INSTALL_REPORT, "o", "install", 1),
                        row(
                                INSTALL_REPORT,
                                segment + ":1",
                                "app-a",
                                "req-o",
                                "report-o-install",
                                "ins-o-install",
                                segment,
                                date,
                                contents(segment, 1, date))));
        assertEquals(2, state.downloads.get());
        assertEquals(2, state.details.get());
        assertTrue(state.invalid.isEmpty());
    }

    @Test
    void decodesDoubleGzipAndRejectsCorruptionExpansionRowLimitsAndInvalidUtf8() throws Exception {
        AppStoreConnectFixture.Scenario doubleGzip = select("double-gzip");
        success(run(SINGLE, defaults(SALES, 100000)), doubleGzip);
        assertRows(List.of(datedRow(SALES, "SALES", fixture.daily.toString(), 1)));
        rejected(
                "bad-gzip",
                SALES,
                "compressed report is truncated, malformed, or could not be staged");
        rejected("bad-tsv", SALES, "TSV contains an unterminated quoted field");
        rejected(
                "bad-utf8",
                SALES,
                "compressed report is truncated, malformed, or could not be staged");

        AppStoreConnectFixture.Scenario expansion = select("oversized");
        int mark = logs().length();
        failure(
                run(SINGLE, replace(defaults(SALES, 100000), "fixture_decoded", "2048")),
                mark,
                "report byte limit exceeded",
                expansion);
        assertRows(List.of());

        AppStoreConnectFixture.Scenario row = select("huge-row");
        mark = logs().length();
        failure(
                run(SINGLE, replace(defaults(SALES, 100000), "fixture_row", "256")),
                mark,
                "TSV row exceeds max_report_row_chars",
                row);
        assertRows(List.of());

        AppStoreConnectFixture.Scenario transfer = select("large-wire");
        mark = logs().length();
        failure(
                run(SINGLE, replace(defaults(SALES, 100000), "fixture_wire", "256")),
                mark,
                "report byte limit exceeded",
                transfer);
        assertRows(List.of());
    }

    @Test
    void capsTrickleAndIdleResponsesAndCancelsTheActiveDownloadConnection() throws Exception {
        for (String name : List.of("stall", "trickle")) {
            AppStoreConnectFixture.Scenario state = select(name);
            int mark = logs().length();
            List<String> args = replace(defaults(APPS, 100000), "fixture_timeout", "1500");
            args = replace(args, "fixture_operation", "1500");
            failure(run(SINGLE, args), mark, "deadline exceeded", state);
            assertRows(List.of());
            state.release = true;
        }
        AppStoreConnectFixture.Scenario state = select("hold-download");
        String job = jobId();
        CompletableFuture<Container.ExecResult> running =
                submit(SINGLE, job, recovering(INSTALL_REPORT));
        try {
            AppStoreConnectFixture.Event event = held(state);
            awaitCheckpoint(job);
            assertEquals(0, rows().size());
            assertEquals(0, engine.cancelJob(job).getExitCode());
            await().atMost(30, TimeUnit.SECONDS)
                    .until(() -> "CANCELED".equals(engine.getJobStatus(job)));
            running.get(45, TimeUnit.SECONDS);
            await().atMost(15, TimeUnit.SECONDS).until(() -> event.closed);
            assertRows(List.of());
        } finally {
            cleanup(state, job);
        }
    }

    @Test
    void restoresBeforeFirstOutputAndRejectsChangedLogicalOptions() throws Exception {
        AppStoreConnectFixture.Scenario state = select("hold-first");
        String job = jobId();
        String mismatch = jobId();
        List<String> args = recovering(APPS);
        CompletableFuture<Container.ExecResult> running = submit(SINGLE, job, args);
        try {
            AppStoreConnectFixture.Event event = held(state);
            awaitCheckpoint(job);
            assertRows(List.of());
            assertEquals(0, engine.savepointJob(job).getExitCode());
            success(running.get(60, TimeUnit.SECONDS), state);
            await().atMost(15, TimeUnit.SECONDS).until(() -> event.closed);
            int mark = logs().length();
            failure(
                    engine.restoreJobWithCheckpoint(SINGLE, job, mismatch, recovering(SALES)),
                    mark,
                    "checkpoint has incompatible scan options or state",
                    state);
            assertEquals(1, state.calls.size());
            state.release = true;
            success(engine.restoreJob(SINGLE, job, args.toArray(new String[0])), state);
            assertRows(List.of(appRow("app-a"), appRow("app-b")));
        } finally {
            cleanup(state, job, mismatch);
        }
    }

    @Test
    void resumesAContinuationWithoutReplayingEarlierRows() throws Exception {
        AppStoreConnectFixture.Scenario state = select("hold-page");
        String job = jobId();
        List<String> args = recovering(APPS);
        CompletableFuture<Container.ExecResult> running = submit(SINGLE, job, args);
        try {
            AppStoreConnectFixture.Event event = held(state);
            assertEquals(AppStoreConnectFixture.CURSOR, event.query.get("cursor"));
            await().atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertRows(List.of(appRow("app-a"))));
            awaitCheckpoint(job);
            assertEquals(0, engine.savepointJob(job).getExitCode());
            success(running.get(60, TimeUnit.SECONDS), state);
            await().atMost(15, TimeUnit.SECONDS).until(() -> event.closed);
            state.release = true;
            success(engine.restoreJob(SINGLE, job, args.toArray(new String[0])), state);
            assertRows(List.of(appRow("app-a"), appRow("app-b")));
            assertEquals(
                    1,
                    state.calls.stream().filter(call -> !call.query.containsKey("cursor")).count());
        } finally {
            cleanup(state, job);
        }
    }

    @Test
    void restoresIndependentParentSegmentAndDatedPhases() throws Exception {
        for (String mode : List.of("hold-child", "hold-detail", "hold-download", "hold-dated")) {
            AppStoreConnectFixture.Scenario state = select(mode);
            boolean dated = "hold-dated".equals(mode);
            List<String> args = recovering(dated ? SALES : INSTALL_REPORT);
            String job = jobId();
            CompletableFuture<Container.ExecResult> running = submit(SINGLE, job, args);
            try {
                AppStoreConnectFixture.Event event = held(state);
                awaitCheckpoint(job);
                assertRows(List.of());
                assertEquals(0, engine.savepointJob(job).getExitCode());
                success(running.get(60, TimeUnit.SECONDS), state);
                await().atMost(15, TimeUnit.SECONDS).until(() -> event.closed);
                state.release = true;
                success(engine.restoreJob(SINGLE, job, args.toArray(new String[0])), state);
                assertRows(
                        dated
                                ? List.of(datedRow(SALES, "SALES", fixture.daily.toString(), 1))
                                : List.of(analyticsRow(INSTALL_REPORT, "o", "install", 1)));
            } finally {
                cleanup(state, job);
            }
        }
    }

    @Test
    void restoresExactReportRowOffsetAndFailsIfTheSavedReportChanges() throws Exception {
        AppStoreConnectFixture.Scenario state = select("mid-report");
        String job = jobId();
        String alternate = jobId();
        List<String> fast = defaults(INSTALL_REPORT, 100000);
        CompletableFuture<Container.ExecResult> running =
                submit(SINGLE, job, defaults(INSTALL_REPORT, 2));
        try {
            await().atMost(45, TimeUnit.SECONDS).until(() -> rows().size() >= 4);
            assertEquals(0, engine.savepointJob(job).getExitCode());
            success(running.get(60, TimeUnit.SECONDS), state);
            List<String> saved = rows();
            assertTrue(saved.size() > 0 && saved.size() < 64, "Must save inside one report");
            state.changed = true;
            int mark = logs().length();
            failure(
                    engine.restoreJobWithCheckpoint(SINGLE, job, alternate, fast),
                    mark,
                    "report changed since checkpoint",
                    state);
            assertRows(saved);
            state.changed = false;
            success(engine.restoreJob(SINGLE, job, fast.toArray(new String[0])), state);
            List<String> expected = new ArrayList<>();
            for (int n = 1; n <= 64; n++) {
                expected.add(analyticsRow(INSTALL_REPORT, "o", "install", n));
            }
            assertRows(expected);
            assertEquals(64, new HashSet<>(rows()).size());
        } finally {
            cleanup(state, job, alternate);
        }
    }

    @Test
    void restoresATerminalSourceWithoutAnyProviderRequest() throws Exception {
        AppStoreConnectFixture.Scenario state = select("matrix");
        String job = jobId();
        CompletableFuture<Container.ExecResult> running = submit(TERMINAL, job, defaults(APPS, 24));
        try {
            await().atMost(40, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                assertRows(List.of(appRow("app-a"), appRow("app-b")));
                                assertEquals("RUNNING", engine.getJobStatus(job));
                                assertTrue(engine.completedPipeline(job));
                            });
            assertEquals(0, engine.savepointJob(job).getExitCode());
            success(running.get(60, TimeUnit.SECONDS), state);
            int requests = state.calls.size();
            success(
                    engine.restoreJob(TERMINAL, job, defaults(APPS, 100000).toArray(new String[0])),
                    state);
            assertRows(List.of(appRow("app-a"), appRow("app-b")));
            assertEquals(requests, state.calls.size());
        } finally {
            cleanup(state, job);
        }
    }

    private List<String> matrixRows(AppStoreConnectFixture.Scenario mode) throws Exception {
        List<String> expected =
                new ArrayList<>(
                        List.of(
                                appRow("app-a"),
                                appRow("app-b"),
                                datedRow(
                                        "subscriber_report",
                                        "SUBSCRIBER",
                                        fixture.daily.toString(),
                                        1),
                                datedRow(SALES, "SALES", fixture.daily.toString(), 1),
                                datedRow(
                                        "subscription_report",
                                        "SUBSCRIPTION",
                                        fixture.daily.toString(),
                                        1),
                                datedRow(
                                        "subscription_event_report",
                                        "SUBSCRIPTION_EVENT",
                                        fixture.eventDay.toString(),
                                        1),
                                datedRow(
                                        "finance_report",
                                        "FINANCIAL",
                                        fixture.fiscal.toString(),
                                        1),
                                row(
                                        "customer_reviews_per_app",
                                        "review-now",
                                        "app-a",
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        AppStoreConnectFixture.review(
                                                "review-now", fixture.reviewFrom.plusSeconds(1))),
                                row(
                                        "customer_reviews_per_app",
                                        "review-boundary",
                                        "app-b",
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        AppStoreConnectFixture.review(
                                                "review-boundary", fixture.reviewFrom))));
        for (String scope : List.of("o", "h")) {
            boolean historical = "h".equals(scope);
            String suffix = historical ? "_historical" : "";
            String request = "req-" + scope;
            expected.add(
                    row(
                            "analytics_report_requests_" + (historical ? "historical" : "ongoing"),
                            request,
                            "app-a",
                            request,
                            null,
                            null,
                            null,
                            null,
                            AppStoreConnectFixture.request(
                                    request, historical ? "ONE_TIME_SNAPSHOT" : "ONGOING")));
            for (String family : List.of("install", "download")) {
                boolean installations = "install".equals(family);
                String label =
                        installations ? "analytics_installations_" : "analytics_app_download_";
                String type =
                        installations
                                ? AppStoreConnectFixture.INSTALL
                                : AppStoreConnectFixture.DOWNLOAD;
                String report = "report-" + scope + "-" + family;
                String instance = "ins-" + scope + "-" + family;
                String segment = "seg-" + scope + "-" + family;
                String date = fixture.daily.toString();
                expected.add(
                        row(
                                label + "reports" + suffix,
                                report,
                                "app-a",
                                request,
                                report,
                                null,
                                null,
                                null,
                                AppStoreConnectFixture.report(report, type)));
                expected.add(
                        row(
                                label + "instances" + suffix,
                                instance,
                                "app-a",
                                request,
                                report,
                                instance,
                                null,
                                date,
                                AppStoreConnectFixture.instance(instance, fixture.daily)));
                if (historical) {
                    String old = "old-" + scope + "-" + family;
                    expected.add(
                            row(
                                    label + "instances" + suffix,
                                    old,
                                    "app-a",
                                    request,
                                    report,
                                    old,
                                    null,
                                    fixture.historical.toString(),
                                    AppStoreConnectFixture.instance(old, fixture.historical)));
                }
                for (String stage : List.of("segments", "segment_details")) {
                    expected.add(
                            row(
                                    label + stage + suffix,
                                    segment,
                                    "app-a",
                                    request,
                                    report,
                                    instance,
                                    segment,
                                    date,
                                    fixture.segment(mode, segment, true, false)));
                }
                expected.add(
                        analyticsRow(
                                (installations ? INSTALL_REPORT : "app_download") + suffix,
                                scope,
                                family,
                                1));
            }
        }
        return expected;
    }

    private String analyticsRow(String resource, String scope, String family, int offset) {
        String parent = scope + "-" + family;
        String segment = "seg-" + parent;
        String date = fixture.daily.toString();
        return row(
                resource,
                segment + ":" + offset,
                "app-a",
                "req-" + scope,
                "report-" + parent,
                "ins-" + parent,
                segment,
                date,
                contents(segment, offset, date));
    }

    private static String datedRow(String resource, String code, String date, int offset) {
        return row(
                resource,
                date + ":" + offset,
                null,
                null,
                null,
                null,
                null,
                date,
                contents(code, offset, null));
    }

    private static ObjectNode contents(String label, int offset, String day) {
        ObjectNode data = JSON.createObjectNode();
        if (day != null) {
            data.put("Date", day);
        }
        return data.put("SKU", "000" + offset)
                .put("Amount", "9007199254740993.0100")
                .put("Description", label + " Café 東京\tline\n\"yes\"")
                .put("Empty", "");
    }

    private static String appRow(String id) {
        return row(APPS, id, id, null, null, null, null, null, AppStoreConnectFixture.app(id));
    }

    private static String row(
            String resource,
            String id,
            String app,
            String request,
            String report,
            String instance,
            String segment,
            String date,
            JsonNode payload) {
        return String.join(
                "|",
                resource,
                id,
                app,
                request,
                report,
                instance,
                segment,
                date,
                payload.toString());
    }

    private List<String> defaults(String resource, int rate) {
        return Arrays.asList(
                "fixture_api=" + fixture.apiOrigin(),
                "fixture_issuer=" + AppStoreConnectFixture.ISSUER,
                "fixture_kid=" + AppStoreConnectFixture.KEY_ID,
                "fixture_key=" + fixture.secret,
                "fixture_vendor=" + AppStoreConnectFixture.VENDOR,
                "fixture_sales=" + fixture.daily,
                "fixture_event_start=" + fixture.eventDay,
                "fixture_event_skip=1900-01-01",
                "fixture_fiscal=" + fixture.fiscal,
                "fixture_analytics=" + fixture.daily.minusDays(10),
                "fixture_reviews=" + fixture.reviewFrom,
                "fixture_resource=" + resource,
                "fixture_rate=" + rate,
                "fixture_pages=10",
                "fixture_timeout=5000",
                "fixture_operation=9000",
                "fixture_wire=1048576",
                "fixture_decoded=1048576",
                "fixture_row=65536");
    }

    private List<String> recovering(String resource) {
        return replace(
                replace(defaults(resource, 100000), "fixture_timeout", "65000"),
                "fixture_operation",
                "70000");
    }

    private static List<String> replace(List<String> values, String name, String value) {
        return values.stream()
                .map(argument -> argument.startsWith(name + "=") ? name + "=" + value : argument)
                .collect(Collectors.toList());
    }

    private void rejected(String mode, String resource, String message) throws Exception {
        AppStoreConnectFixture.Scenario state = select(mode);
        int mark = logs().length();
        failure(run(SINGLE, defaults(resource, 100000)), mark, message, state);
        assertRows(List.of());
        assertTrue(state.invalid.isEmpty(), state.invalid.toString());
    }

    private void success(Container.ExecResult result, AppStoreConnectFixture.Scenario mode) {
        assertEquals(0, result.getExitCode());
        noSecrets(logs() + result.getStdout() + result.getStderr(), mode);
    }

    private void failure(
            Container.ExecResult result,
            int mark,
            String message,
            AppStoreConnectFixture.Scenario mode) {
        assertNotEquals(0, result.getExitCode());
        String output = logs().substring(mark) + result.getStdout() + result.getStderr();
        assertTrue(output.contains(message), "Expected sanitized error containing " + message);
        noSecrets(output, mode);
    }

    private void noSecrets(String text, AppStoreConnectFixture.Scenario mode) {
        assertFalse(
                text.contains(fixture.secret)
                        || text.contains(AppStoreConnectFixture.PRIVATE_TEXT)
                        || text.contains(AppStoreConnectFixture.SIGNED)
                        || mode.tokens.stream().anyMatch(text::contains),
                "Provider secret exposed in job output");
    }

    private String logs() {
        return engine.getServerLogs();
    }

    private Container.ExecResult run(String file, List<String> args) throws Exception {
        return engine.executeJob(file, jobId(), args.toArray(new String[0]));
    }

    private static String jobId() {
        return Long.toString(JOBS.incrementAndGet());
    }

    private CompletableFuture<Container.ExecResult> submit(
            String config, String job, List<String> args) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return engine.executeJob(config, job, args.toArray(new String[0]));
                    } catch (Exception ex) {
                        throw new IllegalStateException(
                                "App Store Connect engine command failed", ex);
                    }
                });
    }

    private void awaitCheckpoint(String job) {
        long previous = engine.getCompletedCheckpointCount(job);
        await().atMost(30, TimeUnit.SECONDS)
                .until(() -> engine.getCompletedCheckpointCount(job) > previous);
    }

    private static AppStoreConnectFixture.Event held(AppStoreConnectFixture.Scenario state) {
        await().atMost(45, TimeUnit.SECONDS)
                .until(() -> state.calls.stream().anyMatch(call -> call.held));
        return state.calls.stream().filter(call -> call.held).findFirst().orElseThrow();
    }

    private void cleanup(AppStoreConnectFixture.Scenario state, String... jobs) throws Exception {
        state.release = true;
        for (String job : jobs) {
            String status = engine.getJobStatus(job);
            if ("RUNNING".equals(status)
                    || "PENDING".equals(status)
                    || "RESTARTING".equals(status)) {
                engine.cancelJob(job);
            }
        }
    }

    private AppStoreConnectFixture.Scenario select(String mode) throws Exception {
        try (Connection connection = db();
                Statement sql = connection.createStatement()) {
            sql.execute("TRUNCATE appstore_rows");
        }
        return fixture.select(mode);
    }

    private Connection db() throws Exception {
        return DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }

    private List<String> rows() throws Exception {
        List<String> result = new ArrayList<>();
        try (Connection connection = db();
                Statement sql = connection.createStatement();
                ResultSet returned =
                        sql.executeQuery(
                                "SELECT resource, id, app_id, request_id, "
                                        + "report_id, instance_id, segment_id, report_date, record FROM appstore_rows")) {
            while (returned.next()) {
                List<String> fields = new ArrayList<>();
                for (int i = 1; i <= 9; i++) {
                    fields.add(returned.getString(i));
                }
                result.add(String.join("|", fields));
            }
        }
        Collections.sort(result);
        return result;
    }

    private void assertRows(List<String> expected) throws Exception {
        List<String> sorted = new ArrayList<>(expected);
        Collections.sort(sorted);
        assertEquals(sorted, rows());
    }

    /**
     * Uses the real Zeta container while preserving argv boundaries for job variables.
     *
     * <p>The normal shell wrapper would pass the synthetic key through its command log.
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
            try (InputStream input = connection.getInputStream()) {
                for (JsonNode pipeline : JSON.readTree(input).path("pipelines")) {
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
