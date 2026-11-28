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
package org.apache.seatunnel.e2e.connector.appleads;

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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.seatunnel.e2e.connector.appleads.AppleAdsFixture.V1;
import static org.apache.seatunnel.e2e.connector.appleads.AppleAdsFixture.V5;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies Apple Ads v1 and v5 through their production Factory and packaged Zeta.
 *
 * <p>Wire scope is asserted by an independent HTTPS peer, and source rows are compared from an
 * actual JDBC sink across normal, retry, fail and restore paths.
 */
class AppleAdsSourceIT extends TestSuiteBase {
    private static final String CAMPAIGNS = "campaigns";
    private static final String CAMPAIGN_REPORT = "campaigns_report_daily";
    private static final String SINGLE = "/appleads_platform_single.conf";
    private static final String LEGACY = "/appleads_legacy_single.conf";
    private static final String ZONED = "/appleads_platform_ortz.conf";
    private static final String TERMINAL = "/appleads_platform_terminal.conf";
    private static final ObjectMapper JSON = AppleAdsFixture.JSON;
    private static final AtomicLong JOBS = new AtomicLong(System.currentTimeMillis());

    @TempDir private static Path temporary;
    private AppleAdsFixture fixture;
    private PostgreSQLContainer<?> postgres;
    private Engine engine;

    @BeforeAll
    void startFixtures() throws Exception {
        fixture = new AppleAdsFixture(temporary);
        Path debug = temporary.resolve("apple-ads-debug.properties");
        Files.writeString(
                debug,
                "appender.console.type=Console\nappender.console.name=OUT\n"
                        + "appender.console.layout.type=PatternLayout\n"
                        + "appender.console.layout.pattern=%p %c %m%n\n"
                        + "rootLogger.level=info\nrootLogger.appenderRef.out.ref=OUT\n"
                        + "logger.http.name=org.apache.http\nlogger.http.level=debug\n");
        postgres =
                new PostgreSQLContainer<>(DockerImageName.parse("postgres:14-alpine"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("apple-ads-db")
                        .withDatabaseName("test")
                        .withUsername("test")
                        .withPassword("test");
        postgres.start();
        try (Connection connection = db();
                Statement sql = connection.createStatement()) {
            sql.execute(
                    "CREATE TABLE apple_ads_rows (resource TEXT, api_mode TEXT, account_id TEXT, "
                            + "entity_id TEXT, campaign_id TEXT, ad_group_id TEXT, report_date TEXT, "
                            + "country_or_region TEXT, record TEXT)");
        }
        Path driver = DependencyJar.of(Driver.class).path();
        engine =
                new Engine() {
                    @Override
                    protected void executeExtraCommands(GenericContainer<?> container) {
                        container.withCopyFileToContainer(
                                MountableFile.forHostPath(fixture.trust),
                                "/tmp/apple-ads-trust.jks");
                        container.withCopyFileToContainer(
                                MountableFile.forHostPath(debug),
                                "/tmp/apple-ads-debug.properties");
                        container.withCopyFileToContainer(
                                MountableFile.forHostPath(driver),
                                "/tmp/seatunnel/plugins/Jdbc/lib/postgresql.jar");
                    }

                    @Override
                    protected String getJavaToolOptions() {
                        return "-Djavax.net.ssl.trustStore=/tmp/apple-ads-trust.jks -Djavax.net.ssl.trustStorePassword="
                                + AppleAdsFixture.PASSWORD
                                + " -Dlog4j2.configurationFile=/tmp/apple-ads-debug.properties";
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
    void readsAllEightResourcesWithTheCorrectVersionAndBothDailyResponseShapes() throws Exception {
        for (String version : List.of(V1, V5)) {
            AppleAdsFixture.Scenario state = select(version, "matrix");
            String file =
                    V1.equals(version)
                            ? "/appleads_platform_catalog.conf"
                            : "/appleads_legacy_catalog.conf";
            success(run(file, args(CAMPAIGNS, 100000)), state);
            List<String> expected = entities(version, null);
            expected.addAll(reports(state, null, fixture.first, fixture.last, 2));
            assertRows(expected);
            assertEquals(
                    8,
                    rows().stream()
                            .map(row -> row.substring(0, row.indexOf('|')))
                            .distinct()
                            .count());
            assertEquals(1, state.tokens.get());
            assertTrue(state.events.stream().filter(e -> !e.oauth).allMatch(e -> e.parsed != null));
            assertTrue(queries(state).stream().anyMatch(q -> q.report && q.campaign == 102));
            assertTrue(queries(state).stream().anyMatch(q -> q.report && q.first.equals(q.last)));
            assertTrue(
                    queries(state).stream()
                            .anyMatch(q -> !q.report && q.group == 301 && q.offset == 2));
        }
    }

    @Test
    void discoversFullParentsEvenForAnExplicitChildAndWhenLegacyAdsReturnsAll() throws Exception {
        for (String version : List.of(V1, V5)) {
            String mode = V1.equals(version) ? "matrix" : "legacy-ad-list";
            AppleAdsFixture.Scenario state = select(version, mode);
            success(run(config(version), args("ads", 100000)), state);
            assertRows(entities(version, "ads"));
            assertTrue(
                    queries(state).stream()
                            .anyMatch(q -> "campaigns".equals(q.dataset) && q.offset == 2));
            assertTrue(
                    queries(state).stream()
                            .anyMatch(q -> "adgroups".equals(q.dataset) && q.offset == 2));
            assertTrue(
                    queries(state).stream()
                            .anyMatch(q -> "adgroups".equals(q.dataset) && q.campaign == 103));
        }
    }

    @Test
    void usesTheDefaultPlatformCalendarSingleDayAndExplicitOrtZLookback() throws Exception {
        AppleAdsFixture.Scenario single = select(V1, "matrix");
        List<String> one =
                replace(args("ads_report_daily", 100000), "fixture_end", fixture.first.toString());
        success(run(SINGLE, one), single);
        assertRows(reports(single, "ads", fixture.first, fixture.first, 2));
        assertTrue(
                queries(single).stream()
                        .filter(q -> q.report)
                        .allMatch(q -> q.first.equals(q.last) && "UTC".equals(q.timezone)));
        assertTrue(
                rows().stream()
                        .anyMatch(
                                row -> row.contains("|2301|101|301|" + fixture.first + "|null|")));

        AppleAdsFixture.Scenario recalculated = select(V1, "matrix");
        success(run(ZONED, args(CAMPAIGN_REPORT, 100000)), recalculated);
        LocalDate inclusive = fixture.first.plusDays(1);
        assertRows(reports(recalculated, "campaigns", inclusive, fixture.last, 2));
        assertTrue(
                queries(recalculated).stream()
                        .allMatch(
                                q ->
                                        "ORTZ".equals(q.timezone)
                                                && inclusive.equals(q.first)
                                                && fixture.last.equals(q.last)));
    }

    @Test
    void handlesEmptyPartitionsAndTraversesTheThousandEntityPageBoundary() throws Exception {
        for (String version : List.of(V1, V5)) {
            AppleAdsFixture.Scenario empty = select(version, "empty");
            success(run(config(version), args(CAMPAIGN_REPORT, 100000)), empty);
            assertRows(List.of());

            AppleAdsFixture.Scenario big = select(version, "big-page");
            success(
                    run(
                            config(version),
                            replace(args(CAMPAIGNS, 100000), "fixture_page_size", "1000")),
                    big);
            assertEquals(
                    List.of(0, 1000),
                    queries(big).stream().map(q -> q.offset).collect(Collectors.toList()));
            List<String> expected = new ArrayList<>();
            for (long id = 1; id <= 1001; id++) {
                expected.add(entityRow(version, CAMPAIGNS, id, 0, 0));
            }
            assertRows(expected);
        }
    }

    @Test
    void rejectsWrongPageCountersChangedTotalsAndIncompleteReportPages() throws Exception {
        for (String version : List.of(V1, V5)) {
            reject(
                    version,
                    "short",
                    CAMPAIGNS,
                    "empty, short or oversized page contradicts pagination totals");
            reject(
                    version,
                    "report-short",
                    CAMPAIGN_REPORT,
                    "empty, short or oversized page contradicts pagination totals");
            reject(
                    version,
                    "scope",
                    CAMPAIGNS,
                    "response row does not belong to the requested account or parent");
            reject(
                    version,
                    "bad-date",
                    CAMPAIGN_REPORT,
                    "daily report metric is outside the requested window");

            AppleAdsFixture.Scenario repeated = select(version, "repeat-offset");
            int mark = logs().length();
            fail(
                    run(config(version), args(CAMPAIGNS, 100000)),
                    mark,
                    "pagination counters disagree with the requested offset or size",
                    repeated);
            assertEquals(2, queries(repeated).size());
            AppleAdsFixture.Scenario changed = select(version, "changed-total");
            mark = logs().length();
            fail(
                    run(config(version), args(CAMPAIGNS, 100000)),
                    mark,
                    "pagination total changed within the parent or report window",
                    changed);
            assertEquals(2, queries(changed).size());

            AppleAdsFixture.Scenario limited = select(version, "matrix");
            mark = logs().length();
            fail(
                    run(
                            config(version),
                            replace(args(CAMPAIGNS, 100000), "fixture_max_pages", "1")),
                    mark,
                    "pagination did not advance or exceeded max_pages_per_partition",
                    limited);
            assertEquals(1, queries(limited).size());
        }
    }

    @Test
    void renewsExpiredAndUnauthorizedTokensWithoutCrossingOriginOrVersion() throws Exception {
        for (String version : List.of(V1, V5)) {
            for (String mode : List.of("ttl", "resign")) {
                AppleAdsFixture.Scenario renewed = select(version, mode);
                success(run(config(version), args(CAMPAIGNS, 100000)), renewed);
                assertRows(entities(version, CAMPAIGNS));
                assertEquals(2, renewed.tokens.get());
                assertTrue(
                        renewed.events.stream()
                                .filter(e -> e.oauth)
                                .allMatch(e -> e.authorization == null && e.context == null));
            }
            AppleAdsFixture.Scenario denied = select(version, "unauthorized");
            int mark = logs().length();
            fail(
                    run(config(version), args(CAMPAIGNS, 100000)),
                    mark,
                    "HTTP 401 after renewing the Apple access token",
                    denied);
            assertEquals(2, denied.calls.get());
            assertEquals(2, denied.tokens.get());
            assertRows(List.of());
        }
        reject(V1, "oauth-denied", CAMPAIGNS, "Apple OAuth exchange returned HTTP 400");
        reject(V1, "oauth-drop", CAMPAIGNS, "Apple OAuth exchange outcome is unknown");
        assertEquals(1, fixtureScenario.tokens.get());
        reject(V1, "bad-token", CAMPAIGNS, "invalid Apple OAuth token response");
        assertEquals(0, fixtureScenario.calls.get());
    }

    @Test
    void retriesReadOnlyPostsAndHonorsBoundedRetryAfter() throws Exception {
        for (String version : List.of(V1, V5)) {
            AppleAdsFixture.Scenario recoverable = select(version, "retry");
            success(run(config(version), args(CAMPAIGN_REPORT, 100000)), recoverable);
            assertRows(reports(recoverable, CAMPAIGNS, fixture.first, fixture.last, 2));
            assertEquals(5, recoverable.calls.get());
            reject(
                    version,
                    "retry-budget",
                    CAMPAIGN_REPORT,
                    "retry or pacing delay exceeds the operation deadline");
            assertEquals(1, fixtureScenario.calls.get());
        }
    }

    @Test
    void failsInvalidRetentionPermissionRedirectAndTlsWithoutCredentialLeakage() throws Exception {
        for (String version : List.of(V1, V5)) {
            reject(version, "retention", CAMPAIGN_REPORT, "Apple Ads API returned HTTP 400");
            reject(version, "forbidden", CAMPAIGNS, "Apple Ads API returned HTTP 403");
            reject(version, "api-redirect", CAMPAIGNS, "Apple Ads API returned HTTP 307");
            assertEquals(2, fixtureScenario.events.size());
        }
        reject(V1, "oauth-redirect", CAMPAIGNS, "Apple OAuth exchange returned HTTP 302");
        assertEquals(1, fixtureScenario.events.size());

        AppleAdsFixture.Scenario invalid = select(V1, "matrix");
        int mark = logs().length();
        fail(
                run(SINGLE, replace(args(CAMPAIGNS, 100000), "fixture_api", fixture.wrong())),
                mark,
                "HTTPS certificate verification or negotiation failed",
                invalid);
        assertEquals(0, invalid.calls.get());

        AppleAdsFixture.Scenario old = select(V1, "matrix");
        mark = logs().length();
        fail(
                run(
                        SINGLE,
                        replace(
                                args(CAMPAIGN_REPORT, 100000),
                                "fixture_start",
                                fixture.first.minusDays(100).toString())),
                mark,
                "Invalid AppleAds option start_date",
                old);
        assertTrue(old.events.isEmpty());
    }

    @Test
    void validatesGzipAndRejectsOversizedOrMalformedResponseData() throws Exception {
        AppleAdsFixture.Scenario compressed = select(V1, "gzip");
        success(run(SINGLE, args(CAMPAIGNS, 100000)), compressed);
        assertRows(entities(V1, CAMPAIGNS));
        reject(V1, "duplicate-json", CAMPAIGNS, "invalid JSON response");
        reject(V5, "api-errors", CAMPAIGNS, "response contained provider errors");
        for (String mode : List.of("expanded-body", "wire-body")) {
            AppleAdsFixture.Scenario large = select(V1, mode);
            int mark = logs().length();
            fail(
                    run(SINGLE, replace(args(CAMPAIGNS, 100000), "fixture_limit", "1024")),
                    mark,
                    ("wire-body".equals(mode) ? "wire" : "decoded")
                            + " response exceeds max_response_bytes",
                    large);
            assertRows(List.of());
        }
    }

    @Test
    void boundsStalledAndTrickleReadsAndCancelsAnActiveParentQuery() throws Exception {
        for (String mode : List.of("stall", "trickle")) {
            AppleAdsFixture.Scenario stalled = select(V1, mode);
            try {
                List<String> timeout = replace(args(CAMPAIGNS, 100000), "fixture_timeout", "2200");
                timeout = replace(timeout, "fixture_operation", "2200");
                int mark = logs().length();
                fail(run(SINGLE, timeout), mark, "HTTPS operation deadline exceeded", stalled);
                assertRows(List.of());
            } finally {
                stalled.release = true;
            }
        }
        AppleAdsFixture.Scenario held = select(V1, "hold-parent");
        String job = jobId();
        CompletableFuture<Container.ExecResult> running = submit(SINGLE, job, recovery("ads"));
        try {
            AppleAdsFixture.Event connection = held(held);
            awaitCheckpoint(job);
            assertEquals(0, engine.cancelJob(job).getExitCode());
            await().atMost(30, TimeUnit.SECONDS)
                    .until(() -> "CANCELED".equals(engine.getJobStatus(job)));
            running.get(45, TimeUnit.SECONDS);
            await().atMost(15, TimeUnit.SECONDS).until(() -> connection.closed);
            assertRows(List.of());
        } finally {
            cleanup(held, job);
        }
    }

    @Test
    void restoresBeforeTokenAndRejectsAStaleModeOrAccountCheckpoint() throws Exception {
        AppleAdsFixture.Scenario state = select(V1, "hold-token");
        String job = jobId();
        String legacy = jobId();
        String account = jobId();
        List<String> options = recovery(CAMPAIGNS);
        CompletableFuture<Container.ExecResult> running = submit(SINGLE, job, options);
        try {
            AppleAdsFixture.Event connection = held(state);
            awaitCheckpoint(job);
            assertRows(List.of());
            assertEquals(0, engine.savepointJob(job).getExitCode());
            success(running.get(60, TimeUnit.SECONDS), state);
            await().atMost(15, TimeUnit.SECONDS).until(() -> connection.closed);
            int mark = logs().length();
            fail(
                    engine.restoreJobWithCheckpoint(LEGACY, job, legacy, options),
                    mark,
                    "checkpoint has incompatible scan options or state",
                    state);
            mark = logs().length();
            List<String> changed = replace(options, "fixture_account", "77881");
            fail(
                    engine.restoreJobWithCheckpoint(SINGLE, job, account, changed),
                    mark,
                    "checkpoint has incompatible scan options or state",
                    state);
            assertEquals(1, state.events.size());
            state.release = true;
            success(engine.restoreJob(SINGLE, job, options.toArray(new String[0])), state);
            assertRows(entities(V1, CAMPAIGNS));
        } finally {
            cleanup(state, job, legacy, account);
        }
    }

    @Test
    void restoresLegacyPageOffsetWithoutReplayingEarlierEntities() throws Exception {
        AppleAdsFixture.Scenario state = select(V5, "hold-page");
        String job = jobId();
        List<String> options = recovery(CAMPAIGNS);
        CompletableFuture<Container.ExecResult> running = submit(LEGACY, job, options);
        try {
            AppleAdsFixture.Event connection = held(state);
            assertEquals(2, connection.parsed.offset);
            await().atMost(25, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    assertRows(
                                            List.of(
                                                    entityRow(V5, CAMPAIGNS, 101, 0, 0),
                                                    entityRow(V5, CAMPAIGNS, 102, 0, 0))));
            awaitCheckpoint(job);
            assertEquals(0, engine.savepointJob(job).getExitCode());
            success(running.get(60, TimeUnit.SECONDS), state);
            await().atMost(15, TimeUnit.SECONDS).until(() -> connection.closed);
            state.release = true;
            success(engine.restoreJob(LEGACY, job, options.toArray(new String[0])), state);
            assertRows(entities(V5, CAMPAIGNS));
            assertEquals(1, queries(state).stream().filter(q -> q.offset == 0).count());
        } finally {
            cleanup(state, job);
        }
    }

    @Test
    void resumesIndependentReportParentDateWindowAndPagination() throws Exception {
        for (String mode : List.of("hold-report-page", "hold-window", "hold-report-parent")) {
            AppleAdsFixture.Scenario state = select(V1, mode);
            String dataset = mode.equals("hold-report-parent") ? "keywords" : CAMPAIGNS;
            String job = jobId();
            List<String> options = recovery(dataset + "_report_daily");
            CompletableFuture<Container.ExecResult> running = submit(SINGLE, job, options);
            try {
                AppleAdsFixture.Event connection = held(state);
                await().atMost(30, TimeUnit.SECONDS).until(() -> !rows().isEmpty());
                awaitCheckpoint(job);
                assertEquals(0, engine.savepointJob(job).getExitCode());
                success(running.get(60, TimeUnit.SECONDS), state);
                await().atMost(15, TimeUnit.SECONDS).until(() -> connection.closed);
                state.release = true;
                success(engine.restoreJob(SINGLE, job, options.toArray(new String[0])), state);
                assertRows(reports(state, dataset, fixture.first, fixture.last, 2));
            } finally {
                cleanup(state, job);
            }
        }
    }

    @Test
    void restoresWithinOneReportPageAndRejectsChangedReportContent() throws Exception {
        AppleAdsFixture.Scenario state = select(V1, "long-report");
        String job = jobId();
        String changed = jobId();
        List<String> slow = replace(args(CAMPAIGN_REPORT, 2), "fixture_page_size", "1000");
        List<String> fast = replace(slow, "fixture_rate", "100000");
        CompletableFuture<Container.ExecResult> running = submit(SINGLE, job, slow);
        try {
            await().atMost(45, TimeUnit.SECONDS).until(() -> rows().size() >= 4);
            assertEquals(0, engine.savepointJob(job).getExitCode());
            success(running.get(60, TimeUnit.SECONDS), state);
            List<String> saved = rows();
            assertTrue(
                    saved.size() > 0 && saved.size() < 128,
                    "Savepoint must cut one provider report page");
            state.changed = true;
            int mark = logs().length();
            fail(
                    engine.restoreJobWithCheckpoint(SINGLE, job, changed, fast),
                    mark,
                    "saved page changed since checkpoint",
                    state);
            assertRows(saved);
            state.changed = false;
            success(engine.restoreJob(SINGLE, job, fast.toArray(new String[0])), state);
            assertRows(reports(state, CAMPAIGNS, fixture.first, fixture.last, 2));
            assertEquals(192, new HashSet<>(rows()).size());
        } finally {
            cleanup(state, job, changed);
        }
    }

    @Test
    void resumesACompletedSplitWithoutIssuingAnotherTokenOrQuery() throws Exception {
        AppleAdsFixture.Scenario state = select(V1, "matrix");
        String job = jobId();
        CompletableFuture<Container.ExecResult> running =
                submit(TERMINAL, job, args(CAMPAIGNS, 24));
        try {
            await().atMost(45, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                assertRows(entities(V1, CAMPAIGNS));
                                assertEquals("RUNNING", engine.getJobStatus(job));
                                assertTrue(engine.completedPipeline(job));
                            });
            assertEquals(0, engine.savepointJob(job).getExitCode());
            success(running.get(60, TimeUnit.SECONDS), state);
            int calls = state.events.size();
            success(
                    engine.restoreJob(
                            TERMINAL, job, args(CAMPAIGNS, 100000).toArray(new String[0])),
                    state);
            assertRows(entities(V1, CAMPAIGNS));
            assertEquals(calls, state.events.size());
        } finally {
            cleanup(state, job);
        }
    }

    private static List<String> entities(String version, String selection) {
        List<String> expected = new ArrayList<>();
        for (String dataset : AppleAdsFixture.DATASETS) {
            if (selection != null && !selection.equals(dataset)) {
                continue;
            }
            for (long campaign : AppleAdsFixture.CAMPAIGN_IDS) {
                if (dataset.equals(CAMPAIGNS)) {
                    expected.add(entityRow(version, dataset, campaign, 0, 0));
                    continue;
                }
                for (long group : AppleAdsFixture.groups(campaign)) {
                    if (dataset.equals("adgroups")) {
                        expected.add(entityRow(version, dataset, group, campaign, 0));
                    } else if (dataset.equals("ads")) {
                        expected.add(entityRow(version, dataset, group + 2000, campaign, group));
                    } else {
                        expected.add(entityRow(version, dataset, group + 1000, campaign, group));
                        if (group == 301) {
                            expected.add(entityRow(version, dataset, 3301, campaign, group));
                            expected.add(entityRow(version, dataset, 4301, campaign, group));
                        }
                    }
                }
            }
        }
        return expected;
    }

    private static List<String> reports(
            AppleAdsFixture.Scenario state,
            String selection,
            LocalDate first,
            LocalDate last,
            int window) {
        List<String> expected = new ArrayList<>();
        boolean platform = V1.equals(state.version);
        for (String dataset : AppleAdsFixture.DATASETS) {
            if (selection != null && !selection.equals(dataset)) {
                continue;
            }
            List<Long> parents =
                    dataset.equals(CAMPAIGNS) ? List.of(0L) : AppleAdsFixture.CAMPAIGN_IDS;
            for (long parent : parents) {
                AppleAdsFixture.Query query =
                        new AppleAdsFixture.Query(
                                platform, true, dataset, parent, 0, 0, 1000, first, last, "UTC");
                for (long id : AppleAdsFixture.ids(state, query)) {
                    for (LocalDate day = first; !day.isAfter(last); day = day.plusDays(1)) {
                        long remainder = ChronoUnit.DAYS.between(day, last) + 1;
                        boolean one =
                                platform && window == 1
                                        || platform
                                                && remainder == 1
                                                && (ChronoUnit.DAYS.between(first, day) % window
                                                        == 0);
                        String country = one && dataset.equals("ads") && id == 2301 ? null : "US";
                        expected.add(reportRow(state, query, id, day, one, country));
                        if (platform && !one && id == 101 && !"long-report".equals(state.mode)) {
                            expected.add(reportRow(state, query, id, day, false, "GB"));
                        }
                    }
                }
            }
        }
        return expected;
    }

    private static String entityRow(
            String version, String dataset, long id, long campaign, long group) {
        boolean platform = V1.equals(version);
        return row(
                version,
                dataset,
                id,
                dataset.equals(CAMPAIGNS) ? id : campaign,
                dataset.equals("adgroups") ? id : group,
                null,
                null,
                AppleAdsFixture.entity(platform, dataset, id, campaign, group));
    }

    private static String reportRow(
            AppleAdsFixture.Scenario state,
            AppleAdsFixture.Query query,
            long id,
            LocalDate day,
            boolean single,
            String country) {
        long group = AppleAdsFixture.reportGroup(query.dataset, id);
        ObjectNode out = JSON.createObjectNode();
        out.set(
                "metadata",
                AppleAdsFixture.metadata(query.platform, query.dataset, query.campaign, group, id));
        out.set(
                "metrics",
                AppleAdsFixture.metrics(
                        state, id, single ? null : day, query.platform ? country : null));
        return row(
                state.version,
                query.dataset + "_report_daily",
                id,
                query.dataset.equals(CAMPAIGNS) ? id : query.campaign,
                group,
                day,
                country,
                out);
    }

    private static String row(
            String version,
            String dataset,
            long id,
            long campaign,
            long group,
            LocalDate day,
            String country,
            JsonNode record) {
        return String.join(
                "|",
                dataset,
                version,
                V1.equals(version) ? AppleAdsFixture.ACCOUNT : AppleAdsFixture.ORG,
                Long.toString(id),
                campaign == 0 ? null : Long.toString(campaign),
                group == 0 ? null : Long.toString(group),
                day == null ? null : day.toString(),
                country,
                record.toString());
    }

    private List<String> args(String resource, int rate) {
        return Arrays.asList(
                "fixture_api=" + fixture.api(),
                "fixture_oauth=" + fixture.oauth(),
                "fixture_client=" + AppleAdsFixture.CLIENT,
                "fixture_secret=" + AppleAdsFixture.SECRET,
                "fixture_account=" + AppleAdsFixture.ACCOUNT,
                "fixture_org=" + AppleAdsFixture.ORG,
                "fixture_resource=" + resource,
                "fixture_start=" + fixture.first,
                "fixture_end=" + fixture.last,
                "fixture_cursor=" + fixture.last,
                "fixture_rate=" + rate,
                "fixture_page_size=2",
                "fixture_window=2",
                "fixture_max_pages=10",
                "fixture_timeout=6000",
                "fixture_operation=9000",
                "fixture_limit=1048576");
    }

    private List<String> recovery(String resource) {
        return replace(
                replace(args(resource, 100000), "fixture_timeout", "65000"),
                "fixture_operation",
                "70000");
    }

    private static List<String> replace(List<String> values, String key, String replacement) {
        return values.stream()
                .map(value -> value.startsWith(key + "=") ? key + "=" + replacement : value)
                .collect(Collectors.toList());
    }

    private static String config(String version) {
        return V1.equals(version) ? SINGLE : LEGACY;
    }

    private void reject(String version, String mode, String resource, String diagnostic)
            throws Exception {
        AppleAdsFixture.Scenario scenario = select(version, mode);
        int mark = logs().length();
        fail(run(config(version), args(resource, 100000)), mark, diagnostic, scenario);
        assertRows(List.of());
    }

    private void success(Container.ExecResult result, AppleAdsFixture.Scenario scenario) {
        assertEquals(0, result.getExitCode());
        noSecrets(logs() + result.getStdout() + result.getStderr());
        assertTrue(scenario.errors.isEmpty(), scenario.errors.toString());
    }

    private void fail(
            Container.ExecResult result,
            int mark,
            String diagnostic,
            AppleAdsFixture.Scenario scenario) {
        assertNotEquals(0, result.getExitCode());
        String text = logs().substring(mark) + result.getStdout() + result.getStderr();
        assertTrue(
                text.contains(diagnostic),
                "Expected a sanitized diagnostic containing " + diagnostic);
        noSecrets(text);
        assertTrue(scenario.errors.isEmpty(), scenario.errors.toString());
    }

    private static void noSecrets(String text) {
        assertFalse(
                text.contains(AppleAdsFixture.SECRET)
                        || text.contains(AppleAdsFixture.ACCESS_PREFIX)
                        || text.contains(AppleAdsFixture.PRIVATE_DETAIL),
                "Apple credential or error payload leaked");
    }

    private String logs() {
        return engine.getServerLogs();
    }

    private Container.ExecResult run(String config, List<String> options) throws Exception {
        return engine.executeJob(config, jobId(), options.toArray(new String[0]));
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
                        throw new IllegalStateException("Apple Ads engine command failed", ex);
                    }
                });
    }

    private void awaitCheckpoint(String job) {
        long previous = engine.getCompletedCheckpointCount(job);
        await().atMost(30, TimeUnit.SECONDS)
                .until(() -> engine.getCompletedCheckpointCount(job) > previous);
    }

    private static List<AppleAdsFixture.Query> queries(AppleAdsFixture.Scenario scenario) {
        return scenario.events.stream()
                .filter(event -> event.parsed != null)
                .map(event -> event.parsed)
                .collect(Collectors.toList());
    }

    private static AppleAdsFixture.Event held(AppleAdsFixture.Scenario state) {
        await().atMost(45, TimeUnit.SECONDS)
                .until(() -> state.events.stream().anyMatch(call -> call.held));
        return state.events.stream().filter(call -> call.held).findFirst().orElseThrow();
    }

    private void cleanup(AppleAdsFixture.Scenario state, String... jobs) throws Exception {
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

    private AppleAdsFixture.Scenario fixtureScenario;

    private AppleAdsFixture.Scenario select(String version, String mode) throws Exception {
        try (Connection connection = db();
                Statement sql = connection.createStatement()) {
            sql.execute("TRUNCATE apple_ads_rows");
        }
        fixtureScenario = fixture.select(version, mode);
        return fixtureScenario;
    }

    private Connection db() throws Exception {
        return DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }

    private List<String> rows() throws Exception {
        List<String> result = new ArrayList<>();
        try (Connection connection = db();
                Statement sql = connection.createStatement();
                ResultSet rows =
                        sql.executeQuery(
                                "SELECT resource, api_mode, account_id, entity_id, campaign_id, "
                                        + "ad_group_id, report_date, country_or_region, record FROM apple_ads_rows")) {
            while (rows.next()) {
                List<String> fields = new ArrayList<>();
                for (int i = 1; i <= 9; i++) {
                    fields.add(rows.getString(i));
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
     * Runs real Zeta commands with explicit argv boundaries for the synthetic secrets.
     *
     * <p>The outer test-container shell logger would otherwise format request variable values.
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
