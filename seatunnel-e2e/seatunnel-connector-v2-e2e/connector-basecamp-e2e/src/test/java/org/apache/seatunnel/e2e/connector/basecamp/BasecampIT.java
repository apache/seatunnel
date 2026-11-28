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

package org.apache.seatunnel.e2e.connector.basecamp;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.basecamp.BasecampRecoveryProbe;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.seatunnel.SeaTunnelContainer;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs provider-contract scenarios through the real Zeta engine and transactional JSON files.
 *
 * <p>The independent Java HTTPS peer verifies every path, query and credential header. Assertions
 * inspect committed rows rather than diagnostic console output or connector internals.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BasecampIT extends TestSuiteBase {
    private static final String ONE = "/basecamp-one.conf";
    private static final String ALL = "/basecamp-all.conf";
    private static final String OAUTH = "/basecamp-oauth.conf";
    private static final String TERMINAL = "/basecamp-terminal.conf";
    private static final ObjectMapper JSON =
            new ObjectMapper()
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    @TempDir static Path directory;
    private static String trust;
    private static String untrusted;
    private final ExecutorService tasks = Executors.newCachedThreadPool();
    private Engine engine;
    private Future<Container.ExecResult> peer;
    private String active;

    @BeforeAll
    void startEngineAndIndependentHttpsPeer() throws Exception {
        KeyStore trusted = identity("trusted");
        KeyStore other = identity("untrusted");
        untrusted = Base64.getEncoder().encodeToString(other.getCertificate("api").getEncoded());
        trust = Base64.getEncoder().encodeToString(trusted.getCertificate("api").getEncoded());
        String certificate =
                Base64.getMimeEncoder(64, new byte[] {'\n'})
                        .encodeToString(trusted.getCertificate("api").getEncoded());
        Files.writeString(
                directory.resolve("trusted.pem"),
                "-----BEGIN CERTIFICATE-----\n" + certificate + "\n-----END CERTIFICATE-----\n");
        engine = new Engine(directory);
        engine.startUp();
        peer = tasks.submit(engine::runPeer);
        Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> engine.exists("ready"));
    }

    @AfterEach
    void rejectsUnexpectedProviderTraffic() throws Exception {
        if (active != null) {
            assertFalse(engine.exists(active + ".invalid"), active);
        }
    }

    @AfterAll
    void stopOwnedPeerAndEngine() throws Exception {
        try {
            if (engine != null) {
                try {
                    if (peer != null) {
                        engine.touch("stop");
                        success(peer.get(20, TimeUnit.SECONDS));
                    }
                } finally {
                    engine.tearDown();
                }
            }
        } finally {
            tasks.shutdownNow();
        }
    }

    private static final String PROJECTS = "/123456/projects.json";
    private static final String RECORDINGS = "/123456/projects/recordings.json";
    private static final String EXTRA =
            ",\"unknown\":{\"unicode\":\"数据🌏\",\"null\":null,\"items\":[false,null,[1,2]],\"integer\":9223372036854775808,\"decimal\":123456789012345678.123456789},\"content\":\"<p>Read &amp; preserve</p>\"";

    @Test
    void readsAllFiveResourcesAndKeepsDisabledDockMetadataWithoutReadingItsEntries()
            throws Exception {
        String name = "all-five", first = project("101"), second = project("102");
        String enabled = dock("201", true).replace(",\"enabled\":true", ""),
                disabled = dock("202", false);
        String entry = record("501", 1),
                todo = feed("601", 1, "101"),
                message = feed("701", 2, null);
        program(
                name,
                List.of(
                        ok(PROJECTS, "", array(first, second)),
                        ok(
                                "/123456/projects/101.json",
                                "",
                                details(
                                        "101",
                                        enabled,
                                        disabled,
                                        "{\"id\":301,\"name\":\"message_board\"}")),
                        ok("/123456/buckets/101/schedules/201/entries.json", "", array(entry)),
                        ok("/123456/projects/102.json", "", details("102")),
                        ok(RECORDINGS, feedQuery("Todo"), array(todo)),
                        ok(RECORDINGS, feedQuery("Message"), array(message))));
        success(run(ALL, new Settings(name, "projects")));
        assertRows(
                name,
                Map.of(
                        key("projects", "101", null, null), expected(null, null, date(1), first),
                        key("projects", "102", null, null), expected(null, null, date(1), second),
                        key("schedules", "201", "101", "201"),
                                expected("101", "201", null, schedule(enabled, "101")),
                        key("schedules", "202", "101", "202"),
                                expected("101", "202", null, schedule(disabled, "101")),
                        key("schedule_entries", "501", "101", "201"),
                                expected("101", "201", date(1), entry),
                        key("todos", "601", "101", null), expected("101", null, date(1), todo),
                        key("messages", "701", null, null),
                                expected(null, null, date(2), message)));
        assertEquals(6, requests(name).size());
    }

    @Test
    void discoversTheFullParentChainWhenOnlyScheduleEntriesAreSelected() throws Exception {
        String name = "child-only",
                first = project("101"),
                second = project("102"),
                entry = record("501", 1);
        program(
                name,
                List.of(
                        next(ok(PROJECTS, "", array(first)), "<?page=2>; rel=\"next\""),
                        ok("/123456/projects/101.json", "", details("101", dock("201", true))),
                        ok("/123456/buckets/101/schedules/201/entries.json", "", array(entry)),
                        ok(PROJECTS, "page=2", array(second)),
                        ok("/123456/projects/102.json", "", details("102"))));
        success(run(ONE, new Settings(name, "schedule_entries")));
        assertRows(
                name,
                Map.of(
                        key("schedule_entries", "501", "101", "201"),
                        expected("101", "201", date(1), entry)));
        assertEquals(5, requests(name).size());
    }

    @Test
    void followsTheNextRelationWhenCommasAppearInsideLinksAndQuotedParameters() throws Exception {
        String name = "quoted-links", first = project("101"), second = project("102");
        String link =
                "<https://3.basecampapi.com/123456/projects.json?page=1>; rel=\"prev\"; title=\"prior, page\", <https://3.basecampapi.com/123456/projects.json?cursor=opaque,one>; title=\"next, page\"; rel=\"next\"";
        program(
                name,
                List.of(
                        next(ok(PROJECTS, "", array(first)), link),
                        ok(PROJECTS, "cursor=opaque,one", array(second))));
        success(run(ONE, new Settings(name, "projects")));
        assertRows(
                name,
                Map.of(
                        key("projects", "101", null, null),
                        expected(null, null, date(1), first),
                        key("projects", "102", null, null),
                        expected(null, null, date(1), second)));
        assertEquals(2, requests(name).size());
    }

    @Test
    void continuesEmptyFeedPagesAndPreservesRecordingInvariantsOnQueryOnlyLinks() throws Exception {
        String name = "empty-feed-next",
                tooNew = feed("900", 4, "101"),
                upper = feed("901", 3, "101"),
                lower = feed("902", 0, "101");
        program(
                name,
                List.of(
                        next(
                                ok(RECORDINGS, feedQuery("Todo"), array(tooNew)),
                                "<?page=2>; rel=\"next\""),
                        next(
                                ok(RECORDINGS, feedQuery("Todo") + "&page=2", array()),
                                "<?page=3>; rel=\"next\""),
                        ok(RECORDINGS, feedQuery("Todo") + "&page=3", array(upper, lower))));
        success(run(ONE, new Settings(name, "todos")));
        assertRows(
                name,
                Map.of(
                        key("todos", "901", "101", null),
                        expected("101", null, date(3), upper),
                        key("todos", "902", "101", null),
                        expected("101", null, date(0), lower)));
        assertEquals(3, requests(name).size());
    }

    @Test
    void preservesEveryFeedRecordInALargeEqualTimestampCohort() throws Exception {
        String name = "equal-time-feed";
        List<String> script = new ArrayList<>();
        Map<String, List<String>> expected = new HashMap<>();
        for (int page = 0; page < 2; page++) {
            List<String> records = new ArrayList<>();
            for (int index = 0; index < 100; index++) {
                String id = Long.toString(9007199254740993L + page * 100 + index),
                        row = feed(id, 1, "101");
                records.add(row);
                expected.put(key("messages", id, "101", null), expected("101", null, date(1), row));
            }
            String step =
                    ok(
                            RECORDINGS,
                            feedQuery("Message") + (page == 0 ? "" : "&page=2"),
                            array(records.toArray(new String[0])));
            script.add(page == 0 ? next(step, "<?page=2>; rel=\"next\"") : step);
        }
        program(name, script);
        success(run(ONE, new Settings(name, "messages")));
        assertRows(name, expected);
        assertEquals(2, requests(name).size());
    }

    @Test
    void rejectsForeignOriginsAccountsPathsAndConflictingRecordingQueryParameters()
            throws Exception {
        for (String link :
                List.of(
                        "<https://credential-trap.invalid/123456/projects/recordings.json?page=2>; rel=\"next\"",
                        "<https://3.basecampapi.com/999999/projects/recordings.json?page=2>; rel=\"next\"",
                        "<https://3.basecampapi.com/123456/projects.json?page=2>; rel=\"next\"",
                        "<?page=2&type=Message>; rel=\"next\"",
                        "<?page=2&type=Todo&type=Message>; rel=\"next\"",
                        "<?page=2>; rel=\"next\", <?page=3>; rel=\"next\"")) {
            String name = "unsafe-next-" + jobId();
            program(name, List.of(next(ok(RECORDINGS, feedQuery("Todo"), array()), link)));
            failure(run(ONE, new Settings(name, "todos")), "Link");
            assertRows(name, Map.of());
            assertEquals(1, requests(name).size());
        }
    }

    @Test
    void rejectsRepeatedPagesAndFailsRatherThanTruncatingReadBudgets() throws Exception {
        String name = "next-cycle";
        program(
                name,
                List.of(
                        next(ok(PROJECTS, "", array()), "<?page=2>; rel=\"next\""),
                        next(ok(PROJECTS, "page=2", array()), "<?page=2>; rel=\"next\"")));
        failure(run(ONE, new Settings(name, "projects")), "Basecamp");
        assertEquals(2, requests(name).size());
        name = "page-budget";
        program(name, List.of(next(ok(PROJECTS, "", array()), "<?page=2>; rel=\"next\"")));
        Settings settings = new Settings(name, "projects");
        settings.maxPages = 1;
        failure(run(ONE, settings), "Basecamp");
        assertEquals(1, requests(name).size());
        name = "item-budget";
        program(name, List.of(ok(PROJECTS, "", array(project("101"), project("102")))));
        settings = new Settings(name, "projects");
        settings.maxItems = 1;
        failure(run(ONE, settings), "Basecamp");
        assertRows(name, Map.of());
        assertEquals(1, requests(name).size());
    }

    @Test
    void exchangesTheExactLaunchpadRefreshFormAndUsesOnlyBearerOnTheDataOrigin() throws Exception {
        String name = "oauth-form", row = project("101");
        program(
                name,
                List.of(
                        oauth(
                                ok(
                                        "/authorization/token",
                                        "",
                                        grant(BasecampPeer.TOKEN, 3600, BasecampPeer.ROTATED)),
                                false),
                        ok(PROJECTS, "", array(row))));
        success(run(OAUTH, new Settings(name, "projects")));
        assertRows(
                name,
                Map.of(key("projects", "101", null, null), expected(null, null, date(1), row)));
        assertEquals(List.of("POST /authorization/token", PROJECTS), requests(name));
    }

    @Test
    void refreshesOnceAfterDataUnauthorizedUsingTheRotatedRefreshToken() throws Exception {
        String name = "oauth-renew", row = project("101");
        program(
                name,
                List.of(
                        oauth(
                                ok(
                                        "/authorization/token",
                                        "",
                                        grant(BasecampPeer.TOKEN, 3600, BasecampPeer.ROTATED)),
                                false),
                        step(PROJECTS, "", 401, "fixed", "", privateError()),
                        oauth(
                                ok(
                                        "/authorization/token",
                                        "",
                                        grant(BasecampPeer.RENEWED, 3600, null)),
                                true),
                        renewed(ok(PROJECTS, "", array(row)))));
        success(run(OAUTH, new Settings(name, "projects")));
        assertRows(
                name,
                Map.of(key("projects", "101", null, null), expected(null, null, date(1), row)));
        assertEquals(4, requests(name).size());
    }

    @Test
    void refreshesAfterActualTokenExpiryBetweenProjectPages() throws Exception {
        String name = "oauth-expiry", first = project("101"), second = project("102");
        program(
                name,
                List.of(
                        oauth(
                                ok(
                                        "/authorization/token",
                                        "",
                                        grant(BasecampPeer.TOKEN, 1, BasecampPeer.ROTATED)),
                                false),
                        next(
                                step(PROJECTS, "", 200, "delay", "", array(first)),
                                "<?page=2>; rel=\"next\""),
                        oauth(
                                ok(
                                        "/authorization/token",
                                        "",
                                        grant(BasecampPeer.RENEWED, 3600, null)),
                                true),
                        renewed(ok(PROJECTS, "page=2", array(second)))));
        success(run(OAUTH, new Settings(name, "projects")));
        assertRows(
                name,
                Map.of(
                        key("projects", "101", null, null),
                        expected(null, null, date(1), first),
                        key("projects", "102", null, null),
                        expected(null, null, date(1), second)));
        assertEquals(4, requests(name).size());
    }

    @Test
    void stopsStaticUnauthorizedAndRepeatedUnauthorizedAfterOauthRenewal() throws Exception {
        String name = "static-unauthorized";
        program(name, List.of(step(PROJECTS, "", 401, "fixed", "", privateError())));
        failure(run(ONE, new Settings(name, "projects")), "Basecamp");
        assertEquals(1, requests(name).size());
        name = "renew-unauthorized";
        program(
                name,
                List.of(
                        oauth(
                                ok(
                                        "/authorization/token",
                                        "",
                                        grant(BasecampPeer.TOKEN, 3600, BasecampPeer.ROTATED)),
                                false),
                        step(PROJECTS, "", 401, "fixed", "", privateError()),
                        oauth(
                                ok(
                                        "/authorization/token",
                                        "",
                                        grant(BasecampPeer.RENEWED, 3600, null)),
                                true),
                        renewed(step(PROJECTS, "", 401, "fixed", "", privateError()))));
        failure(run(OAUTH, new Settings(name, "projects")), "Basecamp");
        assertRows(name, Map.of());
        assertEquals(4, requests(name).size());
    }

    @Test
    void rejectsMalformedGrantsAndDoesNotReplayAmbiguousTokenPosts() throws Exception {
        for (String response :
                List.of(
                        "{}",
                        "{\"access_token\":\"\"}",
                        "{\"access_token\":123}",
                        "{\"access_token\":\"synthetic-basecamp-token\",\"token_type\":\"Basic\",\"expires_in\":3600}")) {
            String name = "bad-grant-" + jobId();
            program(name, List.of(oauth(ok("/authorization/token", "", response), false)));
            failure(run(OAUTH, new Settings(name, "projects")), "Basecamp");
            assertRows(name, Map.of());
            assertEquals(1, requests(name).size());
        }
        for (int status : List.of(400, 401, 503)) {
            String name = "failed-token-" + status;
            program(
                    name,
                    List.of(
                            oauth(
                                    step(
                                            "/authorization/token",
                                            "",
                                            status,
                                            "fixed",
                                            "0",
                                            privateError()),
                                    false)));
            failure(run(OAUTH, new Settings(name, "projects")), "Basecamp");
            assertRows(name, Map.of());
            assertEquals(1, requests(name).size());
        }
        String name = "token-throttled", row = project("101");
        program(
                name,
                List.of(
                        oauth(
                                step("/authorization/token", "", 429, "fixed", "1", privateError()),
                                false),
                        oauth(
                                ok(
                                        "/authorization/token",
                                        "",
                                        grant(BasecampPeer.TOKEN, 3600, null)),
                                false),
                        ok(PROJECTS, "", array(row))));
        success(run(OAUTH, new Settings(name, "projects")));
        assertRows(
                name,
                Map.of(key("projects", "101", null, null), expected(null, null, date(1), row)));
        List<String> times = engine.lines(name + ".times");
        assertTrue(Long.parseLong(times.get(1)) - Long.parseLong(times.get(0)) >= 900);
        assertEquals(3, requests(name).size());
    }

    @Test
    void neverTurnsProjectPermissionsOrMissingParentsIntoEmptyChildren() throws Exception {
        for (int status : List.of(400, 403, 404)) {
            String name = "missing-parent-" + status;
            program(
                    name,
                    List.of(
                            ok(PROJECTS, "", array(project("101"))),
                            step(
                                    "/123456/projects/101.json",
                                    "",
                                    status,
                                    "fixed",
                                    "",
                                    privateError())));
            failure(run(ONE, new Settings(name, "schedule_entries")), "Basecamp");
            assertRows(name, Map.of());
            assertEquals(2, requests(name).size());
        }
    }

    @Test
    void retriesDataThrottlingWithItsDelayAndStopsAtTheRetryBudget() throws Exception {
        String name = "data-throttle", row = project("101");
        program(
                name,
                List.of(
                        step(PROJECTS, "", 429, "fixed", "1", privateError()),
                        ok(PROJECTS, "", array(row))));
        success(run(ONE, new Settings(name, "projects")));
        assertRows(
                name,
                Map.of(key("projects", "101", null, null), expected(null, null, date(1), row)));
        List<String> times = engine.lines(name + ".times");
        assertTrue(Long.parseLong(times.get(1)) - Long.parseLong(times.get(0)) >= 900);
        name = "retry-budget";
        program(
                name,
                List.of(
                        step(PROJECTS, "", 503, "fixed", "0", privateError()),
                        step(PROJECTS, "", 503, "fixed", "0", privateError())));
        Settings settings = new Settings(name, "projects");
        settings.retries = 1;
        failure(run(ONE, settings), "Basecamp");
        assertRows(name, Map.of());
        assertEquals(2, requests(name).size());
    }

    @Test
    void rejectsMalformedCollectionsIdentitiesAndFeedTimestampsBeforePublishing() throws Exception {
        for (String body :
                List.of("{}", "null", "[null]", "[{}]", "[] trailing", "[{\"id\":1,\"id\":2}]")) {
            String name = "malformed-project-" + jobId();
            program(name, List.of(ok(PROJECTS, "", body)));
            failure(run(ONE, new Settings(name, "projects")), "Basecamp");
            assertRows(name, Map.of());
            assertEquals(1, requests(name).size());
        }
        String name = "missing-feed-time";
        program(name, List.of(ok(RECORDINGS, feedQuery("Todo"), "[{\"id\":601}]")));
        failure(run(ONE, new Settings(name, "todos")), "Basecamp");
        assertRows(name, Map.of());
    }

    @Test
    void boundsPlainAndCompressedResponsesAndRefusesRedirects() throws Exception {
        for (String mode : List.of("fixed", "gzip", "redirect")) {
            String name = "body-bound-" + mode;
            String row = project("101").replace("Read &amp; preserve", "x".repeat(20000));
            program(
                    name,
                    List.of(
                            step(
                                    PROJECTS,
                                    "",
                                    "redirect".equals(mode) ? 302 : 200,
                                    mode,
                                    "",
                                    array(row))));
            Settings settings = new Settings(name, "projects");
            settings.bytes = 8192;
            failure(
                    run(ONE, settings),
                    "redirect".equals(mode) ? "HTTP 302" : "max_response_bytes");
            assertRows(name, Map.of());
            assertEquals(1, requests(name).size());
        }
    }

    @Test
    void rejectsAnUntrustedApiCertificateBeforeTransmittingTheBearer() throws Exception {
        String name = "untrusted-certificate";
        program(name, List.of());
        Settings settings = new Settings(name, "projects");
        settings.certificate = untrusted;
        settings.retries = 0;
        failure(run(ONE, settings), "Basecamp");
        assertRows(name, Map.of());
        assertEquals(0, requests(name).size());
    }

    @Test
    void cancelsAnActiveResponseAndClosesItsTlsConnection() throws Exception {
        String name = "cancel-body";
        program(name, List.of(step(PROJECTS, "", 200, "stream", "", "[]")));
        Settings settings = held(name, "projects");
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(ONE, job, settings.variables()));
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> engine.exists(name + ".started"));
            success(engine.cancelJob(job));
            running.get(30, TimeUnit.SECONDS);
            awaitDisconnect(name);
            assertRows(name, Map.of());
        } finally {
            stopIfRunning(job, running);
        }
    }

    @Test
    void boundsTricklingReadsAndRejectsRetryDelaysBeyondTheOperationBudget() throws Exception {
        String name = "trickle-deadline";
        program(name, List.of(step(PROJECTS, "", 200, "stream", "", "[]")));
        Settings settings = new Settings(name, "projects");
        settings.timeout = 1200;
        settings.deadline = 1600;
        settings.retries = 0;
        failure(run(ONE, settings), "Basecamp");
        awaitDisconnect(name);
        assertRows(name, Map.of());
        assertEquals(1, requests(name).size());
        name = "operation-delay";
        program(name, List.of(step(PROJECTS, "", 429, "fixed", "3600", privateError())));
        settings = new Settings(name, "projects");
        settings.timeout = 3000;
        settings.deadline = 5000;
        failure(run(ONE, settings), "operation deadline");
        assertRows(name, Map.of());
        assertEquals(1, requests(name).size());
    }

    @Test
    void restoresTheNextProjectPageWithoutReplayingCommittedProjects() throws Exception {
        String name = "resume-projects", first = project("101"), second = project("102");
        program(
                name,
                List.of(
                        next(ok(PROJECTS, "", array(first)), "<?page=2>; rel=\"next\""),
                        step(PROJECTS, "page=2", 200, "hold", "", array(second)),
                        ok(PROJECTS, "page=2", array(second))));
        Settings settings = held(name, "projects");
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(ONE, job, settings.variables()));
        try {
            awaitRequests(name, 2);
            savepoint(job, running);
            engine.touch(name + ".release");
            success(engine.restoreJob(ONE, job, settings.variables()));
            assertRows(
                    name,
                    Map.of(
                            key("projects", "101", null, null),
                            expected(null, null, date(1), first),
                            key("projects", "102", null, null),
                            expected(null, null, date(1), second)));
            assertEquals(3, requests(name).size());
        } finally {
            engine.touch(name + ".release");
            stopIfRunning(job, running);
        }
    }

    @Test
    void restoresAnEntryUnderItsVerifiedParentsWithoutRepeatingTheirCommittedRows()
            throws Exception {
        String name = "resume-child",
                parent = project("101"),
                dock = dock("201", true),
                entry = record("501", 1);
        String detail = details("101", dock),
                path = "/123456/buckets/101/schedules/201/entries.json";
        program(
                name,
                List.of(
                        ok(PROJECTS, "", array(parent)),
                        ok("/123456/projects/101.json", "", detail),
                        step(path, "", 200, "hold", "", array(entry)),
                        ok(PROJECTS, "", array(parent)),
                        ok("/123456/projects/101.json", "", detail),
                        ok(path, "", array(entry)),
                        ok(RECORDINGS, feedQuery("Todo"), array()),
                        ok(RECORDINGS, feedQuery("Message"), array())));
        Settings settings = held(name, "projects");
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(ALL, job, settings.variables()));
        try {
            awaitRequests(name, 3);
            savepoint(job, running);
            engine.touch(name + ".release");
            success(engine.restoreJob(ALL, job, settings.variables()));
            assertRows(
                    name,
                    Map.of(
                            key("projects", "101", null, null),
                            expected(null, null, date(1), parent),
                            key("schedules", "201", "101", "201"),
                            expected("101", "201", null, schedule(dock, "101")),
                            key("schedule_entries", "501", "101", "201"),
                            expected("101", "201", date(1), entry)));
            assertEquals(8, requests(name).size());
        } finally {
            engine.touch(name + ".release");
            stopIfRunning(job, running);
        }
    }

    @Test
    void rejectsAChangedProjectDockBeforeRestoringItsPendingEntry() throws Exception {
        String name = "changed-dock", parent = project("101");
        String path = "/123456/buckets/101/schedules/201/entries.json";
        program(
                name,
                List.of(
                        ok(PROJECTS, "", array(parent)),
                        ok("/123456/projects/101.json", "", details("101", dock("201", true))),
                        step(path, "", 200, "hold", "", array()),
                        ok(PROJECTS, "", array(parent)),
                        ok("/123456/projects/101.json", "", details("101", dock("201", false)))));
        Settings settings = held(name, "schedule_entries");
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(ONE, job, settings.variables()));
        try {
            awaitRequests(name, 3);
            savepoint(job, running);
            engine.touch(name + ".release");
            failure(
                    engine.restoreJob(ONE, job, settings.variables()),
                    "project dock changed since checkpoint");
            assertRows(name, Map.of());
            assertEquals(5, requests(name).size());
        } finally {
            engine.touch(name + ".release");
            stopIfRunning(job, running);
        }
    }

    @Test
    void restoresAnIncompleteTokenResponseWithoutSavingItsCredentialsInState() throws Exception {
        String name = "resume-oauth", parent = project("101");
        String grant = grant(BasecampPeer.TOKEN, 3600, BasecampPeer.ROTATED);
        program(
                name,
                List.of(
                        oauth(step("/authorization/token", "", 200, "partial", "", grant), false),
                        oauth(ok("/authorization/token", "", grant), false),
                        ok(PROJECTS, "", array(parent))));
        Settings settings = held(name, "projects");
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(OAUTH, job, settings.variables()));
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> engine.exists(name + ".started"));
            savepoint(job, running);
            engine.touch(name + ".release");
            success(engine.restoreJob(OAUTH, job, settings.variables()));
            assertRows(
                    name,
                    Map.of(
                            key("projects", "101", null, null),
                            expected(null, null, date(1), parent)));
            assertEquals(3, requests(name).size());
        } finally {
            engine.touch(name + ".release");
            stopIfRunning(job, running);
        }
    }

    @Test
    void rejectsChangedAccountOrFeedWindowBeforeAnyRestoredRequest() throws Exception {
        for (boolean account : List.of(false, true)) {
            String name = "restore-scope-" + account;
            program(name, List.of(step(RECORDINGS, feedQuery("Todo"), 200, "hold", "", array())));
            Settings settings = held(name, "todos");
            String job = jobId();
            Future<Container.ExecResult> running =
                    tasks.submit(() -> engine.executeJob(ONE, job, settings.variables()));
            try {
                awaitRequests(name, 1);
                savepoint(job, running);
                engine.touch(name + ".release");
                if (account) {
                    settings.account = "999999";
                } else {
                    settings.end = "2024-01-01T00:00:00.004Z";
                }
                failure(engine.restoreJob(ONE, job, settings.variables()), "checkpoint");
                assertRows(name, Map.of());
                assertEquals(1, requests(name).size());
            } finally {
                engine.touch(name + ".release");
                stopIfRunning(job, running);
            }
        }
    }

    @Test
    void restoresTerminalSourcesWithoutAnotherTokenOrDataRequest() throws Exception {
        String name = "resume-terminal", row = project("101");
        program(name, List.of(ok(PROJECTS, "", array(row))));
        Settings settings = new Settings(name, "projects");
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(TERMINAL, job, settings.variables()));
        try {
            awaitRequests(name, 1);
            savepoint(job, running);
            Future<Container.ExecResult> restored =
                    tasks.submit(() -> engine.restoreJob(TERMINAL, job, settings.variables()));
            try {
                long baseline = engine.getCompletedCheckpointCount(job);
                Awaitility.await()
                        .atMost(35, TimeUnit.SECONDS)
                        .until(() -> engine.getCompletedCheckpointCount(job) > baseline + 1);
                assertRows(
                        name,
                        Map.of(
                                key("projects", "101", null, null),
                                expected(null, null, date(1), row)));
                assertEquals(1, requests(name).size());
            } finally {
                stopIfRunning(job, restored);
            }
        } finally {
            stopIfRunning(job, running);
        }
    }

    @Test
    void restoresTheSerializedPageInternalRowPositionUsingPackagedArtifacts() throws Exception {
        String name = "probe-row", page = probePage(false);
        program(name, List.of(ok(PROJECTS, "", page), ok(PROJECTS, "", page)));
        success(engine.runProbe("row"));
        assertEquals(2, requests(name).size());
    }

    @Test
    void rejectsAChangedPageBeforeItsUnverifiedRestoredSuffixEscapes() throws Exception {
        String name = "probe-changed";
        program(
                name,
                List.of(ok(PROJECTS, "", probePage(false)), ok(PROJECTS, "", probePage(true))));
        success(engine.runProbe("changed"));
        assertEquals(2, requests(name).size());
    }

    @Test
    void preservesEmptyAssignmentStickyCancellationAndTerminalStateWithoutHttp() throws Exception {
        String name = "probe-zero";
        program(name, List.of());
        success(engine.runProbe("zero"));
        assertEquals(0, requests(name).size());
    }

    private static String probePage(boolean changed) {
        return array(
                project("101").replace("Read &amp; preserve", "private-project-value-one"),
                project("102")
                        .replace(
                                "Read &amp; preserve",
                                changed
                                        ? "changed-private-project-value"
                                        : "private-project-value-two"));
    }

    private static String date(int millisecond) {
        return "2024-01-01T00:00:00." + String.format(Locale.ROOT, "%03d", millisecond) + "Z";
    }

    private static String project(String id) {
        return "{\"id\":"
                + id
                + ",\"name\":\"Old parent project\",\"created_at\":\"2010-01-01T00:00:00Z\",\"updated_at\":\""
                + date(1)
                + "\""
                + EXTRA
                + "}";
    }

    private static String record(String id, int modified) {
        return "{\"id\":" + id + ",\"updated_at\":\"" + date(modified) + "\"" + EXTRA + "}";
    }

    private static String feed(String id, int modified, String project) {
        String row = record(id, modified);
        return project == null
                ? row
                : row.substring(0, row.length() - 1) + ",\"bucket\":{\"id\":" + project + "}}";
    }

    private static String dock(String id, boolean enabled) {
        return "{\"id\":"
                + id
                + ",\"name\":\"schedule\",\"enabled\":"
                + enabled
                + ",\"url\":\"https://credential-trap.invalid/never-follow\"}";
    }

    private static String details(String id, String... docks) {
        return "{\"id\":" + id + ",\"dock\":" + array(docks) + "}";
    }

    private static String schedule(String dock, String project) throws Exception {
        ObjectNode node = (ObjectNode) JSON.readTree(dock);
        node.put("project_id", Long.parseLong(project));
        node.putObject("substream_data")
                .put("project_id", Long.parseLong(project))
                .put("schedule_id", node.path("id").longValue());
        return node.toString();
    }

    private static String array(String... rows) {
        return "[" + String.join(",", rows) + "]";
    }

    private static String feedQuery(String type) {
        return "direction=desc&sort=updated_at&type=" + type;
    }

    private static String grant(String token, int expires, String refresh) {
        return "{\"access_token\":\""
                + token
                + "\",\"token_type\":\"Bearer\",\"expires_in\":"
                + expires
                + (refresh == null ? "" : ",\"refresh_token\":\"" + refresh + "\"")
                + "}";
    }

    private static String privateError() {
        return "{\"error\":\"private-provider-message\"}";
    }

    private static String ok(String path, String query, String body) {
        return step(path, query, 200, "fixed", "", body);
    }

    private static String step(
            String path, String query, int status, String mode, String retry, String body) {
        return String.join(
                "\t",
                path,
                query,
                Integer.toString(status),
                mode,
                retry,
                Base64.getEncoder().encodeToString(body.getBytes(StandardCharsets.UTF_8)),
                "",
                "data");
    }

    private static String with(String step, int at, String value) {
        String[] fields = step.split("\t", -1);
        fields[at] = value;
        return String.join("\t", fields);
    }

    private static String next(String step, String link) {
        return with(step, 6, link);
    }

    private static String renewed(String step) {
        return with(step, 7, "renewed");
    }

    private static String oauth(String step, boolean rotated) {
        return with(step, 7, rotated ? "rotated-oauth" : "oauth");
    }

    private static String key(String resource, String id, String project, String schedule) {
        return resource
                + "/"
                + id
                + "/"
                + (project == null ? "~" : project)
                + "/"
                + (schedule == null ? "~" : schedule);
    }

    private static List<String> expected(
            String project, String schedule, String modified, String record) {
        return List.of(
                project == null ? "~" : project,
                schedule == null ? "~" : schedule,
                modified == null ? "~" : modified,
                record);
    }

    private static Settings held(String name, String resource) {
        Settings settings = new Settings(name, resource);
        settings.timeout = 180000;
        settings.deadline = 240000;
        return settings;
    }

    private void assertRows(String name, Map<String, List<String>> expected) throws Exception {
        Map<String, JsonNode> actual = new HashMap<>();
        for (String line : engine.rows(name)) {
            JsonNode row = JSON.readTree(line);
            Set<String> fields = new HashSet<>();
            row.fieldNames().forEachRemaining(fields::add);
            assertEquals(
                    Set.of("resource", "id", "project_id", "schedule_id", "updated_at", "record"),
                    fields);
            String
                    project =
                            row.path("project_id").isNull()
                                    ? null
                                    : row.path("project_id").asText(),
                    schedule =
                            row.path("schedule_id").isNull()
                                    ? null
                                    : row.path("schedule_id").asText();
            assertNull(
                    actual.put(
                            key(
                                    row.path("resource").asText(),
                                    row.path("id").asText(),
                                    project,
                                    schedule),
                            row),
                    "duplicate committed identity and context");
        }
        assertEquals(expected.keySet(), actual.keySet());
        for (Map.Entry<String, List<String>> entry : expected.entrySet()) {
            JsonNode row = actual.get(entry.getKey());
            String[] fields = {"project_id", "schedule_id", "updated_at"};
            for (int index = 0; index < fields.length; index++) {
                if ("~".equals(entry.getValue().get(index))) {
                    assertTrue(row.path(fields[index]).isNull());
                } else {
                    assertEquals(entry.getValue().get(index), row.path(fields[index]).asText());
                }
            }
            String record = row.path("record").asText();
            assertEquals(JSON.readTree(entry.getValue().get(3)), JSON.readTree(record));
            if (entry.getValue().get(3).contains("9223372036854775808")) {
                assertTrue(record.contains("9223372036854775808"));
                assertTrue(record.contains("123456789012345678.123456789"));
            }
        }
    }

    private Container.ExecResult run(String config, Settings options) throws Exception {
        return engine.executeJob(config, jobId(), options.variables());
    }

    private void program(String name, List<String> lines) throws Exception {
        rejectsUnexpectedProviderTraffic();
        Path script = Files.createTempFile(directory, "steps-", ".txt");
        Files.write(script, lines, StandardCharsets.UTF_8);
        engine.install(script, name + ".program");
        Path current = Files.createTempFile(directory, "active-", ".txt");
        Files.writeString(current, name);
        engine.install(current, "active");
        active = name;
    }

    private List<String> requests(String name) throws Exception {
        return engine.lines(name + ".requests");
    }

    private void awaitRequests(String name, int count) {
        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> requests(name).size() == count);
    }

    private void awaitDisconnect(String name) {
        Awaitility.await()
                .atMost(15, TimeUnit.SECONDS)
                .until(() -> engine.exists(name + ".disconnect"));
    }

    private void savepoint(String job, Future<Container.ExecResult> running) throws Exception {
        long baseline = engine.getCompletedCheckpointCount(job);
        Awaitility.await()
                .atMost(35, TimeUnit.SECONDS)
                .until(() -> engine.getCompletedCheckpointCount(job) > baseline);
        success(engine.savepointJob(job));
        running.get(30, TimeUnit.SECONDS);
    }

    private void stopIfRunning(String job, Future<Container.ExecResult> running) throws Exception {
        if (!running.isDone()) {
            engine.cancelJob(job);
            running.cancel(true);
        }
    }

    private static String jobId() {
        return Long.toString(
                ThreadLocalRandom.current().nextLong(1_000_000_000_000L, 9_000_000_000_000L));
    }

    private static String text(Container.ExecResult result) {
        return result.getStdout() + result.getStderr();
    }

    private static void success(Container.ExecResult result) {
        assertEquals(0, result.getExitCode(), () -> text(result));
    }

    private static void failure(Container.ExecResult result, String fragment) {
        assertNotEquals(0, result.getExitCode(), () -> text(result));
        assertTrue(text(result).contains(fragment), () -> text(result));
        assertFalse(text(result).contains("synthetic-basecamp"));
        assertFalse(text(result).contains(BasecampPeer.SECRET));
        assertFalse(text(result).contains(BasecampPeer.REFRESH));
        assertFalse(text(result).contains("private-provider-message"));
    }

    private static KeyStore identity(String name) throws Exception {
        String keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
        Path file = directory.resolve(name + ".p12");
        Process process =
                new ProcessBuilder(
                                keytool,
                                "-genkeypair",
                                "-alias",
                                "api",
                                "-keyalg",
                                "RSA",
                                "-keysize",
                                "2048",
                                "-validity",
                                "7",
                                "-dname",
                                "CN=" + BasecampPeer.HOST,
                                "-ext",
                                "SAN=dns:" + BasecampPeer.HOST + ",dns:" + BasecampPeer.AUTH_HOST,
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                file.toString(),
                                "-storepass",
                                BasecampPeer.PASSWORD,
                                "-keypass",
                                BasecampPeer.PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, process.waitFor(), output);
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(file)) {
            store.load(input, BasecampPeer.PASSWORD.toCharArray());
        }
        return store;
    }

    /**
     * Selects an account, one read projection and a frozen feed window per job.
     *
     * <p>Authentication uses the same fixed API and Launchpad hosts as production.
     */
    private static final class Settings {
        private final String name;
        private final String resource;
        private String account = "123456";
        private String certificate = trust;
        private String start = "2024-01-01T00:00:00.000Z";
        private String end = "2024-01-01T00:00:00.003Z";
        private int maxPages = 10000;
        private int maxItems = 10000;
        private int bytes = 4194304;
        private int retries = 2;
        private int timeout = 30000;
        private int deadline = 120000;

        private Settings(String name, String resource) {
            this.name = name;
            this.resource = resource;
        }

        private String[] variables() {
            return new String[] {
                "api.output=" + name,
                "api.resource=" + resource,
                "api.account=" + account,
                "api.user_agent=" + BasecampPeer.AGENT,
                "api.token=" + BasecampPeer.TOKEN,
                "api.client_id=" + BasecampPeer.CLIENT,
                "api.client_secret=" + BasecampPeer.SECRET,
                "api.refresh_token=" + BasecampPeer.REFRESH,
                "api.start=" + start,
                "api.end=" + end,
                "api.trust=" + certificate,
                "api.max_pages=" + maxPages,
                "api.max_items=" + maxItems,
                "api.max_bytes=" + bytes,
                "api.retries=" + retries,
                "api.timeout=" + timeout,
                "api.deadline=" + deadline
            };
        }
    }

    /**
     * Stages the independently compiled peer in the ordinary Java 11 engine container.
     *
     * <p>The connector receives an explicit PEM anchor; the JVM's global trust store is unchanged.
     */
    private static final class Engine extends SeaTunnelContainer {
        private static final String ROOT = "/tmp/basecamp-peer";
        private final Path fixtures;

        private Engine(Path fixtures) {
            this.fixtures = fixtures;
        }

        /**
         * Preserves reserved credential characters as argv without logging command values.
         *
         * <p>The inherited shell join also logs the joined command and its credential values.
         */
        @Override
        protected Container.ExecResult executeCommand(
                GenericContainer<?> container, List<String> command)
                throws IOException, InterruptedException {
            return container.execInContainer(command.toArray(new String[0]));
        }

        @Override
        protected void executeExtraCommands(GenericContainer<?> container)
                throws IOException, InterruptedException {
            super.executeExtraCommands(container);
            container.withExtraHost(BasecampPeer.HOST, "127.0.0.1");
            container.withExtraHost(BasecampPeer.AUTH_HOST, "127.0.0.1");
            container.withExtraHost("credential-trap.invalid", "127.0.0.1");
            for (String name : List.of("trusted.p12", "untrusted.p12", "trusted.pem")) {
                container.withCopyFileToContainer(
                        MountableFile.forHostPath(fixtures.resolve(name)), ROOT + "/" + name);
            }
            try {
                String file = "BasecampPeer.class";
                Path source = Path.of(BasecampPeer.class.getResource(file).toURI());
                container.withCopyFileToContainer(
                        MountableFile.forHostPath(source),
                        ROOT
                                + "/classes/"
                                + BasecampPeer.class.getPackage().getName().replace('.', '/')
                                + "/"
                                + file);
                Path probe = Path.of(BasecampRecoveryProbe.class.getResource("").toURI());
                try (DirectoryStream<Path> classes =
                        Files.newDirectoryStream(probe, "BasecampRecoveryProbe*.class")) {
                    for (Path compiled : classes) {
                        container.withCopyFileToContainer(
                                MountableFile.forHostPath(compiled),
                                ROOT
                                        + "/classes/"
                                        + BasecampRecoveryProbe.class
                                                .getPackage()
                                                .getName()
                                                .replace('.', '/')
                                        + "/"
                                        + compiled.getFileName());
                    }
                }
            } catch (URISyntaxException e) {
                throw new IOException("Cannot stage the Java HTTPS fixture", e);
            }
        }

        private Container.ExecResult runPeer() throws Exception {
            return server.execInContainer(
                    "java", "-cp", ROOT + "/classes", BasecampPeer.class.getName(), ROOT);
        }

        private Container.ExecResult runProbe(String mode) throws Exception {
            ContainerUtil.copyAllConnectorJarToContainer(
                    server,
                    getConnectorModulePath(),
                    getConnectorNamePrefix(),
                    getConnectorType(),
                    SEATUNNEL_HOME);
            return server.execInContainer(
                    "java",
                    "-cp",
                    ROOT + "/classes:/tmp/seatunnel/lib/*:/tmp/seatunnel/connectors/*",
                    BasecampRecoveryProbe.class.getName(),
                    ROOT + "/trusted.pem",
                    BasecampPeer.TOKEN,
                    mode);
        }

        /**
         * Includes checkpoints from the still-running anchor pipeline after the source finishes.
         *
         * <p>The base helper reads only the first pipeline, which is terminal in this scenario.
         */
        @Override
        public long getCompletedCheckpointCount(String jobId) {
            HttpURLConnection connection = null;
            try {
                URL url =
                        new URL(
                                "http",
                                server.getHost(),
                                server.getMappedPort(8080),
                                "/jobs/checkpoints/" + jobId);
                connection = (HttpURLConnection) url.openConnection();
                connection.setConnectTimeout(5000);
                connection.setReadTimeout(5000);
                if (connection.getResponseCode() != 200) {
                    return 0L;
                }
                try (InputStream input = connection.getInputStream()) {
                    long completed = 0L;
                    for (JsonNode pipeline : JSON.readTree(input).path("pipelines")) {
                        completed += pipeline.path("counts").path("completed").asLong();
                    }
                    return completed;
                }
            } catch (IOException e) {
                throw new IllegalStateException("Cannot inspect the test job's checkpoints", e);
            } finally {
                if (connection != null) {
                    connection.disconnect();
                }
            }
        }

        private void install(Path source, String name) {
            server.copyFileToContainer(MountableFile.forHostPath(source), ROOT + "/" + name);
        }

        private boolean exists(String name) throws Exception {
            return server.execInContainer("test", "-f", ROOT + "/" + name).getExitCode() == 0;
        }

        private void touch(String name) throws Exception {
            success(server.execInContainer("touch", ROOT + "/" + name));
        }

        private List<String> lines(String name) throws Exception {
            if (!exists(name)) {
                return Collections.emptyList();
            }
            Container.ExecResult result = server.execInContainer("cat", ROOT + "/" + name);
            success(result);
            return split(result.getStdout());
        }

        private List<String> rows(String name) throws Exception {
            String path = "/tmp/seatunnel_mnt/basecamp-" + name;
            Container.ExecResult result =
                    server.execInContainer(
                            "sh",
                            "-c",
                            "if [ -d "
                                    + path
                                    + " ]; then find "
                                    + path
                                    + " -maxdepth 1 -type f -name 'receipt_*'"
                                    + " -exec sh -c 'for f do cat \"$f\"; printf \"\\n\"; done' sh {} +; fi");
            success(result);
            return split(result.getStdout());
        }

        private static List<String> split(String value) {
            List<String> lines = new ArrayList<>();
            for (String line : value.split("\\r?\\n")) {
                if (!line.isBlank()) {
                    lines.add(line);
                }
            }
            return lines;
        }
    }
}
