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
package org.apache.seatunnel.e2e.connector.campaignmonitor;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.campaignmonitor.CampaignMonitorRecoveryProbe;
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
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
 * Runs all Campaign Monitor v3.3 resources through Zeta and a transactional LocalFile sink.
 *
 * <p>An isolated HTTPS peer verifies exact wire requests and returns independently authored
 * records.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CampaignMonitorIT extends TestSuiteBase {
    private static final ObjectMapper JSON =
            new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    @TempDir static Path directory;
    private static String trust;
    private static String wrongTrust;
    private final ExecutorService tasks = Executors.newCachedThreadPool();
    private Engine engine;
    private Future<Container.ExecResult> peer;
    private String active;

    @BeforeAll
    void startsAnOwnedJava11EngineAndIndependentTlsPeer() throws Exception {
        trust = identity("trusted");
        wrongTrust = identity("untrusted");
        Files.writeString(directory.resolve("trusted.pem"), trust);
        engine = new Engine(directory);
        engine.startUp();
        peer = tasks.submit(engine::peer);
        Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> engine.exists("ready"));
    }

    @AfterEach
    void rejectsUnexpectedOrCredentialBearingRequests() throws Exception {
        if (active != null) {
            assertFalse(engine.exists(active + ".invalid"), active);
        }
    }

    @AfterAll
    void stopsOnlyOwnedResources() throws Exception {
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

    private static final String API = "/api/v3.3";
    private static final String CLIENTS = API + "/clients.json";
    private static final String ADMINS = API + "/admins.json";
    private static final String A = "11111111111111111111111111111111";
    private static final String B = "22222222222222222222222222222222";
    private static final String SENT = "2026-04-01 00:00:00";

    @Test
    void readsAllTwelveResourcesAndKeepsNativeRecordsWithoutClientSecrets() throws Exception {
        String name = "all-twelve";
        Settings settings = new Settings(name);
        String client = client(A, "Cliente 東京");
        String details =
                "{\"ApiKey\":\"private-provider-message\",\"BasicDetails\":{\"ClientID\":\""
                        + A
                        + "\",\"TimeZone\":\"(GMT+10:00) Canberra, Melbourne, Sydney\",\"Country\":\"Australia\"},"
                        + "\"BillingDetails\":{\"BaseRatePerRecipient\":0.00000560012345678900,\"Credits\":9007199254740993}}";
        String segment =
                "{\"SegmentID\":\"seg1\",\"ListID\":\"list1\",\"Title\":\"Subscriber <b>VIP</b>\"}";
        String template =
                "{\"TemplateID\":\"t1\",\"Name\":\"Résumé\",\"PreviewURL\":\"https://credential-trap.invalid/template\"}";
        String person =
                "{\"EmailAddress\":\"Älice@example.test\",\"AccessLevel\":31,\"Status\":\"Active\"}";
        String tag = "{\"Name\":\"Summer 🌍\",\"NumberOfCampaigns\":\"120\"}";
        String list = "{\"ListID\":\"list1\",\"Name\":\"Newsletter\",\"future\":[false,null]}";
        String stop =
                "{\"EmailAddress\":\"blocked@example.test\",\"State\":\"Suppressed\","
                        + "\"Date\":\"2026-03-31 19:00:00\",\"SuppressionReason\":\"Bounced\"}";
        String sent = campaign("sent1", SENT);
        String draft =
                "{\"CampaignID\":\"d1\",\"Subject\":\"Draft <i>preview</i>\","
                        + "\"DateCreated\":\"2026-04-02 18:30:00\",\"PreviewTextURL\":\"https://credential-trap.invalid/text\"}";
        String scheduled =
                "{\"CampaignID\":\"s1\",\"ScheduledTimeZone\":\"(GMT+10:00) Canberra, Melbourne, Sydney\","
                        + "\"DateScheduled\":\"2026-11-02 10:40:00\",\"Tags\":[\"Launch\"]}";
        String admin =
                "{\"EmailAddress\":\"admin@example.test\",\"Status\":\"Waiting to Accept the Invitation\"}";
        program(
                name,
                List.of(
                        reply(CLIENTS, array(client)),
                        reply(detail(A), details),
                        reply(scoped(A, "segments"), array(segment)),
                        reply(scoped(A, "templates"), array(template)),
                        reply(scoped(A, "people"), array(person)),
                        reply(scoped(A, "tags"), array(tag)),
                        reply(scoped(A, "lists"), array(list)),
                        reply(paged(A, "suppressionlist", 1, 1000), page(1, 1000, 1, 1, stop)),
                        reply(sentTarget(A, 1, settings), page(1, 1000, 1, 1, sent))
                                .put("mode", "gzip"),
                        reply(scoped(A, "drafts"), array(draft)),
                        reply(scoped(A, "scheduled"), array(scheduled)),
                        reply(ADMINS, array(admin))));
        success(run(settings));
        Map<String, JsonNode> expected = new HashMap<>();
        expect(expected, "clients", A, A, null, client);
        expect(
                expected,
                "client_details",
                A,
                A,
                null,
                details.replace("\"ApiKey\":\"private-provider-message\",", ""));
        expect(expected, "segments", "seg1", A, null, segment);
        expect(expected, "templates", "t1", A, null, template);
        expect(expected, "people", "Älice@example.test", A, null, person);
        expect(expected, "tags", "Summer 🌍", A, null, tag);
        expect(expected, "subscriber_lists", "list1", A, null, list);
        expect(expected, "suppression_lists", "blocked@example.test", A, null, stop);
        expect(expected, "sent_campaigns", "sent1", A, SENT, sent);
        expect(expected, "draft_campaigns", "d1", A, null, draft);
        expect(expected, "scheduled_campaigns", "s1", A, null, scheduled);
        expect(expected, "admins", "admin@example.test", null, null, admin);
        assertRows(name, expected);
        assertTrue(
                engine.rows(name).stream()
                        .anyMatch(line -> line.contains("0.00000560012345678900")));
        assertTrue(
                engine.rows(name).stream()
                        .noneMatch(line -> line.contains("private-provider-message")));
        consumed(name, 12);
    }

    @Test
    void discoversBothClientsForChildOnlySelectionWithoutMixingEqualAddresses() throws Exception {
        String name = "two-client-child-only";
        String left = "{\"EmailAddress\":\"person@example.test\",\"Name\":\"Left\"}";
        String right = "{\"EmailAddress\":\"person@example.test\",\"Name\":\"Right\"}";
        program(
                name,
                List.of(
                        reply(CLIENTS, array(client(A, "Left"), client(B, "Right"))),
                        reply(scoped(A, "people"), array(left)),
                        reply(scoped(B, "people"), array(right))));
        success(run(new Settings(name, "people")));
        Map<String, JsonNode> expected = new HashMap<>();
        expect(expected, "people", "person@example.test", A, null, left);
        expect(expected, "people", "person@example.test", B, null, right);
        assertRows(name, expected);
        consumed(name, 3);
    }

    @Test
    void readsAll1001CampaignsAcrossTheNativeMaximumPageSize() throws Exception {
        String name = "more-than-1000-campaigns";
        Settings settings = new Settings(name, "sent_campaigns");
        List<String> first = new ArrayList<>();
        Map<String, JsonNode> expected = new HashMap<>();
        for (int i = 0; i < 1001; ++i) {
            String id = "send-" + i;
            String row = campaign(id, SENT);
            if (i < 1000) {
                first.add(row);
            }
            expect(expected, "sent_campaigns", id, A, SENT, row);
        }
        program(
                name,
                List.of(
                        reply(CLIENTS, array(client(A, "First"))),
                        reply(
                                sentTarget(A, 1, settings),
                                page(1, 1000, 1001, 2, first.toArray(new String[0]))),
                        reply(
                                sentTarget(A, 2, settings),
                                page(2, 1000, 1001, 2, campaign("send-1000", SENT)))));
        success(run(settings));
        assertRows(name, expected);
        consumed(name, 3);
    }

    @Test
    void followsTheNextSuppressionPageAfterAShortPageAndAnEmptyPage() throws Exception {
        String name = "short-empty-paged";
        Settings settings = new Settings(name, "suppression_lists");
        settings.pageSize = 10;
        String one = "{\"EmailAddress\":\"a@example.test\",\"State\":\"Suppressed\"}";
        String last = "{\"EmailAddress\":\"z@example.test\",\"SuppressionReason\":null}";
        program(
                name,
                List.of(
                        reply(CLIENTS, array(client(A, "First"))),
                        reply(paged(A, "suppressionlist", 1, 10), page(1, 10, 21, 3, one)),
                        reply(paged(A, "suppressionlist", 2, 10), page(2, 10, 21, 3)),
                        reply(paged(A, "suppressionlist", 3, 10), page(3, 10, 21, 3, last))));
        success(run(settings));
        Map<String, JsonNode> expected = new HashMap<>();
        expect(expected, "suppression_lists", "a@example.test", A, null, one);
        expect(expected, "suppression_lists", "z@example.test", A, null, last);
        assertRows(name, expected);
        consumed(name, 4);
    }

    @Test
    void preservesRepeatedUnzonedTimesAndChecksBothCalendarDateBoundaries() throws Exception {
        String name = "wall-clock-window";
        Settings settings = new Settings(name, "sent_campaigns");
        settings.pageSize = 10;
        settings.start = "2024-11-03";
        settings.end = "2024-11-04";
        String repeated = "2024-11-03 01:30:00";
        String midpoint = "2024-11-03 23:59:59.999999999";
        String first = campaign("repeated-a", repeated);
        String second = campaign("repeated-b", repeated);
        String late = campaign("last-included", midpoint);
        String lower = campaign("lower-bound", "2024-11-03 00:00:00");
        program(
                name,
                List.of(
                        reply(CLIENTS, array(client(A, "First"))),
                        reply(
                                sentTarget(A, 1, settings),
                                page(
                                        1,
                                        10,
                                        13,
                                        2,
                                        campaign("next-day", "2024-11-04 00:00:00"),
                                        first)),
                        reply(
                                sentTarget(A, 2, settings),
                                page(2, 10, 13, 2, second, late, lower))));
        success(run(settings));
        Map<String, JsonNode> expected = new HashMap<>();
        expect(expected, "sent_campaigns", "repeated-a", A, repeated, first);
        expect(expected, "sent_campaigns", "repeated-b", A, repeated, second);
        expect(expected, "sent_campaigns", "last-included", A, midpoint, late);
        expect(expected, "sent_campaigns", "lower-bound", A, "2024-11-03 00:00:00", lower);
        assertRows(name, expected);
        consumed(name, 3);
    }

    @Test
    void usesOnlyTheLowerDateInAnOpenEndedBatchAndAcceptsAnEmptyTerminalPage() throws Exception {
        String name = "open-upper-bound";
        Settings settings = new Settings(name, "sent_campaigns", "suppression_lists");
        settings.end = null;
        program(
                name,
                List.of(
                        reply(CLIENTS, array(client(A, "First"))),
                        reply(paged(A, "suppressionlist", 1, 1000), page(1, 1000, 0, 0)),
                        reply(sentTarget(A, 1, settings), page(1, 1000, 0, 1))));
        success(run(settings));
        assertRows(name, Collections.emptyMap());
        consumed(name, 3);
    }

    @Test
    void rejectsDateScopeErrorsAndUnrecognizedSentDateWithoutPublishingAPage() throws Exception {
        Settings invalid = new Settings("invalid-date", "sent_campaigns");
        invalid.start = "2026-02-30";
        program(invalid.name, List.of());
        failure(run(invalid), "start_date");
        consumed(invalid.name, 0);

        Settings reversed = new Settings("reversed-date", "sent_campaigns");
        reversed.end = reversed.start;
        program(reversed.name, List.of());
        failure(run(reversed), "end_date");
        consumed(reversed.name, 0);

        Settings malformed = new Settings("bad-wire-time", "sent_campaigns");
        program(
                malformed.name,
                List.of(
                        reply(CLIENTS, array(client(A, "First"))),
                        reply(
                                sentTarget(A, 1, malformed),
                                page(
                                        1,
                                        1000,
                                        2,
                                        1,
                                        campaign("valid", SENT),
                                        campaign("invalid", "2026-04-01T00:00:00Z")))));
        failure(run(malformed), "invalid SentDate");
        assertRows(malformed.name, Collections.emptyMap());
        consumed(malformed.name, 2);
    }

    @Test
    void rejectsImpossibleResultsMetadataAndARepeatedPageNumber() throws Exception {
        for (String metadata :
                List.of(
                        page(1, 999, 0, 0),
                        page(1, 1000, 1, 2),
                        page(1, 1000, 0, 1, "{\"EmailAddress\":\"a@example.test\"}"),
                        "{\"Results\":[],\"PageNumber\":\"1\"}")) {
            String name = "bad-meta-" + Math.abs(metadata.hashCode());
            program(
                    name,
                    List.of(
                            reply(CLIENTS, array(client(A, "First"))),
                            reply(paged(A, "suppressionlist", 1, 1000), metadata)));
            failure(run(new Settings(name, "suppression_lists")), "pagination");
            assertRows(name, Collections.emptyMap());
            consumed(name, 2);
        }
        String repeated = "repeated-page-number";
        program(
                repeated,
                List.of(
                        reply(CLIENTS, array(client(A, "First"))),
                        reply(paged(A, "suppressionlist", 1, 1000), page(1, 1000, 1001, 2)),
                        reply(paged(A, "suppressionlist", 2, 1000), page(1, 1000, 1001, 2))));
        failure(
                run(new Settings(repeated, "suppression_lists")),
                "inconsistent pagination metadata");
        consumed(repeated, 3);
    }

    @Test
    void confinesClientPathsAndValidatesTheReturnedClientIdentity() throws Exception {
        String bad = "invalid-client-path";
        program(bad, List.of(reply(CLIENTS, array(client("../another", "Unsafe")))));
        failure(run(new Settings(bad, "people")), "invalid ClientID");
        consumed(bad, 1);

        String mismatch = "changed-detail-client";
        program(
                mismatch,
                List.of(
                        reply(CLIENTS, array(client(A, "First"))),
                        reply(
                                detail(A),
                                "{\"ApiKey\":\"private-provider-message\",\"BasicDetails\":{\"ClientID\":\""
                                        + B
                                        + "\"}}")));
        failure(run(new Settings(mismatch, "client_details")), "do not match the requested client");
        assertRows(mismatch, Collections.emptyMap());
        consumed(mismatch, 2);
    }

    @Test
    void enforcesConfiguredPageScanAndWholeResponseBounds() throws Exception {
        Settings pageLimit = new Settings("max-pages", "suppression_lists");
        pageLimit.pages = 1;
        program(
                pageLimit.name,
                List.of(
                        reply(CLIENTS, array(client(A, "First"))),
                        reply(paged(A, "suppressionlist", 1, 1000), page(1, 1000, 1001, 2))));
        failure(run(pageLimit), "max_pages_per_collection");
        consumed(pageLimit.name, 2);

        Settings scan = new Settings("max-scan", "segments");
        scan.scanPages = 1;
        program(scan.name, List.of(reply(CLIENTS, array(client(A, "First")))));
        failure(run(scan), "max_pages_per_scan");
        consumed(scan.name, 1);

        Settings whole = new Settings("too-many-clients", "clients");
        whole.items = 1;
        program(
                whole.name,
                List.of(reply(CLIENTS, array(client(A, "First"), client(B, "Second")))));
        failure(run(whole), "configured record count");
        assertRows(whole.name, Collections.emptyMap());
        consumed(whole.name, 1);
    }

    @Test
    void surfacesPermissionsAndApiEnvelopeErrorsWithoutLeakingCredentials() throws Exception {
        for (int code : List.of(401, 403, 404)) {
            Settings status = new Settings("http-" + code, "admins");
            status.retries = 3;
            program(
                    status.name,
                    List.of(
                            reply(ADMINS, "private-provider-message " + CampaignMonitorPeer.TOKEN)
                                    .put("status", code)));
            failure(run(status), "HTTP " + code);
            consumed(status.name, 1);
        }
        Settings rejected = new Settings("logical-api-error", "admins");
        program(
                rejected.name,
                List.of(reply(ADMINS, "{\"Code\":401,\"Message\":\"private-provider-message\"}")));
        failure(run(rejected), "API returned an error");
        assertRows(rejected.name, Collections.emptyMap());
        consumed(rejected.name, 1);
    }

    @Test
    void honorsTheResetHintAndRetriesOnlyWithinTheOperationBudget() throws Exception {
        Settings settings = new Settings("throttle-retry", "admins");
        settings.retries = 2;
        String admin = "{\"EmailAddress\":\"admin@example.test\"}";
        program(
                settings.name,
                List.of(
                        reply(ADMINS, "")
                                .put("status", 429)
                                .put("rateReset", "1")
                                .put("retryAfter", "0"),
                        reply(ADMINS, "").put("status", 503),
                        reply(ADMINS, array(admin))));
        success(run(settings));
        List<String> times = engine.lines(settings.name + ".times");
        assertTrue(Long.parseLong(times.get(1)) - Long.parseLong(times.get(0)) >= 850);
        assertTrue(Long.parseLong(times.get(2)) - Long.parseLong(times.get(1)) >= 400);
        Map<String, JsonNode> expected = new HashMap<>();
        expect(expected, "admins", "admin@example.test", null, null, admin);
        assertRows(settings.name, expected);
        consumed(settings.name, 3);

        Settings cap = new Settings("retry-hint-cap", "admins");
        cap.retries = 3;
        cap.retryDelay = 500;
        program(cap.name, List.of(reply(ADMINS, "").put("status", 429).put("rateReset", "2")));
        failure(run(cap), "max_retry_delay_ms");
        consumed(cap.name, 1);
    }

    @Test
    void boundsWireAndGzipBytesAndRejectsMalformedDocuments() throws Exception {
        String huge =
                array(
                        "{\"EmailAddress\":\"a@example.test\",\"Name\":\""
                                + "é".repeat(2000)
                                + "\"}");
        for (String mode : List.of("json", "gzip", "bad-gzip", "trailing", "duplicate", "deep")) {
            Settings settings = new Settings("body-" + mode, "admins");
            String payload = huge;
            if (mode.equals("json") || mode.equals("gzip")) {
                settings.bytes = 256;
            } else if (mode.equals("trailing")) {
                payload = "[] {}";
            } else if (mode.equals("duplicate")) {
                payload =
                        "[{\"EmailAddress\":\"a@example.test\",\"EmailAddress\":\"b@example.test\"}]";
            } else if (mode.equals("deep")) {
                payload =
                        "[{\"EmailAddress\":\"a@example.test\",\"data\":"
                                + "[".repeat(100)
                                + "null"
                                + "]".repeat(100)
                                + "}]";
            }
            program(settings.name, List.of(reply(ADMINS, payload).put("mode", mode)));
            failure(
                    run(settings),
                    mode.equals("json") || mode.equals("gzip")
                            ? "max_response_bytes"
                            : mode.equals("bad-gzip")
                                    ? "invalid gzip response"
                                    : "invalid JSON response");
            assertRows(settings.name, Collections.emptyMap());
            consumed(settings.name, 1);
        }
    }

    @Test
    void verifiesServerCertificatesAndWillNotFollowATrustedRedirect() throws Exception {
        Settings invalid = new Settings("untrusted-cert", "admins");
        invalid.trust = wrongTrust;
        program(invalid.name, List.of());
        failure(run(invalid), "certificate verification");
        consumed(invalid.name, 0);

        Settings redirect = new Settings("redirect-to-trap", "admins");
        program(
                redirect.name,
                List.of(reply(ADMINS, "").put("status", 302).put("mode", "redirect")));
        failure(run(redirect), "HTTP 302");
        consumed(redirect.name, 1);
    }

    @Test
    void endsTricklingResponsesAndCancelsTheActiveSocket() throws Exception {
        String body = "[{\"EmailAddress\":\"a@example.test\"}]";
        Settings deadline = new Settings("deadline", "admins");
        deadline.request = 1500;
        deadline.operation = 1500;
        program(deadline.name, List.of(reply(ADMINS, body).put("mode", "partial")));
        failure(run(deadline), "operation deadline");
        await(deadline.name + ".disconnect");
        assertRows(deadline.name, Collections.emptyMap());
        consumed(deadline.name, 1);

        Settings stop = new Settings("cancel", "admins");
        program(stop.name, List.of(reply(ADMINS, body).put("mode", "partial")));
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(
                        () -> engine.executeJob("/campaignmonitor.conf", job, stop.variables()));
        try {
            await(stop.name + ".started");
            success(engine.cancelJob(job));
            running.get(30, TimeUnit.SECONDS);
            await(stop.name + ".disconnect");
        } finally {
            engine.touch(stop.name + ".release");
            if (!running.isDone()) {
                engine.cancelJob(job);
                running.cancel(true);
            }
        }
        assertRows(stop.name, Collections.emptyMap());
        consumed(stop.name, 1);
    }

    @Test
    void restoresTheSavedChildPageWithoutReopeningAnEarlierClientOrCompletedChild()
            throws Exception {
        Settings settings = new Settings("child-restore", "people", "suppression_lists");
        String roots = array(client(A, "First"), client(B, "Second"));
        String person = "{\"EmailAddress\":\"common@example.test\"}";
        String first = "{\"EmailAddress\":\"first@example.test\"}";
        String second = "{\"EmailAddress\":\"second@example.test\"}";
        ObjectNode pending =
                reply(paged(B, "suppressionlist", 2, 1000), page(2, 1000, 1001, 2, second));
        String job =
                savepointWhilePending(
                        settings,
                        List.of(
                                reply(CLIENTS, roots),
                                reply(scoped(A, "people"), array(person)),
                                reply(
                                        paged(A, "suppressionlist", 1, 1000),
                                        page(1, 1000, 1, 1, first)),
                                reply(scoped(B, "people"), array(person)),
                                reply(
                                        paged(B, "suppressionlist", 1, 1000),
                                        page(1, 1000, 1001, 2, first)),
                                pending.deepCopy().put("mode", "partial")));
        String after = "child-restored";
        program(after, List.of(reply(CLIENTS, roots), pending));
        success(engine.restoreJob("/campaignmonitor.conf", job, settings.variables()));
        Map<String, JsonNode> expected = new HashMap<>();
        expect(expected, "people", "common@example.test", A, null, person);
        expect(expected, "people", "common@example.test", B, null, person);
        expect(expected, "suppression_lists", "first@example.test", A, null, first);
        expect(expected, "suppression_lists", "first@example.test", B, null, first);
        expect(expected, "suppression_lists", "second@example.test", B, null, second);
        assertRows(settings.name, expected);
        consumed(after, 2);
    }

    @Test
    void failsAChangedSavedClientPageBeforeContactingItsChild() throws Exception {
        Settings settings = new Settings("parent-old", "people");
        String job =
                savepointWhilePending(
                        settings,
                        List.of(
                                reply(CLIENTS, array(client(A, "First"))),
                                reply(scoped(A, "people"), "[]").put("mode", "partial")));
        String name = "parent-changed";
        program(name, List.of(reply(CLIENTS, array(client(B, "Second")))));
        failure(
                engine.restoreJob("/campaignmonitor.conf", job, settings.variables()),
                "checkpoint page changed");
        consumed(name, 1);
        assertRows(settings.name, Collections.emptyMap());
    }

    @Test
    void restoresTerminalPositionWhileAnotherPipelineKeepsTheBatchAlive() throws Exception {
        Settings settings = new Settings("terminal", "admins");
        String admin = "{\"EmailAddress\":\"admin@example.test\"}";
        program(settings.name, List.of(reply(ADMINS, array(admin))));
        Map<String, JsonNode> expected = new HashMap<>();
        expect(expected, "admins", "admin@example.test", null, null, admin);
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(
                        () ->
                                engine.executeJob(
                                        "/campaignmonitor-terminal.conf",
                                        job,
                                        settings.variables()));
        try {
            Awaitility.await()
                    .atMost(35, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertRows(settings.name, expected));
            success(engine.savepointJob(job));
            running.get(30, TimeUnit.SECONDS);
        } finally {
            if (!running.isDone()) {
                engine.cancelJob(job);
                running.cancel(true);
            }
        }
        consumed(settings.name, 1);
        String after = "terminal-restored";
        program(after, List.of());
        Future<Container.ExecResult> resumed =
                tasks.submit(
                        () ->
                                engine.restoreJob(
                                        "/campaignmonitor-terminal.conf",
                                        job,
                                        settings.variables()));
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> "RUNNING".equals(engine.getJobStatus(job)));
            long baseline = engine.completedAcrossPipelines(job);
            Awaitility.await()
                    .atMost(35, TimeUnit.SECONDS)
                    .until(() -> engine.completedAcrossPipelines(job) > baseline);
            assertRows(settings.name, expected);
            consumed(after, 0);
        } finally {
            if (!resumed.isDone()) {
                engine.cancelJob(job);
            }
            resumed.get(30, TimeUnit.SECONDS);
        }
    }

    @Test
    void resumesTheExactPackagedReaderRowAndDetectsAChangedChildPage() throws Exception {
        String first = "{\"EmailAddress\":\"first@example.test\",\"Name\":\"initial\"}";
        String second = "{\"EmailAddress\":\"second@example.test\",\"Name\":\"second\"}";
        String changed = "{\"EmailAddress\":\"second@example.test\",\"Name\":\"mutated\"}";
        ObjectNode root = reply(CLIENTS, array(client(A, "First")));
        for (String mode : List.of("resume", "changed", "zero")) {
            String name = "packaged-" + mode;
            List<ObjectNode> script =
                    mode.equals("zero")
                            ? List.of()
                            : List.of(
                                    root,
                                    reply(scoped(A, "people"), array(first, second)),
                                    root.deepCopy(),
                                    reply(
                                            scoped(A, "people"),
                                            array(
                                                    first,
                                                    mode.equals("changed") ? changed : second)));
            program(name, script);
            success(engine.probe(mode));
            consumed(name, script.size());
        }
    }

    private String savepointWhilePending(Settings settings, List<ObjectNode> scripted)
            throws Exception {
        settings.request = 120000;
        settings.operation = 180000;
        program(settings.name, scripted);
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(
                        () ->
                                engine.executeJob(
                                        "/campaignmonitor.conf", job, settings.variables()));
        try {
            await(settings.name + ".started");
            long initial = engine.getCompletedCheckpointCount(job);
            Awaitility.await()
                    .atMost(35, TimeUnit.SECONDS)
                    .until(() -> engine.getCompletedCheckpointCount(job) > initial);
            success(engine.savepointJob(job));
            running.get(30, TimeUnit.SECONDS);
        } finally {
            engine.touch(settings.name + ".release");
            if (!running.isDone()) {
                engine.cancelJob(job);
                running.cancel(true);
            }
        }
        consumed(settings.name, scripted.size());
        return job;
    }

    private static String detail(String client) {
        return API + "/clients/" + client + ".json";
    }

    private static String scoped(String client, String collection) {
        return API + "/clients/" + client + "/" + collection + ".json";
    }

    private static String paged(String client, String collection, int page, int size) {
        return scoped(client, collection) + "?page=" + page + "&pagesize=" + size;
    }

    private static String sentTarget(String client, int page, Settings settings) {
        return paged(client, "campaigns", page, settings.pageSize)
                + "&sentFromDate="
                + settings.start
                + (settings.end == null ? "" : "&sentToDate=" + settings.end);
    }

    private static String client(String id, String name) {
        return JSON.createObjectNode().put("ClientID", id).put("Name", name).toString();
    }

    private static String campaign(String id, String sent) {
        return JSON.createObjectNode().put("CampaignID", id).put("SentDate", sent).toString();
    }

    private static String array(String... items) {
        return "[" + String.join(",", items) + "]";
    }

    private static String page(int index, int limit, int total, int pages, String... items) {
        return "{\"Results\":"
                + array(items)
                + ",\"PageNumber\":"
                + index
                + ",\"PageSize\":"
                + limit
                + ",\"RecordsOnThisPage\":"
                + items.length
                + ",\"TotalNumberOfRecords\":"
                + total
                + ",\"NumberOfPages\":"
                + pages
                + "}";
    }

    private static ObjectNode reply(String target, String response) {
        return JSON.createObjectNode().put("target", target).put("response", response);
    }

    private static void expect(
            Map<String, JsonNode> expected,
            String resource,
            String id,
            String client,
            String sent,
            String raw)
            throws Exception {
        ObjectNode row = JSON.createObjectNode().put("resource", resource).put("id", id);
        row.put("client_id", client);
        row.put("sent_date", sent);
        row.set("record", JSON.readTree(raw));
        assertNull(expected.put(rowKey(row), row));
    }

    private void assertRows(String name, Map<String, JsonNode> expected) throws Exception {
        Map<String, JsonNode> actual = new HashMap<>();
        for (String line : engine.rows(name)) {
            ObjectNode row = (ObjectNode) JSON.readTree(line);
            Set<String> fields = new HashSet<>();
            row.fieldNames().forEachRemaining(fields::add);
            assertEquals(Set.of("resource", "id", "client_id", "sent_date", "record"), fields);
            assertTrue(
                    row.path("record").isTextual(),
                    "the typed record is encoded in one string column");
            row.set("record", JSON.readTree(row.path("record").asText()));

            assertNull(actual.put(rowKey(row), row));
        }
        assertEquals(expected, actual);
    }

    private static String rowKey(ObjectNode row) {
        return row.path("resource").asText()
                + "/"
                + row.path("id").asText()
                + "/"
                + row.path("client_id").toString();
    }

    private void program(String name, List<ObjectNode> steps) throws Exception {
        rejectsUnexpectedOrCredentialBearingRequests();
        ArrayNode script = JSON.createArrayNode();
        steps.forEach(script::add);
        Path file = Files.createTempFile(directory, "script-", ".json");
        Files.writeString(file, script.toString());
        engine.install(file, name + ".json");
        Path current = Files.createTempFile(directory, "active-", ".txt");
        Files.writeString(current, name);
        engine.install(current, "active");
        active = name;
    }

    private void consumed(String name, int count) throws Exception {
        assertFalse(engine.exists(name + ".invalid"), name);
        assertEquals(count, engine.lines(name + ".requests").size(), name);
    }

    private void await(String marker) {
        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> engine.exists(marker));
    }

    private Container.ExecResult run(Settings settings) throws Exception {
        return engine.executeJob("/campaignmonitor.conf", jobId(), settings.variables());
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

    private static void failure(Container.ExecResult result, String reason) {
        assertNotEquals(0, result.getExitCode(), () -> text(result));
        assertTrue(text(result).contains(reason), () -> text(result));
        for (String secret :
                List.of(
                        CampaignMonitorPeer.TOKEN,
                        CampaignMonitorPeer.OTHER_TOKEN,
                        Base64.getEncoder()
                                .encodeToString(
                                        (CampaignMonitorPeer.TOKEN + ":")
                                                .getBytes(StandardCharsets.US_ASCII)),
                        "private-provider-message")) {
            assertFalse(
                    text(result).contains(secret),
                    "gateway credential or server text reached a diagnostic");
        }
    }

    private static String identity(String name) throws Exception {
        Path store = directory.resolve(name + ".p12");
        Process keytool =
                new ProcessBuilder(
                                Path.of(System.getProperty("java.home"), "bin", "keytool")
                                        .toString(),
                                "-genkeypair",
                                "-alias",
                                "gateway",
                                "-keyalg",
                                "RSA",
                                "-keysize",
                                "2048",
                                "-validity",
                                "7",
                                "-dname",
                                "CN=" + CampaignMonitorPeer.HOST,
                                "-ext",
                                "SAN=dns:"
                                        + CampaignMonitorPeer.HOST
                                        + ",dns:"
                                        + CampaignMonitorPeer.TRAP,
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                store.toString(),
                                "-storepass",
                                CampaignMonitorPeer.PASSWORD,
                                "-keypass",
                                CampaignMonitorPeer.PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String output = new String(keytool.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, keytool.waitFor(), output);
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(store)) {
            keyStore.load(input, CampaignMonitorPeer.PASSWORD.toCharArray());
        }
        return Base64.getEncoder().encodeToString(keyStore.getCertificate("gateway").getEncoded());
    }

    /**
     * Synthetic job settings with deterministic time boundaries and explicit limits.
     *
     * <p>Each invocation passes a fresh argument array to the real engine container.
     */
    private static final class Settings {
        final String name;
        final String[] resources;
        String trust = CampaignMonitorIT.trust;
        String start = "2026-04-01";
        String end = "2026-04-03";
        int request = 60000;
        int operation = 70000;
        int retries;
        int retryDelay = 60000;
        int bytes = 8388608;
        int pages = 10000;
        int scanPages = 100000;
        int items = 10000;
        int pageSize = 1000;

        Settings(String name, String... resources) {
            this.name = name;
            this.resources = resources;
        }

        String[] variables() {
            ArrayNode selection = JSON.createArrayNode();
            for (String resource : resources) {
                selection.add(resource);
            }
            List<String> values =
                    new ArrayList<>(
                            List.of(
                                    "api.output=" + name,
                                    "api.resources=" + selection,
                                    "api.token=" + CampaignMonitorPeer.TOKEN,
                                    "api.certificate=" + trust,
                                    "api.request_timeout=" + request,
                                    "api.operation_timeout=" + operation,
                                    "api.retries=" + retries,
                                    "api.retry_delay=" + retryDelay,
                                    "api.bytes=" + bytes,
                                    "api.pages=" + pages,
                                    "api.scan_pages=" + scanPages,
                                    "api.items=" + items,
                                    "api.page_size=" + pageSize));
            if (start != null) {
                values.add("api.start=" + start);
            }
            if (end != null) {
                values.add("api.end=" + end);
            }
            return values.toArray(new String[0]);
        }
    }

    /**
     * Stages the independent peer and existing shaded JSON libraries in the test engine.
     *
     * <p>Host aliases and test certificates belong only to this owned container.
     */
    private static final class Engine extends SeaTunnelContainer {
        private static final String ROOT = "/tmp/campaignmonitor-peer";
        private final Path files;
        private final HttpClient checkpoints =
                HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

        private Engine(Path files) {
            this.files = files;
        }

        /**
         * Preserves individual argv values without logging the synthetic credential settings.
         *
         * <p>The engine receives test variables as individual arguments.
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
            for (String host : List.of(CampaignMonitorPeer.HOST, CampaignMonitorPeer.TRAP)) {
                container.withExtraHost(host, "127.0.0.1");
            }
            container.withCopyFileToContainer(
                    MountableFile.forHostPath(files.resolve("trusted.p12")), ROOT + "/trusted.p12");
            container.withCopyFileToContainer(
                    MountableFile.forHostPath(files.resolve("trusted.pem")), ROOT + "/trusted.pem");
            try {
                stageClasses(container, CampaignMonitorPeer.class);
                stageClasses(container, CampaignMonitorRecoveryProbe.class);
                Set<Path> libraries = new HashSet<>();
                for (Class<?> type :
                        List.of(ObjectMapper.class, JsonFactory.class, JsonProperty.class)) {
                    Path jar =
                            Path.of(
                                    type.getProtectionDomain()
                                            .getCodeSource()
                                            .getLocation()
                                            .toURI());
                    if (libraries.add(jar)) {
                        container.withCopyFileToContainer(
                                MountableFile.forHostPath(jar),
                                ROOT + "/libs/" + jar.getFileName());
                    }
                }
            } catch (URISyntaxException e) {
                throw new IOException("Cannot stage the independent Java HTTPS peer", e);
            }
        }

        private static void stageClasses(GenericContainer<?> container, Class<?> type)
                throws IOException, URISyntaxException {
            Path parent = Path.of(type.getResource("").toURI());
            try (DirectoryStream<Path> files =
                    Files.newDirectoryStream(parent, type.getSimpleName() + "*.class")) {
                for (Path file : files) {
                    container.withCopyFileToContainer(
                            MountableFile.forHostPath(file),
                            ROOT
                                    + "/classes/"
                                    + type.getPackage().getName().replace('.', '/')
                                    + "/"
                                    + file.getFileName());
                }
            }
        }

        private Container.ExecResult peer() throws Exception {
            return server.execInContainer(
                    "java",
                    "-cp",
                    ROOT + "/classes:" + ROOT + "/libs/*",
                    CampaignMonitorPeer.class.getName(),
                    ROOT);
        }

        private Container.ExecResult probe(String mode) throws Exception {
            ContainerUtil.copyAllConnectorJarToContainer(
                    server,
                    getConnectorModulePath(),
                    getConnectorNamePrefix(),
                    getConnectorType(),
                    SEATUNNEL_HOME);
            return server.execInContainer(
                    "java",
                    "-cp",
                    ROOT
                            + "/classes:"
                            + ROOT
                            + "/libs/*:/tmp/seatunnel/lib/*:/tmp/seatunnel/connectors/*",
                    CampaignMonitorRecoveryProbe.class.getName(),
                    ROOT + "/trusted.pem",
                    mode);
        }

        private void install(Path file, String name) {
            server.copyFileToContainer(MountableFile.forHostPath(file), ROOT + "/" + name);
        }

        private void touch(String name) throws Exception {
            success(server.execInContainer("touch", ROOT + "/" + name));
        }

        private boolean exists(String name) throws Exception {
            return server.execInContainer("test", "-f", ROOT + "/" + name).getExitCode() == 0;
        }

        private List<String> lines(String name) throws Exception {
            if (!exists(name)) {
                return Collections.emptyList();
            }
            Container.ExecResult result = server.execInContainer("cat", ROOT + "/" + name);
            success(result);
            return linesOf(result.getStdout());
        }

        private List<String> rows(String name) throws Exception {
            String path = "/tmp/seatunnel_mnt/campaignmonitor-" + name;
            Container.ExecResult result =
                    server.execInContainer(
                            "sh",
                            "-c",
                            "if [ -d "
                                    + path
                                    + " ]; then find "
                                    + path
                                    + " -maxdepth 1 -type f -name 'receipt_*' -exec sh -c 'for f do cat \"$f\"; printf \"\\n\"; done' sh {} +; fi");
            success(result);
            return linesOf(result.getStdout());
        }

        private long completedAcrossPipelines(String id) throws Exception {
            URI uri =
                    new URI(
                            "http",
                            null,
                            server.getHost(),
                            server.getMappedPort(8080),
                            "/jobs/checkpoints/" + id,
                            null,
                            null);
            HttpResponse<String> response =
                    checkpoints.send(
                            HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(5)).build(),
                            BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                return 0L;
            }
            long completed = 0L;
            for (JsonNode pipeline : JSON.readTree(response.body()).path("pipelines")) {
                completed += pipeline.path("counts").path("completed").asLong();
            }
            return completed;
        }

        private static List<String> linesOf(String value) {
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
