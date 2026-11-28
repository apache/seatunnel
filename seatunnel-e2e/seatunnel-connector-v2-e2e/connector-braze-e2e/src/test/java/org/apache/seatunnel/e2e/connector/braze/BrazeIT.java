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

package org.apache.seatunnel.e2e.connector.braze;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.seatunnel.SeaTunnelContainer;

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
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the shipped Braze artifact in Java 11 Zeta.
 *
 * <p>A separate HTTPS peer checks exact queries and scoped authorization. Rows are committed to a
 * transactional LocalFile sink and compared by value.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BrazeIT extends TestSuiteBase {
    private static final String CONF = "/braze.conf";
    private static final String ROOT = "/tmp/braze-https-fixture";
    private static final String OUTPUT = "/tmp/seatunnel_mnt/braze-";
    private static final String CAMPAIGNS = "/campaigns/list";
    private static final String CANVASES = "/canvas/list";
    private static final String EVENTS = "/events/list";
    private static final String SEGMENTS = "/segments/list";
    private static final String CARDS = "/feed/list";
    private static final String FIRST = "2024-01-01";
    private static final String LAST = "2024-01-16";
    private static final ObjectMapper JSON =
            new ObjectMapper()
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));

    @TempDir static Path directory;
    private final ExecutorService tasks = Executors.newCachedThreadPool();
    private Engine engine;
    private Future<Container.ExecResult> peer;
    private String active;

    @BeforeAll
    void startFixture() throws Exception {
        char[] password = BrazePeer.PASSWORD.toCharArray();
        KeyStore identities = KeyStore.getInstance("PKCS12");
        identities.load(null, password);
        KeyStore trust = KeyStore.getInstance("JKS");
        trust.load(null, password);
        Map<String, List<String>> names =
                Map.of(
                        "trusted",
                                List.of(
                                        BrazePeer.HOST,
                                        BrazePeer.EU,
                                        BrazePeer.JP,
                                        BrazePeer.AU,
                                        BrazePeer.REDIRECT),
                        "untrusted", List.of(BrazePeer.HOST),
                        "wrong", List.of(BrazePeer.MISMATCH));
        for (Map.Entry<String, List<String>> entry : names.entrySet()) {
            String alias = entry.getKey();
            KeyStore identity = identity(alias, entry.getValue());
            identities.setKeyEntry(
                    alias,
                    identity.getKey(alias, password),
                    password,
                    identity.getCertificateChain(alias));
            if (!"untrusted".equals(alias)) {
                trust.setCertificateEntry(alias, identity.getCertificate(alias));
            }
        }
        try (OutputStream out = Files.newOutputStream(directory.resolve("identities.p12"))) {
            identities.store(out, password);
        }
        try (OutputStream out = Files.newOutputStream(directory.resolve("trust.jks"))) {
            trust.store(out, password);
        }
        engine = new Engine(directory);
        engine.startUp();
        peer = tasks.submit(engine::runPeer);
        Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> engine.exists("ready"));
    }

    @AfterEach
    void verifyProviderAssertions() throws Exception {
        if (active != null) {
            assertFalse(engine.exists(active + ".invalid"), engine.contents(active + ".invalid"));
        }
    }

    @AfterAll
    void stopFixture() throws Exception {
        try {
            if (engine != null) {
                try {
                    if (peer != null) {
                        engine.touch("stop");
                        success(peer.get(15, TimeUnit.SECONDS));
                    }
                } finally {
                    engine.tearDown();
                }
            }
        } finally {
            tasks.shutdownNow();
        }
    }

    @Test
    void readsEveryExportRouteAndPersistsExactProviderRecords() throws Exception {
        String campaign =
                "{\"id\":\"c+ /a&\",\"name\":\"中文\",\"last_edited\":null,"
                        + "\"numeric\":90071992547409931234567,\"ratio\":0.123456789012345600}";
        String canvasRow = "{\"id\":\"canvas-1\",\"name\":\"Welcome\",\"tags\":[\"greeting\"]}";
        String segment = "{\"id\":\"s1\",\"analytics_tracking_enabled\":false,\"tags\":[]}";
        String card = "{\"id\":\"card-1\",\"title\":\"News\",\"type\":\"text_announcement\"}";
        String event = "中文 + /&";

        export(
                new Settings("full-campaigns", "campaigns"),
                List.of(reply(numbered(CAMPAIGNS, 0), 200, "gzip", list("campaigns", campaign))),
                expected("c+ /a&", null, campaign));
        export(
                new Settings("full-canvases", "canvases"),
                List.of(reply(numbered(CANVASES, 0), list("canvases", canvasRow))),
                expected("canvas-1", null, canvasRow));
        export(
                new Settings("full-segments", "segments"),
                List.of(reply(numbered(SEGMENTS, 0), list("segments", segment))),
                expected("s1", null, segment));
        export(
                new Settings("full-events", "events"),
                List.of(reply(numbered(EVENTS, 0), list("events", "\"" + event + "\""))),
                expected(event, null, "{\"event_name\":\"" + event + "\"}"));
        export(
                new Settings("full-cards", "cards"),
                List.of(
                        reply(numbered(CARDS, 0), list("cards", card)),
                        reply(numbered(CARDS, 1), list("cards"))),
                expected("card-1", null, card));

        String campaignDetail = detail("Campaign");
        export(
                new Settings("full-campaign-details", "campaigns_details"),
                List.of(
                        reply(numbered(CAMPAIGNS, 0), list("campaigns", campaign)),
                        reply("/campaigns/details?campaign_id=c%2B+%2Fa%26", campaignDetail)),
                expected("c+ /a&", null, lineage(campaignDetail, "campaign_id", "c+ /a&")));
        String canvasDetail = detail("Canvas");
        export(
                new Settings("full-canvas-details", "canvases_details"),
                List.of(
                        reply(numbered(CANVASES, 0), list("canvases", canvasRow)),
                        reply("/canvas/details?canvas_id=canvas-1", canvasDetail)),
                expected("canvas-1", null, lineage(canvasDetail, "canvas_id", "canvas-1")));
        String segmentDetail = detail("Segment");
        export(
                new Settings("full-segment-details", "segments_details"),
                List.of(
                        reply(numbered(SEGMENTS, 0), list("segments", segment)),
                        reply("/segments/details?segment_id=s1", segmentDetail)),
                expected("s1", null, lineage(segmentDetail, "segment_id", "s1")));

        String campaignStats =
                "{\"time\":\"2024-01-01\",\"messages\":{\"email\":[{\"sent\":23}]},\"revenue\":3.1400}";
        export(
                days("full-campaign-analytics", "campaigns_analytics", FIRST, FIRST),
                List.of(
                        reply(numbered(CAMPAIGNS, 0), list("campaigns", campaign)),
                        reply(
                                series(
                                        "/campaigns/data_series",
                                        "campaign_id=c%2B+%2Fa%26",
                                        1,
                                        FIRST),
                                list("data", campaignStats))),
                expected("c+ /a&", FIRST, lineage(campaignStats, "campaign_id", "c+ /a&")));
        String canvasStats =
                "{\"time\":\"2024-01-01\",\"total_stats\":{\"entries\":8},"
                        + "\"variant_stats\":{\"variant-1\":{\"revenue\":2.10}},\"step_stats\":{\"step-1\":{\"clicks\":0}}}";
        export(
                days("full-canvas-analytics", "canvases_analytics", FIRST, FIRST),
                List.of(
                        reply(numbered(CANVASES, 0), list("canvases", canvasRow)),
                        reply(canvasTarget("canvas-1", 1, FIRST), canvas(canvasStats))),
                expected("canvas-1", FIRST, lineage(canvasStats, "canvas_id", "canvas-1")));
        String eventStats = "{\"time\":\"2024-01-01\",\"count\":9007199254740993}";
        export(
                days("full-event-analytics", "events_analytics", FIRST, FIRST),
                List.of(
                        reply(numbered(EVENTS, 0), list("events", "\"" + event + "\"")),
                        reply(
                                series(
                                                "/events/data_series",
                                                "event=%E4%B8%AD%E6%96%87+%2B+%2F%26",
                                                1,
                                                FIRST)
                                        + "&unit=day",
                                list("data", eventStats))),
                expected(event, FIRST, lineage(eventStats, "event_name", event)));
        String segmentStats = "{\"time\":\"2024-01-01\",\"size\":17}";
        export(
                days("full-segment-analytics", "segments_analytics", FIRST, FIRST),
                List.of(
                        reply(numbered(SEGMENTS, 0), list("segments", segment)),
                        reply(
                                series("/segments/data_series", "segment_id=s1", 1, FIRST),
                                list("data", segmentStats))),
                expected("s1", FIRST, lineage(segmentStats, "segment_id", "s1")));
        String cardStats =
                "{\"time\":\"2024-01-01\",\"clicks\":3,\"impressions\":18,"
                        + "\"unique_clicks\":2,\"unique_impressions\":12}";
        export(
                days("full-card-analytics", "cards_analytics", FIRST, FIRST),
                List.of(
                        reply(numbered(CARDS, 0), list("cards", card)),
                        reply(
                                series("/feed/data_series", "card_id=card-1", 1, FIRST)
                                        + "&unit=day",
                                list("data", cardStats)),
                        reply(numbered(CARDS, 1), list("cards"))),
                expected("card-1", FIRST, lineage(cardStats, "card_id", "card-1")));

        kpi("kpi_daily_new_users", "/kpi/new_users/data_series", "new_users", 34);
        kpi("kpi_daily_active_users", "/kpi/dau/data_series", "dau", 512);
        kpi("kpi_daily_app_uninstalls", "/kpi/uninstalls/data_series", "uninstalls", 0);
    }

    @Test
    void honorsActualPageSizesAndContinuesThroughEveryParentPage() throws Exception {
        String[] events = new String[250];
        String[] expectedEvents = new String[251];
        for (int i = 0; i < events.length; i++) {
            String name = String.format(java.util.Locale.ROOT, "e-%03d", i);
            events[i] = "\"" + name + "\"";
            expectedEvents[i] = expected(name, null, "{\"event_name\":\"" + name + "\"}");
        }
        expectedEvents[250] = expected("z-tail", null, "{\"event_name\":\"z-tail\"}");
        export(
                new Settings("event-pages", "events"),
                List.of(
                        reply(numbered(EVENTS, 0), list("events", events)),
                        reply(numbered(EVENTS, 1), list("events", "\"z-tail\""))),
                expectedEvents);

        List<ObjectNode> program = new ArrayList<>();
        List<String> wanted = new ArrayList<>();
        program.add(reply(numbered(CAMPAIGNS, 0), list("campaigns", numberedRecords("c", 100))));
        for (int i = 0; i < 101; i++) {
            String id = "c" + i;
            if (i == 100) {
                program.add(reply(numbered(CAMPAIGNS, 1), list("campaigns", "{\"id\":\"c100\"}")));
            }
            String data = detail("item-" + i);
            program.add(reply("/campaigns/details?campaign_id=" + id, data));
            wanted.add(expected(id, null, lineage(data, "campaign_id", id)));
        }
        export(
                new Settings("parent-pages", "campaigns_details"),
                program,
                wanted.toArray(new String[0]));
    }

    @Test
    void appliesListEditFiltersButKeepsAnalyticsDiscoveryIndependent() throws Exception {
        String row = "{\"id\":\"older-campaign\",\"last_edited\":\"2023-03-01T00:00:00Z\"}";
        for (String family : List.of("campaigns", "canvases", "cards")) {
            String path =
                    "campaigns".equals(family)
                            ? CAMPAIGNS
                            : "canvases".equals(family) ? CANVASES : CARDS;
            export(
                    new Settings("filtered-" + family, family).set("start", FIRST),
                    List.of(
                            reply(
                                    numbered(path, 0)
                                            + "&last_edit.time%5Bgt%5D=2024-01-01T00%3A00%3A00Z",
                                    list(family))));
        }
        String stats = "{\"time\":\"2024-01-01\",\"conversions\":1}";
        export(
                days("discover-old-parent", "campaigns_analytics", FIRST, FIRST),
                List.of(
                        reply(numbered(CAMPAIGNS, 0), list("campaigns", row)),
                        reply(
                                series(
                                        "/campaigns/data_series",
                                        "campaign_id=older-campaign",
                                        1,
                                        FIRST),
                                list("data", stats))),
                expected("older-campaign", FIRST, lineage(stats, "campaign_id", "older-campaign")));
    }

    @Test
    void usesInclusiveHundredDayWindowsAndKeepsTheLastPartialWindow() throws Exception {
        String first = "{\"time\":\"2024-01-01\",\"new_users\":1}";
        String boundary = "{\"time\":\"2024-04-09\",\"new_users\":100}";
        String second = "{\"time\":\"2024-04-10\",\"new_users\":101}";
        String last = "{\"time\":\"2024-04-11\",\"new_users\":102}";
        export(
                days("hundred-day", "kpi_daily_new_users", FIRST, "2024-04-11"),
                List.of(
                        reply(
                                series("/kpi/new_users/data_series", "", 100, "2024-04-09"),
                                list("data", first, boundary)),
                        reply(
                                series("/kpi/new_users/data_series", "", 2, "2024-04-11"),
                                list("data", second, last))),
                expected(null, FIRST, first),
                expected(null, "2024-04-09", boundary),
                expected(null, "2024-04-10", second),
                expected(null, "2024-04-11", last));
    }

    @Test
    void scopesEachWorkspaceKeyToItsOwnRestInstance() throws Exception {
        for (Map.Entry<String, String> instance :
                Map.of("eu-01", BrazePeer.EU, "jp-01", BrazePeer.JP, "au-01", BrazePeer.AU)
                        .entrySet()) {
            String tenant = instance.getKey();
            String key = "fixture-token-" + tenant;
            String value = "\"" + tenant + "\"";
            Settings run =
                    new Settings("instance-" + tenant, "events")
                            .set("instance", tenant)
                            .set("token", key);
            export(
                    run,
                    List.of(
                            reply(numbered(EVENTS, 0), list("events", value))
                                    .put("host", instance.getValue())
                                    .put("key", key)),
                    expected(tenant, null, "{\"event_name\":" + value + "}"));
        }
    }

    @Test
    void preservesEmptySuccessButFailsAuthenticationPermissionAndLegacyRemoval() throws Exception {
        export(
                new Settings("empty-event-list", "events_analytics").set("start", FIRST),
                List.of(reply(numbered(EVENTS, 0), list("events"))));
        Settings unauthorized = new Settings("bad-key", "segments");
        program(unauthorized, reply(numbered(SEGMENTS, 0), 401, "", BrazePeer.PRIVATE));
        failure(run(unauthorized), "HTTP 401");
        assertEquals(1, requests(unauthorized).size());

        Settings forbidden = new Settings("bad-scope", "segments_details");
        program(
                forbidden,
                reply(numbered(SEGMENTS, 0), list("segments", "{\"id\":\"s1\"}")),
                reply("/segments/details?segment_id=s1", 403, "", BrazePeer.PRIVATE));
        failure(run(forbidden), "HTTP 403");
        assertEquals(2, requests(forbidden).size());
        assertTrue(rows(forbidden).isEmpty());

        Settings removedList = new Settings("legacy-list-unavailable", "cards");
        program(removedList, reply(numbered(CARDS, 0), 404, "", BrazePeer.PRIVATE));
        failure(run(removedList), "legacy News Feed export is unavailable (HTTP 404)");
        assertEquals(1, requests(removedList).size());

        Settings removedAnalytics =
                days("legacy-series-unavailable", "cards_analytics", FIRST, FIRST);
        program(
                removedAnalytics,
                reply(numbered(CARDS, 0), list("cards", "{\"id\":\"n1\"}")),
                reply(
                        series("/feed/data_series", "card_id=n1", 1, FIRST) + "&unit=day",
                        410,
                        "",
                        BrazePeer.PRIVATE));
        failure(run(removedAnalytics), "legacy News Feed export is unavailable (HTTP 410)");
        assertEquals(2, requests(removedAnalytics).size());
    }

    @Test
    void failsFalseSuccessAndMalformedResponsesWithoutLeakingThem() throws Exception {
        Map<String, String> bad =
                Map.of(
                        "false-success",
                        "{\"message\":\"" + BrazePeer.PRIVATE + "\",\"events\":[]}",
                        "partial-success",
                        "{\"message\":\"success\",\"errors\":[\""
                                + BrazePeer.PRIVATE
                                + "\"],\"events\":[]}",
                        "wrong-shape",
                        "{\"message\":\"success\",\"events\":{}}",
                        "wrong-entry",
                        list("events", "{\"name\":\"x\"}"),
                        "duplicate-key",
                        "{\"message\":\"success\",\"events\":[],\"events\":[]}",
                        "duplicate-event",
                        list("events", "\"duplicate\"", "\"duplicate\""));
        for (Map.Entry<String, String> failure : bad.entrySet()) {
            Settings options = new Settings(failure.getKey(), "events");
            program(options, reply(numbered(EVENTS, 0), failure.getValue()));
            failure(run(options), "Braze resource events");
            assertTrue(rows(options).isEmpty());
        }
    }

    @Test
    void rejectsMismatchedLineageAndOutOfWindowDatesBeforeOutput() throws Exception {
        Settings own = new Settings("wrong-detail-lineage", "campaigns_details");
        program(
                own,
                reply(numbered(CAMPAIGNS, 0), list("campaigns", "{\"id\":\"first\"}")),
                reply(
                        "/campaigns/details?campaign_id=first",
                        "{\"message\":\"success\",\"name\":\"x\",\"campaign_id\":\"other\"}"));
        failure(run(own), "lineage does not match");
        assertTrue(rows(own).isEmpty());

        Settings dates = days("out-of-window", "kpi_daily_new_users", FIRST, FIRST);
        program(
                dates,
                reply(
                        series("/kpi/new_users/data_series", "", 1, FIRST),
                        list("data", "{\"time\":\"2024-01-02\",\"new_users\":42}")));
        failure(run(dates), "bucket is outside the requested window");
        assertTrue(rows(dates).isEmpty());
    }

    @Test
    void abortsRepeatedPagesAndFailsAtTheConfiguredPageLimit() throws Exception {
        String full = list("segments", numberedRecords("s", 100));
        Settings repeated = new Settings("repeated-page", "segments");
        program(repeated, reply(numbered(SEGMENTS, 0), full), reply(numbered(SEGMENTS, 1), full));
        failure(run(repeated), "numbered pagination returned a repeated page");
        assertEquals(2, requests(repeated).size());
        Settings limited = new Settings("max-pages", "segments").set("maxPages", 1);
        program(limited, reply(numbered(SEGMENTS, 0), full));
        failure(run(limited), "pagination reached max_pages before completion");
        assertEquals(1, requests(limited).size());
    }

    @Test
    void enforcesWireAndExpandedResponseBudgets() throws Exception {
        for (String mode : List.of("", "gzip")) {
            Settings options =
                    new Settings(mode.isEmpty() ? "wire-limit" : "gzip-limit", "segments")
                            .set("bytes", 1024);
            String data = "{\"id\":\"s1\",\"name\":\"" + BrazePeer.PRIVATE.repeat(110) + "\"}";
            program(options, reply(numbered(SEGMENTS, 0), 200, mode, list("segments", data)));
            failure(run(options), "max_response_bytes");
            assertTrue(rows(options).isEmpty());
        }
    }

    @Test
    void followsBrazeThrottleHeadersWithinTheFiniteDeadline() throws Exception {
        Settings retry = new Settings("vendor-retry", "segments");
        program(
                retry,
                reply(numbered(SEGMENTS, 0), 429, "", BrazePeer.PRIVATE)
                        .put("brazeRetryAfter", "1")
                        .put("brazeResetIn", 120),
                reply(numbered(SEGMENTS, 0), list("segments", "{\"id\":\"s1\"}")));
        success(run(retry));
        assertRows(retry, expected("s1", null, "{\"id\":\"s1\"}"));
        List<JsonNode> calls = requests(retry);
        assertEquals(2, calls.size());
        assertTrue(
                calls.get(1).path("nanoTime").asLong() - calls.get(0).path("nanoTime").asLong()
                        >= TimeUnit.MILLISECONDS.toNanos(950));

        Settings budget = new Settings("vendor-reset-budget", "segments").set("deadline", 10000);
        program(
                budget,
                reply(numbered(SEGMENTS, 0), 429, "", BrazePeer.PRIVATE).put("brazeResetIn", 60));
        failure(run(budget), "retry delay exceeds request deadline");
        assertEquals(1, requests(budget).size());

        Settings finite = new Settings("retry-limit", "segments").set("attempts", 2);
        program(
                finite,
                reply(numbered(SEGMENTS, 0), 429, "", BrazePeer.PRIVATE),
                reply(numbered(SEGMENTS, 0), 503, "", BrazePeer.PRIVATE));
        failure(run(finite), "retry limit reached after HTTP 503");
        assertEquals(2, requests(finite).size());
    }

    @Test
    void resumesAtTheHeldCanvasWindowThenReadsTheNextParent() throws Exception {
        Settings settings =
                days("canvas-savepoint", "canvases_analytics", FIRST, LAST).set("deadline", 120000);
        String parents = list("canvases", "{\"id\":\"a\",\"name\":\"a\"}", "{\"id\":\"b\"}");
        String sameIds =
                list("canvases", "{\"id\":\"a\",\"name\":\"after edit\"}", "{\"id\":\"b\"}");
        String day14 = "{\"time\":\"2024-01-14\",\"total_stats\":{\"entries\":14}}";
        String day15 = "{\"time\":\"2024-01-15\",\"total_stats\":{\"entries\":15}}";
        program(
                settings,
                reply(numbered(CANVASES, 0), parents),
                reply(canvasTarget("a", 14, "2024-01-14"), canvas(day14)),
                reply(canvasTarget("a", 2, LAST), 200, "hold", canvas(day15)),
                reply(numbered(CANVASES, 0), sameIds),
                reply(canvasTarget("a", 2, LAST), canvas(day15)),
                reply(canvasTarget("b", 14, "2024-01-14"), canvas(day14)),
                reply(canvasTarget("b", 2, LAST), canvas()));
        resume(settings, 3, null);
        assertRows(
                settings,
                expected("a", "2024-01-14", lineage(day14, "canvas_id", "a")),
                expected("a", "2024-01-15", lineage(day15, "canvas_id", "a")),
                expected("b", "2024-01-14", lineage(day14, "canvas_id", "b")));
        assertEquals(7, requests(settings).size());
    }

    @Test
    void detectsReorderedParentPageAfterSavepointWithoutReplayingCompletedChild() throws Exception {
        Settings settings =
                new Settings("parent-drift", "segments_details").set("deadline", 120000);
        String first = detail("first");
        program(
                settings,
                reply(
                        numbered(SEGMENTS, 0),
                        list("segments", "{\"id\":\"first\"}", "{\"id\":\"second\"}")),
                reply("/segments/details?segment_id=first", first),
                reply("/segments/details?segment_id=second", 200, "hold", detail("second")),
                reply(
                        numbered(SEGMENTS, 0),
                        list("segments", "{\"id\":\"second\"}", "{\"id\":\"first\"}")));
        resume(settings, 3, "saved parent page changed since its checkpoint");
        assertRows(settings, expected("first", null, lineage(first, "segment_id", "first")));
        assertEquals(4, requests(settings).size());
    }

    @Test
    void resumesNumberedListAtTheSavedSecondPage() throws Exception {
        Settings settings = new Settings("page-savepoint", "segments").set("deadline", 120000);
        String[] first = numberedRecords("s", 100);
        String last = "{\"id\":\"tail\",\"name\":\"尾\"}";
        program(
                settings,
                reply(numbered(SEGMENTS, 0), list("segments", first)),
                reply(numbered(SEGMENTS, 1), 200, "hold", list("segments", last)),
                reply(numbered(SEGMENTS, 1), list("segments", last)));
        resume(settings, 2, null);
        String[] expected = new String[101];
        for (int index = 0; index < first.length; index++) {
            expected[index] = expected("s" + index, null, first[index]);
        }
        expected[100] = expected("tail", null, last);
        assertRows(settings, expected);
        assertEquals(3, requests(settings).size());
    }

    @Test
    void rejectsInvalidInstanceAnalyticsRangeAndParallelismBeforeRequests() throws Exception {
        Settings wrongInstance =
                new Settings("unsafe-instance", "events").set("instance", "localhost");
        program(wrongInstance);
        failure(run(wrongInstance), "instance must match a documented Braze REST instance");
        assertTrue(requests(wrongInstance).isEmpty());

        Settings missingStart = new Settings("missing-start", "segments_analytics");
        program(missingStart);
        failure(run(missingStart), "start_date is required for analytics and KPI resources");
        assertTrue(requests(missingStart).isEmpty());

        Settings wrongDate = days("wrong-date", "events_analytics", LAST, FIRST);
        program(wrongDate);
        failure(run(wrongDate), "start_date must be before or equal to end_date");
        assertTrue(requests(wrongDate).isEmpty());

        Settings parallel = new Settings("parallel-lists", "segments").set("parallelism", 2);
        program(parallel);
        failure(run(parallel), "source parallelism must be 1");
        assertTrue(requests(parallel).isEmpty());
    }

    private void kpi(String resource, String path, String metric, int value) throws Exception {
        String record = "{\"time\":\"2024-01-01\",\"" + metric + "\":" + value + "}";
        export(
                days("full-" + resource, resource, FIRST, FIRST),
                List.of(reply(series(path, "", 1, FIRST), list("data", record))),
                expected(null, FIRST, record));
    }

    private void export(Settings settings, List<ObjectNode> steps, String... expected)
            throws Exception {
        program(settings, steps.toArray(new ObjectNode[0]));
        success(run(settings));
        assertRows(settings, expected);
        assertEquals(steps.size(), requests(settings).size());
    }

    @Test
    void stopsTricklingSocketsAtTheDeadlineAndDuringRealJobCancellation() throws Exception {
        Settings deadline =
                new Settings("trickle-deadline", "segments")
                        .set("deadline", 4000)
                        .set("readTimeout", 15000);
        program(deadline, reply(numbered(SEGMENTS, 0), 200, "trickle", ""));
        failure(run(deadline), "request deadline exceeded");
        awaitRequests(deadline, 1);
        awaitClosed(deadline);
        assertTrue(rows(deadline).isEmpty());

        Settings cancel = new Settings("trickle-cancel", "segments").set("deadline", 60000);
        program(cancel, reply(numbered(SEGMENTS, 0), 200, "trickle", ""));
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(CONF, job, cancel.variables()));
        try {
            awaitRequests(cancel, 1);
            success(engine.cancelJob(job));
            running.get(30, TimeUnit.SECONDS);
            awaitClosed(cancel);
            assertTrue(rows(cancel).isEmpty());
        } finally {
            if (!running.isDone()) {
                engine.cancelJob(job);
                running.cancel(true);
            }
        }
    }

    @Test
    void rejectsCrossOriginRedirectUntrustedCertificateAndTrustedWrongHostname() throws Exception {
        Settings redirect = new Settings("no-redirect", "segments");
        program(redirect, reply(numbered(SEGMENTS, 0), 302, "", "unexpected redirect"));
        failure(run(redirect), "HTTP 302");
        assertEquals(1, requests(redirect).size());

        try {
            for (String certificate : List.of("untrusted", "wrong")) {
                certificates(certificate);
                Settings options = new Settings("bad-tls-" + certificate, "segments");
                program(options);
                failure(run(options), "HTTPS or TLS exchange failed");
                assertTrue(requests(options).isEmpty());
            }
        } finally {
            certificates("trusted");
        }
    }

    private void resume(Settings options, int countBeforeRestore, String failureClue)
            throws Exception {
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(CONF, job, options.variables()));
        try {
            awaitRequests(options, countBeforeRestore);
            long prior = engine.getCompletedCheckpointCount(job);
            Awaitility.await()
                    .atMost(35, TimeUnit.SECONDS)
                    .until(() -> engine.getCompletedCheckpointCount(job) > prior);
            success(engine.savepointJob(job));
            running.get(30, TimeUnit.SECONDS);
            engine.touch(options.name + ".release");
            Container.ExecResult result = engine.restoreJob(CONF, job, options.variables());
            if (failureClue == null) {
                success(result);
            } else {
                failure(result, failureClue);
            }
        } finally {
            if (!running.isDone()) {
                engine.cancelJob(job);
                running.cancel(true);
            }
        }
    }

    private void program(Settings options, ObjectNode... steps) throws Exception {
        verifyProviderAssertions();
        Path program = Files.createTempFile(directory, "braze-steps-", ".json");
        Files.writeString(program, JSON.createArrayNode().addAll(Arrays.asList(steps)).toString());
        engine.install(program, options.name + ".program");
        installText("active", options.name);
        active = options.name;
    }

    private void certificates(String alias) throws Exception {
        installText("certificate", alias);
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> alias.equals(engine.contents("certificate-applied")));
    }

    private void installText(String path, String content) throws IOException {
        Path source = Files.createTempFile(directory, "braze-fixture-", ".txt");
        Files.writeString(source, content);
        engine.install(source, path);
    }

    private Container.ExecResult run(Settings options) throws Exception {
        return engine.executeJob(CONF, jobId(), options.variables());
    }

    private List<JsonNode> requests(Settings options) throws Exception {
        return lines(engine.contents(options.name + ".requests"));
    }

    private List<JsonNode> rows(Settings options) throws Exception {
        return lines(engine.rows(options.name));
    }

    private void assertRows(Settings options, String... wanted) throws Exception {
        List<String> actual = new ArrayList<>();
        for (JsonNode row : rows(options)) {
            actual.add(
                    expected(
                            nullableText(row.path("entity_id")),
                            nullableText(row.path("time")),
                            row.path("data").asText()));
        }
        List<String> target = new ArrayList<>(Arrays.asList(wanted));
        Collections.sort(target);
        Collections.sort(actual);
        assertEquals(target, actual);
    }

    private static String nullableText(JsonNode node) {
        return node.isNull() || node.isMissingNode() ? null : node.asText();
    }

    private static String expected(String id, String time, String row) throws IOException {
        return JSON.createArrayNode().add(id).add(time).add(JSON.readTree(row)).toString();
    }

    private void awaitRequests(Settings options, int count) {
        Awaitility.await()
                .atMost(count > 100 ? 100 : 35, TimeUnit.SECONDS)
                .until(() -> requests(options).size() == count);
    }

    private void awaitClosed(Settings options) {
        Awaitility.await()
                .atMost(20, TimeUnit.SECONDS)
                .until(() -> engine.exists(options.name + ".disconnect"));
    }

    private static List<JsonNode> lines(String text) throws IOException {
        List<JsonNode> result = new ArrayList<>();
        for (String line : text.split("\\r?\\n")) {
            if (!line.isBlank()) {
                result.add(JSON.readTree(line));
            }
        }
        return result;
    }

    private static ObjectNode reply(String target, String response) {
        return reply(target, 200, "", response);
    }

    private static ObjectNode reply(String target, int status, String mode, String response) {
        return JSON.createObjectNode()
                .put("target", target)
                .put("host", BrazePeer.HOST)
                .put("status", status)
                .put("mode", mode)
                .put("response", response);
    }

    private static String numbered(String path, int page) {
        return path + "?page=" + page;
    }

    private static String list(String member, String... rows) {
        return "{\"message\":\"success\",\"" + member + "\":[" + String.join(",", rows) + "]}";
    }

    private static String detail(String name) {
        return "{\"message\":\"success\",\"name\":\"" + name + "\",\"description\":null}";
    }

    private static String canvas(String... rows) {
        return "{\"message\":\"success\",\"data\":{\"name\":\"c\",\"stats\":["
                + String.join(",", rows)
                + "]}}";
    }

    private static String series(String path, String query, int length, String date) {
        return path
                + "?"
                + (query.isEmpty() ? "" : query + "&")
                + "length="
                + length
                + "&ending_at="
                + date
                + "T23%3A59%3A59Z";
    }

    private static String canvasTarget(String id, int length, String date) {
        return series("/canvas/data_series", "canvas_id=" + id, length, date)
                + "&include_variant_breakdown=true&include_step_breakdown=true";
    }

    private static String lineage(String original, String key, String id) throws IOException {
        ObjectNode value = (ObjectNode) JSON.readTree(original);
        value.put(key, id);
        return value.toString();
    }

    private static String[] numberedRecords(String prefix, int length) {
        String[] result = new String[length];
        for (int index = 0; index < length; index++) {
            result[index] = "{\"id\":\"" + prefix + index + "\",\"last_edited\":null}";
        }
        return result;
    }

    private static Settings days(String name, String resource, String start, String end) {
        return new Settings(name, resource).set("start", start).set("end", end);
    }

    private static String jobId() {
        return Long.toString(
                ThreadLocalRandom.current().nextLong(1_000_000_000_000L, 9_000_000_000_000L));
    }

    private static String output(Container.ExecResult result) {
        return result.getStdout() + result.getStderr();
    }

    private static void success(Container.ExecResult result) {
        assertEquals(0, result.getExitCode(), () -> output(result));
    }

    private static void failure(Container.ExecResult result, String clue) {
        String log = output(result);
        assertNotEquals(0, result.getExitCode(), log);
        assertTrue(log.contains(clue), log);
        assertFalse(log.contains(BrazePeer.PRIVATE), "log contains provider response data");
        assertFalse(log.contains(BrazePeer.KEY), "log contains the provider credential");
    }

    /**
     * Generates an identity for the fixture's trusted, untrusted or mismatched handshake.
     *
     * @param name the entry name in the combined server key store
     * @param hosts all names included in this certificate
     * @return the generated private-key entry and certificate
     */
    private static KeyStore identity(String name, List<String> hosts) throws Exception {
        String keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
        Path target = directory.resolve(name + ".p12");
        String alt = hosts.stream().map(host -> "dns:" + host).collect(Collectors.joining(","));
        Process process =
                new ProcessBuilder(
                                keytool,
                                "-genkeypair",
                                "-alias",
                                name,
                                "-keyalg",
                                "RSA",
                                "-keysize",
                                "2048",
                                "-validity",
                                "7",
                                "-dname",
                                "CN=" + hosts.get(0),
                                "-ext",
                                "SAN=" + alt,
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                target.toString(),
                                "-storepass",
                                BrazePeer.PASSWORD,
                                "-keypass",
                                BrazePeer.PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String log = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, process.waitFor(), log);
        KeyStore key = KeyStore.getInstance("PKCS12");
        try (InputStream stream = Files.newInputStream(target)) {
            key.load(stream, BrazePeer.PASSWORD.toCharArray());
        }
        return key;
    }

    /**
     * Supplies per-job values through the standard command line.
     *
     * <p>Each job chooses one export and uses the normal connector configuration.
     */
    private static final class Settings {
        final String name;
        private final Map<String, Object> values = new LinkedHashMap<>();

        Settings(String name, String resource) {
            this.name = name;
            set("token", BrazePeer.KEY);
            set("resource", resource);
            set("instance", "us-01");
            set("parallelism", 1);
            set("output", OUTPUT + name);
        }

        Settings set(String field, Object value) {
            values.put(field, value);
            return this;
        }

        String[] variables() {
            return values.entrySet().stream()
                    .map(
                            entry -> {
                                Object value = entry.getValue();
                                String config =
                                        value instanceof Number
                                                ? value.toString()
                                                : JSON.getNodeFactory()
                                                        .textNode(value.toString())
                                                        .toString();
                                return "braze." + entry.getKey() + "=" + config;
                            })
                    .toArray(String[]::new);
        }
    }

    /**
     * Installs the independent fixture and prevents external provider DNS in tests.
     *
     * <p>The same production URL, trust-store mechanism and packaged connector remain in use.
     */
    private static final class Engine extends SeaTunnelContainer {
        private final Path fixtures;

        Engine(Path fixtures) {
            this.fixtures = fixtures;
        }

        @Override
        protected String getJavaToolOptions() {
            return "-Djavax.net.ssl.trustStore="
                    + ROOT
                    + "/trust.jks -Djavax.net.ssl.trustStoreType=JKS"
                    + " -Djavax.net.ssl.trustStorePassword="
                    + BrazePeer.PASSWORD;
        }

        @Override
        protected void executeExtraCommands(GenericContainer<?> container) throws IOException {
            for (String host : List.of(BrazePeer.HOST, BrazePeer.EU, BrazePeer.JP, BrazePeer.AU)) {
                container.withExtraHost(host, "127.0.0.1");
            }
            container.withExtraHost(BrazePeer.REDIRECT, "127.0.0.1");
            for (String file : List.of("identities.p12", "trust.jks")) {
                container.withCopyFileToContainer(
                        MountableFile.forHostPath(fixtures.resolve(file)), ROOT + "/" + file);
            }
            for (Class<?> type : List.of(BrazePeer.class, BrazePeer.SwitchingKeyManager.class)) {
                String path = type.getName().replace('.', '/') + ".class";
                try {
                    container.withCopyFileToContainer(
                            MountableFile.forHostPath(
                                    Path.of(type.getResource("/" + path).toURI())),
                            ROOT + "/classes/" + path);
                } catch (URISyntaxException e) {
                    throw new IOException("Cannot stage the Braze HTTPS fixture", e);
                }
            }
            try {
                Path json =
                        Path.of(
                                ObjectMapper.class
                                        .getProtectionDomain()
                                        .getCodeSource()
                                        .getLocation()
                                        .toURI());
                container.withCopyFileToContainer(
                        MountableFile.forHostPath(json),
                        ROOT + (Files.isDirectory(json) ? "/jackson" : "/jackson.jar"));
            } catch (URISyntaxException e) {
                throw new IOException("Cannot stage the Braze fixture JSON library", e);
            }
        }

        Container.ExecResult runPeer() throws Exception {
            return server.execInContainer(
                    "java",
                    "-cp",
                    ROOT + "/classes:" + ROOT + "/jackson:" + ROOT + "/jackson.jar",
                    BrazePeer.class.getName(),
                    ROOT);
        }

        void install(Path file, String name) {
            server.copyFileToContainer(MountableFile.forHostPath(file), ROOT + "/" + name);
        }

        boolean exists(String name) throws Exception {
            return server.execInContainer("test", "-f", ROOT + "/" + name).getExitCode() == 0;
        }

        void touch(String file) throws Exception {
            success(server.execInContainer("touch", ROOT + "/" + file));
        }

        String contents(String name) throws Exception {
            if (!exists(name)) {
                return "";
            }
            Container.ExecResult result = server.execInContainer("cat", ROOT + "/" + name);
            success(result);
            return result.getStdout();
        }

        String rows(String name) throws Exception {
            String dir = OUTPUT + name;
            Container.ExecResult result =
                    server.execInContainer(
                            "sh",
                            "-c",
                            "if [ -d "
                                    + dir
                                    + " ]; then find "
                                    + dir
                                    + " -maxdepth 1 -type f -name 'receipt_*'"
                                    + " -exec sh -c 'for f do cat \"$f\"; printf \"\\n\"; done' sh {} +; fi");
            success(result);
            return result.getStdout();
        }
    }
}
