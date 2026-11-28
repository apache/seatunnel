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

package org.apache.seatunnel.e2e.connector.appsflyer;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;

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
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs the real Zeta source and transactional LocalFile in the Java 11 engine. The independent
 * HTTPS peer verifies each provider request, including signed-download auth.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AppsFlyerIT extends TestSuiteBase {
    private static final String API_URL =
            "https://" + AppsFlyerPeer.HOST + ":" + AppsFlyerPeer.TRUSTED_PORT;
    private static final String DOWNLOAD =
            "https://" + AppsFlyerPeer.DOWNLOAD + ":" + AppsFlyerPeer.TRUSTED_PORT;
    private static final String CONF = "/appsflyer.conf";
    private static final String APP = "com.example+trial";
    private static final String RAW = "/api/raw-data/export/app/com.example%2Btrial/";
    private static final String AGG = "/api/agg-data/export/app/com.example%2Btrial/";
    private static final String PRIVATE = "private-fixture-value";
    private static final String ZONE = "America/New_York";
    private static final String EVENT_FIELDS =
            "ad_unit,amazon_aid,app_type,att,blocked_reason,blocked_reason_rule,"
                    + "blocked_reason_value,blocked_sub_reason,campaign_type,contributor1_match_type,contributor2_match_type,"
                    + "contributor3_match_type,conversion_type,custom_data,deeplink_url,device_category,device_download_time,"
                    + "device_model,gp_broadcast_referrer,gp_click_time,gp_install_begin,gp_referrer,impressions,"
                    + "install_app_store,is_lat,keyword_id,keyword_match_type,match_type,mediation_network,"
                    + "monetization_network,network_account_id,oaid,placement,rejected_reason,rejected_reason_value,segment,"
                    + "store_reinstall";
    private static final ObjectMapper JSON =
            new ObjectMapper()
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    private final LocalDate day = LocalDate.now(ZoneId.of(ZONE)).minusDays(5);

    @TempDir static Path directory;
    private final ExecutorService tasks = Executors.newCachedThreadPool();
    private Engine engine;
    private Future<Container.ExecResult> peer;
    private String active;

    @BeforeAll
    void start() throws Exception {
        KeyStore trusted = makeIdentity("trusted");
        makeIdentity("untrusted");
        KeyStore trust = KeyStore.getInstance("JKS");
        char[] password = AppsFlyerPeer.PASSWORD.toCharArray();
        trust.load(null, password);
        trust.setCertificateEntry("api-fixture", trusted.getCertificate("api"));
        try (OutputStream output = Files.newOutputStream(directory.resolve("trust.jks"))) {
            trust.store(output, password);
        }
        engine = new Engine(directory);
        engine.startUp();
        peer = tasks.submit(engine::runPeer);
        Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> engine.exists("ready"));
    }

    @AfterEach
    void verifyPeerAcceptedEveryRequest() throws Exception {
        if (active != null) {
            assertFalse(engine.exists(active + ".invalid"), active);
        }
    }

    @AfterAll
    void stop() throws Exception {
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
    void readsAllEighteenViewsThroughFourteenExactProviderContracts() throws Exception {
        String name = "all-reports";
        Settings settings = new Settings(name);
        String stamp = day + " 01:02:03";
        List<String> steps = new ArrayList<>();
        String[][] raw = {
            {"in_app_events", "in_app_events_report", "Event Time", EVENT_FIELDS},
            {
                "organic_in_app_events",
                "organic_in_app_events_report",
                "Event Time",
                EVENT_FIELDS.replace("is_lat,", "")
            },
            {"retargeting_in_app_events", "in-app-events-retarget", "Event Time", EVENT_FIELDS},
            {"installs", "installs_report", "Install Time", EVENT_FIELDS},
            {"organic_installs", "organic_installs_report", "Install Time", EVENT_FIELDS},
            {"retargeting_installs", "installs-retarget", "Install Time", EVENT_FIELDS},
            {"uninstall_events", "uninstall_events_report", "Event Time", uninstallFields()},
            {
                "organic_uninstall_events",
                "organic_uninstall_events_report",
                "Event Time",
                uninstallFields()
            }
        };
        for (String[] report : raw) {
            String csv =
                    "\uFEFF"
                            + report[2]
                            + ",Country Code,Is LAT,App Type,AF Cost Value,Extra Cell,Media Source,"
                            + "Media Source (pid)\r\n"
                            + stamp
                            + ",CN,TRUE,N/A,123456789012345678.123456789,"
                            + "\"你好, \"\"quoted\"\"\r\nNULL\",literal,N/A\r\n";
            steps.add(csv(raw(report[1], settings, report[3]), csv));
        }
        String metricCsv =
                "Date,Media Source,Installs,Total Revenue,CTR,ARPU,Future Metric,"
                        + "af_purchase (Unique users),af_purchase (Event counter),af_purchase (Sales in USD),"
                        + "sign up (Event counter)\n"
                        + day
                        + ",Channel A,9007199254740993,"
                        + "123456789012345678.123456789,12.25%,N/A,record-value,2,3,0.000000001,4\n";
        for (String endpoint :
                List.of("daily_report", "partners_by_date_report", "geo_by_date_report")) {
            steps.add(csv(aggregate(endpoint, day, day, false), metricCsv));
            steps.add(csv(aggregate(endpoint, day, day, true), metricCsv));
        }
        program(name, steps);
        success(run(settings));
        assertEquals(14, requests(name).size());
        List<JsonNode> output = rows(name);
        assertEquals(22, output.size());
        Map<String, List<JsonNode>> data = byResource(output);
        assertEquals(18, data.size());
        JsonNode expectedRaw =
                JSON.readTree(
                        "{\"country_code\":\"CN\",\"is_lat\":true,\"app_type\":\"N/A\","
                                + "\"af_cost_value\":\"123456789012345678.123456789\","
                                + "\"extra_cell\":\"你好, \\\"quoted\\\"\\r\\nNULL\",\"media_source\":\"literal\","
                                + "\"additional_columns\":{\"Media Source (pid)\":\"N/A\"}}");
        for (String[] report : raw) {
            JsonNode row = data.get(report[0]).get(0);
            assertEquals(
                    day + "T01:02:03",
                    row.get(report[2].equals("Event Time") ? "event_time" : "install_time")
                            .asText());
            expectedRaw
                    .fields()
                    .forEachRemaining(
                            entry -> assertEquals(entry.getValue(), row.get(entry.getKey())));
            assertEquals(8, row.size());
        }
        for (String report :
                List.of(
                        "daily_report",
                        "retargeting_daily_report",
                        "partners_report",
                        "retargeting_partners_report",
                        "geo_report",
                        "retargeting_geo_report")) {
            JsonNode row = data.get(report).get(0);
            assertAggregate(row);
            assertEquals(
                    "3",
                    row.path("additional_columns").path("af_purchase (Event counter)").asText());
            assertEquals(
                    "4", row.path("additional_columns").path("sign up (Event counter)").asText());
        }
        for (String report :
                List.of(
                        "partners_events_report",
                        "retargeting_partners_events_report",
                        "geo_events_report",
                        "retargeting_geo_events_report")) {
            List<JsonNode> events = data.get(report);
            assertEquals(2, events.size());
            JsonNode purchase =
                    events.stream()
                            .filter(row -> "af_purchase".equals(row.get("event_name").asText()))
                            .findFirst()
                            .orElseThrow();
            JsonNode signup =
                    events.stream()
                            .filter(row -> "sign up".equals(row.get("event_name").asText()))
                            .findFirst()
                            .orElseThrow();
            assertAggregate(purchase);
            assertAggregate(signup);
            assertEquals(2, purchase.get("event_unique_users").intValue());
            assertEquals(3, purchase.get("event_counter").intValue());
            assertEquals(new BigDecimal("0.000000001"), purchase.get("event_sales").decimalValue());
            assertEquals(4, signup.get("event_counter").intValue());
            assertTrue(signup.get("event_unique_users").isNull());
            assertTrue(signup.get("event_sales").isNull());
            assertFalse(signup.has("additional_columns"));
        }
        for (JsonNode row : output) {
            assertEquals(APP, row.get("app_id").asText());
            assertEquals(ZONE, row.get("time_zone").asText());
            String key = row.get("resource").asText();
            assertEquals(
                    key.endsWith("report") ? day.toString() : day + "T01:02:03",
                    row.get("report_time").asText());
        }
    }

    @Test
    void splitsRawOnAnExactSecondAndKeepsOffsetAndBoundaryRows() throws Exception {
        String name = "raw-split";
        Settings settings = seconds(name, 0, 4);
        settings.records = 3;
        String first = event(day + " 00:00:00", "left");
        String middle = event(day + " 00:00:02", "untouched");
        String last = event(day + " 00:00:03", "right");
        program(
                name,
                List.of(
                        csv(
                                raw("in_app_events_report", settings, EVENT_FIELDS),
                                events(first + middle + last)),
                        csv(
                                raw("in_app_events_report", bounds(0, 2), EVENT_FIELDS),
                                events(
                                        event(
                                                day
                                                        + " 00:00:00"
                                                        + ZoneId.of(ZONE)
                                                                .getRules()
                                                                .getOffset(day.atStartOfDay()),
                                                "left"))),
                        csv(
                                raw("in_app_events_report", bounds(2, 4), EVENT_FIELDS),
                                events(middle + last))));
        success(run(settings));
        assertEquals(3, requests(name).size());
        List<JsonNode> rows = rows(name);
        assertEquals(3, rows.size());
        Map<String, String> values = new HashMap<>();
        for (JsonNode row : rows) {
            JsonNode data = JSON.readTree(row.get("data").asText());
            values.put(data.get("event_name").asText(), row.get("report_time").asText());
        }
        assertEquals(
                Map.of(
                        "left",
                        day.atStartOfDay(ZoneId.of(ZONE)).toInstant().toString(),
                        "untouched",
                        day + "T00:00:02",
                        "right",
                        day + "T00:00:03"),
                values);
    }

    @Test
    void splitsInclusiveAggregateQueriesOnlyOnDayBoundaries() throws Exception {
        String name = "aggregate-split";
        Settings settings = new Settings(name);
        settings.resources = "\"daily_report\"";
        settings.end = day.plusDays(2).toString();
        settings.records = 3;
        String first = "Date,Installs\n" + day + ",12\n";
        String second = "Date,Installs\n" + day.plusDays(1) + ",3\n";
        program(
                name,
                List.of(
                        csv(
                                aggregate("daily_report", day, day.plusDays(1), false),
                                first + day + ",13\n" + day + ",14\n"),
                        csv(aggregate("daily_report", day, day, false), first),
                        csv(
                                aggregate("daily_report", day.plusDays(1), day.plusDays(1), false),
                                second)));
        success(run(settings));
        assertEquals(3, requests(name).size());
        List<JsonNode> values = byResource(rows(name)).get("daily_report");
        assertEquals(2, values.size());
        assertTrue(values.contains(JSON.readTree("{\"date\":\"" + day + "\",\"installs\":12}")));
        assertTrue(
                values.contains(
                        JSON.readTree("{\"date\":\"" + day.plusDays(1) + "\",\"installs\":3}")));
    }

    @Test
    void failsOnIrreducibleRawAndAggregateRowCeilingsAndExpandedOutput() throws Exception {
        Settings raw = seconds("raw-ceiling", 0, 1);
        raw.records = 2;
        String oneSecond = event(day + " 00:00:00", "a");
        program(
                raw.name,
                List.of(
                        csv(
                                raw("in_app_events_report", raw, EVENT_FIELDS),
                                events(oneSecond + oneSecond))));
        failure(run(raw), "cannot be divided without overlap");
        assertEquals(1, requests(raw.name).size());
        assertTrue(rows(raw.name).isEmpty());

        Settings agg = new Settings("aggregate-ceiling");
        agg.records = 2;
        agg.resources = "\"daily_report\"";
        program(
                agg.name,
                List.of(
                        csv(
                                aggregate("daily_report", day, day, false),
                                "Date,Installs\n" + day + ",1\n" + day + ",2\n")));
        failure(run(agg), "cannot be divided without overlap");
        assertEquals(1, requests(agg.name).size());
        assertTrue(rows(agg.name).isEmpty());

        Settings expanded = new Settings("expanded-ceiling");
        expanded.records = 2;
        expanded.resources = "\"partners_report\", \"partners_events_report\"";
        program(
                expanded.name,
                List.of(
                        csv(
                                aggregate("partners_by_date_report", day, day, false),
                                "Date,one (Event counter),two (Event counter)\n"
                                        + day
                                        + ",1,2\n")));
        failure(run(expanded), "cannot be divided without overlap");
        assertEquals(1, requests(expanded.name).size());
        assertTrue(rows(expanded.name).isEmpty());
    }

    @Test
    void downloadsSignedCsvWithoutLeakingBearerAcrossOriginsOrOnReturn() throws Exception {
        String name = "signed-valid";
        Settings options = new Settings(name);
        options.resources = "\"daily_report\"";
        String report = "Date,Installs\n" + day + ",7\n";
        String relayed = target(AppsFlyerPeer.HOST, "/same-origin-report", Map.of());
        String signed =
                target(AppsFlyerPeer.DOWNLOAD, "/encrypted%2Bname", Map.of("sig", PRIVATE + "+1"));
        String finalTarget = target(AppsFlyerPeer.HOST, "/final-report", Map.of());
        program(
                name,
                List.of(
                        redirect(
                                aggregate("daily_report", day, day, false),
                                "bearer",
                                "/same-origin-report"),
                        redirect(
                                relayed,
                                "bearer",
                                DOWNLOAD + "/encrypted%2Bname?sig=" + PRIVATE + "%2B1"),
                        redirect(signed, "none", API_URL + "/final-report"),
                        step(finalTarget, "none", 200, "gzip", "", "", report)));
        success(run(options));
        assertEquals(4, requests(name).size());
        assertEquals(
                List.of(JSON.readTree("{\"date\":\"" + day + "\",\"installs\":7}")),
                byResource(rows(name)).get("daily_report"));
    }

    @Test
    void deniesUnapprovedOriginDowngradesAndRedirectChainsBeyondBudget() throws Exception {
        String source = aggregate("daily_report", day, day, false);
        String[] forbidden = {
            "https://unapproved.appsflyer-fixture.invalid/file?sig=" + PRIVATE,
            "http://" + AppsFlyerPeer.HOST + "/file",
            "https://user@" + AppsFlyerPeer.HOST + "/file",
            DOWNLOAD + "/fragment#" + PRIVATE,
            DOWNLOAD + "/file?leaked=%66" + AppsFlyerPeer.TOKEN.substring(1)
        };
        for (int index = 0; index < forbidden.length; index++) {
            Settings settings = new Settings("invalid-redirect-" + index);
            settings.resources = "\"daily_report\"";
            program(settings.name, List.of(redirect(source, "bearer", forbidden[index])));
            failure(run(settings), "outside the configured HTTPS origins");
            assertEquals(1, requests(settings.name).size());
            assertTrue(rows(settings.name).isEmpty());
        }
        Settings hops = new Settings("redirect-hops");
        hops.resources = "\"daily_report\"";
        String bouncing = target(AppsFlyerPeer.HOST, "/bounce", Map.of());
        program(
                hops.name,
                List.of(
                        redirect(source, "bearer", "/bounce"),
                        redirect(bouncing, "bearer", "/bounce"),
                        redirect(bouncing, "bearer", "/bounce"),
                        redirect(bouncing, "bearer", "/bounce")));
        failure(run(hops), "report redirect limit exceeded");
        assertEquals(4, requests(hops.name).size());

        Settings budget = new Settings("request-budget");
        budget.resources = "\"daily_report\"";
        budget.requests = 1;
        program(budget.name, List.of(redirect(source, "bearer", "/bounce")));
        failure(run(budget), "request budget exhausted");
        assertEquals(1, requests(budget.name).size());
    }

    @Test
    void rejectsUntrustedTlsAndInvalidConfigurationBeforeReportRead() throws Exception {
        Settings untrusted = seconds("untrusted", 0, 1);
        untrusted.url = "https://" + AppsFlyerPeer.HOST + ":" + AppsFlyerPeer.UNTRUSTED_PORT;
        program(untrusted.name, List.of());
        failure(run(untrusted), "HTTPS or TLS exchange failed");
        assertTrue(requests(untrusted.name).isEmpty());

        Settings retention = new Settings("retention");
        retention.start = day.minusDays(91).toString();
        program(retention.name, List.of());
        failure(run(retention), "outside retention");
        assertTrue(requests(retention.name).isEmpty());

        Settings mixed = new Settings("config-time");
        mixed.start = day + " 01:02:03";
        program(mixed.name, List.of());
        failure(run(mixed), "must be yyyy-MM-dd");
        assertTrue(requests(mixed.name).isEmpty());
    }

    @Test
    void refusesBadFramingHeadersRowsAndTypedValuesWithoutEmittingPrefix() throws Exception {
        String time = day + " 00:00:00";
        String valid = event(time, "valid");
        String[][] cases = {
            {"csv", "Event Time,Event Name,Event Name\n", "duplicate CSV column"},
            {"csv", "Event Name,Device Type\n", "missing its required cursor"},
            {"csv", events(valid + time + ",malformed,extra\n"), "width does not match"},
            {"csv", events(valid + time + ",\"unfinished\n"), "quoted"},
            {"csv", "Event Time,Is LAT\n" + time + ",invalid\n", "invalid typed boolean"},
            {
                "csv",
                events(valid + event(day + " 00:00:01", "end")),
                "outside the requested local window"
            },
            {"json", "{\"error\":\"" + PRIVATE + "\"}", "non-CSV report"}
        };
        for (int index = 0; index < cases.length; index++) {
            String[] test = cases[index];
            Settings options = seconds("invalid-csv-" + index, 0, 1);
            program(
                    options.name,
                    List.of(
                            step(
                                    raw("in_app_events_report", options, EVENT_FIELDS),
                                    "bearer",
                                    200,
                                    test[0],
                                    "",
                                    "",
                                    test[1])));
            failure(run(options), test[2]);
            assertEquals(1, requests(options.name).size());
            assertTrue(rows(options.name).isEmpty());
        }
        Settings invalidUtf8 = seconds("invalid-utf8", 0, 1);
        String query = raw("in_app_events_report", invalidUtf8, EVENT_FIELDS);
        byte[] bad = (events(valid) + time + ",").getBytes(StandardCharsets.UTF_8);
        bad = Arrays.copyOf(bad, bad.length + 2);
        bad[bad.length - 2] = (byte) 0xc3;
        bad[bad.length - 1] = 0x28;
        program(invalidUtf8.name, List.of(step(query, "bearer", 200, "csv", "", "", bad)));
        failure(run(invalidUtf8), "invalid, incomplete or unreadable CSV");
        assertTrue(rows(invalidUtf8.name).isEmpty());

        Settings badNumber = new Settings("invalid-metric");
        badNumber.resources = "\"daily_report\"";
        program(
                badNumber.name,
                List.of(
                        csv(
                                aggregate("daily_report", day, day, false),
                                "Date,Installs\n" + day + ",not-numeric\n")));
        failure(run(badNumber), "invalid exact decimal");
        assertTrue(rows(badNumber.name).isEmpty());
    }

    @Test
    void acceptsHeaderOnlyReportAndBoundsBytesAfterGzipInflation() throws Exception {
        Settings empty = seconds("empty-csv", 0, 1);
        program(
                empty.name,
                List.of(csv(raw("in_app_events_report", empty, EVENT_FIELDS), events(""))));
        success(run(empty));
        assertEquals(1, requests(empty.name).size());
        assertTrue(rows(empty.name).isEmpty());

        String csv = events(event(day + " 00:00:00", "x".repeat(9000)));
        for (String encoding : List.of("csv", "gzip")) {
            Settings settings = seconds("bounded-" + encoding, 0, 1);
            settings.bytes = 1024;
            program(
                    settings.name,
                    List.of(
                            step(
                                    raw("in_app_events_report", settings, EVENT_FIELDS),
                                    "bearer",
                                    200,
                                    encoding,
                                    "",
                                    "",
                                    csv)));
            failure(run(settings), "exceeds max_response_bytes");
            assertEquals(1, requests(settings.name).size());
            assertTrue(rows(settings.name).isEmpty());
        }
        Settings fields = seconds("bounded-field", 0, 1);
        fields.field = 32;
        program(fields.name, List.of(csv(raw("in_app_events_report", fields, EVENT_FIELDS), csv)));
        failure(run(fields), "max_field_chars");
        assertTrue(rows(fields.name).isEmpty());

        Settings buffer = seconds("bounded-buffer", 0, 1);
        buffer.buffer = 1024;
        program(buffer.name, List.of(csv(raw("in_app_events_report", buffer, EVENT_FIELDS), csv)));
        failure(run(buffer), "exceeds its staged output limit");
        assertTrue(rows(buffer.name).isEmpty());
    }

    @Test
    void boundsRateRetriesAndSurfacesQuotaOrPermissionFailuresWithoutBodyLeak() throws Exception {
        Settings retry = seconds("successful-retry", 0, 1);
        String route = raw("in_app_events_report", retry, EVENT_FIELDS);
        String valid = events(event(day + " 00:00:00", "retried"));
        program(
                retry.name,
                List.of(
                        status(route, 429, "0", PRIVATE),
                        status(route, 503, "0", PRIVATE),
                        csv(route, valid)));
        success(run(retry));
        assertEquals(List.of(route, route, route), requests(retry.name));
        assertEquals(
                "retried",
                byResource(rows(retry.name))
                        .get("in_app_events")
                        .get(0)
                        .get("event_name")
                        .asText());

        Settings exhausted = seconds("attempt-limit", 0, 1);
        exhausted.attempts = 2;
        program(
                exhausted.name,
                List.of(status(route, 503, "", PRIVATE), status(route, 503, "", PRIVATE)));
        failure(run(exhausted), "retry limit reached after HTTP 503");
        assertEquals(2, requests(exhausted.name).size());

        for (int code : new int[] {401, 402, 403, 206}) {
            Settings denied = seconds("denied-" + code, 0, 1);
            program(denied.name, List.of(status(route, code, "", PRIVATE)));
            failure(run(denied), code == 206 ? "unexpected partial report" : "HTTP " + code);
            assertEquals(1, requests(denied.name).size());
            assertTrue(rows(denied.name).isEmpty());
        }
        Settings daily = seconds("daily-quota", 0, 1);
        program(
                daily.name,
                List.of(
                        status(
                                route,
                                400,
                                "",
                                "Your API calls limit has been reached for report type "
                                        + PRIVATE)));
        failure(run(daily), "daily report quota exhausted");
        assertEquals(1, requests(daily.name).size());

        Settings minute = new Settings("minute-quota");
        minute.resources = "\"daily_report\"";
        program(
                minute.name,
                List.of(
                        status(
                                aggregate("daily_report", day, day, false),
                                403,
                                "",
                                "Limit reached for " + PRIVATE)));
        failure(run(minute), "retry delay exceeds request deadline");
        assertEquals(1, requests(minute.name).size());

        Settings delayed = seconds("retry-beyond-budget", 0, 1);
        program(delayed.name, List.of(status(route, 429, "61", PRIVATE)));
        failure(run(delayed), "retry delay exceeds request deadline");
        assertEquals(1, requests(delayed.name).size());
    }

    @Test
    void closesTricklingResponseAtAbsoluteDeadlineAndEngineCancel() throws Exception {
        Settings deadline = seconds("slow-deadline", 0, 1);
        String route = raw("in_app_events_report", deadline, EVENT_FIELDS);
        deadline.deadline = 2500;
        program(deadline.name, List.of(step(route, "bearer", 200, "trickle", "", "", "")));
        failure(run(deadline), "request deadline exceeded");
        awaitDisconnect(deadline.name);
        assertEquals(1, requests(deadline.name).size());
        assertTrue(rows(deadline.name).isEmpty());

        Settings canceled = seconds("canceled", 0, 1);
        canceled.deadline = 90000;
        program(canceled.name, List.of(step(route, "bearer", 200, "trickle", "", "", "")));
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(CONF, job, canceled.variables()));
        try {
            awaitRequests(canceled.name, 1);
            success(engine.cancelJob(job));
            running.get(30, TimeUnit.SECONDS);
            awaitDisconnect(canceled.name);
            assertEquals(1, requests(canceled.name).size());
            assertTrue(rows(canceled.name).isEmpty());
        } finally {
            if (!running.isDone()) {
                engine.cancelJob(job);
                running.cancel(true);
            }
        }
    }

    @Test
    void restoresRemainingAdaptiveSubwindowWithoutRequeryingCompletedHalf() throws Exception {
        Settings options = seconds("restore-subwindow", 0, 4);
        options.records = 3;
        String left = event(day + " 00:00:00", "committed-left");
        String right = event(day + " 00:00:02", "restored-right");
        String root = raw("in_app_events_report", options, EVENT_FIELDS);
        String a = raw("in_app_events_report", bounds(0, 2), EVENT_FIELDS);
        String b = raw("in_app_events_report", bounds(2, 4), EVENT_FIELDS);
        program(
                options.name,
                List.of(
                        csv(root, events(left + left + right)),
                        csv(a, events(left)),
                        step(b, "bearer", 200, "hold", "", "", events(right)),
                        csv(b, events(right))));
        saveAndRestore(options, 3);
        assertEquals(List.of(root, a, b, b), requests(options.name));
        assertNames(options.name, "committed-left", "restored-right");
    }

    @Test
    void restoresBetweenDifferentResourcesAndPersistsEachCompletedWindowOnce() throws Exception {
        Settings options = new Settings("restore-resources");
        options.resources = "\"in_app_events\", \"organic_in_app_events\"";
        String a = raw("in_app_events_report", options, EVENT_FIELDS);
        String b =
                raw("organic_in_app_events_report", options, EVENT_FIELDS.replace("is_lat,", ""));
        String left = events(event(day + " 00:00:00", "paid"));
        String right = events(event(day + " 00:00:01", "organic"));
        program(
                options.name,
                List.of(
                        csv(a, left),
                        step(b, "bearer", 200, "hold", "", "", right),
                        csv(b, right)));
        saveAndRestore(options, 2);
        assertEquals(List.of(a, b, b), requests(options.name));
        Map<String, List<JsonNode>> rows = byResource(rows(options.name));
        assertEquals(2, rows.size());
        assertEquals("paid", rows.get("in_app_events").get(0).get("event_name").asText());
        assertEquals(
                "organic", rows.get("organic_in_app_events").get(0).get("event_name").asText());
        assertEquals(1, rows.get("in_app_events").size());
        assertEquals(1, rows.get("organic_in_app_events").size());
    }

    private void saveAndRestore(Settings options, int calls) throws Exception {
        options.deadline = 120000;
        String job = jobId();
        Future<Container.ExecResult> result =
                tasks.submit(() -> engine.executeJob(CONF, job, options.variables()));
        try {
            awaitRequests(options.name, calls);
            long checkpoints = engine.getCompletedCheckpointCount(job);
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> engine.getCompletedCheckpointCount(job) > checkpoints);
            success(engine.savepointJob(job));
            result.get(30, TimeUnit.SECONDS);
            engine.touch(options.name + ".release");
            success(engine.restoreJob(CONF, job, options.variables()));
        } finally {
            if (!result.isDone()) {
                engine.cancelJob(job);
                result.cancel(true);
            }
        }
    }

    private void awaitRequests(String name, int count) {
        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> requests(name).size() == count);
    }

    private void awaitDisconnect(String name) {
        Awaitility.await()
                .atMost(20, TimeUnit.SECONDS)
                .until(() -> engine.exists(name + ".disconnect"));
    }

    private void program(String name, List<String> steps) throws Exception {
        verifyPeerAcceptedEveryRequest();
        Path prepared = Files.createTempFile(directory, "steps-", ".txt");
        Files.write(prepared, steps, StandardCharsets.UTF_8);
        engine.install(prepared, name + ".program");
        Path activeFile = Files.createTempFile(directory, "active-", ".txt");
        Files.writeString(activeFile, name);
        engine.install(activeFile, "active");
        active = name;
    }

    private List<String> requests(String name) throws Exception {
        return engine.lines(name + ".requests");
    }

    private List<JsonNode> rows(String name) throws Exception {
        List<JsonNode> rows = new ArrayList<>();
        for (String line : engine.rows(name)) {
            rows.add(JSON.readTree(line));
        }
        return rows;
    }

    private static Map<String, List<JsonNode>> byResource(List<JsonNode> rows) throws IOException {
        Map<String, List<JsonNode>> groups = new HashMap<>();
        for (JsonNode row : rows) {
            groups.computeIfAbsent(row.path("resource").asText(), ignored -> new ArrayList<>())
                    .add(JSON.readTree(row.path("data").asText()));
        }
        return groups;
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

    private static void failure(Container.ExecResult result, String expected) {
        assertNotEquals(0, result.getExitCode(), () -> output(result));
        assertTrue(output(result).contains(expected), () -> output(result));
        assertFalse(output(result).contains(AppsFlyerPeer.TOKEN));
        assertFalse(output(result).contains(PRIVATE));
    }

    private static KeyStore makeIdentity(String name) throws Exception {
        String keytool = Paths.get(System.getProperty("java.home"), "bin", "keytool").toString();
        Path target = directory.resolve(name + ".p12");
        Process crypto =
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
                                "CN=" + AppsFlyerPeer.HOST,
                                "-ext",
                                "SAN=dns:" + AppsFlyerPeer.HOST + ",dns:" + AppsFlyerPeer.DOWNLOAD,
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                target.toString(),
                                "-storepass",
                                AppsFlyerPeer.PASSWORD,
                                "-keypass",
                                AppsFlyerPeer.PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String output = new String(crypto.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, crypto.waitFor(), output);
        KeyStore key = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(target)) {
            key.load(input, AppsFlyerPeer.PASSWORD.toCharArray());
        }
        return key;
    }

    private static void assertAggregate(JsonNode value) {
        assertEquals("Channel A", value.get("media_source").asText());
        assertEquals(9007199254740993L, value.get("installs").longValue());
        assertEquals(
                new BigDecimal("123456789012345678.123456789"),
                value.get("total_revenue").decimalValue());
        assertEquals(new BigDecimal("12.25"), value.get("click_through_rate").decimalValue());
        assertTrue(value.get("average_revenue_per_user").isNull());
        assertEquals("record-value", value.get("future_metric").asText());
    }

    private void assertNames(String name, String first, String second) throws Exception {
        List<JsonNode> values = byResource(rows(name)).get("in_app_events");
        assertEquals(2, values.size());
        assertTrue(values.stream().anyMatch(row -> first.equals(row.get("event_name").asText())));
        assertTrue(values.stream().anyMatch(row -> second.equals(row.get("event_name").asText())));
    }

    private Container.ExecResult run(Settings options) throws Exception {
        return engine.executeJob(CONF, jobId(), options.variables());
    }

    private Settings seconds(String name, int from, int to) {
        Settings result = new Settings(name);
        String[] limits = bounds(from, to);
        result.start = limits[0];
        result.end = limits[1];
        result.resources = "\"in_app_events\"";
        return result;
    }

    private String[] bounds(int from, int to) {
        return new String[] {day + " 00:00:0" + from, day + " 00:00:0" + to};
    }

    private static String raw(String endpoint, Settings options, String fields) {
        String from = options.start.length() == 10 ? options.start + " 00:00:00" : options.start;
        String to = options.end.length() == 10 ? options.end + " 00:00:00" : options.end;
        return raw(endpoint, new String[] {from, to}, fields);
    }

    private static String raw(String endpoint, String[] bounds, String fields) {
        return target(
                AppsFlyerPeer.HOST,
                RAW + endpoint + "/v5",
                Map.of(
                        "from",
                        bounds[0],
                        "to",
                        bounds[1],
                        "timezone",
                        ZONE,
                        "additional_fields",
                        fields,
                        "currency",
                        "preferred",
                        "maximum_rows",
                        "1000000"));
    }

    private static String aggregate(
            String endpoint, LocalDate from, LocalDate to, boolean reattribution) {
        Map<String, String> parameters = new HashMap<>();
        parameters.put("from", from.toString());
        parameters.put("to", to.toString());
        parameters.put("timezone", ZONE);
        if (reattribution) {
            parameters.put("reattr", "true");
        }
        return target(AppsFlyerPeer.HOST, AGG + endpoint + "/v5", parameters);
    }

    private static String target(String host, String path, Map<String, String> parameters) {
        return host + " " + path + " " + new TreeMap<>(parameters);
    }

    private static String uninstallFields() {
        return EVENT_FIELDS
                .replace("app_type,att,", "")
                .replace("campaign_type,", "")
                .replace("conversion_type,", "");
    }

    private static String events(String rows) {
        return "Event Time,Event Name\n" + rows;
    }

    private static String event(String time, String name) {
        return time + "," + name + "\n";
    }

    private static String csv(String target, String value) {
        return step(target, "bearer", 200, "csv", "", "", value);
    }

    private static String redirect(String target, String role, String location) {
        return step(target, role, 302, "csv", location, "", "");
    }

    private static String status(String target, int status, String wait, String body) {
        return step(target, "bearer", status, "csv", "", wait, body);
    }

    private static String step(
            String target,
            String role,
            int status,
            String mode,
            String location,
            String wait,
            String body) {
        return step(
                target, role, status, mode, location, wait, body.getBytes(StandardCharsets.UTF_8));
    }

    private static String step(
            String target,
            String role,
            int status,
            String mode,
            String location,
            String wait,
            byte[] body) {
        return String.join(
                "\t",
                target,
                role,
                Integer.toString(status),
                mode,
                location,
                wait,
                Base64.getEncoder().encodeToString(body));
    }

    /**
     * Passes synthetic report settings through the real engine command-line variables. Raw tests
     * can narrow the initial window to a precise second instead of wall-clock waits.
     */
    private final class Settings {
        private final String name;
        private String url = API_URL;
        private String start = day.toString();
        private String end = day.plusDays(1).toString();
        private String resources =
                String.join(
                        ", ",
                        "\"in_app_events\"",
                        "\"organic_in_app_events\"",
                        "\"retargeting_in_app_events\"",
                        "\"installs\"",
                        "\"organic_installs\"",
                        "\"retargeting_installs\"",
                        "\"uninstall_events\"",
                        "\"organic_uninstall_events\"",
                        "\"daily_report\"",
                        "\"retargeting_daily_report\"",
                        "\"partners_report\"",
                        "\"retargeting_partners_report\"",
                        "\"partners_events_report\"",
                        "\"retargeting_partners_events_report\"",
                        "\"geo_report\"",
                        "\"retargeting_geo_report\"",
                        "\"geo_events_report\"",
                        "\"retargeting_geo_events_report\"");
        private int records = 100000;
        private int bytes = 33554432;
        private int buffer = 33554432;
        private int field = 262144;
        private int requests = 1000;
        private int deadline = 30000;
        private int attempts = 3;

        private Settings(String name) {
            this.name = name;
        }

        private String[] variables() {
            return new String[] {
                "fly.token=" + AppsFlyerPeer.TOKEN,
                "fly.api=" + url,
                "fly.app=" + APP,
                "fly.start=" + start,
                "fly.end=" + end,
                "fly.zone=" + ZONE,
                "fly.resources=[" + resources + "]",
                "fly.download=" + DOWNLOAD,
                "fly.output=" + name,
                "fly.records=" + records,
                "fly.bytes=" + bytes,
                "fly.buffer=" + buffer,
                "fly.field=" + field,
                "fly.requests=" + requests,
                "fly.deadline=" + deadline,
                "fly.attempts=" + attempts
            };
        }
    }

    /**
     * Runs the stock Java 11 engine with AppsFlyer's real hostname mapped to the test peer. The JVM
     * trust store includes only the trusted fixture identity.
     */
    private static final class Engine extends SeaTunnelContainer {
        private static final String ROOT = "/tmp/appsflyer-https-fixture";
        private final Path fixtures;

        private Engine(Path fixtures) {
            this.fixtures = fixtures;
        }

        @Override
        protected String getJavaToolOptions() {
            return "-Djavax.net.ssl.trustStore="
                    + ROOT
                    + "/trust.jks -Djavax.net.ssl.trustStoreType=JKS"
                    + " -Djavax.net.ssl.trustStorePassword="
                    + AppsFlyerPeer.PASSWORD;
        }

        @Override
        protected void executeExtraCommands(GenericContainer<?> container) throws IOException {
            container.withExtraHost(AppsFlyerPeer.HOST, "127.0.0.1");
            container.withExtraHost(AppsFlyerPeer.DOWNLOAD, "127.0.0.1");
            for (String file : Arrays.asList("trusted.p12", "untrusted.p12", "trust.jks")) {
                container.withCopyFileToContainer(
                        MountableFile.forHostPath(fixtures.resolve(file)), ROOT + "/" + file);
            }
            String javaPackage = AppsFlyerPeer.class.getPackage().getName().replace('.', '/');
            for (String file : Collections.singletonList("AppsFlyerPeer.class")) {
                try {
                    container.withCopyFileToContainer(
                            MountableFile.forHostPath(
                                    Paths.get(AppsFlyerPeer.class.getResource(file).toURI())),
                            ROOT + "/classes/" + javaPackage + "/" + file);
                } catch (URISyntaxException e) {
                    throw new IOException("Cannot stage the HTTPS provider fixture", e);
                }
            }
        }

        private Container.ExecResult runPeer() throws Exception {
            return server.execInContainer(
                    "java", "-cp", ROOT + "/classes", AppsFlyerPeer.class.getName(), ROOT);
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
            String dir = "/tmp/seatunnel_mnt/appsflyer-" + name;
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
            return split(result.getStdout());
        }

        private static List<String> split(String result) {
            List<String> lines = new ArrayList<>();
            for (String line : result.split("\\r?\\n")) {
                if (!line.trim().isEmpty()) {
                    lines.add(line);
                }
            }
            return lines;
        }
    }
}
