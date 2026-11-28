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

package org.apache.seatunnel.e2e.connector.alphavantage;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.seatunnel.SeaTunnelContainer;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
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
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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
 * Runs the source and transactional LocalFile sink in Zeta. A separate Java process serves scripted
 * TLS responses and records the requests made by the source.
 */
@Execution(ExecutionMode.SAME_THREAD)
@Timeout(value = 5, unit = TimeUnit.MINUTES)
class AlphaVantageIT extends TestSuiteBase {
    private static final String ALL = "/vantage-all.conf";
    private static final String ONE = "/vantage-one.conf";
    private static final String RESUME = "/vantage-resume.conf";
    private static final String URL =
            "https://" + AlphaVantageFixture.HOST + ":" + AlphaVantageFixture.PORT;
    private static final String DAILY = "TIME_SERIES_DAILY";
    private static final String INTRADAY = "TIME_SERIES_INTRADAY";
    private static final String QUOTE = "GLOBAL_QUOTE";
    private static final String DAILY_ENVELOPE = "Time Series (Daily)";
    private static final String OBSERVATION =
            "{\"1. open\":\"123.4500\",\"4. close\":"
                    + "12345678901234.12345678,\"5. volume\":9223372036854775808,"
                    + "\"nested\":{\"null_value\":null,\"unicode\":\"市场🌏\"}}";
    private static final String QUOTE_BODY =
            "{\"Global Quote\":{\"01. symbol\":\""
                    + AlphaVantageFixture.SYMBOL
                    + "\",\"05. price\":\"135.01000\",\"06. volume\":9223372036854775808,"
                    + "\"07. latest trading day\":\"2026-11-06\",\"09. change\":null}}";
    private static final ObjectMapper JSON = new ObjectMapper();

    @TempDir static Path temp;

    private final ExecutorService tasks = Executors.newCachedThreadPool();
    private Engine engine;
    private Future<Container.ExecResult> server;
    private String active;

    @BeforeAll
    void start() throws Exception {
        identities(temp);
        engine = new Engine(temp);
        engine.startUp();
        server = tasks.submit(engine::runFixture);
        Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> engine.exists("ready"));
    }

    @AfterEach
    void noUnexpectedRequests() throws Exception {
        if (active != null) {
            assertFalse(engine.exists(active + ".invalid"));
        }
    }

    @AfterAll
    void stop() throws Exception {
        try {
            if (engine != null) {
                try {
                    if (server != null) {
                        engine.touch("stop");
                        success(server.get(15, TimeUnit.SECONDS));
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
    void readsEveryDatedPointAndQuoteWithExactValuesAndScopedParameters() throws Exception {
        String name = "all-resources";
        String[] functions = {
            INTRADAY,
            DAILY,
            "TIME_SERIES_DAILY_ADJUSTED",
            "TIME_SERIES_WEEKLY",
            "TIME_SERIES_WEEKLY_ADJUSTED",
            "TIME_SERIES_MONTHLY",
            "TIME_SERIES_MONTHLY_ADJUSTED"
        };
        String[] envelopes = {
            "Time Series (5min)",
            DAILY_ENVELOPE,
            DAILY_ENVELOPE,
            "Weekly Time Series",
            "Weekly Adjusted Time Series",
            "Monthly Time Series",
            "Monthly Adjusted Time Series"
        };
        List<String> program = new ArrayList<>();
        Map<String, String> expected = new HashMap<>();
        List<String> order = new ArrayList<>();
        for (int i = 0; i < functions.length; i++) {
            String f = functions[i];
            String nameOfResource = f.toLowerCase(Locale.ROOT);
            String newer = i == 0 ? "2026-11-06 10:15:00" : "2026-11-06";
            String older = i == 0 ? "2026-11-03 15:55:00" : "2026-11-03";
            String body =
                    series(envelopes[i], newer, OBSERVATION, older, "{\"4. close\":\"0.0100\"}");
            program.add(
                    step(
                            f,
                            i <= 2 ? "compact" : "",
                            i == 0 ? "5min" : "",
                            i == 0 ? "false" : "",
                            200,
                            "json",
                            "",
                            body));
            expect(
                    expected,
                    order,
                    nameOfResource,
                    "2026-11-03",
                    i == 0 ? older : null,
                    "{\"4. close\":\"0.0100\"}");
            expect(
                    expected,
                    order,
                    nameOfResource,
                    "2026-11-06",
                    i == 0 ? newer : null,
                    OBSERVATION);
        }
        program.add(quoteStep(200, "json", "", QUOTE_BODY));
        expect(
                expected,
                order,
                "quote",
                "2026-11-06",
                null,
                JSON.readTree(QUOTE_BODY).get("Global Quote").toString());
        program(name, program);

        success(run(ALL, settings(name, "quote").withPacing(2000)));
        List<JsonNode> rows = persisted(name);
        Map<String, JsonNode> actual = new HashMap<>();
        Map<String, List<String>> emitted = new HashMap<>();
        for (JsonNode row : rows) {
            String resource = row.path("resource").asText();
            assertEquals(AlphaVantageFixture.SYMBOL, row.path("symbol").asText());
            assertEquals(
                    "quote".equals(resource) ? null : "US/Eastern",
                    row.path("time_zone").asText(null));
            String key =
                    key(resource, row.path("date").asText(), row.path("timestamp").asText(null));
            assertNull(actual.put(key, JSON.readTree(row.path("data").asText())), key);
            emitted.computeIfAbsent(resource, ignored -> new ArrayList<>()).add(key);
        }
        assertEquals(expected.keySet(), actual.keySet());
        for (Map.Entry<String, List<String>> entry : emitted.entrySet()) {
            List<String> expectedOrder = new ArrayList<>();
            for (String key : order) {
                if (key.startsWith(entry.getKey() + "|")) {
                    expectedOrder.add(key);
                }
            }
            assertEquals(expectedOrder, entry.getValue());
        }
        for (String key : expected.keySet()) {
            assertEquals(JSON.readTree(expected.get(key)), actual.get(key), key);
        }
        for (JsonNode row : rows) {
            if (row.path("data").asText().contains("null_value")) {
                assertTrue(row.path("data").asText().contains("12345678901234.12345678"));
                assertTrue(row.path("data").asText().contains("9223372036854775808"));
            }
        }
        List<String> calls = engine.lines(name + ".attempts");
        assertEquals(8, calls.size());
        List<String> expectedFunctions = new ArrayList<>(Arrays.asList(functions));
        expectedFunctions.add(QUOTE);
        long previous = 0;
        for (int i = 0; i < calls.size(); i++) {
            String[] fields = calls.get(i).split("\\t");
            assertEquals(expectedFunctions.get(i), fields[0]);
            long received = Long.parseLong(fields[1]);
            if (i != 0) {
                assertTrue(received - previous >= 1700);
            }
            previous = received;
        }
    }

    @Test
    void sendsFullAndAdjustedOnlyForFunctionsThatSupportThem() throws Exception {
        String[] functions = {INTRADAY, "TIME_SERIES_DAILY_ADJUSTED"};
        String[] envelopes = {"Time Series (30min)", DAILY_ENVELOPE};
        for (int i = 0; i < functions.length; i++) {
            String f = functions[i];
            String resource = f.toLowerCase(Locale.ROOT);
            String dateKey = i == 0 ? "2026-11-06 10:30:00" : "2026-11-06";
            String name = "full-" + i;
            program(
                    name,
                    Collections.singletonList(
                            step(
                                    f,
                                    "full",
                                    i == 0 ? "30min" : "",
                                    i == 0 ? "true" : "",
                                    200,
                                    "json",
                                    "",
                                    series(
                                            envelopes[i],
                                            dateKey,
                                            "{\"5. adjusted close\":\"99.00100\",\"7. dividend amount\":\"0.1000\"}"))));
            Settings config = settings(name, resource);
            config.interval = "30min";
            config.adjusted = true;
            config.size = "full";
            success(run(ONE, config));
            List<JsonNode> rows = persisted(name);
            assertEquals(1, rows.size());
            JsonNode values = JSON.readTree(rows.get(0).path("data").asText());
            assertEquals("99.00100", values.path("5. adjusted close").asText());
            assertEquals("0.1000", values.path("7. dividend amount").asText());
            assertEquals(Collections.singletonList(f), functions(name));
        }
    }

    @Test
    void completesExplicitEmptySeriesAndQuote() throws Exception {
        String name = "empty";
        program(
                name,
                Arrays.asList(
                        dailyStep(200, "json", "", series(DAILY_ENVELOPE)),
                        quoteStep(200, "json", "", "{\"Global Quote\":{}}")));
        success(run(RESUME, settings(name, "quote")));
        assertTrue(persisted(name).isEmpty());
        assertEquals(Arrays.asList(DAILY, QUOTE), functions(name));
    }

    @Test
    void rejectsMalformedWrongSymbolDuplicateAndOverLimitSeries() throws Exception {
        String name = "shape";
        program(
                name,
                Collections.singletonList(
                        dailyStep(200, "json", "", "{\"Time Series (Daily)\":[] }")));
        failure(
                run(ONE, settings(name, "time_series_daily")),
                "expected a Time Series (Daily) object");
        assertTrue(persisted(name).isEmpty());

        name = "symbol";
        String badSymbol =
                series(DAILY_ENVELOPE, "2026-11-06", OBSERVATION)
                        .replace(AlphaVantageFixture.SYMBOL, "ANOTHER");
        program(name, Collections.singletonList(dailyStep(200, "json", "", badSymbol)));
        failure(run(ONE, settings(name, "time_series_daily")), "response symbol differs");

        name = "wrong-date";
        program(
                name,
                Collections.singletonList(
                        dailyStep(
                                200,
                                "json",
                                "",
                                series(DAILY_ENVELOPE, "2026-11-31", OBSERVATION))));
        failure(run(ONE, settings(name, "time_series_daily")), "invalid trading date");

        name = "duplicate";
        program(
                name,
                Collections.singletonList(
                        dailyStep(
                                200,
                                "json",
                                "",
                                series(DAILY_ENVELOPE, "2026-11-06", "{\"x\":1,\"x\":2}"))));
        failure(
                run(ONE, settings(name, "time_series_daily")),
                "invalid, incomplete or unreadable JSON response");

        name = "records";
        program(
                name,
                Collections.singletonList(
                        dailyStep(
                                200,
                                "json",
                                "",
                                series(
                                        DAILY_ENVELOPE,
                                        "2026-11-06",
                                        OBSERVATION,
                                        "2026-11-03",
                                        OBSERVATION))));
        Settings config = settings(name, "time_series_daily");
        config.records = 1;
        failure(run(ONE, config), "exceeds max_records");
        assertEquals(1, functions(name).size());
    }

    @Test
    void rejectsProviderErrorsAndEntitlementResponsesWithoutLeakingDetails() throws Exception {
        String name = "provider-error";
        program(
                name,
                Collections.singletonList(
                        quoteStep(
                                200,
                                "json",
                                "",
                                "{\"Error Message\":\"private-provider-message "
                                        + AlphaVantageFixture.TOKEN
                                        + "\"}")));
        failure(run(ONE, settings(name, "quote")), "provider rejected the query");
        assertEquals(1, functions(name).size());
        assertTrue(persisted(name).isEmpty());

        name = "premium";
        program(
                name,
                Collections.singletonList(
                        quoteStep(
                                200,
                                "json",
                                "",
                                "{\"Information\":\"This is a premium endpoint with private-provider-message\"}")));
        failure(run(ONE, settings(name, "quote")), "subscription or entitlement requirement");
        assertEquals(1, functions(name).size());

        name = "unknown-information";
        program(
                name,
                Collections.singletonList(
                        quoteStep(
                                200,
                                "json",
                                "",
                                "{\"Information\":\"The supplied key is invalid\"}")));
        failure(run(ONE, settings(name, "quote")), "provider returned information without data");
        assertEquals(1, functions(name).size());

        for (int status : new int[] {401, 403}) {
            name = "auth-" + status;
            program(
                    name,
                    Collections.singletonList(
                            quoteStep(
                                    status,
                                    "json",
                                    "",
                                    "{\"error\":\"private-provider-message\"}")));
            failure(run(ONE, settings(name, "quote")), "HTTP " + status);
            assertEquals(1, functions(name).size());
        }
    }

    @Test
    void retriesProviderNoteInformationAndTemporaryHttpStatusesWithinOneBudget() throws Exception {
        String name = "retry-note";
        program(
                name,
                Arrays.asList(
                        quoteStep(200, "json", "", "{\"Note\":\"API call frequency exceeded\"}"),
                        quoteStep(429, "json", "1", ""),
                        quoteStep(503, "json", "date+2", ""),
                        quoteStep(200, "json", "", QUOTE_BODY)));
        Settings config = settings(name, "quote");
        config.attempts = 4;
        success(run(ONE, config));
        List<String> calls = engine.lines(name + ".attempts");
        assertEquals(4, calls.size());
        long unavailable = Long.parseLong(calls.get(2).split("\\t")[1]);
        long recovered = Long.parseLong(calls.get(3).split("\\t")[1]);
        assertTrue(recovered - unavailable > 1000);
        assertEquals(1, persisted(name).size());

        name = "retry-information";
        program(
                name,
                Collections.singletonList(
                        quoteStep(
                                200,
                                "json",
                                "",
                                "{\"Information\":\"API rate limit of one request per second exceeded\"}")));
        config = settings(name, "quote");
        config.attempts = 2;
        failure(run(ONE, config), "retry limit reached after provider rate limit");
        assertEquals(2, functions(name).size());

        name = "long-retry";
        program(name, Collections.singletonList(quoteStep(429, "json", "90", "")));
        config = settings(name, "quote");
        config.deadline = 7000;
        failure(run(ONE, config), "pacing or retry delay exceeds request deadline");
        assertEquals(1, functions(name).size());
    }

    @Test
    void sharesAttemptBudgetAcrossResourcesAndStopsOnExplicitDailyQuota() throws Exception {
        String name = "reader-budget";
        program(
                name,
                Arrays.asList(
                        dailyStep(503, "json", "", ""),
                        dailyStep(
                                200,
                                "json",
                                "",
                                series(DAILY_ENVELOPE, "2026-11-06", OBSERVATION))));
        Settings limited = settings(name, "quote");
        limited.requests = 2;
        failure(run(RESUME, limited), "request budget exhausted (max_requests)");
        assertEquals(Arrays.asList(DAILY, DAILY), functions(name));

        name = "daily-information";
        program(
                name,
                Collections.singletonList(
                        quoteStep(
                                200,
                                "json",
                                "",
                                "{\"Information\":\"API rate limit of 25 requests per day reached\"}")));
        failure(run(ONE, settings(name, "quote")), "provider daily request quota reached");
        assertEquals(Collections.singletonList(QUOTE), functions(name));
        assertTrue(persisted(name).isEmpty());

        name = "daily-note";
        program(
                name,
                Collections.singletonList(
                        quoteStep(
                                200,
                                "json",
                                "",
                                "{\"Note\":\"Daily quota reached. Please subscribe for more calls.\"}")));
        failure(run(ONE, settings(name, "quote")), "provider daily request quota reached");
        assertEquals(Collections.singletonList(QUOTE), functions(name));
    }

    @Test
    void refusesRedirectsUntrustedCertificatesAndSpoofedOrigins() throws Exception {
        String name = "redirect";
        program(name, Collections.singletonList(quoteStep(302, "redirect", "", "")));
        failure(run(ONE, settings(name, "quote")), "HTTP 302");
        assertEquals(1, functions(name).size());

        name = "untrusted";
        program(name, Collections.singletonList(quoteStep(200, "json", "", QUOTE_BODY)));
        Settings config = settings(name, "quote");
        config.url =
                "https://" + AlphaVantageFixture.HOST + ":" + AlphaVantageFixture.UNTRUSTED_PORT;
        failure(run(ONE, config), "HTTPS or TLS exchange failed");
        assertTrue(functions(name).isEmpty());

        name = "origin";
        program(name, Collections.singletonList(quoteStep(200, "json", "", QUOTE_BODY)));
        config = settings(name, "quote");
        config.url = "https://www.alphavantage.co.foreign.invalid";
        failure(run(ONE, config), "api_url must be the HTTPS origin");
        assertTrue(functions(name).isEmpty());
    }

    @Test
    void enforcesBothWireAndDecodedBodyLimitsAndAcceptsValidGzip() throws Exception {
        String name = "gzip-ok";
        program(name, Collections.singletonList(quoteStep(200, "gzip", "", QUOTE_BODY)));
        Settings config = settings(name, "quote");
        config.bytes = 1024;
        success(run(ONE, config));
        assertEquals(
                "135.01000",
                JSON.readTree(persisted(name).get(0).path("data").asText())
                        .path("05. price")
                        .asText());

        String large = QUOTE_BODY.replace("\"135.01000\"", "\"" + "z".repeat(3000) + "\"");
        for (String encoding : new String[] {"gzip", "json"}) {
            name = "large-" + encoding;
            program(name, Collections.singletonList(quoteStep(200, encoding, "", large)));
            config = settings(name, "quote");
            config.bytes = 1024;
            failure(run(ONE, config), "exceeds max_response_bytes");
            assertEquals(1, functions(name).size());
            assertTrue(persisted(name).isEmpty());
        }
    }

    @Test
    void totalDeadlineClosesASlowFlowBelowTheReadIdleTimeout() throws Exception {
        String name = "deadline";
        program(name, Collections.singletonList(quoteStep(200, "trickle", "", "")));
        Settings config = settings(name, "quote");
        config.deadline = 2000;
        failure(run(ONE, config), "request deadline exceeded");
        Awaitility.await()
                .atMost(15, TimeUnit.SECONDS)
                .until(() -> engine.exists(name + ".disconnect"));
        assertEquals(1, functions(name).size());
    }

    @Test
    void engineCancellationClosesAnActiveHttpsRequest() throws Exception {
        String name = "cancel";
        program(name, Collections.singletonList(quoteStep(200, "trickle", "", "")));
        Settings config = settings(name, "quote");
        config.deadline = 120000;
        String id = id();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(ONE, id, config.variables()));
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> functions(name).size() == 1);
            success(engine.cancelJob(id));
            running.get(30, TimeUnit.SECONDS);
            Awaitility.await()
                    .atMost(20, TimeUnit.SECONDS)
                    .until(() -> engine.exists(name + ".disconnect"));
            assertEquals(1, functions(name).size());
        } finally {
            if (!running.isDone()) {
                engine.cancelJob(id);
                running.cancel(true);
            }
        }
    }

    @Test
    void savesWhileQuoteIsHeldRestoresTheExactScopeAndKeepsDailyRows() throws Exception {
        String name = "restore";
        program(
                name,
                Arrays.asList(
                        dailyStep(
                                200, "json", "", series(DAILY_ENVELOPE, "2026-11-03", OBSERVATION)),
                        quoteStep(200, "hold", "", QUOTE_BODY),
                        quoteStep(200, "json", "", QUOTE_BODY)));
        Settings config = settings(name, "quote");
        config.deadline = 120000;
        String id = id();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(RESUME, id, config.variables()));
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> functions(name).size() == 2);
            long checkpoints = engine.getCompletedCheckpointCount(id);
            Awaitility.await()
                    .atMost(25, TimeUnit.SECONDS)
                    .until(() -> engine.getCompletedCheckpointCount(id) > checkpoints);
            success(engine.savepointJob(id));
            running.get(35, TimeUnit.SECONDS);
            engine.touch(name + ".release");

            Settings wrong = settings("wrong-restore", "quote");
            wrong.interval = "30min";
            failure(
                    engine.restoreJob(RESUME, id, wrong.variables()),
                    "query configuration changed during restore");
            assertEquals(Arrays.asList(DAILY, QUOTE), functions(name));
            success(engine.restoreJob(RESUME, id, config.variables()));
            assertEquals(Arrays.asList(DAILY, QUOTE, QUOTE), functions(name));
            List<JsonNode> rows = persisted(name);
            assertEquals(2, rows.size());
            Map<String, JsonNode> byResource = new HashMap<>();
            for (JsonNode row : rows) {
                byResource.put(row.path("resource").asText(), row);
            }
            assertEquals("2026-11-03", byResource.get("time_series_daily").path("date").asText());
            assertEquals(
                    "135.01000",
                    JSON.readTree(byResource.get("quote").path("data").asText())
                            .path("05. price")
                            .asText());
        } finally {
            engine.touch(name + ".release");
            if (!running.isDone()) {
                engine.cancelJob(id);
                running.cancel(true);
            }
        }
    }

    private Container.ExecResult run(String conf, Settings config) throws Exception {
        return engine.executeJob(conf, Arrays.asList(config.variables()));
    }

    private void program(String name, List<String> steps) throws Exception {
        if (active != null) {
            assertFalse(engine.exists(active + ".invalid"));
        }
        List<String> program = new ArrayList<>();
        program.add(name);
        program.addAll(steps);
        Path source = temp.resolve(name + ".tsv");
        Files.write(source, program, StandardCharsets.UTF_8);
        engine.install(source, "active.tsv");
        active = name;
    }

    private List<String> functions(String name) throws Exception {
        List<String> functions = new ArrayList<>();
        for (String line : engine.lines(name + ".attempts")) {
            functions.add(line.split("\\t", -1)[0]);
        }
        return functions;
    }

    private List<JsonNode> persisted(String name) throws Exception {
        List<JsonNode> rows = new ArrayList<>();
        for (String line : engine.rows(name)) {
            rows.add(JSON.readTree(line));
        }
        return rows;
    }

    private static String dailyStep(int status, String mode, String retry, String data) {
        return step(DAILY, "compact", "", "", status, mode, retry, data);
    }

    private static String quoteStep(int status, String mode, String retry, String data) {
        return step(QUOTE, "", "", "", status, mode, retry, data);
    }

    private static String step(
            String function,
            String size,
            String interval,
            String adjusted,
            int status,
            String mode,
            String retry,
            String payload) {
        String body = Base64.getEncoder().encodeToString(payload.getBytes(StandardCharsets.UTF_8));
        return String.join(
                "\t",
                function,
                size,
                interval,
                adjusted,
                Integer.toString(status),
                mode,
                retry,
                body);
    }

    private static String series(String envelope, String... entries) {
        StringBuilder body =
                new StringBuilder("{\"Meta Data\":{\"2. Symbol\":\"")
                        .append(AlphaVantageFixture.SYMBOL)
                        .append("\",\"5. Time Zone\":\"US/Eastern\"},\"")
                        .append(envelope)
                        .append("\":{");
        for (int i = 0; i < entries.length; i += 2) {
            if (i > 0) {
                body.append(',');
            }
            body.append('"').append(entries[i]).append("\":").append(entries[i + 1]);
        }
        return body.append("}}").toString();
    }

    private static void expect(
            Map<String, String> expected,
            List<String> order,
            String resource,
            String date,
            String timestamp,
            String data) {
        String key = key(resource, date, timestamp);
        expected.put(key, data);
        order.add(key);
    }

    private static String key(String resource, String date, String timestamp) {
        return String.join("|", resource, date, timestamp == null ? "" : timestamp);
    }

    private static Settings settings(String name, String resource) {
        return new Settings(name, resource);
    }

    private static String id() {
        return Long.toString(
                ThreadLocalRandom.current().nextLong(1_000_000_000_000L, 9_000_000_000_000L));
    }

    private static String output(Container.ExecResult result) {
        return result.getStdout() + result.getStderr();
    }

    private static void success(Container.ExecResult result) {
        assertEquals(0, result.getExitCode(), () -> output(result));
    }

    private static void failure(Container.ExecResult result, String reason) {
        assertNotEquals(0, result.getExitCode(), () -> output(result));
        assertTrue(output(result).contains(reason), () -> output(result));
        assertFalse(output(result).contains(AlphaVantageFixture.TOKEN));
        assertFalse(output(result).contains("vantage-fixture-key%25%26%2B"));
        assertFalse(output(result).contains("private-provider-message"));
    }

    private static void identities(Path root) throws Exception {
        KeyStore trusted = keytool(root, "trusted");
        keytool(root, "untrusted");
        KeyStore trust = KeyStore.getInstance("JKS");
        trust.load(null, null);
        trust.setCertificateEntry("fixture", trusted.getCertificate("fixture"));
        try (OutputStream sink = Files.newOutputStream(root.resolve("trust.jks"))) {
            trust.store(sink, AlphaVantageFixture.PASSWORD.toCharArray());
        }
    }

    private static KeyStore keytool(Path root, String name) throws Exception {
        Path output = root.resolve(name + ".p12");
        String password = AlphaVantageFixture.PASSWORD;
        Process child =
                new ProcessBuilder(
                                Paths.get(System.getProperty("java.home"), "bin", "keytool")
                                        .toString(),
                                "-genkeypair",
                                "-alias",
                                "fixture",
                                "-keyalg",
                                "RSA",
                                "-keysize",
                                "2048",
                                "-dname",
                                "CN=" + AlphaVantageFixture.HOST,
                                "-validity",
                                "3",
                                "-ext",
                                "SAN=dns:" + AlphaVantageFixture.HOST,
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                output.toString(),
                                "-storepass",
                                password,
                                "-keypass",
                                password,
                                "-noprompt")
                        .redirectErrorStream(true)
                        .redirectOutput(root.resolve(name + ".keytool.log").toFile())
                        .start();
        if (!child.waitFor(30, TimeUnit.SECONDS) || child.exitValue() != 0) {
            child.destroyForcibly();
            throw new IOException("The HTTPS fixture could not create a TLS identity");
        }
        KeyStore key = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(output)) {
            key.load(in, password.toCharArray());
        }
        return key;
    }

    /**
     * Controls one test job through SeaTunnel configuration variables so each scenario can vary
     * query, pacing and size bounds without changing the container.
     */
    private static final class Settings {
        private final String name;
        private final String resource;
        private String url = URL;
        private String interval = "5min";
        private boolean adjusted;
        private String size = "compact";
        private int bytes = 8388608;
        private int records = 50000;
        private int attempts = 3;
        private int requests = 24;
        private int deadline = 30000;
        private int pacing = 1;

        private Settings(String name, String resource) {
            this.name = name;
            this.resource = resource;
        }

        private Settings withPacing(int milliseconds) {
            pacing = milliseconds;
            return this;
        }

        private String[] variables() {
            return new String[] {
                "vantage.url=" + url,
                "vantage.token=" + AlphaVantageFixture.TOKEN,
                "vantage.symbol=" + AlphaVantageFixture.SYMBOL,
                "vantage.resource=" + resource,
                "vantage.interval=" + interval,
                "vantage.adjusted=" + adjusted,
                "vantage.size=" + size,
                "vantage.bytes=" + bytes,
                "vantage.records=" + records,
                "vantage.attempts=" + attempts,
                "vantage.requests=" + requests,
                "vantage.deadline=" + deadline,
                "vantage.pacing=" + pacing,
                "vantage.output=" + name
            };
        }
    }

    /**
     * Connects Zeta to a trusted local HTTPS fixture while keeping the supported hostname intact.
     */
    private static final class Engine extends SeaTunnelContainer {
        private static final String ROOT = "/tmp/alpha-vantage-fixture";
        private final Path work;

        private Engine(Path directory) {
            this.work = directory;
        }

        @Override
        protected String getJavaToolOptions() {
            return "-Djavax.net.ssl.trustStore="
                    + ROOT
                    + "/trust.jks -Djavax.net.ssl.trustStoreType=JKS"
                    + " -Djavax.net.ssl.trustStorePassword="
                    + AlphaVantageFixture.PASSWORD;
        }

        @Override
        protected void executeExtraCommands(GenericContainer<?> runtime) throws IOException {
            runtime.withExtraHost(AlphaVantageFixture.HOST, "127.0.0.1");
            for (String filename : Arrays.asList("trusted.p12", "untrusted.p12", "trust.jks")) {
                runtime.withCopyFileToContainer(
                        MountableFile.forHostPath(work.resolve(filename)), ROOT + "/" + filename);
            }
            String path = AlphaVantageFixture.class.getPackage().getName().replace('.', '/');
            for (String filename :
                    Arrays.asList("AlphaVantageFixture.class", "AlphaVantageFixture$Step.class")) {
                try {
                    runtime.withCopyFileToContainer(
                            MountableFile.forHostPath(
                                    Paths.get(
                                            AlphaVantageFixture.class
                                                    .getResource(filename)
                                                    .toURI())),
                            ROOT + "/classes/" + path + "/" + filename);
                } catch (URISyntaxException e) {
                    throw new IOException("Cannot stage the independent HTTPS fixture", e);
                }
            }
        }

        private Container.ExecResult runFixture() throws Exception {
            return server.execInContainer(
                    "java", "-cp", ROOT + "/classes", AlphaVantageFixture.class.getName(), ROOT);
        }

        private void install(Path data, String filename) {
            server.copyFileToContainer(MountableFile.forHostPath(data), ROOT + "/" + filename);
        }

        private boolean exists(String file) throws Exception {
            return server.execInContainer("test", "-f", ROOT + "/" + file).getExitCode() == 0;
        }

        private void touch(String file) throws Exception {
            success(server.execInContainer("touch", ROOT + "/" + file));
        }

        private List<String> lines(String file) throws Exception {
            if (!exists(file)) {
                return Collections.emptyList();
            }
            Container.ExecResult result = server.execInContainer("cat", ROOT + "/" + file);
            success(result);
            return readLines(result.getStdout());
        }

        private List<String> rows(String name) throws Exception {
            String dir = "/tmp/seatunnel_mnt/vantage-" + name;
            Container.ExecResult result =
                    server.execInContainer(
                            "sh",
                            "-c",
                            "if [ -d "
                                    + dir
                                    + " ]; then find "
                                    + dir
                                    + " -maxdepth 1 -type f -name 'receipt_*' "
                                    + "-exec sh -c 'for f do cat \"$f\"; printf \"\\n\"; done' sh {} +; fi");
            success(result);
            return readLines(result.getStdout());
        }

        private static List<String> readLines(String output) {
            List<String> lines = new ArrayList<>();
            for (String line : output.split("\\r?\\n")) {
                if (!line.trim().isEmpty()) {
                    lines.add(line);
                }
            }
            return lines;
        }
    }
}
