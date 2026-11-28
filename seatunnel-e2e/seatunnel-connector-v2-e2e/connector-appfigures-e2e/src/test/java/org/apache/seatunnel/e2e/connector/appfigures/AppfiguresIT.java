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

package org.apache.seatunnel.e2e.connector.appfigures;

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
import java.net.HttpURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
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
 * Runs provider-contract scenarios through the real Zeta engine and transactional JSON files.
 *
 * <p>The independent Java HTTPS peer verifies every path, query and credential header. Assertions
 * inspect committed rows rather than diagnostic console output or connector internals.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AppfiguresIT extends TestSuiteBase {
    private static final String ONE = "/appfigures-one.conf";
    private static final String USERS = "/appfigures-users.conf";
    private static final String PRODUCTS = "/appfigures-products.conf";
    private static final String GROUPED = "/appfigures-grouped.conf";
    private static final String STORE = "/appfigures-store.conf";
    private static final String TERMINAL = "/appfigures-terminal.conf";
    private static final String EXTRA =
            ",\"unknown\":{\"unicode\":\"数据🌏\",\"null\":null,\"items\":[false,null,[1,2]],"
                    + "\"integer\":9223372036854775808,\"decimal\":123456789012345678.123456789}";
    private static final String[] REPORTS = {
        "reports_sales",
        "reports_revenue",
        "reports_subscriptions",
        "reports_ads",
        "reports_adspend",
        "reports_ratings"
    };
    private static final String[] REPORT_ROUTES = {
        "reports/sales",
        "reports/revenue",
        "reports/subscriptions",
        "reports/ads",
        "reports/adspend",
        "reports/ratings"
    };
    private static final String[] METRICS = {
        "\"downloads\":7,\"revenue\":1.2300", "\"sales\":1.2300,\"total\":3.5000",
        "\"active_subscriptions\":7,\"mrr\":1.2300", "\"impressions\":7,\"revenue\":1.2300",
        "\"installs\":7,\"cost\":1.2300", "\"total\":7,\"average\":4.5000"
    };
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
    void startEngineAndIndependentHttpsPeer() throws Exception {
        KeyStore trusted = identity("trusted");
        identity("untrusted");
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

    @Test
    void readsAllElevenResourcesWithExactFieldsAndNumericPrecision() throws Exception {
        String user = entity(42, ",\"last_login\":\"2024-01-01T12:00:00\"" + EXTRA);
        check(
                "all-status",
                "status",
                "/v2/",
                "",
                "{\"status\":200,\"user\":" + user + "}",
                Map.of("42", expected("[]", user)));
        String category = entity(17, ",\"name\":\"Unknown future category\"" + EXTRA);
        check(
                "all-categories",
                "data_categories",
                "/v2/data/categories",
                "",
                "{\"17\":" + category + "}",
                Map.of("17", expected("[\"17\"]", category)));
        String usage =
                "{\"api_requests\":{\"remaining\":544},\"partner_api\":{\"used\":0}" + EXTRA + "}";
        check("all-usage", "usage", "/v2/usage", "", usage, Map.of("usage", expected("[]", usage)));
        check(
                "all-users",
                "users",
                "/v2/users",
                "count=2&page=1",
                page(1, 2, 1, 1, user),
                Map.of("42", expected("[]", user)));
        String product = entity(101, ",\"store\":\"apple\"" + EXTRA);
        check(
                "all-products",
                "products_mine_search",
                "/v2/products/mine",
                "store=apple",
                "{\"101\":" + product + "}",
                Map.of("101", expected("[\"101\"]", product)));
        for (int i = 0; i < REPORTS.length; i++) {
            String metrics = "{" + METRICS[i] + EXTRA + "}";
            check(
                    "all-" + REPORTS[i],
                    REPORTS[i],
                    "/v2/" + REPORT_ROUTES[i],
                    "format=json",
                    metrics,
                    Map.of("total", expected("[]", metrics)));
        }
    }

    @Test
    void appliesLocalLoginBoundsAfterCompleteUserPagination() throws Exception {
        String lower = entity(1, ",\"last_login\":\"2024-01-01T00:00:00\"");
        String old = entity(2, ",\"last_login\":\"2023-12-31T23:59:59.999999\"");
        String middle = entity(3, ",\"last_login\":\"2024-01-01T00:00:00.000001\"");
        String upper = entity(4, ",\"last_login\":\"2024-01-02T00:00:00\"");
        String missing = entity(5, ""), absent = entity(6, ",\"last_login\":null");
        String name = "login-window";
        program(
                name,
                List.of(
                        ok("/v2/users", "count=2&page=1", page(1, 2, 6, 3, lower, old)),
                        ok("/v2/users", "count=2&page=2", page(2, 2, 6, 3, middle, upper)),
                        ok("/v2/users", "count=2&page=3", page(3, 2, 6, 3, missing, absent))));
        success(run(USERS, new Settings(name, "users")));
        assertRows(name, "users", Map.of("1", expected("[]", lower), "3", expected("[]", middle)));
        assertEquals(3, requests(name).size());
    }

    @Test
    void readsLegacyProductPagesWithoutInventingFirstPageParameters() throws Exception {
        String a = entity(101, ",\"store\":\"apple\""), b = entity(102, ",\"store\":\"apple\"");
        String c = entity(103, ",\"store\":\"apple\"");
        String name = "legacy-products";
        program(
                name,
                List.of(
                        ok("/v2/products/mine", "store=apple", page(1, 2, 3, 2, a, b)),
                        ok(
                                "/v2/products/mine",
                                "count=2&page=2&store=apple",
                                page(2, 2, 3, 2, c))));
        success(run(PRODUCTS, new Settings(name, "products_mine_search")));
        assertRows(
                name,
                "products_mine_search",
                Map.of(
                        "101",
                        expected("[]", a),
                        "102",
                        expected("[]", b),
                        "103",
                        expected("[]", c)));
        assertEquals(2, requests(name).size());
    }

    @Test
    void preservesGroupedPathsAndUsesResourceSpecificWirePivots() throws Exception {
        String metric = "{\"downloads\":3,\"revenue\":12.3400" + EXTRA + "}";
        String other = "{\"downloads\":0,\"revenue\":0}";
        String name = "sales-groups";
        String query =
                "end_date=2024-01-02&format=json&group_by=products,countries&start_date=2024-01-01";
        program(
                name,
                List.of(
                        ok(
                                "/v2/reports/sales",
                                query,
                                "{\"101\":{\"US\":"
                                        + metric
                                        + ",\"JP\":"
                                        + other
                                        + "},\"102\":{\"US\":"
                                        + metric
                                        + "}}")));
        Settings options = new Settings(name, "reports_sales");
        options.group = "product,country";
        success(run(GROUPED, options));
        assertRows(
                name,
                options.resource,
                Map.of(
                        "EGraMLe4KMNeZ+7mpYDQxhcjdP41DnEZzEuUFnGrY5w=",
                                expected("[\"101\",\"US\"]", metric),
                        "BxmaETHAgImh4M9gCxG6kyHncRZg3Yr1ARQOqxeClUg=",
                                expected("[\"101\",\"JP\"]", other),
                        "3/rNDeoZgJnveDGEq9Hf/aJ/9wt9+DHeQxgq0Q5A0eM=",
                                expected("[\"102\",\"US\"]", metric)));
        for (int index : new int[] {3, 4}) {
            name = "combined-" + REPORTS[index];
            String data = "{" + METRICS[index] + "}";
            program(
                    name,
                    List.of(
                            ok(
                                    "/v2/" + REPORT_ROUTES[index],
                                    "combine=true&end_date=2024-01-02&format=json&group_by=store&start_date=2024-01-01",
                                    "{\"apple\":" + data + "}")));
            Settings combined = new Settings(name, REPORTS[index]);
            combined.group = "store";
            success(run(GROUPED, combined));
            assertRows(
                    name,
                    combined.resource,
                    Map.of(
                            "7v9fYbMcSwuzMh+aFZyPWRn+Zf2S9QUpMINnKOsc/GY=",
                            expected("[\"apple\"]", data)));
        }
    }

    @Test
    void limitsAllSixReportsToDiscoveredStoreProductsAndKeepsTheirTotals() throws Exception {
        String a = entity(101, ",\"store\":\"apple\""), b = entity(102, ",\"store\":\"apple\"");
        for (int index = 0; index < REPORTS.length; index++) {
            String name = "store-" + REPORTS[index];
            String metrics = "{" + METRICS[index] + "}";
            String query =
                    "format=json&products=101,102" + (index == 2 ? "&include_inapps=true" : "");
            program(
                    name,
                    List.of(
                            ok(
                                    "/v2/products/mine",
                                    "store=apple",
                                    "{\"101\":" + a + ",\"102\":" + b + "}"),
                            ok("/v2/" + REPORT_ROUTES[index], query, metrics)));
            success(run(STORE, new Settings(name, REPORTS[index])));
            assertRows(name, REPORTS[index], Map.of("total", expected("[]", metrics)));
            assertEquals(2, requests(name).size());
        }
        String name = "store-empty";
        program(name, List.of(ok("/v2/products/mine", "store=apple", "{}")));
        success(run(STORE, new Settings(name, "reports_sales")));
        assertRows(name, "reports_sales", Map.of());
        assertEquals(List.of("/v2/products/mine?store=apple"), requests(name));
    }

    @Test
    void rejectsDailyQuotaAndPermanentErrorsWithoutRetryingOrDisclosingCredentials()
            throws Exception {
        for (int status : new int[] {420, 401, 402, 403, 400}) {
            String name = "permanent-" + status;
            program(name, List.of(step("/v2/usage", "", status, "fixed", "0", privateError())));
            failure(run(ONE, new Settings(name, "usage")), "HTTP " + status);
            assertEquals(List.of("/v2/usage"), requests(name));
            assertRows(name, "usage", Map.of());
        }
        String name = "error-body";
        program(name, List.of(ok("/v2/usage", "", privateError())));
        failure(run(ONE, new Settings(name, "usage")), "error in the response body");
        assertRows(name, "usage", Map.of());
        assertEquals(1, requests(name).size());
    }

    @Test
    void retriesTransientResponsesWithinOneBudgetAndHonorsRetryAfter() throws Exception {
        String name = "retry-success";
        String usage = "{\"api_requests\":{\"remaining\":21}}";
        program(
                name,
                List.of(
                        step("/v2/usage", "", 503, "fixed", "0", ""),
                        step("/v2/usage", "", 429, "fixed", "1", ""),
                        ok("/v2/usage", "", usage)));
        success(run(ONE, new Settings(name, "usage")));
        assertRows(name, "usage", Map.of("usage", expected("[]", usage)));
        assertEquals(Collections.nCopies(3, "/v2/usage"), requests(name));
        List<String> times = engine.lines(name + ".times");
        assertEquals(3, times.size());
        assertTrue(
                Long.parseLong(times.get(2)) - Long.parseLong(times.get(1)) >= 950L,
                "Retry-After must delay the next request for the provider's full second");
        name = "retry-exhausted";
        program(name, Collections.nCopies(2, step("/v2/usage", "", 429, "fixed", "0", "")));
        Settings bounded = new Settings(name, "usage");
        bounded.attempts = 2;
        failure(run(ONE, bounded), "retry budget exhausted");
        assertEquals(2, requests(name).size());
        name = "retry-too-late";
        program(name, List.of(step("/v2/usage", "", 429, "fixed", "120", "")));
        failure(run(ONE, new Settings(name, "usage")), "retry delay exceeds");
        assertEquals(1, requests(name).size());
    }

    @Test
    void rejectsDuplicateIdsChangingTotalsAndInvalidProviderShapes() throws Exception {
        String user = entity(1, "");
        for (String second : List.of(page(2, 2, 3, 2, user), page(2, 2, 4, 2, entity(3, "")))) {
            String name = "bad-page-" + jobId();
            program(
                    name,
                    List.of(
                            ok(
                                    "/v2/users",
                                    "count=2&page=1",
                                    page(1, 2, 3, 2, user, entity(2, ""))),
                            ok("/v2/users", "count=2&page=2", second)));
            failure(run(ONE, new Settings(name, "users")), "Appfigures");
            assertEquals(2, requests(name).size());
        }
        for (String body :
                List.of(
                        "[]",
                        "{\"api_requests\":{},\"api_requests\":{}}",
                        "{\"api_requests\":{}} trailing")) {
            String name = "bad-json-" + jobId();
            program(name, List.of(ok("/v2/usage", "", body)));
            failure(run(ONE, new Settings(name, "usage")), "Appfigures");
            assertRows(name, "usage", Map.of());
        }
        String name = "wrong-store";
        program(
                name,
                List.of(
                        ok(
                                "/v2/products/mine",
                                "store=apple",
                                "{\"1\":" + entity(1, ",\"store\":\"google_play\"") + "}")));
        failure(run(ONE, new Settings(name, "products_mine_search")), "selected store");
        assertRows(name, "products_mine_search", Map.of());
    }

    @Test
    void readsGzipAndBoundsDecodedBytesAndRowCounts() throws Exception {
        String usage = "{\"api_requests\":{\"remaining\":7}" + EXTRA + "}";
        String name = "compressed";
        program(name, List.of(step("/v2/usage", "", 200, "gzip", "", usage)));
        success(run(ONE, new Settings(name, "usage")));
        assertRows(name, "usage", Map.of("usage", expected("[]", usage)));
        for (String mode : List.of("fixed", "gzip")) {
            name = "large-" + mode;
            String huge = "{\"api_requests\":{\"text\":\"" + "x".repeat(16000) + "\"}}";
            program(name, List.of(step("/v2/usage", "", 200, mode, "", huge)));
            Settings limited = new Settings(name, "usage");
            limited.bytes = 1024;
            failure(run(ONE, limited), "max_response_bytes");
            assertRows(name, "usage", Map.of());
        }
        name = "row-limit";
        program(
                name,
                List.of(
                        ok(
                                "/v2/data/categories",
                                "",
                                "{\"1\":" + entity(1, "") + ",\"2\":" + entity(2, "") + "}")));
        Settings rows = new Settings(name, "data_categories");
        rows.maxRows = 1;
        failure(run(ONE, rows), "max_rows");
        assertRows(name, "data_categories", Map.of());
    }

    @Test
    void rejectsRedirectsUntrustedCertificatesAndHostnameMismatch() throws Exception {
        String name = "redirect";
        program(name, List.of(step("/v2/usage", "", 302, "redirect", "", "")));
        failure(run(ONE, new Settings(name, "usage")), "HTTP 302");
        assertEquals(List.of("/v2/usage"), requests(name));
        for (String endpoint :
                List.of(
                        "https://" + AppfiguresPeer.HOST + ":" + AppfiguresPeer.UNTRUSTED_PORT,
                        "https://localhost:" + AppfiguresPeer.PORT)) {
            name = "bad-tls-" + jobId();
            program(name, List.of(ok("/v2/usage", "", "{\"api_requests\":{}}")));
            Settings tls = new Settings(name, "usage");
            tls.endpoint = endpoint;
            failure(run(ONE, tls), "certificate or hostname");
            assertTrue(requests(name).isEmpty());
        }
    }

    @Test
    void interruptsTrickledResponsesOnTheDeadlineAndEngineCancellation() throws Exception {
        String name = "deadline-stream";
        program(name, List.of(step("/v2/usage", "", 200, "stream", "", "")));
        Settings limit = new Settings(name, "usage");
        limit.timeout = 1500;
        limit.attempts = 1;
        failure(run(ONE, limit), "Appfigures");
        awaitDisconnect(name);
        long started = Long.parseLong(engine.lines(name + ".started").get(0));
        long ended = Long.parseLong(engine.lines(name + ".disconnect").get(0));
        assertTrue(ended - started < 8000L, "the request deadline must close a trickling body");
        name = "cancel-stream";
        program(name, List.of(step("/v2/usage", "", 200, "stream", "", "")));
        Settings cancel = new Settings(name, "usage");
        cancel.timeout = 180000;
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(ONE, job, cancel.variables()));
        try {
            awaitRequests(name, 1);
            success(engine.cancelJob(job));
            running.get(30, TimeUnit.SECONDS);
            awaitDisconnect(name);
            assertEquals(1, requests(name).size());
        } finally {
            stopIfRunning(job, running);
        }
    }

    @Test
    void restoresTheNextUserPageWithoutRefetchingCommittedUsers() throws Exception {
        String name = "resume-page";
        String a = entity(1, ""), b = entity(2, ""), c = entity(3, "");
        program(
                name,
                List.of(
                        ok("/v2/users", "count=2&page=1", page(1, 2, 3, 2, a, b)),
                        step("/v2/users", "count=2&page=2", 200, "hold", "", page(2, 2, 3, 2, c)),
                        ok("/v2/users", "count=2&page=2", page(2, 2, 3, 2, c))));
        Settings options = new Settings(name, "users");
        options.timeout = 180000;
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(ONE, job, options.variables()));
        try {
            awaitRequests(name, 2);
            savepoint(job, running);
            engine.touch(name + ".release");
            success(engine.restoreJob(ONE, job, options.variables()));
            assertRows(
                    name,
                    "users",
                    Map.of("1", expected("[]", a), "2", expected("[]", b), "3", expected("[]", c)));
            assertEquals(
                    List.of(
                            "/v2/users?count=2&page=1",
                            "/v2/users?count=2&page=2",
                            "/v2/users?count=2&page=2"),
                    requests(name));
        } finally {
            engine.touch(name + ".release");
            stopIfRunning(job, running);
        }
    }

    @Test
    void restoresWhileReceivingAResponseWithoutPublishingPartialJson() throws Exception {
        String name = "resume-response";
        String user = entity(7, EXTRA);
        String body = page(1, 2, 1, 1, user);
        program(
                name,
                List.of(
                        step("/v2/users", "count=2&page=1", 200, "partial", "", body),
                        ok("/v2/users", "count=2&page=1", body)));
        Settings options = new Settings(name, "users");
        options.timeout = 180000;
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(ONE, job, options.variables()));
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> engine.exists(name + ".started"));
            savepoint(job, running);
            engine.touch(name + ".release");
            success(engine.restoreJob(ONE, job, options.variables()));
            assertRows(name, "users", Map.of("7", expected("[]", user)));
            assertEquals(Collections.nCopies(2, "/v2/users?count=2&page=1"), requests(name));
        } finally {
            engine.touch(name + ".release");
            stopIfRunning(job, running);
        }
    }

    @Test
    void rejectsChangedReadSettingsBeforeRestoringProviderTraffic() throws Exception {
        String name = "resume-changed-settings";
        program(
                name,
                List.of(
                        step(
                                "/v2/users",
                                "count=2&page=1",
                                200,
                                "hold",
                                "",
                                page(1, 2, 1, 1, entity(1, "")))));
        Settings options = new Settings(name, "users");
        options.timeout = 180000;
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(ONE, job, options.variables()));
        try {
            awaitRequests(name, 1);
            savepoint(job, running);
            engine.touch(name + ".release");
            options.pageSize = 3;
            failure(
                    engine.restoreJob(ONE, job, options.variables()),
                    "resource or read settings changed");
            assertEquals(List.of("/v2/users?count=2&page=1"), requests(name));
            assertRows(name, "users", Map.of());
        } finally {
            engine.touch(name + ".release");
            stopIfRunning(job, running);
        }
    }

    @Test
    void restoresCompletedSourceWithoutQueryingAgainWhileAnotherPipelineRuns() throws Exception {
        String name = "resume-terminal";
        String usage = "{\"api_requests\":{\"remaining\":8}}";
        program(name, List.of(ok("/v2/usage", "", usage)));
        Settings options = new Settings(name, "usage");
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(TERMINAL, job, options.variables()));
        try {
            awaitRequests(name, 1);
            savepoint(job, running);
            Future<Container.ExecResult> restored =
                    tasks.submit(() -> engine.restoreJob(TERMINAL, job, options.variables()));
            try {
                long baseline = engine.getCompletedCheckpointCount(job);
                Awaitility.await()
                        .atMost(35, TimeUnit.SECONDS)
                        .until(() -> engine.getCompletedCheckpointCount(job) > baseline + 1);
                assertEquals(List.of("/v2/usage"), requests(name));
                assertRows(name, "usage", Map.of("usage", expected("[]", usage)));
            } finally {
                stopIfRunning(job, restored);
            }
        } finally {
            stopIfRunning(job, running);
        }
    }

    private void check(
            String name,
            String resource,
            String path,
            String query,
            String body,
            Map<String, List<String>> expected)
            throws Exception {
        program(name, List.of(ok(path, query, body)));
        success(run(ONE, new Settings(name, resource)));
        assertRows(name, resource, expected);
        assertEquals(List.of(path + (query.isEmpty() ? "" : "?" + query)), requests(name));
    }

    private void assertRows(String name, String resource, Map<String, List<String>> expected)
            throws Exception {
        Map<String, JsonNode> actual = new HashMap<>();
        for (String line : engine.rows(name)) {
            JsonNode row = JSON.readTree(line);
            assertEquals(
                    Set.of("resource", "record_id", "dimension_keys", "payload"), fieldNames(row));
            assertEquals(resource, row.path("resource").asText());
            assertNull(
                    actual.put(row.path("record_id").asText(), row), "duplicate committed record");
        }
        assertEquals(expected.keySet(), actual.keySet());
        for (Map.Entry<String, List<String>> entry : expected.entrySet()) {
            JsonNode row = actual.get(entry.getKey());
            assertEquals(
                    JSON.readTree(entry.getValue().get(0)),
                    JSON.readTree(row.path("dimension_keys").asText()));
            String payload = row.path("payload").asText();
            assertEquals(JSON.readTree(entry.getValue().get(1)), JSON.readTree(payload));
            if (entry.getValue().get(1).contains("9223372036854775808")) {
                assertTrue(payload.contains("9223372036854775808"));
                assertTrue(payload.contains("123456789012345678.123456789"));
            }
        }
    }

    private static Set<String> fieldNames(JsonNode row) {
        Set<String> names = new HashSet<>();
        row.fieldNames().forEachRemaining(names::add);
        return names;
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

    private static List<String> expected(String keys, String payload) {
        return List.of(keys, payload);
    }

    private static String entity(int id, String extra) {
        return "{\"id\":" + id + extra + "}";
    }

    private static String page(int page, int count, int total, int pages, String... rows) {
        return "{\"metadata\":{\"resultset\":{\"page\":"
                + page
                + ",\"count\":"
                + count
                + ",\"total_count\":"
                + total
                + ",\"total_pages\":"
                + pages
                + "}},\"results\":["
                + String.join(",", rows)
                + "]}";
    }

    private static String privateError() {
        return "{\"error\":\"private-provider-message " + AppfiguresPeer.TOKEN + "\"}";
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
                Base64.getEncoder().encodeToString(body.getBytes(StandardCharsets.UTF_8)));
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
        assertFalse(text(result).contains(AppfiguresPeer.TOKEN));
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
                                "CN=" + AppfiguresPeer.HOST,
                                "-ext",
                                "SAN=dns:" + AppfiguresPeer.HOST,
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                file.toString(),
                                "-storepass",
                                AppfiguresPeer.PASSWORD,
                                "-keypass",
                                AppfiguresPeer.PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, process.waitFor(), output);
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(file)) {
            store.load(input, AppfiguresPeer.PASSWORD.toCharArray());
        }
        return store;
    }

    /**
     * Supplies job variables while keeping optional filters out of unfiltered configurations.
     *
     * <p>Each scenario owns a separate output directory and request journal.
     */
    private static final class Settings {
        private final String name;
        private final String resource;
        private String endpoint = "https://" + AppfiguresPeer.HOST + ":" + AppfiguresPeer.PORT;
        private String group = "";
        private int bytes = 8388608;
        private int maxRows = 100000;
        private int attempts = 4;
        private int timeout = 30000;
        private int pageSize = 2;

        private Settings(String name, String resource) {
            this.name = name;
            this.resource = resource;
        }

        private String[] variables() {
            List<String> values =
                    new ArrayList<>(
                            List.of(
                                    "app.output=" + name,
                                    "app.resource=" + resource,
                                    "app.token=" + AppfiguresPeer.TOKEN,
                                    "app.endpoint=" + endpoint,
                                    "app.page_size=" + pageSize,
                                    "app.max_pages=1000",
                                    "app.max_rows=" + maxRows,
                                    "app.max_bytes=" + bytes,
                                    "app.attempts=" + attempts,
                                    "app.timeout=" + timeout));
            if (!group.isEmpty()) {
                values.add("app.group_by=" + group);
            }
            return values.toArray(new String[0]);
        }
    }

    /**
     * Stages the independently compiled peer in the ordinary Java 11 engine container.
     *
     * <p>The connector receives an explicit PEM anchor; the JVM's global trust store is unchanged.
     */
    private static final class Engine extends SeaTunnelContainer {
        private static final String ROOT = "/tmp/appfigures-peer";
        private final Path fixtures;

        private Engine(Path fixtures) {
            this.fixtures = fixtures;
        }

        @Override
        protected void executeExtraCommands(GenericContainer<?> container)
                throws IOException, InterruptedException {
            super.executeExtraCommands(container);
            container.withExtraHost(AppfiguresPeer.HOST, "127.0.0.1");
            for (String name : List.of("trusted.p12", "untrusted.p12", "trusted.pem")) {
                container.withCopyFileToContainer(
                        MountableFile.forHostPath(fixtures.resolve(name)), ROOT + "/" + name);
            }
            try {
                String file = "AppfiguresPeer.class";
                Path source = Path.of(AppfiguresPeer.class.getResource(file).toURI());
                container.withCopyFileToContainer(
                        MountableFile.forHostPath(source),
                        ROOT
                                + "/classes/"
                                + AppfiguresPeer.class.getPackage().getName().replace('.', '/')
                                + "/"
                                + file);
            } catch (URISyntaxException e) {
                throw new IOException("Cannot stage the Java HTTPS fixture", e);
            }
        }

        private Container.ExecResult runPeer() throws Exception {
            return server.execInContainer(
                    "java", "-cp", ROOT + "/classes", AppfiguresPeer.class.getName(), ROOT);
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
            String path = "/tmp/seatunnel_mnt/appfigures-" + name;
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
