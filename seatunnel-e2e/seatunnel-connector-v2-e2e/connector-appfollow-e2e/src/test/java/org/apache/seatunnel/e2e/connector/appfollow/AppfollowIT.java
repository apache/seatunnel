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

package org.apache.seatunnel.e2e.connector.appfollow;

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
 * Runs AppFollow's whole resource graph through Zeta and a transactional LocalFile sink. An
 * independent Java HTTPS provider verifies exact routes, credentials and request counts.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AppfollowIT extends TestSuiteBase {
    private static final String API_URL =
            "https://" + AppfollowPeer.HOST + ":" + AppfollowPeer.TRUSTED_PORT;
    private static final String ALL = "/appfollow-all.conf";
    private static final String ONE = "/appfollow-one.conf";
    private static final String USERS = "/api/v2/account/users";
    private static final String COLLECTIONS = "/api/v2/account/apps";
    private static final String APP_LISTS = "/api/v2/account/apps/app?apps_id=";
    private static final String STATS = "/api/v2/reviews/stats?ext_id=";
    private static final String RATINGS = "/api/v2/meta/ratings?ext_id=";
    private static final String RAW_SHARED = "com.demo+beta&region=测试";
    private static final String ENCODED_SHARED = "com.demo%2Bbeta%26region%3D%E6%B5%8B%E8%AF%95";
    private static final String OTHER = "com.demo.other";
    private static final String ZERO_DATA =
            "{\"nested\":[null,false,{\"text\":\"圈🌏\"}],"
                    + "\"large\":9223372036854775808,\"amount\":123456789012345678.123456789}";
    private static final String USER =
            "{\"id\":9007199254740993,\"name\":\"读取者\",\"metadata\":" + ZERO_DATA + "}";
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
    void start() throws Exception {
        KeyStore trusted = makeIdentity("trusted");
        makeIdentity("untrusted");
        KeyStore trust = KeyStore.getInstance("JKS");
        char[] password = AppfollowPeer.PASSWORD.toCharArray();
        trust.load(null, password);
        trust.setCertificateEntry("fixture", trusted.getCertificate("api"));
        try (OutputStream output = Files.newOutputStream(directory.resolve("trust.jks"))) {
            trust.store(output, password);
        }
        engine = new Engine(directory);
        engine.startUp();
        peer = tasks.submit(engine::runPeer);
        Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> engine.exists("ready"));
    }

    @AfterEach
    void validatesPeerRequests() throws Exception {
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
    void emitsAllFiveResourcesAndPreservesAppMembershipInEveryCollection() throws Exception {
        String name = "all";
        String first = collection("7", "첫번째");
        String second = collection("8", "second");
        String empty = collection("9", "without-apps");
        String a = app(10, RAW_SHARED);
        String b = app(11, OTHER);
        String c = app(12, RAW_SHARED);
        String sharedStats = stats(RAW_SHARED, 23);
        String otherStats = stats(OTHER, 47);
        String sharedRatings = ratings(RAW_SHARED, "4.1500");
        String otherRatings = ratings(OTHER, "3.2918");
        program(
                name,
                Arrays.asList(
                        ok(USERS, array(USER)),
                        ok(COLLECTIONS, collectionResponse(first, second, empty)),
                        ok(APP_LISTS + "7", appResponse(a, b)),
                        ok(APP_LISTS + "8", "{\"apps_app\":" + c + "}"),
                        ok(APP_LISTS + "9", appResponse()),
                        ok(STATS + ENCODED_SHARED, sharedStats),
                        ok(STATS + OTHER, otherStats),
                        ok(STATS + ENCODED_SHARED, sharedStats),
                        ok(RATINGS + ENCODED_SHARED, sharedRatings),
                        ok(RATINGS + OTHER, otherRatings),
                        ok(RATINGS + ENCODED_SHARED, sharedRatings)));
        success(run(ALL, new Settings(name)));

        Map<String, JsonNode> rows = rows(name);
        assertEquals(13, rows.size());
        assertData(rows, "users", null, null, USER);
        assertData(rows, "app_collections", "7", null, first);
        assertData(rows, "app_collections", "8", null, second);
        assertData(rows, "app_collections", "9", null, empty);
        assertData(rows, "app_lists", "7", RAW_SHARED, enriched(a, "7", RAW_SHARED));
        assertData(rows, "app_lists", "7", OTHER, enriched(b, "7", OTHER));
        assertData(rows, "app_lists", "8", RAW_SHARED, enriched(c, "8", RAW_SHARED));
        assertData(rows, "stat_reviews", "7", RAW_SHARED, sharedStats);
        assertData(rows, "stat_reviews", "7", OTHER, otherStats);
        assertData(rows, "stat_reviews", "8", RAW_SHARED, sharedStats);
        assertData(rows, "ratings", "7", RAW_SHARED, sharedRatings);
        assertData(rows, "ratings", "7", OTHER, otherRatings);
        assertData(rows, "ratings", "8", RAW_SHARED, sharedRatings);
        assertTrue(
                rows.get(key("ratings", "7", RAW_SHARED)).path("data").asText().contains("4.1500"));
        assertTrue(
                rows.get(key("users", null, null))
                        .path("data")
                        .asText()
                        .contains("123456789012345678.123456789"));
        assertEquals(
                Arrays.asList(
                        USERS,
                        COLLECTIONS,
                        APP_LISTS + "7",
                        APP_LISTS + "8",
                        APP_LISTS + "9",
                        STATS + ENCODED_SHARED,
                        STATS + OTHER,
                        STATS + ENCODED_SHARED,
                        RATINGS + ENCODED_SHARED,
                        RATINGS + OTHER,
                        RATINGS + ENCODED_SHARED),
                requests(name));
    }

    @Test
    void discoversRequiredParentsWithoutEmittingThemAndHandlesEmptyCollections() throws Exception {
        String name = "ratings-only";
        String id = "9223372036854775812";
        String data = "{\"ratings\":{\"ext_id\":\"" + OTHER + "\",\"list\":null},\"cost\":null}";
        program(
                name,
                Arrays.asList(
                        ok(COLLECTIONS, "{\"apps\":" + collection(id, "one") + "}"),
                        ok(APP_LISTS + id, appResponse(app(11, OTHER))),
                        ok(RATINGS + OTHER, data)));
        Settings config = new Settings(name);
        config.resource = "ratings";
        success(run(ONE, config));
        Map<String, JsonNode> rows = rows(name);
        assertEquals(1, rows.size());
        assertData(rows, "ratings", id, OTHER, data);
        assertEquals(Arrays.asList(COLLECTIONS, APP_LISTS + id, RATINGS + OTHER), requests(name));

        name = "no-collections";
        program(name, Collections.singletonList(ok(COLLECTIONS, collectionResponse())));
        config = new Settings(name);
        config.resource = "ratings";
        success(run(ONE, config));
        assertEquals(Collections.singletonList(COLLECTIONS), requests(name));
        assertTrue(rows(name).isEmpty());
    }

    @Test
    void preservesNullAppContextWhenMetricsAreUnselectedAndRejectsMissingRequiredExternalId()
            throws Exception {
        String raw = "{\"app_id\":17,\"app\":{\"ext_id\":null},\"created\":null}";
        String name = "apps-only";
        program(
                name,
                Arrays.asList(
                        ok(COLLECTIONS, collectionResponse(collection("7", "primary"))),
                        ok(APP_LISTS + "7", appResponse(raw))));
        Settings appList = new Settings(name);
        appList.resource = "app_lists";
        success(run(ONE, appList));
        assertData(rows(name), "app_lists", "7", null, enriched(raw, "7", null));
        assertEquals(Arrays.asList(COLLECTIONS, APP_LISTS + "7"), requests(name));

        name = "missing-app-identity";
        program(
                name,
                Arrays.asList(
                        ok(COLLECTIONS, collectionResponse(collection("7", "primary"))),
                        ok(APP_LISTS + "7", appResponse(raw))));
        Settings metrics = new Settings(name);
        metrics.resource = "stat_reviews";
        failure(run(ONE, metrics), "application has no external ID for selected metrics");
        assertEquals(Arrays.asList(COLLECTIONS, APP_LISTS + "7"), requests(name));
        assertTrue(rows(name).isEmpty());
    }

    @Test
    void refusesMalformedEnvelopesAndCrossParentIdentityMismatches() throws Exception {
        String name = "missing-apps";
        program(name, Collections.singletonList(ok(COLLECTIONS, "{\"data\":[]}")));
        Settings collections = new Settings(name);
        collections.resource = "app_collections";
        failure(run(ONE, collections), "invalid app_collections response shape");

        name = "mismatched-apps";
        String wrong =
                app(11, OTHER).replace("\"app_id\":11", "\"app_id\":11,\"app_collection_id\":8");
        program(
                name,
                Arrays.asList(
                        ok(COLLECTIONS, collectionResponse(collection("7", "primary"))),
                        ok(APP_LISTS + "7", appResponse(wrong))));
        Settings apps = new Settings(name);
        apps.resource = "app_lists";
        failure(run(ONE, apps), "response collection ID differs from the requested collection");

        name = "mismatched-external";
        program(
                name,
                Arrays.asList(
                        ok(COLLECTIONS, collectionResponse(collection("7", "primary"))),
                        ok(APP_LISTS + "7", appResponse(app(11, OTHER))),
                        ok(STATS + OTHER, stats("different-app", 1))));
        Settings reviews = new Settings(name);
        reviews.resource = "stat_reviews";
        failure(run(ONE, reviews), "response external ID differs from the requested app");
        assertTrue(rows(name).isEmpty());

        name = "bad-ratings-list";
        program(
                name,
                Arrays.asList(
                        ok(COLLECTIONS, collectionResponse(collection("7", "primary"))),
                        ok(APP_LISTS + "7", appResponse(app(11, OTHER))),
                        ok(RATINGS + OTHER, "{\"ratings\":{\"list\":{\"not\":\"array\"}}}")));
        Settings ratings = new Settings(name);
        ratings.resource = "ratings";
        failure(run(ONE, ratings), "expected an array or null for ratings.list");
    }

    @Test
    void declinesUnknownPaginationAndDuplicateJsonWithoutInventingParameters() throws Exception {
        String name = "header-next";
        program(name, Collections.singletonList(step(USERS, 200, "next", "", array(USER))));
        failure(
                run(ONE, new Settings(name)),
                "unexpected continuation for an unpaginated resource");
        assertEquals(Collections.singletonList(USERS), requests(name));
        assertTrue(rows(name).isEmpty());

        name = "body-next";
        program(
                name,
                Collections.singletonList(
                        ok(COLLECTIONS, "{\"apps\":[],\"pagination\":{\"next\":\"2\"}}")));
        Settings list = new Settings(name);
        list.resource = "app_collections";
        failure(run(ONE, list), "unexpected continuation for an unpaginated resource");
        assertEquals(Collections.singletonList(COLLECTIONS), requests(name));

        name = "duplicate";
        program(name, Collections.singletonList(ok(USERS, "[{\"id\":1,\"id\":2}]")));
        failure(run(ONE, new Settings(name)), "invalid, incomplete or unreadable JSON response");
    }

    @Test
    void handlesPermanentCreditPermissionAndDependencyFailuresWithoutLoggingPrivateData()
            throws Exception {
        for (int status : new int[] {401, 402, 403, 422, 424}) {
            String name = "permanent-" + status;
            program(
                    name,
                    Collections.singletonList(
                            step(
                                    USERS,
                                    status,
                                    "fixed",
                                    "",
                                    "{\"error\":\"private-provider-message "
                                            + AppfollowPeer.TOKEN
                                            + "\"}")));
            failure(run(ONE, new Settings(name)), "HTTP " + status);
            assertEquals(Collections.singletonList(USERS), requests(name));
            assertTrue(rows(name).isEmpty());
        }
        String name = "ratings-400";
        program(
                name,
                Arrays.asList(
                        ok(COLLECTIONS, collectionResponse(collection("7", "primary"))),
                        ok(APP_LISTS + "7", appResponse(app(11, OTHER))),
                        step(RATINGS + OTHER, 400, "fixed", "", "{}")));
        Settings ratings = new Settings(name);
        ratings.resource = "ratings";
        failure(run(ONE, ratings), "HTTP 400 (invalid AppFollow request)");
        assertEquals(Arrays.asList(COLLECTIONS, APP_LISTS + "7", RATINGS + OTHER), requests(name));
        assertTrue(rows(name).isEmpty());
    }

    @Test
    void boundsTemporaryRetriesAndSharesTheRequestBudgetWithParentDiscovery() throws Exception {
        String name = "successful-retry";
        program(
                name,
                Arrays.asList(
                        step(USERS, 503, "fixed", "", ""),
                        step(USERS, 429, "fixed", "1", ""),
                        ok(USERS, USER)));
        success(run(ONE, new Settings(name)));
        assertEquals(Collections.nCopies(3, USERS), requests(name));
        assertData(rows(name), "users", null, null, USER);

        name = "retry-limit";
        program(name, Collections.nCopies(2, step(USERS, 429, "fixed", "1", "")));
        Settings finite = new Settings(name);
        finite.attempts = 2;
        failure(run(ONE, finite), "retry limit reached after HTTP 429");
        assertEquals(Collections.nCopies(2, USERS), requests(name));

        name = "retry-delay";
        program(name, Collections.singletonList(step(USERS, 429, "fixed", "3600", "")));
        failure(run(ONE, new Settings(name)), "retry delay exceeds request deadline");
        assertEquals(Collections.singletonList(USERS), requests(name));

        name = "reader-budget";
        program(
                name,
                Arrays.asList(
                        step(COLLECTIONS, 503, "fixed", "", ""),
                        ok(COLLECTIONS, collectionResponse(collection("7", "primary")))));
        Settings capped = new Settings(name);
        capped.resource = "ratings";
        capped.maxRequests = 2;
        failure(run(ONE, capped), "request budget exhausted (max_requests)");
        assertEquals(Collections.nCopies(2, COLLECTIONS), requests(name));
    }

    @Test
    void capsDiscoveredAttachmentsAndResponseRecordsBeforeMoreRequests() throws Exception {
        String name = "entity-bound";
        program(
                name,
                Arrays.asList(
                        ok(
                                COLLECTIONS,
                                collectionResponse(collection("7", "one"), collection("8", "two"))),
                        ok(APP_LISTS + "7", appResponse(app(11, OTHER), app(12, RAW_SHARED))),
                        ok(APP_LISTS + "8", appResponse(app(21, OTHER)))));
        Settings cap = new Settings(name);
        cap.resource = "ratings";
        cap.maxEntities = 2;
        failure(run(ONE, cap), "discovery exceeds max_entities");
        assertEquals(Arrays.asList(COLLECTIONS, APP_LISTS + "7", APP_LISTS + "8"), requests(name));
        assertTrue(rows(name).isEmpty());

        name = "record-bound";
        program(name, Collections.singletonList(ok(USERS, array(USER, "{\"id\":2}"))));
        Settings records = new Settings(name);
        records.maxRecords = 1;
        failure(run(ONE, records), "response exceeds max_records");
        assertEquals(Collections.singletonList(USERS), requests(name));
        assertTrue(rows(name).isEmpty());
    }

    @Test
    void enforcesTlsTrustAndOriginAndDoesNotFollowRedirects() throws Exception {
        String name = "redirect";
        program(name, Collections.singletonList(step(USERS, 302, "redirect", "", "")));
        failure(run(ONE, new Settings(name)), "HTTP 302");
        assertEquals(Collections.singletonList(USERS), requests(name));

        name = "untrusted";
        program(name, Collections.singletonList(ok(USERS, array(USER))));
        Settings certificate = new Settings(name);
        certificate.url = "https://" + AppfollowPeer.HOST + ":" + AppfollowPeer.UNTRUSTED_PORT;
        failure(run(ONE, certificate), "HTTPS or TLS exchange failed");
        assertTrue(requests(name).isEmpty());

        name = "origin";
        program(name, Collections.singletonList(ok(USERS, array(USER))));
        Settings origin = new Settings(name);
        origin.url = "http://" + AppfollowPeer.HOST;
        failure(run(ONE, origin), "api_url must be the HTTPS origin of api.appfollow.io");
        assertTrue(requests(name).isEmpty());
    }

    @Test
    void acceptsGzipAndLimitsBothWireAndInflatedBytes() throws Exception {
        String name = "gzip";
        program(name, Collections.singletonList(step(USERS, 200, "gzip", "", array(USER))));
        success(run(ONE, new Settings(name)));
        assertData(rows(name), "users", null, null, USER);

        String oversized = array("{\"id\":1,\"extra\":\"" + "x".repeat(8000) + "\"}");
        for (String mode : Arrays.asList("fixed", "gzip")) {
            name = "oversize-" + mode;
            program(name, Collections.singletonList(step(USERS, 200, mode, "", oversized)));
            Settings bound = new Settings(name);
            bound.maxBytes = 1024;
            failure(run(ONE, bound), "response exceeds max_response_bytes");
            assertTrue(rows(name).isEmpty());
        }
    }

    @Test
    void deadlineAndEngineCancellationDisconnectAnActiveHttpsRequest() throws Exception {
        String timeout = "slow-deadline";
        program(timeout, Collections.singletonList(step(USERS, 200, "slow", "", "")));
        Settings deadline = new Settings(timeout);
        deadline.deadline = 2000;
        failure(run(ONE, deadline), "request deadline exceeded");
        awaitDisconnect(timeout);
        assertEquals(Collections.singletonList(USERS), requests(timeout));

        String cancel = "slow-cancel";
        program(cancel, Collections.singletonList(step(USERS, 200, "slow", "", "")));
        Settings stoppable = new Settings(cancel);
        stoppable.deadline = 90000;
        String job = jobId();
        Future<Container.ExecResult> result =
                tasks.submit(() -> engine.executeJob(ONE, job, stoppable.variables()));
        try {
            awaitRequests(cancel, 1);
            success(engine.cancelJob(job));
            result.get(30, TimeUnit.SECONDS);
            awaitDisconnect(cancel);
            assertEquals(Collections.singletonList(USERS), requests(cancel));
        } finally {
            if (!result.isDone()) {
                engine.cancelJob(job);
                result.cancel(true);
            }
        }
    }

    @Test
    void savesBetweenParentCollectionsAndKeepsCompletedDiscoveryWithoutAnotherChargedRequest()
            throws Exception {
        String name = "restore-discovery";
        String collected = collectionResponse(collection("7", "first"), collection("8", "second"));
        String app7 = appResponse(app(11, RAW_SHARED));
        String app8 = appResponse(app(12, RAW_SHARED));
        String result = ratings(RAW_SHARED, "4.25");
        program(
                name,
                Arrays.asList(
                        ok(COLLECTIONS, collected),
                        ok(APP_LISTS + "7", app7),
                        step(APP_LISTS + "8", 200, "hold", "", app8),
                        ok(APP_LISTS + "8", app8),
                        ok(RATINGS + ENCODED_SHARED, result),
                        ok(RATINGS + ENCODED_SHARED, result)));
        Settings config = new Settings(name);
        config.resource = "ratings";
        saveAndRestore(config, 3);
        assertEquals(
                Arrays.asList(
                        COLLECTIONS,
                        APP_LISTS + "7",
                        APP_LISTS + "8",
                        APP_LISTS + "8",
                        RATINGS + ENCODED_SHARED,
                        RATINGS + ENCODED_SHARED),
                requests(name));
        Map<String, JsonNode> rows = rows(name);
        assertEquals(2, rows.size());
        assertData(rows, "ratings", "7", RAW_SHARED, result);
        assertData(rows, "ratings", "8", RAW_SHARED, result);
    }

    @Test
    void savesBetweenMetricChildrenAndRestoresWithoutRepeatingTheCompletedApp() throws Exception {
        String name = "restore-metrics";
        String first = stats(RAW_SHARED, 14);
        String second = stats(OTHER, 21);
        program(
                name,
                Arrays.asList(
                        ok(COLLECTIONS, collectionResponse(collection("7", "one"))),
                        ok(APP_LISTS + "7", appResponse(app(11, RAW_SHARED), app(12, OTHER))),
                        ok(STATS + ENCODED_SHARED, first),
                        step(STATS + OTHER, 200, "hold", "", second),
                        ok(STATS + OTHER, second)));
        Settings config = new Settings(name);
        config.resource = "stat_reviews";
        saveAndRestore(config, 4);
        assertEquals(
                Arrays.asList(
                        COLLECTIONS,
                        APP_LISTS + "7",
                        STATS + ENCODED_SHARED,
                        STATS + OTHER,
                        STATS + OTHER),
                requests(name));
        Map<String, JsonNode> rows = rows(name);
        assertEquals(2, rows.size());
        assertData(rows, "stat_reviews", "7", RAW_SHARED, first);
        assertData(rows, "stat_reviews", "7", OTHER, second);
    }

    private void saveAndRestore(Settings options, int calls) throws Exception {
        options.deadline = 120000;
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(ONE, job, options.variables()));
        try {
            awaitRequests(options.name, calls);
            long checkpoints = engine.getCompletedCheckpointCount(job);
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> engine.getCompletedCheckpointCount(job) > checkpoints);
            success(engine.savepointJob(job));
            running.get(30, TimeUnit.SECONDS);
            engine.touch(options.name + ".release");
            success(engine.restoreJob(ONE, job, options.variables()));
        } finally {
            if (!running.isDone()) {
                engine.cancelJob(job);
                running.cancel(true);
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

    private static void assertData(
            Map<String, JsonNode> rows, String resource, String collection, String ext, String data)
            throws IOException {
        JsonNode row = rows.get(key(resource, collection, ext));
        assertEquals(JSON.readTree(data), JSON.readTree(row.path("data").textValue()));
    }

    private Container.ExecResult run(String conf, Settings options) throws Exception {
        return engine.executeJob(conf, jobId(), options.variables());
    }

    private void program(String name, List<String> steps) throws Exception {
        validatesPeerRequests();
        Path script = Files.createTempFile(directory, "steps-", ".txt");
        Files.write(script, steps, StandardCharsets.UTF_8);
        engine.install(script, name + ".program");
        Path activeFile = Files.createTempFile(directory, "case-", ".txt");
        Files.writeString(activeFile, name);
        engine.install(activeFile, "active");
        active = name;
    }

    private List<String> requests(String name) throws Exception {
        return engine.lines(name + ".requests");
    }

    private Map<String, JsonNode> rows(String name) throws Exception {
        Map<String, JsonNode> rows = new HashMap<>();
        for (String line : engine.rows(name)) {
            JsonNode row = JSON.readTree(line);
            String key =
                    key(
                            row.path("resource").textValue(),
                            row.path("app_collection_id").asText(null),
                            row.path("ext_id").asText(null));
            assertNull(rows.put(key, row), key);
        }
        return rows;
    }

    private static String key(String resource, String collection, String ext) {
        return resource + "|" + collection + "|" + ext;
    }

    private static String step(
            String path, int status, String mode, String retryAfter, String payload) {
        return String.join(
                "\t",
                path,
                Integer.toString(status),
                mode,
                retryAfter,
                Base64.getEncoder().encodeToString(payload.getBytes(StandardCharsets.UTF_8)));
    }

    private static String ok(String path, String payload) {
        return step(path, 200, "fixed", "", payload);
    }

    private static String collection(String id, String title) {
        return "{\"id\":" + id + ",\"title\":\"" + title + "\",\"created\":null}";
    }

    private static String app(int id, String ext) {
        return "{\"app_id\":"
                + id
                + ",\"app\":{\"ext_id\":\""
                + ext
                + "\",\"metadata\":"
                + ZERO_DATA
                + "},\"count_reviews\":null}";
    }

    private static String enriched(String app, String collection, String ext) {
        String external = ext == null ? "null" : "\"" + ext + "\"";
        return app.substring(0, app.length() - 1)
                + ",\"app_collection_id\":"
                + collection
                + ",\"ext_id\":"
                + external
                + "}";
    }

    private static String stats(String ext, int count) {
        return "{\"ext_id\":\""
                + ext
                + "\",\"store\":null,\"reviews_stat\":{\"reviews\":"
                + count
                + ",\"replies\":null,\"values\":"
                + ZERO_DATA
                + "}}";
    }

    private static String ratings(String ext, String rating) {
        return "{\"ratings\":{\"ext_id\":\""
                + ext
                + "\",\"list\":[{\"date\":\"2024-02-01\",\"rating\":"
                + rating
                + ",\"stars_total\":9223372036854775808,\"country\":null}],\"store\":\"gp\"}}";
    }

    private static String collectionResponse(String... collections) {
        return "{\"apps\":" + array(collections) + "}";
    }

    private static String appResponse(String... apps) {
        return "{\"apps_app\":" + array(apps) + "}";
    }

    private static String array(String... entries) {
        return "[" + String.join(",", entries) + "]";
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
        assertFalse(output(result).contains(AppfollowPeer.TOKEN));
        assertFalse(output(result).contains("private-provider-message"));
    }

    private static KeyStore makeIdentity(String name) throws Exception {
        String keytool = Paths.get(System.getProperty("java.home"), "bin", "keytool").toString();
        Path target = directory.resolve(name + ".p12");
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
                                "CN=" + AppfollowPeer.HOST,
                                "-ext",
                                "SAN=dns:" + AppfollowPeer.HOST,
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                target.toString(),
                                "-storepass",
                                AppfollowPeer.PASSWORD,
                                "-keypass",
                                AppfollowPeer.PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, process.waitFor(), output);
        KeyStore keys = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(target)) {
            keys.load(input, AppfollowPeer.PASSWORD.toCharArray());
        }
        return keys;
    }

    /**
     * Configures each fixture scenario through normal job-launch arguments. The product module
     * receives the same options exposed to users.
     */
    private static final class Settings {
        private final String name;
        private String resource = "users";
        private String url = API_URL;
        private int maxRecords = 10000;
        private int maxEntities = 10000;
        private int maxRequests = 1000;
        private int maxBytes = 16777216;
        private int deadline = 30000;
        private int attempts = 4;

        private Settings(String name) {
            this.name = name;
        }

        private String[] variables() {
            return new String[] {
                "follow.token=" + AppfollowPeer.TOKEN, "follow.resource=" + resource,
                        "follow.url=" + url,
                "follow.records=" + maxRecords, "follow.entities=" + maxEntities,
                        "follow.requests=" + maxRequests,
                "follow.bytes=" + maxBytes, "follow.deadline=" + deadline,
                        "follow.attempts=" + attempts,
                "follow.output=" + name
            };
        }
    }

    /**
     * Runs the stock Java 11 engine with the AppFollow HTTPS hostname mapped to the test peer. The
     * JVM trust store includes only the trusted fixture identity.
     */
    private static final class Engine extends SeaTunnelContainer {
        private static final String ROOT = "/tmp/appfollow-https-fixture";
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
                    + AppfollowPeer.PASSWORD;
        }

        @Override
        protected void executeExtraCommands(GenericContainer<?> container) throws IOException {
            container.withExtraHost(AppfollowPeer.HOST, "127.0.0.1");
            for (String file : Arrays.asList("trusted.p12", "untrusted.p12", "trust.jks")) {
                container.withCopyFileToContainer(
                        MountableFile.forHostPath(fixtures.resolve(file)), ROOT + "/" + file);
            }
            String javaPackage = AppfollowPeer.class.getPackage().getName().replace('.', '/');
            for (String file : Arrays.asList("AppfollowPeer.class", "AppfollowPeer$Reply.class")) {
                try {
                    container.withCopyFileToContainer(
                            MountableFile.forHostPath(
                                    Paths.get(AppfollowPeer.class.getResource(file).toURI())),
                            ROOT + "/classes/" + javaPackage + "/" + file);
                } catch (URISyntaxException e) {
                    throw new IOException("Cannot stage the HTTPS provider fixture", e);
                }
            }
        }

        private Container.ExecResult runPeer() throws Exception {
            return server.execInContainer(
                    "java", "-cp", ROOT + "/classes", AppfollowPeer.class.getName(), ROOT);
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
            String dir = "/tmp/seatunnel_mnt/appfollow-" + name;
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
