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

package org.apache.seatunnel.e2e.connector.apify;

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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs the Apify source and transactional LocalFile sink in a real Zeta container. A separate Java
 * TLS service verifies API requests and serves controlled pages.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ApifyDatasetIT extends TestSuiteBase {
    private static final String API_URL =
            "https://" + ApifyProvider.HOST + ":" + ApifyProvider.TRUSTED_PORT;
    private static final String ALL = "/apify-all.conf";
    private static final String ONE = "/apify-one.conf";
    private static final String RESUME = "/apify-resume.conf";
    private static final String COLLECTION = "/v2/datasets";
    private static final String DATASET_ID = "qa-account~sample+items";
    private static final String DATASET = COLLECTION + "/qa-account~sample%2Bitems";
    private static final String ITEMS = DATASET + "/items";
    private static final String METADATA =
            "{\"id\":\"dataset-real-1\",\"name\":\"sample\",\"fields\":[\"url\",null],"
                    + "\"cleanItemCount\":9007199254740993,\"schema\":{\"views\":{\"overview\":\"测试\"}},"
                    + "\"stats\":{\"readCount\":17,\"storageBytes\":9223372036854775808},\"title\":null}";
    private static final String METADATA_WITH_ACCESS =
            METADATA.substring(0, METADATA.length() - 1)
                    + ",\"urlSigningSecretKey\":\"fixture-owner-signing-key\","
                    + "\"itemsPublicUrl\":\"https://api.apify.com/v2/datasets/hidden?signature=fixture-signed-url\"}";
    private static final String CRAWLER =
            "{\"url\":\"https://example.invalid/页面\",\"markdown\":\"# 水\",\"text\":null,"
                    + "\"#hidden\":{\"urlSigningSecretKey\":\"ordinary-item-field\",\"itemsPublicUrl\":null},"
                    + "\"crawl\":{\"depth\":2,\"httpStatusCode\":200,\"loadedTime\":\"2026-11-01T00:00:00Z\"},"
                    + "\"metadata\":{\"title\":\"网页\",\"canonicalUrl\":null},\"score\":123456789012345678.123456789,"
                    + "\"custom\":[9223372036854775808,true,null]}";
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
        char[] password = ApifyProvider.PASSWORD.toCharArray();
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
    void readsFourResourcesWithIndependentExactPayloadAndEndpointAssertions() throws Exception {
        String name = "all-resources";
        String a = "{\"id\":\"a\",\"title\":null}";
        String b = "{\"id\":\"b\",\"name\":\"Пример\",\"itemCount\":9007199254740993}";
        String c = "{\"id\":\"c\",\"stats\":{\"inflatedBytes\":9223372036854775808}}";
        program(
                name,
                Arrays.asList(
                        step(COLLECTION, 0, 2, 200, "fixed", 0, 2, 3, "", listing(0, 2, 3, a, b)),
                        step(COLLECTION, 2, 1, 200, "fixed", 2, 1, 4, "", listing(2, 1, 4, c)),
                        dataset(METADATA_WITH_ACCESS),
                        items(0, 2, 2, 2, array(CRAWLER, "{}")),
                        items(0, 2, 2, 2, array(CRAWLER, "{}"))));
        success(run(ALL, new Settings(name)));

        List<JsonNode> rows = rows(name);
        assertEquals(8, rows.size());
        Map<String, List<JsonNode>> payloads = byResource(rows);
        assertPayloads(
                payloads.get("dataset_collection"),
                JSON.readTree(a),
                JSON.readTree(b),
                JSON.readTree(c));
        assertEquals(Collections.singletonList(JSON.readTree(METADATA)), payloads.get("dataset"));
        assertEquals(
                Arrays.asList(JSON.readTree(CRAWLER), JSON.readTree("{}")),
                payloads.get("item_collection_website_content_crawler"));
        assertEquals(
                Arrays.asList(JSON.readTree(wrap(CRAWLER)), JSON.readTree(wrap("{}"))),
                payloads.get("item_collection"));
        for (JsonNode row : rows) {
            String apiResource = row.path("resource").asText();
            if (apiResource.startsWith("item_collection")) {
                assertEquals(DATASET_ID, row.path("dataset_id").asText());
            } else {
                assertEquals(
                        JSON.readTree(row.path("payload").asText()).path("id").asText(),
                        row.path("dataset_id").asText());
            }
            String payload = row.path("payload").asText();
            if (payload.contains("canonicalUrl")) {
                assertTrue(payload.contains("123456789012345678.123456789"));
                assertTrue(payload.contains("9223372036854775808"));
                assertTrue(payload.contains("ordinary-item-field"));
            }
            assertFalse(payload.contains("fixture-owner-signing-key"));
            assertFalse(payload.contains("fixture-signed-url"));
        }
        assertEquals(
                Arrays.asList(
                        req(COLLECTION, 0, 2),
                        req(COLLECTION, 2, 1),
                        DATASET + "||",
                        req(ITEMS, 0, 2),
                        req(ITEMS, 0, 2)),
                requests(name));
    }

    @Test
    void advancesShortAndEmptyPagesByScannedRowsWithinInitialTotal() throws Exception {
        String name = "sparse-items";
        String first = "{\"order\":0,\"#visible-by-default\":true}";
        String second = "{\"order\":4}";
        String third = "{\"order\":5}";
        String fourth = "{\"order\":6}";
        program(
                name,
                Arrays.asList(
                        items(0, 2, 2, 7, array(first)),
                        items(2, 2, 2, 7, "[]"),
                        items(4, 2, 1, 7, array(second)),
                        items(5, 2, 2, 8, array(third, fourth))));
        success(run(ONE, new Settings(name)));
        assertEquals(
                Arrays.asList(
                        req(ITEMS, 0, 2), req(ITEMS, 2, 2), req(ITEMS, 4, 2), req(ITEMS, 5, 2)),
                requests(name));
        assertPayloads(
                byResource(rows(name)).get("item_collection"),
                JSON.readTree(wrap(first)),
                JSON.readTree(wrap(second)),
                JSON.readTree(wrap(third)),
                JSON.readTree(wrap(fourth)));
    }

    @Test
    void completesEmptyListAndEmptyItemsAndAllowsCollectionWithoutDatasetId() throws Exception {
        String name = "empty-items";
        program(name, Collections.singletonList(items(0, 2, 0, 0, "[]")));
        success(run(ONE, new Settings(name)));
        assertEquals(Collections.singletonList(req(ITEMS, 0, 2)), requests(name));
        assertTrue(rows(name).isEmpty());

        name = "empty-list";
        program(
                name,
                Collections.singletonList(
                        step(COLLECTION, 0, 2, 200, "fixed", 0, 0, 0, "", listing(0, 2, 0))));
        Settings collection = new Settings(name);
        collection.resource = "dataset_collection";
        collection.datasetId = "";
        success(run(ONE, collection));
        assertEquals(Collections.singletonList(req(COLLECTION, 0, 2)), requests(name));
        assertTrue(rows(name).isEmpty());
    }

    @Test
    void rejectsOffsetsAndPaginationMetadataThatDisagreeWithTheResponse() throws Exception {
        String name = "wrong-offset";
        program(
                name,
                Collections.singletonList(
                        step(ITEMS, 0, 2, 200, "fixed", 1, 1, 3, "", array("{\"n\":1}"))));
        failure(run(ONE, new Settings(name)), "unexpected pagination offset or limit");
        assertEquals(Collections.singletonList(req(ITEMS, 0, 2)), requests(name));
        assertTrue(rows(name).isEmpty());

        name = "inconsistent-list";
        program(
                name,
                Collections.singletonList(
                        step(
                                COLLECTION,
                                0,
                                2,
                                200,
                                "fixed",
                                0,
                                1,
                                3,
                                "",
                                listing(0, 2, 2, "{\"id\":\"a\"}"))));
        Settings collection = new Settings(name);
        collection.resource = "dataset_collection";
        failure(
                run(ONE, collection),
                "pagination headers disagree with dataset collection metadata");

        name = "too-many-items";
        program(name, Collections.singletonList(items(0, 2, 1, 4, array("{}", "{}"))));
        failure(
                run(ONE, new Settings(name)),
                "pagination count disagrees with the response records");

        name = "missing-headers";
        program(name, Collections.singletonList(status(ITEMS, 0, 2, 200, "", "[]")));
        failure(run(ONE, new Settings(name)), "invalid pagination total");
    }

    @Test
    void rejectsNonProgressAndDatasetShrinkAndBoundsThePageCount() throws Exception {
        String name = "no-progress";
        program(name, Collections.singletonList(items(0, 2, 0, 4, "[]")));
        failure(run(ONE, new Settings(name)), "pagination made no progress");
        assertEquals(Collections.singletonList(req(ITEMS, 0, 2)), requests(name));

        name = "dataset-shrank";
        program(name, Arrays.asList(items(0, 2, 2, 3, array("{}", "{}")), items(2, 1, 0, 2, "[]")));
        failure(run(ONE, new Settings(name)), "resource became shorter");
        assertEquals(Arrays.asList(req(ITEMS, 0, 2), req(ITEMS, 2, 1)), requests(name));

        name = "page-budget";
        program(name, Collections.singletonList(items(0, 2, 2, 4, array("{}", "{}"))));
        Settings budget = new Settings(name);
        budget.maxPages = 1;
        failure(run(ONE, budget), "max_pages exceeded");
        assertEquals(Collections.singletonList(req(ITEMS, 0, 2)), requests(name));
    }

    @Test
    void rejectsWrongEnvelopesDuplicateKeysAndNonObjectRecords() throws Exception {
        String name = "wrong-envelope";
        program(name, Collections.singletonList(items(0, 2, 1, 1, "{\"items\":[{}]}")));
        failure(
                run(ONE, new Settings(name)),
                "pagination count disagrees with the response records");
        assertTrue(rows(name).isEmpty());

        name = "duplicate-json";
        program(name, Collections.singletonList(items(0, 2, 1, 1, "[{\"dup\":1,\"dup\":2}]")));
        failure(run(ONE, new Settings(name)), "invalid, incomplete or unreadable JSON response");

        name = "non-object";
        program(name, Collections.singletonList(items(0, 2, 1, 1, "[null]")));
        failure(run(ONE, new Settings(name)), "expected an object for each dataset record");
    }

    @Test
    void failsAuthenticationPermissionAndExpiryWithoutExposingTheProviderBody() throws Exception {
        for (int status : new int[] {401, 403, 404}) {
            String name = "permanent-" + status;
            program(
                    name,
                    Collections.singletonList(
                            status(
                                    ITEMS,
                                    0,
                                    2,
                                    status,
                                    "",
                                    "{\"error\":\"private-provider-body "
                                            + ApifyProvider.TOKEN
                                            + "\"}")));
            failure(run(ONE, new Settings(name)), "HTTP " + status);
            assertEquals(Collections.singletonList(req(ITEMS, 0, 2)), requests(name));
            assertTrue(rows(name).isEmpty());
        }
    }

    @Test
    void boundsRateLimitRetriesAndRetriesTheExactSamePage() throws Exception {
        String name = "retries";
        program(
                name,
                Arrays.asList(
                        status(ITEMS, 0, 2, 503, "", ""),
                        status(ITEMS, 0, 2, 429, "1", ""),
                        items(0, 2, 1, 1, array("{\"kind\":\"retried\"}"))));
        success(run(ONE, new Settings(name)));
        assertEquals(Collections.nCopies(3, req(ITEMS, 0, 2)), requests(name));
        assertEquals(
                Collections.singletonList(JSON.readTree(wrap("{\"kind\":\"retried\"}"))),
                byResource(rows(name)).get("item_collection"));

        name = "retry-limit";
        program(name, Collections.nCopies(2, status(ITEMS, 0, 2, 429, "1", "")));
        Settings finite = new Settings(name);
        finite.attempts = 2;
        failure(run(ONE, finite), "retry limit reached after HTTP 429");
        assertEquals(Collections.nCopies(2, req(ITEMS, 0, 2)), requests(name));

        name = "retry-deadline";
        program(name, Collections.singletonList(status(ITEMS, 0, 2, 429, "90", "")));
        Settings deadline = new Settings(name);
        deadline.deadline = 5000;
        failure(run(ONE, deadline), "retry delay exceeds request deadline");
        assertEquals(Collections.singletonList(req(ITEMS, 0, 2)), requests(name));
    }

    @Test
    void validatesTlsAndOriginAndNeverFollowsRedirects() throws Exception {
        String name = "redirect";
        program(
                name,
                Collections.singletonList(step(ITEMS, 0, 2, 302, "redirect", 0, -1, 0, "", "")));
        failure(run(ONE, new Settings(name)), "HTTP 302");
        assertEquals(Collections.singletonList(req(ITEMS, 0, 2)), requests(name));

        name = "untrusted";
        program(name, Collections.singletonList(items(0, 2, 0, 0, "[]")));
        Settings untrusted = new Settings(name);
        untrusted.apiUrl = "https://" + ApifyProvider.HOST + ":" + ApifyProvider.UNTRUSTED_PORT;
        failure(run(ONE, untrusted), "HTTPS or TLS exchange failed");
        assertTrue(requests(name).isEmpty());

        name = "unapproved-origin";
        program(name, Collections.singletonList(items(0, 2, 0, 0, "[]")));
        Settings origin = new Settings(name);
        origin.apiUrl = "http://api.apify.com";
        failure(run(ONE, origin), "api_url must be the HTTPS origin");
        assertTrue(requests(name).isEmpty());

        name = "invalid-id";
        program(name, Collections.singletonList(items(0, 2, 0, 0, "[]")));
        Settings invalid = new Settings(name);
        invalid.datasetId = "account~%2Fitem";
        failure(run(ONE, invalid), "dataset_id must be a single dataset ID");
        assertTrue(requests(name).isEmpty());
    }

    @Test
    void acceptsGzipAndCapsEncodedAndDecodedBodies() throws Exception {
        String name = "gzip";
        program(
                name,
                Collections.singletonList(
                        step(ITEMS, 0, 2, 200, "gzip", 0, 1, 1, "", array(CRAWLER))));
        success(run(ONE, new Settings(name)));
        assertEquals(
                Collections.singletonList(JSON.readTree(wrap(CRAWLER))),
                byResource(rows(name)).get("item_collection"));

        String body = array("{\"value\":\"" + "x".repeat(8000) + "\"}");
        for (String encoding : Arrays.asList("fixed", "gzip")) {
            name = "oversize-" + encoding;
            program(
                    name,
                    Collections.singletonList(step(ITEMS, 0, 2, 200, encoding, 0, 1, 1, "", body)));
            Settings bound = new Settings(name);
            bound.maxBytes = 1024;
            failure(run(ONE, bound), "response exceeds max_response_bytes");
            assertEquals(Collections.singletonList(req(ITEMS, 0, 2)), requests(name));
            assertTrue(rows(name).isEmpty());
        }
    }

    @Test
    void closesSlowRequestsOnDeadlineAndEngineCancellation() throws Exception {
        String deadlineName = "slow-deadline";
        program(
                deadlineName,
                Collections.singletonList(step(ITEMS, 0, 2, 200, "trickle", 0, 1, 1, "", "")));
        Settings deadline = new Settings(deadlineName);
        deadline.deadline = 2000;
        failure(run(ONE, deadline), "request deadline exceeded");
        awaitDisconnect(deadlineName);
        assertEquals(Collections.singletonList(req(ITEMS, 0, 2)), requests(deadlineName));

        String cancelName = "cancel";
        program(
                cancelName,
                Collections.singletonList(step(ITEMS, 0, 2, 200, "trickle", 0, 1, 1, "", "")));
        Settings canceled = new Settings(cancelName);
        canceled.deadline = 90000;
        String job = jobId();
        Future<Container.ExecResult> result =
                tasks.submit(() -> engine.executeJob(ONE, job, canceled.variables()));
        try {
            awaitRequests(cancelName, 1);
            success(engine.cancelJob(job));
            result.get(30, TimeUnit.SECONDS);
            awaitDisconnect(cancelName);
            assertEquals(Collections.singletonList(req(ITEMS, 0, 2)), requests(cancelName));
        } finally {
            if (!result.isDone()) {
                engine.cancelJob(job);
                result.cancel(true);
            }
        }
    }

    @Test
    void restoresBetweenPagesAtTheRecordedFenceWithoutRefetchingCompletedRows() throws Exception {
        String name = "restore-pages";
        String a = "{\"n\":0}";
        String b = "{\"n\":1}";
        String c = "{\"n\":2}";
        program(
                name,
                Arrays.asList(
                        items(0, 2, 2, 3, array(a, b)),
                        step(ITEMS, 2, 1, 200, "hold", 2, 1, 3, "", array(c)),
                        items(2, 1, 1, 5, array(c))));
        saveAndRestore(ONE, new Settings(name), 2);
        assertEquals(
                Arrays.asList(req(ITEMS, 0, 2), req(ITEMS, 2, 1), req(ITEMS, 2, 1)),
                requests(name));
        assertPayloads(
                byResource(rows(name)).get("item_collection"),
                JSON.readTree(wrap(a)),
                JSON.readTree(wrap(b)),
                JSON.readTree(wrap(c)));
    }

    @Test
    void restoresBetweenResourcesAndFetchesMetadataExactlyOnce() throws Exception {
        String name = "restore-resources";
        String item = "{\"n\":7,\"message\":\"after savepoint\"}";
        program(
                name,
                Arrays.asList(
                        dataset(METADATA),
                        step(ITEMS, 0, 2, 200, "hold", 0, 1, 1, "", array(item)),
                        items(0, 2, 1, 1, array(item))));
        saveAndRestore(RESUME, new Settings(name), 2);
        assertEquals(
                Arrays.asList(DATASET + "||", req(ITEMS, 0, 2), req(ITEMS, 0, 2)), requests(name));
        Map<String, List<JsonNode>> payloads = byResource(rows(name));
        assertEquals(Collections.singletonList(JSON.readTree(METADATA)), payloads.get("dataset"));
        assertEquals(
                Collections.singletonList(JSON.readTree(wrap(item))),
                payloads.get("item_collection"));
    }

    private void saveAndRestore(String config, Settings options, int calls) throws Exception {
        options.deadline = 120000;
        String job = jobId();
        Future<Container.ExecResult> result =
                tasks.submit(() -> engine.executeJob(config, job, options.variables()));
        try {
            awaitRequests(options.name, calls);
            long checkpoints = engine.getCompletedCheckpointCount(job);
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> engine.getCompletedCheckpointCount(job) > checkpoints);
            success(engine.savepointJob(job));
            result.get(30, TimeUnit.SECONDS);
            engine.touch(options.name + ".release");
            success(engine.restoreJob(config, job, options.variables()));
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

    private static void assertPayloads(List<JsonNode> rows, JsonNode... expected) {
        List<JsonNode> actual = new ArrayList<>(rows);
        for (JsonNode payload : expected) {
            assertTrue(actual.remove(payload), payload::toString);
        }
        assertTrue(actual.isEmpty(), actual::toString);
    }

    private Container.ExecResult run(String conf, Settings settings) throws Exception {
        return engine.executeJob(conf, jobId(), settings.variables());
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
                    .add(JSON.readTree(row.path("payload").asText()));
        }
        return groups;
    }

    private static String dataset(String value) {
        return step(DATASET, -1, -1, 200, "fixed", -1, -1, -1, "", "{\"data\":" + value + "}");
    }

    private static String items(int offset, int limit, int count, int total, String value) {
        return step(ITEMS, offset, limit, 200, "fixed", offset, count, total, "", value);
    }

    private static String status(
            String route, int offset, int limit, int status, String retryAfter, String body) {
        return step(route, offset, limit, status, "fixed", -1, -1, -1, retryAfter, body);
    }

    private static String step(
            String route,
            int offset,
            int limit,
            int status,
            String mode,
            int returnedOffset,
            int count,
            int total,
            String retryAfter,
            String body) {
        return String.join(
                "\t",
                route,
                nonnegative(offset),
                nonnegative(limit),
                Integer.toString(status),
                mode,
                nonnegative(returnedOffset),
                nonnegative(count),
                nonnegative(total),
                retryAfter,
                Base64.getEncoder().encodeToString(body.getBytes(StandardCharsets.UTF_8)));
    }

    private static String nonnegative(int n) {
        return n < 0 ? "" : Integer.toString(n);
    }

    private static String req(String route, int offset, int limit) {
        return route + "|" + offset + "|" + limit;
    }

    private static String listing(int offset, int limit, int total, String... entries) {
        return "{\"data\":{\"desc\":false,\"offset\":"
                + offset
                + ",\"limit\":"
                + limit
                + ",\"total\":"
                + total
                + ",\"count\":"
                + entries.length
                + ",\"items\":"
                + array(entries)
                + "}}";
    }

    private static String array(String... entries) {
        return "[" + String.join(",", entries) + "]";
    }

    private static String wrap(String item) {
        return "{\"data\":" + item + "}";
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
        assertFalse(output(result).contains(ApifyProvider.TOKEN));
        assertFalse(output(result).contains("private-provider-body"));
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
                                "CN=" + ApifyProvider.HOST,
                                "-ext",
                                "SAN=dns:" + ApifyProvider.HOST,
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                target.toString(),
                                "-storepass",
                                ApifyProvider.PASSWORD,
                                "-keypass",
                                ApifyProvider.PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String output = new String(crypto.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, crypto.waitFor(), output);
        KeyStore key = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(target)) {
            key.load(input, ApifyProvider.PASSWORD.toCharArray());
        }
        return key;
    }

    /**
     * Controls test jobs through the same configuration arguments used by the CLI. Each scenario
     * sets only the bounds or resource-selection values it exercises.
     */
    private static final class Settings {
        private final String name;
        private String apiUrl = API_URL;
        private String datasetId = DATASET_ID;
        private String resource = "item_collection";
        private int maxPages = 10000;
        private int maxBytes = 16777216;
        private int deadline = 30000;
        private int attempts = 4;

        private Settings(String name) {
            this.name = name;
        }

        private String[] variables() {
            return new String[] {
                "apify.token=" + ApifyProvider.TOKEN, "apify.url=" + apiUrl,
                        "apify.dataset=" + datasetId,
                "apify.resource=" + resource, "apify.output=" + name, "apify.pages=" + maxPages,
                "apify.bytes=" + maxBytes, "apify.deadline=" + deadline,
                        "apify.attempts=" + attempts
            };
        }
    }

    /**
     * Runs the stock Java 11 engine with Apify's real hostname mapped to the test peer. The JVM
     * trust store includes only the trusted fixture identity.
     */
    private static final class Engine extends SeaTunnelContainer {
        private static final String ROOT = "/tmp/apify-https-fixture";
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
                    + ApifyProvider.PASSWORD;
        }

        @Override
        protected void executeExtraCommands(GenericContainer<?> container) throws IOException {
            container.withExtraHost(ApifyProvider.HOST, "127.0.0.1");
            for (String file : Arrays.asList("trusted.p12", "untrusted.p12", "trust.jks")) {
                container.withCopyFileToContainer(
                        MountableFile.forHostPath(fixtures.resolve(file)), ROOT + "/" + file);
            }
            String javaPackage = ApifyProvider.class.getPackage().getName().replace('.', '/');
            for (String file : Arrays.asList("ApifyProvider.class", "ApifyProvider$Reply.class")) {
                try {
                    container.withCopyFileToContainer(
                            MountableFile.forHostPath(
                                    Paths.get(ApifyProvider.class.getResource(file).toURI())),
                            ROOT + "/classes/" + javaPackage + "/" + file);
                } catch (URISyntaxException e) {
                    throw new IOException("Cannot stage the HTTPS provider fixture", e);
                }
            }
        }

        private Container.ExecResult runPeer() throws Exception {
            return server.execInContainer(
                    "java", "-cp", ROOT + "/classes", ApifyProvider.class.getName(), ROOT);
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
            String dir = "/tmp/seatunnel_mnt/apify-" + name;
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
