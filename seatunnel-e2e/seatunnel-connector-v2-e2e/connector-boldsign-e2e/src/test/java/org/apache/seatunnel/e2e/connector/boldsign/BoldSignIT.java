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

package org.apache.seatunnel.e2e.connector.boldsign;

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
 * Exercises the shipped BoldSign artifact in Java 11 Zeta.
 *
 * <p>A separate HTTPS peer checks exact paths and GET-only requests. Rows are committed to a
 * transactional LocalFile sink and compared by value.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BoldSignIT extends TestSuiteBase {
    private static final String CONF = "/boldsign.conf";
    private static final String ROOT = "/tmp/boldsign-https-fixture";
    private static final String OUTPUT = "/tmp/seatunnel_mnt/boldsign-";
    private static final String DOCUMENTS = "/v1/document/list";
    private static final String BRANDS = "/v1/brand/list";
    private static final String CONTACTS = "/v1/contacts/list";
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
        char[] password = BoldSignPeer.PASSWORD.toCharArray();
        KeyStore identities = KeyStore.getInstance("PKCS12");
        identities.load(null, password);
        KeyStore trust = KeyStore.getInstance("JKS");
        trust.load(null, password);
        Map<String, List<String>> names =
                Map.of(
                        "trusted",
                                List.of(
                                        BoldSignPeer.HOST,
                                        BoldSignPeer.EU,
                                        BoldSignPeer.CA,
                                        BoldSignPeer.AU,
                                        BoldSignPeer.REDIRECT),
                        "untrusted", List.of(BoldSignPeer.HOST),
                        "wrong", List.of(BoldSignPeer.MISMATCH));
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
    void readsEveryResourceUsingTheCurrentWireSchemaAndExactJsonPayload() throws Exception {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put("documents", "/v1/document/list");
        resources.put("brands", "/v1/brand/list");
        resources.put("senderIdentities", "/v1/senderIdentities/list");
        resources.put("teams", "/v1/teams/list");
        resources.put("templates", "/v1/template/list");
        resources.put("users_list", "/v1/users/list");
        resources.put("custom_fields", "/v1/customField/list");
        resources.put("contacts", "/v1/contacts/list");
        for (Map.Entry<String, String> entry : resources.entrySet()) {
            String resource = entry.getKey();
            String path = entry.getValue();
            Settings options =
                    new Settings("scope-" + resource.toLowerCase(java.util.Locale.ROOT), resource);
            String row =
                    "{\"documentId\":\"doc-1\",\"brandId\":\"b1\",\"teamId\":\"t1\","
                            + "\"customFieldId\":\"f1\",\"label\":\"中文\",\"createdDate\":1700000000,"
                            + "\"sender\":{\"email\":\"app@example.test\",\"value\":90071992547409931234567,"
                            + "\"number\":0.123456789012345600,\"message\":null}}";
            String brand = null;
            if ("brands".equals(resource)) {
                program(options, reply(path, list("result", row)));
            } else if ("custom_fields".equals(resource)) {
                brand = "b1";
                program(
                        options,
                        reply(BRANDS, list("result", "{\"brandId\":\"b1\"}")),
                        reply(path + "?brandId=b1", 200, "gzip", list("result", row)));
            } else {
                String collection = "teams".equals(resource) ? "results" : "result";
                program(
                        options,
                        reply(
                                numbered(path, 1, 50),
                                200,
                                "gzip",
                                page(collection, 1, 50, 1, null, row)));
            }
            success(run(options));
            assertRows(options, expected(brand, row));
            assertEquals(brand == null ? 1 : 2, requests(options).size());
        }
    }

    @Test
    void discoversEachCustomFieldBrandWithoutNumberedQueriesOrGlobalDeduplication()
            throws Exception {
        Settings options = new Settings("custom-brand-association", "custom_fields");
        String parentA = "{\"brandId\":\"b1\",\"name\":\"first\"}";
        String parentB = "{\"brandId\":\"日本+ brand/&\",\"name\":\"second\"}";
        String fieldA = "{\"customFieldId\":\"shared\",\"type\":\"Text\",\"value\":null}";
        String fieldB =
                "{\"customFieldId\":\"shared\",\"brandId\":\"日本+ brand/&\",\"value\":false}";
        program(
                options,
                reply(BRANDS, list("result", parentA, parentB)),
                reply("/v1/customField/list?brandId=b1", list("result", fieldA)),
                reply(
                        "/v1/customField/list?brandId=%E6%97%A5%E6%9C%AC%2B+brand%2F%26",
                        list("result", fieldB)));
        success(run(options));
        assertRows(options, expected("b1", fieldA), expected("日本+ brand/&", fieldB));
        assertEquals(3, requests(options).size());
    }

    @Test
    void followsNumberedMetadataAndTheActualCountFreeTeamAndUserContracts() throws Exception {
        for (String resource : List.of("teams", "users_list", "contacts")) {
            Settings options = new Settings("page-shape-" + resource, resource).set("pageSize", 2);
            String path =
                    "teams".equals(resource)
                            ? "/v1/teams/list"
                            : "contacts".equals(resource) ? "/v1/contacts/list" : "/v1/users/list";
            String key = "teams".equals(resource) ? "results" : "result";
            Integer total = "contacts".equals(resource) ? 3 : null;
            String one = "{\"id\":\"1\",\"date\":null}";
            String two = "{\"id\":\"2\",\"nested\":{\"name\":\"same\"}}";
            String three = "{\"id\":\"3\",\"flag\":true}";
            program(
                    options,
                    reply(numbered(path, 1, 2), page(key, 1, 2, total, null, one, two)),
                    reply(numbered(path, 2, 2), page(key, 2, 2, total, null, three)));
            success(run(options));
            assertRows(options, expected(null, one), expected(null, two), expected(null, three));
            assertEquals(2, requests(options).size());
        }
    }

    @Test
    void appliesWholeSecondCreationWindowsLocallyToEveryDatedCollection() throws Exception {
        Map<String, String> resources =
                Map.of(
                        "documents",
                        "/v1/document/list",
                        "teams",
                        "/v1/teams/list",
                        "templates",
                        "/v1/template/list",
                        "users_list",
                        "/v1/users/list");
        long first = 1700000000;
        long last = first + 2;
        for (Map.Entry<String, String> entry : resources.entrySet()) {
            Settings options =
                    new Settings("epochs-" + entry.getKey(), entry.getKey())
                            .set("start", java.time.Instant.ofEpochSecond(first).toString())
                            .set("end", java.time.Instant.ofEpochSecond(last).toString());
            String open = document("first", first, 9007199254740994L, "cursor");
            String same = document("same-second", first, 9007199254740993L, "cursor");
            String close = document("last", last, 9007199254740992L, "cursor");
            String before = document("before", first - 1, 9007199254740995L, "cursor");
            String after = document("after", last + 1, 9007199254740991L, "cursor");
            String key = "teams".equals(entry.getKey()) ? "results" : "result";
            program(
                    options,
                    reply(
                            numbered(entry.getValue(), 1, 50),
                            page(key, 1, 50, 5, 1, before, open, same, close, after)));
            success(run(options));
            assertRows(options, expected(null, open), expected(null, same), expected(null, close));
            assertEquals(1, requests(options).size());
        }
    }

    @Test
    void switchesAfterExactlyTenThousandWithATiedCursorAndRestoresTheOverlap() throws Exception {
        Settings options =
                new Settings("cursor-rollover-savepoint", "documents")
                        .set("pageSize", 100)
                        .set("deadline", 120000)
                        .set("start", "2023-11-14T22:13:20Z")
                        .set("end", "2023-11-14T22:13:20Z");
        long selected = 1700000000L;
        long tailCursor = 9007199254740992L;
        long anchor = 9007199254740994L;
        String lastA = document("same-a", selected, tailCursor, "cursor");
        String lastB = document("same-b", selected, tailCursor, "nextCursor");
        String tiedNew = document("same-c", selected, tailCursor, "cursor");
        String after = document("after-rollover", selected, 9007199254740991L, "cursor");
        List<ObjectNode> script = new ArrayList<>();
        for (int page = 1; page <= 100; page++) {
            String[] rows = new String[100];
            for (int i = 0; i < rows.length; i++) {
                int n = (page - 1) * rows.length + i;
                rows[i] = document("before-" + n, selected - 1, 9007199254750991L - n, "cursor");
            }
            if (page == 100) {
                rows[97] = document("anchor-doc", selected - 1, anchor, "cursor");
                rows[98] = lastA;
                rows[99] = lastB;
            }
            script.add(
                    reply(
                            numbered(DOCUMENTS, page, 100),
                            page("result", page, 100, 10002, 101, rows)));
        }
        String continuation = numbered(DOCUMENTS, 1, 100) + "&NextCursor=" + anchor;
        String wrapped = page("result", 1, 100, 4, 1, lastA, lastB, tiedNew, after);
        script.add(reply(continuation, 200, "hold", wrapped));
        script.add(reply(continuation, wrapped));
        program(options, script.toArray(ObjectNode[]::new));
        resume(options, 101, null);
        assertEquals(102, requests(options).size());
        assertRows(
                options,
                expected(null, lastA),
                expected(null, lastB),
                expected(null, tiedNew),
                expected(null, after));
    }

    @Test
    void choosesOnlyTheConfiguredCorporateRegionAndKeepsHttpAuthenticationLocal() throws Exception {
        Map<String, String> endpoints =
                Map.of(
                        "us",
                        BoldSignPeer.HOST,
                        "eu",
                        BoldSignPeer.EU,
                        "ca",
                        BoldSignPeer.CA,
                        "au",
                        BoldSignPeer.AU);
        for (Map.Entry<String, String> endpoint : endpoints.entrySet()) {
            Settings options =
                    new Settings("region-" + endpoint.getKey(), "brands")
                            .set("region", endpoint.getKey());
            String row = "{\"brandId\":\"tenant-" + endpoint.getKey() + "\",\"name\":null}";
            program(options, reply(BRANDS, list("result", row)).put("host", endpoint.getValue()));
            success(run(options));
            assertRows(options, expected(null, row));
            assertEquals(1, requests(options).size());
        }
    }

    @Test
    void distinguishesEmptyUnpagedBrandListFromInvalidKeyOrForbiddenChild() throws Exception {
        Settings empty = new Settings("no-brands", "custom_fields");
        program(empty, reply(BRANDS, list("result")));
        success(run(empty));
        assertEquals(1, requests(empty).size());
        assertTrue(rows(empty).isEmpty());

        Settings forbidden = new Settings("forbidden-child", "custom_fields");
        program(
                forbidden,
                reply(BRANDS, list("result", "{\"brandId\":\"b1\"}")),
                reply("/v1/customField/list?brandId=b1", 403, "", BoldSignPeer.PRIVATE));
        failure(run(forbidden), "HTTP 403");
        assertEquals(2, requests(forbidden).size());
        assertTrue(rows(forbidden).isEmpty());

        Settings invalid = new Settings("invalid-key", "brands");
        program(invalid, reply(BRANDS, 401, "", BoldSignPeer.PRIVATE));
        failure(run(invalid), "HTTP 401");
        assertEquals(1, requests(invalid).size());
    }

    @Test
    void rejectsInvalidEnvelopesInconsistentPageMetadataAndCrossedBrandPayloads() throws Exception {
        Map<String, String> malformed = new LinkedHashMap<>();
        malformed.put("duplicate-json", "{\"result\":[],\"result\":[]}");
        malformed.put("wrong-envelope", "{\"results\":[]}");
        malformed.put("non-object", "{\"result\":[null]}");
        malformed.put("false-200", "{\"error\":\"" + BoldSignPeer.PRIVATE + "\"}");
        for (Map.Entry<String, String> test : malformed.entrySet()) {
            Settings options = new Settings(test.getKey(), "brands");
            program(options, reply(BRANDS, test.getValue()));
            failure(run(options), "BoldSign");
            assertTrue(rows(options).isEmpty());
        }

        Settings metadata = new Settings("wrong-page", "contacts");
        program(metadata, reply(numbered(CONTACTS, 1, 50), page("result", 2, 50, 1, null, "{}")));
        failure(run(metadata), "pageDetails does not match");
        assertTrue(rows(metadata).isEmpty());

        Settings crossed = new Settings("crossed-brand", "custom_fields");
        program(
                crossed,
                reply(BRANDS, list("result", "{\"brandId\":\"b1\"}")),
                reply("/v1/customField/list?brandId=b1", list("result", "{\"brandId\":\"b2\"}")));
        failure(run(crossed), "does not match its requested brand");
        assertTrue(rows(crossed).isEmpty());
    }

    @Test
    void capsNumberedPagesAndFailsAnUnexpectedEmptyPageWhenMoreIsReported() throws Exception {
        Settings cap = new Settings("pages-cap", "templates").set("maxPages", 1).set("pageSize", 1);
        String path = "/v1/template/list";
        program(cap, reply(numbered(path, 1, 1), page("result", 1, 1, 2, 2, "{}")));
        failure(run(cap), "requires more than max_pages");
        assertTrue(rows(cap).isEmpty());

        Settings inconsistent = new Settings("empty-more", "contacts").set("pageSize", 10);
        program(inconsistent, reply(numbered(CONTACTS, 1, 10), page("result", 1, 10, 20, null)));
        failure(run(inconsistent), "empty numbered page has contradictory");
        assertTrue(rows(inconsistent).isEmpty());
    }

    @Test
    void rejectsConflictingDocumentCursorsAndMissingTimestampInExplicitWindow() throws Exception {
        Settings mismatch = new Settings("cursor-conflict", "documents");
        String record =
                "{\"documentId\":\"d1\",\"createdDate\":1700000000,\"cursor\":20,\"nextCursor\":21}";
        program(mismatch, reply(numbered(DOCUMENTS, 1, 50), page("result", 1, 50, 1, 1, record)));
        failure(run(mismatch), "conflicting document cursor fields");
        assertTrue(rows(mismatch).isEmpty());

        Settings date =
                new Settings("missing-created-date", "templates")
                        .set("start", "2020-01-01T00:00:00Z");
        program(
                date,
                reply(
                        numbered("/v1/template/list", 1, 50),
                        page(
                                "result",
                                1,
                                50,
                                1,
                                1,
                                "{\"documentId\":\"d2\",\"createdDate\":null}")));
        failure(run(date), "createdDate is required");
        assertTrue(rows(date).isEmpty());
    }

    @Test
    void enforcesIndependentWireExpandedAndUnpagedBounds() throws Exception {
        for (String mode : List.of("", "gzip")) {
            Settings options =
                    new Settings(mode.isEmpty() ? "wire-cap" : "expanded-cap", "brands")
                            .set("bytes", 1024);
            String response =
                    list(
                            "result",
                            "{\"brandId\":\"b1\",\"private\":\""
                                    + BoldSignPeer.PRIVATE.repeat(120)
                                    + "\"}");
            program(options, reply(BRANDS, 200, mode, response));
            failure(run(options), "max_response_bytes");
            assertTrue(rows(options).isEmpty());
        }
        Settings max = new Settings("max-unpaged", "brands").set("maxUnpaged", 1);
        program(max, reply(BRANDS, list("result", "{\"brandId\":\"b1\"}", "{\"brandId\":\"b2\"}")));
        failure(run(max), "oversized result collection");
        assertTrue(rows(max).isEmpty());
    }

    @Test
    void honorsRetryAfterBoundsAttemptsAndKeepsResponseNotesOutOfDiagnostics() throws Exception {
        Settings retry = new Settings("retry", "brands");
        String row = "{\"brandId\":\"saved\",\"name\":\"fixture\"}";
        program(
                retry,
                reply(BRANDS, 429, "", BoldSignPeer.PRIVATE).put("retryAfter", "1"),
                reply(BRANDS, list("result", row)));
        success(run(retry));
        assertRows(retry, expected(null, row));
        List<JsonNode> calls = requests(retry);
        assertEquals(2, calls.size());
        assertTrue(
                calls.get(1).path("nanoTime").asLong() - calls.get(0).path("nanoTime").asLong()
                        >= TimeUnit.MILLISECONDS.toNanos(950));

        Settings finite = new Settings("retry-limit", "brands").set("attempts", 2);
        program(
                finite,
                reply(BRANDS, 429, "", BoldSignPeer.PRIVATE),
                reply(BRANDS, 503, "", BoldSignPeer.PRIVATE));
        failure(run(finite), "retry limit reached after HTTP 503");
        assertEquals(2, requests(finite).size());

        Settings budget = new Settings("retry-budget", "brands").set("deadline", 15000);
        program(budget, reply(BRANDS, 429, "", BoldSignPeer.PRIVATE).put("retryAfter", "60"));
        failure(run(budget), "exceeds request deadline");
        assertEquals(1, requests(budget).size());
    }

    @Test
    void stopsTricklingSocketsAtTheDeadlineAndDuringRealJobCancellation() throws Exception {
        Settings deadline =
                new Settings("trickle-deadline", "brands")
                        .set("deadline", 4000)
                        .set("readTimeout", 15000);
        program(deadline, reply(BRANDS, 200, "trickle", ""));
        failure(run(deadline), "request deadline exceeded");
        awaitRequests(deadline, 1);
        awaitClosed(deadline);
        assertTrue(rows(deadline).isEmpty());

        Settings cancel = new Settings("trickle-cancel", "brands").set("deadline", 60000);
        program(cancel, reply(BRANDS, 200, "trickle", ""));
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
        Settings redirect = new Settings("no-redirect", "brands");
        program(redirect, reply(BRANDS, 302, "", "unexpected redirect"));
        failure(run(redirect), "HTTP 302");
        assertEquals(1, requests(redirect).size());

        try {
            for (String certificate : List.of("untrusted", "wrong")) {
                certificates(certificate);
                Settings options = new Settings("bad-tls-" + certificate, "brands");
                program(options);
                failure(run(options), "HTTPS or TLS exchange failed");
                assertTrue(requests(options).isEmpty());
            }
        } finally {
            certificates("trusted");
        }
    }

    @Test
    void resumesExactlyTheSavedCustomFieldBrandAndRejectsAChangedParentInventory()
            throws Exception {
        String first = "{\"customFieldId\":\"first\",\"text\":\"α\"}";
        String second = "{\"customFieldId\":\"second\",\"text\":\"β\"}";
        String initialParents = list("result", "{\"brandId\":\"b1\"}", "{\"brandId\":\"b2\"}");
        String sameParents =
                list("result", "{\"brandId\":\"b1\",\"name\":\"new\"}", "{\"brandId\":\"b2\"}");
        String changedParents = list("result", "{\"brandId\":\"b2\"}", "{\"brandId\":\"b1\"}");
        String pathOne = "/v1/customField/list?brandId=b1";
        String pathTwo = "/v1/customField/list?brandId=b2";

        Settings options = new Settings("brand-savepoint", "custom_fields").set("deadline", 120000);
        program(
                options,
                reply(BRANDS, initialParents),
                reply(pathOne, list("result", first)),
                reply(pathTwo, 200, "hold", list("result", second)),
                reply(BRANDS, sameParents),
                reply(pathTwo, list("result", second)));
        resume(options, 3, null);
        assertRows(options, expected("b1", first), expected("b2", second));
        assertEquals(5, requests(options).size());

        Settings changed = new Settings("brand-drift", "custom_fields").set("deadline", 120000);
        program(
                changed,
                reply(BRANDS, initialParents),
                reply(pathOne, list("result", first)),
                reply(pathTwo, 200, "hold", list("result", second)),
                reply(BRANDS, changedParents));
        resume(changed, 3, "saved brand inventory changed");
        assertRows(changed, expected("b1", first));
        assertEquals(4, requests(changed).size());
    }

    @Test
    void resumesTheSecondNumberedPageWithoutRepeatingTheFirst() throws Exception {
        Settings options =
                new Settings("numbered-savepoint", "contacts")
                        .set("pageSize", 1)
                        .set("deadline", 120000);
        String first = "{\"id\":\"first\",\"content\":\"α\"}";
        String second = "{\"id\":\"second\",\"content\":\"β\"}";
        program(
                options,
                reply(numbered(CONTACTS, 1, 1), page("result", 1, 1, 2, null, first)),
                reply(numbered(CONTACTS, 2, 1), 200, "hold", page("result", 2, 1, 2, null, second)),
                reply(numbered(CONTACTS, 2, 1), page("result", 2, 1, 2, null, second)));
        resume(options, 2, null);
        assertRows(options, expected(null, first), expected(null, second));
        assertEquals(3, requests(options).size());
    }

    @Test
    void rejectsUnsafeRegionInvalidDatePagePolicyAndReaderParallelismBeforeTraffic()
            throws Exception {
        Settings region = new Settings("invalid-region", "brands").set("region", "localhost");
        program(region);
        failure(run(region), "region must be us, eu, ca, or au");
        assertTrue(requests(region).isEmpty());

        Settings page = new Settings("invalid-document-size", "documents").set("pageSize", 99);
        program(page);
        failure(run(page), "page_size must divide 10000 evenly");
        assertTrue(requests(page).isEmpty());

        Settings date =
                new Settings("invalid-filter", "brands").set("start", "2023-11-14T22:13:20Z");
        program(date);
        failure(run(date), "creation time options require");
        assertTrue(requests(date).isEmpty());

        Settings parallel = new Settings("bad-parallelism", "contacts").set("parallelism", 2);
        program(parallel);
        failure(run(parallel), "source parallelism must be 1");
        assertTrue(requests(parallel).isEmpty());
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
        Path program = Files.createTempFile(directory, "boldsign-steps-", ".json");
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
        Path source = Files.createTempFile(directory, "boldsign-fixture-", ".txt");
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
            JsonNode value = row.path("brand_id");
            String brand = value.isNull() || value.isMissingNode() ? null : value.asText();
            actual.add(expected(brand, row.path("data").asText()));
        }
        List<String> target = new ArrayList<>(Arrays.asList(wanted));
        Collections.sort(target);
        Collections.sort(actual);
        assertEquals(target, actual);
    }

    private static String expected(String brand, String row) throws IOException {
        return (brand == null ? "<no-brand>" : brand) + "|" + JSON.readTree(row).toString();
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
                .put("host", BoldSignPeer.HOST)
                .put("status", status)
                .put("mode", mode)
                .put("response", response);
    }

    private static String numbered(String path, int page, int size) {
        return path + "?Page=" + page + "&PageSize=" + size;
    }

    private static String list(String member, String... rows) {
        return "{\"" + member + "\":[" + String.join(",", rows) + "]}";
    }

    private static String page(
            String member, int page, int size, Integer total, Integer pages, String... rows) {
        String metadata =
                "\"pageDetails\":{\"page\":"
                        + page
                        + ",\"pageSize\":"
                        + size
                        + (total == null ? "" : ",\"totalRecordsCount\":" + total)
                        + (pages == null ? "" : ",\"totalPages\":" + pages)
                        + "}";
        return "{" + metadata + "," + list(member, rows).substring(1);
    }

    private static String document(String id, long created, long cursor, String cursorField) {
        return "{\"documentId\":\""
                + id
                + "\",\"createdDate\":"
                + created
                + ",\""
                + cursorField
                + "\":"
                + cursor
                + "}";
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
        assertFalse(log.contains(BoldSignPeer.PRIVATE), "log contains provider response data");
        assertFalse(log.contains(BoldSignPeer.KEY), "log contains the provider credential");
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
                                BoldSignPeer.PASSWORD,
                                "-keypass",
                                BoldSignPeer.PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String log = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, process.waitFor(), log);
        KeyStore key = KeyStore.getInstance("PKCS12");
        try (InputStream stream = Files.newInputStream(target)) {
            key.load(stream, BoldSignPeer.PASSWORD.toCharArray());
        }
        return key;
    }

    /**
     * Supplies per-job values through the standard command line.
     *
     * <p>Most cases use the connector's true default page size and request settings.
     */
    private static final class Settings {
        final String name;
        private final Map<String, Object> values = new LinkedHashMap<>();

        Settings(String name, String resource) {
            this.name = name;
            set("token", BoldSignPeer.KEY);
            set("resource", resource);
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
                                return "boldsign." + entry.getKey() + "=" + config;
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
                    + BoldSignPeer.PASSWORD;
        }

        @Override
        protected void executeExtraCommands(GenericContainer<?> container) throws IOException {
            for (String host :
                    List.of(BoldSignPeer.HOST, BoldSignPeer.EU, BoldSignPeer.CA, BoldSignPeer.AU)) {
                container.withExtraHost(host, "127.0.0.1");
            }
            container.withExtraHost(BoldSignPeer.REDIRECT, "127.0.0.1");
            for (String file : List.of("identities.p12", "trust.jks")) {
                container.withCopyFileToContainer(
                        MountableFile.forHostPath(fixtures.resolve(file)), ROOT + "/" + file);
            }
            for (Class<?> type :
                    List.of(BoldSignPeer.class, BoldSignPeer.SwitchingKeyManager.class)) {
                String path = type.getName().replace('.', '/') + ".class";
                try {
                    container.withCopyFileToContainer(
                            MountableFile.forHostPath(
                                    Path.of(type.getResource("/" + path).toURI())),
                            ROOT + "/classes/" + path);
                } catch (URISyntaxException e) {
                    throw new IOException("Cannot stage the BoldSign HTTPS fixture", e);
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
                throw new IOException("Cannot stage the BoldSign fixture JSON library", e);
            }
        }

        Container.ExecResult runPeer() throws Exception {
            return server.execInContainer(
                    "java",
                    "-cp",
                    ROOT + "/classes:" + ROOT + "/jackson:" + ROOT + "/jackson.jar",
                    BoldSignPeer.class.getName(),
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
