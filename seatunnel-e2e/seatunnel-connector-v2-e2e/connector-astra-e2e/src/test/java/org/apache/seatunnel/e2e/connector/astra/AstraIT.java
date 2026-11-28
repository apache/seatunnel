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

package org.apache.seatunnel.e2e.connector.astra;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
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

import javax.net.ssl.HttpsURLConnection;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
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
 * Executes the Astra sink through real Zeta jobs and checks an independent persistent HTTPS peer.
 * Embedding routes, database acknowledgements, scoped mutations and restores are observable.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AstraIT extends TestSuiteBase {
    private static final String CONF = "/astra.conf";
    private static final String ROOT = "/tmp/astra-https-fixture";
    private static final String RANKS =
            "https://openaipublic.blob.core.windows.net/encodings/r50k_base.tiktoken";
    private static final String RANK_SHA =
            "306cd27f03c1a714eca7108e03d66b7dc042abe8c258b44c199a7ed9838dd930";
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final List<String> HOSTS =
            List.of(
                    AstraPeer.DATABASE,
                    AstraPeer.OPENAI,
                    AstraPeer.AZURE,
                    AstraPeer.COHERE,
                    AstraPeer.COMPATIBLE);
    private static final String DB = url(AstraPeer.DATABASE, AstraPeer.TRUSTED);

    @TempDir static Path directory;
    private final ExecutorService tasks = Executors.newCachedThreadPool();
    private Engine engine;
    private Future<Container.ExecResult> fixture;
    private String active;

    @BeforeAll
    void start() throws Exception {
        downloadRanks();
        Files.writeString(directory.resolve("untrusted-ranks"), "invalid local rank data");
        KeyStore trusted = identity("trusted");
        identity("untrusted");
        char[] password = AstraPeer.PASSWORD.toCharArray();
        KeyStore trust = KeyStore.getInstance("JKS");
        trust.load(null, password);
        trust.setCertificateEntry("astra-fixture", trusted.getCertificate("fixture"));
        try (OutputStream out = Files.newOutputStream(directory.resolve("trust.jks"))) {
            trust.store(out, password);
        }
        engine = new Engine(directory);
        engine.startUp();
        fixture = tasks.submit(engine::runPeer);
        Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> engine.exists("ready"));
    }

    @AfterEach
    void verifyPeerContract() throws Exception {
        if (active != null) {
            assertFalse(
                    engine.exists(active + ".invalid"),
                    () -> {
                        try {
                            return engine.contents(active + ".invalid");
                        } catch (Exception failure) {
                            return "Could not read provider assertion";
                        }
                    });
        }
    }

    @AfterAll
    void stop() throws Exception {
        try {
            if (engine != null) {
                try {
                    if (fixture != null) {
                        engine.touch("stop");
                        success(fixture.get(15, TimeUnit.SECONDS));
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
    void createsCosineCollectionAndPersistsExactVectorsAndRenamedWildcardMetadata()
            throws Exception {
        Settings options = settings("typed-metadata");
        options.exists = false;
        options.aliases
                .put("context.team", "department")
                .put("users.*.name", "members")
                .put("body", "content");
        options.rows =
                rows(
                        row("INSERT", 41, "你好 visible 🌎", "ns", "news", List.of(0.5, -0.25, 1.0)),
                        row("INSERT", 41, "second copy", "ns", "news", List.of(-0.5, 0.25, 1.0)));
        startCase(options);
        success(run(options));
        assertEquals(
                List.of("findCollections", "createCollection", "findCollections", "insertMany"),
                operations(options));
        List<JsonNode> docs = stored(options);
        assertEquals(2, docs.size());
        assertNotEquals(docs.get(0).path("_id"), docs.get(1).path("_id"));
        JsonNode first = docs.get(0);
        assertEquals("content: 你好 visible 🌎", first.path("text").asText());
        assertEquals("content: second copy", docs.get(1).path("text").asText());
        assertVector(first, 0.5, -0.25, 1.0);
        assertVector(docs.get(1), -0.5, 0.25, 1.0);
        JsonNode meta = first.path("metadata");
        assertEquals(41, meta.path("id").longValue());
        assertEquals("研发", meta.path("department").asText());
        assertEquals(JSON.readTree("[\"Zoë\",\"李\"]"), meta.path("members"));
        assertTrue(meta.path("optional").isNull());
        assertEquals(4, meta.size());
        assertEquals("namespace-a", first.path("_st_namespace").asText());
        assertEquals("articles", first.path("_st_stream").asText());
        assertEquals(scope("namespace-a", "articles"), first.path("_st_scope").asText());
        assertFalse(first.has("$vectorize"));
    }

    @Test
    void writesTwentyDocumentBatchesAndNeverCollapsesAppendRecords() throws Exception {
        Settings options = settings("append-batches");
        for (int i = 0; i < 45; i++) {
            options.rows.add(
                    row("INSERT", i / 2, "row-" + i, "tenant", "feed", List.of(1.0, 0.0, 0.0)));
        }
        startCase(options);
        success(run(options));
        assertEquals(
                List.of(20, 20, 5),
                requests(options, "insertMany").stream()
                        .map(req -> req.path("body").path("insertMany").path("documents").size())
                        .collect(Collectors.toList()));
        List<JsonNode> docs = stored(options);
        assertEquals(45, docs.size());
        assertEquals(45, docs.stream().map(doc -> doc.path("_id").asText()).distinct().count());
        for (int i = 0; i < docs.size(); i++) {
            assertEquals("body: row-" + i, docs.get(i).path("text").asText());
        }
        assertTrue(requests(options, "deleteMany").isEmpty());
    }

    @Test
    void overwriteClearsTwentyPlusDocumentsOnlyForExactNamespaceAndStream() throws Exception {
        Settings options = settings("scoped-overwrite");
        options.mode = "overwrite";
        for (int i = 0; i < 25; i++) {
            options.seed.add(seed("old-" + i, "namespace-a", "articles", null));
        }
        options.seed.add(seed("other-namespace", "namespace-b", "articles", null));
        options.seed.add(seed("other-stream", "namespace-a", "comments", null));
        options.rows.add(
                row("INSERT", 1, "replacement", "ignored", "ignored", List.of(1.0, 0.0, 0.0)));
        startCase(options);
        success(run(options));
        List<JsonNode> deletes = requests(options, "deleteMany");
        assertEquals(2, deletes.size());
        for (JsonNode command : deletes) {
            assertEquals(
                    JSON.createObjectNode().put("_st_scope", scope("namespace-a", "articles")),
                    command.path("body").path("deleteMany").path("filter"));
        }
        List<JsonNode> docs = stored(options);
        assertEquals(3, docs.size());
        assertEquals(
                2,
                docs.stream().filter(doc -> doc.path("_id").asText().startsWith("other-")).count());
        assertEquals("body: replacement", docs.get(2).path("text").asText());
        assertEquals(scope("namespace-a", "articles"), docs.get(2).path("_st_scope").asText());
    }

    @Test
    void deduplicatesCompositeKeysInInputOrderAndPreservesOtherScopes() throws Exception {
        Settings options = settings("scoped-dedup");
        options.mode = "append_dedup";
        options.namespaceField = "tenant";
        options.streamField = "feed";
        options.omit = true;
        options.keys = names("id", "context.team");
        String record = hash("[42,\"研发\"]");
        for (int i = 0; i < 25; i++) {
            options.seed.add(seed("old-" + i, "main", "news", record));
        }
        options.seed.add(seed("untouched-namespace", "other", "news", record));
        options.seed.add(seed("untouched-stream", "main", "updates", record));
        options.rows =
                rows(
                        row(
                                "INSERT",
                                42,
                                "first replacement",
                                "main",
                                "news",
                                List.of(0.5, 0.0, 0.0)),
                        row(
                                "UPDATE_AFTER",
                                42,
                                "last replacement",
                                "main",
                                "news",
                                List.of(0.0, 1.0, 0.0)),
                        row(
                                "INSERT",
                                42,
                                "different scope",
                                "main",
                                "research",
                                List.of(0.0, 0.0, 1.0)));
        startCase(options);
        success(run(options));
        List<JsonNode> deletes = requests(options, "deleteMany");
        assertEquals(4, deletes.size());
        JsonNode expected =
                JSON.createObjectNode()
                        .put("_st_scope", scope("main", "news"))
                        .put("_st_record_id", record);
        for (JsonNode delete : deletes.subList(0, 3)) {
            assertEquals(expected, delete.path("body").path("deleteMany").path("filter"));
        }
        assertEquals(
                JSON.createObjectNode()
                        .put("_st_scope", scope("main", "research"))
                        .put("_st_record_id", record),
                deletes.get(3).path("body").path("deleteMany").path("filter"));
        List<JsonNode> docs = stored(options);
        assertEquals(4, docs.size());
        assertEquals(
                2,
                docs.stream()
                        .filter(doc -> doc.path("_id").asText().startsWith("untouched-"))
                        .count());
        JsonNode latest = docs.get(2);
        assertEquals(record, latest.path("_st_record_id").asText());
        assertEquals(
                hash(JSON.writeValueAsString(List.of(scope("main", "news"), record, 0))),
                latest.path("_id").asText());
        assertVector(latest, 0.0, 1.0, 0.0);
        assertFalse(latest.has("text"));
        assertFalse(latest.path("metadata").has("body"));
        assertEquals(scope("main", "research"), docs.get(3).path("_st_scope").asText());
        assertVector(docs.get(3), 0.0, 0.0, 1.0);
    }

    @Test
    void appliesCdcTombstonesWithoutGeneratingChunksOrTouchingOtherRecords() throws Exception {
        Settings options = settings("tombstones");
        options.mode = "append_dedup";
        options.keys = names("id");
        options.seed.add(seed("remove", "namespace-a", "articles", hash("[42]")));
        options.seed.add(seed("retain-other-key", "namespace-a", "articles", hash("[41]")));
        options.seed.add(seed("retain-other-scope", "namespace-b", "articles", hash("[42]")));
        options.rows = rows(row("DELETE", 42, null, "unused", "unused", null));
        startCase(options);
        success(run(options));
        assertEquals(List.of("findCollections", "deleteMany"), operations(options));
        assertEquals(
                List.of("retain-other-key", "retain-other-scope"),
                stored(options).stream()
                        .map(doc -> doc.path("_id").asText())
                        .collect(Collectors.toList()));
    }

    @Test
    void requestsAllRemoteEmbeddingContractsAndUsesExplicitLocalSimulation() throws Exception {
        for (String mode :
                List.of("openai", "azure_openai", "cohere", "openai_compatible", "fake")) {
            Settings options = settings("embedding-" + mode.replace('_', '-'));
            options.remote(mode);
            options.rows =
                    rows(
                            row("INSERT", 10, "alpha", "tenant", "stream", null),
                            row("INSERT", 11, "beta", "tenant", "stream", null));
            startCase(options);
            success(run(options));
            List<JsonNode> docs = stored(options);
            assertEquals(2, docs.size());
            assertEquals("body: alpha", docs.get(0).path("text").asText());
            assertEquals("body: beta", docs.get(1).path("text").asText());
            assertEquals(options.dimensions, docs.get(0).path("$vector").size());
            if ("fake".equals(mode)) {
                assertTrue(requests(options, "embedding").isEmpty());
                assertNotEquals(docs.get(0).path("$vector"), docs.get(1).path("$vector"));
                assertTrue(docs.get(0).path("$vector").get(0).isNumber());
            } else {
                List<JsonNode> calls = requests(options, "embedding");
                assertEquals(1, calls.size());
                assertEquals(options.embedHost, calls.get(0).path("host").asText());
                assertEquals(
                        names("body: alpha", "body: beta"),
                        calls.get(0).path("body").path("cohere".equals(mode) ? "texts" : "input"));
                assertEquals(0.5, docs.get(0).path("$vector").get(0).asDouble());
                assertEquals(-0.5, docs.get(1).path("$vector").get(0).asDouble());
                assertEquals(
                        0.25, docs.get(0).path("$vector").get(options.dimensions - 1).asDouble());
            }
        }

        Settings anonymous = settings("embedding-anonymous");
        anonymous.remote("openai_compatible");
        anonymous.fault = "anonymous";
        anonymous.embedToken = "";
        anonymous.rows.add(row("INSERT", 1, "alpha", "tenant", "stream", null));
        startCase(anonymous);
        success(run(anonymous));
        assertVector(stored(anonymous).get(0), 0.5, 0.0, 0.25);
    }

    @Test
    void splitsByLanguageMarkedStructureAndRealGpt2TokenCeilings() throws Exception {
        Settings literal = settings("literal");
        literal.chunk = 4;
        literal.separators = names("|");
        literal.rows.add(
                row("INSERT", 1, "alpha|beta|gamma", "unused", "unused", List.of(1.0, 0.0, 0.0)));
        startCase(literal);
        success(run(literal));
        assertEquals(List.of("body: alpha", "beta|gamma"), text(literal));

        Settings overlap = settings("token-overlap");
        overlap.chunk = 4;
        overlap.overlap = 1;
        overlap.rows.add(
                row(
                        "INSERT",
                        1,
                        "alpha beta gamma delta",
                        "unused",
                        "unused",
                        List.of(1.0, 0.0, 0.0)));
        startCase(overlap);
        success(run(overlap));
        assertEquals(List.of("body: alpha beta", "beta gamma delta"), text(overlap));

        Settings markdown = settings("markdown");
        markdown.chunk = 5;
        markdown.splitter = "markdown";
        markdown.rows.add(
                row(
                        "INSERT",
                        1,
                        "alpha\n# Heading\nvalue",
                        "unused",
                        "unused",
                        List.of(1.0, 0.0, 0.0)));
        startCase(markdown);
        success(run(markdown));
        assertEquals(List.of("body: alpha", "# Heading\nvalue"), text(markdown));

        Settings code = settings("code");
        code.chunk = 5;
        code.splitter = "code";
        code.language = "java";
        code.rows.add(
                row(
                        "INSERT",
                        1,
                        "package a;\nclass A {}\nclass B {}",
                        "unused",
                        "unused",
                        List.of(1.0, 0.0, 0.0)));
        startCase(code);
        success(run(code));
        assertEquals(List.of("body: package a;", "class A {}", "class B {}"), text(code));

        Settings proof = settings("token-not-characters");
        proof.chunk = 5;
        proof.rows.add(row("INSERT", 1, "hello world", "unused", "unused", List.of(1.0, 0.0, 0.0)));
        startCase(proof);
        success(run(proof));
        assertEquals(List.of("body: hello world"), text(proof));
    }

    @Test
    void rejectsInvalidRowsRanksAndProjectionBeforeAstraWrites() throws Exception {
        for (String fault :
                List.of(
                        "missing-text",
                        "wrong-vector",
                        "nonfinite",
                        "empty-vector",
                        "bad-ranks",
                        "row-limit",
                        "chunk-limit",
                        "buffer-limit",
                        "null-key")) {
            Settings options = settings(fault);
            String body = "hello";
            List<Double> vector = List.of(0.5, 0.0, 0.25);
            String reason;
            switch (fault) {
                case "missing-text":
                    options.textFields = names("context.absent");
                    reason = "none of the selected text fields";
                    break;
                case "wrong-vector":
                    vector = List.of(0.5, 0.25);
                    reason = "vector dimensions do not match";
                    break;
                case "nonfinite":
                    vector = List.of(1e50, 0.25, 0.5);
                    reason = "nonfinite or nonnumeric";
                    break;
                case "empty-vector":
                    vector = List.of(0.0, 0.0, 0.0);
                    reason = "must not be all zero";
                    break;
                case "bad-ranks":
                    options.ranks = ROOT + "/untrusted-ranks";
                    options.chunk = 5;
                    body = "hello world";
                    reason = "tokenizer rank data checksum mismatch";
                    break;
                case "row-limit":
                    options.maxRow = 256;
                    body = "a".repeat(400);
                    reason = "input exceeds max_row_bytes";
                    break;
                case "chunk-limit":
                    options.maxChunks = 1;
                    options.chunk = 4;
                    body = "alpha beta gamma delta";
                    reason = "max_chunks_per_row";
                    break;
                case "buffer-limit":
                    options.maxBuffer = 1024;
                    reason = "expanded row exceeds max_buffer_bytes";
                    break;
                default:
                    options.mode = "append_dedup";
                    options.keys = names("context.absent");
                    reason = "primary key components";
            }
            options.rows.add(row("INSERT", 1, body, "tenant", "news", vector));
            startCase(options);
            failure(run(options), reason);
            assertTrue(operations(options).isEmpty());
        }
    }

    @Test
    void failsOnShapeOrIndexErrorsBeforeOverwriteCanDeleteExistingRows() throws Exception {
        Settings options = settings("missing-index");
        options.remote("openai");
        options.mode = "overwrite";
        options.fault = "embed-bad-index";
        options.seed.add(seed("preserve-on-error", "namespace-a", "articles", null));
        options.rows =
                rows(
                        row("INSERT", 1, "alpha", "ns", "news", null),
                        row("INSERT", 2, "beta", "ns", "news", null));
        startCase(options);
        failure(run(options), "duplicate or invalid result index");
        assertEquals(List.of("embedding"), operations(options));
        assertEquals(
                List.of("preserve-on-error"),
                stored(options).stream()
                        .map(doc -> doc.path("_id").asText())
                        .collect(Collectors.toList()));
    }

    @Test
    void rejectsWrongCollectionVectorSchemaAndStalledScopedDeletion() throws Exception {
        for (String fault : List.of("dimension", "metric", "delete-stall", "read-error")) {
            Settings options = settings("collection-" + fault);
            options.mode = "overwrite";
            options.fault = fault;
            options.metric = "metric".equals(fault) ? "euclidean" : "cosine";
            options.fixtureDimensions = "dimension".equals(fault) ? 4 : 3;
            options.seed.add(seed("preserve-old", "namespace-a", "articles", null));
            options.rows.add(row("INSERT", 1, "new", "ns", "news", List.of(1.0, 0.0, 0.0)));
            startCase(options);
            failure(
                    run(options),
                    "delete-stall".equals(fault)
                            ? "invalid scoped delete acknowledgement"
                            : "read-error".equals(fault)
                                    ? "provider returned errors"
                                    : "dimension or metric is incompatible");
            assertTrue(requests(options, "createCollection").isEmpty());
            assertTrue(requests(options, "insertMany").isEmpty());
            assertEquals("preserve-old", stored(options).get(0).path("_id").asText());
        }

        Settings limited = settings("delete-request-limit");
        limited.mode = "overwrite";
        limited.maxDelete = 1;
        limited.rows.add(row("INSERT", 1, "new", "ns", "news", List.of(1.0, 0.0, 0.0)));
        for (int i = 0; i < 25; i++) {
            limited.seed.add(seed("old-" + i, "namespace-a", "articles", null));
        }
        startCase(limited);
        failure(run(limited), "filtered deletion exceeded max_delete_requests");
        assertEquals(1, requests(limited, "deleteMany").size());
        assertTrue(requests(limited, "insertMany").isEmpty());
        assertEquals(5, stored(limited).size());
    }

    @Test
    void boundsRequestAndResponseBodiesBeforeAnyDataMutations() throws Exception {
        Settings request = settings("request-bytes");
        request.maxBody = 1024;
        request.chunk = 8191;
        request.rows.add(row("INSERT", 1, "x".repeat(1100), "ns", "feed", List.of(0.5, 0.0, 0.25)));
        startCase(request);
        failure(run(request), "single document exceeds the permitted request size");
        assertTrue(requests(request, "insertMany").isEmpty());

        Settings response = settings("response-bytes");
        response.maxBody = 1024;
        response.fault = "oversized-response";
        response.rows.add(row("INSERT", 1, "short", "ns", "feed", List.of(0.5, 0.0, 0.25)));
        startCase(response);
        failure(run(response), "response exceeds the configured byte bound");
        assertEquals(List.of("findCollections"), operations(response));
    }

    @Test
    void neverAcknowledgesUnknownOrPartialWritesAndVerifiesDuplicateDigests() throws Exception {
        for (String fault :
                List.of(
                        "false-ack",
                        "partial",
                        "unknown",
                        "duplicate-match",
                        "duplicate-conflict")) {
            Settings options = settings("ack-" + fault);
            options.fault = fault;
            options.rows =
                    rows(
                            row("INSERT", 1, "alpha", "ns", "feed", List.of(0.5, 0.0, 0.25)),
                            row("INSERT", 2, "beta", "ns", "feed", List.of(0.25, 0.0, 0.5)));
            startCase(options);
            Container.ExecResult result = run(options);
            if ("duplicate-match".equals(fault)) {
                success(result);
                assertEquals(List.of("body: alpha", "body: beta"), text(options));
                assertEquals(2, requests(options, "findOne").size());
            } else if ("duplicate-conflict".equals(fault)) {
                failure(result, "duplicate document does not match");
                assertEquals(1, requests(options, "findOne").size());
            } else if ("unknown".equals(fault)) {
                failure(result, "no valid mutation acknowledgement");
                assertEquals(List.of("body: alpha"), text(options));
            } else if ("partial".equals(fault)) {
                failure(result, "partial write");
                assertEquals(List.of("body: alpha"), text(options));
            } else {
                failure(result, "unexpected or repeated inserted ID");
                assertTrue(stored(options).isEmpty());
            }
            assertEquals(1, requests(options, "insertMany").size());
        }
    }

    @Test
    void boundsWriteAndEmbeddingRateRetriesAndRetainsTheIdenticalRequest() throws Exception {
        Settings write = settings("write-throttle");
        write.fault = "throttle-once";
        write.rows.add(row("INSERT", 1, "alpha", "ns", "feed", List.of(1.0, 0.0, 0.0)));
        startCase(write);
        success(run(write));
        List<JsonNode> attempts = requests(write, "insertMany");
        assertEquals(2, attempts.size());
        assertEquals(attempts.get(0), attempts.get(1));
        assertEquals(List.of("body: alpha"), text(write));

        Settings exhausted = settings("rate-exhausted");
        exhausted.fault = "throttled";
        exhausted.attempts = 2;
        exhausted.rows.add(row("INSERT", 1, "alpha", "ns", "feed", List.of(1.0, 0.0, 0.0)));
        startCase(exhausted);
        failure(run(exhausted), "HTTP 429");
        assertEquals(2, requests(exhausted, "insertMany").size());
        assertTrue(stored(exhausted).isEmpty());

        Settings embed = settings("embed-throttle");
        embed.remote("cohere");
        embed.fault = "embed-throttle";
        embed.rows.add(row("INSERT", 1, "alpha", "ns", "feed", null));
        startCase(embed);
        success(run(embed));
        assertEquals(2, requests(embed, "embedding").size());
        assertEquals(List.of("body: alpha"), text(embed));
    }

    @Test
    void rejectsTlsRedirectsUnsafeParallelismAndDynamicOverwrite() throws Exception {
        Settings redirect = settings("redirect");
        redirect.fault = "redirect";
        redirect.rows.add(row("INSERT", 1, "a", "ns", "feed", List.of(1.0, 0.0, 0.0)));
        startCase(redirect);
        failure(run(redirect), "HTTP 302");
        assertEquals(List.of("findCollections"), operations(redirect));

        Settings tls = settings("tls");
        tls.endpoint = url(AstraPeer.DATABASE, AstraPeer.UNTRUSTED);
        tls.rows = redirect.rows;
        startCase(tls);
        failure(run(tls), "HTTPS exchange failed");
        assertTrue(operations(tls).isEmpty());

        Settings coarse = settings("parallel");
        coarse.parallelism = 2;
        coarse.rows = redirect.rows;
        startCase(coarse);
        failure(run(coarse), "sink parallelism must be 1");
        assertTrue(operations(coarse).isEmpty());

        Settings routing = settings("dynamic-overwrite");
        routing.mode = "overwrite";
        routing.namespaceField = "tenant";
        routing.rows = redirect.rows;
        startCase(routing);
        failure(run(routing), "overwrite requires a configured static namespace and stream");
        assertTrue(operations(routing).isEmpty());
    }

    @Test
    void enforcesAbsoluteDeadlineAndDisconnectsTheCanceledJob() throws Exception {
        Settings deadline = settings("deadline");
        deadline.fault = "slow";
        deadline.deadline = 2200;
        deadline.rows.add(row("INSERT", 1, "new", "ns", "feed", List.of(1.0, 0.0, 0.0)));
        startCase(deadline);
        failure(run(deadline), "request deadline exceeded");
        awaitDisconnect(deadline);
        assertEquals(List.of("findCollections"), operations(deadline));
        assertTrue(stored(deadline).isEmpty());

        Settings canceled = settings("cancel");
        canceled.fault = "slow";
        canceled.deadline = 60000;
        canceled.rows = deadline.rows;
        startCase(canceled);
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(CONF, job, canceled.variables()));
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> !operations(canceled).isEmpty());
            success(engine.cancelJob(job));
            running.get(30, TimeUnit.SECONDS);
            awaitDisconnect(canceled);
            assertEquals(List.of("findCollections"), operations(canceled));
            assertTrue(stored(canceled).isEmpty());
        } finally {
            if (!running.isDone()) {
                engine.cancelJob(job);
                running.cancel(true);
            }
        }
    }

    @Test
    void savepointRestoresSequenceAndNeverClearsTheScopeTwice() throws Exception {
        Settings options = settings("restored-overwrite");
        options.mode = "overwrite";
        options.jobMode = "STREAMING";
        options.rowNum = 2;
        options.splitNum = 2;
        options.interval = 18000;
        options.seed.add(seed("old", "namespace-a", "articles", null));
        options.seed.add(seed("separate-scope", "namespace-b", "articles", null));
        options.rows.add(row("INSERT", 10, "each split", "ns", "news", List.of(1.0, 0.0, 0.0)));
        startCase(options);
        String job = jobId();
        Future<Container.ExecResult> initial =
                tasks.submit(() -> engine.executeJob(CONF, job, options.variables()));
        try {
            awaitInserts(options, 1);
            Container.ExecResult saved = engine.savepointJob(job);
            success(saved);
            initial.get(30, TimeUnit.SECONDS);
            assertEquals(1, requests(options, "insertMany").size());
            List<JsonNode> before = stored(options);
            assertEquals(2, before.size());
            String firstId = before.get(1).path("_id").asText();

            Future<Container.ExecResult> restored =
                    tasks.submit(() -> engine.restoreJob(CONF, job, options.variables()));
            try {
                awaitInserts(options, 2);
                success(engine.cancelJob(job));
                restored.get(30, TimeUnit.SECONDS);
            } finally {
                if (!restored.isDone()) {
                    engine.cancelJob(job);
                    restored.cancel(true);
                }
            }
            List<JsonNode> after = stored(options);
            assertEquals(1, requests(options, "deleteMany").size());
            assertEquals(3, after.size());
            assertEquals("separate-scope", after.get(0).path("_id").asText());
            assertEquals(firstId, after.get(1).path("_id").asText());
            assertNotEquals(firstId, after.get(2).path("_id").asText());
            assertEquals("body: each split", after.get(1).path("text").asText());
            assertEquals("body: each split", after.get(2).path("text").asText());
        } finally {
            if (!initial.isDone()) {
                engine.cancelJob(job);
                initial.cancel(true);
            }
        }
    }

    private Settings settings(String name) {
        return new Settings(name);
    }

    private void startCase(Settings options) throws Exception {
        verifyPeerContract();
        Path data = Files.createTempFile(directory, "astra-case-", ".json");
        ObjectNode input =
                JSON.createObjectNode()
                        .put(
                                "dimensions",
                                options.fixtureDimensions == 0
                                        ? options.dimensions
                                        : options.fixtureDimensions)
                        .put("exists", options.exists)
                        .put("metric", options.metric)
                        .put("fault", options.fault);
        input.set("documents", options.seed);
        Files.writeString(data, input.toString());
        engine.install(data, options.name + ".input");
        Path marker = Files.createTempFile(directory, "astra-case-", ".active");
        Files.writeString(marker, options.name);
        engine.install(marker, "active");
        active = options.name;
    }

    private Container.ExecResult run(Settings options) throws Exception {
        return engine.executeJob(CONF, jobId(), options.variables());
    }

    private List<JsonNode> stored(Settings options) throws Exception {
        String value = engine.contents(options.name + ".documents");
        if (value.isEmpty()) {
            return List.of();
        }
        List<JsonNode> result = new ArrayList<>();
        JSON.readTree(value).forEach(result::add);
        return result;
    }

    private List<String> text(Settings options) throws Exception {
        return stored(options).stream()
                .map(entry -> entry.path("text").asText())
                .collect(Collectors.toList());
    }

    private List<JsonNode> requests(Settings options, String command) throws Exception {
        List<JsonNode> result = new ArrayList<>();
        String log = engine.contents(options.name + ".requests");
        for (String entry : log.split("\n")) {
            if (!entry.isBlank()) {
                JsonNode json = JSON.readTree(entry);
                if (command == null || command.equals(json.path("operation").asText())) {
                    result.add(json);
                }
            }
        }
        return result;
    }

    private List<String> operations(Settings options) throws Exception {
        return requests(options, null).stream()
                .map(entry -> entry.path("operation").asText())
                .collect(Collectors.toList());
    }

    private void awaitInserts(Settings options, int count) {
        Awaitility.await()
                .atMost(45, TimeUnit.SECONDS)
                .until(
                        () ->
                                requests(options, "insertMany").size() == count
                                        && stored(options).size() == count + 1);
    }

    private void awaitDisconnect(Settings options) {
        Awaitility.await()
                .atMost(20, TimeUnit.SECONDS)
                .until(() -> engine.exists(options.name + ".disconnect"));
    }

    private static ObjectNode row(
            String kind, long id, String body, String tenant, String feed, List<Double> vector) {
        ObjectNode row = JSON.createObjectNode().put("kind", kind);
        ArrayNode data = row.putArray("fields").add(id).add(body).add(tenant).add(feed);
        if (vector == null) {
            data.addNull();
        } else {
            ArrayNode values = data.addArray();
            vector.forEach(values::add);
        }
        data.addArray().add("研发").add(true);
        ArrayNode users = data.addArray();
        users.addObject().put("name", "Zoë");
        users.addObject().put("name", "李");
        data.addNull();
        return row;
    }

    private static ArrayNode rows(ObjectNode... rows) {
        return JSON.createArrayNode().addAll(Arrays.asList(rows));
    }

    private static ArrayNode names(String... values) {
        ArrayNode array = JSON.createArrayNode();
        for (String value : values) {
            array.add(value);
        }
        return array;
    }

    private static ObjectNode seed(String id, String namespace, String stream, String record)
            throws Exception {
        ObjectNode doc =
                JSON.createObjectNode()
                        .put("_id", id)
                        .put("_st_scope", scope(namespace, stream))
                        .put("_st_namespace", namespace)
                        .put("_st_stream", stream)
                        .put("text", "original")
                        .put("_st_payload_hash", "existing-content");
        if (record != null) {
            doc.put("_st_record_id", record);
        }
        doc.putArray("$vector").add(1.0).add(0.0).add(0.0);
        return doc;
    }

    private static String scope(String namespace, String stream) throws Exception {
        return hash(JSON.writeValueAsString(List.of(namespace, stream)));
    }

    private static String hash(String plain) throws Exception {
        byte[] input = plain.getBytes(StandardCharsets.UTF_8);
        return Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(MessageDigest.getInstance("SHA-256").digest(input));
    }

    private static void assertVector(JsonNode doc, double... values) {
        JsonNode vector = doc.path("$vector");
        assertEquals(values.length, vector.size());
        for (int i = 0; i < values.length; i++) {
            assertEquals(values[i], vector.get(i).asDouble());
        }
    }

    private static String jobId() {
        return Long.toString(
                ThreadLocalRandom.current().nextLong(1_000_000_000_000L, 9_000_000_000_000L));
    }

    private static String url(String host, int port) {
        return "https://" + host + ":" + port;
    }

    private static String output(Container.ExecResult result) {
        return result.getStdout() + result.getStderr();
    }

    private static void success(Container.ExecResult result) {
        assertEquals(0, result.getExitCode(), () -> output(result));
    }

    private static void failure(Container.ExecResult result, String reason) {
        String output = output(result);
        assertNotEquals(0, result.getExitCode(), output);
        assertTrue(output.contains(reason), output);
        assertFalse(output.contains(AstraPeer.DB_TOKEN));
        assertFalse(output.contains(AstraPeer.EMBED_TOKEN));
        assertFalse(output.contains(AstraPeer.PRIVATE));
    }

    private static KeyStore identity(String name) throws Exception {
        String keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
        Path target = directory.resolve(name + ".p12");
        String alt = HOSTS.stream().map(host -> "dns:" + host).collect(Collectors.joining(","));
        Process process =
                new ProcessBuilder(
                                keytool,
                                "-genkeypair",
                                "-alias",
                                "fixture",
                                "-keyalg",
                                "RSA",
                                "-keysize",
                                "2048",
                                "-validity",
                                "7",
                                "-dname",
                                "CN=" + AstraPeer.DATABASE,
                                "-ext",
                                "SAN=" + alt,
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                target.toString(),
                                "-storepass",
                                AstraPeer.PASSWORD,
                                "-keypass",
                                AstraPeer.PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, process.waitFor(), output);
        KeyStore key = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(target)) {
            key.load(input, AstraPeer.PASSWORD.toCharArray());
        }
        return key;
    }

    private static void downloadRanks() throws Exception {
        HttpsURLConnection connection =
                (HttpsURLConnection) URI.create(RANKS).toURL().openConnection();
        connection.setInstanceFollowRedirects(false);
        connection.setConnectTimeout(15000);
        connection.setReadTimeout(15000);
        try {
            assertEquals(200, connection.getResponseCode());
            byte[] data;
            try (InputStream response = connection.getInputStream()) {
                data = response.readNBytes(1048577);
            }
            assertTrue(data.length < 1048577);
            StringBuilder actual = new StringBuilder();
            for (byte value : MessageDigest.getInstance("SHA-256").digest(data)) {
                actual.append(String.format("%02x", value & 255));
            }
            assertEquals(RANK_SHA, actual.toString());
            Files.write(directory.resolve("ranks"), data);
        } finally {
            connection.disconnect();
        }
    }

    /**
     * Supplies synthetic jobs through the same CLI variables available in a normal installation.
     * The fixture independently checks API credentials, dimensions and request routes.
     */
    private static final class Settings {
        final String name;
        String endpoint = DB;
        String mode = "append";
        String namespace = "namespace-a";
        String stream = "articles";
        String namespaceField = "";
        String streamField = "";
        String metric = "cosine";
        String fault = "";
        String splitter = "separator";
        String language = "";
        String ranks = ROOT + "/ranks";
        String embedding = "from_field";
        String embedHost = "";
        String embedUrl = "";
        String embedToken = AstraPeer.EMBED_TOKEN;
        String jobMode = "BATCH";
        boolean exists = true;
        boolean omit;
        boolean keep;
        int dimensions = 3;
        int fixtureDimensions;
        int chunk = 400;
        int overlap;
        int markdown = 1;
        int deadline = 30000;
        int attempts = 3;
        int maxRow = 262144;
        int maxBuffer = 4194304;
        int maxBody = 4194304;
        int maxChunks = 100;
        int maxDelete = 500;
        int parallelism = 1;
        int rowNum = 1;
        int splitNum = 1;
        int interval = 1;
        ArrayNode rows = JSON.createArrayNode();
        ArrayNode keys = JSON.createArrayNode();
        ArrayNode textFields = names("body");
        ArrayNode metadataFields = names("id", "context.team", "users.*.name", "optional");
        ArrayNode separators = names("\n\n", "\n", " ", "");
        ObjectNode aliases = JSON.createObjectNode();
        ArrayNode seed = JSON.createArrayNode();

        Settings(String name) {
            this.name = name;
        }

        void remote(String mode) {
            embedding = mode;
            switch (mode) {
                case "openai":
                    embedHost = AstraPeer.OPENAI;
                    dimensions = 1536;
                    break;
                case "azure_openai":
                    embedHost = AstraPeer.AZURE;
                    dimensions = 1536;
                    break;
                case "cohere":
                    embedHost = AstraPeer.COHERE;
                    dimensions = 1024;
                    break;
                case "openai_compatible":
                    embedHost = AstraPeer.COMPATIBLE;
                    dimensions = 3;
                    break;
                case "fake":
                    dimensions = 1536;
                    return;
                default:
                    throw new IllegalArgumentException(mode);
            }
            embedUrl =
                    url(embedHost, AstraPeer.TRUSTED)
                            + ("openai_compatible".equals(mode) ? "/private/v1" : "");
        }

        String[] variables() {
            return new String[] {
                "astra.endpoint=" + endpoint,
                "astra.token=" + AstraPeer.DB_TOKEN,
                "astra.jobMode=" + jobMode,
                "astra.parallelism=" + parallelism,
                "astra.rowNum=" + rowNum,
                "astra.splitNum=" + splitNum,
                "astra.interval=" + interval,
                "astra.rows=" + rows,
                "astra.mode=" + mode,
                "astra.namespace=" + namespace,
                "astra.stream=" + stream,
                "astra.namespaceField=" + namespaceField,
                "astra.streamField=" + streamField,
                "astra.keys=" + keys,
                "astra.textFields=" + textFields,
                "astra.metadataFields=" + metadataFields,
                "astra.aliases=" + aliases,
                "astra.omit=" + omit,
                "astra.splitter=" + splitter,
                "astra.separators=" + separators,
                "astra.keep=" + keep,
                "astra.language=" + language,
                "astra.markdown=" + markdown,
                "astra.chunk=" + chunk,
                "astra.overlap=" + overlap,
                "astra.ranks=" + ranks,
                "astra.embedding=" + embedding,
                "astra.embedUrl=" + embedUrl,
                "astra.embedToken=" + embedToken,
                "astra.dimensions=" + dimensions,
                "astra.maxRow=" + maxRow,
                "astra.maxBuffer=" + maxBuffer,
                "astra.maxBody=" + maxBody,
                "astra.maxChunks=" + maxChunks,
                "astra.maxDelete=" + maxDelete,
                "astra.deadline=" + deadline,
                "astra.attempts=" + attempts
            };
        }
    }

    /**
     * Runs the Java 11 engine and mounts independent fixture identities and vocabulary data. Local
     * peer endpoints share the container network namespace through explicit host aliases.
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
                    + AstraPeer.PASSWORD;
        }

        @Override
        protected void executeExtraCommands(GenericContainer<?> container) throws IOException {
            HOSTS.forEach(host -> container.withExtraHost(host, "127.0.0.1"));
            for (String file :
                    List.of(
                            "trusted.p12",
                            "untrusted.p12",
                            "trust.jks",
                            "ranks",
                            "untrusted-ranks")) {
                container.withCopyFileToContainer(
                        MountableFile.forHostPath(fixtures.resolve(file)), ROOT + "/" + file);
            }
            String javaPackage = AstraPeer.class.getPackage().getName().replace('.', '/');
            try {
                for (String file : List.of("AstraPeer.class", "AstraPeer$Case.class")) {
                    container.withCopyFileToContainer(
                            MountableFile.forHostPath(
                                    Paths.get(AstraPeer.class.getResource(file).toURI())),
                            ROOT + "/classes/" + javaPackage + "/" + file);
                }
                Path json =
                        Path.of(
                                ObjectMapper.class
                                        .getProtectionDomain()
                                        .getCodeSource()
                                        .getLocation()
                                        .toURI());
                String target = Files.isDirectory(json) ? "/jackson" : "/jackson.jar";
                container.withCopyFileToContainer(MountableFile.forHostPath(json), ROOT + target);
            } catch (URISyntaxException e) {
                throw new IOException("Cannot stage the HTTPS datastore fixture", e);
            }
        }

        Container.ExecResult runPeer() throws Exception {
            return server.execInContainer(
                    "java",
                    "-cp",
                    ROOT + "/classes:" + ROOT + "/jackson:" + ROOT + "/jackson.jar",
                    AstraPeer.class.getName(),
                    ROOT);
        }

        void install(Path source, String name) {
            server.copyFileToContainer(MountableFile.forHostPath(source), ROOT + "/" + name);
        }

        boolean exists(String name) throws Exception {
            return server.execInContainer("test", "-f", ROOT + "/" + name).getExitCode() == 0;
        }

        void touch(String name) throws Exception {
            success(server.execInContainer("touch", ROOT + "/" + name));
        }

        String contents(String name) throws Exception {
            if (!exists(name)) {
                return "";
            }
            Container.ExecResult read = server.execInContainer("cat", ROOT + "/" + name);
            success(read);
            return read.getStdout();
        }
    }
}
