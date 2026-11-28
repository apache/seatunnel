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

package org.apache.seatunnel.e2e.connector.assemblyai;

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
class AssemblyAIIT extends TestSuiteBase {
    private static final String ONE = "/assemblyai-one.conf";
    private static final String ALL = "/assemblyai-all.conf";
    private static final String TERMINAL = "/assemblyai-terminal.conf";
    private static final String T1 = "11111111-1111-4111-8111-111111111111";
    private static final String T2 = "22222222-2222-4222-8222-222222222222";
    private static final String T3 = "33333333-3333-4333-8333-333333333333";
    private static final String NEXT = "44444444-4444-4444-8444-444444444444";
    private static final ObjectMapper JSON =
            new ObjectMapper()
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    @TempDir static Path directory;
    private static String trust;
    private final ExecutorService tasks = Executors.newCachedThreadPool();
    private Engine engine;
    private Future<Container.ExecResult> peer;
    private String active;

    private static final String EXTRA =
            ",\"unknown\":{\"unicode\":\"数据🌏\",\"null\":null,\"items\":[false,null,[1,2]],"
                    + "\"integer\":9223372036854775808,\"decimal\":123456789012345678.123456789}";

    @BeforeAll
    void startEngineAndIndependentHttpsPeer() throws Exception {
        KeyStore trusted = identity("trusted");
        identity("untrusted");
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

    @Test
    void readsAllFourResourcesWithStableParentAndChildIdentities() throws Exception {
        String name = "all-resources";
        String a = summary(T1, "2024-01-02T00:00:00.000000", "completed", false);
        String b = summary(T2, "2024-01-01T12:00:00Z", "processing", false);
        String c = summary(T3, "2024-01-01T00:00:00Z", "completed", false);
        String sentence =
                "{\"text\":\"A sentence.\",\"start\":0,\"end\":1000,\"confidence\":0.987654321"
                        + EXTRA
                        + "}";
        String paragraph =
                "{\"text\":\"A paragraph.\",\"start\":0,\"end\":2000,\"words\":[]" + EXTRA + "}";
        String audio = redacted(true);
        program(
                name,
                List.of(
                        ok("/v2/transcript", "limit=2", list(older(NEXT), null, a, b)),
                        ok(child(T1, "sentences"), "", children(T1, "sentences", sentence)),
                        ok(child(T1, "paragraphs"), "", children(T1, "paragraphs", paragraph)),
                        ok(detail(T1), "", detailRecord(T1, true)),
                        ok(child(T1, "redacted-audio"), "", audio),
                        ok("/v2/transcript", "before_id=" + NEXT + "&limit=2", list(null, null, c)),
                        ok(child(T3, "sentences"), "", children(T3, "sentences", sentence)),
                        ok(child(T3, "paragraphs"), "", children(T3, "paragraphs", paragraph)),
                        ok(detail(T3), "", detailRecord(T3, false))));
        success(run(ALL, new Settings(name, "transcripts")));
        assertRows(
                name,
                Map.of(
                        key("transcripts", T1),
                        expected(T1, a),
                        key("transcripts", T2),
                        expected(T2, b),
                        key("transcripts", T3),
                        expected(T3, c),
                        key("transcript_sentences", T1 + ":1"),
                        expected(T1, sentence),
                        key("paragraphs", T1 + ":1"),
                        expected(T1, paragraph),
                        key("redacted_audio", T1),
                        expected(T1, audio),
                        key("transcript_sentences", T3 + ":1"),
                        expected(T3, sentence),
                        key("paragraphs", T3 + ":1"),
                        expected(T3, paragraph)));
        assertEquals(9, requests(name).size());
    }

    @Test
    void traversesPrevUrlsAndContinuesAnEmptyPageWithoutTakingNextUrls() throws Exception {
        String name = "old-direction";
        String a = summary(T1, "2024-01-01T12:00:00Z", "completed", false);
        String b = summary(T2, "2024-01-01T00:00:00Z", "completed", false);
        program(
                name,
                List.of(
                        ok(
                                "/v2/transcript",
                                "limit=2",
                                list(older(T3), origin() + "/credential-trap", a)),
                        ok(
                                "/v2/transcript",
                                "before_id=" + T3 + "&limit=2",
                                list(older(NEXT), older(T1))),
                        ok(
                                "/v2/transcript",
                                "before_id=" + NEXT + "&limit=2",
                                list(null, older(T3), b))));
        success(run(ONE, new Settings(name, "transcripts")));
        assertRows(
                name,
                Map.of(
                        key("transcripts", T1),
                        expected(T1, a),
                        key("transcripts", T2),
                        expected(T2, b)));
        assertEquals(
                List.of(
                        "/v2/transcript?limit=2",
                        "/v2/transcript?before_id=" + T3 + "&limit=2",
                        "/v2/transcript?before_id=" + NEXT + "&limit=2"),
                requests(name));
    }

    @Test
    void includesBothWindowEndpointsAndTreatsZoneLessProviderTimesAsUtc() throws Exception {
        String name = "time-window";
        String lower = summary(T1, "2024-01-01T00:00:00", "queued", false);
        String upper = summary(T2, "2024-01-02T00:00:00.000000Z", "error", false);
        String old = summary(T3, "2023-12-31T23:59:59.999999", "completed", false);
        String future = summary(NEXT, "2024-01-02T00:00:00.000001Z", "completed", false);
        program(
                name,
                List.of(
                        ok("/v2/transcript", "limit=2", list(older(T3), null, future, upper)),
                        ok(
                                "/v2/transcript",
                                "before_id=" + T3 + "&limit=2",
                                list(null, null, lower, old))));
        success(run(ONE, new Settings(name, "transcripts")));
        assertRows(
                name,
                Map.of(
                        key("transcripts", T1),
                        expected(T1, lower),
                        key("transcripts", T2),
                        expected(T2, upper)));
        assertEquals(2, requests(name).size());
    }

    @Test
    void keepsDeletedAndUnfinishedSummariesWithoutRequestingTheirChildren() throws Exception {
        String name = "ineligible-parents";
        String queued = summary(T1, "2024-01-01T01:00:00Z", "queued", false);
        String deleted = summary(T2, "2024-01-01T00:00:00Z", "completed", true);
        program(name, List.of(ok("/v2/transcript", "limit=2", list(null, null, queued, deleted))));
        success(run(ALL, new Settings(name, "transcripts")));
        assertRows(
                name,
                Map.of(
                        key("transcripts", T1),
                        expected(T1, queued),
                        key("transcripts", T2),
                        expected(T2, deleted)));
        assertEquals(1, requests(name).size());
    }

    @Test
    void exportsReadyAudioMetadataAndRejectsUndocumentedStatusesWithoutDownloadingAudio()
            throws Exception {
        for (boolean ready : List.of(false, true)) {
            String name = "redacted-" + ready;
            String parent = summary(T1, "2024-01-01T12:00:00Z", "completed", false);
            String metadata = redacted(ready);
            program(
                    name,
                    List.of(
                            ok("/v2/transcript", "limit=2", list(null, null, parent)),
                            ok(detail(T1), "", detailRecord(T1, true)),
                            ok(child(T1, "redacted-audio"), "", metadata)));
            Container.ExecResult result = run(ONE, new Settings(name, "redacted_audio"));
            if (ready) {
                success(result);
                assertRows(name, Map.of(key("redacted_audio", T1), expected(T1, metadata)));
            } else {
                failure(result, "redacted audio response lacks ready metadata");
                assertRows(name, Map.of());
            }
            assertEquals(3, requests(name).size());
        }
    }

    @Test
    void reportsUnknownChildFailuresAndRejectsConflictingParentIdentities() throws Exception {
        String parent = summary(T1, "2024-01-01T12:00:00Z", "completed", false);
        for (int status : new int[] {401, 403, 404}) {
            String name = "child-http-" + status;
            program(
                    name,
                    List.of(
                            ok("/v2/transcript", "limit=2", list(null, null, parent)),
                            step(child(T1, "sentences"), "", status, "fixed", "", privateError())));
            failure(run(ONE, new Settings(name, "transcript_sentences")), "HTTP " + status);
            assertRows(name, Map.of());
            assertEquals(2, requests(name).size());
        }
        String name = "wrong-parent";
        program(
                name,
                List.of(
                        ok("/v2/transcript", "limit=2", list(null, null, parent)),
                        ok(
                                child(T1, "paragraphs"),
                                "",
                                children(T2, "paragraphs", "{\"text\":\"wrong parent\"}"))));
        failure(run(ONE, new Settings(name, "paragraphs")), "AssemblyAI");
        assertRows(name, Map.of());
    }

    @Test
    void reportsUnknownRedactedAudioNotFoundWithoutTreatingItAsDisabled() throws Exception {
        String name = "redacted-not-found";
        String parent = summary(T1, "2024-01-01T12:00:00Z", "completed", false);
        program(
                name,
                List.of(
                        ok("/v2/transcript", "limit=2", list(null, null, parent)),
                        ok(detail(T1), "", detailRecord(T1, true)),
                        step(child(T1, "redacted-audio"), "", 404, "fixed", "", privateError())));
        failure(run(ONE, new Settings(name, "redacted_audio")), "HTTP 404");
        assertRows(name, Map.of());
        assertEquals(3, requests(name).size());
    }

    @Test
    void rejectsForeignMalformedAndRepeatedContinuationWithoutSendingCredentials()
            throws Exception {
        for (String cursor :
                List.of(
                        "https://foreign.invalid/v2/transcript?limit=2&before_id=" + T1,
                        origin() + "/other?limit=2&before_id=" + T1,
                        origin() + "/v2/transcript?limit=3&before_id=" + T1,
                        origin() + "/v2/transcript?limit=2&before_id=" + T1 + "&before_id=" + T2,
                        origin() + "/v2/transcript?limit=2&before_id=" + T1 + "#secret")) {
            String name = "unsafe-cursor-" + jobId();
            program(name, List.of(ok("/v2/transcript", "limit=2", list(cursor, null))));
            failure(run(ONE, new Settings(name, "transcripts")), "AssemblyAI");
            assertEquals(1, requests(name).size());
            assertRows(name, Map.of());
        }
        String name = "cursor-cycle";
        program(
                name,
                List.of(
                        ok("/v2/transcript", "limit=2", list(older(T1), null)),
                        ok(
                                "/v2/transcript",
                                "before_id=" + T1 + "&limit=2",
                                list(older(T1), null))));
        failure(run(ONE, new Settings(name, "transcripts")), "AssemblyAI");
        assertEquals(2, requests(name).size());
    }

    @Test
    void capsEndlessPageTraversalAndRejectsContradictoryPageDetails() throws Exception {
        String name = "page-limit";
        program(
                name,
                List.of(
                        ok("/v2/transcript", "limit=2", list(older(T1), null)),
                        ok(
                                "/v2/transcript",
                                "before_id=" + T1 + "&limit=2",
                                list(older(T2), null))));
        Settings bounded = new Settings(name, "transcripts");
        bounded.maxPages = 2;
        failure(run(ONE, bounded), "AssemblyAI");
        assertEquals(2, requests(name).size());
        name = "bad-count";
        program(
                name,
                List.of(
                        ok(
                                "/v2/transcript",
                                "limit=2",
                                "{\"transcripts\":[],\"page_details\":{\"limit\":2,\"result_count\":1}}")));
        failure(run(ONE, new Settings(name, "transcripts")), "AssemblyAI");
        assertRows(name, Map.of());
    }

    @Test
    void rejectsInvalidJsonAndMissingCreatedTimestamps() throws Exception {
        for (String body :
                List.of(
                        "[]",
                        "{\"transcripts\":{}}",
                        "{\"transcripts\":[null]}",
                        "{\"transcripts\":[],\"transcripts\":[]}",
                        "{\"transcripts\":[]} trailing",
                        "{\"transcripts\":[{\"id\":\""
                                + T1
                                + "\",\"status\":\"completed\"}],"
                                + "\"page_details\":{\"limit\":2,\"result_count\":1}}")) {
            String name = "bad-json-" + jobId();
            program(name, List.of(ok("/v2/transcript", "limit=2", body)));
            failure(run(ONE, new Settings(name, "transcripts")), "AssemblyAI");
            assertRows(name, Map.of());
        }
    }

    @Test
    void honorsRetryAfterWithinOneBoundedRetryBudget() throws Exception {
        String name = "retry-success";
        String parent = summary(T1, "2024-01-01T12:00:00Z", "completed", false);
        program(
                name,
                List.of(
                        step("/v2/transcript", "limit=2", 429, "fixed", "1", ""),
                        step("/v2/transcript", "limit=2", 503, "fixed", "0", ""),
                        ok("/v2/transcript", "limit=2", list(null, null, parent))));
        success(run(ONE, new Settings(name, "transcripts")));
        assertRows(name, Map.of(key("transcripts", T1), expected(T1, parent)));
        List<String> times = engine.lines(name + ".times");
        assertEquals(3, times.size());
        assertTrue(Long.parseLong(times.get(1)) - Long.parseLong(times.get(0)) >= 950L);
        name = "retry-exhausted";
        program(
                name,
                Collections.nCopies(3, step("/v2/transcript", "limit=2", 429, "fixed", "0", "")));
        failure(run(ONE, new Settings(name, "transcripts")), "AssemblyAI");
        assertEquals(3, requests(name).size());
        name = "retry-beyond-deadline";
        program(name, List.of(step("/v2/transcript", "limit=2", 429, "fixed", "120", "")));
        Settings deadline = new Settings(name, "transcripts");
        deadline.deadline = 1500;
        deadline.timeout = 1000;
        failure(run(ONE, deadline), "AssemblyAI");
        assertEquals(1, requests(name).size());
    }

    @Test
    void rejectsHttpAndProviderErrorsWithoutLeakingCredentialsOrResponseText() throws Exception {
        for (int status : new int[] {200, 400, 401, 403, 404}) {
            String name = "provider-error-" + status;
            program(
                    name,
                    List.of(
                            step(
                                    "/v2/transcript",
                                    "limit=2",
                                    status,
                                    "fixed",
                                    "",
                                    privateError())));
            failure(
                    run(ONE, new Settings(name, "transcripts")),
                    status == 200 ? "provider returned an error response" : "HTTP " + status);
            assertRows(name, Map.of());
            assertEquals(1, requests(name).size());
        }
    }

    @Test
    void decodesGzipAndCapsDecodedBytesBeforePublishingRows() throws Exception {
        String parent = summary(T1, "2024-01-01T12:00:00Z", "completed", false);
        String name = "gzip";
        program(
                name,
                List.of(
                        step(
                                "/v2/transcript",
                                "limit=2",
                                200,
                                "gzip",
                                "",
                                list(null, null, parent))));
        success(run(ONE, new Settings(name, "transcripts")));
        assertRows(name, Map.of(key("transcripts", T1), expected(T1, parent)));
        for (String mode : List.of("fixed", "gzip")) {
            name = "oversized-" + mode;
            String huge =
                    parent.substring(0, parent.length() - 1)
                            + ",\"padding\":\""
                            + "x".repeat(16000)
                            + "\"}";
            program(
                    name,
                    List.of(
                            step(
                                    "/v2/transcript",
                                    "limit=2",
                                    200,
                                    mode,
                                    "",
                                    list(null, null, huge))));
            Settings bounded = new Settings(name, "transcripts");
            bounded.bytes = 1024;
            failure(run(ONE, bounded), "max_response_bytes");
            assertRows(name, Map.of());
        }
    }

    @Test
    void rejectsRedirectsUntrustedCertificatesAndHostnameMismatch() throws Exception {
        String name = "redirect";
        program(name, List.of(step("/v2/transcript", "limit=2", 302, "redirect", "", "")));
        failure(run(ONE, new Settings(name, "transcripts")), "HTTP 302");
        assertEquals(1, requests(name).size());
        for (String endpoint :
                List.of(
                        "https://" + AssemblyAIPeer.HOST + ":" + AssemblyAIPeer.UNTRUSTED_PORT,
                        "https://localhost:" + AssemblyAIPeer.PORT)) {
            name = "bad-tls-" + jobId();
            program(name, List.of(ok("/v2/transcript", "limit=2", list(null, null))));
            Settings tls = new Settings(name, "transcripts");
            tls.endpoint = endpoint;
            tls.retries = 0;
            failure(run(ONE, tls), "HTTPS certificate verification or negotiation failed");
            assertTrue(requests(name).isEmpty());
        }
    }

    @Test
    void closesTricklingResponsesAtDeadlineAndOnEngineCancellation() throws Exception {
        String name = "deadline-stream";
        program(name, List.of(step("/v2/transcript", "limit=2", 200, "stream", "", "")));
        Settings limit = new Settings(name, "transcripts");
        limit.deadline = 1500;
        limit.timeout = 1000;
        limit.retries = 0;
        failure(run(ONE, limit), "AssemblyAI");
        awaitDisconnect(name);
        long started = Long.parseLong(engine.lines(name + ".started").get(0));
        long ended = Long.parseLong(engine.lines(name + ".disconnect").get(0));
        assertTrue(ended - started < 8000L);
        name = "cancel-stream";
        program(name, List.of(step("/v2/transcript", "limit=2", 200, "stream", "", "")));
        Settings cancel = held(name, "transcripts");
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
    void restoresTheOlderPageWithoutRepeatingCommittedTranscriptSummaries() throws Exception {
        String name = "resume-page";
        String a = summary(T1, "2024-01-01T12:00:00Z", "completed", false);
        String b = summary(T2, "2024-01-01T00:00:00Z", "completed", false);
        String query = "before_id=" + T3 + "&limit=2";
        program(
                name,
                List.of(
                        ok("/v2/transcript", "limit=2", list(older(T3), null, a)),
                        step("/v2/transcript", query, 200, "hold", "", list(null, null, b)),
                        ok("/v2/transcript", query, list(null, null, b))));
        Settings options = held(name, "transcripts");
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
                    Map.of(
                            key("transcripts", T1),
                            expected(T1, a),
                            key("transcripts", T2),
                            expected(T2, b)));
            assertEquals(
                    List.of(
                            "/v2/transcript?limit=2",
                            "/v2/transcript?" + query,
                            "/v2/transcript?" + query),
                    requests(name));
        } finally {
            engine.touch(name + ".release");
            stopIfRunning(job, running);
        }
    }

    @Test
    void restoresAChildInsideItsOwnParentWithoutRepeatingAnEarlierParent() throws Exception {
        String name = "resume-parent";
        String a = summary(T1, "2024-01-01T12:00:00Z", "completed", false);
        String b = summary(T2, "2024-01-01T00:00:00Z", "completed", false);
        String page = list(null, null, a, b);
        String first = "{\"text\":\"parent one\",\"start\":0,\"end\":1}";
        String second = "{\"text\":\"parent two\",\"start\":0,\"end\":2}";
        program(
                name,
                List.of(
                        ok("/v2/transcript", "limit=2", page),
                        ok(child(T1, "sentences"), "", children(T1, "sentences", first)),
                        ok(child(T1, "paragraphs"), "", children(T1, "paragraphs")),
                        ok(detail(T1), "", detailRecord(T1, false)),
                        step(
                                child(T2, "sentences"),
                                "",
                                200,
                                "hold",
                                "",
                                children(T2, "sentences", second)),
                        ok("/v2/transcript", "limit=2", page),
                        ok(child(T2, "sentences"), "", children(T2, "sentences", second)),
                        ok(child(T2, "paragraphs"), "", children(T2, "paragraphs")),
                        ok(detail(T2), "", detailRecord(T2, false))));
        Settings options = held(name, "transcripts");
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(ALL, job, options.variables()));
        try {
            awaitRequests(name, 5);
            savepoint(job, running);
            engine.touch(name + ".release");
            success(engine.restoreJob(ALL, job, options.variables()));
            assertRows(
                    name,
                    Map.of(
                            key("transcripts", T1),
                            expected(T1, a),
                            key("transcripts", T2),
                            expected(T2, b),
                            key("transcript_sentences", T1 + ":1"),
                            expected(T1, first),
                            key("transcript_sentences", T2 + ":1"),
                            expected(T2, second)));
            assertEquals(9, requests(name).size());
        } finally {
            engine.touch(name + ".release");
            stopIfRunning(job, running);
        }
    }

    @Test
    void restoresWhileReceivingAResponseWithoutPublishingPartialJson() throws Exception {
        String name = "resume-response";
        String parent = summary(T1, "2024-01-01T12:00:00Z", "completed", false);
        String body = list(null, null, parent);
        program(
                name,
                List.of(
                        step("/v2/transcript", "limit=2", 200, "partial", "", body),
                        ok("/v2/transcript", "limit=2", body)));
        Settings options = held(name, "transcripts");
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
            assertRows(name, Map.of(key("transcripts", T1), expected(T1, parent)));
            assertEquals(Collections.nCopies(2, "/v2/transcript?limit=2"), requests(name));
        } finally {
            engine.touch(name + ".release");
            stopIfRunning(job, running);
        }
    }

    @Test
    void refusesAChangedRegionOrWindowBeforeRestoringProviderTraffic() throws Exception {
        String name = "resume-scope";
        program(
                name,
                List.of(step("/v2/transcript", "limit=2", 200, "hold", "", list(null, null))));
        Settings options = held(name, "transcripts");
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(ONE, job, options.variables()));
        try {
            awaitRequests(name, 1);
            savepoint(job, running);
            engine.touch(name + ".release");
            options.region = "EU";
            failure(engine.restoreJob(ONE, job, options.variables()), "checkpoint");
            assertEquals(1, requests(name).size());
            assertRows(name, Map.of());
        } finally {
            engine.touch(name + ".release");
            stopIfRunning(job, running);
        }
    }

    @Test
    void restoresTerminalSourceWithoutRepeatingProviderRequests() throws Exception {
        String name = "resume-terminal";
        String parent = summary(T1, "2024-01-01T12:00:00Z", "completed", false);
        program(name, List.of(ok("/v2/transcript", "limit=2", list(null, null, parent))));
        Settings options = new Settings(name, "transcripts");
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
                assertEquals(1, requests(name).size());
                assertRows(name, Map.of(key("transcripts", T1), expected(T1, parent)));
            } finally {
                stopIfRunning(job, restored);
            }
        } finally {
            stopIfRunning(job, running);
        }
    }

    private void assertRows(String name, Map<String, List<String>> expected) throws Exception {
        Map<String, JsonNode> actual = new HashMap<>();
        for (String line : engine.rows(name)) {
            JsonNode row = JSON.readTree(line);
            assertEquals(Set.of("resource", "id", "transcript_id", "record"), fieldNames(row));
            String id = key(row.path("resource").asText(), row.path("id").asText());
            assertNull(actual.put(id, row), "duplicate committed record");
        }
        assertEquals(expected.keySet(), actual.keySet());
        for (Map.Entry<String, List<String>> entry : expected.entrySet()) {
            JsonNode row = actual.get(entry.getKey());
            assertEquals(entry.getValue().get(0), row.path("transcript_id").asText());
            String record = row.path("record").asText();
            assertEquals(JSON.readTree(entry.getValue().get(1)), JSON.readTree(record));
            if (entry.getValue().get(1).contains("9223372036854775808")) {
                assertTrue(record.contains("9223372036854775808"));
                assertTrue(record.contains("123456789012345678.123456789"));
            }
        }
    }

    private static Set<String> fieldNames(JsonNode row) {
        Set<String> names = new HashSet<>();
        row.fieldNames().forEachRemaining(names::add);
        return names;
    }

    private static List<String> expected(String parent, String record) {
        return List.of(parent, record);
    }

    private static String key(String resource, String id) {
        return resource + "/" + id;
    }

    private static String origin() {
        return "https://" + AssemblyAIPeer.HOST + ":" + AssemblyAIPeer.PORT;
    }

    private static String older(String before) {
        return origin() + "/v2/transcript?limit=2&before_id=" + before;
    }

    private static String detail(String parent) {
        return "/v2/transcript/" + parent;
    }

    private static String child(String parent, String route) {
        return detail(parent) + "/" + route;
    }

    private static String summary(String id, String created, String status, boolean deleted) {
        return "{\"id\":\""
                + id
                + "\",\"created\":\""
                + created
                + "\",\"status\":\""
                + status
                + "\",\"audio_url\":\""
                + (deleted ? "http://deleted_by_user" : "https://audio.invalid/never-download")
                + "\""
                + EXTRA
                + "}";
    }

    private static String list(String previous, String next, String... rows) {
        return "{\"page_details\":{\"limit\":2,\"result_count\":"
                + rows.length
                + ",\"prev_url\":"
                + (previous == null ? "null" : "\"" + previous + "\"")
                + ",\"next_url\":"
                + (next == null ? "null" : "\"" + next + "\"")
                + "},\"transcripts\":["
                + String.join(",", rows)
                + "]}";
    }

    private static String children(String parent, String route, String... rows) {
        return "{\"id\":\"" + parent + "\",\"" + route + "\":[" + String.join(",", rows) + "]}";
    }

    private static String detailRecord(String parent, boolean redact) {
        return "{\"id\":\""
                + parent
                + "\",\"status\":\"completed\",\"audio_url\":\"https://audio.invalid/not-fetched\","
                + "\"redact_pii_audio\":"
                + redact
                + "}";
    }

    private static String redacted(boolean ready) {
        return "{\"status\":\""
                + (ready ? "redacted_audio_ready" : "unknown_future_status")
                + "\",\"redacted_audio_url\":\"https://signed-audio.invalid/never-download?Signature=private-value\"}";
    }

    private static Settings held(String name, String resource) {
        Settings settings = new Settings(name, resource);
        settings.timeout = 180000;
        settings.deadline = 240000;
        return settings;
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

    private static String privateError() {
        return "{\"error\":\"private-provider-message " + AssemblyAIPeer.TOKEN + "\"}";
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
        assertFalse(text(result).contains("synthetic-assemblyai"));
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
                                "CN=" + AssemblyAIPeer.HOST,
                                "-ext",
                                "SAN=dns:" + AssemblyAIPeer.HOST,
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                file.toString(),
                                "-storepass",
                                AssemblyAIPeer.PASSWORD,
                                "-keypass",
                                AssemblyAIPeer.PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, process.waitFor(), output);
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(file)) {
            store.load(input, AssemblyAIPeer.PASSWORD.toCharArray());
        }
        return store;
    }

    /**
     * Keeps the transcript visibility window fixed across engine submission and restore.
     *
     * <p>The fixture origin and certificate are explicit and do not change JVM trust settings.
     */
    private static final class Settings {
        private final String name;
        private final String resource;
        private String endpoint = origin();
        private String region = "US";
        private String start = "2024-01-01T00:00:00Z";
        private String end = "2024-01-02T00:00:00Z";
        private int bytes = 4194304;
        private int maxPages = 1000;
        private int retries = 2;
        private int timeout = 30000;
        private int deadline = 120000;
        private int pageSize = 2;

        private Settings(String name, String resource) {
            this.name = name;
            this.resource = resource;
        }

        private String[] variables() {
            return new String[] {
                "api.output=" + name,
                "api.resource=" + resource,
                "api.token=" + AssemblyAIPeer.TOKEN,
                "api.origin=" + endpoint,
                "api.trust=" + trust,
                "api.region=" + region,
                "api.start=" + start,
                "api.end=" + end,
                "api.page_size=" + pageSize,
                "api.max_pages=" + maxPages,
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
        private static final String ROOT = "/tmp/assemblyai-peer";
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
            container.withExtraHost(AssemblyAIPeer.HOST, "127.0.0.1");
            for (String name : List.of("trusted.p12", "untrusted.p12", "trusted.pem")) {
                container.withCopyFileToContainer(
                        MountableFile.forHostPath(fixtures.resolve(name)), ROOT + "/" + name);
            }
            try {
                String file = "AssemblyAIPeer.class";
                Path source = Path.of(AssemblyAIPeer.class.getResource(file).toURI());
                container.withCopyFileToContainer(
                        MountableFile.forHostPath(source),
                        ROOT
                                + "/classes/"
                                + AssemblyAIPeer.class.getPackage().getName().replace('.', '/')
                                + "/"
                                + file);
            } catch (URISyntaxException e) {
                throw new IOException("Cannot stage the Java HTTPS fixture", e);
            }
        }

        private Container.ExecResult runPeer() throws Exception {
            return server.execInContainer(
                    "java", "-cp", ROOT + "/classes", AssemblyAIPeer.class.getName(), ROOT);
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
            String path = "/tmp/seatunnel_mnt/assemblyai-" + name;
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
