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
package org.apache.seatunnel.e2e.connector.buzzsprout;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.buzzsprout.BuzzsproutRecoveryProbe;
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
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
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
import java.net.http.HttpResponse.BodyHandlers;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies native Buzzsprout exports through the real Java 11 Zeta engine and HTTPS.
 *
 * <p>Independent literal provider fixtures validate every request and compare committed full rows,
 * including their configured podcast context. Provider URLs and record fields are literal fixtures.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class BuzzsproutIT extends TestSuiteBase {
    private static final ObjectMapper JSON =
            new ObjectMapper()
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    private static final String START = "2024-01-01T00:00:00Z";
    private static final String END = "2024-01-02T00:00:00Z";
    private static final String PODCAST = "9007199254740993";
    private static final String EPISODES = "/api/" + PODCAST + "/episodes";
    private static final String EXTRA =
            ",\"extra\":{\"unicode\":\"数据🌏\",\"null\":null,\"array\":[false,null,[1,2]],\"integer\":9223372036854775808,\"decimal\":123456789012345678.12345678900}";
    private static String trust, wrongTrust, wrongNameTrust;
    @TempDir static Path directory;
    private final ExecutorService tasks = Executors.newCachedThreadPool();
    private Engine engine;
    private Future<Container.ExecResult> peer;
    private String active;

    @BeforeAll
    void startsOnlyOwnedTlsAndEngineServices() throws Exception {
        trust = identity("trusted", BuzzsproutPeer.HOST);
        wrongTrust = identity("untrusted", BuzzsproutPeer.HOST);
        wrongNameTrust = identity("wrong-name", "unrelated.invalid");
        engine = new Engine(directory);
        engine.startUp();
        peer = tasks.submit(engine::peer);
        Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> engine.exists("ready"));
    }

    @AfterEach
    void rejectsUnexpectedTrafficAndRestoresTheNormalCertificate() throws Exception {
        if (active != null) {
            assertFalse(engine.exists(active + ".invalid"), active);
        }
        selectIdentity("trusted");
    }

    @AfterAll
    void stopsOnlyOwnedServices() throws Exception {
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
    void exportsBothListsInConfiguredOrderAndPreservesAllJsonValues() throws Exception {
        String podcast =
                "{\"id\":"
                        + PODCAST
                        + ",\"title\":\"节目🌏\",\"rss_url\":\"https://buzzsprout-trap.invalid/feed\""
                        + EXTRA
                        + "}";
        String episode =
                episode(
                        "9223372036854775807",
                        "2024-01-01T08:00:00+08:00",
                        ",\"private\":true,\"inactive_at\":\"2023-01-01T00:00:00Z\",\"tags\":\"one,two\",\"media_type\":\"video\",\"audio_url\":\"https://buzzsprout-trap.invalid/audio\"");
        for (boolean reversed : List.of(false, true)) {
            Settings settings =
                    reversed
                            ? new Settings("ordered-episodes", "episodes", "podcasts")
                            : new Settings("ordered-podcasts");
            List<ObjectNode> steps =
                    reversed
                            ? List.of(
                                    get(EPISODES, array(episode)),
                                    get("/api/podcasts", array(podcast)))
                            : List.of(
                                    get("/api/podcasts", array(podcast)),
                                    get(EPISODES, array(episode)));
            program(settings.name, steps);
            success(run(settings));
            assertRows(
                    settings.name,
                    List.of(
                            expected("podcasts", PODCAST, PODCAST, null, podcast),
                            expected("episodes", "9223372036854775807", PODCAST, START, episode)));
            consumed(settings.name, 2);
            for (String row : engine.rows(settings.name)) {
                assertTrue(row.contains("123456789012345678.12345678900"));
            }
        }
    }

    @Test
    void publishes65SameTimestampEpisodesAcrossAllReaderChunks() throws Exception {
        List<String> records = new ArrayList<>();
        List<JsonNode> expected = new ArrayList<>();
        for (int index = 0; index < 65; index++) {
            String id = Integer.toString(1000 + index), record = episode(id, START, "");
            records.add(record);
            expected.add(expected("episodes", id, PODCAST, START, record));
        }
        Settings settings = new Settings("65-cohort", "episodes");
        program(settings.name, List.of(get(EPISODES, array(records.toArray(new String[0])))));
        success(run(settings));
        assertRows(settings.name, expected);
        consumed(settings.name, 1);
    }

    @Test
    void usesExactHalfOpenPublicationTimesWithoutFilteringPrivateOrInactiveEpisodes()
            throws Exception {
        String start = episode("1", START, ",\"private\":true"),
                last =
                        episode(
                                "2",
                                "2024-01-01T23:59:59.999999999Z",
                                ",\"inactive_at\":\"2024-01-01T01:00:00Z\"");
        Settings settings = new Settings("time-window", "episodes");
        program(
                settings.name,
                List.of(
                        get(
                                EPISODES,
                                array(
                                        episode("3", "2023-12-31T23:59:59.999999999Z", ""),
                                        start,
                                        last,
                                        episode("4", END, "")))));
        success(run(settings));
        assertRows(
                settings.name,
                List.of(
                        expected("episodes", "1", PODCAST, START, start),
                        expected(
                                "episodes", "2", PODCAST, "2024-01-01T23:59:59.999999999Z", last)));
        consumed(settings.name, 1);
    }

    @Test
    void appliesTheUndatedPolicyOnlyToMissingOrNullDates() throws Exception {
        String absent = "{\"id\":1,\"private\":true}",
                nullDate = "{\"id\":2,\"published_at\":null}";
        for (boolean include : List.of(false, true)) {
            Settings settings = new Settings("undated-" + include, "episodes");
            settings.undated = include;
            program(settings.name, List.of(get(EPISODES, array(absent, nullDate))));
            success(run(settings));
            assertRows(
                    settings.name,
                    include
                            ? List.of(
                                    expected("episodes", "1", PODCAST, null, absent),
                                    expected("episodes", "2", PODCAST, null, nullDate))
                            : List.of());
            consumed(settings.name, 1);
        }
        Settings invalid = new Settings("undated-invalid", "episodes");
        invalid.undated = true;
        fails(
                invalid,
                List.of(get(EPISODES, array("{\"id\":1,\"published_at\":\"not-a-date\"}"))),
                "Buzzsprout");
    }

    @Test
    void retainsFutureScheduledEpisodesWhenExplicitlyInsideTheConfiguredWindow() throws Exception {
        Settings settings = new Settings("scheduled", "episodes");
        settings.end = "2100-01-01T00:00:00Z";
        String scheduled = episode("1", "2099-01-01T00:00:00Z", ",\"private\":false");
        program(settings.name, List.of(get(EPISODES, array(scheduled))));
        success(run(settings));
        assertRows(
                settings.name,
                List.of(expected("episodes", "1", PODCAST, "2099-01-01T00:00:00Z", scheduled)));
        consumed(settings.name, 1);
    }

    @Test
    void treatsEmptyArraysAsCompleteAndDoesNotRequireEpisodesForPodcastOnlyJobs() throws Exception {
        for (String resource : List.of("podcasts", "episodes")) {
            Settings settings = new Settings("empty-" + resource, resource);
            program(
                    settings.name,
                    List.of(get(resource.equals("podcasts") ? "/api/podcasts" : EPISODES, "[]")));
            success(run(settings));
            assertRows(settings.name, List.of());
            consumed(settings.name, 1);
        }
    }

    @Test
    void rejectsPartialListMetadataForeignPodcastContextAndDuplicateIds() throws Exception {
        fails(
                new Settings("unexpected-link", "episodes"),
                List.of(get(EPISODES, "[]").put("Link", "<?page=2>; rel=next")),
                "Buzzsprout");
        String foreign = episode("1", START, ",\"podcast_id\":42");
        fails(
                new Settings("foreign-podcast", "episodes"),
                List.of(get(EPISODES, array(foreign))),
                "Buzzsprout");
        String duplicate = episode("1", START, "");
        fails(
                new Settings("duplicate-id", "episodes"),
                List.of(get(EPISODES, array(duplicate, duplicate))),
                "Buzzsprout");
    }

    @Test
    void rejectsMalformedShapesInvalidIdentitiesAndNonJsonMime() throws Exception {
        for (String body :
                List.of(
                        "{}",
                        "[null]",
                        "[false]",
                        "[{\"id\":0}]",
                        "[{\"id\":-1}]",
                        "[{\"id\":1.5}]",
                        "[{\"id\":\"1\"}]",
                        "[{\"id\":1,\"id\":2}]",
                        "[]{}")) {
            fails(
                    new Settings("shape-" + Math.abs(body.hashCode()), "podcasts"),
                    List.of(get("/api/podcasts", body)),
                    "Buzzsprout");
        }
        fails(
                new Settings("html-mime", "podcasts"),
                List.of(get("/api/podcasts", "[]").put("mime", "text/html")),
                "Buzzsprout");
    }

    @Test
    void failsOnPermissionsAndProviderErrorsWithoutPersistingTheirSecretBodies() throws Exception {
        for (int status : List.of(401, 403, 404, 429, 503)) {
            fails(
                    new Settings("status-" + status, "podcasts"),
                    List.of(
                            get("/api/podcasts", "private-provider-message " + BuzzsproutPeer.TOKEN)
                                    .put("status", status)),
                    "Buzzsprout");
        }
        fails(
                new Settings("logical-error", "podcasts"),
                List.of(
                        get(
                                "/api/podcasts",
                                "{\"error\":{\"code\":\"forbidden\",\"message\":\"private-provider-message\"}}")),
                "Buzzsprout");
    }

    @Test
    void retriesAHeaderless429WithAtLeastOneSecondDelayAndHonorsRetryAfter() throws Exception {
        for (String hint : List.of("none", "2", "1.25")) {
            Settings settings = new Settings("retry-" + hint, "podcasts");
            settings.attempts = 2;
            ObjectNode throttled =
                    get("/api/podcasts", "private-provider-message").put("status", 429);
            if (!hint.equals("none")) {
                throttled.put("Retry-After", hint);
            }
            program(settings.name, List.of(throttled, get("/api/podcasts", "[]")));
            success(run(settings));
            List<String> times = engine.lines(settings.name + ".times");
            assertEquals(2, times.size());
            assertTrue(
                    Long.parseLong(times.get(1)) - Long.parseLong(times.get(0))
                            >= (hint.equals("2") ? 2000L : hint.equals("1.25") ? 1250L : 1000L));
            assertRows(settings.name, List.of());
            consumed(settings.name, 2);
        }
        Settings invalid = new Settings("bad-retry-header", "podcasts");
        invalid.attempts = 2;
        fails(
                invalid,
                List.of(
                        get("/api/podcasts", "private-provider-message")
                                .put("status", 429)
                                .put("Retry-After", "broken")),
                "Buzzsprout");
    }

    @Test
    void enforcesTheAttemptBudgetForTransientGetRequests() throws Exception {
        Settings settings = new Settings("attempt-budget", "podcasts");
        settings.attempts = 2;
        ObjectNode unavailable =
                get("/api/podcasts", "private-provider-message")
                        .put("status", 503)
                        .put("Retry-After", "0");
        fails(settings, List.of(unavailable, unavailable.deepCopy()), "Buzzsprout");
    }

    @Test
    void verifiesTrustAndHostnameIndependentlyAndRejectsRedirects() throws Exception {
        Settings wrong = new Settings("wrong-trust", "podcasts");
        wrong.certificate = wrongTrust;
        fails(wrong, List.of(), "Buzzsprout");
        selectIdentity("wrong-name");
        Settings name = new Settings("wrong-name", "podcasts");
        name.certificate = wrongNameTrust;
        fails(name, List.of(), "Buzzsprout");
        selectIdentity("trusted");
        fails(
                new Settings("redirect", "podcasts"),
                List.of(get("/api/podcasts", "").put("status", 302).put("mode", "redirect")),
                "Buzzsprout");
    }

    @Test
    void boundsDecodedBytesAndAcceptsNegotiatedGzipButRejectsUnknownEncoding() throws Exception {
        Settings limit = new Settings("response-bytes", "podcasts");
        limit.bytes = 1024;
        fails(
                limit,
                List.of(
                        get(
                                "/api/podcasts",
                                "[{\"id\":1,\"title\":\"" + "x".repeat(2000) + "\"}]")),
                "Buzzsprout");
        String name = "gzip-success", podcast = "{\"id\":1,\"title\":\"数据🌏\"}";
        program(name, List.of(get("/api/podcasts", array(podcast)).put("mode", "gzip")));
        success(run(new Settings(name, "podcasts")));
        consumed(name, 1);
        assertRows(name, List.of(expected("podcasts", "1", "1", null, podcast)));
        Settings expanded = new Settings("gzip-expanded", "podcasts");
        expanded.bytes = 1024;
        fails(
                expanded,
                List.of(
                        get("/api/podcasts", "[{\"id\":1,\"title\":\"" + "x".repeat(8000) + "\"}]")
                                .put("mode", "gzip")),
                "max_response_bytes");
        fails(
                new Settings("unsupported-compression", "podcasts"),
                List.of(get("/api/podcasts", "[]").put("encoding", "br")),
                "Buzzsprout");
    }

    @Test
    void boundsTricklingBodiesAndCancelsTheActiveEngineRequest() throws Exception {
        Settings deadline = new Settings("deadline", "podcasts");
        deadline.timeout = 1500;
        fails(deadline, List.of(get("/api/podcasts", "[]").put("mode", "trickle")), "Buzzsprout");
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> engine.exists(deadline.name + ".disconnect"));
        Settings settings = new Settings("cancel", "podcasts");
        program(settings.name, List.of(get("/api/podcasts", "[]").put("mode", "trickle")));
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(
                        () -> engine.executeJob("/buzzsprout.conf", job, settings.variables()));
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> engine.exists(settings.name + ".started"));
            success(engine.cancelJob(job));
            running.get(30, TimeUnit.SECONDS);
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .until(() -> engine.exists(settings.name + ".disconnect"));
            consumed(settings.name, 1);
            assertRows(settings.name, List.of());
        } finally {
            if (!running.isDone()) {
                engine.cancelJob(job);
                running.cancel(true);
            }
        }
    }

    @Test
    void rejectsInvalidUtf8DepthAndConfigurationBeforePublishingRows() throws Exception {
        byte[] prefix = "[{\"id\":1,\"title\":\"".getBytes(StandardCharsets.UTF_8),
                suffix = "\"}]".getBytes(StandardCharsets.UTF_8);
        byte[] invalid = new byte[prefix.length + suffix.length + 2];
        System.arraycopy(prefix, 0, invalid, 0, prefix.length);
        invalid[prefix.length] = (byte) 0xc3;
        invalid[prefix.length + 1] = (byte) 0x28;
        System.arraycopy(suffix, 0, invalid, prefix.length + 2, suffix.length);
        fails(
                new Settings("invalid-utf8", "podcasts"),
                List.of(
                        get("/api/podcasts", "")
                                .put("bytes", Base64.getEncoder().encodeToString(invalid))),
                "UTF-8");
        fails(
                new Settings("deep", "podcasts"),
                List.of(
                        get(
                                "/api/podcasts",
                                "[{\"id\":1,\"value\":"
                                        + "[".repeat(90)
                                        + "null"
                                        + "]".repeat(90)
                                        + "}]")),
                "Buzzsprout");
        fails(new Settings("duplicate-selection", "podcasts", "podcasts"), List.of(), "resources");
        Settings attempts = new Settings("invalid-attempts", "podcasts");
        attempts.attempts = 11;
        fails(attempts, List.of(), "max_attempts");
        Settings reverse = new Settings("invalid-window", "episodes");
        reverse.end = "2023-01-01T00:00:00Z";
        fails(reverse, List.of(), "start_date");
    }

    @Test
    void restoresBetweenTheTwoListsWithoutRequestingOrDuplicatingTheCommittedPodcast()
            throws Exception {
        Settings settings = new Settings("between-lists");
        String podcast = "{\"id\":" + PODCAST + ",\"title\":\"podcast\"}",
                episode = episode("1", START, "");
        program(
                settings.name,
                List.of(
                        get("/api/podcasts", array(podcast)),
                        get(EPISODES, array(episode)).put("mode", "partial")));
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(
                        () -> engine.executeJob("/buzzsprout.conf", job, settings.variables()));
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> engine.exists(settings.name + ".started"));
            long baseline = engine.getCompletedCheckpointCount(job);
            Awaitility.await()
                    .atMost(35, TimeUnit.SECONDS)
                    .until(() -> engine.getCompletedCheckpointCount(job) > baseline);
            success(engine.savepointJob(job));
            running.get(30, TimeUnit.SECONDS);
        } finally {
            engine.touch(settings.name + ".release");
            if (!running.isDone()) {
                engine.cancelJob(job);
                running.cancel(true);
            }
        }
        consumed(settings.name, 2);
        program("between-restored", List.of(get(EPISODES, array(episode))));
        success(engine.restoreJob("/buzzsprout.conf", job, settings.variables()));
        consumed("between-restored", 1);
        assertRows(
                settings.name,
                List.of(
                        expected("podcasts", PODCAST, PODCAST, null, podcast),
                        expected("episodes", "1", PODCAST, START, episode)));
    }

    @Test
    void restoresATerminalPipelineWithoutNetworkOrDuplicateTransactionalFiles() throws Exception {
        Settings settings = new Settings("terminal-original", "podcasts");
        String podcast = "{\"id\":1,\"title\":\"podcast\"}";
        List<JsonNode> expected = List.of(expected("podcasts", "1", "1", null, podcast));
        program(settings.name, List.of(get("/api/podcasts", array(podcast))));
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(
                        () ->
                                engine.executeJob(
                                        "/buzzsprout-terminal.conf", job, settings.variables()));
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
        program("terminal-restored", List.of());
        Future<Container.ExecResult> resumed =
                tasks.submit(
                        () ->
                                engine.restoreJob(
                                        "/buzzsprout-terminal.conf", job, settings.variables()));
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> "RUNNING".equals(engine.getJobStatus(job)));
            long baseline = engine.completedAcrossPipelines(job);
            Awaitility.await()
                    .atMost(35, TimeUnit.SECONDS)
                    .until(() -> engine.completedAcrossPipelines(job) > baseline);
            assertRows(settings.name, expected);
            consumed("terminal-restored", 0);
        } finally {
            if (!resumed.isDone()) {
                engine.cancelJob(job);
            }
            resumed.get(30, TimeUnit.SECONDS);
        }
    }

    @Test
    void serializesTheExact32RowChunkAndRejectsChangesEvenToFilteredRecords() throws Exception {
        List<String> records = new ArrayList<>();
        for (int index = 0; index < 33; index++) {
            records.add(episode(Integer.toString(1000 + index), START, ""));
        }
        records.add(episode("2000", "2023-01-01T00:00:00Z", ""));
        String page = array(records.toArray(new String[0]));
        for (String mode : List.of("resume", "changed", "changed-filtered")) {
            String replay =
                    mode.equals("changed")
                            ? page.replace("1032", "2032")
                            : mode.equals("changed-filtered") ? page.replace("2000", "3000") : page;
            String name = "reader-" + mode;
            program(name, List.of(get(EPISODES, page), get(EPISODES, replay)));
            success(engine.probe(mode));
            consumed(name, 2);
        }
        program("reader-zero", List.of());
        success(engine.probe("zero"));
        consumed("reader-zero", 0);
    }

    private static String episode(String id, String published, String extra) {
        return "{\"id\":" + id + ",\"published_at\":\"" + published + "\"" + extra + EXTRA + "}";
    }

    private static String array(String... records) {
        return "[" + String.join(",", records) + "]";
    }

    private static ObjectNode get(String path, String response) {
        return JSON.createObjectNode().put("path", path).put("query", "").put("response", response);
    }

    private static JsonNode expected(
            String resource, String id, String podcast, String published, String payload)
            throws IOException {
        ObjectNode row =
                JSON.createObjectNode()
                        .put("resource", resource)
                        .put("record_id", id)
                        .put("podcast_id", podcast)
                        .put("published_at", published);
        row.set("payload", JSON.readTree(payload));
        return row;
    }

    private void assertRows(String name, List<JsonNode> expected) throws Exception {
        Map<JsonNode, Integer> actual = new HashMap<>(), wanted = new HashMap<>();
        for (String line : engine.rows(name)) {
            JsonNode parsed = JSON.readTree(line);
            assertTrue(parsed.isObject() && parsed.size() == 5, line);
            assertTrue(parsed.path("payload").isTextual(), line);
            ObjectNode row = (ObjectNode) parsed;
            row.set("payload", JSON.readTree(parsed.path("payload").textValue()));
            actual.merge(row, 1, Integer::sum);
        }
        expected.forEach(row -> wanted.merge(row, 1, Integer::sum));
        assertEquals(wanted, actual, name);
    }

    private void program(String name, List<ObjectNode> steps) throws Exception {
        ArrayNode script = JSON.createArrayNode();
        steps.forEach(script::add);
        Path file = Files.createTempFile(directory, "script-", ".json");
        Files.writeString(file, script.toString());
        engine.install(file, name + ".json");
        Path selected = Files.createTempFile(directory, "active-", ".txt");
        Files.writeString(selected, name);
        engine.install(selected, "active");
        active = name;
    }

    private void selectIdentity(String identity) throws Exception {
        if (identity.equals("trusted") && !engine.exists("identity")) {
            return;
        }
        Path selected = Files.createTempFile(directory, "identity-", ".txt");
        Files.writeString(selected, identity);
        engine.install(selected, "identity");
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> engine.lines("identity-ready").equals(List.of(identity)));
    }

    private void consumed(String name, int count) throws Exception {
        assertFalse(engine.exists(name + ".invalid"), name);
        assertEquals(count, engine.lines(name + ".requests").size(), name);
    }

    private Container.ExecResult run(Settings settings) throws Exception {
        String config =
                settings.resources.length == 1 && settings.resources[0].equals("podcasts")
                        ? "/buzzsprout-podcasts.conf"
                        : "/buzzsprout.conf";
        return engine.executeJob(config, jobId(), settings.variables());
    }

    private void fails(Settings settings, List<ObjectNode> steps, String reason) throws Exception {
        program(settings.name, steps);
        Container.ExecResult result = run(settings);
        String output = result.getStdout() + result.getStderr();
        assertNotEquals(0, result.getExitCode(), output);
        assertTrue(output.contains(reason), output);
        assertFalse(output.contains(BuzzsproutPeer.TOKEN), "API key leaked");
        assertFalse(output.contains("private-provider-message"), "provider payload leaked");
        consumed(settings.name, steps.size());
    }

    private static String jobId() {
        return Long.toString(
                ThreadLocalRandom.current().nextLong(1_000_000_000_000L, 9_000_000_000_000L));
    }

    private static void success(Container.ExecResult result) {
        assertEquals(0, result.getExitCode(), result.getStdout() + result.getStderr());
    }

    private static String identity(String name, String hostname) throws Exception {
        Path store = directory.resolve(name + ".p12");
        Process keytool =
                new ProcessBuilder(
                                Path.of(System.getProperty("java.home"), "bin", "keytool")
                                        .toString(),
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
                                "CN=" + hostname,
                                "-ext",
                                "SAN=dns:" + hostname,
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                store.toString(),
                                "-storepass",
                                BuzzsproutPeer.PASSWORD,
                                "-keypass",
                                BuzzsproutPeer.PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String output = new String(keytool.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, keytool.waitFor(), output);
        KeyStore keys = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(store)) {
            keys.load(input, BuzzsproutPeer.PASSWORD.toCharArray());
        }
        Files.writeString(
                directory.resolve(name + ".pem"),
                "-----BEGIN CERTIFICATE-----\n"
                        + Base64.getMimeEncoder(64, new byte[] {'\n'})
                                .encodeToString(keys.getCertificate("api").getEncoded())
                        + "\n-----END CERTIFICATE-----\n");
        return Engine.PEER + "/" + name + ".pem";
    }

    /**
     * Supplies isolated output and explicit finite list-acquisition budgets.
     *
     * <p>The default selection tests both documented lists in their configured order.
     */
    private static final class Settings {
        private final String name;
        private final String[] resources;
        private String certificate = trust, end = END;
        private boolean undated;
        private int attempts = 1, timeout = 120000, bytes = 8388608;

        private Settings(String name, String... resources) {
            this.name = name;
            this.resources =
                    resources.length == 0 ? new String[] {"podcasts", "episodes"} : resources;
        }

        private String[] variables() {
            ArrayNode selection = JSON.createArrayNode();
            for (String resource : resources) {
                selection.add(resource);
            }
            return new String[] {
                "api.output=" + name,
                "api.resources=" + selection,
                "api.token=" + BuzzsproutPeer.TOKEN,
                "api.endpoint=https://" + BuzzsproutPeer.HOST,
                "api.certificate=" + certificate,
                "api.podcast=" + PODCAST,
                "api.start=" + START,
                "api.end=" + end,
                "api.undated=" + undated,
                "api.attempts=" + attempts,
                "api.timeout=" + timeout,
                "api.bytes=" + bytes
            };
        }
    }

    /**
     * Hosts normal packaged plugins and independent Java TLS fixtures in one owned worker.
     *
     * <p>DNS and certificate trust overrides apply only to this integration-test container.
     */
    private static final class Engine extends SeaTunnelContainer {
        private static final String PEER = "/tmp/buzzsprout-peer";
        private final Path fixtures;
        private final HttpClient checkpoints =
                HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

        private Engine(Path fixtures) {
            this.fixtures = fixtures;
        }

        /**
         * Passes entry-point variables as argv rather than a logged shell command.
         *
         * <p>This also preserves reserved characters in test credentials and timestamp options.
         */
        @Override
        protected Container.ExecResult executeCommand(
                GenericContainer<?> container, List<String> command)
                throws IOException, InterruptedException {
            return container.execInContainer(command.toArray(new String[0]));
        }

        /**
         * Stages only the owned peer, certificates and existing shaded JSON libraries.
         *
         * <p>The production API alias resolves to loopback only inside this container.
         */
        @Override
        protected void executeExtraCommands(GenericContainer<?> container)
                throws IOException, InterruptedException {
            super.executeExtraCommands(container);
            container.withExtraHost(BuzzsproutPeer.HOST, "127.0.0.1");
            container.withExtraHost("buzzsprout-trap.invalid", "127.0.0.1");
            for (String name :
                    List.of(
                            "trusted.p12",
                            "wrong-name.p12",
                            "trusted.pem",
                            "untrusted.pem",
                            "wrong-name.pem")) {
                container.withCopyFileToContainer(
                        MountableFile.forHostPath(fixtures.resolve(name)), PEER + "/" + name);
            }
            try {
                stage(container, BuzzsproutPeer.class);
                stage(container, BuzzsproutRecoveryProbe.class);
                Set<Path> jars = new HashSet<>();
                for (Class<?> type :
                        List.of(ObjectMapper.class, JsonFactory.class, JsonProperty.class)) {
                    Path jar =
                            Path.of(
                                    type.getProtectionDomain()
                                            .getCodeSource()
                                            .getLocation()
                                            .toURI());
                    if (jars.add(jar)) {
                        container.withCopyFileToContainer(
                                MountableFile.forHostPath(jar),
                                PEER + "/libs/" + jar.getFileName());
                    }
                }
            } catch (URISyntaxException failure) {
                throw new IOException("Cannot stage the Java Buzzsprout fixture", failure);
            }
        }

        private static void stage(GenericContainer<?> container, Class<?> type)
                throws IOException, URISyntaxException {
            Path folder = Path.of(type.getResource("").toURI());
            try (DirectoryStream<Path> files =
                    Files.newDirectoryStream(folder, type.getSimpleName() + "*.class")) {
                for (Path file : files) {
                    container.withCopyFileToContainer(
                            MountableFile.forHostPath(file),
                            PEER
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
                    PEER + "/classes:" + PEER + "/libs/*",
                    BuzzsproutPeer.class.getName(),
                    PEER);
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
                    PEER
                            + "/classes:"
                            + PEER
                            + "/libs/*:/tmp/seatunnel/lib/*:/tmp/seatunnel/connectors/*",
                    BuzzsproutRecoveryProbe.class.getName(),
                    trust,
                    mode);
        }

        private void install(Path file, String name) {
            server.copyFileToContainer(MountableFile.forHostPath(file), PEER + "/" + name);
        }

        private void touch(String name) throws Exception {
            success(server.execInContainer("touch", PEER + "/" + name));
        }

        private boolean exists(String name) throws Exception {
            return server.execInContainer("test", "-f", PEER + "/" + name).getExitCode() == 0;
        }

        private List<String> lines(String name) throws Exception {
            if (!exists(name)) {
                return Collections.emptyList();
            }
            Container.ExecResult result = server.execInContainer("cat", PEER + "/" + name);
            success(result);
            return linesOf(result.getStdout());
        }

        private List<String> rows(String name) throws Exception {
            String path = "/tmp/seatunnel_mnt/buzzsprout-" + name;
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

        private static List<String> linesOf(String output) {
            List<String> result = new ArrayList<>();
            for (String line : output.split("\\R")) {
                if (!line.isBlank()) {
                    result.add(line);
                }
            }
            return result;
        }
    }
}
