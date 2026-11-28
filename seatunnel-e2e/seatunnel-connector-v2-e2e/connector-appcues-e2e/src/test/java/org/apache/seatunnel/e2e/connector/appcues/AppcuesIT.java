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

package org.apache.seatunnel.e2e.connector.appcues;

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
import java.time.Instant;
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
 * Uses the real Zeta engine and transactional LocalFile sink to check Appcues resource records. A
 * separate Java HTTPS peer validates the job's method, account path, request headers, and query.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AppcuesIT extends TestSuiteBase {
    private static final String API_URL =
            "https://" + AppcuesPeer.US + ":" + AppcuesPeer.TRUSTED_PORT;
    private static final String ALL = "/appcues-all.conf";
    private static final String ONE = "/appcues-one.conf";
    private static final String DEFAULT_END = "/appcues-default-end.conf";
    private static final String RESUME = "/appcues-resume.conf";
    private static final String START = "2024-01-02T00:00:00Z";
    private static final String POINT = "2024-01-02T04:15:06Z";
    private static final String END = "2024-01-03T00:00:00Z";
    private static final String EXTRA =
            ",\"bundle\":{\"#hidden\":\"测试🌏\",\"none\":null,"
                    + "\"exact\":123456789012345678.123456789,\"large\":9223372036854775808,"
                    + "\"array\":[{\"value\":false},null,[1,2]]}";
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
        char[] password = AppcuesPeer.PASSWORD.toCharArray();
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
    void readsEveryResourceWithItsExactOutputAndDocumentedTimestampFormat() throws Exception {
        String name = "all";
        String millis = millis(POINT);
        String[] resources = {
            "launchpads", "flows", "banners", "checklists", "pins", "tags", "segments"
        };
        String[] timestamps = {
            millis, jsonString(POINT), millis, millis, millis, jsonString(POINT), jsonString(POINT)
        };
        List<String> scripts = new ArrayList<>();
        List<String> expectedRequests = new ArrayList<>();
        Map<String, String> expected = new HashMap<>();
        for (int i = 0; i < resources.length; i++) {
            String resource = resources[i];
            String field =
                    "flows".equals(resource) || "checklists".equals(resource)
                            ? "created_at"
                            : "updated_at";
            String data = record(resource, field, timestamps[i], EXTRA);
            scripts.add(ok(resource, array(data)));
            expectedRequests.add(request(resource));
            expected.put(resource, data);
        }
        program(name, scripts);
        success(run(ALL, new Settings(name)));

        Map<String, JsonNode> actual = rows(name);
        assertEquals(expected.keySet(), actual.keySet());
        for (Map.Entry<String, String> entry : expected.entrySet()) {
            String resource = entry.getKey();
            JsonNode row = actual.get(resource);
            assertEquals(resource, row.path("resource").asText());
            assertEquals(POINT, row.path("cursor_time").asText());
            String data = row.path("data").asText();
            assertEquals(JSON.readTree(entry.getValue()), JSON.readTree(data));
            assertTrue(data.contains("123456789012345678.123456789"));
            assertTrue(data.contains("9223372036854775808"));
        }
        assertEquals(expectedRequests, requests(name));
    }

    @Test
    void usesHalfOpenBoundsAndOnlyTheDeclaredTimestampField() throws Exception {
        String name = "creation-window";
        String createdAtStart =
                record(
                        "start",
                        "created_at",
                        jsonString(START),
                        ",\"updated_at\":" + jsonString(END));
        String createdInside = record("inside", "created_at", millis(POINT), ",\"updated_at\":0");
        String createdBefore =
                record("before", "created_at", "0", ",\"updated_at\":" + jsonString(POINT));
        String createdAtEnd = record("end", "created_at", jsonString(END), "");
        String missing = "{\"id\":\"missing\",\"updated_at\":" + jsonString(POINT) + "}";
        String nullTime =
                record("null-time", "created_at", "null", ",\"updated_at\":" + jsonString(POINT));
        program(
                name,
                Collections.singletonList(
                        ok(
                                "flows",
                                array(
                                        createdAtEnd,
                                        createdInside,
                                        nullTime,
                                        missing,
                                        createdAtStart,
                                        createdBefore))));
        success(run(ONE, new Settings(name)));
        Map<String, JsonNode> rows = rows(name);
        assertEquals(2, rows.size());
        assertRecord(rows.get("start"), START, createdAtStart);
        assertRecord(rows.get("inside"), POINT, createdInside);

        name = "millisecond-window";
        String inside =
                record(
                        "selected-pin",
                        "updated_at",
                        jsonString(millis(POINT)),
                        ",\"created_at\":0");
        String upper =
                record("upper-pin", "updated_at", millis(END), ",\"created_at\":" + millis(POINT));
        program(name, Collections.singletonList(ok("pins", array(upper, inside))));
        Settings pins = new Settings(name);
        pins.resource = "pins";
        success(run(ONE, pins));
        rows = rows(name);
        assertEquals(1, rows.size());
        assertRecord(rows.get("selected-pin"), POINT, inside);

        name = "iso-fraction";
        String fractional =
                record(
                        "fractional-tag",
                        "updated_at",
                        jsonString("2024-01-02T04:15:06.123456Z"),
                        "");
        program(name, Collections.singletonList(ok("tags", array(fractional))));
        Settings tags = new Settings(name);
        tags.resource = "tags";
        success(run(ONE, tags));
        assertRecord(rows(name).get("fractional-tag"), "2024-01-02T04:15:06.123456Z", fractional);
    }

    @Test
    void validatesFullArraysAndRejectsBadTimestampsBeforeAnyOutput() throws Exception {
        String name = "malformed-cursor";
        program(
                name,
                Collections.singletonList(
                        ok("flows", array(flow("good"), record("bad", "created_at", "true", "")))));
        failure(run(ONE, new Settings(name)), "invalid created_at timestamp");
        assertTrue(rows(name).isEmpty());

        name = "record-limit";
        program(
                name,
                Collections.singletonList(
                        ok(
                                "flows",
                                array(
                                        record("old-1", "created_at", "0", ""),
                                        record("old-2", "created_at", "1", "")))));
        Settings bounded = new Settings(name);
        bounded.maxRecords = 1;
        failure(run(ONE, bounded), "response exceeds max_records");

        name = "iso-type";
        program(
                name,
                Collections.singletonList(
                        ok("segments", array(record("wrong", "updated_at", millis(POINT), "")))));
        Settings segments = new Settings(name);
        segments.resource = "segments";
        failure(run(ONE, segments), "invalid updated_at timestamp");

        name = "bad-id";
        program(
                name,
                Collections.singletonList(
                        ok("flows", "[{\"created_at\":" + jsonString(POINT) + "}]")));
        failure(run(ONE, new Settings(name)), "expected a record id");
    }

    @Test
    void completesEmptyArraysAndAnEmptyWindowAndRejectsInvertedBounds() throws Exception {
        String name = "empty-array";
        program(name, Collections.singletonList(ok("flows", "[]")));
        success(run(ONE, new Settings(name)));
        assertTrue(rows(name).isEmpty());
        assertEquals(Collections.singletonList(request("flows")), requests(name));

        name = "empty-window";
        program(name, Collections.singletonList(ok("flows", array(flow("none")))));
        Settings empty = new Settings(name);
        empty.end = START;
        success(run(ONE, empty));
        assertTrue(rows(name).isEmpty());

        name = "inverted";
        program(name, Collections.singletonList(ok("flows", "[]")));
        Settings inverted = new Settings(name);
        inverted.end = "2024-01-01T23:59:59Z";
        failure(run(ONE, inverted), "end_date must not be earlier than start_date");
        assertTrue(requests(name).isEmpty());
    }

    @Test
    void refusesUnexpectedPaginationAndMalformedEnvelopes() throws Exception {
        for (String mode : Arrays.asList("next-link", "next-token", "partial-status")) {
            String name = "pagination-" + mode;
            int status = "partial-status".equals(mode) ? 206 : 200;
            program(
                    name,
                    Collections.singletonList(
                            step(AppcuesPeer.US, "flows", status, mode, "", array(flow("no")))));
            failure(
                    run(ONE, new Settings(name)),
                    "unexpected continuation for an unpaginated resource");
            assertEquals(Collections.singletonList(request("flows")), requests(name));
            assertTrue(rows(name).isEmpty());
        }

        String name = "wrapped-list";
        program(
                name,
                Collections.singletonList(ok("flows", "{\"data\":[],\"next\":\"unexpected\"}")));
        failure(run(ONE, new Settings(name)), "expected an unpaginated JSON array");

        name = "duplicate-keys";
        program(name, Collections.singletonList(ok("flows", "[{\"id\":\"a\",\"id\":\"b\"}]")));
        failure(run(ONE, new Settings(name)), "invalid, incomplete or unreadable JSON response");
    }

    @Test
    void treatsAuthenticationAndAuthorizationFailuresAsPermanentAndPrivate() throws Exception {
        for (int status : new int[] {401, 403}) {
            String name = "permanent-" + status;
            program(
                    name,
                    Collections.singletonList(
                            step(
                                    AppcuesPeer.US,
                                    "flows",
                                    status,
                                    "fixed",
                                    "",
                                    "{\"error\":\"private-provider-message "
                                            + AppcuesPeer.SECRET
                                            + "\"}")));
            failure(run(ONE, new Settings(name)), "HTTP " + status);
            assertEquals(Collections.singletonList(request("flows")), requests(name));
            assertTrue(rows(name).isEmpty());
        }
    }

    @Test
    void retriesTemporaryStatusWithinOneResourceBudget() throws Exception {
        String name = "retry-success";
        String data = flow("retried");
        program(
                name,
                Arrays.asList(
                        step(AppcuesPeer.US, "flows", 503, "fixed", "", ""),
                        step(AppcuesPeer.US, "flows", 429, "fixed", "1", ""),
                        ok("flows", array(data))));
        success(run(ONE, new Settings(name)));
        assertEquals(Collections.nCopies(3, request("flows")), requests(name));
        assertRecord(rows(name).get("retried"), POINT, data);

        name = "retry-limit";
        program(name, Collections.nCopies(2, step(AppcuesPeer.US, "flows", 429, "fixed", "1", "")));
        Settings finite = new Settings(name);
        finite.attempts = 2;
        failure(run(ONE, finite), "retry limit reached after HTTP 429");
        assertEquals(Collections.nCopies(2, request("flows")), requests(name));

        name = "retry-late";
        program(
                name,
                Collections.singletonList(step(AppcuesPeer.US, "flows", 429, "fixed", "120", "")));
        failure(run(ONE, new Settings(name)), "retry delay exceeds request deadline");
        assertEquals(Collections.singletonList(request("flows")), requests(name));
    }

    @Test
    void verifiesBothSupportedTlsOriginsAndRejectsUntrustedOrRedirectedPeers() throws Exception {
        String name = "eu";
        String data = flow("eu");
        program(
                name,
                Collections.singletonList(
                        step(AppcuesPeer.EU, "flows", 200, "fixed", "", array(data))));
        Settings eu = new Settings(name);
        eu.url = "https://" + AppcuesPeer.EU + ":" + AppcuesPeer.TRUSTED_PORT;
        success(run(ONE, eu));
        assertEquals(Collections.singletonList(AppcuesPeer.EU + "/flows"), requests(name));
        assertRecord(rows(name).get("eu"), POINT, data);

        name = "redirect";
        program(
                name,
                Collections.singletonList(step(AppcuesPeer.US, "flows", 302, "redirect", "", "")));
        failure(run(ONE, new Settings(name)), "HTTP 302");
        assertEquals(Collections.singletonList(request("flows")), requests(name));

        name = "untrusted";
        program(name, Collections.singletonList(ok("flows", "[]")));
        Settings badCertificate = new Settings(name);
        badCertificate.url = "https://" + AppcuesPeer.US + ":" + AppcuesPeer.UNTRUSTED_PORT;
        failure(run(ONE, badCertificate), "HTTPS or TLS exchange failed");
        assertTrue(requests(name).isEmpty());

        name = "wrong-origin";
        program(name, Collections.singletonList(ok("flows", "[]")));
        Settings wrongOrigin = new Settings(name);
        wrongOrigin.url = "http://" + AppcuesPeer.US;
        failure(run(ONE, wrongOrigin), "api_url must be an Appcues US or EU HTTPS origin");
        assertTrue(requests(name).isEmpty());

        name = "wrong-path";
        program(name, Collections.singletonList(ok("flows", "[]")));
        Settings wrongPath = new Settings(name);
        wrongPath.account = "abc%2fdef";
        failure(run(ONE, wrongPath), "account_id must be a single account path segment");
        assertTrue(requests(name).isEmpty());
    }

    @Test
    void readsGzipAndEnforcesSeparateWireAndDecodedByteLimits() throws Exception {
        String name = "gzip";
        String data = flow("gz");
        program(
                name,
                Collections.singletonList(
                        step(AppcuesPeer.US, "flows", 200, "gzip", "", array(data))));
        success(run(ONE, new Settings(name)));
        assertRecord(rows(name).get("gz"), POINT, data);

        String oversized =
                record(
                        "too-big",
                        "created_at",
                        jsonString(POINT),
                        ",\"oversize\":\"" + "x".repeat(8000) + "\"");
        for (String mode : Arrays.asList("fixed", "gzip")) {
            name = "oversize-" + mode;
            program(
                    name,
                    Collections.singletonList(
                            step(AppcuesPeer.US, "flows", 200, mode, "", array(oversized))));
            Settings bounded = new Settings(name);
            bounded.maxBytes = 1024;
            failure(run(ONE, bounded), "response exceeds max_response_bytes");
            assertTrue(rows(name).isEmpty());
        }
    }

    @Test
    void disconnectsSlowResponsesAtTheDeadlineAndOnEngineCancellation() throws Exception {
        String timeout = "trickle-deadline";
        program(
                timeout,
                Collections.singletonList(step(AppcuesPeer.US, "flows", 200, "slow", "", "")));
        Settings bounded = new Settings(timeout);
        bounded.deadline = 2000;
        failure(run(ONE, bounded), "request deadline exceeded");
        awaitDisconnect(timeout);
        assertEquals(Collections.singletonList(request("flows")), requests(timeout));

        String cancel = "trickle-cancel";
        program(
                cancel,
                Collections.singletonList(step(AppcuesPeer.US, "flows", 200, "slow", "", "")));
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
            assertEquals(Collections.singletonList(request("flows")), requests(cancel));
        } finally {
            if (!result.isDone()) {
                engine.cancelJob(job);
                result.cancel(true);
            }
        }
    }

    @Test
    void restoresBetweenResourcesWithoutFetchingTheCompletedResourceAgain() throws Exception {
        String name = "resume-resources";
        String flow = flow("before-savepoint");
        String tag = record("after-savepoint", "updated_at", jsonString(POINT), "");
        program(
                name,
                Arrays.asList(
                        ok("flows", array(flow)),
                        step(AppcuesPeer.US, "tags", 200, "hold", "", array(tag)),
                        ok("tags", array(tag))));
        Settings config = new Settings(name);
        config.deadline = 120000;
        String job = jobId();
        Future<Container.ExecResult> result =
                tasks.submit(() -> engine.executeJob(RESUME, job, config.variables()));
        try {
            awaitRequests(name, 2);
            savepoint(job, result);
            engine.touch(name + ".release");
            success(engine.restoreJob(RESUME, job, config.variables()));
            assertEquals(
                    Arrays.asList(request("flows"), request("tags"), request("tags")),
                    requests(name));
            Map<String, JsonNode> rows = rows(name);
            assertEquals(2, rows.size());
            assertRecord(rows.get("before-savepoint"), POINT, flow);
            assertRecord(rows.get("after-savepoint"), POINT, tag);
        } finally {
            if (!result.isDone()) {
                engine.cancelJob(job);
                result.cancel(true);
            }
        }
    }

    @Test
    void savesDuringAnArrayReadAndRestoresTheOriginalDefaultEnd() throws Exception {
        String name = "resume-array";
        String old = flow("inside-frozen-window");
        String initial = step(AppcuesPeer.US, "flows", 200, "partial", "", array(old));
        program(name, Collections.singletonList(initial));
        Settings config = new Settings(name);
        config.deadline = 120000;
        String job = jobId();
        Future<Container.ExecResult> result =
                tasks.submit(() -> engine.executeJob(DEFAULT_END, job, config.variables()));
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> engine.exists(name + ".partial"));
            savepoint(job, result);
            String late =
                    record(
                            "after-original-window",
                            "created_at",
                            jsonString(Instant.now().toString()),
                            "");
            installProgram(name, Arrays.asList(initial, ok("flows", array(old, late))));
            engine.touch(name + ".release");
            success(engine.restoreJob(DEFAULT_END, job, config.variables()));
            assertEquals(Collections.nCopies(2, request("flows")), requests(name));
            Map<String, JsonNode> rows = rows(name);
            assertEquals(1, rows.size());
            assertRecord(rows.get("inside-frozen-window"), POINT, old);
        } finally {
            if (!result.isDone()) {
                engine.cancelJob(job);
                result.cancel(true);
            }
        }
    }

    private void savepoint(String job, Future<Container.ExecResult> execute) throws Exception {
        long checkpoints = engine.getCompletedCheckpointCount(job);
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .until(() -> engine.getCompletedCheckpointCount(job) > checkpoints);
        success(engine.savepointJob(job));
        execute.get(30, TimeUnit.SECONDS);
    }

    private void awaitRequests(String name, int count) {
        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> requests(name).size() == count);
    }

    private void awaitDisconnect(String name) {
        Awaitility.await()
                .atMost(20, TimeUnit.SECONDS)
                .until(() -> engine.exists(name + ".disconnect"));
    }

    private static void assertRecord(JsonNode row, String normalizedTime, String data)
            throws IOException {
        assertEquals(normalizedTime, row.path("cursor_time").asText());
        assertEquals(JSON.readTree(data), JSON.readTree(row.path("data").asText()));
    }

    private Container.ExecResult run(String resource, Settings settings) throws Exception {
        return engine.executeJob(resource, jobId(), settings.variables());
    }

    private void program(String name, List<String> steps) throws Exception {
        validatesPeerRequests();
        installProgram(name, steps);
        Path activeFile = Files.createTempFile(directory, "active-", ".txt");
        Files.writeString(activeFile, name);
        engine.install(activeFile, "active");
        active = name;
    }

    private void installProgram(String name, List<String> steps) throws IOException {
        Path script = Files.createTempFile(directory, "steps-", ".txt");
        Files.write(script, steps, StandardCharsets.UTF_8);
        engine.install(script, name + ".program");
    }

    private List<String> requests(String name) throws Exception {
        return engine.lines(name + ".requests");
    }

    private Map<String, JsonNode> rows(String name) throws Exception {
        Map<String, JsonNode> rows = new HashMap<>();
        for (String line : engine.rows(name)) {
            JsonNode row = JSON.readTree(line);
            String id = row.path("id").asText();
            assertNull(rows.put(id, row), id);
        }
        return rows;
    }

    private static String step(
            String host, String resource, int status, String mode, String retry, String payload) {
        return String.join(
                "\t",
                host,
                resource,
                Integer.toString(status),
                mode,
                retry,
                Base64.getEncoder().encodeToString(payload.getBytes(StandardCharsets.UTF_8)));
    }

    private static String ok(String resource, String payload) {
        return step(AppcuesPeer.US, resource, 200, "fixed", "", payload);
    }

    private static String request(String resource) {
        return AppcuesPeer.US + "/" + resource;
    }

    private static String record(String id, String field, String timestamp, String extra) {
        return "{\"id\":" + jsonString(id) + ",\"" + field + "\":" + timestamp + extra + "}";
    }

    private static String flow(String id) {
        return record(id, "created_at", jsonString(POINT), "");
    }

    private static String array(String... records) {
        return "[" + String.join(",", records) + "]";
    }

    private static String jsonString(String literal) {
        return "\"" + literal + "\"";
    }

    private static String millis(String timestamp) {
        return Long.toString(Instant.parse(timestamp).toEpochMilli());
    }

    private static String jobId() {
        return Long.toString(
                ThreadLocalRandom.current().nextLong(1_000_000_000_000L, 9_000_000_000_000L));
    }

    private static String resultText(Container.ExecResult result) {
        return result.getStdout() + result.getStderr();
    }

    private static void success(Container.ExecResult result) {
        assertEquals(0, result.getExitCode(), () -> resultText(result));
    }

    private static void failure(Container.ExecResult result, String expected) {
        assertNotEquals(0, result.getExitCode(), () -> resultText(result));
        assertTrue(resultText(result).contains(expected), () -> resultText(result));
        assertFalse(resultText(result).contains(AppcuesPeer.KEY));
        assertFalse(resultText(result).contains(AppcuesPeer.SECRET));
        assertFalse(
                resultText(result)
                        .contains(AppcuesPeer.AUTHORIZATION.substring("Basic ".length())));
        assertFalse(resultText(result).contains("private-provider-message"));
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
                                "CN=" + AppcuesPeer.US,
                                "-ext",
                                "SAN=dns:" + AppcuesPeer.US + ",dns:" + AppcuesPeer.EU,
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                target.toString(),
                                "-storepass",
                                AppcuesPeer.PASSWORD,
                                "-keypass",
                                AppcuesPeer.PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String output = new String(crypto.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, crypto.waitFor(), output);
        KeyStore key = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(target)) {
            key.load(input, AppcuesPeer.PASSWORD.toCharArray());
        }
        return key;
    }

    /**
     * Maps independent E2E scenarios to ordinary SeaTunnel job configuration variables. The source
     * identity, security and parsing guardrails are exercised through the job launcher.
     */
    private static final class Settings {
        private final String name;
        private String url = API_URL;
        private String account = AppcuesPeer.ACCOUNT;
        private String start = START;
        private String end = END;
        private String resource = "flows";
        private int maxBytes = 16777216;
        private int maxRecords = 50000;
        private int deadline = 30000;
        private int attempts = 4;

        private Settings(String name) {
            this.name = name;
        }

        private String[] variables() {
            return new String[] {
                "app.user=" + AppcuesPeer.KEY,
                "app.password=" + AppcuesPeer.SECRET,
                "app.url=" + url,
                "app.account=" + account,
                "app.start=" + start,
                "app.end=" + end,
                "app.resource=" + resource,
                "app.output=" + name,
                "app.bytes=" + maxBytes,
                "app.records=" + maxRecords,
                "app.deadline=" + deadline,
                "app.attempts=" + attempts
            };
        }
    }

    /**
     * Runs the stock Java 11 engine with Appcues regional hostnames mapped to the test peer. The
     * JVM trust store includes only the trusted fixture identity.
     */
    private static final class Engine extends SeaTunnelContainer {
        private static final String ROOT = "/tmp/appcues-https-fixture";
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
                    + AppcuesPeer.PASSWORD;
        }

        @Override
        protected void executeExtraCommands(GenericContainer<?> container) throws IOException {
            container
                    .withExtraHost(AppcuesPeer.US, "127.0.0.1")
                    .withExtraHost(AppcuesPeer.EU, "127.0.0.1");
            for (String file : Arrays.asList("trusted.p12", "untrusted.p12", "trust.jks")) {
                container.withCopyFileToContainer(
                        MountableFile.forHostPath(fixtures.resolve(file)), ROOT + "/" + file);
            }
            String javaPackage = AppcuesPeer.class.getPackage().getName().replace('.', '/');
            for (String file : Arrays.asList("AppcuesPeer.class", "AppcuesPeer$Reply.class")) {
                try {
                    container.withCopyFileToContainer(
                            MountableFile.forHostPath(
                                    Paths.get(AppcuesPeer.class.getResource(file).toURI())),
                            ROOT + "/classes/" + javaPackage + "/" + file);
                } catch (URISyntaxException e) {
                    throw new IOException("Cannot stage the HTTPS provider fixture", e);
                }
            }
        }

        private Container.ExecResult runPeer() throws Exception {
            return server.execInContainer(
                    "java", "-cp", ROOT + "/classes", AppcuesPeer.class.getName(), ROOT);
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
            String dir = "/tmp/seatunnel_mnt/appcues-" + name;
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
