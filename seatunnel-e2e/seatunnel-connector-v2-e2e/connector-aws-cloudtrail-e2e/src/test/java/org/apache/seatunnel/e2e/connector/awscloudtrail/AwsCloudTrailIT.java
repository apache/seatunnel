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

package org.apache.seatunnel.e2e.connector.awscloudtrail;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
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
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
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
 * Runs CloudTrail in Java 11 Zeta and persists actual output with transactional LocalFile. The TLS
 * peer validates region, exact parameter types, credentials and every signed byte.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AwsCloudTrailIT extends TestSuiteBase {
    private static final String CONF = "/aws-cloudtrail.conf";
    private static final String ROOT = "/tmp/cloudtrail-https-fixture";
    private static final ObjectMapper JSON =
            new ObjectMapper()
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    private static final List<String> TRUSTED_NAMES =
            List.of(CloudTrailPeer.EAST, CloudTrailPeer.CHINA, CloudTrailPeer.GOV);
    private final Instant end =
            Instant.now().minus(1, ChronoUnit.HOURS).truncatedTo(ChronoUnit.SECONDS);
    private final Instant start = end.minus(2, ChronoUnit.HOURS);

    @TempDir static Path directory;
    private final ExecutorService tasks = Executors.newCachedThreadPool();
    private Engine engine;
    private Future<Container.ExecResult> peer;
    private String active;

    @BeforeAll
    void startFixture() throws Exception {
        KeyStore trusted = identity("trusted", TRUSTED_NAMES);
        identity("untrusted", List.of(CloudTrailPeer.UNTRUSTED));
        char[] password = CloudTrailPeer.PASSWORD.toCharArray();
        KeyStore trust = KeyStore.getInstance("JKS");
        trust.load(null, password);
        trust.setCertificateEntry("cloudtrail-fixture", trusted.getCertificate("fixture"));
        try (OutputStream stream = Files.newOutputStream(directory.resolve("trust.jks"))) {
            trust.store(stream, password);
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
    void readsFiftyEventsThenAnEmptyContinuationAndPreservesSameSecondDuplicates()
            throws Exception {
        Settings options = settings("all-pages");
        options.attribute = "Username";
        options.value = "研究, 服务_☀";
        ArrayNode first = JSON.createArrayNode();
        BigDecimal same =
                BigDecimal.valueOf(end.getEpochSecond() - 10).add(new BigDecimal("0.123456789"));
        String audit = " {\n\"message\":\"你好\",\"amount\":9007199254740993.123456789}\n";
        ObjectNode exact = event("shared-id", BigDecimal.valueOf(end.getEpochSecond()), audit);
        exact.put("Username", "研究员").put("ReadOnly", "true").put("AccessKeyId", "audit-access-id");
        exact.putArray("Resources")
                .addNull()
                .addObject()
                .put("ResourceName", "arn:aws:test/数据")
                .put("ResourceType", "table");
        exact.put("FutureMetric", new BigDecimal("9007199254740993.123456789"));
        first.add(exact);
        for (int i = 1; i < 50; i++) {
            first.add(event("event-" + i, same, "{\"sequence\":" + i + "}"));
        }
        ArrayNode last =
                JSON.createArrayNode()
                        .add(event("event-49", same, "provider delivered this ID again"))
                        .add(event("oldest", BigDecimal.valueOf(start.getEpochSecond()), null));
        program(
                options,
                step(options, null, 200, "gzip", result(first, "after-first")),
                step(
                        options,
                        "after-first",
                        200,
                        "fixed",
                        result(JSON.createArrayNode(), "after-empty")),
                step(options, "after-empty", 200, "fixed", result(last, null)));

        success(run(options));
        List<JsonNode> calls = requests(options);
        assertEquals(3, calls.size());
        assertSameParameters(calls, null, "after-first", "after-empty");
        assertPacing(calls);
        List<JsonNode> rows = rows(options);
        assertEquals(52, rows.size());
        JsonNode original =
                rows.stream()
                        .filter(row -> "shared-id".equals(row.path("event_id").asText()))
                        .findFirst()
                        .orElseThrow();
        assertEquals(audit, original.path("cloud_trail_event").asText());
        assertEquals(Long.toString(end.getEpochSecond()), original.path("event_time").asText());
        assertEquals("us-east-1", original.path("region").asText());
        assertEquals("management_events", original.path("resource").asText());
        JsonNode data = JSON.readTree(original.path("data").asText());
        assertEquals(JSON.readTree(exact.toString()), data);
        assertEquals(
                2,
                rows.stream()
                        .filter(row -> "event-49".equals(row.path("event_id").asText()))
                        .count());
        assertEquals(
                50,
                rows.stream()
                        .filter(row -> same.toPlainString().equals(row.path("event_time").asText()))
                        .count());
        JsonNode oldest =
                rows.stream()
                        .filter(row -> "oldest".equals(row.path("event_id").asText()))
                        .findFirst()
                        .orElseThrow();
        assertEquals(Long.toString(start.getEpochSecond()), oldest.path("event_time").asText());
        assertTrue(oldest.path("cloud_trail_event").isNull());
    }

    @Test
    void freezesDefaultUtcMidnightAndInitialCutoffThroughPagination() throws Exception {
        Settings options = settings("implicit-window");
        options.implicit = true;
        program(
                options,
                step(options, null, 200, "fixed", result(JSON.createArrayNode(), "more")),
                step(options, "more", 200, "fixed", result(JSON.createArrayNode(), null)));
        long before = Instant.now().getEpochSecond();
        success(run(options));
        long after = Instant.now().getEpochSecond();

        List<JsonNode> calls = requests(options);
        assertEquals(2, calls.size());
        assertSameParameters(calls, null, "more");
        JsonNode body = calls.get(0).path("request");
        long cutoff = body.path("EndTime").asLong();
        assertTrue(cutoff >= before && cutoff <= after);
        assertEquals(
                Instant.ofEpochSecond(cutoff).truncatedTo(ChronoUnit.DAYS).getEpochSecond(),
                body.path("StartTime").asLong());
        assertTrue(rows(options).isEmpty());
    }

    @Test
    void signsRegionalCommercialChinaAndGovLookupWithCorrectCredentialScope() throws Exception {
        Settings china = settings("china");
        china.region = "cn-north-1";
        china.host = CloudTrailPeer.CHINA;
        china.temporary = true;
        china.access = CloudTrailPeer.OTHER_ACCESS;
        china.pageSize = 1;
        ObjectNode entry = event("china-event", null, null);
        entry.remove("CloudTrailEvent");
        entry.putNull("Resources").putNull("Username");
        program(
                china,
                step(china, null, 200, "fixed", result(JSON.createArrayNode().add(entry), null)));

        success(run(china));
        List<JsonNode> got = rows(china);
        assertEquals(1, got.size());
        assertEquals("cn-north-1", got.get(0).path("region").asText());
        assertTrue(got.get(0).path("event_time").isNull());
        assertEquals(entry, JSON.readTree(got.get(0).path("data").asText()));
        assertTrue(requests(china).get(0).path("temporaryCredentials").asBoolean());

        Settings gov = settings("gov");
        gov.region = "us-gov-west-1";
        gov.host = CloudTrailPeer.GOV;
        program(gov, step(gov, null, 200, "fixed", result(JSON.createArrayNode(), null)));
        success(run(gov));
        assertFalse(requests(gov).get(0).path("temporaryCredentials").asBoolean());
        assertEquals(CloudTrailPeer.GOV, requests(gov).get(0).path("host").asText());
    }

    @Test
    void retriesDocumentedThrottleAndTransientStatusesOnTheIdenticalPage() throws Exception {
        Settings options = settings("retry");
        options.attempts = 4;
        ObjectNode single = event("retried", BigDecimal.valueOf(end.getEpochSecond()), "{}");
        program(
                options,
                step(options, null, 400, "fixed", message("ThrottlingException")),
                step(options, null, 429, "fixed", message("ignored")).put("retryAfter", "0"),
                step(options, null, 503, "fixed", message("ServiceUnavailable")),
                step(
                        options,
                        null,
                        200,
                        "fixed",
                        result(JSON.createArrayNode().add(single), null)));
        success(run(options));
        List<JsonNode> calls = requests(options);
        assertEquals(4, calls.size());
        assertSameParameters(calls, null, null, null, null);
        assertPacing(calls);
        assertEquals("retried", rows(options).get(0).path("event_id").asText());

        Settings exhausted = settings("attempts");
        exhausted.attempts = 2;
        program(
                exhausted,
                step(exhausted, null, 400, "fixed", "").put("errorType", "ThrottlingException:"),
                step(exhausted, null, 503, "fixed", message("ServiceUnavailable")));
        failure(run(exhausted), "retry limit reached after HTTP 503");
        assertEquals(2, requests(exhausted).size());
        assertTrue(rows(exhausted).isEmpty());

        Settings retryAfter = settings("retry-outside-budget");
        retryAfter.deadline = 2400;
        program(retryAfter, step(retryAfter, null, 429, "fixed", "").put("retryAfter", "300"));
        failure(run(retryAfter), "pacing or retry delay exceeds request deadline");
        assertEquals(1, requests(retryAfter).size());
    }

    @Test
    void failsPermanentAwsErrorsAndNeverResetsAnInvalidContinuation() throws Exception {
        Settings continuation = settings("expired-token");
        program(
                continuation,
                step(
                        continuation,
                        null,
                        200,
                        "fixed",
                        result(JSON.createArrayNode(), "opaque-expired")),
                step(
                        continuation,
                        "opaque-expired",
                        400,
                        "fixed",
                        message("InvalidNextTokenException")));
        failure(run(continuation), "HTTP 400 InvalidNextTokenException");
        assertSameParameters(requests(continuation), null, "opaque-expired");

        String[] codes = {
            "InvalidTimeRangeException",
            "InvalidLookupAttributesException",
            "SignatureDoesNotMatch",
            "AccessDeniedException",
            "ExpiredTokenException"
        };
        for (int i = 0; i < codes.length; i++) {
            Settings options = settings("permanent-" + i);
            int status = i == 3 ? 403 : 400;
            program(options, step(options, null, status, "fixed", message(codes[i])));
            failure(run(options), "HTTP " + status + " " + codes[i]);
            assertEquals(1, requests(options).size());
            assertTrue(rows(options).isEmpty());
        }
    }

    @Test
    void rejectsRetentionFutureAmbiguityAndUnsafeConfigurationBeforeRequest() throws Exception {
        Settings tooOld = settings("retention");
        tooOld.from = Instant.now().minus(91, ChronoUnit.DAYS).truncatedTo(ChronoUnit.SECONDS);
        program(tooOld);
        failure(run(tooOld), "outside the 90-day CloudTrail history");
        assertTrue(requests(tooOld).isEmpty());

        Settings future = settings("future");
        future.to = Instant.now().plus(1, ChronoUnit.DAYS).truncatedTo(ChronoUnit.SECONDS);
        program(future);
        failure(run(future), "end at or before the current time");
        assertTrue(requests(future).isEmpty());

        Settings filter = settings("filter");
        filter.attribute = "ResourceName";
        filter.value = "_".repeat(1001);
        program(filter);
        failure(run(filter), "weighted length limit");
        assertTrue(requests(filter).isEmpty());

        Settings unsafe = settings("unsafe-region");
        unsafe.region = "us-east-1.untrusted.invalid";
        program(unsafe);
        failure(run(unsafe), "not a supported CloudTrail control plane region");
        assertTrue(requests(unsafe).isEmpty());

        Settings readers = settings("parallelism");
        readers.parallelism = 2;
        program(readers);
        failure(run(readers), "source parallelism must be 1");
        assertTrue(requests(readers).isEmpty());
    }

    @Test
    void validatesPageStructureTypesOrderingAndFrozenBoundsBeforeOutput() throws Exception {
        Settings options = settings("page-template");
        ObjectNode valid = event("first", BigDecimal.valueOf(end.getEpochSecond() - 3), "{}");
        ObjectNode outside = event("outside", BigDecimal.valueOf(start.getEpochSecond() - 1), "{}");
        ObjectNode wrongOrder = event("newer", BigDecimal.valueOf(end.getEpochSecond() - 1), "{}");
        String[][] failures = {
            {"{\"Events\":[{}],\"Events\":[]}", "expected a valid JSON object"},
            {"{\"Events\":{\"a\":1}}", "invalid Events page size or shape"},
            {
                "{\"Events\":[],\"__type\":\"" + CloudTrailPeer.PRIVATE + "\"}",
                "error in an HTTP 200 response"
            },
            {"{\"Events\":[],\"NextToken\":42}", "invalid continuation token"},
            {"{\"Events\":[{\"EventTime\":\"123\"}]}", "EventTime must be a number"},
            {
                "{\"Events\":[{\"CloudTrailEvent\":{}}]}",
                "optional event text field has an invalid type"
            },
            {result(JSON.createArrayNode().add(valid).add(outside), null), "event time is outside"},
            {
                result(JSON.createArrayNode().add(valid).add(wrongOrder), null),
                "events are not newest-first"
            }
        };
        for (int i = 0; i < failures.length; i++) {
            Settings invalid = settings("bad-page-" + i);
            program(invalid, step(invalid, null, 200, "fixed", failures[i][0]));
            failure(run(invalid), failures[i][1]);
            assertEquals(1, requests(invalid).size());
            assertTrue(rows(invalid).isEmpty());
        }
        options.pageSize = 1;
        program(
                options,
                step(
                        options,
                        null,
                        200,
                        "fixed",
                        result(JSON.createArrayNode().add(valid).add(valid), null)));
        failure(run(options), "invalid Events page size or shape");
        assertTrue(rows(options).isEmpty());
    }

    @Test
    void boundsContinuationCyclingAndMaximumPages() throws Exception {
        Settings options = settings("cycle");
        program(
                options,
                step(options, null, 200, "fixed", result(JSON.createArrayNode(), "first")),
                step(options, "first", 200, "fixed", result(JSON.createArrayNode(), "again")),
                step(options, "again", 200, "fixed", result(JSON.createArrayNode(), "first")));
        failure(run(options), "repeated continuation token");
        assertSameParameters(requests(options), null, "first", "again");
        assertTrue(rows(options).isEmpty());

        Settings max = settings("page-limit");
        max.pages = 1;
        program(
                max,
                step(max, null, 200, "fixed", result(JSON.createArrayNode(), "unexpected-later")));
        failure(run(max), "requires more than max_pages");
        assertEquals(1, requests(max).size());
        assertTrue(rows(max).isEmpty());
    }

    @Test
    void capsBothPlainAndInflatedBodiesAndRejectsPartialResponses() throws Exception {
        String body =
                result(
                        JSON.createArrayNode()
                                .add(
                                        event(
                                                "one",
                                                BigDecimal.valueOf(end.getEpochSecond()),
                                                "x".repeat(10000))),
                        null);
        for (String coding : List.of("fixed", "gzip")) {
            Settings options = settings("size-" + coding);
            options.bytes = 1024;
            program(options, step(options, null, 200, coding, body));
            failure(run(options), "response exceeds max_response_bytes");
            assertEquals(1, requests(options).size());
            assertTrue(rows(options).isEmpty());
        }
        Settings partial = settings("partial");
        program(partial, step(partial, null, 206, "fixed", "{\"Events\":[]}"));
        failure(run(partial), "HTTP 206");
        assertEquals(1, requests(partial).size());
    }

    @Test
    void deniesRedirectsAndChecksTlsTrustAndHostname() throws Exception {
        Settings redirected = settings("redirect");
        program(
                redirected,
                step(redirected, null, 302, "fixed", "")
                        .put(
                                "location",
                                "https://"
                                        + CloudTrailPeer.CHINA
                                        + "/?signed="
                                        + CloudTrailPeer.PRIVATE));
        failure(run(redirected), "HTTP 302");
        assertEquals(1, requests(redirected).size());

        Settings untrusted = settings("untrusted");
        untrusted.region = "eu-west-1";
        untrusted.host = CloudTrailPeer.UNTRUSTED;
        program(untrusted);
        failure(run(untrusted), "HTTPS or TLS exchange failed");
        assertTrue(requests(untrusted).isEmpty());

        Settings wrongName = settings("wrong-name");
        wrongName.region = "us-west-2";
        wrongName.host = CloudTrailPeer.WRONG_HOST;
        program(wrongName);
        failure(run(wrongName), "HTTPS or TLS exchange failed");
        assertTrue(requests(wrongName).isEmpty());
    }

    @Test
    void closesTricklingSocketsOnAbsoluteDeadlineAndZetaCancel() throws Exception {
        Settings slow = settings("deadline");
        slow.deadline = 2300;
        program(slow, step(slow, null, 200, "trickle", ""));
        failure(run(slow), "request deadline exceeded");
        awaitClosed(slow);
        assertEquals(1, requests(slow).size());

        Settings cancel = settings("canceled");
        cancel.deadline = 90000;
        program(cancel, step(cancel, null, 200, "trickle", ""));
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(CONF, job, cancel.variables()));
        try {
            awaitRequests(cancel, 1);
            success(engine.cancelJob(job));
            running.get(30, TimeUnit.SECONDS);
            awaitClosed(cancel);
            assertEquals(1, requests(cancel).size());
            assertTrue(rows(cancel).isEmpty());
        } finally {
            if (!running.isDone()) {
                engine.cancelJob(job);
                running.cancel(true);
            }
        }
    }

    @Test
    void restoresAtPendingPageWithExactLookupBoundsAndNoReplayOfTheCompletedPage()
            throws Exception {
        Settings options = settings("restore");
        options.deadline = 90000;
        options.attribute = "ReadOnly";
        options.value = "true";
        options.temporary = true;
        BigDecimal time = BigDecimal.valueOf(end.getEpochSecond() - 10);
        String a =
                result(
                        JSON.createArrayNode().add(event("first", time, "first event")),
                        "saved-token");
        String b = result(JSON.createArrayNode().add(event("second", time, "second event")), null);
        program(
                options,
                step(options, null, 200, "fixed", a),
                step(options, "saved-token", 200, "hold", b),
                step(options, "saved-token", 200, "fixed", b));
        String job = jobId();
        Future<Container.ExecResult> initial =
                tasks.submit(() -> engine.executeJob(CONF, job, options.variables()));
        try {
            awaitRequests(options, 2);
            long prior = engine.getCompletedCheckpointCount(job);
            Awaitility.await()
                    .atMost(40, TimeUnit.SECONDS)
                    .until(() -> engine.getCompletedCheckpointCount(job) > prior);
            success(engine.savepointJob(job));
            initial.get(30, TimeUnit.SECONDS);
            engine.touch(options.name + ".release");
            success(engine.restoreJob(CONF, job, options.variables()));
            assertSameParameters(requests(options), null, "saved-token", "saved-token");
            List<JsonNode> stored = rows(options);
            assertEquals(2, stored.size());
            assertEquals(
                    Set.of("first", "second"),
                    stored.stream()
                            .map(row -> row.path("event_id").asText())
                            .collect(Collectors.toSet()));
            assertTrue(
                    stored.stream()
                            .allMatch(
                                    row ->
                                            time.toPlainString()
                                                    .equals(row.path("event_time").asText())));
        } finally {
            if (!initial.isDone()) {
                engine.cancelJob(job);
                initial.cancel(true);
            }
        }
    }

    private Settings settings(String name) {
        return new Settings(name, start, end);
    }

    private void program(Settings options, ObjectNode... steps) throws Exception {
        verifyProviderAssertions();
        Path input = Files.createTempFile(directory, "cloudtrail-steps-", ".json");
        ArrayNode data = JSON.createArrayNode().addAll(Arrays.asList(steps));
        Files.writeString(input, data.toString());
        engine.install(input, options.name + ".program");
        Path activeFile = Files.createTempFile(directory, "cloudtrail-active-", ".txt");
        Files.writeString(activeFile, options.name);
        engine.install(activeFile, "active");
        active = options.name;
    }

    private Container.ExecResult run(Settings options) throws Exception {
        return engine.executeJob(CONF, jobId(), options.variables());
    }

    private List<JsonNode> requests(Settings options) throws Exception {
        return jsonLines(engine.contents(options.name + ".requests"));
    }

    private List<JsonNode> rows(Settings options) throws Exception {
        return jsonLines(engine.rows(options.name));
    }

    private static List<JsonNode> jsonLines(String contents) throws IOException {
        List<JsonNode> result = new ArrayList<>();
        for (String line : contents.split("\\r?\\n")) {
            if (!line.isBlank()) {
                result.add(JSON.readTree(line));
            }
        }
        return result;
    }

    private void awaitRequests(Settings options, int count) {
        Awaitility.await()
                .atMost(35, TimeUnit.SECONDS)
                .until(() -> requests(options).size() == count);
    }

    private void awaitClosed(Settings options) {
        Awaitility.await()
                .atMost(20, TimeUnit.SECONDS)
                .until(() -> engine.exists(options.name + ".disconnect"));
    }

    private static void assertPacing(List<JsonNode> calls) {
        for (int i = 1; i < calls.size(); i++) {
            long current = calls.get(i).path("nanoTime").asLong();
            long previous = calls.get(i - 1).path("nanoTime").asLong();
            assertTrue(current - previous >= TimeUnit.MILLISECONDS.toNanos(440));
            if (i >= 2) {
                long first = calls.get(i - 2).path("nanoTime").asLong();
                assertTrue(current - first >= TimeUnit.MILLISECONDS.toNanos(900));
            }
        }
    }

    private static void assertSameParameters(List<JsonNode> calls, String... continuations) {
        assertEquals(continuations.length, calls.size());
        ObjectNode original = calls.get(0).path("request").deepCopy();
        original.remove("NextToken");
        for (int i = 0; i < calls.size(); i++) {
            ObjectNode current = calls.get(i).path("request").deepCopy();
            JsonNode token = current.remove("NextToken");
            if (continuations[i] == null) {
                assertTrue(token == null);
            } else {
                assertEquals(continuations[i], token.asText());
            }
            assertEquals(original, current);
        }
    }

    private static ObjectNode event(String id, BigDecimal epoch, String raw) {
        ObjectNode event =
                JSON.createObjectNode()
                        .put("EventId", id)
                        .put("EventName", "DescribeInstances")
                        .put("EventSource", "ec2.amazonaws.com");
        if (epoch == null) {
            event.putNull("EventTime");
        } else {
            event.put("EventTime", epoch);
        }
        event.put("CloudTrailEvent", raw);
        return event;
    }

    private static String result(ArrayNode events, String next) {
        ObjectNode reply = JSON.createObjectNode();
        reply.set("Events", events);
        if (next != null) {
            reply.put("NextToken", next);
        }
        return reply.toString();
    }

    private static String message(String code) {
        return JSON.createObjectNode()
                .put("__type", "com.amazonaws.cloudtrail.v20131101#" + code)
                .put("Message", CloudTrailPeer.PRIVATE)
                .toString();
    }

    private static ObjectNode step(
            Settings settings, String next, int status, String mode, String response) {
        ObjectNode request =
                JSON.createObjectNode()
                        .put("StartTime", settings.from.getEpochSecond())
                        .put("EndTime", settings.to.getEpochSecond())
                        .put("MaxResults", settings.pageSize);
        if (next != null) {
            request.put("NextToken", next);
        }
        if (!settings.attribute.isEmpty()) {
            request.putArray("LookupAttributes")
                    .addObject()
                    .put("AttributeKey", settings.attribute)
                    .put("AttributeValue", settings.value);
        }
        ObjectNode step =
                JSON.createObjectNode()
                        .put("host", settings.host)
                        .put("access", settings.access)
                        .put("temporaryCredentials", settings.temporary)
                        .put("region", settings.region)
                        .put("status", status)
                        .put("mode", mode)
                        .put("response", response)
                        .put("implicitWindow", settings.implicit);
        step.set("request", request);
        return step;
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
        String output = output(result);
        assertNotEquals(0, result.getExitCode(), output);
        assertTrue(output.contains(clue), output);
        assertFalse(output.contains(CloudTrailPeer.PRIVATE));
        assertFalse(output.contains(CloudTrailPeer.SECRET));
        assertFalse(output.contains(CloudTrailPeer.TOKEN));
        assertFalse(output.contains(CloudTrailPeer.ACCESS));
        assertFalse(output.contains("opaque-expired"));
    }

    private static KeyStore identity(String name, List<String> hosts) throws Exception {
        String keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
        Path target = directory.resolve(name + ".p12");
        String alt = hosts.stream().map(host -> "dns:" + host).collect(Collectors.joining(","));
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
                                "CN=" + hosts.get(0),
                                "-ext",
                                "SAN=" + alt,
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                target.toString(),
                                "-storepass",
                                CloudTrailPeer.PASSWORD,
                                "-keypass",
                                CloudTrailPeer.PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String log = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, process.waitFor(), log);
        KeyStore key = KeyStore.getInstance("PKCS12");
        try (InputStream stream = Files.newInputStream(target)) {
            key.load(stream, CloudTrailPeer.PASSWORD.toCharArray());
        }
        return key;
    }

    /**
     * Supplies lookup options through the real Zeta CLI, including explicit dates and credentials.
     * The mock provider verifies the independently assembled wire expectations.
     */
    private static final class Settings {
        final String name;
        Instant from;
        Instant to;
        String region = "us-east-1";
        String host = CloudTrailPeer.EAST;
        String access = CloudTrailPeer.ACCESS;
        String attribute = "";
        String value = "";
        boolean temporary;
        boolean implicit;
        int parallelism = 1;
        int pageSize = 50;
        int pages = 2000;
        int bytes = 8388608;
        int deadline = 30000;
        int attempts = 3;

        Settings(String name, Instant from, Instant to) {
            this.name = name;
            this.from = from;
            this.to = to;
        }

        String[] variables() {
            return new String[] {
                "lookup.name=" + name,
                "lookup.access=" + access,
                "lookup.secret=" + CloudTrailPeer.SECRET,
                "lookup.security=" + (temporary ? CloudTrailPeer.TOKEN : ""),
                "lookup.region=" + region,
                "lookup.start=" + (implicit ? "" : from.toString()),
                "lookup.end=" + (implicit ? "" : to.toString()),
                "lookup.attribute=" + attribute,
                "lookup.value=\"" + value + "\"",
                "lookup.pageSize=" + pageSize,
                "lookup.pages=" + pages,
                "lookup.bytes=" + bytes,
                "lookup.deadline=" + deadline,
                "lookup.attempts=" + attempts,
                "lookup.parallelism=" + parallelism
            };
        }
    }

    /**
     * Makes test AWS DNS names resolve exclusively to local trusted or untrusted HTTPS listeners.
     * The normal JDK 11 engine loads the fixture trust store and the shipped connector artifact.
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
                    + CloudTrailPeer.PASSWORD;
        }

        @Override
        protected void executeExtraCommands(GenericContainer<?> container) throws IOException {
            TRUSTED_NAMES.forEach(host -> container.withExtraHost(host, "127.0.0.1"));
            container.withExtraHost(CloudTrailPeer.UNTRUSTED, "127.0.0.2");
            container.withExtraHost(CloudTrailPeer.WRONG_HOST, "127.0.0.1");
            for (String file : List.of("trusted.p12", "untrusted.p12", "trust.jks")) {
                container.withCopyFileToContainer(
                        MountableFile.forHostPath(fixtures.resolve(file)), ROOT + "/" + file);
            }
            String pack = CloudTrailPeer.class.getPackage().getName().replace('.', '/');
            try {
                container.withCopyFileToContainer(
                        MountableFile.forHostPath(
                                Path.of(
                                        CloudTrailPeer.class
                                                .getResource("CloudTrailPeer.class")
                                                .toURI())),
                        ROOT + "/classes/" + pack + "/CloudTrailPeer.class");
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
            } catch (URISyntaxException error) {
                throw new IOException("Cannot stage the CloudTrail HTTPS provider fixture", error);
            }
        }

        Container.ExecResult runPeer() throws Exception {
            return server.execInContainer(
                    "java",
                    "-cp",
                    ROOT + "/classes:" + ROOT + "/jackson:" + ROOT + "/jackson.jar",
                    CloudTrailPeer.class.getName(),
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
            String dir = "/tmp/seatunnel_mnt/cloudtrail-" + name;
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
