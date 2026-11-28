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

package org.apache.seatunnel.e2e.connector.onehundredms;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.seatunnel.SeaTunnelContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.seatunnel.e2e.connector.onehundredms.OneHundredMsFixture.KEY;
import static org.apache.seatunnel.e2e.connector.onehundredms.OneHundredMsFixture.PRIVATE_ERROR;
import static org.apache.seatunnel.e2e.connector.onehundredms.OneHundredMsFixture.SECRET;
import static org.apache.seatunnel.e2e.connector.onehundredms.OneHundredMsFixture.STORE_PASSWORD;
import static org.apache.seatunnel.e2e.connector.onehundredms.OneHundredMsFixture.TOKEN;
import static org.apache.seatunnel.e2e.connector.onehundredms.OneHundredMsFixture.compressed;
import static org.apache.seatunnel.e2e.connector.onehundredms.OneHundredMsFixture.respond;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Real Zeta jobs verify 100ms resource rows, API requests, and checkpointed resume against an
 * independent HTTPS fixture and a transactional LocalFile sink.
 */
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK})
@Execution(ExecutionMode.SAME_THREAD)
@Timeout(value = 7, unit = TimeUnit.MINUTES)
class OneHundredMsIT extends TestSuiteBase {
    private static final String TOKEN_CONFIG = "/onehundredms_to_localfile.conf";
    private static final String INCREMENTAL_CONFIG = "/onehundredms_incremental_to_localfile.conf";
    private static final String APP_CONFIG = "/onehundredms_app_to_localfile.conf";
    private static final String TRUST_STORE = "/tmp/100ms-fixture-trust.jks";
    private static final String LIST_ROOMS = "/v2/rooms?limit=10&enabled=true";
    private static final String NO_ROOMS = "/v2/rooms?limit=10&enabled=false";
    private static final String LIST_TEMPLATES = "/v2/templates?limit=10";
    private static final String LIST_RECORDINGS = "/v2/recordings?limit=10";
    private static final String ROOM_OLD = "r/old ?+";
    private static final String ROOM_OLD_ENCODED = "r%2Fold%20%3F%2B";
    private static final String EVENTS_OLD =
            "/v2/analytics/events?limit=10&room_id=" + ROOM_OLD_ENCODED;
    private static final String SNAPSHOT = "snapshot +/=?";
    private static final String SNAPSHOT_ENCODED = "snapshot%20%2B%2F%3D%3F";
    private static final String FIRST_DATE = "2024-01-01T00:00:00Z";
    private static final String OLD_DATE = "2020-01-01T00:00:00Z";
    private static final String FUTURE_DATE = "2999-01-01T00:00:00Z";
    private static final String EMPTY = "{\"data\":[]}";
    private static final String EMPTY_EVENTS = "{\"events\":[]}";
    private static final ObjectMapper JSON =
            new ObjectMapper()
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    @TempDir private static Path temp;
    private OneHundredMsFixture api;
    private OneHundredMsFixture untrusted;
    private Engine engine;
    private final ExecutorService jobs = Executors.newSingleThreadExecutor();

    @BeforeAll
    void setUp() throws Exception {
        Path trust = temp.resolve("trusted.jks");
        api = new OneHundredMsFixture(trust);
        untrusted = new OneHundredMsFixture(temp.resolve("untrusted.jks"));
        engine = new Engine(trust);
        engine.startUp();
    }

    @AfterAll
    void tearDown() throws Exception {
        jobs.shutdownNow();
        try (OneHundredMsFixture trusted = api;
                OneHundredMsFixture rejected = untrusted) {
            if (engine != null) {
                engine.tearDown();
            }
        }
    }

    @Test
    void exportsAllEightResourcesWithUnfilteredParentDiscoveryAndExactNumbers() throws Exception {
        Map<String, String> data = inventory();
        OneHundredMsFixture.Scenario scenario =
                api.set(
                        false,
                        (request, exchange, index) -> {
                            String body = data.get(request.target);
                            if (body == null
                                    && ("/v2/active-rooms/r-tail/peers".equals(request.target)
                                            || "/v2/active-rooms/r-disabled/peers"
                                                    .equals(request.target))) {
                                respond(exchange, 404, "{\"message\":\"room no longer active\"}");
                            } else {
                                assertTrue(
                                        body != null,
                                        "Unexpected resource query " + request.target);
                                if (EVENTS_OLD.equals(request.target)) {
                                    compressed(exchange, body);
                                } else {
                                    respond(exchange, 200, body);
                                }
                            }
                        });
        List<JsonNode> rows =
                finish(start(INCREMENTAL_CONFIG, api, 15000, 4194304, 3, 20, FIRST_DATE, null));
        Map<String, JsonNode> byId =
                rows.stream()
                        .collect(
                                Collectors.toMap(
                                        row ->
                                                row.path("resource").asText()
                                                        + "/"
                                                        + row.path("record_id").asText(),
                                        row -> row));
        assertEquals(14, byId.size());
        assertEquals(true, payload(byId.get("sessions/live")).path("active").asBoolean());
        assertEquals(false, payload(byId.get("sessions/past")).path("active").asBoolean());
        assertEquals(FIRST_DATE, byId.get("rooms/r-current").path("updated_at").asText());
        assertEquals(FIRST_DATE, byId.get("rooms/r-tail").path("updated_at").asText());
        assertFalse(byId.containsKey("rooms/" + ROOM_OLD));
        JsonNode peer = byId.get("active_room_peers/peer-key");
        assertEquals(ROOM_OLD, peer.path("parent_id").asText());
        assertEquals("雪 🦊", payload(peer).path("name").textValue());
        assertEquals(
                "peer-identified",
                payload(byId.get("active_room_peers/peer-identified")).path("id").asText());
        assertEquals(FIRST_DATE, byId.get("templates/t-new").path("updated_at").asText());
        assertFalse(byId.containsKey("templates/t-old"));
        assertEquals("t-old", byId.get("template_settings/t-old").path("parent_id").asText());
        assertTrue(payload(byId.get("template_settings/t-old")).path("recording").isNull());
        assertEquals("us", payload(byId.get("template_settings/t-new")).path("region").asText());
        assertTrue(
                payload(byId.get("templates_destinations/t-old"))
                        .path("hlsDestinations")
                        .has("web"));
        assertTrue(
                payload(byId.get("templates_destinations/t-old"))
                        .path("transcriptions")
                        .has("default"));
        assertTrue(payload(byId.get("templates_destinations/t-new")).isEmpty());
        JsonNode event = payload(byId.get("analytics_events/e-one"));
        assertEquals(new BigInteger("9007199254740993"), event.path("samples").bigIntegerValue());
        assertEquals(
                new BigDecimal("12345678901234567890.12345678"), event.path("cost").decimalValue());
        assertTrue(event.path("optional").isNull());
        assertEquals(ROOM_OLD, byId.get("analytics_events/e-two").path("parent_id").asText());
        assertEquals("completed", payload(byId.get("recordings/rec")).path("status").asText());
        assertFalse(byId.containsKey("recordings/future"));
        List<String> requests = targets(scenario);
        assertEquals(3, requests.stream().filter(LIST_ROOMS::equals).count());
        assertEquals(3, requests.stream().filter(LIST_TEMPLATES::equals).count());
        assertTrue(requests.contains(EVENTS_OLD + "&start=" + SNAPSHOT_ENCODED));
        assertTrue(requests.contains(EVENTS_OLD + "&start=last%2B"));
        assertTrue(requests.contains("/v2/active-rooms/" + ROOM_OLD_ENCODED + "/peers"));
    }

    @Test
    void signsRefreshesAndReusesManagementJwtOnlyWithinItsLifetime() throws Exception {
        OneHundredMsFixture.Scenario scenario =
                api.set(
                        true,
                        (request, exchange, index) -> {
                            if (index == 0) {
                                assertEquals(LIST_RECORDINGS, request.target);
                                respond(exchange, 401, PRIVATE_ERROR);
                            } else if (index == 1) {
                                assertEquals(LIST_RECORDINGS, request.target);
                                respond(
                                        exchange,
                                        200,
                                        list(recording("signed", FIRST_DATE), "continuation"));
                            } else {
                                assertEquals(
                                        LIST_RECORDINGS + "&start=continuation", request.target);
                                respond(exchange, 200, EMPTY);
                            }
                        });
        List<JsonNode> rows =
                finish(start(APP_CONFIG, api, 10000, 4194304, 3, 20, null, "recordings"));
        assertEquals(1, rows.size());
        assertEquals("signed", rows.get(0).path("record_id").asText());
        List<OneHundredMsFixture.Request> calls = scenario.requests();
        assertEquals(3, calls.size());
        assertNotEquals(calls.get(0).jti, calls.get(1).jti);
        assertEquals(calls.get(1).jti, calls.get(2).jti);
    }

    @Test
    void honorsRetryAfterAndFiniteRetryBudgets() throws Exception {
        OneHundredMsFixture.Scenario scenario =
                api.set(
                        false,
                        (request, exchange, index) -> {
                            assertEquals(LIST_RECORDINGS, request.target);
                            if (index == 0) {
                                exchange.getResponseHeaders().set("Retry-After", "1");
                                respond(exchange, 429, PRIVATE_ERROR);
                            } else if (index == 1) {
                                String header =
                                        DateTimeFormatter.RFC_1123_DATE_TIME.format(
                                                Instant.now()
                                                        .plusSeconds(3)
                                                        .atZone(ZoneOffset.UTC));
                                exchange.getResponseHeaders().set("Retry-After", header);
                                respond(exchange, 503, PRIVATE_ERROR);
                            } else {
                                compressed(
                                        exchange, list(recording("recovered", FIRST_DATE), null));
                            }
                        });
        List<JsonNode> rows =
                finish(start(TOKEN_CONFIG, api, 15000, 4194304, 3, 20, null, "recordings"));
        assertEquals(1, rows.size());
        List<OneHundredMsFixture.Request> requests = scenario.requests();
        assertEquals(3, requests.size());
        assertTrue(
                requests.get(1).arrival - requests.get(0).arrival >= TimeUnit.SECONDS.toNanos(1));
        assertTrue(
                requests.get(2).arrival - requests.get(1).arrival >= TimeUnit.SECONDS.toNanos(2));

        OneHundredMsFixture.Scenario capped =
                api.set(false, (request, exchange, index) -> respond(exchange, 503, PRIVATE_ERROR));
        fail(start(TOKEN_CONFIG, api, 9000, 4194304, 2, 20, null, "recordings"), "HTTP 503");
        assertEquals(2, targets(capped).size());

        OneHundredMsFixture.Scenario tooLate =
                api.set(
                        false,
                        (request, exchange, index) -> {
                            exchange.getResponseHeaders().set("Retry-After", "900");
                            respond(exchange, 429, PRIVATE_ERROR);
                        });
        fail(
                start(TOKEN_CONFIG, api, 9000, 4194304, 3, 20, null, "recordings"),
                "retry delay exceeds");
        assertEquals(1, targets(tooLate).size());
    }

    @Test
    void rejectsCursorReuseAndRepeatedRowsWithFreshCursors() throws Exception {
        OneHundredMsFixture.Scenario cyclical =
                api.set(
                        false,
                        (request, exchange, index) -> {
                            assertEquals(
                                    index == 0
                                            ? LIST_RECORDINGS
                                            : LIST_RECORDINGS
                                                    + "&start="
                                                    + (index == 1 ? "a" : "b"),
                                    request.target);
                            respond(
                                    exchange,
                                    200,
                                    list(
                                            recording("cycle-" + index, FIRST_DATE),
                                            index == 1 ? "b" : "a"));
                        });
        fail(
                start(TOKEN_CONFIG, api, 9000, 4194304, 3, 20, null, "recordings"),
                "cursor repeated or cycled");
        assertEquals(3, targets(cyclical).size());

        OneHundredMsFixture.Scenario stalled =
                api.set(
                        false,
                        (request, exchange, index) ->
                                respond(
                                        exchange,
                                        200,
                                        list(recording("same", FIRST_DATE), "different-" + index)));
        fail(start(TOKEN_CONFIG, api, 9000, 4194304, 3, 20, null, "recordings"), "same records");
        assertEquals(2, targets(stalled).size());

        OneHundredMsFixture.Scenario endless =
                api.set(
                        false,
                        (request, exchange, index) ->
                                respond(
                                        exchange,
                                        200,
                                        "{\"data\":[],\"last\":\"next-" + index + "\"}"));
        fail(
                start(TOKEN_CONFIG, api, 9000, 4194304, 3, 2, null, "recordings"),
                "max_pages was reached");
        assertEquals(2, targets(endless).size());
    }

    @Test
    void rejectsMalformedAndOversizedDecodedBodiesWithoutPrintingData() throws Exception {
        for (String bad :
                Arrays.asList(
                        "{\"data\":[],\"data\":[]}", "{\"data\":[]}{}", "{\"data\":[false]}")) {
            OneHundredMsFixture.Scenario scenario =
                    api.set(false, (request, exchange, index) -> respond(exchange, 200, bad));
            fail(
                    start(TOKEN_CONFIG, api, 9000, 4194304, 3, 20, null, "recordings"),
                    "invalid, incomplete or oversized JSON",
                    "expected a provider record object");
            assertEquals(1, targets(scenario).size());
        }
        StringBuilder oversized = new StringBuilder();
        for (int i = 0; i < 300; i++) {
            oversized.append(PRIVATE_ERROR);
        }
        OneHundredMsFixture.Scenario zipped =
                api.set(
                        false,
                        (request, exchange, index) ->
                                compressed(
                                        exchange, "{\"data\":[],\"large\":\"" + oversized + "\"}"));
        fail(start(TOKEN_CONFIG, api, 9000, 1024, 3, 20, null, "recordings"), "oversized JSON");
        assertEquals(1, targets(zipped).size());
    }

    @Test
    void failsStaticTokenErrorsAndRejectsRedirectsAndUntrustedTls() throws Exception {
        OneHundredMsFixture.Scenario other =
                untrusted.set(false, (request, exchange, index) -> respond(exchange, 200, EMPTY));
        for (int status : new int[] {401, 403, 307}) {
            OneHundredMsFixture.Scenario rejected =
                    api.set(
                            false,
                            (request, exchange, index) -> {
                                if (status == 307) {
                                    exchange.getResponseHeaders()
                                            .set("Location", untrusted.origin() + "/redirected");
                                }
                                respond(exchange, status, PRIVATE_ERROR);
                            });
            fail(
                    start(TOKEN_CONFIG, api, 9000, 4194304, 3, 20, null, "recordings"),
                    "HTTP " + status);
            assertEquals(1, targets(rejected).size());
        }
        fail(
                start(TOKEN_CONFIG, untrusted, 9000, 4194304, 3, 20, null, "recordings"),
                "TLS connection or certificate validation failed");
        assertTrue(other.requests().isEmpty());
    }

    @Test
    void failsAnExpiredAnalyticsContinuationRatherThanRestartingTheSnapshot() throws Exception {
        OneHundredMsFixture.Scenario expired =
                api.set(
                        false,
                        (request, exchange, index) -> {
                            if (LIST_ROOMS.equals(request.target)) {
                                respond(exchange, 200, list(room(ROOM_OLD, OLD_DATE), null));
                            } else if (EVENTS_OLD.equals(request.target)) {
                                respond(exchange, 200, events("{\"id\":\"first\"}", SNAPSHOT));
                            } else {
                                assertEquals(
                                        EVENTS_OLD + "&start=" + SNAPSHOT_ENCODED, request.target);
                                respond(exchange, 410, PRIVATE_ERROR);
                            }
                        });
        fail(
                start(TOKEN_CONFIG, api, 9000, 4194304, 3, 20, null, "analytics_events"),
                "analytics continuation rejected or expired (HTTP 410)");
        assertEquals(
                Arrays.asList(LIST_ROOMS, EVENTS_OLD, EVENTS_OLD + "&start=" + SNAPSHOT_ENCODED),
                targets(expired));
    }

    @Test
    void enforcesTotalDeadlineEvenWhenPeerTricklesTheBody() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch stopped = new CountDownLatch(1);
        OneHundredMsFixture.Scenario slow =
                api.set(
                        false,
                        (request, exchange, index) -> {
                            exchange.sendResponseHeaders(200, 0);
                            exchange.getResponseBody()
                                    .write("{\"data\":[".getBytes(StandardCharsets.UTF_8));
                            exchange.getResponseBody().flush();
                            started.countDown();
                            for (int sent = 0;
                                    sent < 100 && !stopped.await(100, TimeUnit.MILLISECONDS);
                                    sent++) {
                                exchange.getResponseBody().write(' ');
                                exchange.getResponseBody().flush();
                            }
                            stopped.await(30, TimeUnit.SECONDS);
                        });
        try {
            Job job = start(TOKEN_CONFIG, api, 3000, 4194304, 3, 20, null, "recordings");
            assertTrue(started.await(45, TimeUnit.SECONDS));
            long begin = System.nanoTime();
            fail(
                    job,
                    "request deadline exceeded",
                    "invalid, incomplete or oversized JSON",
                    "HTTPS request did not complete");
            assertTrue(System.nanoTime() - begin < TimeUnit.SECONDS.toNanos(20));
            assertEquals(1, targets(slow).size());
        } finally {
            stopped.countDown();
        }
    }

    @Test
    void checkpointsWhileRequestIsHeldAndRestoresTheExactAnalyticsContinuation() throws Exception {
        CountDownLatch snapshotHeld = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger continuation = new AtomicInteger();
        OneHundredMsFixture.Scenario fixture =
                api.set(
                        false,
                        (request, exchange, index) -> {
                            if (LIST_ROOMS.equals(request.target)) {
                                respond(exchange, 200, list(room(ROOM_OLD, OLD_DATE), null));
                            } else if (EVENTS_OLD.equals(request.target)) {
                                respond(exchange, 200, events("{\"id\":\"before\"}", SNAPSHOT));
                            } else if (NO_ROOMS.equals(request.target)) {
                                respond(exchange, 200, EMPTY);
                            } else {
                                assertEquals(
                                        EVENTS_OLD + "&start=" + SNAPSHOT_ENCODED, request.target);
                                if (continuation.incrementAndGet() == 1) {
                                    snapshotHeld.countDown();
                                    release.await(30, TimeUnit.SECONDS);
                                } else {
                                    respond(exchange, 200, events("{\"id\":\"after\"}", null));
                                }
                            }
                        });
        Job original = start(TOKEN_CONFIG, api, 60000, 4194304, 3, 20, null, "analytics_events");
        try {
            assertTrue(snapshotHeld.await(45, TimeUnit.SECONDS));
            long baseline = engine.getCompletedCheckpointCount(original.id);
            Awaitility.await()
                    .atMost(20, TimeUnit.SECONDS)
                    .until(() -> engine.getCompletedCheckpointCount(original.id) > baseline);
            Awaitility.await()
                    .atMost(20, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    assertEquals(
                                            Arrays.asList("before"),
                                            ids(readRows(original.output))));
            engine.stopJob(original.id);
            Awaitility.await()
                    .atMost(20, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> assertEquals("CANCELED", engine.getJobStatus(original.id)));
            original.result.get(25, TimeUnit.SECONDS);
        } finally {
            release.countDown();
        }

        String id = id();
        int logOffset = engine.getServerLogs().length();
        Future<Container.ExecResult> result =
                jobs.submit(
                        () ->
                                engine.restoreJobWithCheckpoint(
                                        TOKEN_CONFIG, original.id, id, original.variables));
        List<String> values =
                ids(finish(new Job(id, original.output, logOffset, original.variables, result)));
        assertEquals(Arrays.asList("after", "before"), values);
        assertEquals(
                Arrays.asList(
                        LIST_ROOMS,
                        EVENTS_OLD,
                        EVENTS_OLD + "&start=" + SNAPSHOT_ENCODED,
                        EVENTS_OLD + "&start=" + SNAPSHOT_ENCODED,
                        NO_ROOMS),
                targets(fixture));
    }

    private static Map<String, String> inventory() throws Exception {
        Map<String, String> data = new HashMap<>();
        data.put(
                "/v2/sessions?limit=10&active=true",
                list("{\"id\":\"live\",\"active\":true}", null));
        data.put(
                "/v2/sessions?limit=10&active=false",
                list("{\"id\":\"past\",\"active\":false}", null));
        data.put(
                LIST_ROOMS,
                list(room(ROOM_OLD, OLD_DATE) + "," + room("r-current", FIRST_DATE), "page +/?"));
        data.put(LIST_ROOMS + "&start=page%20%2B%2F%3F", list("", "tail"));
        data.put(
                LIST_ROOMS + "&start=tail",
                list(
                        "{\"id\":\"r-tail\",\"updated_at\":\"0001-01-01T00:00:00Z\",\"created_at\":\""
                                + FIRST_DATE
                                + "\"}",
                        null));
        data.put(NO_ROOMS, list(room("r-disabled", OLD_DATE), null));
        data.put(
                "/v2/active-rooms/" + ROOM_OLD_ENCODED + "/peers",
                "{\"peers\":{\"peer-key\":{\"name\":\"雪 🦊\"},"
                        + "\"peer-identified\":{\"id\":\"peer-identified\",\"role\":\"host\"}}}");
        data.put("/v2/active-rooms/r-current/peers", "{\"peers\":{}}");
        data.put(
                LIST_TEMPLATES,
                list("{\"id\":\"t-old\",\"updatedAt\":\"" + OLD_DATE + "\"}", "next"));
        data.put(
                LIST_TEMPLATES + "&start=next",
                list("{\"id\":\"t-new\",\"updatedAt\":\"" + FIRST_DATE + "\"}", null));
        data.put("/v2/templates/t-old/settings", "{\"region\":\"in\",\"recording\":null}");
        data.put("/v2/templates/t-new/settings", "{\"region\":\"us\"}");
        data.put(
                "/v2/templates/t-old/destinations",
                "{\"hlsDestinations\":{\"web\":{}},\"transcriptions\":{\"default\":{}}}");
        data.put("/v2/templates/t-new/destinations", "{}");
        data.put(
                EVENTS_OLD,
                events(
                        "{\"id\":\"e-one\",\"samples\":9007199254740993,"
                                + "\"cost\":12345678901234567890.12345678,\"optional\":null}",
                        SNAPSHOT));
        data.put(EVENTS_OLD + "&start=" + SNAPSHOT_ENCODED, events("", "last+"));
        data.put(EVENTS_OLD + "&start=last%2B", events("{\"event_id\":\"e-two\"}", null));
        for (String room : Arrays.asList("r-current", "r-tail", "r-disabled")) {
            data.put("/v2/analytics/events?limit=10&room_id=" + room, EMPTY_EVENTS);
        }
        data.put(
                LIST_RECORDINGS,
                list(recording("rec", FIRST_DATE) + "," + recording("future", FUTURE_DATE), null));
        return data;
    }

    private static String list(String data, String last) throws IOException {
        return "{\"data\":["
                + data
                + "]"
                + (last == null ? "" : ",\"last\":" + JSON.writeValueAsString(last))
                + "}";
    }

    private static String events(String values, String next) throws IOException {
        return "{\"events\":["
                + values
                + "]"
                + (next == null ? "" : ",\"next\":" + JSON.writeValueAsString(next))
                + "}";
    }

    private static String room(String id, String date) throws IOException {
        return "{\"id\":" + JSON.writeValueAsString(id) + ",\"updated_at\":\"" + date + "\"}";
    }

    private static String recording(String id, String date) {
        return "{\"id\":\"" + id + "\",\"updated_at\":\"" + date + "\",\"status\":\"completed\"}";
    }

    private static JsonNode payload(JsonNode row) throws IOException {
        return JSON.readTree(row.path("data").textValue());
    }

    private static List<String> targets(OneHundredMsFixture.Scenario fixture) {
        return fixture.requests().stream()
                .map(request -> request.target)
                .collect(Collectors.toList());
    }

    private static List<String> ids(List<JsonNode> rows) {
        return rows.stream()
                .map(row -> row.path("record_id").asText())
                .sorted()
                .collect(Collectors.toList());
    }

    private Job start(
            String resourceFile,
            OneHundredMsFixture peer,
            int budget,
            int maxBytes,
            int attempts,
            int maxPages,
            String since,
            String resource) {
        String id = id();
        List<String> variables =
                new ArrayList<>(
                        Arrays.asList(
                                "fixture.url=" + peer.origin(),
                                "job.output=" + id,
                                "max.pages=" + maxPages,
                                "max.bytes=" + maxBytes,
                                "request.ms=" + budget,
                                "attempts=" + attempts));
        if (since != null) {
            variables.add("since=" + since);
        }
        if (resource != null) {
            variables.add("fixture.resource=" + resource);
        }
        int logOffset = engine.getServerLogs().length();
        Future<Container.ExecResult> result =
                jobs.submit(
                        () ->
                                engine.executeJob(
                                        resourceFile, id, variables.toArray(new String[0])));
        return new Job(id, id, logOffset, variables, result);
    }

    private List<JsonNode> finish(Job job) throws Exception {
        Container.ExecResult result = job.result.get(50, TimeUnit.SECONDS);
        assertEquals(0, result.getExitCode(), result.getStderr());
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertEquals("FINISHED", engine.getJobStatus(job.id)));
        return readRows(job.output);
    }

    private void fail(Job job, String... messages) throws Exception {
        Container.ExecResult result = job.result.get(30, TimeUnit.SECONDS);
        assertTrue(result.getExitCode() != 0 || "FAILED".equals(engine.getJobStatus(job.id)));
        String logs = engine.getServerLogs();
        String diagnostic =
                result.getStdout()
                        + result.getStderr()
                        + logs.substring(Math.min(logs.length(), job.logOffset));
        assertTrue(
                Arrays.stream(messages).anyMatch(diagnostic::contains),
                () -> "Expected one of " + Arrays.toString(messages));
        assertFalse(diagnostic.contains(TOKEN));
        assertFalse(diagnostic.contains(KEY));
        assertFalse(diagnostic.contains(SECRET));
        assertFalse(diagnostic.contains(PRIVATE_ERROR));
        assertFalse(diagnostic.contains("Authorization: Bearer"));
    }

    private List<JsonNode> readRows(String output) throws Exception {
        String directory = "/tmp/seatunnel_mnt/100ms-" + output;
        Container.ExecResult files =
                engine.command(
                        "sh",
                        "-c",
                        "if test -d "
                                + directory
                                + "; then find "
                                + directory
                                + " -maxdepth 1 -name 'receipt_*' -type f "
                                + "-exec sh -c 'for f do cat \"$f\"; printf \"\\n\"; done' sh {} +; fi");
        assertEquals(0, files.getExitCode(), files.getStderr());
        List<JsonNode> rows = new ArrayList<>();
        for (String line : files.getStdout().split("\\R")) {
            if (!line.trim().isEmpty()) {
                rows.add(JSON.readTree(line));
            }
        }
        return rows;
    }

    private static String id() {
        return Long.toString(ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE));
    }

    /** Isolates a CLI run's log interval and output directory. */
    private static final class Job {
        final String id;
        final String output;
        final int logOffset;
        final List<String> variables;
        final Future<Container.ExecResult> result;

        Job(
                String id,
                String output,
                int logOffset,
                List<String> variables,
                Future<Container.ExecResult> result) {
            this.id = id;
            this.output = output;
            this.logOffset = logOffset;
            this.variables = variables;
            this.result = result;
        }
    }

    /** Installs only the test CA in the dedicated Zeta JVM trust store. */
    private static final class Engine extends SeaTunnelContainer {
        private final Path trust;

        Engine(Path trust) {
            this.trust = trust;
        }

        @Override
        protected String getJavaToolOptions() {
            return "-Djavax.net.ssl.trustStore="
                    + TRUST_STORE
                    + " -Djavax.net.ssl.trustStorePassword="
                    + STORE_PASSWORD
                    + " -Djavax.net.ssl.trustStoreType=JKS";
        }

        @Override
        protected void executeExtraCommands(GenericContainer<?> container) {
            container.withCopyFileToContainer(MountableFile.forHostPath(trust), TRUST_STORE);
        }

        Container.ExecResult command(String... command) throws IOException, InterruptedException {
            return server.execInContainer(command);
        }
    }
}
