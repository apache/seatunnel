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

package org.apache.seatunnel.e2e.connector.acuityscheduling;

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

import static org.apache.seatunnel.e2e.connector.acuityscheduling.AcuityHttpsFixture.BASIC_VALUE;
import static org.apache.seatunnel.e2e.connector.acuityscheduling.AcuityHttpsFixture.PASSWORD;
import static org.apache.seatunnel.e2e.connector.acuityscheduling.AcuityHttpsFixture.PRIVATE_ERROR;
import static org.apache.seatunnel.e2e.connector.acuityscheduling.AcuityHttpsFixture.STORE_PASSWORD;
import static org.apache.seatunnel.e2e.connector.acuityscheduling.AcuityHttpsFixture.gzip;
import static org.apache.seatunnel.e2e.connector.acuityscheduling.AcuityHttpsFixture.respond;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Checks real Zeta execution against an independent Acuity HTTPS peer and persisted JSON rows. */
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK})
@Execution(ExecutionMode.SAME_THREAD)
@Timeout(value = 7, unit = TimeUnit.MINUTES)
class AcuitySchedulingIT extends TestSuiteBase {
    private static final String ALL = "/acuity_all_to_localfile.conf";
    private static final String DATED = "/acuity_dated_to_localfile.conf";
    private static final String LIST = "/acuity_list_to_localfile.conf";
    private static final String RESUME = "/acuity_resume_to_localfile.conf";
    private static final String SAVEPOINT = "/acuity_savepoint_to_localfile.conf";
    private static final String TRUST = "/tmp/acuity-e2e-trust.jks";
    private static final String BASE = "/api/v1/";
    private static final String DAY_ONE = "2024-03-09";
    private static final String DAY_TWO = "2024-03-10";
    private static final String DAY_THREE = "2024-03-11";
    private static final String EMPTY = "[]";
    private static final ObjectMapper JSON =
            new ObjectMapper()
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    @TempDir private static Path temp;
    private AcuityHttpsFixture trusted;
    private AcuityHttpsFixture untrusted;
    private RuntimeEngine engine;
    private final ExecutorService jobs = Executors.newSingleThreadExecutor();

    @BeforeAll
    void startFixtures() throws Exception {
        Path trust = temp.resolve("trusted.jks");
        trusted = new AcuityHttpsFixture(trust);
        untrusted = new AcuityHttpsFixture(temp.resolve("untrusted.jks"));
        engine = new RuntimeEngine(trust);
        engine.startUp();
    }

    @AfterAll
    void stopFixtures() throws Exception {
        jobs.shutdownNow();
        try (AcuityHttpsFixture allow = trusted;
                AcuityHttpsFixture reject = untrusted) {
            if (engine != null) {
                engine.tearDown();
            }
        }
    }

    @Test
    void readsAllSevenResourcesAcrossDaylightSavingAndSubdividesSaturatedWindows()
            throws Exception {
        String scheduled =
                "{\"id\":9007199254740993,\"datetime\":\"2024-03-09T09:30:00-0500\","
                        + "\"timezone\":\"America/New_York\",\"forms\":[{\"id\":7,\"values\":["
                        + "{\"fieldID\":99,\"value\":\"雪 🦊\"}]}],\"certificate\":null,"
                        + "\"charge\":12345678901234567890.12345678}";
        String canceled =
                "{\"id\":42,\"datetime\":\"2024-03-10T09:30:00-0400\","
                        + "\"canceled\":true,\"noShow\":true,\"forms\":[]}";
        String block =
                "{\"id\":50,\"start\":\"2024-03-09T23:30:00-0500\","
                        + "\"end\":\"2024-03-10T03:30:00-0400\",\"calendarID\":12}";
        String calendar = "{\"id\":12,\"name\":\"东区\",\"timezone\":\"America/New_York\"}";
        String client =
                "{\"firstName\":\"雪\",\"email\":\"fixture@example.invalid\",\"phone\":null}";
        String type = "{\"id\":3,\"name\":\"Consultation\",\"active\":true,\"price\":\"9.50\"}";
        String label = "{\"id\":4,\"name\":\"Completed\",\"color\":\"pink\"}";
        String form =
                "{\"id\":7,\"name\":\"Intake\",\"fields\":[{\"id\":99,\"required\":true}],"
                        + "\"appointmentTypeIDs\":[3],\"description\":null}";
        Map<String, String> responses = new HashMap<>();
        responses.put(dated("appointments", 2, DAY_ONE, DAY_THREE), two(88, 89));
        responses.put(dated("appointments", 2, DAY_ONE, DAY_TWO), two(90, 91));
        responses.put(dated("appointments", 2, DAY_ONE, DAY_ONE), array(scheduled));
        responses.put(dated("appointments", 2, DAY_TWO, DAY_TWO), array(canceled));
        responses.put(dated("appointments", 2, DAY_THREE, DAY_THREE), EMPTY);
        responses.put(BASE + "calendars", array(calendar));
        responses.put(BASE + "clients", array(client));
        responses.put(BASE + "appointment-types", array(type));
        responses.put(dated("blocks", 2, DAY_ONE, DAY_THREE), array(block));
        responses.put(BASE + "labels", array(label));
        responses.put(BASE + "forms", array(form));
        AcuityHttpsFixture.Scenario fixture =
                trusted.select(
                        (request, exchange, index) -> {
                            String body = responses.get(request.target);
                            assertNotNull(body, "Unexpected request " + request.target);
                            if (request.target.equals(dated("appointments", 2, DAY_ONE, DAY_ONE))) {
                                gzip(exchange, body);
                            } else {
                                respond(exchange, 200, body);
                            }
                        });
        List<JsonNode> rows =
                completed(
                        start(
                                ALL, trusted, null, DAY_ONE, DAY_THREE, null, 3, 2, 15000, 4194304,
                                20000, 3));
        Map<String, List<JsonNode>> resources =
                rows.stream().collect(Collectors.groupingBy(row -> row.path("resource").asText()));
        assertEquals(8, rows.size());
        assertEquals(7, resources.size());
        Map<String, JsonNode> appointments =
                resources.get("appointments").stream()
                        .collect(
                                Collectors.toMap(
                                        row -> row.path("record_id").asText(), row -> row));
        assertEquals(2, appointments.size());
        assertRow(appointments.get("9007199254740993"), scheduled, DAY_ONE, DAY_ONE);
        assertEquals(
                new BigDecimal("12345678901234567890.12345678"),
                payload(appointments.get("9007199254740993")).path("charge").decimalValue());
        assertRow(appointments.get("42"), canceled, DAY_TWO, DAY_TWO);
        assertRow(only(resources, "calendars"), calendar, null, null);
        assertRow(only(resources, "clients"), client, null, null);
        assertTrue(only(resources, "clients").path("record_id").asText().startsWith("email:"));
        assertRow(only(resources, "appointment-types"), type, null, null);
        assertRow(only(resources, "blocks"), block, DAY_ONE, DAY_THREE);
        assertRow(only(resources, "labels"), label, null, null);
        assertRow(only(resources, "forms"), form, null, null);
        assertEquals(11, fixture.requests().size());
    }

    @Test
    void rereadsSevenCalendarDaysFromExplicitPriorBoundary() throws Exception {
        List<String> expected =
                Arrays.asList(
                        dated("appointments", 100, "2024-03-03", "2024-03-05"),
                        dated("appointments", 100, "2024-03-06", "2024-03-08"),
                        dated("appointments", 100, DAY_ONE, DAY_TWO));
        AcuityHttpsFixture.Scenario fixture =
                trusted.select(
                        (request, exchange, index) -> {
                            assertEquals(expected.get(index), request.target);
                            respond(exchange, 200, array(record(index + 1)));
                        });
        List<JsonNode> rows =
                completed(
                        start(
                                RESUME,
                                trusted,
                                null,
                                "2024-03-01",
                                DAY_TWO,
                                DAY_TWO,
                                3,
                                100,
                                12000,
                                4194304,
                                20000,
                                3));
        assertEquals(expected, targets(fixture));
        Map<String, JsonNode> byId =
                rows.stream()
                        .collect(
                                Collectors.toMap(
                                        row -> row.path("record_id").asText(), row -> row));
        assertRow(byId.get("1"), record(1), "2024-03-03", "2024-03-05");
        assertRow(byId.get("2"), record(2), "2024-03-06", "2024-03-08");
        assertRow(byId.get("3"), record(3), DAY_ONE, DAY_TWO);
    }

    @Test
    void failsSingleDaySaturationForEachDateEndpointWithoutPartialRows() throws Exception {
        for (String resource : Arrays.asList("appointments", "blocks")) {
            AcuityHttpsFixture.Scenario fixture =
                    trusted.select(
                            (request, exchange, index) -> {
                                assertEquals(dated(resource, 2, DAY_TWO, DAY_TWO), request.target);
                                respond(exchange, 200, two(1, 2));
                            });
            Job job =
                    start(
                            DATED, trusted, resource, DAY_TWO, DAY_TWO, null, 1, 2, 9000, 4194304,
                            20000, 3);
            failed(job, "single-day window 2024-03-10 reached max_date_results");
            assertEquals(1, targets(fixture).size());
            assertTrue(receipts(job.output).isEmpty());
        }
    }

    @Test
    void handlesEmptyFullListAndBoundsUndatedResponseWithoutInventingPages() throws Exception {
        AcuityHttpsFixture.Scenario empty =
                trusted.select(
                        (request, exchange, index) -> {
                            assertEquals(BASE + "forms", request.target);
                            respond(exchange, 200, EMPTY);
                        });
        assertTrue(completed(startList("forms", 100, 4194304, 9000, 3)).isEmpty());
        assertEquals(Arrays.asList(BASE + "forms"), targets(empty));

        AcuityHttpsFixture.Scenario large =
                trusted.select(
                        (request, exchange, index) -> {
                            assertEquals(BASE + "clients", request.target);
                            respond(exchange, 200, two(1, 2));
                        });
        Job job = startList("clients", 1, 4194304, 9000, 3);
        failed(job, "list exceeds max_records_per_response");
        assertEquals(Arrays.asList(BASE + "clients"), targets(large));
        assertTrue(receipts(job.output).isEmpty());
    }

    @Test
    void rejectsMalformedShapeDuplicateKeysTrailingAndOversizedJson() throws Exception {
        for (String bad :
                Arrays.asList("{\"data\":[]}", "[false]", "[]{}", "[{\"id\":1,\"id\":2}]")) {
            AcuityHttpsFixture.Scenario fixture =
                    trusted.select((request, exchange, index) -> respond(exchange, 200, bad));
            Job job = startList("forms", 100, 4194304, 9000, 3);
            failed(
                    job,
                    "expected a JSON list response",
                    "expected a response array of JSON objects",
                    "invalid, incomplete or oversized JSON body");
            assertEquals(Arrays.asList(BASE + "forms"), targets(fixture));
            assertTrue(receipts(job.output).isEmpty());
        }
        StringBuilder payload = new StringBuilder();
        for (int i = 0; i < 500; i++) {
            payload.append(PRIVATE_ERROR);
        }
        AcuityHttpsFixture.Scenario compressed =
                trusted.select(
                        (request, exchange, index) ->
                                gzip(exchange, "[{\"id\":1,\"value\":\"" + payload + "\"}]"));
        failed(startList("forms", 100, 1024, 9000, 3), "oversized JSON body");
        assertEquals(Arrays.asList(BASE + "forms"), targets(compressed));
    }

    @Test
    void checksBasicCredentialsAndRejects401403RedirectAndUntrustedTls() throws Exception {
        AcuityHttpsFixture.Scenario destination =
                untrusted.select(
                        (request, exchange, index) -> respond(exchange, 200, array(record(1))));
        for (int code : new int[] {401, 403, 307}) {
            AcuityHttpsFixture.Scenario rejection =
                    trusted.select(
                            (request, exchange, index) -> {
                                if (code == 307) {
                                    exchange.getResponseHeaders()
                                            .set(
                                                    "Location",
                                                    untrusted.origin() + "/another-origin");
                                }
                                respond(exchange, code, PRIVATE_ERROR);
                            });
            failed(startList("labels", 100, 4194304, 9000, 3), "HTTP " + code);
            assertEquals(Arrays.asList(BASE + "labels"), targets(rejection));
        }
        failed(
                start(LIST, untrusted, "forms", null, null, null, 1, 100, 9000, 4194304, 100, 3),
                "TLS connection or certificate validation failed");
        assertTrue(destination.requests().isEmpty());
    }

    @Test
    void honorsThrottleTimingAndRejectsExhaustedAttemptsOrWaitBudget() throws Exception {
        AcuityHttpsFixture.Scenario retries =
                trusted.select(
                        (request, exchange, index) -> {
                            assertEquals(BASE + "labels", request.target);
                            if (index == 0) {
                                exchange.getResponseHeaders().set("Retry-After", "1");
                                respond(exchange, 429, PRIVATE_ERROR);
                            } else if (index == 1) {
                                String value =
                                        DateTimeFormatter.RFC_1123_DATE_TIME.format(
                                                Instant.now()
                                                        .plusSeconds(3)
                                                        .atZone(ZoneOffset.UTC));
                                exchange.getResponseHeaders().set("Retry-After", value);
                                respond(exchange, 503, PRIVATE_ERROR);
                            } else {
                                respond(exchange, 200, array(record(1)));
                            }
                        });
        List<JsonNode> rows = completed(startList("labels", 100, 4194304, 15000, 3));
        assertEquals(Arrays.asList("1"), ids(rows));
        List<AcuityHttpsFixture.Request> calls = retries.requests();
        assertEquals(3, calls.size());
        assertTrue(calls.get(1).arrival - calls.get(0).arrival >= TimeUnit.SECONDS.toNanos(1));
        assertTrue(calls.get(2).arrival - calls.get(1).arrival >= TimeUnit.SECONDS.toNanos(2));

        AcuityHttpsFixture.Scenario exhausted =
                trusted.select((request, exchange, index) -> respond(exchange, 429, PRIVATE_ERROR));
        failed(startList("labels", 100, 4194304, 9000, 2), "HTTP 429");
        assertEquals(2, targets(exhausted).size());

        AcuityHttpsFixture.Scenario tooLate =
                trusted.select(
                        (request, exchange, index) -> {
                            exchange.getResponseHeaders().set("Retry-After", "900");
                            respond(exchange, 429, PRIVATE_ERROR);
                        });
        failed(startList("labels", 100, 4194304, 9000, 3), "exceeds request deadline");
        assertEquals(1, targets(tooLate).size());
    }

    @Test
    void enforcesTotalDeadlineAgainstTrickleResponses() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AcuityHttpsFixture.Scenario held =
                trusted.select(
                        (request, exchange, index) -> {
                            exchange.sendResponseHeaders(200, 0);
                            exchange.getResponseBody()
                                    .write("[{\"id\":1,".getBytes(StandardCharsets.UTF_8));
                            exchange.getResponseBody().flush();
                            started.countDown();
                            for (int i = 0;
                                    i < 100 && !release.await(100, TimeUnit.MILLISECONDS);
                                    i++) {
                                exchange.getResponseBody().write(' ');
                                exchange.getResponseBody().flush();
                            }
                            release.await(30, TimeUnit.SECONDS);
                        });
        try {
            Job job = startList("forms", 100, 4194304, 3000, 3);
            assertTrue(started.await(50, TimeUnit.SECONDS));
            long began = System.nanoTime();
            failed(
                    job,
                    "request deadline exceeded",
                    "HTTPS request did not complete",
                    "invalid, incomplete or oversized JSON body");
            assertTrue(System.nanoTime() - began < TimeUnit.SECONDS.toNanos(20));
            assertEquals(Arrays.asList(BASE + "forms"), targets(held));
            assertTrue(receipts(job.output).isEmpty());
        } finally {
            release.countDown();
        }
    }

    @Test
    void resumesUncommittedWindowFromCheckpointAfterCancel() throws Exception {
        String first = dated("appointments", 100, DAY_ONE, DAY_ONE);
        String second = dated("appointments", 100, DAY_TWO, DAY_TWO);
        CountDownLatch nextRequest = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger secondAttempts = new AtomicInteger();
        AcuityHttpsFixture.Scenario fixture =
                trusted.select(
                        (request, exchange, index) -> {
                            if (first.equals(request.target)) {
                                respond(exchange, 200, array(record(101)));
                            } else {
                                assertEquals(second, request.target);
                                if (secondAttempts.incrementAndGet() == 1) {
                                    nextRequest.countDown();
                                    release.await(35, TimeUnit.SECONDS);
                                } else {
                                    respond(exchange, 200, array(record(102)));
                                }
                            }
                        });
        Job original =
                start(
                        DATED,
                        trusted,
                        "appointments",
                        DAY_ONE,
                        DAY_TWO,
                        null,
                        1,
                        100,
                        65000,
                        4194304,
                        20000,
                        3);
        try {
            awaitHeldCheckpoint(original, nextRequest, "101");
            engine.stopJob(original.id);
            awaitStatus(original.id, "CANCELED");
            original.result.get(30, TimeUnit.SECONDS);
        } finally {
            release.countDown();
        }

        String restored = nextId();
        Future<Container.ExecResult> result =
                jobs.submit(
                        () ->
                                engine.restoreJobWithCheckpoint(
                                        DATED, original.id, restored, original.variables));
        List<JsonNode> rows =
                completed(
                        new Job(
                                restored,
                                original.output,
                                original.variables,
                                engine.getServerLogs().length(),
                                result));
        assertEquals(Arrays.asList("101", "102"), ids(rows));
        assertEquals(Arrays.asList(first, second, second), targets(fixture));
    }

    @Test
    void savepointRestoreSkipsTheCompletedDateResource() throws Exception {
        String first = dated("appointments", 100, DAY_TWO, DAY_TWO);
        String next = BASE + "calendars";
        CountDownLatch nextRequest = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger attempts = new AtomicInteger();
        AcuityHttpsFixture.Scenario fixture =
                trusted.select(
                        (request, exchange, index) -> {
                            if (first.equals(request.target)) {
                                respond(exchange, 200, array(record(300)));
                            } else {
                                assertEquals(next, request.target);
                                if (attempts.incrementAndGet() == 1) {
                                    nextRequest.countDown();
                                    release.await(35, TimeUnit.SECONDS);
                                } else {
                                    respond(exchange, 200, array(record(301)));
                                }
                            }
                        });
        Job original =
                start(
                        SAVEPOINT, trusted, null, DAY_TWO, DAY_TWO, null, 1, 100, 65000, 4194304,
                        20000, 3);
        try {
            awaitHeldCheckpoint(original, nextRequest, "300");
            Container.ExecResult savepoint = engine.savepointJob(original.id);
            assertEquals(0, savepoint.getExitCode(), savepoint.getStderr());
            awaitStatus(original.id, "SAVEPOINT_DONE");
            assertEquals(0, original.result.get(30, TimeUnit.SECONDS).getExitCode());
        } finally {
            release.countDown();
        }
        Future<Container.ExecResult> result =
                jobs.submit(
                        () ->
                                engine.restoreJob(
                                        SAVEPOINT,
                                        original.id,
                                        original.variables.toArray(new String[0])));
        List<JsonNode> rows =
                completed(
                        new Job(
                                original.id,
                                original.output,
                                original.variables,
                                engine.getServerLogs().length(),
                                result));
        assertEquals(Arrays.asList("300", "301"), ids(rows));
        assertEquals(Arrays.asList(first, next, next), targets(fixture));
    }

    private void awaitHeldCheckpoint(Job job, CountDownLatch held, String priorId)
            throws Exception {
        assertTrue(held.await(50, TimeUnit.SECONDS));
        long baseline = engine.getCompletedCheckpointCount(job.id);
        Awaitility.await()
                .atMost(20, TimeUnit.SECONDS)
                .until(() -> engine.getCompletedCheckpointCount(job.id) > baseline);
        Awaitility.await()
                .atMost(20, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> assertEquals(Arrays.asList(priorId), ids(receipts(job.output))));
    }

    private Job startList(String resource, int rows, int bytes, int budget, int attempts) {
        return start(
                LIST, trusted, resource, null, null, null, 1, 100, budget, bytes, rows, attempts);
    }

    private Job start(
            String config,
            AcuityHttpsFixture peer,
            String resource,
            String from,
            String to,
            String resume,
            int days,
            int max,
            int budget,
            int bytes,
            int rows,
            int attempts) {
        String id = nextId();
        List<String> args =
                new ArrayList<>(
                        Arrays.asList(
                                "fixture.url=" + peer.origin(),
                                "fixture.out=" + id,
                                "fixture.days=" + days,
                                "fixture.limit=" + max,
                                "fixture.timeout=" + budget,
                                "fixture.bytes=" + bytes,
                                "fixture.rows=" + rows,
                                "fixture.attempts=" + attempts));
        if (resource != null) {
            args.add("fixture.resource=" + resource);
        }
        if (from != null) {
            args.add("fixture.start=" + from);
            args.add("fixture.end=" + to);
        }
        if (resume != null) {
            args.add("fixture.resume=" + resume);
        }
        int offset = engine.getServerLogs().length();
        Future<Container.ExecResult> result =
                jobs.submit(() -> engine.executeJob(config, id, args.toArray(new String[0])));
        return new Job(id, id, args, offset, result);
    }

    private List<JsonNode> completed(Job job) throws Exception {
        Container.ExecResult result = job.result.get(50, TimeUnit.SECONDS);
        assertEquals(0, result.getExitCode(), result.getStderr());
        awaitStatus(job.id, "FINISHED");
        return receipts(job.output);
    }

    private void awaitStatus(String id, String status) {
        Awaitility.await()
                .atMost(20, TimeUnit.SECONDS)
                .untilAsserted(() -> assertEquals(status, engine.getJobStatus(id)));
    }

    private void failed(Job job, String... diagnostics) throws Exception {
        Container.ExecResult result = job.result.get(30, TimeUnit.SECONDS);
        assertTrue(result.getExitCode() != 0 || "FAILED".equals(engine.getJobStatus(job.id)));
        String logs = engine.getServerLogs();
        String output =
                result.getStdout()
                        + result.getStderr()
                        + logs.substring(Math.min(logs.length(), job.logOffset));
        assertTrue(
                Arrays.stream(diagnostics).anyMatch(output::contains),
                () -> "Expected a matching diagnostic from " + Arrays.toString(diagnostics));
        assertFalse(output.contains(PASSWORD));
        assertFalse(output.contains(BASIC_VALUE));
        assertFalse(output.contains(PRIVATE_ERROR));
    }

    private List<JsonNode> receipts(String id) throws Exception {
        String directory = "/tmp/seatunnel_mnt/acuity-" + id;
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

    private static JsonNode only(Map<String, List<JsonNode>> rows, String resource) {
        List<JsonNode> values = rows.get(resource);
        assertNotNull(values, resource);
        assertEquals(1, values.size());
        return values.get(0);
    }

    private static void assertRow(JsonNode row, String data, String from, String to)
            throws IOException {
        assertNotNull(row);
        JsonNode exact = JSON.readTree(data);
        assertEquals(exact, payload(row));
        if (exact.has("id")) {
            assertEquals(exact.path("id").asText(), row.path("record_id").asText());
        }
        assertEquals(from, row.path("window_start").asText(null));
        assertEquals(to, row.path("window_end").asText(null));
    }

    private static JsonNode payload(JsonNode row) throws IOException {
        return JSON.readTree(row.path("data").asText());
    }

    private static List<String> ids(List<JsonNode> rows) {
        return rows.stream()
                .map(row -> row.path("record_id").asText())
                .sorted()
                .collect(Collectors.toList());
    }

    private static List<String> targets(AcuityHttpsFixture.Scenario fixture) {
        return fixture.requests().stream()
                .map(request -> request.target)
                .collect(Collectors.toList());
    }

    private static String dated(String resource, int limit, String from, String to) {
        return BASE
                + resource
                + "?max="
                + limit
                + "&minDate="
                + from
                + "&maxDate="
                + to
                + ("appointments".equals(resource) ? "&showall=true" : "")
                + "&calendarID=12";
    }

    private static String array(String object) {
        return "[" + object + "]";
    }

    private static String record(int id) {
        return "{\"id\":" + id + "}";
    }

    private static String two(int first, int second) {
        return "[" + record(first) + "," + record(second) + "]";
    }

    private static String nextId() {
        return Long.toString(ThreadLocalRandom.current().nextLong(1L, Long.MAX_VALUE));
    }

    /** Captures a job's independent output directory and server-log interval. */
    private static final class Job {
        final String id;
        final String output;
        final List<String> variables;
        final int logOffset;
        final Future<Container.ExecResult> result;

        Job(
                String id,
                String output,
                List<String> variables,
                int logOffset,
                Future<Container.ExecResult> result) {
            this.id = id;
            this.output = output;
            this.variables = variables;
            this.logOffset = logOffset;
            this.result = result;
        }
    }

    /** Installs a trust store containing only the fixture's trusted certificate. */
    private static final class RuntimeEngine extends SeaTunnelContainer {
        private final Path trust;

        RuntimeEngine(Path trust) {
            this.trust = trust;
        }

        @Override
        protected String getJavaToolOptions() {
            return "-Djavax.net.ssl.trustStore="
                    + TRUST
                    + " -Djavax.net.ssl.trustStorePassword="
                    + STORE_PASSWORD
                    + " -Djavax.net.ssl.trustStoreType=JKS";
        }

        @Override
        protected void executeExtraCommands(GenericContainer<?> container) {
            container.withCopyFileToContainer(MountableFile.forHostPath(trust), TRUST);
        }

        Container.ExecResult command(String... command) throws IOException, InterruptedException {
            return server.execInContainer(command);
        }
    }
}
