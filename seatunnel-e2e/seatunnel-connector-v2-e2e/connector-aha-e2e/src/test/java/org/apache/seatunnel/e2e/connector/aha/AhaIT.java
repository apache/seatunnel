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

package org.apache.seatunnel.e2e.connector.aha;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.seatunnel.SeaTunnelContainer;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
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

@Timeout(value = 8, unit = TimeUnit.MINUTES)
class AhaIT extends TestSuiteBase {
    private static final String SELECTED = "/aha-selected.conf";
    private static final String ALL = "/aha-all.conf";
    private static final String RESUME = "/aha-resume.conf";
    private static final String URL = "https://fixture.aha.io:8443";
    private static final String GOALS = target("/goals", 1, 2);
    private static final String PRODUCTS = target("/products", 1, 2);
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String TRUST = "/tmp/aha-https-trust.jks";

    @TempDir static Path temp;
    private AhaHttpsFixture fixture;
    private AhaHttpsFixture untrusted;
    private GenericContainer<?> gateway;
    private Engine engine;

    @BeforeAll
    void start() throws Exception {
        fixture = new AhaHttpsFixture(temp, "trusted");
        untrusted = new AhaHttpsFixture(temp, "untrusted");
        Testcontainers.exposeHostPorts(fixture.port(), untrusted.port());
        URL fixtureClass = AhaFixtureGateway.class.getResource("AhaFixtureGateway.class");
        Path gatewayClass = Paths.get(fixtureClass.toURI());
        String main = AhaFixtureGateway.class.getName();
        gateway =
                new GenericContainer<>(DockerImageName.parse("eclipse-temurin:11-jdk"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(AhaHttpsFixture.TENANT)
                        .withAccessToHost(true)
                        .withExposedPorts(8443, 8444)
                        .withCopyFileToContainer(
                                MountableFile.forHostPath(gatewayClass),
                                "/opt/fixture/" + main.replace('.', '/') + ".class")
                        .withCommand(
                                "java",
                                "-cp",
                                "/opt/fixture",
                                main,
                                String.valueOf(fixture.port()),
                                String.valueOf(untrusted.port()))
                        .waitingFor(Wait.forListeningPort());
        gateway.start();
        engine = new Engine(temp.resolve("trusted-trust.jks"));
        engine.startUp(NETWORK);
    }

    @AfterEach
    void noUnexpectedTargets() {
        assertEquals(0, fixture.invalidRequests());
        assertEquals(0, untrusted.invalidRequests());
    }

    @AfterAll
    void stop() throws Exception {
        try {
            if (engine != null) {
                engine.tearDown();
            }
        } finally {
            try {
                if (gateway != null) {
                    gateway.stop();
                }
            } finally {
                if (fixture != null) {
                    fixture.close();
                }
                if (untrusted != null) {
                    untrusted.close();
                }
            }
        }
    }

    @Test
    void readsAllEightResourcesAndKeepsNestedPagesScopedToTheirParents() throws Exception {
        Map<String, AhaHttpsFixture.Reply> replies = new HashMap<>();
        String rich =
                "{\"id\":9223372036854775808,\"label\":\"需求🌐\",\"nested\":{\"null_value\":null,"
                        + "\"list\":[true,12.34000]}}";
        replies.put(
                target("/features", 1, 2), response("features", 1, 2, 3, false, rich, item("F2")));
        replies.put(target("/features", 2, 2), response("features", 2, 2, 3, true, item("F3")));
        addParents(replies, "products", "/products", "P/1.é", "P2", "P3");
        addParents(replies, "ideas", "/ideas", "I+1", "I2", "I3");
        replies.put(
                target("/products/P%2F1%2E%C3%A9/idea_categories", 1, 2),
                response("idea_categories", 1, 2, 3, true, item("C1"), item("C2")));
        replies.put(
                target("/products/P%2F1%2E%C3%A9/idea_categories", 2, 2),
                response("idea_categories", 2, 2, 3, false, item("C3")));
        replies.put(
                target("/products/P2/idea_categories", 1, 2),
                response("idea_categories", 1, 0, 0, false));
        replies.put(
                target("/products/P3/idea_categories", 1, 2),
                response("idea_categories", 1, 1, 1, false, item("C4")));
        replies.put(
                target("/ideas/I%2B1/endorsements", 1, 2),
                response("idea_endorsements", 1, 1, 1, false, item("E1")));
        replies.put(
                target("/ideas/I2/endorsements", 1, 2),
                response("idea_endorsements", 1, 1, 0, true));
        replies.put(
                target("/ideas/I3/endorsements", 1, 2),
                response("idea_endorsements", 1, 1, 1, false, item("E1")));
        replies.put(
                target("/ideas/I%2B1/idea_comments", 1, 2),
                response("idea_comments", 1, 1, 1, false, item("M1")));
        replies.put(
                target("/ideas/I2/idea_comments", 1, 2),
                response("idea_comments", 1, 1, 1, false, item("M2")));
        replies.put(
                target("/ideas/I3/idea_comments", 1, 2), response("idea_comments", 1, 0, 0, false));
        replies.put(target("/users", 1, 2), response("users", 1, 1, 1, false, item("U1")));
        replies.put(GOALS, response("goals", 1, 1, 1, false, item("G1")));
        fixture.script(replies);

        success(run(ALL, "all", "goals", URL, 2, 4194304, 3, 15000));
        Map<String, String> expected = new HashMap<>();
        expect(expected, "features", null, "F2", "F3");
        expected.put("features|null|9223372036854775808", rich);
        expect(expected, "products", null, "P/1.é", "P2", "P3");
        expect(expected, "ideas", null, "I+1", "I2", "I3");
        expect(expected, "idea_categories", "P/1.é", "C1", "C2", "C3");
        expect(expected, "idea_categories", "P3", "C4");
        expect(expected, "idea_endorsements", "I+1", "E1");
        expect(expected, "idea_endorsements", "I3", "E1");
        expect(expected, "idea_comments", "I+1", "M1");
        expect(expected, "idea_comments", "I2", "M2");
        expect(expected, "users", null, "U1");
        expect(expected, "goals", null, "G1");
        rows("all", expected);
        assertEquals(2, fixture.hits(PRODUCTS));
        assertEquals(3, fixture.hits(target("/ideas", 2, 2)));
    }

    @Test
    void completesAnEmptyPageWithoutFollowingForeignPaginationLinks() throws Exception {
        fixture.script(
                Collections.singletonMap(
                        GOALS,
                        (request, attempt) ->
                                AhaHttpsFixture.json(
                                        request,
                                        "{\"goals\":[],\"pagination\":{\"current_page\":1,"
                                                + "\"total_pages\":0,\"total_records\":0,\"next_page\":"
                                                + "\"https://foreign.example/private\"}}")));
        success(run(SELECTED, "empty", "goals", URL, 2, 4194304, 3, 15000));
        rows("empty", Collections.emptyMap());
        assertEquals(1, fixture.hits(GOALS));
    }

    @Test
    void rejectsInconsistentPagesAndExplicitPartialResponses() throws Exception {
        String second = target("/goals", 2, 2);
        Map<String, AhaHttpsFixture.Reply> repeated = new HashMap<>();
        repeated.put(GOALS, response("goals", 1, 2, 4, false, item("a"), item("b")));
        repeated.put(second, response("goals", 2, 2, 4, false, item("a"), item("b")));
        fixture.script(repeated);
        failure(run(SELECTED, "repeated", "goals", URL, 2, 4194304, 3, 15000), "repeated");

        Map<String, AhaHttpsFixture.Reply> changed = new HashMap<>(repeated);
        changed.put(second, response("goals", 2, 2, 3, false, item("c")));
        fixture.script(changed);
        failure(
                run(SELECTED, "changed", "goals", URL, 2, 4194304, 3, 15000),
                "pagination totals changed");

        fixture.script(
                Collections.singletonMap(GOALS, response("goals", 2, 2, 2, false, item("x"))));
        failure(run(SELECTED, "wrongindex", "goals", URL, 2, 4194304, 3, 15000), "current_page");

        fixture.script(
                Collections.singletonMap(GOALS, response("goals", 1, 1, 2, false, item("x"))));
        failure(
                run(SELECTED, "shorttotal", "goals", URL, 2, 4194304, 3, 15000),
                "pagination totals");

        fixture.script(Collections.singletonMap(GOALS, response("goals", 1, 2, 2, false)));
        failure(run(SELECTED, "earlyempty", "goals", URL, 2, 4194304, 3, 15000), "empty");

        fixture.script(
                Collections.singletonMap(
                        GOALS,
                        (request, attempt) ->
                                AhaHttpsFixture.json(
                                        request,
                                        "{\"goals\":[],\"truncated\":true,\"has_more\":false,\"pagination\":"
                                                + "{\"current_page\":1,\"total_pages\":0,\"total_records\":0}}")));
        failure(run(SELECTED, "truncated", "goals", URL, 2, 4194304, 3, 15000), "truncated");
        assertEquals(1, fixture.hits(GOALS));
    }

    @Test
    void refusesRedirectsAndAnUntrustedTenantCertificate() throws Exception {
        fixture.script(
                Collections.singletonMap(
                        GOALS,
                        (request, attempt) -> {
                            request.getResponseHeaders()
                                    .add("Location", "https://fixture.aha.io:8444" + GOALS);
                            AhaHttpsFixture.status(request, 302);
                        }));
        failure(run(SELECTED, "redirect", "goals", URL, 2, 4194304, 3, 15000), "HTTP 302");
        assertEquals(1, fixture.hits(GOALS));
        assertEquals(0, untrusted.hits(GOALS));

        failure(
                run(
                        SELECTED,
                        "untrusted",
                        "goals",
                        "https://fixture.aha.io:8444",
                        2,
                        4194304,
                        3,
                        15000),
                "HTTPS or TLS exchange failed");
        assertEquals(0, untrusted.hits(GOALS));

        failure(
                run(
                        SELECTED,
                        "suffix",
                        "goals",
                        "https://fixture.aha.io.foreign.invalid",
                        2,
                        4194304,
                        3,
                        15000),
                "api_url must be an HTTPS origin");
        assertEquals(1, fixture.hits(GOALS));
    }

    @Test
    void failsOnForbiddenResponseWithoutRetryOrLeakingTheResponse() throws Exception {
        fixture.script(
                Collections.singletonMap(
                        GOALS,
                        (request, attempt) -> {
                            byte[] privateData =
                                    "provider-private-idea-title"
                                            .getBytes(java.nio.charset.StandardCharsets.UTF_8);
                            request.sendResponseHeaders(403, privateData.length);
                            request.getResponseBody().write(privateData);
                        }));
        Container.ExecResult rejected =
                run(SELECTED, "forbidden", "goals", URL, 2, 4194304, 3, 15000);
        failure(rejected, "HTTP 403");
        assertFalse(output(rejected).contains("provider-private-idea-title"));
        assertEquals(1, fixture.hits(GOALS));
    }

    @Test
    void retriesRateLimitsWithinBudgetAndBoundsAttemptsAndRetryDelay() throws Exception {
        fixture.script(
                Collections.singletonMap(
                        GOALS,
                        (request, attempt) -> {
                            if (attempt == 1) {
                                request.getResponseHeaders()
                                        .add(
                                                "X-Ratelimit-Reset",
                                                String.valueOf(Instant.now().getEpochSecond() + 1));
                                request.getResponseHeaders().add("Retry-After", "1");
                                AhaHttpsFixture.status(request, 429);
                            } else if (attempt == 2) {
                                request.getResponseHeaders()
                                        .add(
                                                "Retry-After",
                                                DateTimeFormatter.RFC_1123_DATE_TIME.format(
                                                        Instant.now()
                                                                .plusSeconds(2)
                                                                .atOffset(ZoneOffset.UTC)));
                                AhaHttpsFixture.status(request, 503);
                            } else {
                                response("goals", 1, 1, 1, false, item("after-limit"))
                                        .send(request, attempt);
                            }
                        }));
        success(run(SELECTED, "rate", "goals", URL, 2, 4194304, 3, 15000));
        assertEquals(3, fixture.hits(GOALS));
        rows("rate", Collections.singletonMap("goals|null|after-limit", item("after-limit")));

        fixture.script(
                Collections.singletonMap(
                        GOALS, (request, attempt) -> AhaHttpsFixture.status(request, 429)));
        failure(
                run(SELECTED, "maxretry", "goals", URL, 2, 4194304, 2, 15000),
                "retry limit reached");
        assertEquals(2, fixture.hits(GOALS));

        fixture.script(
                Collections.singletonMap(
                        GOALS,
                        (request, attempt) -> {
                            request.getResponseHeaders().add("Retry-After", "90");
                            AhaHttpsFixture.status(request, 429);
                        }));
        failure(
                run(SELECTED, "delay", "goals", URL, 2, 4194304, 3, 10000),
                "retry delay exceeds request deadline");
        assertEquals(1, fixture.hits(GOALS));
    }

    @Test
    void acceptsGzipAndRejectsOversizedOrAmbiguousJsonBytes() throws Exception {
        fixture.script(
                Collections.singletonMap(
                        GOALS,
                        (request, attempt) ->
                                AhaHttpsFixture.gzip(
                                        request, page("goals", 1, 1, 1, false, item("zip")))));
        success(run(SELECTED, "gzip", "goals", URL, 2, 1024, 3, 15000));
        rows("gzip", Collections.singletonMap("goals|null|zip", item("zip")));

        StringBuilder large = new StringBuilder("{\"id\":\"zip\",\"title\":\"");
        for (int i = 0; i < 2048; i++) {
            large.append('x');
        }
        large.append("\"}");
        fixture.script(
                Collections.singletonMap(
                        GOALS,
                        (request, attempt) ->
                                AhaHttpsFixture.gzip(
                                        request, page("goals", 1, 1, 1, false, large.toString()))));
        failure(run(SELECTED, "bodylimit", "goals", URL, 2, 1024, 3, 15000), "max_response_bytes");
        assertEquals(1, fixture.hits(GOALS));

        fixture.script(
                Collections.singletonMap(
                        GOALS,
                        (request, attempt) ->
                                AhaHttpsFixture.json(
                                        request, "{\"goals\":[],\"goals\":[],\"pagination\":{}}")));
        failure(
                run(SELECTED, "duplicate", "goals", URL, 2, 4194304, 3, 15000),
                "invalid, incomplete, or unreadable JSON");
    }

    @Test
    void totalDeadlineStopsATrickleBelowTheReadIdleTimeout() throws Exception {
        CountDownLatch disconnected = new CountDownLatch(1);
        fixture.script(
                Collections.singletonMap(
                        GOALS,
                        (request, attempt) -> {
                            request.sendResponseHeaders(200, 0);
                            try (OutputStream body = request.getResponseBody()) {
                                for (int i = 0; i < 150; i++) {
                                    body.write(' ');
                                    body.flush();
                                    TimeUnit.MILLISECONDS.sleep(100);
                                }
                            } catch (IOException e) {
                                disconnected.countDown();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }));
        failure(
                run(SELECTED, "trickle", "goals", URL, 2, 4194304, 3, 1000),
                "request deadline exceeded");
        assertTrue(disconnected.await(10, TimeUnit.SECONDS));
        assertEquals(1, fixture.hits(GOALS));
    }

    @Test
    void abortsJobWhileARequestIsHeldWithoutWaitingForItsDeadline() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        fixture.script(Collections.singletonMap(GOALS, (request, attempt) -> held(release)));
        ExecutorService client = Executors.newSingleThreadExecutor();
        String job = jobId();
        Future<Container.ExecResult> running =
                client.submit(
                        () ->
                                engine.executeJob(
                                        SELECTED,
                                        job,
                                        variables("cancel", "goals", URL, 2, 4194304, 3, 120000)));
        try {
            awaitHit(GOALS);
            success(engine.cancelJob(job));
            running.get(30, TimeUnit.SECONDS);
            Awaitility.await()
                    .atMost(20, TimeUnit.SECONDS)
                    .until(() -> "CANCELED".equals(engine.getJobStatus(job)));
            assertEquals(1, fixture.hits(GOALS));
        } finally {
            release.countDown();
            running.cancel(true);
            client.shutdownNow();
        }
    }

    @Test
    void savesAndRestoresNextChildPageWhileDiscoveryAndPriorResourcesStayCompleted()
            throws Exception {
        String features = target("/features", 1, 2);
        String childOne = target("/products/p1/idea_categories", 1, 2);
        String childTwo = target("/products/p1/idea_categories", 2, 2);
        String childOther = target("/products/p2/idea_categories", 1, 2);
        Map<String, AhaHttpsFixture.Reply> replies = new HashMap<>();
        CountDownLatch release = new CountDownLatch(1);
        replies.put(features, response("features", 1, 1, 1, false, item("finished")));
        replies.put(PRODUCTS, response("products", 1, 1, 2, false, item("p1"), item("p2")));
        replies.put(childOne, response("idea_categories", 1, 2, 3, false, item("a"), item("b")));
        replies.put(
                childTwo,
                (request, attempt) -> {
                    if (attempt == 1) {
                        held(release);
                    } else {
                        response("idea_categories", 2, 2, 3, true, item("c"))
                                .send(request, attempt);
                    }
                });
        replies.put(childOther, response("idea_categories", 1, 1, 0, false));
        fixture.script(replies);
        String job = jobId();
        String[] vars = variables("resume", "goals", URL, 2, 4194304, 3, 120000);
        ExecutorService client = Executors.newSingleThreadExecutor();
        Future<Container.ExecResult> running =
                client.submit(() -> engine.executeJob(RESUME, job, vars));
        try {
            awaitHit(childTwo);
            long previous = engine.getCompletedCheckpointCount(job);
            Awaitility.await()
                    .atMost(20, TimeUnit.SECONDS)
                    .until(() -> engine.getCompletedCheckpointCount(job) > previous);
            success(engine.savepointJob(job));
            running.get(30, TimeUnit.SECONDS);
            release.countDown();

            failure(
                    engine.restoreJob(
                            RESUME,
                            job,
                            variables("wrongscope", "goals", URL, 3, 4194304, 3, 120000)),
                    "incompatible checkpoint parameters");
            assertEquals(1, fixture.hits(childTwo));
            success(engine.restoreJob(RESUME, job, vars));
            Map<String, String> expected = new HashMap<>();
            expect(expected, "features", null, "finished");
            expect(expected, "idea_categories", "p1", "a", "b", "c");
            rows("resume", expected);
            assertEquals(1, fixture.hits(features));
            assertEquals(1, fixture.hits(PRODUCTS));
            assertEquals(1, fixture.hits(childOne));
            assertEquals(2, fixture.hits(childTwo));
            assertEquals(1, fixture.hits(childOther));
        } finally {
            release.countDown();
            running.cancel(true);
            client.shutdownNow();
        }
    }

    private Container.ExecResult run(
            String config,
            String prefix,
            String resource,
            String url,
            int pageSize,
            int maxBytes,
            int attempts,
            int deadline)
            throws Exception {
        return engine.executeJob(
                config,
                Arrays.asList(
                        variables(prefix, resource, url, pageSize, maxBytes, attempts, deadline)));
    }

    private static String[] variables(
            String prefix,
            String resource,
            String url,
            int pageSize,
            int maxBytes,
            int attempts,
            int deadline) {
        return new String[] {
            "job.output=" + prefix,
            "aha.resource=" + resource,
            "aha.url=" + url,
            "aha.token=" + AhaHttpsFixture.TOKEN,
            "aha.size=" + pageSize,
            "aha.bytes=" + maxBytes,
            "aha.attempts=" + attempts,
            "aha.deadline=" + deadline
        };
    }

    private static void held(CountDownLatch latch) throws IOException {
        try {
            if (!latch.await(120, TimeUnit.SECONDS)) {
                throw new IOException("fixture was held beyond its test budget");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void awaitHit(String target) {
        Awaitility.await().atMost(40, TimeUnit.SECONDS).until(() -> fixture.hits(target) == 1);
    }

    private static String jobId() {
        return Long.toString(
                ThreadLocalRandom.current().nextLong(1_000_000_000_000L, 9_000_000_000_000L));
    }

    private static String target(String route, int page, int size) {
        return "/api/v1" + route + "?page=" + page + "&per_page=" + size;
    }

    private static String item(String id) {
        return "{\"id\":\"" + id + "\"}";
    }

    private static AhaHttpsFixture.Reply response(
            String resource, int page, int pages, int total, boolean wrapped, String... items) {
        return (request, attempt) ->
                AhaHttpsFixture.json(request, page(resource, page, pages, total, wrapped, items));
    }

    private static String page(
            String resource, int page, int pages, int total, boolean wrapped, String... items) {
        String pagination =
                "{\"current_page\":"
                        + page
                        + ",\"total_pages\":"
                        + pages
                        + ",\"total_records\":"
                        + total
                        + "}";
        return "{\""
                + resource
                + "\":["
                + String.join(",", items)
                + "],\"pagination\":"
                + (wrapped ? "[" + pagination + "]" : pagination)
                + "}";
    }

    private static void addParents(
            Map<String, AhaHttpsFixture.Reply> replies,
            String key,
            String route,
            String first,
            String second,
            String third) {
        replies.put(target(route, 1, 2), response(key, 1, 2, 3, false, item(first), item(second)));
        replies.put(target(route, 2, 2), response(key, 2, 2, 3, true, item(third)));
    }

    private static void expect(
            Map<String, String> expected, String resource, String parent, String... ids) {
        for (String id : ids) {
            expected.put(resource + "|" + parent + "|" + id, item(id));
        }
    }

    private void rows(String prefix, Map<String, String> expected) throws Exception {
        Map<String, JsonNode> actual = new HashMap<>();
        for (String line : engine.readRows(prefix)) {
            JsonNode row = JSON.readTree(line);
            String key =
                    row.path("resource").asText()
                            + "|"
                            + row.path("parent_id").asText("null")
                            + "|"
                            + row.path("record_id").asText();
            JsonNode data = JSON.readTree(row.path("data").textValue());
            assertNull(actual.put(key, data), "duplicate persisted row " + key);
            if ("9223372036854775808".equals(row.path("record_id").asText())) {
                assertTrue(row.path("data").textValue().contains("12.34000"));
            }
        }
        assertEquals(expected.keySet(), actual.keySet());
        for (Map.Entry<String, String> entry : expected.entrySet()) {
            assertEquals(
                    JSON.readTree(entry.getValue()), actual.get(entry.getKey()), entry.getKey());
        }
    }

    private static String output(Container.ExecResult result) {
        return result.getStdout() + result.getStderr();
    }

    private static void success(Container.ExecResult result) {
        assertEquals(0, result.getExitCode(), () -> output(result));
    }

    private static void failure(Container.ExecResult result, String reason) {
        assertNotEquals(0, result.getExitCode(), () -> output(result));
        assertTrue(output(result).contains(reason), () -> output(result));
        assertFalse(output(result).contains(AhaHttpsFixture.TOKEN));
    }

    private static final class Engine extends SeaTunnelContainer {
        private final Path trust;

        private Engine(Path trust) {
            this.trust = trust;
        }

        @Override
        protected String getJavaToolOptions() {
            return "-Djavax.net.ssl.trustStore="
                    + TRUST
                    + " -Djavax.net.ssl.trustStorePassword="
                    + AhaHttpsFixture.STORE_PASSWORD
                    + " -Djavax.net.ssl.trustStoreType=JKS";
        }

        @Override
        protected void executeExtraCommands(GenericContainer<?> runtime) {
            runtime.withCopyFileToContainer(MountableFile.forHostPath(trust), TRUST);
        }

        private List<String> readRows(String prefix) throws Exception {
            String path = "/tmp/seatunnel_mnt/aha-" + prefix;
            Container.ExecResult result =
                    server.execInContainer(
                            "sh",
                            "-c",
                            "if [ -d "
                                    + path
                                    + " ]; then find "
                                    + path
                                    + " -maxdepth 1 -type f -name 'receipt_*' "
                                    + "-exec sh -c 'for f do cat \"$f\"; printf \"\\n\"; done' sh {} +; fi");
            success(result);
            List<String> rows = new ArrayList<>();
            for (String line : result.getStdout().split("\\r?\\n")) {
                if (!line.trim().isEmpty()) {
                    rows.add(line);
                }
            }
            return rows;
        }
    }
}
