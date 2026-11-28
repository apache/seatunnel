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
package org.apache.seatunnel.e2e.connector.breezyhr;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.breezyhr.BreezyHrRecoveryProbe;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.seatunnel.SeaTunnelContainer;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.net.URLDecoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs Breezy HR through a real SeaTunnel Zeta engine and an isolated Java TLS account.
 *
 * <p>Tests compare page numbers and bounds in captured GET requests and inspect LocalFile's
 * committed JSON rows. The provider peer never calls the real billable API.
 */
class BreezyHrIT extends TestSuiteBase {
    private static final ObjectMapper JSON =
            new ObjectMapper()
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    private static final String TOKEN = "breezy_pat_fixture-private+/=?";
    private static final String HIDDEN_MESSAGE = "private-provider-response-token";
    private static final String HOST = "breezyhr-peer";
    private static final String PEM = "/tmp/breezyhr.pem";
    private static final String OTHER_PEM = "/tmp/unrelated-breezyhr.pem";
    private static final String ENDPOINT = "https://" + HOST + ":18443";
    private static final String SESSION = "fixture-session-private+/=?";
    private static final String COMPANY = "6a1ef2bc42b0";
    private static final String PREFIX = "/v3/company/" + COMPANY;
    private static final String POSITIONS = PREFIX + "/positions";
    private static final String PIPELINES = PREFIX + "/pipelines";
    private static final List<String> ALL = List.of("positions", "candidates", "pipelines");

    private final Engine engine = new Engine();
    private final ExecutorService submissions = Executors.newSingleThreadExecutor();
    private final HttpClient testControl =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private GenericContainer<?> peer;
    private Path certificate;

    /**
     * Provisions independent certificates, the protocol peer and the engine.
     *
     * <p>The unrelated certificate lets a negative test use a genuine trust failure with the
     * expected HTTPS endpoint still running.
     */
    @BeforeAll
    void boot(@TempDir Path testFiles) throws Exception {
        String runner = DockerClientFactory.instance().dockerHostIpAddress();
        assertTrue(runner.matches("[A-Za-z0-9.:-]+"));
        String runnerType = runner.matches("[0-9.]+") || runner.contains(":") ? "ip:" : "dns:";
        String create =
                "keytool -genkeypair -alias provider -storetype PKCS12 -storepass breezyhr-fixture "
                        + "-keyalg RSA -keysize 2048 -validity 30 -dname CN="
                        + HOST
                        + " -ext SAN=dns:"
                        + HOST
                        + ",dns:breezyhr-redirect-host,"
                        + runnerType
                        + runner
                        + " -keystore ";
        String export =
                "keytool -exportcert -rfc -alias provider -storepass breezyhr-fixture -keystore ";
        String entry =
                create
                        + "/tmp/provider.p12 && "
                        + export
                        + "/tmp/provider.p12 -file "
                        + PEM
                        + " && "
                        + create
                        + "/tmp/unrelated.p12 && "
                        + export
                        + "/tmp/unrelated.p12 -file "
                        + OTHER_PEM
                        + " && exec java --add-modules jdk.httpserver -cp /tmp "
                        + BreezyHrPeer.class.getName()
                        + " /tmp/provider.p12";
        String providerClass = "/" + BreezyHrPeer.class.getName().replace('.', '/') + ".class";
        byte[] bytecode;
        try (InputStream bits = BreezyHrPeer.class.getResourceAsStream(providerClass)) {
            if (bits == null) {
                throw new IllegalStateException("Missing packaged HTTPS fixture");
            }
            bytecode = bits.readAllBytes();
        }
        peer =
                new GenericContainer<>(DockerImageName.parse("eclipse-temurin:11-jdk"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST, "breezyhr-other-host", "breezyhr-redirect-host")
                        .withExposedPorts(18080, 18443)
                        .withCopyToContainer(Transferable.of(bytecode), "/tmp" + providerClass)
                        .withCommand("sh", "-c", entry)
                        .waitingFor(
                                Wait.forHttp("/requests")
                                        .forPort(18080)
                                        .withStartupTimeout(Duration.ofMinutes(2)));
        peer.start();
        engine.startUp();
        String testedCertificate = copyCertificate(PEM);
        copyCertificate(OTHER_PEM);
        certificate = Files.writeString(testFiles.resolve("fixture.pem"), testedCertificate);
    }

    /**
     * Stops both owned containers and async submissions after the suite.
     *
     * <p>The provider is still released if teardown of Zeta reports an error.
     */
    @AfterAll
    void stop() throws Exception {
        submissions.shutdownNow();
        try {
            engine.tearDown();
        } finally {
            if (peer != null) {
                peer.stop();
            }
        }
    }

    @Test
    void readsAllResourcesAcrossFullAndShortParentAndChildPages() throws Exception {
        List<String> tape = new ArrayList<>();
        List<Seen> expectedWire = new ArrayList<>();
        List<ObjectNode> expectedRows = new ArrayList<>();
        String specialParent = "position- 01";
        String specialRoute = candidates("position-%2001");
        ObjectNode special = position(specialParent);
        special.putObject("type").put("name", "全职");
        special.putObject("location").putObject("country").put("id", "CN").put("name", "中国");
        special.putNull("application_form");
        ArrayNode parents = rows(special);
        expectedRows.add(
                out("positions", specialParent, specialParent, "全职", "CN", "中国", null, special));
        for (int n = 2; n <= 50; n++) {
            ObjectNode parent = position("position-" + n);
            parents.add(parent);
            expectedRows.add(positionOut(parent));
        }
        tape.add(ok(POSITIONS, parents));
        expectedWire.add(wire(POSITIONS, 50, 1));

        ObjectNode shared = candidate("same-candidate");
        shared.putObject("stage").put("name", "面试");
        shared.putObject("overall_score")
                .put("score", new BigDecimal("123456789012345678.123456789000"))
                .put("count", new BigInteger("9223372036854775808"));
        shared.putObject("resume").put("url", "https://breezyhr-redirect-host:18443/unread");
        ArrayNode children = rows(shared);
        expectedRows.add(
                out("candidates", "same-candidate", specialParent, null, null, null, "面试", shared));
        for (int n = 2; n <= 50; n++) {
            ObjectNode child = candidate("candidate-" + n);
            children.add(child);
            expectedRows.add(candidateOut(specialParent, child));
        }
        ObjectNode last = candidate("candidate-51");
        expectedRows.add(candidateOut(specialParent, last));
        tape.add(ok(specialRoute, children));
        tape.add(ok(specialRoute, rows(last)));
        tape.add(ok(specialRoute, rows()));
        expectedWire.add(wire(specialRoute, 50, 1));
        expectedWire.add(wire(specialRoute, 50, 2));
        expectedWire.add(wire(specialRoute, 50, 3));

        ObjectNode secondParentShared = candidate("same-candidate");
        expectedRows.add(candidateOut("position-2", secondParentShared));
        String secondRoute = candidates("position-2");
        tape.add(ok(secondRoute, rows(secondParentShared)));
        tape.add(ok(secondRoute, rows()));
        expectedWire.add(wire(secondRoute, 50, 1));
        expectedWire.add(wire(secondRoute, 50, 2));

        for (int n = 3; n <= 50; n++) {
            String route = candidates("position-" + n);
            tape.add(ok(route, rows()));
            expectedWire.add(wire(route, 50, 1));
        }
        ObjectNode late = position("position-51");
        expectedRows.add(positionOut(late));
        tape.add(ok(POSITIONS, rows(late)));
        tape.add(ok(candidates("position-51"), rows()));
        tape.add(ok(POSITIONS, rows()));
        expectedWire.add(wire(POSITIONS, 50, 2));
        expectedWire.add(wire(candidates("position-51"), 50, 1));
        expectedWire.add(wire(POSITIONS, 50, 3));

        ObjectNode primary = pipeline("p1");
        primary.withArray("pipeline")
                .addObject()
                .put("id", "applied")
                .put("name", "已申请")
                .putObject("actions")
                .putNull("templates");
        ObjectNode pool = pipeline("p2");
        ObjectNode map = options();
        map.set("候选池", pool);
        map.set("default", primary);
        tape.add(ok(PIPELINES, map));
        expectedRows.add(pipelineOut("default", primary));
        expectedRows.add(pipelineOut("候选池", pool));
        expectedWire.add(wire(PIPELINES, 0, 0));

        reset(tape.toArray(new String[0]));
        try (Job job = job(ALL, options(), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, expectedRows);
        }
        assertTrace(expectedWire.toArray(new Seen[0]));
    }

    @Test
    void discoversParentPagesWithoutEmittingUnselectedPositions() throws Exception {
        String empty = candidates("empty");
        String later = candidates("later");
        ObjectNode child = candidate("only-child");
        reset(
                ok(POSITIONS, rows(position("empty"))),
                ok(empty, rows()),
                ok(POSITIONS, rows(position("later"))),
                ok(later, rows(child)),
                ok(later, rows()),
                ok(POSITIONS, rows()));
        try (Job job = job(List.of("candidates"), options().put("page_size", 2), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of(candidateOut("later", child)));
        }
        assertTrace(
                wire(POSITIONS, 2, 1),
                wire(empty, 2, 1),
                wire(POSITIONS, 2, 2),
                wire(later, 2, 1),
                wire(later, 2, 2),
                wire(POSITIONS, 2, 3));
    }

    @Test
    void readsOnlySelectedPositionsAndUsesTheBareSessionTokenWhenConfigured() throws Exception {
        ObjectNode parent = position("without-child-scope");
        reset(ok(POSITIONS, rows(parent)), ok(POSITIONS, rows()));
        try (Job job = job(List.of("positions"), options().put("token", SESSION), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of(positionOut(parent)));
        }
        Seen first = wire(POSITIONS, 50, 1);
        Seen second = wire(POSITIONS, 50, 2);
        assertTrace(
                new Seen(first.route, first.query, 0L, "session"),
                new Seen(second.route, second.query, 0L, "session"));
    }

    @Test
    void permitsEmptyInitialListsAndReadsThePipelineMapWithoutListParameters() throws Exception {
        ObjectNode value = pipeline("pipeline-id");
        ObjectNode map = options();
        map.set("default", value);
        reset(ok(POSITIONS, rows()), ok(PIPELINES, map));
        try (Job job = job(ALL, options(), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of(pipelineOut("default", value)));
        }
        assertTrace(wire(POSITIONS, 50, 1), wire(PIPELINES, 0, 0));

        reset(ok(PIPELINES, options()));
        try (Job job = job(List.of("pipelines"), options(), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of());
        }
        assertTrace(wire(PIPELINES, 0, 0));
    }

    @Test
    void validatesEachArrayCompletelyBeforeAnyRowsOrChildrenArePublished() throws Exception {
        String valid = position("good").toString();
        rejectOne(
                ok(POSITIONS, options().put("error", HIDDEN_MESSAGE)),
                ALL,
                options(),
                wire(POSITIONS, 50, 1),
                "expected a JSON array");
        rejectOne(
                ok(POSITIONS, rows(position("good"), options())),
                ALL,
                options(),
                wire(POSITIONS, 50, 1),
                "expected a non-empty textual _id");
        rejectOne(
                ok(POSITIONS, rows(position("good"), JSON.getNodeFactory().nullNode())),
                ALL,
                options(),
                wire(POSITIONS, 50, 1),
                "expected each list record");
        rejectOne(
                ok(POSITIONS, rows(position("a"), position("b"))),
                ALL,
                options().put("page_size", 1),
                wire(POSITIONS, 1, 1),
                "no larger than page_size");
        rejectOne(
                frame(
                        POSITIONS,
                        200,
                        "plain",
                        "",
                        "",
                        ("[" + valid + "] " + HIDDEN_MESSAGE).getBytes(StandardCharsets.UTF_8)),
                ALL,
                options(),
                wire(POSITIONS, 50, 1),
                "single valid UTF-8 JSON");
        rejectOne(
                frame(
                        POSITIONS,
                        200,
                        "plain",
                        "",
                        "",
                        "[{\"_id\":\"a\",\"_id\":\"b\"}]".getBytes(StandardCharsets.UTF_8)),
                ALL,
                options(),
                wire(POSITIONS, 50, 1),
                "single valid UTF-8 JSON");
    }

    @Test
    void rejectsAnAuthenticationErrorEnvelopeOrMalformedPipelineValue() throws Exception {
        rejectOne(
                ok(PIPELINES, rows(pipeline("one"))),
                List.of("pipelines"),
                options(),
                wire(PIPELINES, 0, 0),
                "expected a JSON pipeline map");
        ObjectNode invalid = options();
        invalid.set("default", pipeline("valid"));
        invalid.putObject("error").put("message", HIDDEN_MESSAGE);
        rejectOne(
                ok(PIPELINES, invalid),
                List.of("pipelines"),
                options(),
                wire(PIPELINES, 0, 0),
                "expected a named pipeline object");
    }

    @Test
    void rejectsUnsafeProviderPositionPathBeforeRequestingAnyCandidates() throws Exception {
        for (String invalid : List.of("..", "a/b", "a%2Fhidden")) {
            rejectOne(
                    ok(POSITIONS, rows(position("good"), position(invalid))),
                    ALL,
                    options(),
                    wire(POSITIONS, 50, 1),
                    "position identifier is unsafe");
        }
    }

    @Test
    void detectsAdjacentOverlapAndAnEarlierPageRepeatedInAnotherOrder() throws Exception {
        ObjectNode a = position("a");
        ObjectNode b = position("b");
        ObjectNode c = position("c");
        rejectOne(
                ok(POSITIONS, rows(a, a)),
                List.of("positions"),
                options(),
                wire(POSITIONS, 50, 1),
                "duplicate identifier");
        reset(ok(POSITIONS, rows(a, b)), ok(POSITIONS, rows(b, c)));
        failJob(List.of("positions"), options(), "duplicate identifier");
        assertTrace(wire(POSITIONS, 50, 1), wire(POSITIONS, 50, 2));

        reset(ok(POSITIONS, rows(a, b)), ok(POSITIONS, rows(c)), ok(POSITIONS, rows(b, a)));
        failJob(List.of("positions"), options(), "provider repeated a prior page");
        assertTrace(wire(POSITIONS, 50, 1), wire(POSITIONS, 50, 2), wire(POSITIONS, 50, 3));
    }

    @Test
    void boundsPageAndScanCountsBeforeOpeningAnotherRequest() throws Exception {
        reset(ok(POSITIONS, rows(position("one"))));
        failJob(
                List.of("positions", "pipelines"),
                options().put("max_pages_per_scope", 1),
                "max_pages_per_scope exhausted");
        assertTrace(wire(POSITIONS, 50, 1));

        String child = candidates("one");
        reset(ok(POSITIONS, rows(position("one"))), ok(child, rows()));
        failJob(ALL, options().put("max_requests", 2), "max_requests exhausted");
        assertTrace(wire(POSITIONS, 50, 1), wire(child, 50, 1));
    }

    @Test
    void failsAccessDenialsAndUnexpectedSuccessStatusesWithoutReplay() throws Exception {
        for (int status : List.of(401, 403, 404, 204, 206)) {
            rejectOne(
                    reply(
                            POSITIONS,
                            status,
                            "plain",
                            "",
                            "",
                            options().put("message", HIDDEN_MESSAGE)),
                    ALL,
                    options(),
                    wire(POSITIONS, 50, 1),
                    "HTTP " + status);
        }
        for (int status : List.of(401, 403)) {
            String child = candidates("denied");
            reset(
                    ok(POSITIONS, rows(position("denied"))),
                    reply(
                            child,
                            status,
                            "plain",
                            "",
                            "",
                            options().put("message", HIDDEN_MESSAGE)));
            try (Job job = job(List.of("candidates", "pipelines"), options(), false, false)) {
                bad(
                        engine.executeJob(job.configName),
                        "candidates page 1: access denied (HTTP " + status + ")");
                assertOutput(job, List.of());
            }
            assertTrace(wire(POSITIONS, 50, 1), wire(child, 50, 1));
        }
    }

    @Test
    void honorsTheLargerOfRetryAfterAndTheProviderResetTimestamp() throws Exception {
        for (String resetFormat : List.of("future-ms", "future-seconds")) {
            reset(
                    reply(PIPELINES, 429, "plain", "1", resetFormat, options()),
                    ok(PIPELINES, options()));
            try (Job job =
                    job(List.of("pipelines"), options().put("max_attempts", 2), false, false)) {
                good(engine.executeJob(job.configName));
                assertOutput(job, List.of());
            }
            assertTrace(wire(PIPELINES, 0, 0), wire(PIPELINES, 0, 0));
            List<Seen> observed = seen();
            long minimum = "future-ms".equals(resetFormat) ? 1350L : 1950L;
            assertTrue(
                    TimeUnit.NANOSECONDS.toMillis(observed.get(1).at - observed.get(0).at)
                            >= minimum);
        }
    }

    @Test
    void failsUnknownOrOutOfBudgetThrottleTimesAndAcceptsAConfirmedZero() throws Exception {
        rejectOne(
                reply(PIPELINES, 429, "plain", "", "", options()),
                List.of("pipelines"),
                options(),
                wire(PIPELINES, 0, 0),
                "without a confirmed retry delay");
        rejectOne(
                reply(PIPELINES, 429, "plain", "", "42", options()),
                List.of("pipelines"),
                options(),
                wire(PIPELINES, 0, 0),
                "invalid retry timing");
        rejectOne(
                reply(PIPELINES, 429, "plain", "90", "", options()),
                List.of("pipelines"),
                options(),
                wire(PIPELINES, 0, 0),
                "retry delay exceeds request deadline");

        reset(reply(PIPELINES, 429, "plain", "0", "", options()), ok(PIPELINES, options()));
        try (Job job = job(List.of("pipelines"), options().put("max_attempts", 2), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of());
        }
        assertTrace(wire(PIPELINES, 0, 0), wire(PIPELINES, 0, 0));
    }

    @Test
    void neverReissuesAServiceUnavailableResponseOutsideTheExplicitAttemptBudget()
            throws Exception {
        rejectOne(
                reply(PIPELINES, 503, "plain", "0", "", options()),
                List.of("pipelines"),
                options().put("max_attempts", 1),
                wire(PIPELINES, 0, 0),
                "HTTP 503 retry budget exhausted");
    }

    @Test
    void handlesDecodedGzipUtf8AndTheBodyBound() throws Exception {
        ObjectNode parent = position("unicode");
        reset(reply(POSITIONS, 200, "gzip", "", "", rows(parent)), ok(POSITIONS, rows()));
        try (Job job = job(List.of("positions"), options(), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of(positionOut(parent)));
        }
        assertTrace(wire(POSITIONS, 50, 1), wire(POSITIONS, 50, 2));

        ObjectNode oversize = position("oversize").put("long", "x".repeat(8000));
        rejectOne(
                reply(POSITIONS, 200, "gzip", "", "", rows(oversize)),
                List.of("positions"),
                options().put("max_response_bytes", 1024),
                wire(POSITIONS, 50, 1),
                "max_response_bytes");
        rejectOne(
                reply(POSITIONS, 200, "unsupported", "", "", rows(parent)),
                List.of("positions"),
                options(),
                wire(POSITIONS, 50, 1),
                "unsupported response compression");
        rejectOne(
                reply(POSITIONS, 200, "binary", "", "", rows(parent)),
                List.of("positions"),
                options(),
                wire(POSITIONS, 50, 1),
                "expected Content-Type application/json");
        rejectOne(
                frame(POSITIONS, 200, "plain", "", "", new byte[] {'[', (byte) 0xFF, ']'}),
                List.of("positions"),
                options(),
                wire(POSITIONS, 50, 1),
                "single valid UTF-8 JSON");
    }

    @Test
    void retriesATruncatedGetWithoutPublishingAResponsePrefix() throws Exception {
        ObjectNode lost = position("truncated-prefix");
        ObjectNode whole = position("whole-response");
        reset(
                reply(POSITIONS, 200, "truncated", "", "", rows(lost)),
                ok(POSITIONS, rows(whole)),
                ok(POSITIONS, rows()));
        try (Job job = job(List.of("positions"), options().put("max_attempts", 2), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of(positionOut(whole)));
        }
        assertTrace(wire(POSITIONS, 50, 1), wire(POSITIONS, 50, 1), wire(POSITIONS, 50, 2));
    }

    @Test
    void requiresExpectedCertificateAndHostnameAndDoesNotFollowARedirect() throws Exception {
        reset();
        failJob(ALL, options().put("trust_certificate", OTHER_PEM), "TLS certificate or hostname");
        assertTrace();
        failJob(
                ALL,
                options().put("endpoint", "https://breezyhr-other-host:18443"),
                "TLS certificate or hostname");
        assertTrace();
        rejectOne(
                reply(POSITIONS, 302, "redirect", "", "", options()),
                ALL,
                options(),
                wire(POSITIONS, 50, 1),
                "HTTP 302");
    }

    @Test
    void stopsAContinuousBodyAtTheTotalDeadline() throws Exception {
        rejectOne(
                stall(POSITIONS),
                ALL,
                options().put("request_timeout_ms", 2500),
                wire(POSITIONS, 50, 1),
                "request deadline exceeded");
        await(() -> trace().contains("disconnect\t" + POSITIONS), "deadline closed connection");
    }

    @Test
    void cancelsTheActiveCandidateTransferWithoutOpeningSubsequentScopes() throws Exception {
        String child = candidates("pending");
        reset(ok(POSITIONS, rows(position("pending"))), stall(child));
        try (Job job =
                job(
                        List.of("candidates", "pipelines"),
                        options().put("request_timeout_ms", 60000),
                        true,
                        false)) {
            Future<Container.ExecResult> running = start(job, job.id);
            try {
                await(() -> seen().size() == 2, "blocked candidate response");
            } finally {
                cancel(job.id, running);
            }
            await(() -> trace().contains("disconnect\t" + child), "cancelled candidate connection");
            assertOutput(job, List.of());
        }
        assertTrace(wire(POSITIONS, 50, 1), wire(child, 50, 1));
    }

    @Test
    void restoresANestedCandidatePageAfterItsParentAndFirstChildPageAreCommitted()
            throws Exception {
        ObjectNode parent = position("restore");
        ObjectNode first = candidate("first");
        ObjectNode second = candidate("second");
        ObjectNode value = pipeline("restore-pipeline");
        ObjectNode map = options();
        map.set("primary", value);
        String child = candidates("restore");
        reset(ok(POSITIONS, rows(parent)), ok(child, rows(first)), stall(child));
        List<ObjectNode> before = List.of(positionOut(parent), candidateOut("restore", first));
        try (Job job = job(ALL, options().put("request_timeout_ms", 60000), true, false)) {
            saveActive(job, 3, before, child);
            queue(
                    ok(child, rows(second)),
                    ok(child, rows()),
                    ok(POSITIONS, rows()),
                    ok(PIPELINES, map));
            good(engine.restoreJob(job.configName, job.id));
            assertOutput(
                    job,
                    List.of(
                            positionOut(parent),
                            candidateOut("restore", first),
                            candidateOut("restore", second),
                            pipelineOut("primary", value)));
        }
        assertTrace(
                wire(POSITIONS, 50, 1),
                wire(child, 50, 1),
                wire(child, 50, 2),
                wire(child, 50, 2),
                wire(child, 50, 3),
                wire(POSITIONS, 50, 2),
                wire(PIPELINES, 0, 0));
    }

    @Test
    void rejectsCompanyDriftBeforeAResumedReaderCanOpenTransport() throws Exception {
        reset(stall(POSITIONS));
        try (Job original = job(ALL, options().put("request_timeout_ms", 60000), true, false);
                Job changed = job(ALL, options().put("company_id", "4f19abcd2c04"), true, false)) {
            saveActive(original, 1, List.of(), POSITIONS);
            bad(engine.restoreJob(changed.configName, original.id), "read scope changed");
        }
        assertTrace(wire(POSITIONS, 50, 1));
    }

    @Test
    void restoresTheTerminalPositionBesideAnIndependentActivePipeline() throws Exception {
        ObjectNode value = pipeline("terminal");
        ObjectNode map = options();
        map.set("default", value);
        ObjectNode expected = pipelineOut("default", value);
        reset(ok(PIPELINES, map));
        try (Job job = job(List.of("pipelines"), options(), true, true)) {
            saveActive(job, 1, List.of(expected), null);
            Future<Container.ExecResult> restored =
                    submissions.submit(() -> engine.restoreJob(job.configName, job.id));
            try {
                await(() -> "RUNNING".equals(engine.getJobStatus(job.id)), "restored anchor");
                long before = engine.checkpointCount(job.id);
                await(() -> engine.checkpointCount(job.id) > before, "restored active checkpoint");
                assertOutput(job, List.of(expected));
                assertTrace(wire(PIPELINES, 0, 0));
            } finally {
                cancel(job.id, restored);
            }
        }
    }

    @Test
    void rejectsInvalidConfigurationBeforeContactingTheVendor() throws Exception {
        reset();
        failJob(ALL, options().put("company_id", "foo/bar"), "company's API identifier");
        failJob(ALL, options().put("token", "Bearer " + TOKEN), "without a prefix");
        failJob(ALL, options().put("page_size", 51), "expected a value from 1 to 50");
        failJob(List.of("positions", "positions"), options(), "choose each resource only once");
        failJob(List.of("invented"), options(), "unknown resource name");
        failJob(
                ALL,
                options().put("endpoint", "http://breezyhr-peer:18443"),
                "expected a bare HTTPS origin");
        assertTrace();
    }

    @Test
    void restoresOnlyTheUnpublishedSuffixAndRejectsAChangedPartialPage() throws Exception {
        List<String> ids = new ArrayList<>();
        ArrayNode page = rows();
        for (int n = 1; n <= 33; n++) {
            String id = "position-" + n;
            ids.add(id);
            page.add(position(id));
        }
        ArrayNode changed = page.deepCopy();
        changed.set(32, position("position-33").put("name", "updated"));
        reset(
                ok(POSITIONS, page),
                ok(POSITIONS, page),
                ok(POSITIONS, rows()),
                ok(POSITIONS, changed));
        BreezyHrRecoveryProbe reader = recovery(List.of("positions"));
        List<String> first = new ArrayList<>();
        byte[] saved = reader.firstChunk(first);
        assertEquals(ids.subList(0, 32), first);
        assertFalse(new String(saved, StandardCharsets.ISO_8859_1).contains(TOKEN));
        assertFalse(new String(saved, StandardCharsets.ISO_8859_1).contains("position-1"));
        List<String> remaining = new ArrayList<>();
        reader.consume(saved, remaining);
        assertEquals(ids.subList(32, 33), remaining);
        List<String> rejected = new ArrayList<>();
        Exception mismatch = assertThrows(Exception.class, () -> reader.consume(saved, rejected));
        assertTrue(mismatch.getMessage().contains("provider page changed since checkpoint"));
        assertTrue(rejected.isEmpty());
        assertTrace(
                wire(POSITIONS, 50, 1),
                wire(POSITIONS, 50, 1),
                wire(POSITIONS, 50, 2),
                wire(POSITIONS, 50, 1));
    }

    @Test
    void restoresUnassignedCoordinatorAndHandlesEmptyAssignmentAndStickyCancellation()
            throws Exception {
        reset();
        BreezyHrRecoveryProbe reader = recovery(List.of("pipelines"));
        byte[] pending = reader.recoverUnassigned();
        reader.finishWithoutAssignment();
        List<String> output = new ArrayList<>();
        reader.stopBeforeAssignment(output);
        assertTrue(output.isEmpty());
        assertTrace();

        ObjectNode map = options();
        map.set("key", pipeline("id"));
        queue(ok(PIPELINES, map));
        reader.consume(pending, output);
        assertEquals(List.of("key"), output);
        assertTrace(wire(PIPELINES, 0, 0));
    }

    private void rejectOne(
            String scripted,
            List<String> resources,
            ObjectNode settings,
            Seen expected,
            String diagnostic)
            throws Exception {
        reset(scripted);
        try (Job job = job(resources, settings, false, false)) {
            bad(engine.executeJob(job.configName), diagnostic);
            assertOutput(job, List.of());
        }
        assertTrace(expected);
    }

    private void failJob(List<String> resources, ObjectNode values, String diagnostic)
            throws Exception {
        try (Job job = job(resources, values, false, false)) {
            bad(engine.executeJob(job.configName), diagnostic);
        }
    }

    private void saveActive(Job job, int calls, List<ObjectNode> committed, String stalledPath)
            throws Exception {
        Future<Container.ExecResult> running = start(job, job.id);
        try {
            await(() -> seen().size() == calls, "expected provider request");
            awaitOutput(job, committed);
            good(engine.savepointJob(job.id));
            running.get(45, TimeUnit.SECONDS);
        } finally {
            if (!running.isDone()) {
                cancel(job.id, running);
            }
        }
        if (stalledPath != null) {
            await(() -> trace().contains("disconnect\t" + stalledPath), "savepoint closed request");
        }
    }

    private BreezyHrRecoveryProbe recovery(List<String> resources) throws Exception {
        URI url =
                new URI("https", null, peer.getHost(), peer.getMappedPort(18443), null, null, null);
        return new BreezyHrRecoveryProbe(url.toString(), certificate.toString(), TOKEN, resources);
    }

    private Future<Container.ExecResult> start(Job job, String id) {
        return submissions.submit(() -> engine.start(job.configName, id));
    }

    private void cancel(String id, Future<Container.ExecResult> running) throws Exception {
        try {
            if (!running.isDone()) {
                engine.stopJob(id);
            }
            running.get(45, TimeUnit.SECONDS);
        } finally {
            if (!running.isDone()) {
                running.cancel(true);
            }
        }
    }

    private static String id() {
        return Long.toString(UUID.randomUUID().getLeastSignificantBits() & Long.MAX_VALUE);
    }

    private String copyCertificate(String path) throws Exception {
        Container.ExecResult certificate = peer.execInContainer("cat", path);
        assertEquals(0, certificate.getExitCode(), certificate.getStderr());
        engine.putCertificate(path, certificate.getStdout());
        return certificate.getStdout();
    }

    private static Job job(
            List<String> selected, ObjectNode overrides, boolean stream, boolean anchor)
            throws Exception {
        String name = "breezyhr-" + UUID.randomUUID();
        String path = "/tmp/seatunnel_mnt/" + name;
        ObjectNode settings =
                options()
                        .put("plugin_output", "breezyhr_data")
                        .put("token", TOKEN)
                        .put("company_id", COMPANY)
                        .put("endpoint", ENDPOINT)
                        .put("trust_certificate", PEM);
        ArrayNode resources = settings.putArray("resources");
        selected.forEach(resources::add);
        settings.setAll(overrides);
        String anchorInput =
                anchor
                        ? " FakeSource {\n plugin_output=anchor\n row.num=1000\n split.num=1000\n"
                                + " split.read-interval=250\n schema.fields.name=\"string\"\n}\n"
                        : "";
        String anchorOutput = anchor ? " Console { plugin_input=anchor }\n" : "";
        String contents =
                "env {\n job.mode="
                        + (stream ? "STREAMING" : "BATCH")
                        + "\n parallelism=1\n checkpoint.interval=350\n}\n"
                        + "source {\n BreezyHR "
                        + settings
                        + "\n"
                        + anchorInput
                        + "}\n"
                        + "sink {\n LocalFile {\n plugin_input=breezyhr_data\n path=\""
                        + path
                        + "\"\n"
                        + " file_format_type=json\n is_enable_transaction=true\n custom_filename=true\n"
                        + " file_name_expression=\"breezyhr_$"
                        + "{transactionId}\"\n}\n"
                        + anchorOutput
                        + "}\n";
        Path directory = ContainerUtil.getCurrentModulePath().resolve("src/test/resources");
        Files.createDirectories(directory);
        return new Job(
                Files.writeString(
                        directory.resolve(name + ".conf"), contents, StandardCharsets.UTF_8),
                path);
    }

    private static ObjectNode options() {
        return JSON.createObjectNode();
    }

    private static String candidates(String positionPath) {
        return PREFIX + "/position/" + positionPath + "/candidates";
    }

    private static ObjectNode position(String id) {
        return options().put("_id", id).put("name", "职位 " + id).putNull("location");
    }

    private static ObjectNode candidate(String id) {
        return options().put("_id", id).put("name", "人选 " + id).putNull("stage");
    }

    private static ObjectNode pipeline(String id) {
        ObjectNode pipeline =
                options().put("_id", id).put("name", "流程 " + id).put("type", "position");
        pipeline.putArray("pipeline");
        return pipeline;
    }

    private static ArrayNode rows(JsonNode... records) {
        ArrayNode array = JSON.createArrayNode();
        for (JsonNode record : records) {
            array.add(record);
        }
        return array;
    }

    private static ObjectNode out(
            String resource,
            String id,
            String parent,
            String type,
            String countryId,
            String countryName,
            String stage,
            ObjectNode payload) {
        ObjectNode row =
                options()
                        .put("resource", resource)
                        .put("record_id", id)
                        .put("position_id", parent)
                        .put("position_type", type)
                        .put("country_id", countryId)
                        .put("country_name", countryName)
                        .put("stage_name", stage);
        row.set("payload", payload);
        return row;
    }

    private static ObjectNode positionOut(ObjectNode item) {
        String id = item.path("_id").textValue();
        return out("positions", id, id, null, null, null, null, item);
    }

    private static ObjectNode candidateOut(String parent, ObjectNode item) {
        return out(
                "candidates", item.path("_id").textValue(), parent, null, null, null, null, item);
    }

    private static ObjectNode pipelineOut(String key, ObjectNode item) {
        return out("pipelines", key, null, null, null, null, null, item);
    }

    private static String reply(
            String path, int status, String mode, String retryAfter, String reset, JsonNode body) {
        return frame(
                path,
                status,
                mode,
                retryAfter,
                reset,
                body.toString().getBytes(StandardCharsets.UTF_8));
    }

    private static String frame(
            String path, int status, String mode, String retryAfter, String reset, byte[] body) {
        String encoded = Base64.getEncoder().encodeToString(body);
        return String.join("\t", Integer.toString(status), mode, retryAfter, encoded, path, reset);
    }

    private static String ok(String path, JsonNode body) {
        return reply(path, 200, "plain", "", "", body);
    }

    private static String stall(String path) {
        return reply(path, 200, "chunked", "", "", options());
    }

    private void reset(String... replies) throws Exception {
        prepare("/reset", replies);
    }

    private void queue(String... replies) throws Exception {
        prepare("/queue", replies);
    }

    private URI fixture(String path) throws Exception {
        return new URI("http", null, peer.getHost(), peer.getMappedPort(18080), path, null, null);
    }

    private void prepare(String path, String[] replies) throws Exception {
        HttpRequest request =
                HttpRequest.newBuilder(fixture(path))
                        .timeout(Duration.ofSeconds(5))
                        .POST(HttpRequest.BodyPublishers.ofString(String.join("\n", replies)))
                        .build();
        assertEquals(
                200,
                testControl.send(request, HttpResponse.BodyHandlers.discarding()).statusCode());
    }

    private String trace() throws Exception {
        HttpRequest get =
                HttpRequest.newBuilder(fixture("/requests")).timeout(Duration.ofSeconds(5)).build();
        HttpResponse<String> result = testControl.send(get, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, result.statusCode());
        return result.body();
    }

    private List<Seen> seen() throws Exception {
        List<Seen> requests = new ArrayList<>();
        for (String frame : trace().split("\n")) {
            if (!frame.startsWith("call\t")) {
                continue;
            }
            String[] parts = frame.split("\t", -1);
            assertEquals("GET", parts[1]);
            assertTrue(List.of("pat", "session").contains(parts[4]), "expected a bare token");
            assertEquals("true", parts[5], "the connector must ask for JSON");
            URI request = URI.create(parts[2]);
            assertTrue(request.getRawPath().startsWith(PREFIX + "/"));
            Map<String, String> query = new TreeMap<>();
            if (request.getRawQuery() != null) {
                for (String param : request.getRawQuery().split("&")) {
                    String[] pair = param.split("=", 2);
                    assertEquals(2, pair.length);
                    assertNull(
                            query.put(pair[0], URLDecoder.decode(pair[1], StandardCharsets.UTF_8)));
                }
            }
            requests.add(new Seen(request.getRawPath(), query, Long.parseLong(parts[3]), parts[4]));
        }
        return requests;
    }

    private static Seen wire(String path, int pageSize, int page) {
        Map<String, String> query =
                path.equals(PIPELINES)
                        ? Map.of()
                        : Map.of(
                                "page_size",
                                String.valueOf(pageSize),
                                "page",
                                String.valueOf(page),
                                "sort",
                                "updated");
        return new Seen(path, query, 0L, "pat");
    }

    private void assertTrace(Seen... expected) throws Exception {
        List<Seen> actual = seen();
        assertEquals(
                expected.length,
                actual.size(),
                () ->
                        actual.stream()
                                .map(item -> item.route + item.query)
                                .collect(Collectors.joining(", ")));
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i].route, actual.get(i).route, "route at index " + i);
            assertEquals(expected[i].query, actual.get(i).query, "query at index " + i);
            assertEquals(expected[i].auth, actual.get(i).auth, "authorization shape at index " + i);
        }
    }

    private void assertOutput(Job job, List<ObjectNode> expected) throws Exception {
        assertEquals(frequencies(expected), frequencies(engine.outputs(job.path)));
    }

    private void awaitOutput(Job job, List<ObjectNode> expected) throws Exception {
        await(
                () -> frequencies(engine.outputs(job.path)).equals(frequencies(expected)),
                "committed local output");
    }

    private static Map<ObjectNode, Long> frequencies(List<ObjectNode> values) {
        return values.stream()
                .collect(Collectors.groupingBy(value -> value, Collectors.counting()));
    }

    private static void good(Container.ExecResult result) {
        assertEquals(0, result.getExitCode(), () -> result.getStdout() + result.getStderr());
    }

    private void bad(Container.ExecResult result, String diagnostic) {
        assertNotEquals(0, result.getExitCode(), diagnostic);
        String visible = result.getStdout() + result.getStderr() + engine.getServerLogs();
        assertTrue(visible.contains(diagnostic), "Missing diagnostic " + diagnostic);
        assertFalse(visible.contains(TOKEN) || visible.contains(SESSION), "access key leaked");
        assertFalse(visible.contains(HIDDEN_MESSAGE), "provider error text leaked");
    }

    private static void await(Probe check, String purpose) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
        do {
            if (check.ready()) {
                return;
            }
            Thread.sleep(100L);
        } while (System.nanoTime() < until);
        throw new AssertionError("Timed out waiting for " + purpose);
    }

    /**
     * An observed wire request with a provider-side timestamp.
     *
     * <p>The timing is comparable within this fixture process and never uses wall-clock time.
     */
    private static final class Seen {
        private final String route;
        private final Map<String, String> query;
        private final long at;
        private final String auth;

        private Seen(String route, Map<String, String> query, long at, String auth) {
            this.route = route;
            this.query = query;
            this.at = at;
            this.auth = auth;
        }
    }

    /**
     * Files for a job submitted via the regular Zeta CLI.
     *
     * <p>The same output folder survives restoration so tests can detect duplicated writes.
     */
    private static final class Job implements AutoCloseable {
        private final Path config;
        private final String configName;
        private final String path;
        private final String id = BreezyHrIT.id();

        private Job(Path config, String path) {
            this.config = config;
            this.configName = "/" + config.getFileName();
            this.path = path;
        }

        @Override
        public void close() throws Exception {
            Files.deleteIfExists(config);
        }
    }

    /**
     * Checked condition against the active fixture or engine.
     *
     * <p>Unexpected read failures are exposed immediately rather than hidden behind a wait timeout.
     */
    @FunctionalInterface
    private interface Probe {
        boolean ready() throws Exception;
    }

    /**
     * Exposes the committed LocalFile results and live checkpoint counters for this suite.
     *
     * <p>Fixture tests use the actual starter, LocalFile committer and Zeta REST status.
     */
    private static final class Engine extends SeaTunnelContainer {
        private final HttpClient rest = HttpClient.newHttpClient();

        private void putCertificate(String path, String contents) {
            server.copyFileToContainer(Transferable.of(contents), path);
        }

        private Container.ExecResult start(String config, String id) throws Exception {
            return executeJob(server, config, id, List.of());
        }

        private List<ObjectNode> outputs(String directory) throws Exception {
            Container.ExecResult files =
                    server.execInContainer(
                            "sh",
                            "-c",
                            "if [ -d '"
                                    + directory
                                    + "' ]; then find '"
                                    + directory
                                    + "' -maxdepth 1 -type f -name 'breezyhr_*' -exec cat {} +; fi");
            assertEquals(0, files.getExitCode(), files.getStderr());
            List<ObjectNode> output = new ArrayList<>();
            for (String line : files.getStdout().split("\\R")) {
                if (!line.isBlank()) {
                    ObjectNode row = (ObjectNode) JSON.readTree(line);
                    assertTrue(row.path("payload").isTextual());
                    row.set("payload", JSON.readTree(row.path("payload").textValue()));
                    output.add(row);
                }
            }
            return output;
        }

        private long checkpointCount(String id) throws Exception {
            URI address =
                    new URI(
                            "http",
                            null,
                            server.getHost(),
                            server.getMappedPort(8080),
                            "/jobs/checkpoints/" + id,
                            null,
                            null);
            HttpRequest query =
                    HttpRequest.newBuilder(address).timeout(Duration.ofSeconds(5)).build();
            HttpResponse<String> response = rest.send(query, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                return 0;
            }
            // The Breezy HR pipeline can already have ended. Observe all active pipeline counters.
            long completed = 0;
            for (JsonNode pipeline : JSON.readTree(response.body()).path("pipelines")) {
                completed += pipeline.path("counts").path("completed").asLong();
            }
            return completed;
        }
    }
}
