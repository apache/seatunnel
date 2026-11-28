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
package org.apache.seatunnel.e2e.connector.babelforce;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.babelforce.BabelforceRecoveryProbe;
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
import java.net.URI;
import java.net.URLDecoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
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
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Checks the Babelforce connector through Zeta and a separately running Java HTTPS peer.
 *
 * <p>The suite inspects real request paths, query arguments and authentication, plus committed
 * LocalFile JSON. Every caller ID and recording reference is synthetic.
 */
class BabelforceIT extends TestSuiteBase {
    private static final ObjectMapper JSON =
            new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    private static final String ID = "fixture-access-id-private";
    private static final String TOKEN = "fixture-token-private";
    private static final String PRIVATE_CALL = "private-report-body-5550107";
    private static final String PATH = "/api/v2/calls/reporting/simple";
    private static final String HOST = "babel-peer";
    private static final String HTTPS = "https://" + HOST + ":19443";
    private static final String CERT = "/tmp/babelforce-provider.pem";
    private static final String WRONG_CERT = "/tmp/babelforce-unrelated.pem";
    private static final long FROM = 1730592000L;
    private static final long UNTIL = FROM + 10;

    private final Engine engine = new Engine();
    private final ExecutorService submissions = Executors.newSingleThreadExecutor();
    private final HttpClient admin =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private GenericContainer<?> peer;
    private Path certificate;

    /**
     * Starts a JDK 11 peer and the standard engine on its isolated network.
     *
     * <p>An independent valid certificate allows a genuine trust mismatch in another scenario.
     */
    @BeforeAll
    void boot(@TempDir Path temp) throws Exception {
        String runner = DockerClientFactory.instance().dockerHostIpAddress();
        assertTrue(runner.matches("[A-Za-z0-9.:-]+"));
        String type = runner.matches("[0-9.]+") || runner.contains(":") ? "ip:" : "dns:";
        String generate =
                "keytool -genkeypair -alias report -keyalg RSA -keysize 2048 -storetype PKCS12 "
                        + "-storepass babelforce-fixture -validity 30 -dname CN="
                        + HOST
                        + " -ext SAN=dns:"
                        + HOST
                        + ","
                        + type
                        + runner
                        + " -keystore ";
        String export =
                "keytool -exportcert -rfc -alias report -storepass babelforce-fixture -keystore ";
        String launch =
                generate
                        + "/tmp/report.p12 && "
                        + export
                        + "/tmp/report.p12 -file "
                        + CERT
                        + " && "
                        + generate
                        + "/tmp/wrong.p12 && "
                        + export
                        + "/tmp/wrong.p12 -file "
                        + WRONG_CERT
                        + " && exec java --add-modules jdk.httpserver -cp /tmp "
                        + BabelforcePeer.class.getName()
                        + " /tmp/report.p12";
        String binary = "/" + BabelforcePeer.class.getName().replace('.', '/') + ".class";
        byte[] bytes;
        try (InputStream stream = BabelforcePeer.class.getResourceAsStream(binary)) {
            if (stream == null) {
                throw new IllegalStateException("Babelforce fixture binary is missing");
            }
            bytes = stream.readAllBytes();
        }
        peer =
                new GenericContainer<>(DockerImageName.parse("eclipse-temurin:11-jdk"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST, "babel-bad-host")
                        .withExposedPorts(19443, 19080)
                        .withCopyToContainer(Transferable.of(bytes), "/tmp" + binary)
                        .withCommand("sh", "-c", launch)
                        .waitingFor(
                                Wait.forHttp("/log")
                                        .forPort(19080)
                                        .withStartupTimeout(Duration.ofMinutes(2)));
        peer.start();
        engine.startUp();
        certificate = Files.writeString(temp.resolve("vendor.pem"), installCert(CERT));
        installCert(WRONG_CERT);
    }

    /**
     * Releases both containers after the suite, including failed scenarios.
     *
     * <p>Every async submission and fixture response belongs to this test class.
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
    void readsAFullPageAndAnEmptyNonterminalPageThenStopsAtTheAdvertisedTail() throws Exception {
        List<ObjectNode> expected = new ArrayList<>();
        ArrayNode first = JSON.createArrayNode();
        ObjectNode parent =
                options()
                        .put("id", "550e8400-e29b-41d4-a716-446655440000")
                        .put("dateCreated", FROM)
                        .putNull("anonymous")
                        .put("agentName", "東京 – café");
        ObjectNode child =
                call("child-call", "2024-11-03T03:00:00.000001+0300")
                        .put("parentId", parent.path("id").asText());
        child.set(
                "recordings",
                JSON.readTree(
                        "[{\"state\":\"purged\",\"url\":\"https://example.invalid/audio\","
                                + "\"file\":{\"size\":922337203685477581100,\"name\":null}}]"));
        child.set("metric", JSON.readTree("1234567890.123456789000000010"));
        first.add(parent).add(child);
        expected.add(out(parent, null, "2024-11-03T00:00:00Z"));
        expected.add(out(child, parent.path("id").asText(), "2024-11-03T00:00:00.000001Z"));
        for (int n = 2; n < 102; n++) {
            ObjectNode another = call("same-second-" + n, "2024-11-03T00:00:00.123456");
            first.add(another);
            expected.add(out(another, null, "2024-11-03T00:00:00.123456Z"));
        }
        ObjectNode tail = call("last-in-end-second", "2024-11-03T00:00:10.999999999Z");
        expected.add(out(tail, null, "2024-11-03T00:00:10.999999999Z"));
        program(
                ok(page(1, 3, 103, 102, "?page=2", first)),
                ok(
                        page(
                                2,
                                3,
                                103,
                                102,
                                HTTPS + PATH + "?page=3&max=2&dateCreated.start=0",
                                items())),
                ok(page(3, 3, 103, 102, null, items(tail))));
        try (Job job = job(options(), false, false)) {
            pass(engine.executeJob(job.name));
            assertRows(job, expected);
        }
        wire(report(1), report(2), report(3));
    }

    @Test
    void supportsAnExistingBearerTokenWithTheOfficialSdkTimeArguments() throws Exception {
        ObjectNode first = call("time-mode", Long.toString(FROM));
        ObjectNode second = call("offset-timestamp", "2024-11-03T01:00:10+01:00");
        program(ok(page(1, 1, 2, 2, null, items(first, second))));
        ObjectNode config =
                options()
                        .put("region", "ap-southeast")
                        .put("auth_mode", "bearer")
                        .put("window_parameters", "time")
                        .put("page_size", 2);
        config.putNull("access_key");
        try (Job job = job(config, false, false)) {
            pass(engine.executeJob(job.name));
            assertRows(
                    job,
                    List.of(
                            out(first, null, Instant.ofEpochSecond(FROM).toString()),
                            out(second, null, Instant.ofEpochSecond(UNTIL).toString())));
        }
        wire(request("bearer", "time", 1, 2, FROM, UNTIL));
    }

    @Test
    void supportsADocumentedTerminalEnvelopeWithoutPagesAndAZeroPageResult() throws Exception {
        ObjectNode data = call("one", "2024-11-03T00:00:00");
        ObjectNode direct = options();
        direct.set("items", items(data));
        direct.putObject("pagination").put("current", 1).put("max", 102).put("total", 1);
        program(ok(direct));
        try (Job job = job(options(), false, false)) {
            pass(engine.executeJob(job.name));
            assertRows(job, List.of(out(data, null, "2024-11-03T00:00:00Z")));
        }
        wire(report(1));

        program(ok(page(0, 0, 0, 0, null, items())));
        try (Job job = job(options(), false, false)) {
            pass(engine.executeJob(job.name));
            assertRows(job, List.of());
        }
        wire(report(1));
    }

    @Test
    void rejectsMissingPageCountsWhenAContinuationOrTotalContradictsEndOfInput() throws Exception {
        ObjectNode noPages = page(1, 1, 2, 102, null, items(call("first", "2024-11-03T00:00:00Z")));
        ((ObjectNode) noPages.path("pagination")).remove("pages");
        reject(ok(noPages), "records do not match the provider pagination total", options());

        ObjectNode next = page(1, 1, 1, 102, "2", items(call("first", "2024-11-03T00:00:00Z")));
        ((ObjectNode) next.path("pagination")).remove("pages");
        reject(ok(next), "contradictory or unsafe pagination continuation", options());
    }

    @Test
    void rejectsChangedTotalsRepeatedPagesAndTruncatedAdvertisedData() throws Exception {
        ObjectNode record = call("call-repeated", "2024-11-03T00:00:00Z");
        program(
                ok(page(1, 2, 2, 102, null, items(record))),
                ok(page(2, 2, 3, 102, null, items(record.deepCopy().put("id", "changed")))));
        try (Job job = job(options(), false, false)) {
            fail(engine.executeJob(job.name), "provider pagination total changed");
        }
        wire(report(1), report(2));

        program(
                ok(page(1, 2, 2, 102, null, items(record))),
                ok(page(2, 2, 2, 102, null, items(record.deepCopy().put("state", "purged")))));
        try (Job job = job(options(), false, false)) {
            fail(engine.executeJob(job.name), "provider repeated a previously read page");
        }
        wire(report(1), report(2));

        reject(
                ok(page(1, 1, 55, 102, null, items(record))),
                "records do not match the provider pagination total",
                options());
    }

    @Test
    void rejectsContradictoryPaginationOffsetsCrossOriginLinksAndPageLimits() throws Exception {
        ObjectNode record = call("visible", "2024-11-03T00:00:00Z");
        reject(
                ok(page(2, 3, 1, 102, null, items(record))),
                "contradictory pagination position or size",
                options());
        reject(
                ok(page(1, 3, 1, 102, "https://foreign.invalid" + PATH + "?page=2", items(record))),
                "contradictory or unsafe pagination continuation",
                options());
        reject(
                ok(page(1, 3, 1, 102, "?page=3", items(record))),
                "contradictory or unsafe pagination continuation",
                options());
        reject(
                ok(page(1, 3, 1, 102, null, items(record))),
                "advertised pages exceed max_pages",
                options().put("max_pages", 1));
    }

    @Test
    void neverTreatsABadDateOrIgnoredCreationFilterAsACompleteRead() throws Exception {
        reject(
                ok(page(1, 1, 1, 102, null, items(call("older", "2024-11-02T23:59:59.999999Z")))),
                "dateCreated falls outside the requested Unix-second window",
                options());
        reject(
                ok(page(1, 1, 1, 102, null, items(call("newer", "2024-11-03T00:00:11Z")))),
                "dateCreated falls outside the requested Unix-second window",
                options());
        reject(
                ok(page(1, 1, 1, 102, null, items(call("no-date", "opaque")))),
                "dateCreated is not a supported timestamp",
                options());
    }

    @Test
    void surfacesPermissionAndFailedEnvelopeWithoutLoggingEitherCredentialOrReportBody()
            throws Exception {
        ObjectNode body = options().put("error", PRIVATE_CALL).put("success", false);
        for (int status : new int[] {401, 403}) {
            reject(
                    reply(status, "", "", body),
                    "requires authorized call-report credentials",
                    options());
        }
        reject(ok(body), "provider reported a failed call-report envelope", options());
        reject(
                reply(206, "", "", page(1, 1, 0, 0, null, items())),
                "provider returned a partial HTTP call report",
                options());
        reject(
                ok(options().put("success", true).put("items", "{}")),
                "invalid items or pagination",
                options());
    }

    @Test
    void boundsRetryCountAndDelayAndRepeatsTheSameQueryAfterRateLimiting() throws Exception {
        ObjectNode data = call("retry", "2024-11-03T00:00:00Z");
        program(
                reply(429, "", "1", options().put("message", PRIVATE_CALL)),
                reply(503, "", "0", options()),
                ok(page(1, 1, 1, 102, null, items(data))));
        try (Job job = job(options().put("max_attempts", 3), false, false)) {
            pass(engine.executeJob(job.name));
            assertRows(job, List.of(out(data, null, "2024-11-03T00:00:00Z")));
        }
        wire(report(1), report(1), report(1));
        List<Wire> frames = requests();
        assertTrue(TimeUnit.NANOSECONDS.toMillis(frames.get(1).at - frames.get(0).at) >= 950L);
        assertTrue(TimeUnit.NANOSECONDS.toMillis(frames.get(2).at - frames.get(1).at) >= 350L);
        reject(
                reply(503, "", "0", options()),
                "HTTP 503 retry budget exhausted",
                options().put("max_attempts", 1));
        reject(
                reply(429, "", "60", options()),
                "retry delay exceeds request deadline",
                options().put("request_timeout_ms", 1400));
    }

    @Test
    void declinesRedirectsAndRejectsInvalidTrustAndHostnameIndependently() throws Exception {
        reject(reply(307, "redirect", "", options()), "unexpected HTTP 307", options());
        program();
        try (Job job = job(options().put("trust_certificate", WRONG_CERT), false, false)) {
            fail(engine.executeJob(job.name), "TLS certificate or hostname validation failed");
        }
        try (Job job =
                job(options().put("endpoint", "https://babel-bad-host:19443"), false, false)) {
            fail(engine.executeJob(job.name), "TLS certificate or hostname validation failed");
        }
        wire();
    }

    @Test
    void boundsDecodedGzipAndEnforcesDeadlineDespiteOngoingBytes() throws Exception {
        ObjectNode data = call("gzip", "2024-11-03T00:00:00.000001Z").put("region", "東京");
        program(reply(200, "gzip", "", page(1, 1, 1, 102, null, items(data))));
        try (Job job = job(options().put("max_response_bytes", 1024), false, false)) {
            pass(engine.executeJob(job.name));
            assertRows(job, List.of(out(data, null, "2024-11-03T00:00:00.000001Z")));
        }
        wire(report(1));
        ObjectNode large =
                page(1, 1, 1, 102, null, items(data.deepCopy().put("padding", "a".repeat(4096))));
        reject(
                reply(200, "gzip", "", large),
                "response exceeds max_response_bytes",
                options().put("max_response_bytes", 1024));
        reject(stalled(), "request deadline exceeded", options().put("request_timeout_ms", 1300));
        await(() -> trace().contains("closed\t" + PATH), "the timeout connection closing");
    }

    @Test
    void cancelsAStalledBodyAndNeverStartsTheNextPage() throws Exception {
        program(stalled());
        try (Job job = job(options().put("request_timeout_ms", 60000), true, false)) {
            String id = nextJobId();
            Future<Container.ExecResult> running = start(job, id);
            try {
                await(() -> requests().size() == 1, "the first live HTTPS call");
            } finally {
                stopJob(id, running);
            }
        }
        await(() -> trace().contains("closed\t" + PATH), "the cancelled connection closing");
        wire(report(1));
    }

    @Test
    void restoresAnInflightNextPageWithoutDroppingRowsCreatedInTheSameSecond() throws Exception {
        ObjectNode first = call("first-cross-page", "2024-11-03T00:00:00.000001Z");
        ObjectNode last = call("last-cross-page", "2024-11-03T00:00:00.000002Z");
        program(ok(page(1, 2, 2, 102, "2", items(first))), stalled());
        ObjectNode firstRow = out(first, null, "2024-11-03T00:00:00.000001Z");
        try (Job job = job(options().put("request_timeout_ms", 60000), true, false)) {
            String id = nextJobId();
            Future<Container.ExecResult> one = start(job, id);
            try {
                await(() -> requests().size() == 2, "the next provider page");
                committed(job, List.of(firstRow));
                pass(engine.savepointJob(id));
                one.get(45, TimeUnit.SECONDS);
            } finally {
                if (!one.isDone()) {
                    stopJob(id, one);
                }
            }
            await(
                    () -> trace().contains("closed\t" + PATH),
                    "savepoint release of the previous request");
            append(ok(page(2, 2, 2, 102, null, items(last))));
            pass(engine.restoreJob(job.name, id));
            assertRows(job, List.of(firstRow, out(last, null, "2024-11-03T00:00:00.000002Z")));
        }
        wire(report(1), report(2), report(2));
    }

    @Test
    void rejectsChangedRegionBeforeTheRestoredScanMakesAProviderRequest() throws Exception {
        program(stalled());
        ObjectNode original = options().put("region", "services").put("request_timeout_ms", 60000);
        ObjectNode different = original.deepCopy().put("region", "us-east");
        try (Job job = job(original, true, false);
                Job changed = job(different, true, false)) {
            String id = nextJobId();
            Future<Container.ExecResult> running = start(job, id);
            try {
                await(() -> requests().size() == 1, "the first outstanding call");
                pass(engine.savepointJob(id));
                running.get(45, TimeUnit.SECONDS);
                fail(engine.restoreJob(changed.name, id), "read scope changed");
            } finally {
                if (!running.isDone()) {
                    stopJob(id, running);
                }
            }
        }
        wire(report(1));
    }

    @Test
    void remembersACompletedScanWhileRestoringAnotherLivePipeline() throws Exception {
        ObjectNode data = call("terminal", "2024-11-03T00:00:00Z");
        ObjectNode row = out(data, null, "2024-11-03T00:00:00Z");
        program(ok(page(1, 1, 1, 102, null, items(data))));
        try (Job job = job(options(), true, true)) {
            String id = nextJobId();
            Future<Container.ExecResult> original = start(job, id);
            try {
                committed(job, List.of(row));
                pass(engine.savepointJob(id));
                original.get(45, TimeUnit.SECONDS);
            } finally {
                if (!original.isDone()) {
                    stopJob(id, original);
                }
            }
            Future<Container.ExecResult> restored =
                    submissions.submit(() -> engine.restoreJob(job.name, id));
            try {
                await(
                        () -> "RUNNING".equals(engine.getJobStatus(id)),
                        "live restored companion pipeline");
                long count = engine.checkpoints(id);
                await(() -> engine.checkpoints(id) > count, "a subsequent completed checkpoint");
                assertRows(job, List.of(row));
                wire(report(1));
            } finally {
                stopJob(id, restored);
            }
        }
    }

    @Test
    void rejectsUnknownRegionCredentialMixtureAndUnsafeOriginWithoutSendingAnyRequest()
            throws Exception {
        program();
        try (Job job = job(options().put("region", "evil.example"), false, false)) {
            fail(engine.executeJob(job.name), "expected services, us-east or ap-southeast");
        }
        try (Job job = job(options().put("auth_mode", "bearer"), false, false)) {
            fail(engine.executeJob(job.name), "supply an access ID only in access_key mode");
        }
        try (Job job = job(options().put("endpoint", "http://babel-peer:19443"), false, false)) {
            fail(engine.executeJob(job.name), "expected an HTTPS origin");
        }
        try (Job job = job(options().put("date_created_from", UNTIL + 1), false, false)) {
            fail(engine.executeJob(job.name), "expected ordered Unix seconds");
        }
        wire();
    }

    @Test
    void restoresOnlyTheUnpublishedSuffixAndRejectsAChangedPage() throws Exception {
        List<String> ids =
                IntStream.range(0, 73).mapToObj(i -> "same-page-" + i).collect(Collectors.toList());
        ArrayNode array = items();
        ids.forEach(id -> array.add(call(id, "2024-11-03T00:00:00.000001Z")));
        ObjectNode original = page(1, 1, ids.size(), 102, null, array);
        ArrayNode changed = array.deepCopy();
        changed.set(72, call("changed-after-savepoint", "2024-11-03T00:00:00.000001Z"));
        program(ok(original), ok(original), ok(page(1, 1, ids.size(), 102, null, changed)));
        BabelforceRecoveryProbe probe = recovery(FROM, UNTIL);
        List<String> seen = new ArrayList<>();
        byte[] snapshot = probe.firstChunk(seen);
        assertEquals(ids.subList(0, 32), seen);
        String checkpointBytes = new String(snapshot, StandardCharsets.ISO_8859_1);
        assertFalse(checkpointBytes.contains(ID) || checkpointBytes.contains(TOKEN));

        List<String> suffix = new ArrayList<>();
        probe.restore(snapshot, suffix);
        assertEquals(ids.subList(32, 73), suffix);
        List<String> rejected = new ArrayList<>();
        Exception failure = assertThrows(Exception.class, () -> probe.restore(snapshot, rejected));
        assertTrue(failure.getMessage().contains("provider response changed after checkpoint"));
        assertTrue(rejected.isEmpty());
        wire(report(1), report(1), report(1));
    }

    @Test
    void keepsAnImplicitEndAcrossInitialAssignmentAndResolvesTheThreeProviderOrigins()
            throws Exception {
        program();
        for (String region : List.of("services", "us-east", "ap-southeast")) {
            assertEquals(
                    "https://" + region + ".babelforce.com",
                    BabelforceRecoveryProbe.origin(region, ID, TOKEN));
        }
        BabelforceRecoveryProbe probe = recovery(FROM, null);
        byte[] assigned = probe.restoredAssignment();
        wire();
        long until = BabelforceRecoveryProbe.cutoff(assigned);
        append(ok(page(1, 1, 1, 102, null, items(call("at-frozen-cutoff", Long.toString(until))))));
        List<String> output = new ArrayList<>();
        probe.restore(assigned, output);
        assertEquals(List.of("at-frozen-cutoff"), output);
        wire(request("keys", "dateCreated", 1, 102, FROM, until));
    }

    @Test
    void staysCancelledBeforeTheReaderReceivesAnySplit() throws Exception {
        program();
        List<String> output = new ArrayList<>();
        recovery(FROM, UNTIL).cancelFirst(output);
        assertTrue(output.isEmpty());
        wire();
    }

    private BabelforceRecoveryProbe recovery(long from, Long to) throws Exception {
        URI endpoint =
                new URI("https", null, peer.getHost(), peer.getMappedPort(19443), null, null, null);
        return new BabelforceRecoveryProbe(
                endpoint.toString(), certificate.toString(), ID, TOKEN, from, to);
    }

    private void reject(String frame, String diagnostic, ObjectNode overrides) throws Exception {
        program(frame);
        try (Job job = job(overrides, false, false)) {
            fail(engine.executeJob(job.name), diagnostic);
        }
        wire(report(1));
    }

    private static String nextJobId() {
        return Long.toString(UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE);
    }

    private Future<Container.ExecResult> start(Job job, String id) {
        return submissions.submit(() -> engine.submit(job.name, id));
    }

    private void stopJob(String id, Future<Container.ExecResult> active) throws Exception {
        try {
            if (!active.isDone()) {
                engine.stopJob(id);
            }
            active.get(45, TimeUnit.SECONDS);
        } finally {
            if (!active.isDone()) {
                active.cancel(true);
            }
        }
    }

    private String installCert(String path) throws Exception {
        Container.ExecResult result = peer.execInContainer("cat", path);
        assertEquals(0, result.getExitCode(), result.getStderr());
        engine.install(path, result.getStdout());
        return result.getStdout();
    }

    private static Job job(ObjectNode overrides, boolean streaming, boolean anchor)
            throws Exception {
        String unique = "babel-calls-" + UUID.randomUUID();
        String directory = "/tmp/seatunnel_mnt/" + unique;
        ObjectNode settings =
                options()
                        .put("plugin_output", "recordings")
                        .put("access_key", ID)
                        .put("token", TOKEN)
                        .put("date_created_from", FROM)
                        .put("date_created_to", UNTIL)
                        .put("endpoint", HTTPS)
                        .put("trust_certificate", CERT);
        settings.setAll(overrides);
        String independentSource =
                anchor
                        ? " FakeSource {\n plugin_output=continue_job\n"
                                + " row.num=1000\n split.num=1000\n split.read-interval=250\n"
                                + " schema.fields.value=\"string\"\n }\n"
                        : "";
        String independentSink = anchor ? " Console { plugin_input=continue_job }\n" : "";
        String config =
                "env {\n job.mode="
                        + (streaming ? "STREAMING" : "BATCH")
                        + "\n parallelism=1\n checkpoint.interval=350\n}\n"
                        + "source {\n Babelforce "
                        + settings
                        + "\n"
                        + independentSource
                        + "}\n"
                        + "sink {\n LocalFile {\n plugin_input=recordings\n path=\""
                        + directory
                        + "\"\n"
                        + " file_format_type=json\n is_enable_transaction=true\n custom_filename=true\n"
                        + " file_name_expression=\"committed_$"
                        + "{transactionId}\"\n }\n"
                        + independentSink
                        + "}\n";
        Path resources = ContainerUtil.getCurrentModulePath().resolve("src/test/resources");
        Files.createDirectories(resources);
        return new Job(Files.writeString(resources.resolve(unique + ".conf"), config), directory);
    }

    private static ObjectNode options() {
        return JSON.createObjectNode();
    }

    private static ObjectNode call(String id, String date) {
        return options().put("id", id).put("dateCreated", date);
    }

    private static ArrayNode items(JsonNode... entries) {
        ArrayNode records = JSON.createArrayNode();
        for (JsonNode entry : entries) {
            records.add(entry);
        }
        return records;
    }

    private static ObjectNode page(
            int current, int pages, long total, int max, String next, ArrayNode items) {
        ObjectNode root = options();
        ObjectNode metadata =
                root.putObject("pagination")
                        .put("current", current)
                        .put("pages", pages)
                        .put("total", total)
                        .put("max", max);
        if (next != null) {
            metadata.put("next", next);
        }
        root.set("items", items);
        return root;
    }

    private static ObjectNode out(ObjectNode raw, String parent, String utc) {
        ObjectNode output =
                options()
                        .put("call_id", raw.path("id").asText())
                        .put("parent_id", parent)
                        .put("date_created", utc);
        output.set("payload", raw);
        return output;
    }

    private static String reply(int status, String mode, String retryAfter, JsonNode payload) {
        String encoded =
                Base64.getEncoder()
                        .encodeToString(payload.toString().getBytes(StandardCharsets.UTF_8));
        return String.join("\t", Integer.toString(status), mode, retryAfter, encoded);
    }

    private static String ok(JsonNode payload) {
        return reply(200, "", "", payload);
    }

    private static String stalled() {
        return reply(200, "trickle", "", options());
    }

    private URI control(String path) throws Exception {
        return new URI("http", null, peer.getHost(), peer.getMappedPort(19080), path, null, null);
    }

    private void program(String... station) throws Exception {
        write("/program", station);
    }

    private void append(String... station) throws Exception {
        write("/append", station);
    }

    private void write(String path, String[] replies) throws Exception {
        HttpRequest post =
                HttpRequest.newBuilder(control(path))
                        .timeout(Duration.ofSeconds(5))
                        .POST(HttpRequest.BodyPublishers.ofString(String.join("\n", replies)))
                        .build();
        assertEquals(200, admin.send(post, HttpResponse.BodyHandlers.discarding()).statusCode());
    }

    private String trace() throws Exception {
        HttpRequest read =
                HttpRequest.newBuilder(control("/log")).timeout(Duration.ofSeconds(5)).build();
        HttpResponse<String> response = admin.send(read, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        return response.body();
    }

    private List<Wire> requests() throws Exception {
        List<Wire> got = new ArrayList<>();
        for (String frame : trace().split("\n")) {
            if (frame.startsWith("call\t")) {
                String[] fields = frame.split("\t", -1);
                assertEquals("GET", fields[1]);
                assertEquals("0", fields[5], "simple reports have no request body");
                URI target = URI.create(fields[2]);
                Map<String, String> query = new TreeMap<>();
                if (target.getRawQuery() != null) {
                    for (String param : target.getRawQuery().split("&")) {
                        String[] pair = param.split("=", 2);
                        assertEquals(2, pair.length);
                        assertNull(
                                query.put(
                                        pair[0],
                                        URLDecoder.decode(pair[1], StandardCharsets.UTF_8)));
                    }
                }
                got.add(
                        new Wire(
                                List.of(fields[3], target.getPath(), query),
                                Long.parseLong(fields[4])));
            }
        }
        return got;
    }

    @SafeVarargs
    private final void wire(List<Object>... expected) throws Exception {
        assertEquals(
                List.of(expected),
                requests().stream().map(it -> it.call).collect(Collectors.toList()));
    }

    private static List<Object> report(int requestedPage) {
        return request("keys", "dateCreated", requestedPage, 102, FROM, UNTIL);
    }

    private static List<Object> request(
            String auth, String filter, int page, int size, long from, long until) {
        return List.of(
                auth,
                PATH,
                Map.of(
                        "page",
                        Integer.toString(page),
                        "max",
                        Integer.toString(size),
                        filter + ".start",
                        Long.toString(from),
                        filter + ".end",
                        Long.toString(until)));
    }

    private static Map<ObjectNode, Long> multiplicities(List<ObjectNode> rows) {
        return rows.stream().collect(Collectors.groupingBy(node -> node, Collectors.counting()));
    }

    private void assertRows(Job job, List<ObjectNode> expected) throws Exception {
        assertEquals(multiplicities(expected), multiplicities(engine.results(job.directory)));
    }

    private void committed(Job job, List<ObjectNode> expected) throws Exception {
        await(
                () ->
                        multiplicities(expected)
                                .equals(multiplicities(engine.results(job.directory))),
                "committed LocalFile report rows");
    }

    private static void pass(Container.ExecResult exit) {
        assertEquals(0, exit.getExitCode(), () -> exit.getStdout() + exit.getStderr());
    }

    private void fail(Container.ExecResult exit, String expected) {
        assertNotEquals(0, exit.getExitCode(), expected);
        String logs = exit.getStdout() + exit.getStderr() + engine.getServerLogs();
        assertTrue(logs.contains(expected), () -> "Missing diagnostic " + expected);
        assertFalse(logs.contains(ID), "provider access ID leaked");
        assertFalse(logs.contains(TOKEN), "provider token leaked");
        assertFalse(logs.contains(PRIVATE_CALL), "provider call data leaked");
    }

    private static void await(Condition condition, String purpose) throws Exception {
        long expires = System.nanoTime() + TimeUnit.SECONDS.toNanos(45L);
        do {
            if (condition.ready()) {
                return;
            }
            Thread.sleep(100L);
        } while (System.nanoTime() - expires < 0L);
        throw new AssertionError("Timed out waiting for " + purpose);
    }

    /**
     * The peer's captured authorization mode, request and monotonic arrival clock.
     *
     * <p>Comparing wire arrival times proves that the engine observed the retry delay.
     */
    private static final class Wire {
        private final List<Object> call;
        private final long at;

        private Wire(List<Object> call, long at) {
            this.call = call;
            this.at = at;
        }
    }

    /**
     * A regular CLI job with a dedicated output directory.
     *
     * <p>The directory is reused on savepoint restore so duplicate rows are visible.
     */
    private static final class Job implements AutoCloseable {
        private final Path config;
        private final String name;
        private final String directory;

        private Job(Path config, String directory) {
            this.config = config;
            this.name = "/" + config.getFileName();
            this.directory = directory;
        }

        @Override
        public void close() throws Exception {
            Files.deleteIfExists(config);
        }
    }

    /**
     * Observes expected progress at wire, checkpoint or sink time.
     *
     * <p>A failed read remains visible rather than being counted as an elapsed wait.
     */
    @FunctionalInterface
    private interface Condition {
        boolean ready() throws Exception;
    }

    /**
     * Exposes only committed result files and aggregate checkpoint counters.
     *
     * <p>The normal job launcher, LocalFile committer and Zeta lifecycle are exercised directly.
     */
    private static final class Engine extends SeaTunnelContainer {
        private final HttpClient rest = HttpClient.newHttpClient();

        private void install(String path, String contents) {
            server.copyFileToContainer(Transferable.of(contents), path);
        }

        private Container.ExecResult submit(String configuration, String jobId) throws Exception {
            return executeJob(server, configuration, jobId, List.of());
        }

        private List<ObjectNode> results(String location) throws Exception {
            Container.ExecResult read =
                    server.execInContainer(
                            "sh",
                            "-c",
                            "if [ -d '"
                                    + location
                                    + "' ]; then find '"
                                    + location
                                    + "' -maxdepth 1 -type f -name 'committed_*' -exec cat {} +; fi");
            assertEquals(0, read.getExitCode(), read.getStderr());
            List<ObjectNode> output = new ArrayList<>();
            for (String line : read.getStdout().split("\\R")) {
                if (!line.isBlank()) {
                    ObjectNode row = (ObjectNode) JSON.readTree(line);
                    assertTrue(row.path("payload").isTextual());
                    row.set("payload", JSON.readTree(row.path("payload").textValue()));
                    output.add(row);
                }
            }
            return output;
        }

        private long checkpoints(String id) throws Exception {
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
                    rest.send(
                            HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(5)).build(),
                            HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                return 0L;
            }
            long count = 0L;
            for (JsonNode pipeline : JSON.readTree(response.body()).path("pipelines")) {
                count += pipeline.path("counts").path("completed").asLong();
            }
            return count;
        }
    }
}
