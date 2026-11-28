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
package org.apache.seatunnel.e2e.connector.beamer;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.beamer.BeamerRecoveryProbe;
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
import java.time.OffsetDateTime;
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
 * Runs Beamer through a real SeaTunnel Zeta engine and an isolated Java TLS account.
 *
 * <p>Tests compare page numbers and bounds in captured GET requests and inspect LocalFile's
 * committed JSON rows. The provider peer never calls the real billable API.
 */
class BeamerIT extends TestSuiteBase {
    private static final ObjectMapper JSON =
            new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    private static final String TOKEN = "beamer-fixture-private+/=?";
    private static final String HIDDEN_MESSAGE = "private-provider-response-token";
    private static final String HOST = "beamer-peer";
    private static final String PEM = "/tmp/beamer.pem";
    private static final String OTHER_PEM = "/tmp/unrelated-beamer.pem";
    private static final String ENDPOINT = "https://" + HOST + ":18443";
    private static final String START = "2024-03-01T00:00:00Z";
    private static final String END = "2024-04-01T00:00:00Z";
    private static final String QUERY_START = "2024-02-29T23:59:59Z";
    private static final String COHORT = "2024-03-15T12:00:00.123456789Z";

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
                "keytool -genkeypair -alias provider -storetype PKCS12 -storepass beamer-fixture "
                        + "-keyalg RSA -keysize 2048 -validity 30 -dname CN="
                        + HOST
                        + " -ext SAN=dns:"
                        + HOST
                        + ",dns:beamer-redirect-host,"
                        + runnerType
                        + runner
                        + " -keystore ";
        String export =
                "keytool -exportcert -rfc -alias provider -storepass beamer-fixture -keystore ";
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
                        + BeamerPeer.class.getName()
                        + " /tmp/provider.p12";
        String providerClass = "/" + BeamerPeer.class.getName().replace('.', '/') + ".class";
        byte[] bytecode;
        try (InputStream bits = BeamerPeer.class.getResourceAsStream(providerClass)) {
            if (bits == null) {
                throw new IllegalStateException("Missing packaged HTTPS fixture");
            }
            bytecode = bits.readAllBytes();
        }
        peer =
                new GenericContainer<>(DockerImageName.parse("eclipse-temurin:11-jdk"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST, "beamer-other-host", "beamer-redirect-host")
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
    void readsAFullPagePlusTwoShortPagesAndCommitsExactResponses() throws Exception {
        ArrayNode first = JSON.createArrayNode();
        List<ObjectNode> expected = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            ObjectNode feedback =
                    record("response-" + i)
                            .put("feedback", "很有帮助 🌊 — merci " + i)
                            .put("score", i % 11)
                            .putNull("userEmail")
                            .put("userId", "synthetic-" + i);
            feedback.putObject("extension")
                    .put("score_detail", new BigDecimal("0.12345678901234567890123456789"))
                    .putArray("labels")
                    .add("中文")
                    .addNull();
            first.add(feedback);
            expected.add(out(feedback));
        }
        ObjectNode anonymous = record(null).putNull("id").put("feedback", "anonymous");
        ObjectNode tail = record("short-page").put("userFirstName", "Élodie");
        ObjectNode finalRow = record("last-page").put("browser", "Example Browser");
        expected.add(out(anonymous));
        expected.add(out(tail));
        expected.add(out(finalRow));
        reset(ok(first), ok(rows(anonymous, tail)), ok(rows(finalRow)), ok(rows()));
        try (Job job = job(options(), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, expected);
        }
        expectCalls(0, 1, 2, 3);
    }

    @Test
    void enforcesExactHalfOpenDatesAcrossGuardOnlyPagesAndTheSameSecond() throws Exception {
        ObjectNode below = dated("before-boundary", "2024-03-01T00:00:00.000000099Z");
        ObjectNode first = dated("at-start", "2024-03-01T05:30:00.000000100+05:30");
        ObjectNode guard = dated("guard", QUERY_START);
        ObjectNode last = dated("before-end", "2024-04-01T00:00:00.333333332Z");
        ObjectNode end = dated("at-end", "2024-04-01T00:00:00.333333333Z");
        ObjectNode after = dated("rounded-upper", "2024-04-01T00:00:01Z");
        reset(ok(rows(below, first)), ok(rows(guard)), ok(rows(end, last, after)), ok(rows()));
        try (Job job =
                job(
                        options()
                                .put("start_date", "2024-03-01T00:00:00.000000100Z")
                                .put("end_date", "2024-04-01T00:00:00.333333333Z")
                                .put("page_size", 3),
                        false,
                        false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of(out(first), out(last)));
        }
        expectCalls(3, QUERY_START, "2024-04-01T00:00:01Z", 0, 1, 2, 3);
    }

    @Test
    void endsAnEmptyWindowWithoutAnyAdditionalProviderRequest() throws Exception {
        reset(ok(rows()));
        try (Job job = job(options(), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of());
        }
        expectCalls(0);
    }

    @Test
    void rejectsA200ErrorObjectAndOversizedPageWithoutPublishingItsContents() throws Exception {
        rejected("expected an NPS array", options(), new int[] {0}, ok(hiddenError()));
        rejected(
                "expected an NPS array",
                options().put("page_size", 1),
                new int[] {0},
                ok(rows(record("one"), record("two"))));
        rejected(
                "response is not valid JSON",
                options(),
                new int[] {0},
                frame(200, "plain", "", "[] " + HIDDEN_MESSAGE));
        rejected(
                "non-object",
                options(),
                new int[] {0},
                ok(JSON.createArrayNode().add("unexpected")));
    }

    @Test
    void rejectsMalformedDatesAndOutOfWindowResultsBeforePublishingAPage() throws Exception {
        rejected(
                "no supported creation date",
                options(),
                new int[] {0},
                ok(rows(record("valid"), options().put("id", "missing-date"))));
        rejected(
                "no supported creation date",
                options(),
                new int[] {0},
                ok(rows(dated("malformed-date", "2024-02-30T00:00:00Z"))));
        rejected(
                "outside the request window",
                options(),
                new int[] {0},
                ok(rows(dated("ignored-filter", "2025-01-01T00:00:00Z"))));
        rejected(
                "invalid NPS response ID",
                options(),
                new int[] {0},
                ok(rows(record("wrong-id-type").put("id", 12))));
    }

    @Test
    void failsOnDuplicateIdOverlapAndAnOlderPageThatComesBackInDifferentOrder() throws Exception {
        ObjectNode a = record("a");
        ObjectNode b = record("b");
        ObjectNode c = record("c");
        ObjectNode d = record("d");
        rejected("duplicate NPS response ID", options(), new int[] {0}, ok(rows(a, a)));
        rejected(
                "duplicate NPS response ID",
                options(),
                new int[] {0, 1},
                ok(rows(a, b)),
                ok(rows(b, c)));
        rejected(
                "provider repeated an NPS page",
                options(),
                new int[] {0, 1, 2},
                ok(rows(a, b)),
                ok(rows(c, d)),
                ok(rows(b, a)));
    }

    @Test
    void failsWhenTheBudgetCannotMakeTheTerminatingRequest() throws Exception {
        rejected(
                "max_pages exhausted",
                options().put("max_pages", 1),
                new int[] {0},
                ok(rows(record("known-row"))));
    }

    @Test
    void doesNotRetryMonthlyQuotaOrAuthenticationAndPermissionFailures() throws Exception {
        rejected(
                "HTTP 429 account request quota exhausted",
                options(),
                new int[] {0},
                reply(429, "plain", "1", hiddenError()),
                ok(rows()));
        rejected("HTTP 401", options(), new int[] {0}, reply(401, "plain", "", hiddenError()));
        rejected("HTTP 403", options(), new int[] {0}, reply(403, "plain", "", hiddenError()));
    }

    @Test
    void retriesAnEligiblePageAtTheServerLowerBoundAndNeverLetsTheClientReplayItImplicitly()
            throws Exception {
        ObjectNode row = record("after-retry");
        reset(reply(503, "plain", "1", hiddenError()), ok(rows(row)), ok(rows()));
        try (Job job = job(options(), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of(out(row)));
        }
        expectCalls(0, 0, 1);
        List<Seen> calls = seen();
        assertTrue(TimeUnit.NANOSECONDS.toMillis(calls.get(1).at - calls.get(0).at) >= 950L);
        rejected(
                "HTTP 503 retry budget exhausted",
                options().put("max_attempts", 1),
                new int[] {0},
                reply(503, "plain", "0", hiddenError()),
                ok(rows()));
        rejected(
                "retry delay exceeds request deadline",
                options().put("request_timeout_ms", 3000),
                new int[] {0},
                reply(503, "plain", "30", hiddenError()));
    }

    @Test
    void rejectsCrossOriginRedirectsAndNeverForwardsTheAccountKey() throws Exception {
        rejected(
                "HTTP 302",
                options(),
                new int[] {0},
                reply(302, "redirect", "", hiddenError()),
                ok(rows()));
        assertFalse(trace().contains("/redirect-receiver"));
    }

    @Test
    void requiresBothTheTrustedCertificateAndItsExpectedHostname() throws Exception {
        reset(ok(rows()));
        try (Job wrongAnchor = job(options().put("trust_certificate", OTHER_PEM), false, false)) {
            bad(
                    engine.executeJob(wrongAnchor.configName),
                    "TLS certificate or hostname validation failed");
        }
        try (Job wrongHost =
                job(options().put("endpoint", "https://beamer-other-host:18443"), false, false)) {
            bad(
                    engine.executeJob(wrongHost.configName),
                    "TLS certificate or hostname validation failed");
        }
        expectCalls();
    }

    @Test
    void boundsDecodedGzipAndRejectsUnsupportedContentEncoding() throws Exception {
        ObjectNode row = record("gzip-ok").put("feedback", "feedback readable after gzip");
        reset(reply(200, "gzip", "", rows(row)), ok(rows()));
        try (Job job = job(options().put("max_response_bytes", 1024), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of(out(row)));
        }
        expectCalls(0, 1);
        rejected(
                "response exceeds max_response_bytes",
                options().put("max_response_bytes", 1024),
                new int[] {0},
                reply(
                        200,
                        "gzip",
                        "",
                        rows(record("too-large").put("feedback", "x".repeat(4000)))));
        rejected(
                "unsupported response compression",
                options(),
                new int[] {0},
                reply(200, "unsupported", "", rows()));
    }

    @Test
    void releasesTheLiveResponseWhenTheWholeAcquisitionDeadlineExpires() throws Exception {
        rejected(
                "request deadline exceeded",
                options().put("request_timeout_ms", 2800),
                new int[] {0},
                stall());
        await(() -> trace().contains("disconnect	/v0/nps"), "deadline disconnected provider");
    }

    @Test
    void cancelsTheActiveResponseBeforeASecondRequestCanStart() throws Exception {
        reset(stall());
        try (Job job = job(options().put("request_timeout_ms", 60000), true, false)) {
            String id = id();
            Future<Container.ExecResult> running = start(job, id);
            try {
                await(() -> seen().size() == 1, "active NPS transfer");
            } finally {
                cancel(id, running);
            }
            await(() -> trace().contains("disconnect	/v0/nps"), "job cancellation disconnect");
            assertOutput(job, List.of());
        }
        expectCalls(0);
    }

    @Test
    void restoresAStalledNextPageWithoutRepeatingTheFirstCommittedSinkRow() throws Exception {
        ObjectNode first = record("first");
        ObjectNode more = record("suffix");
        reset(ok(rows(first)), stall());
        try (Job job = job(options().put("request_timeout_ms", 60000), true, false)) {
            saveActive(job, 2, List.of(out(first)));
            queue(ok(rows(more)), ok(rows()));
            good(engine.restoreJob(job.configName, job.id));
            assertOutput(job, List.of(out(first), out(more)));
        }
        expectCalls(0, 1, 1, 2);
    }

    @Test
    void refusesToRestoreAnInFlightFirstPageWithANewPageSize() throws Exception {
        reset(stall());
        try (Job original = job(options().put("request_timeout_ms", 60000), true, false);
                Job changed = job(options().put("page_size", 42), true, false)) {
            saveActive(original, 1, List.of());
            bad(engine.restoreJob(changed.configName, original.id), "read scope changed");
        }
        expectCalls(0);
    }

    @Test
    void preservesFinishedPositionWhenTheRunningAnchorPipelineIsRestored() throws Exception {
        ObjectNode tail = record("exactly-once-in-output");
        reset(ok(rows(tail)), ok(rows()));
        try (Job job = job(options(), true, true)) {
            saveActive(job, 2, List.of(out(tail)), false);
            Future<Container.ExecResult> restored =
                    submissions.submit(() -> engine.restoreJob(job.configName, job.id));
            try {
                await(
                        () -> "RUNNING".equals(engine.getJobStatus(job.id)),
                        "restored anchor pipeline");
                long previous = engine.checkpointCount(job.id);
                await(
                        () -> engine.checkpointCount(job.id) > previous,
                        "restored anchor checkpoint");
                assertOutput(job, List.of(out(tail)));
                expectCalls(0, 1);
            } finally {
                cancel(job.id, restored);
            }
        }
    }

    @Test
    void validatesDateRangePageSizeAndHttpsOriginBeforeIssuingProviderRequests() throws Exception {
        reset();
        verifyConfig(options().put("start_date", END), "must be earlier than the end");
        verifyConfig(options().put("page_size", 101), "expected a value between 1 and 100");
        verifyConfig(
                options().put("endpoint", "http://beamer-peer:18443"), "expected an HTTPS origin");
        verifyConfig(options().put("end_date", "2024-04-01"), "RFC 3339 timestamp");
        expectCalls();
    }

    @Test
    void restoresTheExactPartialReaderPageAndRejectsAnyChangedTailBeforePublishing()
            throws Exception {
        List<String> wanted = new ArrayList<>();
        ArrayNode data = JSON.createArrayNode();
        for (int i = 0; i < 67; i++) {
            wanted.add("partial-" + i);
            data.add(record(wanted.get(i)));
        }
        ArrayNode changed = data.deepCopy();
        changed.set(66, record("different-tail"));
        reset(ok(data), ok(data), ok(rows()), ok(changed));
        BeamerRecoveryProbe reader = recovery();
        List<String> initial = new ArrayList<>();
        byte[] state = reader.firstChunk(initial);
        assertEquals(wanted.subList(0, 32), initial);
        List<String> resumed = new ArrayList<>();
        reader.consume(state, resumed);
        assertEquals(wanted.subList(32, 67), resumed);

        List<String> refused = new ArrayList<>();
        Exception changedPage = assertThrows(Exception.class, () -> reader.consume(state, refused));
        assertTrue(changedPage.getMessage().contains("provider page changed after checkpoint"));
        assertTrue(refused.isEmpty());
        assertEquals(
                List.of("0", "0", "1", "0"),
                seen().stream().map(call -> call.query.get("page")).collect(Collectors.toList()));
        assertEquals(
                1,
                seen().stream()
                        .skip(1)
                        .limit(2)
                        .map(call -> call.query.get("dateTo"))
                        .distinct()
                        .count());
    }

    @Test
    void freezesTheImplicitEndAcrossSerializedCoordinatorAssignment() throws Exception {
        reset();
        BeamerRecoveryProbe reader = recovery();
        byte[] state = reader.recoverUnassigned();
        expectCalls();
        Instant cutoff = BeamerRecoveryProbe.frozenEnd(state);
        Instant before = cutoff.minusNanos(1);
        queue(
                ok(rows(dated("before", before.toString()), dated("cutoff", cutoff.toString()))),
                ok(rows()));
        List<String> output = new ArrayList<>();
        reader.consume(state, output);
        assertEquals(List.of("before"), output);
        long rounded = cutoff.getEpochSecond() + (cutoff.getNano() == 0 ? 0L : 1L);
        expectCalls(100, QUERY_START, Instant.ofEpochSecond(rounded).toString(), 0, 1);
    }

    @Test
    void cancellationBeforeReaderAssignmentNeverOpensTransport() throws Exception {
        reset();
        List<String> collected = new ArrayList<>();
        recovery().stopBeforeAssignment(collected);
        assertTrue(collected.isEmpty());
        expectCalls();
    }

    private void rejected(String diagnostic, ObjectNode overrides, int[] pages, String... tape)
            throws Exception {
        reset(tape);
        try (Job job = job(overrides, false, false)) {
            bad(engine.executeJob(job.configName), diagnostic);
            if (pages.length == 1) {
                assertOutput(job, List.of());
            }
        }
        expectCalls(overrides.path("page_size").asInt(100), QUERY_START, END, pages);
    }

    private void verifyConfig(ObjectNode overrides, String diagnostic) throws Exception {
        try (Job job = job(overrides, false, false)) {
            bad(engine.executeJob(job.configName), diagnostic);
        }
    }

    private void saveActive(Job job, int requests, List<ObjectNode> committed) throws Exception {
        saveActive(job, requests, committed, true);
    }

    private void saveActive(Job job, int requests, List<ObjectNode> committed, boolean stalled)
            throws Exception {
        Future<Container.ExecResult> running = start(job, job.id);
        try {
            await(() -> seen().size() == requests, "the expected NPS page request");
            awaitOutput(job, committed);
            good(engine.savepointJob(job.id));
            running.get(45, TimeUnit.SECONDS);
        } finally {
            if (!running.isDone()) {
                cancel(job.id, running);
            }
        }
        if (stalled) {
            await(() -> trace().contains("disconnect	/v0/nps"), "savepoint closed the old call");
        }
    }

    private BeamerRecoveryProbe recovery() throws Exception {
        URI url =
                new URI("https", null, peer.getHost(), peer.getMappedPort(18443), null, null, null);
        return new BeamerRecoveryProbe(url.toString(), certificate.toString(), TOKEN, START);
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

    private static Job job(ObjectNode overrides, boolean stream, boolean anchor) throws Exception {
        String name = "beamer-" + UUID.randomUUID();
        String path = "/tmp/seatunnel_mnt/" + name;
        ObjectNode settings =
                options()
                        .put("plugin_output", "nps_data")
                        .put("token", TOKEN)
                        .put("start_date", START)
                        .put("end_date", END)
                        .put("endpoint", ENDPOINT)
                        .put("trust_certificate", PEM);
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
                        + "source {\n Beamer "
                        + settings
                        + "\n"
                        + anchorInput
                        + "}\n"
                        + "sink {\n LocalFile {\n plugin_input=nps_data\n path=\""
                        + path
                        + "\"\n"
                        + " file_format_type=json\n is_enable_transaction=true\n custom_filename=true\n"
                        + " file_name_expression=\"nps_$"
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

    private static ObjectNode record(String id) {
        return dated(id, COHORT);
    }

    private static ObjectNode dated(String id, String time) {
        return options().put("id", id).put("date", time).put("score", 9);
    }

    private static ArrayNode rows(ObjectNode... records) {
        ArrayNode array = JSON.createArrayNode();
        for (ObjectNode record : records) {
            array.add(record);
        }
        return array;
    }

    private static ObjectNode out(ObjectNode record) {
        ObjectNode row =
                options()
                        .put("record_id", record.path("id").textValue())
                        .put(
                                "created_at",
                                OffsetDateTime.parse(record.path("date").textValue())
                                        .toInstant()
                                        .toString());
        row.set("payload", record);
        return row;
    }

    private static ObjectNode hiddenError() {
        return options().put("error", HIDDEN_MESSAGE);
    }

    private static String reply(int status, String mode, String retryAfter, JsonNode body) {
        return frame(status, mode, retryAfter, body.toString());
    }

    private static String frame(int status, String mode, String retryAfter, String body) {
        String encoded = Base64.getEncoder().encodeToString(body.getBytes(StandardCharsets.UTF_8));
        return String.join("\t", Integer.toString(status), mode, retryAfter, encoded);
    }

    private static String ok(JsonNode body) {
        return reply(200, "plain", "", body);
    }

    private static String stall() {
        return reply(200, "chunked", "", options());
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
            assertEquals("true", parts[4], "Authorization must be absent");
            assertEquals("true", parts[5], "the exact Beamer-Api-Key header must be present");
            URI request = URI.create(parts[2]);
            assertEquals("/v0/nps", request.getPath());
            Map<String, String> query = new TreeMap<>();
            assertTrue(request.getRawQuery() != null);
            for (String param : request.getRawQuery().split("&")) {
                String[] tuple = param.split("=", 2);
                assertEquals(2, tuple.length);
                assertNull(
                        query.put(tuple[0], URLDecoder.decode(tuple[1], StandardCharsets.UTF_8)));
            }
            assertEquals(
                    List.of("dateFrom", "dateTo", "maxResults", "page"),
                    List.copyOf(query.keySet()));
            requests.add(new Seen(query, Long.parseLong(parts[3])));
        }
        return requests;
    }

    private void expectCalls(int... pages) throws Exception {
        expectCalls(100, QUERY_START, END, pages);
    }

    private void expectCalls(int pageSize, String from, String to, int... pages) throws Exception {
        List<Seen> requests = seen();
        assertEquals(pages.length, requests.size());
        for (int i = 0; i < pages.length; i++) {
            assertEquals(
                    Map.of(
                            "dateFrom",
                            from,
                            "dateTo",
                            to,
                            "maxResults",
                            String.valueOf(pageSize),
                            "page",
                            String.valueOf(pages[i])),
                    requests.get(i).query);
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
        assertFalse(visible.contains(TOKEN), "access key leaked");
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
        private final Map<String, String> query;
        private final long at;

        private Seen(Map<String, String> query, long at) {
            this.query = query;
            this.at = at;
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
        private final String id = BeamerIT.id();

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
                                    + "' -maxdepth 1 -type f -name 'nps_*' -exec cat {} +; fi");
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
            // The NPS pipeline can already have ended. Observe all active pipeline counters.
            long completed = 0;
            for (JsonNode pipeline : JSON.readTree(response.body()).path("pipelines")) {
                completed += pipeline.path("counts").path("completed").asLong();
            }
            return completed;
        }
    }
}
