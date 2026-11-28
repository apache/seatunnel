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
package org.apache.seatunnel.e2e.connector.bluetally;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.bluetally.BlueTallyRecoveryProbe;
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
 * Runs BlueTally through a real SeaTunnel Zeta engine and an isolated Java TLS account.
 *
 * <p>Tests compare page numbers and bounds in captured GET requests and inspect LocalFile's
 * committed JSON rows. The provider peer never calls the real billable API.
 */
class BlueTallyIT extends TestSuiteBase {
    private static final ObjectMapper JSON =
            new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    private static final String TOKEN = "bluetally-fixture-private+/=?";
    private static final String HIDDEN_MESSAGE = "private-provider-response-token";
    private static final String HOST = "bluetally-peer";
    private static final String PEM = "/tmp/bluetally.pem";
    private static final String OTHER_PEM = "/tmp/unrelated-bluetally.pem";
    private static final String ENDPOINT = "https://" + HOST + ":18443";
    private static final String START = "2024-03-01T00:00:00Z";
    private static final String END = "2024-04-01T00:00:00Z";
    private static final String ASSETS = "/api/v1/assets";
    private static final String EMPLOYEES = "/api/v1/employees";
    private static final String COHORT = "2024-03-15T12:00:00.123456789Z";
    private static final List<String> BOTH = List.of("assets", "employees");

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
                "keytool -genkeypair -alias provider -storetype PKCS12 -storepass bluetally-fixture "
                        + "-keyalg RSA -keysize 2048 -validity 30 -dname CN="
                        + HOST
                        + " -ext SAN=dns:"
                        + HOST
                        + ",dns:bluetally-redirect-host,"
                        + runnerType
                        + runner
                        + " -keystore ";
        String export =
                "keytool -exportcert -rfc -alias provider -storepass bluetally-fixture -keystore ";
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
                        + BlueTallyPeer.class.getName()
                        + " /tmp/provider.p12";
        String providerClass = "/" + BlueTallyPeer.class.getName().replace('.', '/') + ".class";
        byte[] bytecode;
        try (InputStream bits = BlueTallyPeer.class.getResourceAsStream(providerClass)) {
            if (bits == null) {
                throw new IllegalStateException("Missing packaged HTTPS fixture");
            }
            bytecode = bits.readAllBytes();
        }
        peer =
                new GenericContainer<>(DockerImageName.parse("eclipse-temurin:11-jdk"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST, "bluetally-other-host", "bluetally-redirect-host")
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
    void readsBothResourcesWithExactSourceIdsPayloadsAndRawOffsets() throws Exception {
        List<ObjectNode> expected = new ArrayList<>();
        ArrayNode assets = JSON.createArrayNode();
        ObjectNode atEnd = asset(1, END).put("tenant_id", 21);
        assets.add(atEnd);
        for (int i = 2; i <= 50; i++) {
            ObjectNode asset = asset(i, COHORT);
            asset.put("asset_name", "编译台 🖥️ Équipe " + i);
            asset.put("purchase_cost", new BigDecimal("9223372036854775808.0123456789012345"));
            asset.putNull("asset_serial");
            asset.putArray("deployed_to")
                    .addObject()
                    .put("employee_id", 8)
                    .putNull("checkout_date");
            asset.putArray("extensions")
                    .addNull()
                    .add("https://bluetally-redirect-host:18443/ignored");
            assets.add(asset);
            expected.add(out("assets", 21, asset));
        }
        ObjectNode lower = asset(51, START).put("id", new BigInteger("900719925474099312345"));
        ObjectNode old = asset(52, "2024-02-29T23:59:59.999999999Z");
        ObjectNode employee = employee(2, COHORT).put("notes", "L'équipe").put("archived", true);
        employee.putArray("assets").add(48).addNull();
        employee.putArray("custom_fields").addObject().put("分工", "infra");
        expected.add(out("assets", 21, lower));
        expected.add(out("employees", 21, employee));
        reset(
                ok(ASSETS, assets),
                ok(ASSETS, rows(lower)),
                ok(ASSETS, rows(old)),
                ok(ASSETS, rows()),
                ok(EMPLOYEES, rows(employee)),
                ok(EMPLOYEES, rows()));
        try (Job job = job(BOTH, options().put("tenant_id", 21), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, expected);
        }
        assertTrace(
                wire(ASSETS, 50, 0, 21),
                wire(ASSETS, 50, 50, 21),
                wire(ASSETS, 50, 51, 21),
                wire(ASSETS, 50, 52, 21),
                wire(EMPLOYEES, 50, 0, 21),
                wire(EMPLOYEES, 50, 1, 21));
    }

    @Test
    void preservesEqualNanosecondBoundariesAcrossPagesAndSkipsOnlyOutsideRows() throws Exception {
        String start = "2024-03-01T00:00:00.000000100Z";
        String end = "2024-04-01T00:00:00.000000200Z";
        ObjectNode exactEnd = employee(1, end);
        ObjectNode lastIncluded = employee(2, "2024-04-01T00:00:00.000000199Z");
        ObjectNode sameLatest = employee(3, "2024-04-01T05:30:00.000000199+05:30");
        ObjectNode atStart = employee(4, start);
        ObjectNode sameStart = employee(5, "2024-03-01T05:30:00.000000100+05:30");
        ObjectNode below = employee(6, "2024-03-01T00:00:00.000000099Z");
        reset(
                ok(EMPLOYEES, rows(exactEnd, lastIncluded)),
                ok(EMPLOYEES, rows(sameLatest)),
                ok(EMPLOYEES, rows(atStart, sameStart)),
                ok(EMPLOYEES, rows(below)),
                ok(EMPLOYEES, rows()));
        try (Job job =
                job(
                        List.of("employees"),
                        options().put("start_date", start).put("end_date", end).put("page_size", 2),
                        false,
                        false)) {
            good(engine.executeJob(job.configName));
            assertOutput(
                    job,
                    List.of(
                            out("employees", null, lastIncluded),
                            out("employees", null, sameLatest),
                            out("employees", null, atStart),
                            out("employees", null, sameStart)));
        }
        assertTrace(
                wire(EMPLOYEES, 2, 0, null),
                wire(EMPLOYEES, 2, 2, null),
                wire(EMPLOYEES, 2, 3, null),
                wire(EMPLOYEES, 2, 5, null),
                wire(EMPLOYEES, 2, 6, null));
    }

    @Test
    void continuesToTheNextSelectedResourceAfterAnEmptyFirstArray() throws Exception {
        ObjectNode asset = asset(101, COHORT);
        reset(ok(EMPLOYEES, rows()), ok(ASSETS, rows(asset)), ok(ASSETS, rows()));
        try (Job job = job(List.of("employees", "assets"), options(), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of(out("assets", null, asset)));
        }
        assertTrace(
                wire(EMPLOYEES, 50, 0, null), wire(ASSETS, 50, 0, null), wire(ASSETS, 50, 1, null));
    }

    @Test
    void allowsTheDocumentedMaximumLimitAndTerminatesOnAnEmptyArray() throws Exception {
        reset(ok(ASSETS, rows()));
        try (Job job = job(List.of("assets"), options().put("page_size", 1000), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of());
        }
        assertTrace(wire(ASSETS, 1000, 0, null));
    }

    @Test
    void validatesAnEntireResponseBeforePublishingAnyRecord() throws Exception {
        checkBadPage(
                ok(ASSETS, options().put("message", HIDDEN_MESSAGE)),
                options(),
                "expected a BlueTally array",
                50,
                null);
        checkBadPage(
                frame(ASSETS, 200, "plain", "", "[] " + HIDDEN_MESSAGE),
                options(),
                "response is not valid JSON",
                50,
                null);
        checkBadPage(
                ok(ASSETS, rows(asset(1, COHORT), asset(2, "2024-02-30T00:00:00Z"))),
                options(),
                "no valid updated_at timestamp",
                50,
                null);
        checkBadPage(
                ok(ASSETS, rows(asset(1, COHORT).put("id", "asset-label"))),
                options(),
                "no integer system id",
                50,
                null);
        checkBadPage(
                ok(ASSETS, rows(asset(1, COHORT), asset(2, COHORT))),
                options().put("page_size", 1),
                "within the requested limit",
                1,
                null);
        checkBadPage(
                ok(ASSETS, JSON.createArrayNode().addNull()),
                options(),
                "non-object record",
                50,
                null);
    }

    @Test
    void detectsAServerThatIgnoresTheRequestedSortWithinOrAcrossPages() throws Exception {
        ObjectNode older = asset(1, START);
        ObjectNode newer = asset(2, COHORT);
        checkBadPage(
                ok(ASSETS, rows(older, newer)), options(), "out-of-order updated_at", 50, null);
        reset(ok(ASSETS, rows(older)), ok(ASSETS, rows(newer)));
        failJob(List.of("assets"), options(), "out-of-order updated_at");
        assertTrace(wire(ASSETS, 50, 0, null), wire(ASSETS, 50, 1, null));
    }

    @Test
    void rejectsDuplicatedIdsAndARepeatedEarlierPage() throws Exception {
        ObjectNode a = asset(1, COHORT);
        ObjectNode b = asset(2, COHORT);
        ObjectNode c = asset(3, COHORT);
        checkBadPage(ok(ASSETS, rows(a, a)), options(), "duplicate record ID", 50, null);
        reset(ok(ASSETS, rows(a, b)), ok(ASSETS, rows(b, c)));
        failJob(List.of("assets"), options(), "duplicate record ID");
        assertTrace(wire(ASSETS, 50, 0, null), wire(ASSETS, 50, 2, null));
        reset(ok(ASSETS, rows(a)), ok(ASSETS, rows(b)), ok(ASSETS, rows(a)));
        failJob(List.of("assets"), options(), "provider repeated a resource page");
        assertTrace(
                wire(ASSETS, 50, 0, null), wire(ASSETS, 50, 1, null), wire(ASSETS, 50, 2, null));
    }

    @Test
    void refusesAMismatchedTenantEvenWhenTheFirstRowWasValid() throws Exception {
        checkBadPage(
                ok(
                        ASSETS,
                        rows(
                                asset(1, COHORT).put("tenant_id", 21),
                                asset(2, COHORT).put("tenant_id", 22))),
                options().put("tenant_id", 21),
                "outside the configured tenant",
                50,
                21);
    }

    @Test
    void boundsTheNumberOfPagesWithoutOpeningTheFollowingResource() throws Exception {
        reset(ok(ASSETS, rows(asset(1, COHORT))));
        failJob(
                BOTH,
                options().put("max_pages_per_resource", 1),
                "max_pages_per_resource exhausted");
        assertTrace(wire(ASSETS, 50, 0, null));
    }

    @Test
    void handlesAccessFailuresAndAnUndatedRateLimitWithoutRetry() throws Exception {
        for (int status : List.of(401, 403, 404, 206)) {
            reset(
                    reply(
                            ASSETS,
                            status,
                            "plain",
                            "",
                            options().put("private_message", HIDDEN_MESSAGE)));
            failJob(BOTH, options(), "HTTP " + status);
            assertTrace(wire(ASSETS, 50, 0, null));
        }
        reset(
                reply(ASSETS, 429, "plain", "", options().put("message", HIDDEN_MESSAGE)),
                ok(ASSETS, rows()));
        failJob(BOTH, options(), "without a confirmed retry delay");
        assertTrace(wire(ASSETS, 50, 0, null));
    }

    @Test
    void honorsThePositiveRetryDelayWithinOneBudgetAndNeverAutomaticallyReplays503()
            throws Exception {
        ObjectNode row = asset(5, COHORT);
        reset(
                reply(ASSETS, 429, "plain", "1", options()),
                ok(ASSETS, rows(row)),
                ok(ASSETS, rows()));
        try (Job job = job(List.of("assets"), options(), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of(out("assets", null, row)));
        }
        assertTrace(
                wire(ASSETS, 50, 0, null), wire(ASSETS, 50, 0, null), wire(ASSETS, 50, 1, null));
        List<Seen> attempts = seen();
        assertTrue(TimeUnit.NANOSECONDS.toMillis(attempts.get(1).at - attempts.get(0).at) >= 950L);

        reset(reply(ASSETS, 503, "plain", "0", options()), ok(ASSETS, rows()));
        failJob(BOTH, options().put("max_attempts", 1), "HTTP 503 retry budget exhausted");
        assertTrace(wire(ASSETS, 50, 0, null));

        reset(reply(ASSETS, 429, "plain", "30", options()));
        failJob(
                BOTH,
                options().put("request_timeout_ms", 3000),
                "retry delay exceeds request deadline");
        assertTrace(wire(ASSETS, 50, 0, null));
    }

    @Test
    void measuresTheDecodedGzipBodyAndRejectsUnsupportedTypes() throws Exception {
        ObjectNode row = asset(8, COHORT);
        reset(reply(ASSETS, 200, "gzip", "", rows(row)), ok(ASSETS, rows()));
        try (Job job =
                job(List.of("assets"), options().put("max_response_bytes", 1024), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of(out("assets", null, row)));
        }
        assertTrace(wire(ASSETS, 50, 0, null), wire(ASSETS, 50, 1, null));
        checkBadPage(
                reply(
                        ASSETS,
                        200,
                        "gzip",
                        "",
                        rows(asset(9, COHORT).put("notes", "z".repeat(5000)))),
                options().put("max_response_bytes", 1024),
                "response exceeds max_response_bytes",
                50,
                null);
        checkBadPage(
                reply(ASSETS, 200, "unsupported", "", rows()),
                options(),
                "unsupported response compression",
                50,
                null);
        checkBadPage(
                frame(ASSETS, 200, "binary", "", "[]"),
                options(),
                "expected Content-Type application/json",
                50,
                null);
    }

    @Test
    void neverFollowsARedirectAndRequiresExpectedTrustAndHostname() throws Exception {
        reset(reply(ASSETS, 302, "redirect", "", options()));
        failJob(BOTH, options(), "HTTP 302");
        assertTrace(wire(ASSETS, 50, 0, null));
        assertFalse(trace().contains("/redirect-receiver"));
        reset(ok(ASSETS, rows()));
        failJob(
                BOTH,
                options().put("trust_certificate", OTHER_PEM),
                "TLS certificate or hostname validation failed");
        failJob(
                BOTH,
                options().put("endpoint", "https://bluetally-other-host:18443"),
                "TLS certificate or hostname validation failed");
        assertTrace();
    }

    @Test
    void disconnectsATricklingResponseWhenTheRequestDeadlineExpires() throws Exception {
        reset(stall(ASSETS));
        failJob(BOTH, options().put("request_timeout_ms", 2800), "request deadline exceeded");
        await(() -> trace().contains("disconnect\t" + ASSETS), "deadline disconnected transfer");
        assertTrace(wire(ASSETS, 50, 0, null));
    }

    @Test
    void closesActiveTransferOnJobCancellationAndNeverRequestsEmployees() throws Exception {
        reset(stall(ASSETS));
        try (Job job = job(BOTH, options().put("request_timeout_ms", 60000), true, false)) {
            Future<Container.ExecResult> running = start(job, job.id);
            try {
                await(() -> seen().size() == 1, "active asset transfer");
            } finally {
                cancel(job.id, running);
            }
            await(
                    () -> trace().contains("disconnect\t" + ASSETS),
                    "server observed job cancellation");
            assertOutput(job, List.of());
        }
        assertTrace(wire(ASSETS, 50, 0, null));
    }

    @Test
    void restoresTheExactRawOffsetAfterAFilteredPageAndKeepsTenantScope() throws Exception {
        ObjectNode above = asset(1, END);
        ObjectNode first = asset(2, COHORT);
        ObjectNode second = asset(3, COHORT);
        ObjectNode committed = out("assets", 19, first);
        reset(ok(ASSETS, rows(above, first)), stall(ASSETS));
        try (Job job =
                job(
                        List.of("assets"),
                        options().put("tenant_id", 19).put("request_timeout_ms", 60000),
                        true,
                        false)) {
            saveActive(job, 2, List.of(committed), ASSETS);
            queue(ok(ASSETS, rows(second)), ok(ASSETS, rows()));
            good(engine.restoreJob(job.configName, job.id));
            assertOutput(job, List.of(committed, out("assets", 19, second)));
        }
        assertTrace(
                wire(ASSETS, 50, 0, 19),
                wire(ASSETS, 50, 2, 19),
                wire(ASSETS, 50, 2, 19),
                wire(ASSETS, 50, 3, 19));
    }

    @Test
    void recoversTheNextResourceWithoutRereadingTheCompletedAssetResource() throws Exception {
        ObjectNode a = asset(5, COHORT);
        ObjectNode b = employee(5, COHORT);
        ObjectNode assetRow = out("assets", null, a);
        reset(ok(ASSETS, rows(a)), ok(ASSETS, rows()), stall(EMPLOYEES));
        try (Job job = job(BOTH, options().put("request_timeout_ms", 60000), true, false)) {
            saveActive(job, 3, List.of(assetRow), EMPLOYEES);
            queue(ok(EMPLOYEES, rows(b)), ok(EMPLOYEES, rows()));
            good(engine.restoreJob(job.configName, job.id));
            assertOutput(job, List.of(assetRow, out("employees", null, b)));
        }
        assertTrace(
                wire(ASSETS, 50, 0, null),
                wire(ASSETS, 50, 1, null),
                wire(EMPLOYEES, 50, 0, null),
                wire(EMPLOYEES, 50, 0, null),
                wire(EMPLOYEES, 50, 1, null));
    }

    @Test
    void failsRestorationWithADifferentTenantBeforeAnotherProviderRequest() throws Exception {
        reset(stall(ASSETS));
        try (Job job =
                        job(
                                BOTH,
                                options().put("tenant_id", 19).put("request_timeout_ms", 60000),
                                true,
                                false);
                Job changed = job(BOTH, options().put("tenant_id", 20), true, false)) {
            saveActive(job, 1, List.of(), ASSETS);
            bad(engine.restoreJob(changed.configName, job.id), "read scope changed");
        }
        assertTrace(wire(ASSETS, 50, 0, 19));
    }

    @Test
    void keepsTheTerminalPositionAfterRestoringAnIndependentActivePipeline() throws Exception {
        ObjectNode row = asset(20, COHORT);
        ObjectNode expected = out("assets", null, row);
        reset(ok(ASSETS, rows(row)), ok(ASSETS, rows()));
        try (Job job = job(List.of("assets"), options(), true, true)) {
            saveActive(job, 2, List.of(expected), null);
            Future<Container.ExecResult> restored =
                    submissions.submit(() -> engine.restoreJob(job.configName, job.id));
            try {
                await(() -> "RUNNING".equals(engine.getJobStatus(job.id)), "restored anchor");
                long before = engine.checkpointCount(job.id);
                await(() -> engine.checkpointCount(job.id) > before, "restored active checkpoint");
                assertOutput(job, List.of(expected));
                assertTrace(wire(ASSETS, 50, 0, null), wire(ASSETS, 50, 1, null));
            } finally {
                cancel(job.id, restored);
            }
        }
    }

    @Test
    void rejectsInvalidConfigurationBeforeOpeningTheFirstConnection() throws Exception {
        reset();
        failJob(
                BOTH,
                options().put("start_date", END),
                "must be earlier than the update window end");
        failJob(BOTH, options().put("tenant_id", -1), "positive 32-bit tenant identifier");
        failJob(BOTH, options().put("page_size", 1001), "expected a value between 1 and 1000");
        failJob(List.of("assets", "assets"), options(), "choose each resource only once");
        failJob(
                BOTH,
                options().put("endpoint", "http://bluetally-peer:18443"),
                "expected an HTTPS origin");
        assertTrace();
    }

    @Test
    void restoresOnlyTheUnpublishedSuffixAndRejectsChangedPastPageData() throws Exception {
        List<String> wanted = new ArrayList<>();
        ArrayNode page = JSON.createArrayNode();
        for (int i = 1; i <= 48; i++) {
            wanted.add(String.valueOf(i));
            page.add(asset(i, COHORT));
        }
        ArrayNode changed = page.deepCopy();
        changed.set(46, asset(99, COHORT));
        reset(ok(ASSETS, page), ok(ASSETS, page), ok(ASSETS, rows()), ok(ASSETS, changed));
        BlueTallyRecoveryProbe reader = recovery(List.of("assets"), END);
        List<String> initial = new ArrayList<>();
        byte[] state = reader.firstChunk(initial);
        assertEquals(wanted.subList(0, 32), initial);
        List<String> recovered = new ArrayList<>();
        reader.consume(state, recovered);
        assertEquals(wanted.subList(32, 48), recovered);
        List<String> refused = new ArrayList<>();
        Exception mismatch = assertThrows(Exception.class, () -> reader.consume(state, refused));
        assertTrue(mismatch.getMessage().contains("provider page changed after checkpoint"));
        assertTrue(refused.isEmpty());
        assertTrace(
                wire(ASSETS, 50, 0, null),
                wire(ASSETS, 50, 0, null),
                wire(ASSETS, 50, 48, null),
                wire(ASSETS, 50, 0, null));
    }

    @Test
    void freezesDefaultEndAcrossUnassignedCoordinatorRestoration() throws Exception {
        reset();
        BlueTallyRecoveryProbe reader = recovery(BOTH, null);
        byte[] state = reader.recoverUnassigned();
        assertTrace();
        Instant saved = BlueTallyRecoveryProbe.savedEnd(state);
        queue(
                ok(
                        ASSETS,
                        rows(asset(1, saved.toString()), asset(2, saved.minusNanos(1).toString()))),
                ok(ASSETS, rows()),
                ok(
                        EMPLOYEES,
                        rows(
                                employee(3, saved.toString()),
                                employee(4, saved.minusNanos(1).toString()))),
                ok(EMPLOYEES, rows()));
        List<String> emitted = new ArrayList<>();
        reader.consume(state, emitted);
        assertEquals(List.of("2", "4"), emitted);
        assertTrace(
                wire(ASSETS, 50, 0, null),
                wire(ASSETS, 50, 2, null),
                wire(EMPLOYEES, 50, 0, null),
                wire(EMPLOYEES, 50, 2, null));
    }

    @Test
    void doesNotOpenTransportAfterCancellationBeforeAssignment() throws Exception {
        reset();
        List<String> emitted = new ArrayList<>();
        recovery(BOTH, END).stopBeforeAssignment(emitted);
        assertTrue(emitted.isEmpty());
        assertTrace();
    }

    private void checkBadPage(
            String tape, ObjectNode overrides, String diagnostic, int limit, Integer tenant)
            throws Exception {
        reset(tape);
        try (Job job = job(BOTH, overrides, false, false)) {
            bad(engine.executeJob(job.configName), diagnostic);
            assertOutput(job, List.of());
        }
        assertTrace(wire(ASSETS, limit, 0, tenant));
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

    private BlueTallyRecoveryProbe recovery(List<String> resources, String end) throws Exception {
        URI url =
                new URI("https", null, peer.getHost(), peer.getMappedPort(18443), null, null, null);
        return new BlueTallyRecoveryProbe(
                url.toString(), certificate.toString(), TOKEN, resources, end);
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
        String name = "bluetally-" + UUID.randomUUID();
        String path = "/tmp/seatunnel_mnt/" + name;
        ObjectNode settings =
                options()
                        .put("plugin_output", "bluetally_data")
                        .put("token", TOKEN)
                        .put("start_date", START)
                        .put("end_date", END)
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
                        + "source {\n BlueTally "
                        + settings
                        + "\n"
                        + anchorInput
                        + "}\n"
                        + "sink {\n LocalFile {\n plugin_input=bluetally_data\n path=\""
                        + path
                        + "\"\n"
                        + " file_format_type=json\n is_enable_transaction=true\n custom_filename=true\n"
                        + " file_name_expression=\"bluetally_$"
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

    private static ObjectNode asset(int id, String updated) {
        return options()
                .put("id", id)
                .put("asset_id", "INVENTORY-" + id)
                .put("updated_at", updated);
    }

    private static ObjectNode employee(int id, String updated) {
        return options()
                .put("id", id)
                .put("name", "示例 employee " + id)
                .put("updated_at", updated)
                .putNull("email");
    }

    private static ArrayNode rows(JsonNode... records) {
        ArrayNode array = JSON.createArrayNode();
        for (JsonNode record : records) {
            array.add(record);
        }
        return array;
    }

    private static ObjectNode out(String resource, Integer tenant, ObjectNode payload) {
        ObjectNode row =
                options()
                        .put("resource", resource)
                        .put("record_id", payload.path("id").asText())
                        .put("tenant_id", tenant)
                        .put(
                                "updated_at",
                                OffsetDateTime.parse(payload.path("updated_at").textValue())
                                        .toInstant()
                                        .toString());
        row.set("payload", payload);
        return row;
    }

    private static String reply(
            String path, int status, String mode, String retryAfter, JsonNode body) {
        return frame(path, status, mode, retryAfter, body.toString());
    }

    private static String frame(
            String path, int status, String mode, String retryAfter, String body) {
        String encoded = Base64.getEncoder().encodeToString(body.getBytes(StandardCharsets.UTF_8));
        return String.join("\t", Integer.toString(status), mode, retryAfter, encoded, path);
    }

    private static String ok(String path, JsonNode body) {
        return reply(path, 200, "plain", "", body);
    }

    private static String stall(String path) {
        return reply(path, 200, "chunked", "", options());
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
            assertEquals("true", parts[4], "the expected Bearer credential must be present");
            assertEquals("true", parts[5], "the connector must ask for JSON");
            URI request = URI.create(parts[2]);
            assertTrue(request.getRawPath().startsWith("/api/v1/"));
            Map<String, String> query = new TreeMap<>();
            if (request.getRawQuery() != null) {
                for (String param : request.getRawQuery().split("&")) {
                    String[] pair = param.split("=", 2);
                    assertEquals(2, pair.length);
                    assertNull(
                            query.put(pair[0], URLDecoder.decode(pair[1], StandardCharsets.UTF_8)));
                }
            }
            requests.add(new Seen(request.getRawPath(), query, Long.parseLong(parts[3])));
        }
        return requests;
    }

    private static Seen wire(String path, int limit, int offset, Integer tenant) {
        Map<String, String> query =
                new TreeMap<>(
                        Map.of(
                                "limit",
                                String.valueOf(limit),
                                "offset",
                                String.valueOf(offset),
                                "sort",
                                "updated_at",
                                "order",
                                "desc"));
        if (tenant != null) {
            query.put("tenant_id", tenant.toString());
        }
        return new Seen(path, query, 0L);
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
        private final String route;
        private final Map<String, String> query;
        private final long at;

        private Seen(String route, Map<String, String> query, long at) {
            this.route = route;
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
        private final String id = BlueTallyIT.id();

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
                                    + "' -maxdepth 1 -type f -name 'bluetally_*' -exec cat {} +; fi");
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
            // The BlueTally pipeline can already have ended. Observe all active pipeline counters.
            long completed = 0;
            for (JsonNode pipeline : JSON.readTree(response.body()).path("pipelines")) {
                completed += pipeline.path("counts").path("completed").asLong();
            }
            return completed;
        }
    }
}
