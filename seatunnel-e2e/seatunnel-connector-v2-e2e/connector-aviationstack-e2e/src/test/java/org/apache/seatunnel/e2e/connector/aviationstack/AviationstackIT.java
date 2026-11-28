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
package org.apache.seatunnel.e2e.connector.aviationstack;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.aviationstack.AviationstackRecoveryProbe;
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
 * Runs Aviationstack through a real SeaTunnel Zeta engine and an isolated Java TLS account.
 *
 * <p>Tests compare absolute offsets in the captured GET requests and inspect LocalFile's committed
 * JSON rows. The provider peer never calls the real billable API.
 */
class AviationstackIT extends TestSuiteBase {
    private static final ObjectMapper JSON =
            new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    private static final String ACCESS_KEY = "aviation-fixture-private +/ß=?";
    private static final String HIDDEN_MESSAGE = "private-provider-response-token";
    private static final String HOST = "aviationstack-peer";
    private static final String PEM = "/tmp/aviationstack.pem";
    private static final String OTHER_PEM = "/tmp/unrelated-aviationstack.pem";
    private static final String ENDPOINT = "https://" + HOST + ":18443";
    private static final List<String> CATALOGS =
            List.of(
                    "airports",
                    "airlines",
                    "airplanes",
                    "aircraft_types",
                    "cities",
                    "countries",
                    "taxes");

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
                "keytool -genkeypair -alias provider -storetype PKCS12 -storepass aviation-fixture "
                        + "-keyalg RSA -keysize 2048 -validity 30 -dname CN="
                        + HOST
                        + " -ext SAN=dns:"
                        + HOST
                        + ","
                        + runnerType
                        + runner
                        + " -keystore ";
        String export =
                "keytool -exportcert -rfc -alias provider -storepass aviation-fixture -keystore ";
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
                        + AviationstackPeer.class.getName()
                        + " /tmp/provider.p12";
        String providerClass = "/" + AviationstackPeer.class.getName().replace('.', '/') + ".class";
        byte[] bytecode;
        try (InputStream bits = AviationstackPeer.class.getResourceAsStream(providerClass)) {
            if (bits == null) {
                throw new IllegalStateException("Missing packaged HTTPS fixture");
            }
            bytecode = bits.readAllBytes();
        }
        peer =
                new GenericContainer<>(DockerImageName.parse("eclipse-temurin:11-jdk"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST, "aviationstack-other-host")
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
    void readsAllSevenCatalogsAndUsesTheRawCountAfterAFullAndAShortPage() throws Exception {
        List<ObjectNode> allRows = new ArrayList<>();
        ArrayNode hundred = JSON.createArrayNode();
        for (int i = 0; i < 100; i++) {
            ObjectNode record = record("airport-" + i);
            if (i == 0) {
                record =
                        (ObjectNode)
                                JSON.readTree(
                                        "{\"id\":922337203685477580900,"
                                                + "\"airport_name\":\"서울 ✈\",\"price\":123456789.0000000000000000123,"
                                                + "\"phone_number\":null,\"attributes\":{\"items\":[null,true,\"direct\"]}}");
            }
            hundred.add(record);
            allRows.add(out("airports", record));
        }
        ObjectNode tail = record("short-page");
        ObjectNode last = record("at-total");
        allRows.add(out("airports", tail));
        allRows.add(out("airports", last));
        List<String> tape =
                new ArrayList<>(
                        List.of(
                                ok(page(100, 0, 102, hundred)),
                                ok(page(100, 100, 102, rows(tail))),
                                ok(page(100, 101, 102, rows(last)))));
        List<String> paths =
                new ArrayList<>(List.of("airports:100:0", "airports:100:100", "airports:100:101"));
        for (String catalog : CATALOGS.subList(1, CATALOGS.size())) {
            ObjectNode row = record("shared-id").put("catalog_label", catalog).putNull("optional");
            tape.add(ok(page(100, 0, 1, rows(row))));
            allRows.add(out(catalog, row));
            paths.add(catalog + ":100:0");
        }
        reset(tape.toArray(new String[0]));
        try (Job job = job(CATALOGS, options(), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, allRows);
        }
        expectCalls(paths);
    }

    @Test
    void filtersAircraftDeliveryTimeLocallyAndCountsFilteredPages() throws Exception {
        ObjectNode old = dated("old", "2024-02-29T23:59:59.999999999Z");
        ObjectNode absent = record("unknown-date");
        ObjectNode empty = record("null-date").putNull("delivery_date");
        ObjectNode day = dated("at-start-day", "2024-03-01");
        ObjectNode utc = dated("at-start-utc", "2024-03-01T00:00:00.000000Z");
        ObjectNode offset = dated("legacy-offset", "2024-03-01T05:30:00.000000+0530");
        ObjectNode space = dated("legacy-space", "2024-03-01 00:00:00.000001+00:00");
        ObjectNode end = dated("at-end", "2024-03-02T00:00:00Z");
        ObjectNode late = dated("too-new", "2024-03-02T00:00:00.000000001Z");
        reset(
                ok(page(3, 0, 9, rows(old, absent, empty))),
                ok(page(3, 3, 9, rows(day, utc, offset))),
                ok(page(3, 6, 9, rows(space, end, late))));
        ObjectNode window =
                options()
                        .put("page_size", 3)
                        .put("start_date", "2024-03-01T00:00:00Z")
                        .put("end_date", "2024-03-02T00:00:00Z");
        try (Job job = job(List.of("airplanes"), window, false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(
                    job,
                    List.of(
                            out("airplanes", day),
                            out("airplanes", utc),
                            out("airplanes", offset),
                            out("airplanes", space),
                            out("airplanes", end)));
        }
        expectCalls(List.of("airplanes:3:0", "airplanes:3:3", "airplanes:3:6"));
    }

    @Test
    void preservesUndatedAircraftInAnUnfilteredRunAndSurfacesAnInvalidBoundedDate()
            throws Exception {
        ObjectNode absent = record("missing");
        ObjectNode empty = record("null").putNull("delivery_date");
        reset(ok(page(100, 0, 2, rows(absent, empty))));
        try (Job job = job(List.of("airplanes"), options(), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of(out("airplanes", absent), out("airplanes", empty)));
        }
        expectCalls(List.of("airplanes:100:0"));

        reset(ok(page(100, 0, 1, rows(dated("invalid", "2024-02-30T00:00:00Z")))));
        try (Job job =
                job(
                        List.of("airplanes"),
                        options().put("start_date", "2024-01-01T00:00:00Z"),
                        false,
                        false)) {
            bad(
                    engine.executeJob(job.configName),
                    "delivery_date is not a supported provider date");
        }
        expectCalls(List.of("airplanes:100:0"));
    }

    @Test
    void cleanEmptyCatalogContinuesToTheNextSelectedResource() throws Exception {
        ObjectNode tax = record("one-tax");
        reset(ok(page(100, 0, 0, rows())), ok(page(100, 0, 1, rows(tax))));
        try (Job job = job(List.of("countries", "taxes"), options(), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of(out("taxes", tax)));
        }
        expectCalls(List.of("countries:100:0", "taxes:100:0"));
    }

    @Test
    void failsOnContradictoryPaginationEmptyNonterminalAndRepeatedProviderPages() throws Exception {
        ObjectNode item = record("one");
        ObjectNode countMismatch = page(100, 0, 1, rows(item));
        ((ObjectNode) countMismatch.get("pagination")).put("count", 0);
        checkPagination(List.of(ok(countMismatch)), "contradictory pagination", 0);
        checkPagination(List.of(ok(page(100, 1, 2, rows(item)))), "contradictory pagination", 0);
        checkPagination(List.of(ok(page(100, 0, 1, rows()))), "empty page before", 0);
        checkPagination(List.of(ok(page(100, 0, 2, rows(item, item)))), "duplicate record ID", 0);
        checkPagination(
                List.of(
                        ok(page(100, 0, 2, rows(item))),
                        ok(page(100, 1, 2, rows(item.deepCopy().put("mutated", true))))),
                "provider repeated a catalog page",
                0,
                1);
        checkPagination(
                List.of(ok(page(100, 0, 2, rows(item))), ok(page(100, 1, 3, rows(record("two"))))),
                "provider catalog total changed",
                0,
                1);
    }

    @Test
    void boundsTheLogicalPagesWithoutRequestingAnExtraPage() throws Exception {
        reset(ok(page(100, 0, 2, rows(record("one")))));
        try (Job job =
                job(
                        List.of("airports", "taxes"),
                        options().put("max_pages_per_resource", 1),
                        false,
                        false)) {
            bad(engine.executeJob(job.configName), "max_pages_per_resource exhausted");
        }
        expectCalls(List.of("airports:100:0"));
    }

    @Test
    void failsEachKnownQuotaImmediatelyWithoutContinuingToAnotherCatalog() throws Exception {
        for (String type :
                List.of(
                        "usage_limit_reached",
                        "daily_usage_limit_reached",
                        "fair_use_limit_reached")) {
            reset(
                    ok(page(100, 0, 2, rows(record("already-read")))),
                    reply(429, "plain", "1", error(104, type)));
            try (Job job = job(List.of("airlines", "countries"), options(), false, false)) {
                bad(
                        engine.executeJob(job.configName),
                        "account request quota exhausted (" + type + ")");
            }
            expectCalls(List.of("airlines:100:0", "airlines:100:1"));
        }
        reset(reply(200, "plain", "", error("usage_limit_reached")));
        try (Job job = job(List.of("airlines", "countries"), options(), false, false)) {
            bad(
                    engine.executeJob(job.configName),
                    "account request quota exhausted (usage_limit_reached)");
        }
        expectCalls(List.of("airlines:100:0"));
    }

    @Test
    void separatesAttemptsAndRecognizesAPlanDenialEvenInsideAnHttp200Envelope() throws Exception {
        for (int status : new int[] {200, 403}) {
            reset(reply(status, "plain", "", error(105, "function_access_restricted")));
            try (Job job = job(List.of("airports", "airlines"), options(), false, false)) {
                bad(engine.executeJob(job.configName), "the account plan denied this resource");
            }
            expectCalls(List.of("airports:100:0"));
        }
        reset(reply(401, "plain", "", error(101, "invalid_access_key")));
        try (Job job = job(List.of("airports"), options(), false, false)) {
            bad(engine.executeJob(job.configName), "invalid_access_key");
        }
        expectCalls(List.of("airports:100:0"));

        reset(reply(429, "plain", "", error("other")));
        try (Job job = job(List.of("airports"), options(), false, false)) {
            bad(engine.executeJob(job.configName), "without a recognized transient rate limit");
        }
        expectCalls(List.of("airports:100:0"));
    }

    @Test
    void honorsRetryAfterAndDoesNotHideRetriesInTheTransport() throws Exception {
        ObjectNode tax = record("retried");
        reset(
                reply(429, "plain", "1", error(106, "rate_limit_reached")),
                reply(503, "plain", "0", error(503, "maintenance_mode")),
                ok(page(100, 0, 1, rows(tax))));
        try (Job job = job(List.of("taxes"), options().put("max_attempts", 3), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of(out("taxes", tax)));
        }
        expectCalls(List.of("taxes:100:0", "taxes:100:0", "taxes:100:0"));
        List<Seen> calls = seen();
        assertTrue(TimeUnit.NANOSECONDS.toMillis(calls.get(1).at - calls.get(0).at) >= 950L);
        assertTrue(TimeUnit.NANOSECONDS.toMillis(calls.get(2).at - calls.get(1).at) >= 350L);

        reset(reply(503, "plain", "0", error(503, "maintenance_mode")));
        try (Job job = job(List.of("taxes"), options().put("max_attempts", 1), false, false)) {
            bad(engine.executeJob(job.configName), "HTTP 503 retry budget exhausted");
        }
        expectCalls(List.of("taxes:100:0"));

        reset(
                reply(200, "plain", "", error("rate_limit_reached")),
                reply(429, "plain", "", error(106, "rate_limit_reached")));
        try (Job job = job(List.of("taxes"), options().put("max_attempts", 2), false, false)) {
            bad(engine.executeJob(job.configName), "rate_limit_reached retry budget exhausted");
        }
        expectCalls(List.of("taxes:100:0", "taxes:100:0"));
    }

    @Test
    void boundsDecompressedBytesAndAcceptsAValidGzipPage() throws Exception {
        ObjectNode small = record("gzip").put("airport_name", "Zürich");
        reset(reply(200, "gzip", "", page(100, 0, 1, rows(small))));
        try (Job job =
                job(List.of("airports"), options().put("max_response_bytes", 1024), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of(out("airports", small)));
        }
        expectCalls(List.of("airports:100:0"));

        reset(
                reply(
                        200,
                        "gzip",
                        "",
                        page(100, 0, 1, rows(record("gzip").put("expanded", "x".repeat(5000))))));
        try (Job job =
                job(List.of("airports"), options().put("max_response_bytes", 1024), false, false)) {
            bad(engine.executeJob(job.configName), "response exceeds max_response_bytes");
        }
        expectCalls(List.of("airports:100:0"));
    }

    @Test
    void enforcesTrustAndHostnameAndPreventsCredentialRedirects() throws Exception {
        reset();
        try (Job job =
                job(
                        List.of("airports"),
                        options().put("trust_certificate", OTHER_PEM),
                        false,
                        false)) {
            bad(engine.executeJob(job.configName), "TLS certificate or hostname validation failed");
        }
        try (Job job =
                job(
                        List.of("airports"),
                        options().put("endpoint", "https://aviationstack-other-host:18443"),
                        false,
                        false)) {
            bad(engine.executeJob(job.configName), "TLS certificate or hostname validation failed");
        }
        expectCalls(List.of());

        reset(reply(302, "redirect", "", options()));
        try (Job job = job(List.of("airports"), options(), false, false)) {
            bad(engine.executeJob(job.configName), "provider rejected the request (HTTP 302)");
        }
        expectCalls(List.of("airports:100:0"));
    }

    @Test
    void closesAContinuouslyTricklingResponseAtTheDeadline() throws Exception {
        reset(stall());
        try (Job job =
                job(List.of("airports"), options().put("request_timeout_ms", 1300), false, false)) {
            bad(engine.executeJob(job.configName), "request deadline exceeded");
        }
        await(() -> trace().contains("disconnect\t/v1/airports"), "timeout disconnect");
        expectCalls(List.of("airports:100:0"));

        reset(reply(429, "plain", "60", error(106, "rate_limit_reached")));
        try (Job job =
                job(List.of("airports"), options().put("request_timeout_ms", 1300), false, false)) {
            bad(engine.executeJob(job.configName), "retry delay exceeds request deadline");
        }
        expectCalls(List.of("airports:100:0"));
    }

    @Test
    void cancelsAStalledFirstProviderCallWithoutIssuingAnother() throws Exception {
        reset(stall());
        try (Job job =
                job(
                        List.of("airports", "countries"),
                        options().put("request_timeout_ms", 60000),
                        true,
                        false)) {
            String id = id();
            Future<Container.ExecResult> running = start(job, id);
            try {
                await(() -> seen().size() == 1, "active provider transfer");
            } finally {
                cancel(id, running);
            }
            await(
                    () -> trace().contains("disconnect\t/v1/airports"),
                    "job cancellation disconnect");
            expectCalls(List.of("airports:100:0"));
        }
    }

    @Test
    void restoresAStalledNextPageAndKeepsTheCatalogTransitionAndSinkRowsExact() throws Exception {
        ObjectNode first = record("page-zero");
        ObjectNode finalAirport = record("page-one");
        ObjectNode tax = record("tax");
        reset(ok(page(100, 0, 2, rows(first))), stall());
        try (Job job =
                job(
                        List.of("airports", "taxes"),
                        options().put("request_timeout_ms", 60000),
                        true,
                        false)) {
            String id = id();
            Future<Container.ExecResult> running = start(job, id);
            try {
                await(() -> seen().size() == 2, "the next offset request");
                awaitOutput(job, List.of(out("airports", first)));
                good(engine.savepointJob(id));
                running.get(45, TimeUnit.SECONDS);
            } finally {
                if (!running.isDone()) {
                    cancel(id, running);
                }
            }
            await(
                    () -> trace().contains("disconnect\t/v1/airports"),
                    "savepoint closed the old call");
            queue(ok(page(100, 1, 2, rows(finalAirport))), ok(page(100, 0, 1, rows(tax))));
            good(engine.restoreJob(job.configName, id));
            assertOutput(
                    job,
                    List.of(
                            out("airports", first),
                            out("airports", finalAirport),
                            out("taxes", tax)));
        }
        expectCalls(List.of("airports:100:0", "airports:100:1", "airports:100:1", "taxes:100:0"));
    }

    @Test
    void savesTheInitialUnfinishedRequestAndRejectsAChangedCatalogScope() throws Exception {
        reset(stall());
        try (Job job =
                        job(
                                List.of("airports", "taxes"),
                                options().put("request_timeout_ms", 60000),
                                true,
                                false);
                Job changed = job(List.of("taxes", "airports"), options(), true, false)) {
            String id = id();
            Future<Container.ExecResult> running = start(job, id);
            try {
                await(() -> seen().size() == 1, "the first catalog request");
                good(engine.savepointJob(id));
                running.get(45, TimeUnit.SECONDS);
            } finally {
                if (!running.isDone()) {
                    cancel(id, running);
                }
            }
            await(
                    () -> trace().contains("disconnect\t/v1/airports"),
                    "initial call released during savepoint");
            bad(engine.restoreJob(changed.configName, id), "read scope changed");
        }
        expectCalls(List.of("airports:100:0"));
    }

    @Test
    void retainsTheTerminalPositionWhenRestoringBesideAnIndependentPipeline() throws Exception {
        ObjectNode finalRow = record("exactly-one");
        reset(ok(page(100, 0, 1, rows(finalRow))));
        try (Job job = job(List.of("countries"), options(), true, true)) {
            String id = id();
            Future<Container.ExecResult> running = start(job, id);
            try {
                awaitOutput(job, List.of(out("countries", finalRow)));
                good(engine.savepointJob(id));
                running.get(45, TimeUnit.SECONDS);
            } finally {
                if (!running.isDone()) {
                    cancel(id, running);
                }
            }
            Future<Container.ExecResult> restored =
                    submissions.submit(() -> engine.restoreJob(job.configName, id));
            try {
                await(
                        () -> "RUNNING".equals(engine.getJobStatus(id)),
                        "restored independent pipeline");
                long previous = engine.checkpointCount(id);
                await(
                        () -> engine.checkpointCount(id) > previous,
                        "second completed live checkpoint");
                assertOutput(job, List.of(out("countries", finalRow)));
                expectCalls(List.of("countries:100:0"));
            } finally {
                cancel(id, restored);
            }
        }
    }

    @Test
    void rejectsBadConfigurationBeforeOpeningTheProviderConnection() throws Exception {
        reset();
        try (Job job =
                job(
                        List.of("airplanes"),
                        options().put("endpoint", "http://aviationstack-peer:18443"),
                        false,
                        false)) {
            bad(engine.executeJob(job.configName), "expected an HTTPS origin");
        }
        try (Job job = job(List.of("airplanes", "airplanes"), options(), false, false)) {
            bad(engine.executeJob(job.configName), "select each required catalog once");
        }
        try (Job job =
                job(
                        List.of("airports"),
                        options().put("start_date", "2024-02-21T00:00:00Z"),
                        false,
                        false)) {
            bad(engine.executeJob(job.configName), "a delivery window requires airplanes");
        }
        expectCalls(List.of());
    }

    @Test
    void restoresTheExactPartialPageAndRejectsAChangedTailBeforePublishing() throws Exception {
        List<String> wanted =
                IntStream.range(0, 67)
                        .mapToObj(index -> "partial-" + index)
                        .collect(Collectors.toList());
        ArrayNode data = JSON.createArrayNode();
        wanted.forEach(value -> data.add(record(value)));
        ArrayNode changed = data.deepCopy();
        changed.set(66, record("different-tail"));
        reset(
                ok(page(100, 0, 67, data)),
                ok(page(100, 0, 67, data)),
                ok(page(100, 0, 67, changed)));
        AviationstackRecoveryProbe reader = recovery("airports", null);
        List<String> initial = new ArrayList<>();
        byte[] state = reader.firstChunk(initial);
        assertEquals(wanted.subList(0, 32), initial);
        List<String> recovered = new ArrayList<>();
        reader.consume(state, recovered);
        assertEquals(wanted.subList(32, 67), recovered);

        List<String> refused = new ArrayList<>();
        Exception changedPage = assertThrows(Exception.class, () -> reader.consume(state, refused));
        assertTrue(changedPage.getMessage().contains("provider page changed after checkpoint"));
        assertTrue(refused.isEmpty());
        expectCalls(List.of("airports:100:0", "airports:100:0", "airports:100:0"));
    }

    @Test
    void keepsTheImplicitDateWindowAcrossSerializationBeforeFirstAssignment() throws Exception {
        reset();
        AviationstackRecoveryProbe reader = recovery("airplanes", "2024-01-01T00:00:00Z");
        byte[] state = reader.recoverUnassigned();
        expectCalls(List.of());
        Instant cutoff = AviationstackRecoveryProbe.frozenEnd(state);
        queue(
                ok(
                        page(
                                100,
                                0,
                                2,
                                rows(
                                        dated("at-cutoff", cutoff.toString()),
                                        dated("late", cutoff.plusNanos(1).toString())))));
        List<String> output = new ArrayList<>();
        reader.consume(state, output);
        assertEquals(List.of("at-cutoff"), output);
        expectCalls(List.of("airplanes:100:0"));
    }

    @Test
    void cancellationBeforeAssignmentPreventsTheReaderFromOpeningTransport() throws Exception {
        reset();
        List<String> collected = new ArrayList<>();
        recovery("airports", null).stopBeforeAssignment(collected);
        assertTrue(collected.isEmpty());
        expectCalls(List.of());
    }

    private AviationstackRecoveryProbe recovery(String resource, String startDate)
            throws Exception {
        URI url =
                new URI("https", null, peer.getHost(), peer.getMappedPort(18443), null, null, null);
        return new AviationstackRecoveryProbe(
                url.toString(), certificate.toString(), ACCESS_KEY, resource, startDate);
    }

    private void checkPagination(List<String> tape, String expected, long... offsets)
            throws Exception {
        reset(tape.toArray(new String[0]));
        try (Job job = job(List.of("airports", "countries"), options(), false, false)) {
            bad(engine.executeJob(job.configName), expected);
        }
        List<String> requests = new ArrayList<>();
        for (long offset : offsets) {
            requests.add("airports:100:" + offset);
        }
        expectCalls(requests);
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
            List<String> catalogs, ObjectNode overrides, boolean stream, boolean anchor)
            throws Exception {
        String name = "aviationstack-" + UUID.randomUUID();
        String path = "/tmp/seatunnel_mnt/" + name;
        ObjectNode settings =
                options()
                        .put("plugin_output", "catalog_data")
                        .put("access_key", ACCESS_KEY)
                        .put("endpoint", ENDPOINT)
                        .put("trust_certificate", PEM);
        ArrayNode resources = settings.putArray("resources");
        catalogs.forEach(resources::add);
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
                        + "source {\n Aviationstack "
                        + settings
                        + "\n"
                        + anchorInput
                        + "}\n"
                        + "sink {\n LocalFile {\n plugin_input=catalog_data\n path=\""
                        + path
                        + "\"\n"
                        + " file_format_type=json\n is_enable_transaction=true\n custom_filename=true\n"
                        + " file_name_expression=\"catalog_$"
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
        return options().put("id", id);
    }

    private static ObjectNode dated(String id, String time) {
        return record(id).put("delivery_date", time);
    }

    private static ArrayNode rows(ObjectNode... records) {
        ArrayNode array = JSON.createArrayNode();
        for (ObjectNode record : records) {
            array.add(record);
        }
        return array;
    }

    private static ObjectNode page(long limit, long offset, long total, ArrayNode data) {
        ObjectNode envelope = options();
        envelope.putObject("pagination")
                .put("limit", limit)
                .put("offset", offset)
                .put("count", data.size())
                .put("total", total);
        envelope.set("data", data);
        return envelope;
    }

    private static ObjectNode error(int code, String type) {
        ObjectNode envelope = options().put("success", false);
        envelope.putObject("error").put("code", code).put("type", type).put("info", HIDDEN_MESSAGE);
        return envelope;
    }

    private static ObjectNode error(String code) {
        ObjectNode envelope = options();
        envelope.putObject("error").put("code", code).put("message", HIDDEN_MESSAGE);
        return envelope;
    }

    private static ObjectNode out(String catalog, ObjectNode record) {
        ObjectNode row =
                options().put("resource", catalog).put("record_id", record.path("id").asText());
        row.set("payload", record);
        return row;
    }

    private static String reply(int status, String mode, String retryAfter, JsonNode body) {
        String encoded =
                Base64.getEncoder()
                        .encodeToString(body.toString().getBytes(StandardCharsets.UTF_8));
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
            if (frame.startsWith("call\t")) {
                String[] parts = frame.split("\t", -1);
                assertEquals("GET", parts[1]);
                assertEquals("true", parts[4], "the key belongs in the provider query only");
                URI request = URI.create(parts[2]);
                assertTrue(
                        request.getPath().startsWith("/v1/"), "only catalog routes may be called");
                Map<String, String> query = new TreeMap<>();
                assertTrue(request.getRawQuery() != null);
                for (String param : request.getRawQuery().split("&")) {
                    String[] tuple = param.split("=", 2);
                    assertEquals(2, tuple.length);
                    assertNull(
                            query.put(
                                    tuple[0], URLDecoder.decode(tuple[1], StandardCharsets.UTF_8)),
                            "no duplicate provider query arguments");
                }
                assertEquals(ACCESS_KEY, query.remove("access_key"));
                assertEquals(List.of("limit", "offset"), List.copyOf(query.keySet()));
                requests.add(
                        new Seen(
                                request.getPath().substring(4)
                                        + ":"
                                        + query.get("limit")
                                        + ":"
                                        + query.get("offset"),
                                Long.parseLong(parts[3])));
            }
        }
        return requests;
    }

    private void expectCalls(List<String> requests) throws Exception {
        assertEquals(
                requests, seen().stream().map(call -> call.describe).collect(Collectors.toList()));
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
        assertFalse(visible.contains("aviation-fixture-private"), "access key leaked");
        assertFalse(visible.contains(HIDDEN_MESSAGE), "provider error text leaked");
        assertFalse(visible.contains("access_key=aviation"), "request URI leaked");
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
        private final String describe;
        private final long at;

        private Seen(String describe, long at) {
            this.describe = describe;
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
                                    + "' -maxdepth 1 -type f -name 'catalog_*' -exec cat {} +; fi");
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
            // The catalog pipeline can already have ended. Observe all active pipeline counters.
            long completed = 0;
            for (JsonNode pipeline : JSON.readTree(response.body()).path("pipelines")) {
                completed += pipeline.path("counts").path("completed").asLong();
            }
            return completed;
        }
    }
}
