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

package org.apache.seatunnel.e2e.connector.alpacabroker;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Exercises the native Source through real SeaTunnel engines against a strict HTTPS broker.
 *
 * <p>Tests compare engine output with the requests observed by the separate provider fixture.
 */
@Execution(ExecutionMode.SAME_THREAD)
class AlpacaBrokerSourceIT extends TestSuiteBase implements TestResource {
    private static final ObjectMapper JSON =
            new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    private static final String HOST = "alpaca-fixture";
    private static final String ORIGIN = "https://" + HOST + ":8443";
    private static final String KEY = AlpacaBrokerFixture.KEY;
    private static final String SECRET = AlpacaBrokerFixture.SECRET;
    private static final String CERTIFICATE = "/tmp/seatunnel-broker-cert.pem";
    private static final String ACTIVITY_BOUNDARY = "2024-02-29T00:00:00Z";
    private static final String ACTIVITY_END = "2024-03-01T00:00:00Z";
    private static final String CURSOR = "opaque /?+= %中文";
    private static final String ACCOUNT_END = "2021-01-01T00:00:00Z";

    private GenericContainer<?> broker;
    private Path files;

    /**
     * Start an independent fixture. Each test resets its routes before making a request.
     *
     * <p>Only the test-generated certificate is installed in engine containers for fixture
     * requests.
     */
    @BeforeAll
    @Override
    public void startUp() throws Exception {
        files = Files.createTempDirectory("broker-api-it-");
        generateCertificate("broker", HOST);
        generateCertificate("unrelated", HOST);
        Path compiledTests = artifact(AlpacaBrokerFixture.class);
        broker =
                new GenericContainer<>(DockerImageName.parse("eclipse-temurin:11-jdk"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST, "alpaca-unlisted-host")
                        .withExposedPorts(8080)
                        .withCopyFileToContainer(
                                MountableFile.forHostPath(compiledTests), "/broker/classes")
                        .withCopyFileToContainer(
                                MountableFile.forHostPath(files.resolve("broker.p12")),
                                "/broker/tls.p12")
                        .waitingFor(Wait.forHttp("/ready").forStatusCode(200));
        Set<Path> jackson = new LinkedHashSet<>();
        for (Class<?> dependency :
                List.of(ObjectMapper.class, JsonFactory.class, JsonProperty.class)) {
            jackson.add(artifact(dependency));
        }
        StringBuilder classpath = new StringBuilder("/broker/classes");
        int next = 0;
        for (Path library : jackson) {
            String dependency = "/broker/jackson-" + next++ + ".jar";
            broker.withCopyFileToContainer(MountableFile.forHostPath(library), dependency);
            classpath.append(':').append(dependency);
        }
        broker.withCommand(
                "java",
                "--add-modules",
                "jdk.httpserver",
                "-cp",
                classpath.toString(),
                AlpacaBrokerFixture.class.getName(),
                "/broker/tls.p12");
        broker.start();
    }

    private static Path artifact(Class<?> type) throws Exception {
        return Path.of(type.getProtectionDomain().getCodeSource().getLocation().toURI());
    }

    /**
     * Release the fixture and generated credentials even after an assertion fails.
     *
     * <p>Cleanup also runs if only part of the test setup succeeded.
     */
    @AfterAll
    @Override
    public void tearDown() throws Exception {
        try {
            if (broker != null) {
                broker.close();
            }
        } finally {
            if (files != null) {
                for (String name : List.of("broker", "unrelated")) {
                    for (String ext : List.of(".p12", ".pem")) {
                        Files.deleteIfExists(files.resolve(name + ext));
                    }
                }
                Files.deleteIfExists(files);
            }
        }
    }

    @TestTemplate
    void assetRowsPreserveUtf8NullAndFinancialScale(TestContainer engine) throws Exception {
        trusted(engine);
        ObjectNode fixture = object();
        fixture.putArray("steps").add(object().put("mode", "gzip"));
        setup(fixture);
        List<Row> rows = read(engine, options("assets").put("asset_class", "crypto"));
        Assertions.assertEquals(1, rows.size());
        Row asset = rows.get(0);
        Assertions.assertEquals(uuid(1), asset.id);
        Assertions.assertEquals("crypto", asset.payload.path("class").asText());
        Assertions.assertEquals("München 東京", asset.payload.path("name").asText());
        Assertions.assertEquals("0.0000000100", asset.payload.path("price_increment").asText());
        Assertions.assertTrue(asset.payload.path("description").isNull());
        Assertions.assertNull(asset.account);
        Assertions.assertEquals("crypto", trace().get(0).at("/query/asset_class").asText());
        assertBrokerCall(trace().get(0), "/v1/assets");
    }

    @TestTemplate
    void activitiesRespectBothExclusiveBoundsAndAnOpaqueCursor(TestContainer engine)
            throws Exception {
        trusted(engine);
        setup(activityFixture().put("activity_cap", 1));
        ObjectNode options = activityOptions();
        List<Row> rows = read(engine, options);
        Assertions.assertEquals(List.of("a-1", CURSOR, "a-3"), ids(rows));
        Assertions.assertEquals("2024-02-28", rows.get(0).payload.path("date").asText());
        Assertions.assertEquals(
                "12345678901234567890.000100", rows.get(0).payload.path("amount").asText());
        Assertions.assertTrue(rows.get(1).payload.path("optional").isNull());
        JsonNode calls = trace();
        Assertions.assertEquals(4, calls.size());
        Assertions.assertEquals(CURSOR, calls.get(2).at("/query/page_token").asText());
        Assertions.assertEquals("a-3", calls.get(3).at("/query/page_token").asText());
        for (JsonNode call : calls) {
            assertBrokerCall(call, "/v1/accounts/activities");
            Assertions.assertEquals("asc", call.at("/query/direction").asText());
            Assertions.assertEquals(ACTIVITY_BOUNDARY, call.at("/query/after").asText());
            Assertions.assertEquals(ACTIVITY_END, call.at("/query/until").asText());
        }

        setup(activityFixture().put("repeat_cursor", true));
        String logs = rejected(engine, options, "exclusive activity cursor was repeated");
        Assertions.assertEquals(List.of("a-1", CURSOR), ids(rows(logs, "accounts_activities")));
        Assertions.assertEquals(2, trace().size());
    }

    @TestTemplate
    void accountTimestampCohortReturnsEveryDistinctIdAcrossTheFullPage(TestContainer engine)
            throws Exception {
        trusted(engine);
        setup(object().put("count", 1003));
        ObjectNode options =
                options("accounts")
                        .put("start_time", "2020-01-01T01:00:00+01:00")
                        .put("end_time", ACCOUNT_END);
        List<Row> rows = read(engine, options);
        Assertions.assertEquals(1003, rows.size());
        Assertions.assertEquals(1003, new HashSet<>(ids(rows)).size());
        for (int index = 1; index <= rows.size(); index++) {
            Assertions.assertEquals(uuid(index), rows.get(index - 1).id);
        }
        Assertions.assertEquals(
                "123456789012345678901234.0000100",
                rows.get(999).payload.path("last_equity").asText());
        Assertions.assertTrue(rows.get(999).payload.at("/details/optional").isNull());
        JsonNode calls = trace();
        Assertions.assertEquals(2, calls.size());
        Assertions.assertEquals(
                "2020-01-01T00:00:00Z", calls.get(0).at("/query/created_after").asText());
        Assertions.assertEquals(
                "2020-01-02T00:00:00.123456Z", calls.get(1).at("/query/created_after").asText());
        for (JsonNode call : calls) {
            assertBrokerCall(call, "/v1/accounts");
            Assertions.assertEquals(ACCOUNT_END, call.at("/query/created_before").asText());
        }
    }

    @TestTemplate
    void ambiguousAllTiedPageFailsAndEmptyOrPartialListsFinish(TestContainer engine)
            throws Exception {
        trusted(engine);
        ObjectNode opts = options("accounts").put("end_time", ACCOUNT_END);
        setup(object().put("count", 1000).put("same_timestamp", true));
        Assertions.assertTrue(
                rows(rejected(engine, opts, "cannot paginate the tie"), "accounts").isEmpty());
        Assertions.assertEquals(1, trace().size());

        setup(object().put("count", 3));
        Assertions.assertEquals(
                List.of(uuid(2), uuid(3)),
                ids(read(engine, opts.deepCopy().put("start_time", "2020-01-02T00:00:00Z"))));
        Assertions.assertEquals(1, trace().size());
        setup(object().put("count", 0));
        Assertions.assertTrue(read(engine, opts).isEmpty());
        Assertions.assertEquals(1, trace().size());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void traversesDocumentsUnderOlderParentsAndRetainsLeapDayEdges(TestContainer engine)
            throws Exception {
        trusted(engine);
        ObjectNode state = object().put("count", 1001);
        ObjectNode docs = state.putObject("documents");
        docs.putArray(uuid(1))
                .add(document(201, "2024-02-28"))
                .add(document(202, "2024-02-29"))
                .add(document(203, "2024-03-01"));
        docs.putArray(uuid(1001)).add(document(204, "2024-02-29"));
        setup(state);
        ObjectNode docsOptions =
                options("account_documents")
                        .put("start_date", "2024-02-29")
                        .put("end_date", "2024-02-29")
                        .put("request_timeout_ms", 20000);
        List<Row> rows = read(engine, docsOptions);
        Assertions.assertEquals(List.of(uuid(202), uuid(204)), ids(rows));
        Assertions.assertEquals(uuid(1), rows.get(0).account);
        Assertions.assertEquals(uuid(1001), rows.get(1).account);
        Assertions.assertEquals("2024-02-29", rows.get(0).payload.path("date").asText());
        JsonNode calls = trace();
        Assertions.assertEquals(1003, calls.size());
        Assertions.assertTrue(calls.get(0).at("/query/created_after").isMissingNode());
        Assertions.assertEquals(
                "2020-01-02T00:00:00.123456Z", calls.get(999).at("/query/created_after").asText());
        String fixedEnd = calls.get(0).at("/query/created_before").asText();
        Assertions.assertEquals(fixedEnd, calls.get(999).at("/query/created_before").asText());
        assertBrokerCall(calls.get(1), "/v1/accounts/" + uuid(1) + "/documents");
        assertBrokerCall(calls.get(1002), "/v1/accounts/" + uuid(1001) + "/documents");
        Assertions.assertEquals("2024-02-29", calls.get(1002).at("/query/start").asText());

        setup(object());
        Assertions.assertTrue(read(engine, docsOptions).isEmpty());
        Assertions.assertEquals(1, trace().size());
        assertBrokerCall(trace().get(0), "/v1/accounts");
    }

    @TestTemplate
    void calendarDistinguishesTradingAndSettlementDates(TestContainer engine) throws Exception {
        trusted(engine);
        ObjectNode fixture = object();
        fixture.putArray("calendar")
                .add(day("2024-02-28", "2024-02-29", "16:00"))
                .add(day("2024-02-29", "2024-03-01", "13:00"))
                .add(day("2024-03-01", "2024-03-04", "16:00"));
        setup(fixture);
        ObjectNode opts =
                options("calendar").put("start_date", "2024-02-29").put("end_date", "2024-02-29");
        List<Row> trading = read(engine, opts);
        Assertions.assertEquals(List.of("2024-02-29"), ids(trading));
        Assertions.assertEquals("13:00", trading.get(0).payload.path("close").asText());
        Assertions.assertEquals("trading", trace().get(0).at("/query/date_type").asText());
        assertBrokerCall(trace().get(0), "/v1/calendar");

        setup(fixture);
        Assertions.assertEquals(
                List.of("2024-02-28"),
                ids(read(engine, opts.deepCopy().put("calendar_date_type", "settlement"))));
        Assertions.assertEquals("settlement", trace().get(0).at("/query/date_type").asText());
    }

    @TestTemplate
    void clockAndCountryDictionaryUseStableRecordIdentities(TestContainer engine) throws Exception {
        trusted(engine);
        setup(object());
        List<Row> clock = read(engine, options("clock"));
        Assertions.assertEquals(List.of("2024-02-29T09:30:10.123456789-05:00"), ids(clock));
        Assertions.assertTrue(clock.get(0).payload.path("is_open").asBoolean());
        Assertions.assertTrue(clock.get(0).payload.path("nullable").isNull());
        assertBrokerCall(trace().get(0), "/v1/clock");

        setup(object());
        List<Row> countries = read(engine, options("country-info"));
        Assertions.assertEquals(Set.of("ALA", "USA"), new HashSet<>(ids(countries)));
        Row usa = countries.stream().filter(row -> row.id.equals("USA")).findFirst().orElseThrow();
        Row island =
                countries.stream().filter(row -> row.id.equals("ALA")).findFirst().orElseThrow();
        Assertions.assertEquals("restricted", usa.payload.path("securities_risk_rating").asText());
        Assertions.assertTrue(usa.payload.path("unknown_extra").isNull());
        Assertions.assertEquals("Åland Islands", island.payload.path("full_name").asText());
        assertBrokerCall(trace().get(0), "/v1/country-info");
    }

    @TestTemplate
    void retriesOnlyWithinExplicitPolicyAndHonorsRateReset(TestContainer engine) throws Exception {
        trusted(engine);
        ObjectNode fixture = object();
        ArrayNode steps = fixture.putArray("steps");
        ObjectNode rateLimit = object().put("status", 429).put("rate_reset_seconds", 2);
        rateLimit.putObject("headers").put("Retry-After", "1");
        steps.add(rateLimit);
        ObjectNode unavailable = object().put("status", 503);
        unavailable.putObject("headers").put("Retry-After", "0");
        steps.add(unavailable);
        setup(fixture);
        Assertions.assertEquals(
                1, read(engine, options("assets").put("request_timeout_ms", 9000)).size());
        JsonNode calls = trace();
        Assertions.assertEquals(3, calls.size());
        long first =
                calls.get(1).path("monotonic_ms").asLong()
                        - calls.get(0).path("monotonic_ms").asLong();
        long second =
                calls.get(2).path("monotonic_ms").asLong()
                        - calls.get(1).path("monotonic_ms").asLong();
        Assertions.assertTrue(first >= 950, "retried before the advertised rate window");
        Assertions.assertTrue(
                calls.get(1).path("epoch_ms").asLong() >= calls.get(0).path("reset_at_ms").asLong(),
                "retried before the provider's quota reset");
        Assertions.assertTrue(second >= 350, "transport bypassed the explicit retry policy");

        ObjectNode single = object();
        single.putArray("steps").add(unavailable);
        setup(single);
        rejected(engine, options("assets").put("max_attempts", 1), "HTTP 503");
        Assertions.assertEquals(1, trace().size());

        rateLimit.putObject("headers").put("Retry-After", "15");
        setup(fixture);
        rejected(
                engine,
                options("assets").put("request_timeout_ms", 3000),
                "retry delay exceeds request deadline");
        Assertions.assertEquals(1, trace().size());
    }

    @TestTemplate
    void keepsCredentialsPrivateAndRejectsRedirectsAndInvalidBodies(TestContainer engine)
            throws Exception {
        trusted(engine);
        ObjectNode options = options("assets");
        setup(object());
        rejected(
                engine, options.deepCopy().put("password", "test-wrong-broker-secret"), "HTTP 401");
        Assertions.assertEquals(1, trace().size());
        Assertions.assertFalse(trace().get(0).path("auth").asBoolean());

        ObjectNode script = object();
        script.putArray("steps").add(object().put("status", 403));
        setup(script);
        rejected(engine, options, "HTTP 403");
        Assertions.assertEquals(1, trace().size());

        ObjectNode redirect = object().put("status", 302);
        redirect.putObject("headers").put("Location", ORIGIN + "/v1/clock");
        script.putArray("steps").add(redirect);
        setup(script);
        rejected(engine, options, "HTTP 302");
        Assertions.assertEquals(1, trace().size());
        assertBrokerCall(trace().get(0), "/v1/assets");

        script.putArray("steps").add(object().put("body", "[{\"id\": "));
        setup(script);
        rejected(engine, options, "response is not a complete JSON document");
        script.putArray("steps").add(object().put("body", "[{\"id\":\"x\",\"id\":\"y\"}]"));
        setup(script);
        rejected(engine, options, "response is not a complete JSON document");
        script.putArray("steps")
                .add(object().put("body", "{\"message\":\"sensitive-broker-error\"}"));
        setup(script);
        rejected(engine, options, "expected an array response");

        script.putArray("steps")
                .add(
                        object().put("mode", "gzip")
                                .put(
                                        "body",
                                        "[{\"id\":\"id\",\"large\":\""
                                                + "x".repeat(4096)
                                                + "\"}]"));
        setup(script);
        rejected(
                engine,
                options.deepCopy().put("max_response_bytes", 1024),
                "response exceeds max_response_bytes");
        Assertions.assertEquals(1, trace().size());
    }

    @TestTemplate
    void sandboxOverrideKeepsTlsTrustAndLiveCredentialsSeparate(TestContainer engine)
            throws Exception {
        trusted(engine);
        String unrelated = "/tmp/seatunnel-broker-unrelated.pem";
        engine.copyAbsolutePathToContainer(files.resolve("unrelated.pem").toString(), unrelated);
        ObjectNode options = options("assets");
        setup(object());
        rejected(engine, options.deepCopy().put("trust_certificate", unrelated), "TLS trust");
        rejected(
                engine,
                options.deepCopy().put("base_url", "https://alpaca-unlisted-host:8443"),
                "TLS trust");
        rejected(engine, options.deepCopy().put("environment", "LIVE"), "custom sandbox origin");
        rejected(
                engine,
                options.deepCopy().put("base_url", "http://" + HOST + ":8443"),
                "expected a custom HTTPS origin");
        rejected(
                engine,
                options.deepCopy().put("base_url", "https://broker-api.alpaca.markets"),
                "expected a custom HTTPS origin");
        Assertions.assertEquals(0, trace().size());
        Assertions.assertEquals(1, read(engine, options).size());
        assertBrokerCall(trace().get(0), "/v1/assets");
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void closesHeldAndTrickledBodiesAtDeadlineAndOnCancellation(TestContainer engine)
            throws Exception {
        trusted(engine);
        for (String mode : List.of("hold", "trickle")) {
            ObjectNode fixture = object();
            fixture.putArray("steps").add(object().put("mode", mode));
            setup(fixture);
            rejected(
                    engine,
                    options("assets").put("request_timeout_ms", 1600),
                    "request deadline exceeded");
            awaitDisconnect(0, 4500L);
            Assertions.assertEquals(1, trace().size());
        }

        ObjectNode fixture = object();
        fixture.putArray("steps").add(object().put("mode", "hold"));
        setup(fixture);
        Path config = jobFile(options("assets").put("request_timeout_ms", 90000), true, false);
        try (EngineJob job = EngineJob.submit(engine, config)) {
            Awaitility.await()
                    .atMost(75, TimeUnit.SECONDS)
                    .until(() -> trace().path(0).path("held").asBoolean());
            job.cancel();
            awaitDisconnect(0, 20000L);
            Assertions.assertEquals(1, trace().size());
        } finally {
            Files.deleteIfExists(config);
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void restoresTheExactPendingCursorAndRejectsAChangedWindow(TestContainer engine)
            throws Exception {
        trusted(engine);
        setup(activityFixture().put("hold_token", CURSOR));
        ObjectNode options = activityOptions().put("request_timeout_ms", 120000);
        Path config = jobFile(options, true, false);
        Path otherEnd =
                jobFile(options.deepCopy().put("end_time", "2024-03-02T00:00:00Z"), true, false);
        String before = engine.getServerLogs();
        try (EngineJob first = EngineJob.submit(engine, config)) {
            Awaitility.await()
                    .atMost(75, TimeUnit.SECONDS)
                    .until(() -> trace().path(1).path("held").asBoolean());
            Awaitility.await()
                    .atMost(20, TimeUnit.SECONDS)
                    .until(
                            () ->
                                    ids(rows(fresh(engine, before), "accounts_activities"))
                                            .equals(List.of("a-1", CURSOR)));
            awaitNewCheckpoint(engine, first.id);
            first.savepoint();
            awaitDisconnect(1, 30000L);
            Assertions.assertEquals(2, trace().size());

            String incompatibility = engine.getServerLogs();
            Container.ExecResult changed =
                    engine.restoreJobWithCheckpoint(resource(otherEnd), first.id, jobId());
            Assertions.assertNotEquals(0, changed.getExitCode());
            String logs = diagnostics(engine, incompatibility, changed);
            Assertions.assertTrue(
                    logs.contains("differ") || logs.contains("changed source options"), logs);
            Assertions.assertEquals(2, trace().size());

            String after = engine.getServerLogs();
            Container.ExecResult restored =
                    engine.restoreJobWithCheckpoint(resource(config), first.id, jobId());
            okay(restored);
            Assertions.assertEquals(
                    List.of("a-3"), ids(output(engine, after, restored, "accounts_activities")));
            JsonNode calls = trace();
            Assertions.assertEquals(4, calls.size());
            Assertions.assertEquals(CURSOR, calls.get(2).at("/query/page_token").asText());
            Assertions.assertEquals(ACTIVITY_END, calls.get(2).at("/query/until").asText());
            Assertions.assertEquals("a-3", calls.get(3).at("/query/page_token").asText());
        } finally {
            Files.deleteIfExists(otherEnd);
            Files.deleteIfExists(config);
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void accountSavepointPreservesTheTimestampCohortAndFrozenUpperBound(TestContainer engine)
            throws Exception {
        trusted(engine);
        String boundary = "2020-01-02T00:00:00.123456Z";
        setup(object().put("count", 1003).put("hold_created_after", boundary));
        Path config = jobFile(options("accounts").put("request_timeout_ms", 120000), true, false);
        String before = engine.getServerLogs();
        try (EngineJob first = EngineJob.submit(engine, config)) {
            Awaitility.await()
                    .atMost(75, TimeUnit.SECONDS)
                    .until(() -> trace().path(1).path("held").asBoolean());
            Awaitility.await()
                    .atMost(20, TimeUnit.SECONDS)
                    .until(() -> rows(fresh(engine, before), "accounts").size() == 998);
            String fixedUpper = trace().path(0).at("/query/created_before").asText();
            Assertions.assertEquals(boundary, trace().path(1).at("/query/created_after").asText());
            awaitNewCheckpoint(engine, first.id);
            first.savepoint();
            awaitDisconnect(1, 30000L);
            String resume = engine.getServerLogs();
            Container.ExecResult result =
                    engine.restoreJobWithCheckpoint(resource(config), first.id, jobId());
            okay(result);
            Assertions.assertEquals(
                    List.of(uuid(999), uuid(1000), uuid(1001), uuid(1002), uuid(1003)),
                    ids(output(engine, resume, result, "accounts")));
            Assertions.assertEquals(3, trace().size());
            Assertions.assertEquals(boundary, trace().path(2).at("/query/created_after").asText());
            Assertions.assertEquals(
                    fixedUpper, trace().path(2).at("/query/created_before").asText());
        } finally {
            Files.deleteIfExists(config);
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void documentSavepointResumesTheCurrentParentWithoutRediscovery(TestContainer engine)
            throws Exception {
        trusted(engine);
        String savedParent = "/v1/accounts/" + uuid(2) + "/documents";
        ObjectNode state = object().put("count", 3).put("hold_path", savedParent);
        ObjectNode docs = state.putObject("documents");
        docs.putArray(uuid(1)).add(document(201, "2024-02-29"));
        docs.putArray(uuid(2)).add(document(202, "2024-02-29"));
        setup(state);
        Path config =
                jobFile(
                        options("account_documents")
                                .put("request_timeout_ms", 120000)
                                .put("start_date", "2024-02-29")
                                .put("end_date", "2024-02-29"),
                        true,
                        false);
        String before = engine.getServerLogs();
        try (EngineJob first = EngineJob.submit(engine, config)) {
            Awaitility.await()
                    .atMost(75, TimeUnit.SECONDS)
                    .until(() -> trace().path(2).path("held").asBoolean());
            Awaitility.await()
                    .atMost(20, TimeUnit.SECONDS)
                    .until(
                            () ->
                                    ids(rows(fresh(engine, before), "account_documents"))
                                            .equals(List.of(uuid(201))));
            awaitNewCheckpoint(engine, first.id);
            first.savepoint();
            awaitDisconnect(2, 30000L);
            String resume = engine.getServerLogs();
            Container.ExecResult result =
                    engine.restoreJobWithCheckpoint(resource(config), first.id, jobId());
            okay(result);
            List<Row> remaining = output(engine, resume, result, "account_documents");
            Assertions.assertEquals(List.of(uuid(202)), ids(remaining));
            Assertions.assertEquals(uuid(2), remaining.get(0).account);
            Assertions.assertEquals(5, trace().size());
            assertBrokerCall(trace().get(3), savedParent);
            assertBrokerCall(trace().get(4), "/v1/accounts/" + uuid(3) + "/documents");
        } finally {
            Files.deleteIfExists(config);
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void terminalSavepointDoesNotRequestOrEmitTheResourceAgain(TestContainer engine)
            throws Exception {
        trusted(engine);
        setup(object());
        Path config = jobFile(options("assets"), true, true);
        String before = engine.getServerLogs();
        try (EngineJob first = EngineJob.submit(engine, config)) {
            Awaitility.await()
                    .atMost(75, TimeUnit.SECONDS)
                    .until(
                            () ->
                                    ids(rows(fresh(engine, before), "assets"))
                                            .equals(List.of(uuid(1))));
            awaitNewCheckpoint(engine, first.id);
            first.savepoint();
            Assertions.assertEquals(1, trace().size());

            String after = engine.getServerLogs();
            try (EngineJob resumed = EngineJob.restore(engine, config, first.id)) {
                status(engine, resumed.id, "RUNNING");
                awaitNewCheckpoint(engine, resumed.id);
                Assertions.assertEquals(1, trace().size());
                Assertions.assertTrue(rows(fresh(engine, after), "assets").isEmpty());
                resumed.cancel();
            }
        } finally {
            Files.deleteIfExists(config);
        }
    }

    private static ObjectNode activityOptions() {
        return options("accounts_activities")
                .put("start_time", ACTIVITY_BOUNDARY)
                .put("end_time", ACTIVITY_END)
                .put("page_size", 2);
    }

    private static ObjectNode activityFixture() {
        ObjectNode scenario = object();
        scenario.putArray("activity_rows")
                .add(activity("at-lower", ACTIVITY_BOUNDARY, "2024-02-29"))
                .add(activity("a-1", "2024-02-29T00:00:01Z", "2024-02-28"))
                .add(activity(CURSOR, "2024-02-29T00:01:01Z", "2024-02-29"))
                .add(activity("a-3", "2024-02-29T00:02:01Z", "2024-02-29"))
                .add(activity("at-upper", ACTIVITY_END, "2024-03-01"));
        return scenario;
    }

    private static ObjectNode activity(String id, String created, String date) {
        return object().put("id", id)
                .put("created_at", created)
                .put("date", date)
                .put("amount", "12345678901234567890.000100")
                .putNull("optional");
    }

    private static ObjectNode day(String date, String settlement, String close) {
        return object().put("date", date)
                .put("settlement_date", settlement)
                .put("open", "09:30")
                .put("close", close);
    }

    private static ObjectNode document(int id, String date) {
        return object().put("id", uuid(id))
                .put("date", date)
                .put("type", "account_statement")
                .putNull("sub_type");
    }

    private static String uuid(int index) {
        return String.format(Locale.ROOT, "02518f15-2069-4120-8da3-%012x", index);
    }

    private static ObjectNode object() {
        return JSON.createObjectNode();
    }

    private static ObjectNode options(String resource) {
        return object().put("resource", resource)
                .put("username", KEY)
                .put("password", SECRET)
                .put("base_url", ORIGIN)
                .put("trust_certificate", CERTIFICATE);
    }

    private void trusted(TestContainer engine) {
        engine.copyAbsolutePathToContainer(files.resolve("broker.pem").toString(), CERTIFICATE);
    }

    private static void assertBrokerCall(JsonNode call, String path) {
        Assertions.assertEquals(path, call.path("path").asText());
        Assertions.assertEquals(HOST + ":8443", call.path("host").asText());
        Assertions.assertTrue(call.path("auth").asBoolean());
        Assertions.assertFalse(call.path("trading_headers").asBoolean());
    }

    private void setup(ObjectNode scenario) throws IOException {
        request("/setup", scenario);
    }

    private JsonNode trace() throws IOException {
        return request("/trace", null);
    }

    private JsonNode request(String path, ObjectNode content) throws IOException {
        HttpURLConnection connection =
                (HttpURLConnection)
                        new URL("http", broker.getHost(), broker.getMappedPort(8080), path)
                                .openConnection();
        connection.setConnectTimeout(4000);
        connection.setReadTimeout(8000);
        try {
            if (content != null) {
                connection.setRequestMethod("POST");
                connection.setDoOutput(true);
                connection.setRequestProperty("Content-Type", "application/json");
                try (OutputStream out = connection.getOutputStream()) {
                    JSON.writeValue(out, content);
                }
            }
            Assertions.assertEquals(200, connection.getResponseCode());
            try (InputStream in = connection.getInputStream()) {
                return JSON.readTree(in);
            }
        } finally {
            connection.disconnect();
        }
    }

    private void awaitDisconnect(int call, long maximumMs) throws IOException {
        Awaitility.await()
                .atMost(12, TimeUnit.SECONDS)
                .until(() -> trace().path(call).path("disconnect_ms").isIntegralNumber());
        Assertions.assertTrue(trace().path(call).path("disconnect_ms").asLong() < maximumMs);
    }

    private static void awaitNewCheckpoint(TestContainer engine, String job) {
        long baseline = engine.getCompletedCheckpointCount(job);
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .until(() -> engine.getCompletedCheckpointCount(job) > baseline);
    }

    private static List<Row> read(TestContainer container, ObjectNode options) throws Exception {
        String before = container.getServerLogs();
        Path config = jobFile(options, false, false);
        try {
            Container.ExecResult result = container.executeJob(resource(config));
            okay(result);
            return output(container, before, result, options.path("resource").asText());
        } finally {
            Files.deleteIfExists(config);
        }
    }

    private static String rejected(TestContainer engine, ObjectNode options, String reason)
            throws Exception {
        String before = engine.getServerLogs();
        Path config = jobFile(options, false, false);
        try {
            Container.ExecResult result = engine.executeJob(resource(config));
            String all = diagnostics(engine, before, result);
            Assertions.assertNotEquals(0, result.getExitCode(), all);
            Assertions.assertTrue(all.contains(reason), () -> "Expected " + reason + "\n" + all);
            Assertions.assertFalse(all.contains(KEY));
            Assertions.assertFalse(all.contains(SECRET));
            Assertions.assertFalse(all.contains(options.path("password").asText()));
            Assertions.assertFalse(all.contains("sensitive-broker-error"));
            String logs = fresh(engine, before);
            return logs.contains("SeaTunnelRow#kind=") ? logs : result.getStdout();
        } finally {
            Files.deleteIfExists(config);
        }
    }

    private static String diagnostics(
            TestContainer engine, String before, Container.ExecResult result) {
        return result.getStdout() + "\n" + result.getStderr() + "\n" + fresh(engine, before);
    }

    private static List<Row> output(
            TestContainer engine, String before, Container.ExecResult result, String resource)
            throws IOException {
        List<Row> captured = rows(fresh(engine, before), resource);
        return captured.isEmpty() ? rows(result.getStdout(), resource) : captured;
    }

    private static List<Row> rows(String log, String resource) throws IOException {
        List<Row> data = new ArrayList<>();
        String start = " : " + resource + ", ";
        for (String line : log.split("\\R")) {
            int kind = line.indexOf("SeaTunnelRow#kind=");
            int index = kind < 0 ? -1 : line.indexOf(start, kind);
            if (index >= 0) {
                String[] fields = line.substring(index + start.length()).split(", ", 3);
                Assertions.assertEquals(3, fields.length, line);
                data.add(
                        new Row(
                                fields[0],
                                fields[1].isEmpty() ? null : fields[1],
                                JSON.readTree(fields[2])));
            }
        }
        return data;
    }

    private static List<String> ids(List<Row> rows) {
        return rows.stream().map(row -> row.id).collect(Collectors.toList());
    }

    private static String fresh(TestContainer engine, String previous) {
        String log = engine.getServerLogs();
        return log.startsWith(previous) ? log.substring(previous.length()) : "";
    }

    private static void okay(Container.ExecResult result) {
        Assertions.assertEquals(
                0, result.getExitCode(), result.getStdout() + "\n" + result.getStderr());
    }

    private static String resource(Path file) {
        return "/" + file.getFileName();
    }

    private static String jobId() {
        return Long.toString(JobIdGenerator.newJobId());
    }

    private static void status(TestContainer engine, String job, String state) {
        Awaitility.await()
                .atMost(40, TimeUnit.SECONDS)
                .until(() -> state.equals(engine.getJobStatus(job)));
    }

    private static Path jobFile(ObjectNode options, boolean streaming, boolean keepAlive)
            throws IOException {
        String env = streaming ? "STREAMING\n  checkpoint.interval = 500" : "BATCH";
        ObjectNode selected = options.deepCopy().put("plugin_output", "broker");
        String other =
                keepAlive
                        ? "\n FakeSource {\n plugin_output=keepalive\n schema={fields {marker=string}}\n"
                                + " row.num=1\n split.read-interval=150\n rows=[{kind=INSERT,fields=[\"keep job open\"]}]\n }"
                        : "";
        String inputs = keepAlive ? "[broker,keepalive]" : "[broker]";
        Path resources = ContainerUtil.getCurrentModulePath().resolve("src/test/resources");
        Files.createDirectories(resources);
        Path file = Files.createTempFile(resources, "alpaca-broker-", ".conf");
        return Files.writeString(
                file,
                "env {\n parallelism=1\n job.retry.times=0\n job.mode="
                        + env
                        + "\n}\nsource {\n AlpacaBroker "
                        + selected
                        + other
                        + "\n}\nsink {\n Console { plugin_input="
                        + inputs
                        + " }\n}\n");
    }

    private void generateCertificate(String name, String dns) throws Exception {
        Path key = files.resolve(name + ".p12");
        String password = AlpacaBrokerFixture.KEYSTORE_PASSWORD;
        Process child =
                new ProcessBuilder(
                                Path.of(System.getProperty("java.home"), "bin", "keytool")
                                        .toString(),
                                "-genkeypair",
                                "-noprompt",
                                "-alias",
                                "broker",
                                "-keyalg",
                                "RSA",
                                "-keysize",
                                "2048",
                                "-validity",
                                "3",
                                "-storetype",
                                "PKCS12",
                                "-dname",
                                "CN=" + dns,
                                "-ext",
                                "SAN=DNS:" + dns,
                                "-keystore",
                                key.toString(),
                                "-storepass",
                                password)
                        .redirectErrorStream(true)
                        .start();
        try {
            Assertions.assertTrue(child.waitFor(30, TimeUnit.SECONDS));
            Assertions.assertEquals(
                    0,
                    child.exitValue(),
                    new String(child.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
        } finally {
            child.destroyForcibly();
        }
        KeyStore certs = KeyStore.getInstance("PKCS12");
        try (InputStream stream = Files.newInputStream(key)) {
            certs.load(stream, password.toCharArray());
        }
        writePem(
                files.resolve(name + ".pem"),
                "CERTIFICATE",
                certs.getCertificate("broker").getEncoded());
    }

    private static void writePem(Path file, String label, byte[] bytes) throws IOException {
        String base64 = Base64.getMimeEncoder(64, new byte[] {'\n'}).encodeToString(bytes);
        Files.writeString(
                file,
                "-----BEGIN " + label + "-----\n" + base64 + "\n-----END " + label + "-----\n",
                StandardCharsets.US_ASCII);
    }

    /**
     * Parsed Console sink output for one original Broker API object.
     *
     * <p>The test retains the payload as a JSON tree so it can assert values, types, and nulls
     * independently.
     */
    private static final class Row {
        private final String id;
        private final String account;
        private final JsonNode payload;

        private Row(String id, String account, JsonNode payload) {
            this.id = id;
            this.account = account;
            this.payload = payload;
        }
    }

    /**
     * Runs the blocking job CLI separately so the test can drive its control plane.
     *
     * <p>This lets a test issue a real savepoint or cancellation while the provider holds a
     * response.
     */
    private static final class EngineJob implements AutoCloseable {
        private final TestContainer engine;
        private final String id = jobId();
        private final ExecutorService executor = Executors.newSingleThreadExecutor();
        private final Future<Container.ExecResult> finished;

        private EngineJob(TestContainer engine, Path file, String prior) {
            this.engine = engine;
            finished =
                    executor.submit(
                            () ->
                                    prior == null
                                            ? engine.executeJob(resource(file), id)
                                            : engine.restoreJobWithCheckpoint(
                                                    resource(file), prior, id));
        }

        private static EngineJob submit(TestContainer engine, Path file) {
            return new EngineJob(engine, file, null);
        }

        private static EngineJob restore(TestContainer engine, Path file, String prior) {
            return new EngineJob(engine, file, prior);
        }

        private void savepoint() throws Exception {
            okay(engine.savepointJob(id));
            status(engine, id, "SAVEPOINT_DONE");
            okay(finished.get(60, TimeUnit.SECONDS));
        }

        private void cancel() throws Exception {
            okay(engine.cancelJob(id));
            status(engine, id, "CANCELED");
            finished.get(60, TimeUnit.SECONDS);
        }

        /**
         * Prevent an assertion failure from leaving a job running in the shared test container.
         *
         * <p>A terminal job is left alone so cleanup does not obscure the original failure.
         */
        @Override
        public void close() throws Exception {
            try {
                String current = engine.getJobStatus(id);
                if ("RUNNING".equals(current) || "SCHEDULED".equals(current)) {
                    engine.cancelJob(id);
                }
            } finally {
                executor.shutdownNow();
            }
        }
    }
}
