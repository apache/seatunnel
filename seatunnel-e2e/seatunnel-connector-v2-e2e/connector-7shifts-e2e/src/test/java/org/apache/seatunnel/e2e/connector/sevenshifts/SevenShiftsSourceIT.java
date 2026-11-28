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

package org.apache.seatunnel.e2e.connector.sevenshifts;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
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
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Execution(ExecutionMode.SAME_THREAD)
class SevenShiftsSourceIT extends TestSuiteBase implements TestResource {
    private static final String HOST = "sevenshifts-fixture";
    private static final String ORIGIN = "http://" + HOST + ":8080";
    private static final String TOKEN = "fixture-seven-shifts-read-9674";
    private static final String PRIVATE_ERROR = "private-seven-shifts-body-5162";
    private static final String CURSOR = "page +中/=?&";
    private static final String LAST = "last+batch/?=";
    private static final String START = "2026-04-06T01:00:00.123456789+02:00";
    private static final long FIRST_COMPANY = 9007199254740993L;
    private static final long SECOND_COMPANY = 9007199254740995L;
    private static final long FIRST_USER = 9007199254742017L;
    private static final long SECOND_USER = 9007199254742019L;
    private static final ObjectMapper JSON =
            new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    private GenericContainer<?> fixture;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        Path keys = Files.createTempDirectory("seatunnel-7shifts-tls-");
        try {
            certificate(keys);
            fixture =
                    new GenericContainer<>(DockerImageName.parse("python:3.11-alpine"))
                            .withNetwork(NETWORK)
                            .withNetworkAliases(HOST)
                            .withExposedPorts(8080)
                            .withCopyFileToContainer(
                                    MountableFile.forHostPath(
                                            ContainerUtil.getResourcesFile(
                                                            "/seven_shifts_fixture.py")
                                                    .toPath()),
                                    "/fixture.py")
                            .withCopyFileToContainer(
                                    MountableFile.forHostPath(keys.resolve("cert.pem")),
                                    "/fixture-cert.pem")
                            .withCopyFileToContainer(
                                    MountableFile.forHostPath(keys.resolve("key.pem")),
                                    "/fixture-key.pem")
                            .withCommand("python", "/fixture.py")
                            .waitingFor(Wait.forHttp("/health").forStatusCode(200));
            fixture.start();
        } finally {
            Files.deleteIfExists(keys.resolve("cert.pem"));
            Files.deleteIfExists(keys.resolve("key.pem"));
            Files.deleteIfExists(keys.resolve("fixture.p12"));
            Files.deleteIfExists(keys);
        }
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (fixture != null) {
            fixture.close();
        }
    }

    @TestTemplate
    void companiesFollowOpaqueCursorsAndKeepExactData(TestContainer container) throws Exception {
        reset(settings("gzip"));
        List<OutputRow> rows = successful(container, options("companies").put("start_date", START));
        Assertions.assertEquals(2, rows.size());
        OutputRow first = row(rows, "company-" + FIRST_COMPANY);
        Assertions.assertEquals(FIRST_COMPANY, first.company);
        Assertions.assertNull(first.user);
        Assertions.assertEquals("Équipe 中文", first.payload.path("name").asText());
        Assertions.assertEquals(FIRST_COMPANY, first.payload.path("id").asLong());
        Assertions.assertTrue(first.payload.path("nullable").isNull());
        Assertions.assertEquals(9007199254740997L, first.payload.at("/nested/0/number").asLong());
        Assertions.assertTrue(first.payload.at("/nested/0/flag").asBoolean());
        Assertions.assertEquals(
                0,
                new BigDecimal("12345678901234567890.12345678901234567890")
                        .compareTo(first.payload.path("amount").decimalValue()));
        Assertions.assertEquals(SECOND_COMPANY, row(rows, "company-" + SECOND_COMPANY).company);
        JsonNode calls = calls();
        Assertions.assertEquals(3, calls.size());
        Assertions.assertEquals(CURSOR, calls.get(1).at("/query/cursor/0").asText());
        Assertions.assertEquals(LAST, calls.get(2).at("/query/cursor/0").asText());
        for (JsonNode call : calls) {
            assertProtocol(call, "/v2/companies");
            Assertions.assertEquals("2026-04-04", call.at("/query/modified_since/0").asText());
            Assertions.assertFalse(call.path("query").has("limit"));
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void readsAllCompanyResourcesUsingUnfilteredParentDiscovery(TestContainer container)
            throws Exception {
        for (String kind :
                Arrays.asList(
                        "locations", "departments", "roles", "users", "time_punches", "shifts")) {
            reset(settings("deflate"));
            List<OutputRow> rows =
                    successful(
                            container, options(kind).put("start_date", START).put("page_size", 3));
            Assertions.assertEquals("locations".equals(kind) ? 3 : 2, rows.size(), kind);
            for (long company : new long[] {FIRST_COMPANY, SECOND_COMPANY}) {
                String marker =
                        ("users".equals(kind)
                                ? "employee-" + company + "-" + FIRST_USER
                                : kind + "-" + company);
                OutputRow result = row(rows, marker);
                Assertions.assertEquals(company, result.company);
                if ("users".equals(kind) || "time_punches".equals(kind)) {
                    Assertions.assertEquals(Long.valueOf(FIRST_USER), result.user);
                } else if ("shifts".equals(kind)) {
                    Assertions.assertEquals(Long.valueOf(0L), result.user);
                } else {
                    Assertions.assertNull(result.user);
                }
                if ("roles".equals(kind)) {
                    Assertions.assertEquals(0, result.payload.path("department_id").asInt(-1));
                }
            }
            if ("locations".equals(kind)) {
                Assertions.assertTrue(
                        row(rows, "location-after-cursor").payload.path("modified").isNull());
            }
            JsonNode calls = calls();
            Assertions.assertEquals("locations".equals(kind) ? 7 : 5, calls.size(), kind);
            for (int i = 0; i < 3; i++) {
                assertProtocol(calls.get(i), "/v2/companies");
                Assertions.assertFalse(calls.get(i).path("query").has("modified_since"));
            }
            String since =
                    "time_punches".equals(kind) || "shifts".equals(kind)
                            ? "2026-04-05T22:59:59Z"
                            : "2026-04-04";
            for (int i = 3; i < calls.size(); i++) {
                JsonNode call = calls.get(i);
                assertProtocol(
                        call,
                        i == calls.size() - 1
                                ? companyPath(SECOND_COMPANY, kind)
                                : companyPath(FIRST_COMPANY, kind));
                Assertions.assertEquals("3", call.at("/query/limit/0").asText());
                Assertions.assertEquals(since, call.at("/query/modified_since/0").asText());
            }
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void readsEveryUserResourceWithCompanyAndUserIdentity(TestContainer container)
            throws Exception {
        for (String kind :
                Arrays.asList(
                        "wages",
                        "assignments",
                        "location_assignments",
                        "department_assignments",
                        "role_assignments")) {
            reset(settings(null));
            List<OutputRow> rows = successful(container, options(kind));
            Assertions.assertEquals(3, rows.size(), kind);
            long[][] parents = {
                {FIRST_COMPANY, FIRST_USER},
                {FIRST_COMPANY, SECOND_USER},
                {SECOND_COMPANY, FIRST_USER}
            };
            List<String> routes = new ArrayList<>();
            for (long[] parent : parents) {
                OutputRow result = row(rows, kind + "-" + parent[0] + "-" + parent[1]);
                Assertions.assertEquals(parent[0], result.company);
                Assertions.assertEquals(Long.valueOf(parent[1]), result.user);
                routes.add(companyPath(parent[0], "users/" + parent[1] + "/" + kind));
                switch (kind) {
                    case "wages":
                        Assertions.assertEquals(
                                2001, result.payload.at("/current_wages/0/wage_cents").asInt());
                        Assertions.assertEquals(
                                2500, result.payload.at("/upcoming_wages/0/wage_cents").asInt());
                        break;
                    case "assignments":
                        Assertions.assertEquals(15, result.payload.at("/locations/0/id").asInt());
                        Assertions.assertTrue(result.payload.path("departments").isArray());
                        Assertions.assertEquals(45, result.payload.at("/roles/0/id").asInt());
                        break;
                    case "location_assignments":
                        Assertions.assertEquals(15, result.payload.path("location_id").asInt());
                        break;
                    case "department_assignments":
                        Assertions.assertEquals(30, result.payload.path("department_id").asInt());
                        break;
                    case "role_assignments":
                        Assertions.assertEquals(45, result.payload.path("role_id").asInt());
                        Assertions.assertTrue(result.payload.path("is_primary").asBoolean());
                        break;
                    default:
                        throw new AssertionError(kind);
                }
            }
            JsonNode requests = calls();
            Assertions.assertEquals(9, requests.size(), kind);
            for (int i = 0; i < requests.size(); i++) {
                JsonNode call = requests.get(i);
                Assertions.assertFalse(call.path("query").has("modified_since"));
                Assertions.assertFalse(call.path("query").has("status"));
                if (i < 3) {
                    assertProtocol(call, "/v2/companies");
                } else if (i < 5) {
                    assertProtocol(call, companyPath(FIRST_COMPANY, "users"));
                } else if (i == 7) {
                    assertProtocol(call, companyPath(SECOND_COMPANY, "users"));
                } else {
                    assertProtocol(call, routes.remove(0));
                    Assertions.assertTrue(call.path("query").isEmpty());
                }
            }
            Assertions.assertTrue(routes.isEmpty());
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void emptyDiscoveryAndInvalidConfigurationsCauseNoChildRequest(TestContainer container)
            throws Exception {
        reset(settings("empty"));
        Assertions.assertTrue(successful(container, options("wages")).isEmpty());
        Assertions.assertEquals(1, calls().size());
        assertProtocol(calls().get(0), "/v2/companies");

        reset(settings(null));
        failed(container, options("wages").put("start_date", START), "start_date is unsupported");
        failed(
                container,
                options("users").put("allow_insecure_http", false),
                "base_url must be an HTTPS origin");
        failed(
                container,
                options("users").put("api_version", "2026-02-30"),
                "api_version must use a valid");
        Assertions.assertTrue(calls().isEmpty());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void honorsRetryAfterAndBoundedAttempts(TestContainer container) throws Exception {
        reset(steps("429", "dated_retry", "gzip", "deflate"));
        Assertions.assertEquals(
                2, successful(container, options("companies").put("start_date", START)).size());
        JsonNode calls = calls();
        Assertions.assertEquals(5, calls.size());
        Assertions.assertTrue(
                calls.get(1).path("received_ms").asLong()
                                - calls.get(0).path("received_ms").asLong()
                        >= 1000);
        Assertions.assertTrue(
                calls.get(2).path("received_ms").asLong() + 10
                        >= calls.get(1).path("retry_after_monotonic_ms").asLong());
        for (int i = 0; i < 3; i++) {
            Assertions.assertFalse(calls.get(i).path("query").has("cursor"));
        }

        reset(settings("500"));
        failed(
                container,
                options("companies").put("max_attempts", 2),
                "HTTP 500 exhausted the configured attempts");
        Assertions.assertEquals(2, calls().size());

        reset(settings("long_retry"));
        failed(
                container,
                options("companies").put("request_timeout_ms", 3000),
                "retry delay exceeds the request deadline");
        Assertions.assertEquals(1, calls().size());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void deniesUntrustedTlsAndNeverFollowsRedirectsOrRetriesAuth(TestContainer container)
            throws Exception {
        reset(settings(null));
        failed(
                container,
                options("companies").put("token", TOKEN + "-invalid"),
                "unexpected HTTP 401");
        Assertions.assertEquals(1, calls().size());
        Assertions.assertFalse(calls().get(0).path("auth").asBoolean());

        reset(settings("403"));
        failed(container, options("companies"), "unexpected HTTP 403");
        Assertions.assertEquals(1, calls().size());

        reset(settings("404"));
        failed(container, options("wages"), "unexpected HTTP 404");
        Assertions.assertEquals(1, calls().size());

        reset(settings("redirect"));
        failed(container, options("companies"), "unexpected HTTP 307");
        Assertions.assertEquals(1, calls().size());

        reset(settings(null));
        failed(
                container,
                options("companies")
                        .put("base_url", "https://" + HOST + ":8443")
                        .put("allow_insecure_http", false),
                "TLS transport failed");
        Assertions.assertTrue(calls().isEmpty());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void rejectsMalformedPagesBeforePublishingRows(TestContainer container) throws Exception {
        for (String[] test :
                new String[][] {
                    {"invalid_json", "invalid JSON page"},
                    {"invalid_shape", "data must be an array"},
                    {"invalid_cursor", "invalid next cursor"},
                    {"zip_bomb", "response exceeds 8 MiB"}
                }) {
            reset(settings(test[0]));
            String logs = failed(container, options("companies"), test[1]);
            Assertions.assertTrue(output(logs, "companies").isEmpty());
            Assertions.assertEquals(1, calls().size());
        }
        reset(settings("wrong_parent"));
        String logs = failed(container, options("locations"), "parent ID does not match");
        Assertions.assertTrue(output(logs, "locations").isEmpty());
        Assertions.assertEquals(4, calls().size());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void rejectsRepeatedCursorBeforePublishingItsPage(TestContainer container) throws Exception {
        reset(settings("repeat"));
        String logs = failed(container, options("companies"), "pagination repeated a cursor");
        List<OutputRow> rows = output(logs, "companies");
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals(FIRST_COMPANY, rows.get(0).company);
        Assertions.assertEquals(2, calls().size());
        Assertions.assertEquals(CURSOR, calls().get(1).at("/query/cursor/0").asText());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void closesHeldAndTrickledPagesAtTheAbsoluteDeadline(TestContainer container) throws Exception {
        for (String mode : Arrays.asList("hold", "trickle")) {
            reset(settings(mode));
            String logs =
                    failed(
                            container,
                            options("companies").put("request_timeout_ms", 1500),
                            "request deadline exceeded");
            Assertions.assertTrue(output(logs, "companies").isEmpty());
            Awaitility.await()
                    .atMost(6, TimeUnit.SECONDS)
                    .until(() -> calls().get(0).path("disconnect_ms").isIntegralNumber());
            long disconnected = calls().get(0).path("disconnect_ms").asLong();
            Assertions.assertTrue(disconnected < 5000, mode + " disconnect " + disconnected);
            Assertions.assertEquals(1, calls().size());
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void cancelClosesTheInFlightReadWithoutStartingAnother(TestContainer container)
            throws Exception {
        reset(settings("hold"));
        Path file = config(options("companies").put("request_timeout_ms", 90000), "BATCH");
        String id = jobId();
        ExecutorService jobs = Executors.newSingleThreadExecutor();
        try {
            Future<Container.ExecResult> running =
                    jobs.submit(() -> container.executeJob(resource(file), id));
            Awaitility.await()
                    .atMost(90, TimeUnit.SECONDS)
                    .until(() -> calls().path(0).path("held").asBoolean());
            succeed(container.cancelJob(id));
            status(container, id, "CANCELED");
            running.get(60, TimeUnit.SECONDS);
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .until(() -> calls().path(0).path("disconnect_ms").isIntegralNumber());
            Assertions.assertTrue(calls().path(0).path("disconnect_ms").asLong() < 90000);
            Assertions.assertEquals(1, calls().size());
        } finally {
            try {
                request("/release", "");
                cancelIfRunning(container, id);
            } finally {
                jobs.shutdownNow();
                Files.deleteIfExists(file);
            }
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void restoresThePendingCompanyCursorAfterANewerCheckpoint(TestContainer container)
            throws Exception {
        reset(
                settings(null)
                        .put("hold_cursor", CURSOR)
                        .put("hold_path", companyPath(FIRST_COMPANY, "locations")));
        Path file =
                config(
                        options("locations").put("request_timeout_ms", 120000),
                        "STREAMING\n  checkpoint.interval = 1000");
        String initial = jobId();
        String resumed = jobId();
        ExecutorService jobs = Executors.newSingleThreadExecutor();
        String firstLog = container.getServerLogs();
        try {
            Future<Container.ExecResult> running =
                    jobs.submit(() -> container.executeJob(resource(file), initial));
            Awaitility.await()
                    .atMost(90, TimeUnit.SECONDS)
                    .until(() -> calls().path(4).path("held").asBoolean());
            Awaitility.await()
                    .atMost(25, TimeUnit.SECONDS)
                    .until(() -> fresh(container, firstLog).contains("locations-" + FIRST_COMPANY));
            long baseline = container.getCompletedCheckpointCount(initial);
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> container.getCompletedCheckpointCount(initial) > baseline);
            Assertions.assertEquals(5, calls().size());
            succeed(container.savepointJob(initial));
            status(container, initial, "SAVEPOINT_DONE");
            succeed(running.get(60, TimeUnit.SECONDS));
            Assertions.assertEquals(5, calls().size());

            request("/release", "");
            String restoreLog = container.getServerLogs();
            Container.ExecResult result =
                    container.restoreJobWithCheckpoint(resource(file), initial, resumed);
            succeed(result);
            List<OutputRow> rows = output(result, fresh(container, restoreLog), "locations");
            Assertions.assertEquals(2, rows.size());
            Assertions.assertEquals(FIRST_COMPANY, row(rows, "location-after-cursor").company);
            Assertions.assertEquals(
                    SECOND_COMPANY, row(rows, "locations-" + SECOND_COMPANY).company);
            Assertions.assertEquals(8, calls().size());
            Assertions.assertEquals(CURSOR, calls().path(5).at("/query/cursor/0").asText());
            Assertions.assertEquals(LAST, calls().path(6).at("/query/cursor/0").asText());
            assertProtocol(calls().path(7), companyPath(SECOND_COMPANY, "locations"));
        } finally {
            try {
                request("/release", "");
                cancelIfRunning(container, initial);
                cancelIfRunning(container, resumed);
            } finally {
                jobs.shutdownNow();
                Files.deleteIfExists(file);
            }
        }
    }

    private static void assertProtocol(JsonNode call, String path) {
        Assertions.assertEquals(path, call.path("path").asText());
        Assertions.assertTrue(call.path("auth").asBoolean(), "Missing or incorrect Bearer header");
        Assertions.assertTrue(call.path("version").asBoolean(), "Missing or incorrect API version");
        Assertions.assertTrue(
                call.path("customer").asBoolean(), "Direct token must omit x-company-guid");
    }

    private static String companyPath(long company, String endpoint) {
        return "/v2/company/" + company + "/" + endpoint;
    }

    private static OutputRow row(List<OutputRow> rows, String marker) {
        return rows.stream()
                .filter(value -> marker.equals(value.payload.path("marker").asText()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing output row " + marker));
    }

    private List<OutputRow> successful(TestContainer container, ObjectNode options)
            throws Exception {
        String before = container.getServerLogs();
        Container.ExecResult result = run(container, options);
        succeed(result);
        return output(result, fresh(container, before), options.path("resource").asText());
    }

    private String failed(TestContainer container, ObjectNode options, String reason)
            throws Exception {
        String before = container.getServerLogs();
        Container.ExecResult result = run(container, options);
        String logs =
                result.getStdout() + "\n" + result.getStderr() + "\n" + fresh(container, before);
        Assertions.assertNotEquals(0, result.getExitCode(), logs);
        Assertions.assertTrue(
                logs.contains(reason),
                () -> "Expected " + reason + " in this job's diagnostics\n" + logs);
        Assertions.assertFalse(logs.contains(options.path("token").asText()));
        Assertions.assertFalse(logs.contains(TOKEN));
        Assertions.assertFalse(logs.contains(PRIVATE_ERROR));
        String server = fresh(container, before);
        return server.contains("SeaTunnelRow#kind=") ? server : result.getStdout();
    }

    private static List<OutputRow> output(Container.ExecResult result, String logs, String resource)
            throws IOException {
        List<OutputRow> fromServer = output(logs, resource);
        return fromServer.isEmpty() ? output(result.getStdout(), resource) : fromServer;
    }

    private static List<OutputRow> output(String logs, String resource) throws IOException {
        List<OutputRow> rows = new ArrayList<>();
        String marker = " : " + resource + ", ";
        for (String line : logs.split("\\R")) {
            int kind = line.indexOf("SeaTunnelRow#kind=");
            int from = kind < 0 ? -1 : line.indexOf(marker, kind);
            if (from >= 0) {
                String[] parts = line.substring(from + 3).split(", ", 4);
                Assertions.assertEquals(4, parts.length, line);
                rows.add(
                        new OutputRow(
                                Long.parseLong(parts[1]),
                                parts[2].isEmpty() ? null : Long.valueOf(parts[2]),
                                JSON.readTree(parts[3])));
            }
        }
        return rows;
    }

    private static String fresh(TestContainer container, String baseline) {
        String logs = container.getServerLogs();
        return logs.startsWith(baseline) ? logs.substring(baseline.length()) : "";
    }

    private static void succeed(Container.ExecResult result) {
        Assertions.assertEquals(
                0, result.getExitCode(), result.getStderr() + "\n" + result.getStdout());
    }

    private static Container.ExecResult run(TestContainer container, ObjectNode options)
            throws Exception {
        Path file = config(options, "BATCH");
        try {
            return container.executeJob(resource(file));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    private static Path config(ObjectNode options, String mode) throws IOException {
        String cfg =
                "env {\n  parallelism = 1\n  job.retry.times = 0\n  job.mode = "
                        + mode
                        + "\n}\nsource {\n  SevenShifts "
                        + options
                        + "\n}\nsink {\n  Console {}\n}\n";
        Path file =
                Files.createTempFile(
                        ContainerUtil.getCurrentModulePath().resolve("src/test/resources"),
                        "seven-shifts-e2e-",
                        ".conf");
        Files.write(file, cfg.getBytes(StandardCharsets.UTF_8));
        return file;
    }

    private static ObjectNode options(String resource) {
        return JSON.createObjectNode()
                .put("resource", resource)
                .put("token", TOKEN)
                .put("base_url", ORIGIN)
                .put("allow_insecure_http", true);
    }

    private static ObjectNode settings(String byDefault) {
        return JSON.createObjectNode().put("mode", byDefault);
    }

    private static ObjectNode steps(String... steps) {
        ObjectNode config = settings(null);
        Arrays.stream(steps).forEach(config.putArray("steps")::add);
        return config;
    }

    private void reset(ObjectNode settings) throws IOException {
        request("/reset", settings.toString());
    }

    private JsonNode calls() throws IOException {
        return request("/state", null).path("calls");
    }

    private JsonNode request(String route, String body) throws IOException {
        HttpURLConnection connection =
                (HttpURLConnection)
                        new URL("http", fixture.getHost(), fixture.getMappedPort(8080), route)
                                .openConnection();
        connection.setConnectTimeout(5000);
        connection.setReadTimeout(5000);
        try {
            if (body != null) {
                connection.setRequestMethod("POST");
                connection.setDoOutput(true);
                connection.setRequestProperty("Content-Type", "application/json");
                try (OutputStream output = connection.getOutputStream()) {
                    output.write(body.getBytes(StandardCharsets.UTF_8));
                }
                Assertions.assertEquals(204, connection.getResponseCode());
                return null;
            }
            Assertions.assertEquals(200, connection.getResponseCode());
            try (InputStream input = connection.getInputStream()) {
                return JSON.readTree(input);
            }
        } finally {
            connection.disconnect();
        }
    }

    private static String resource(Path file) {
        return "/" + file.getFileName();
    }

    private static String jobId() {
        return Long.toString(JobIdGenerator.newJobId());
    }

    private static void status(TestContainer container, String id, String expected) {
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .until(() -> expected.equals(container.getJobStatus(id)));
    }

    private static void cancelIfRunning(TestContainer container, String id) throws Exception {
        String state = container.getJobStatus(id);
        if ("RUNNING".equals(state) || "SCHEDULED".equals(state)) {
            container.cancelJob(id);
        }
    }

    /** Generates a test-only, untrusted certificate without sharing private keys in the source. */
    private static void certificate(Path directory) throws Exception {
        Path keystore = directory.resolve("fixture.p12");
        Path keytool = Paths.get(System.getProperty("java.home"), "bin", "keytool");
        Process process =
                new ProcessBuilder(
                                keytool.toString(),
                                "-genkeypair",
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                keystore.toString(),
                                "-alias",
                                "fixture",
                                "-storepass",
                                "changeit",
                                "-keyalg",
                                "RSA",
                                "-keysize",
                                "2048",
                                "-validity",
                                "2",
                                "-dname",
                                "CN=" + HOST,
                                "-ext",
                                "SAN=dns:" + HOST,
                                "-noprompt")
                        .redirectOutput(ProcessBuilder.Redirect.INHERIT)
                        .redirectError(ProcessBuilder.Redirect.INHERIT)
                        .start();
        if (!process.waitFor(30, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            throw new IOException("Timed out generating the test certificate");
        }
        Assertions.assertEquals(0, process.exitValue());
        KeyStore keys = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(keystore)) {
            keys.load(in, "changeit".toCharArray());
        }
        pem(
                directory.resolve("cert.pem"),
                "CERTIFICATE",
                keys.getCertificate("fixture").getEncoded());
        pem(
                directory.resolve("key.pem"),
                "PRIVATE KEY",
                keys.getKey("fixture", "changeit".toCharArray()).getEncoded());
    }

    private static void pem(Path file, String label, byte[] value) throws IOException {
        String data = Base64.getMimeEncoder(64, new byte[] {'\n'}).encodeToString(value);
        Files.write(
                file,
                ("-----BEGIN " + label + "-----\n" + data + "\n-----END " + label + "-----\n")
                        .getBytes(StandardCharsets.US_ASCII));
    }

    private static final class OutputRow {
        private final long company;
        private final Long user;
        private final JsonNode payload;

        private OutputRow(long company, Long user, JsonNode payload) {
            this.company = company;
            this.user = user;
            this.payload = payload;
        }
    }
}
