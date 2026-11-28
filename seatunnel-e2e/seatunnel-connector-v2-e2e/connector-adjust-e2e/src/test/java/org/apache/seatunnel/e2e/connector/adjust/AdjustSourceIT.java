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

package org.apache.seatunnel.e2e.connector.adjust;

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
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Execution(ExecutionMode.SAME_THREAD)
class AdjustSourceIT extends TestSuiteBase implements TestResource {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String HOST = "adjust-report-fixture";
    private static final String ORIGIN = "http://" + HOST + ":8080";
    private static final String TEST_TOKEN = "adjust-fixture-report-access-597";
    private static final String PRIVATE_DIAGNOSTIC = "adjust-fixture-private-error-258";

    private GenericContainer<?> server;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        Path tls = Files.createTempDirectory("adjust-report-tls-");
        try {
            generateCertificate(tls);
            server =
                    new GenericContainer<>(DockerImageName.parse("python:3.11-alpine"))
                            .withNetwork(NETWORK)
                            .withNetworkAliases(HOST)
                            .withExposedPorts(8080)
                            .withCopyFileToContainer(
                                    MountableFile.forHostPath(
                                            ContainerUtil.getResourcesFile(
                                                            "/adjust_reports_fixture.py")
                                                    .toPath()),
                                    "/adjust_reports_fixture.py")
                            .withCopyFileToContainer(
                                    MountableFile.forHostPath(tls.resolve("certificate.pem")),
                                    "/fixture.pem")
                            .withCopyFileToContainer(
                                    MountableFile.forHostPath(tls.resolve("certificate.key")),
                                    "/fixture.key")
                            .withCommand("python", "/adjust_reports_fixture.py")
                            .waitingFor(Wait.forHttp("/health").forStatusCode(200));
            server.start();
        } finally {
            Files.deleteIfExists(tls.resolve("report.p12"));
            Files.deleteIfExists(tls.resolve("certificate.pem"));
            Files.deleteIfExists(tls.resolve("certificate.key"));
            Files.deleteIfExists(tls);
        }
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (server != null) {
            server.close();
        }
    }

    @TestTemplate
    void exportsTypedDynamicColumnsAndSkipsEmptyDays(TestContainer engine) throws Exception {
        LocalDate today = LocalDate.now(ZoneOffset.UTC);
        LocalDate start = today.minusDays(4);
        ObjectNode script = modes("gzip");
        script.putObject("days")
                .put(start.plusDays(1).toString(), "204")
                .put(start.plusDays(2).toString(), "empty")
                .put(start.plusDays(3).toString(), "deflate");
        reset(script);
        ObjectNode selection = options(start);
        selection
                .putArray("dimensions")
                .add("app")
                .add("country")
                .add("campaign_id_network")
                .add("day");
        selection.putArray("metrics").add("installs").add("network_cost");
        selection.putArray("additional_metrics").add("purchase_renewals");
        List<String[]> records = succeed(engine, selection);
        Assertions.assertEquals(4, records.size());
        for (int i = 0; i < 4; i += 2) {
            LocalDate expected = start.plusDays(i == 0 ? 0 : 3);
            String[] fields = records.get(i);
            Assertions.assertEquals(7, fields.length, Arrays.toString(fields));
            Assertions.assertArrayEquals(
                    new String[] {
                        expected.toString(),
                        "Café 東京",
                        "DE",
                        "9007199254740993",
                        "9223372036854775808"
                    },
                    Arrays.copyOf(fields, 5));
            decimalEquals("21.000000000000000137", fields[5]);
            decimalEquals("-3.141592653589793238", fields[6]);
            String[] sparse = records.get(i + 1);
            Assertions.assertArrayEquals(
                    new String[] {expected.toString(), "", "JP", "", "", ""},
                    Arrays.copyOf(sparse, 6));
            decimalEquals("0", sparse[6]);
        }
        JsonNode calls = calls();
        Assertions.assertEquals(4, calls.size());
        for (int i = 0; i < 4; i++) {
            assertRequest(
                    calls.path(i),
                    start.plusDays(i),
                    "+00:00",
                    "app,country,campaign_id_network,day",
                    "installs,network_cost,purchase_renewals");
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void anchorsInclusionOfTodayInTheRequestedOffset(TestContainer engine) throws Exception {
        ZoneOffset offset = ZoneOffset.of("+05:30");
        LocalDate day = LocalDate.now(offset);

        reset(modes(null));
        ObjectNode report = options(day).put("utc_offset", offset.getId());
        Assertions.assertTrue(succeed(engine, report).isEmpty());
        Assertions.assertEquals(0, calls().size());

        reset(modes(null));
        List<String[]> rows = succeed(engine, report.put("until_today", true));
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals(day.toString(), rows.get(0)[0]);
        Assertions.assertEquals(1, calls().size());
        assertRequest(calls().path(0), day, "+05:30", "app,day", "installs");
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void rejectsInvalidSelectionBeforeHttp(TestContainer engine) throws Exception {
        reset(modes(null));
        LocalDate day = previousDay();
        ObjectNode overlap = options(day);
        overlap.putArray("additional_metrics").add("installs");
        fail(engine, overlap, "names must be unique");
        fail(
                engine,
                options(day).put("utc_offset", "America/New_York"),
                "utc_offset must be a fixed");
        fail(engine, options(day).put("start_date", "2026-02-29"), "start_date must be a valid");
        fail(engine, options(day.minusYears(12)), "at most 3660 report days");
        fail(
                engine,
                options(day).put("allow_insecure_http", false),
                "base_url must be an HTTPS origin");
        ObjectNode unknownScale = options(day);
        unknownScale.putObject("metric_scales").put("missing_metric", "8");
        fail(engine, unknownScale, "metric_scales must reference selected metrics");
        Assertions.assertEquals(0, calls().size());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void rejectsPartialAndNonadvancingDailyReportsAtomically(TestContainer engine)
            throws Exception {
        String[][] scenarios = {
            {"pagination", "unsupported pagination"},
            {"warning", "report contains warnings"},
            {"shape", "rows must be an array"},
            {"malformed", "invalid JSON report"},
            {"stale", "row day does not match"},
            {"repeated", "repeats a dimension group"},
            {"precision", "does not fit the configured decimal"},
            {"exponent", "does not fit the configured decimal"},
            {"huge", "report body exceeds 8 MiB"}
        };
        for (String[] scenario : scenarios) {
            reset(modes(scenario[0]));
            String output = fail(engine, options(previousDay()), scenario[1]);
            Assertions.assertTrue(records(output).isEmpty(), scenario[0]);
            Assertions.assertEquals(1, calls().size(), scenario[0]);
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void retriesOnlyBoundedStatusesAndHonorsBothRetryAfterForms(TestContainer engine)
            throws Exception {
        ObjectNode script = modes(null);
        script.putArray("sequence").add("429").add("date_retry").add("gzip");
        reset(script);
        LocalDate day = previousDay();
        Assertions.assertEquals(1, succeed(engine, options(day)).size());
        JsonNode calls = calls();
        Assertions.assertEquals(3, calls.size());
        for (JsonNode call : calls) {
            assertRequest(call, day, "+00:00", "app,day", "installs");
        }
        Assertions.assertTrue(
                calls.path(1).path("started").asLong()
                        >= calls.path(0).path("started").asLong() + 1000);
        Assertions.assertTrue(
                calls.path(2).path("started").asLong() + 10 >= calls.path(1).path("due").asLong());

        reset(modes("503"));
        fail(
                engine,
                options(day).put("max_attempts", 2),
                "HTTP 503 exhausted the configured attempts");
        Assertions.assertEquals(2, calls().size());

        reset(modes("excessive_retry"));
        fail(
                engine,
                options(day).put("request_timeout_ms", 2500),
                "retry delay exceeds the report day deadline");
        Assertions.assertEquals(1, calls().size());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void rejectsBadAuthRedirectsAndAnUntrustedPeer(TestContainer engine) throws Exception {
        LocalDate day = previousDay();
        reset(modes(null));
        fail(engine, options(day).put("token", TEST_TOKEN + "-invalid"), "unexpected HTTP 401");
        Assertions.assertEquals(1, calls().size());
        Assertions.assertFalse(calls().path(0).path("bearer").asBoolean());

        for (String mode : Arrays.asList("403", "redirect")) {
            reset(modes(mode));
            fail(engine, options(day), "unexpected HTTP " + ("403".equals(mode) ? "403" : "307"));
            Assertions.assertEquals(1, calls().size());
        }

        reset(modes(null));
        fail(
                engine,
                options(day)
                        .put("base_url", "https://" + HOST + ":8443")
                        .put("allow_insecure_http", false),
                "TLS transport failed");
        Assertions.assertEquals(0, calls().size());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void disconnectsAtTheTotalDeadlineEvenWhenBytesContinue(TestContainer engine) throws Exception {
        for (String mode : Arrays.asList("hold", "drip")) {
            reset(modes(mode));
            String output =
                    fail(
                            engine,
                            options(previousDay()).put("request_timeout_ms", 1500),
                            "report day deadline exceeded");
            Assertions.assertTrue(records(output).isEmpty());
            Awaitility.await()
                    .atMost(8, TimeUnit.SECONDS)
                    .until(() -> calls().path(0).path("closed_after").isNumber());
            Assertions.assertEquals(1, calls().size());
            Assertions.assertTrue(calls().path(0).path("closed_after").asLong() < 5000);
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void cancelsTheActiveReportBeforeItsDeadline(TestContainer engine) throws Exception {
        reset(modes("hold"));
        Path file = jobFile(options(previousDay()).put("request_timeout_ms", 120000), false, false);
        String id = newId();
        ExecutorService submit = Executors.newSingleThreadExecutor();
        try {
            Future<Container.ExecResult> work =
                    submit.submit(() -> engine.executeJob(conf(file), id));
            Awaitility.await()
                    .atMost(90, TimeUnit.SECONDS)
                    .until(() -> calls().path(0).path("held").asBoolean());
            good(engine.cancelJob(id));
            awaitStatus(engine, id, "CANCELED");
            work.get(60, TimeUnit.SECONDS);
            Awaitility.await()
                    .atMost(12, TimeUnit.SECONDS)
                    .until(() -> calls().path(0).path("closed_after").isNumber());
            Assertions.assertEquals(1, calls().size());
            Assertions.assertTrue(calls().path(0).path("closed_after").asLong() < 120000);
        } finally {
            try {
                release();
                cancelIfRunning(engine, id);
            } finally {
                submit.shutdownNow();
                Files.deleteIfExists(file);
            }
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void restoresAnInFlightDayAndRejectsChangedReportParameters(TestContainer engine)
            throws Exception {
        LocalDate start = previousDay().minusDays(2);
        ObjectNode controls = modes(null);
        controls.putObject("days").put(start.plusDays(1).toString(), "hold");
        reset(controls);
        ObjectNode report = options(start).put("request_timeout_ms", 120000);
        Path file = jobFile(report, true, false);
        Path changed = jobFile(report.deepCopy().put("utc_offset", "+02:00"), true, false);
        String old = newId();
        String restored = newId();
        String rejected = newId();
        ExecutorService submit = Executors.newSingleThreadExecutor();
        try {
            String before = engine.getServerLogs();
            Future<Container.ExecResult> work =
                    submit.submit(() -> engine.executeJob(conf(file), old));
            Awaitility.await()
                    .atMost(90, TimeUnit.SECONDS)
                    .until(() -> calls().path(1).path("held").asBoolean());
            Awaitility.await()
                    .atMost(20, TimeUnit.SECONDS)
                    .until(() -> fresh(engine, before).contains(" : " + start + ", "));
            nextCheckpoint(engine, old);
            good(engine.savepointJob(old));
            awaitStatus(engine, old, "SAVEPOINT_DONE");
            good(work.get(60, TimeUnit.SECONDS));
            Assertions.assertEquals(2, calls().size());
            release();

            String mismatch = engine.getServerLogs();
            Container.ExecResult invalid =
                    engine.restoreJobWithCheckpoint(conf(changed), old, rejected);
            Assertions.assertNotEquals(0, invalid.getExitCode());
            Assertions.assertTrue(
                    join(invalid, fresh(engine, mismatch))
                            .contains("checkpoint report parameters do not match"));
            Assertions.assertEquals(2, calls().size());

            String resume = engine.getServerLogs();
            Container.ExecResult result =
                    engine.restoreJobWithCheckpoint(conf(file), old, restored);
            good(result);
            List<String[]> rows = records(result, fresh(engine, resume));
            Assertions.assertEquals(2, rows.size());
            Assertions.assertEquals(start.plusDays(1).toString(), rows.get(0)[0]);
            Assertions.assertEquals(start.plusDays(2).toString(), rows.get(1)[0]);
            Assertions.assertEquals(4, calls().size());
            assertRequest(calls().path(2), start.plusDays(1), "+00:00", "app,day", "installs");
            assertRequest(calls().path(3), start.plusDays(2), "+00:00", "app,day", "installs");
        } finally {
            try {
                release();
                cancelIfRunning(engine, old);
                cancelIfRunning(engine, restored);
                cancelIfRunning(engine, rejected);
            } finally {
                submit.shutdownNow();
                Files.deleteIfExists(file);
                Files.deleteIfExists(changed);
            }
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void terminalRestoreDoesNotRequeryCompletedDays(TestContainer engine) throws Exception {
        reset(modes(null));
        LocalDate day = previousDay();
        Path file = jobFile(options(day), true, true);
        String old = newId();
        String restored = newId();
        ExecutorService submit = Executors.newSingleThreadExecutor();
        try {
            String before = engine.getServerLogs();
            Future<Container.ExecResult> initial =
                    submit.submit(() -> engine.executeJob(conf(file), old));
            Awaitility.await()
                    .atMost(90, TimeUnit.SECONDS)
                    .until(() -> fresh(engine, before).contains(" : " + day + ", "));
            nextCheckpoint(engine, old);
            good(engine.savepointJob(old));
            awaitStatus(engine, old, "SAVEPOINT_DONE");
            good(initial.get(60, TimeUnit.SECONDS));
            Assertions.assertEquals(1, calls().size());

            Future<Container.ExecResult> resumed =
                    submit.submit(() -> engine.restoreJobWithCheckpoint(conf(file), old, restored));
            awaitStatus(engine, restored, "RUNNING");
            nextCheckpoint(engine, restored);
            Assertions.assertEquals(1, calls().size());
            good(engine.cancelJob(restored));
            awaitStatus(engine, restored, "CANCELED");
            resumed.get(60, TimeUnit.SECONDS);
            Assertions.assertEquals(1, calls().size());
        } finally {
            try {
                cancelIfRunning(engine, old);
                cancelIfRunning(engine, restored);
            } finally {
                submit.shutdownNow();
                Files.deleteIfExists(file);
            }
        }
    }

    private static void assertRequest(
            JsonNode call, LocalDate day, String offset, String dimensions, String metrics) {
        Assertions.assertEquals("/reports-service/report", call.path("route").asText());
        Assertions.assertTrue(call.path("bearer").asBoolean());
        Assertions.assertEquals(day + ":" + day, call.at("/query/date_period/0").asText());
        Assertions.assertEquals(offset, call.at("/query/utc_offset/0").asText());
        Assertions.assertEquals(dimensions, call.at("/query/dimensions/0").asText());
        Assertions.assertEquals(metrics, call.at("/query/metrics/0").asText());
        Assertions.assertEquals("false", call.at("/query/format_dates/0").asText());
        Assertions.assertEquals(5, call.path("query").size());
    }

    private static void decimalEquals(String expected, String actual) {
        Assertions.assertEquals(
                0,
                new BigDecimal(expected).compareTo(new BigDecimal(actual)),
                () -> "Expected exact decimal " + expected + " but got " + actual);
    }

    private static LocalDate previousDay() {
        return LocalDate.now(ZoneOffset.UTC).minusDays(1);
    }

    private static ObjectNode options(LocalDate day) {
        ObjectNode opts =
                JSON.createObjectNode()
                        .put("start_date", day.toString())
                        .put("base_url", ORIGIN)
                        .put("allow_insecure_http", true)
                        .put("token", TEST_TOKEN);
        opts.putArray("dimensions").add("app");
        opts.putArray("metrics").add("installs");
        opts.putObject("metric_scales").put("installs", "0");
        return opts;
    }

    private static ObjectNode modes(String mode) {
        return JSON.createObjectNode().put("mode", mode);
    }

    private static List<String[]> succeed(TestContainer engine, ObjectNode options)
            throws Exception {
        String before = engine.getServerLogs();
        Container.ExecResult result = run(engine, options);
        good(result);
        return records(result, fresh(engine, before));
    }

    private static String fail(TestContainer engine, ObjectNode options, String expected)
            throws Exception {
        String before = engine.getServerLogs();
        Container.ExecResult result = run(engine, options);
        String after = fresh(engine, before);
        String logs = join(result, after);
        Assertions.assertNotEquals(0, result.getExitCode(), logs);
        Assertions.assertTrue(
                logs.contains(expected),
                () -> "Missing " + expected + " in job diagnostics\n" + logs);
        Assertions.assertFalse(logs.contains(TEST_TOKEN));
        Assertions.assertFalse(logs.contains(PRIVATE_DIAGNOSTIC));
        return logs;
    }

    private static List<String[]> records(Container.ExecResult result, String server) {
        List<String[]> fromServer = records(server);
        return fromServer.isEmpty() ? records(result.getStdout()) : fromServer;
    }

    private static List<String[]> records(String logs) {
        List<String[]> rows = new ArrayList<>();
        for (String line : logs.split("\\R")) {
            int kind = line.indexOf("SeaTunnelRow#kind=");
            int start = kind < 0 ? -1 : line.indexOf(" : ", kind);
            if (start >= 0) {
                String[] fields = line.substring(start + 3).split(", ", -1);
                if (fields[0].matches("[0-9]{4}-[0-9]{2}-[0-9]{2}")) {
                    rows.add(fields);
                }
            }
        }
        return rows;
    }

    private static String join(Container.ExecResult result, String server) {
        return result.getStdout() + "\n" + result.getStderr() + "\n" + server;
    }

    private static String fresh(TestContainer engine, String baseline) {
        String actual = engine.getServerLogs();
        return actual.startsWith(baseline) ? actual.substring(baseline.length()) : "";
    }

    private static void good(Container.ExecResult result) {
        Assertions.assertEquals(0, result.getExitCode(), () -> join(result, ""));
    }

    private static Container.ExecResult run(TestContainer engine, ObjectNode options)
            throws Exception {
        Path file = jobFile(options, false, false);
        try {
            return engine.executeJob(conf(file));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    private static Path jobFile(ObjectNode options, boolean streaming, boolean keepAlive)
            throws IOException {
        String env = streaming ? "STREAMING\n  checkpoint.interval = 1000" : "BATCH";
        String extra =
                keepAlive
                        ? "\n  FakeSource {\n"
                                + "    plugin_output = \"keepalive\"\n"
                                + "    schema = { fields { marker = string } }\n"
                                + "    row.num = 1\n"
                                + "    split.read-interval = 150\n"
                                + "    rows = [{kind = INSERT, fields = [\"keep-alive\"]}]\n"
                                + "  }"
                        : "";
        ObjectNode source = options.deepCopy();
        source.put("plugin_output", "reports");
        String inputs = keepAlive ? "[\"reports\", \"keepalive\"]" : "[\"reports\"]";
        String config =
                "env {\n  parallelism = 1\n  job.retry.times = 0\n  job.mode = "
                        + env
                        + "\n}\nsource {\n  Adjust "
                        + source
                        + extra
                        + "\n}\nsink {\n  Console { plugin_input = "
                        + inputs
                        + " }\n}\n";
        Path file =
                Files.createTempFile(
                        ContainerUtil.getCurrentModulePath().resolve("src/test/resources"),
                        "adjust-e2e-",
                        ".conf");
        Files.write(file, config.getBytes(StandardCharsets.UTF_8));
        return file;
    }

    private void reset(ObjectNode script) throws IOException {
        request("/control/reset", script.toString());
    }

    private void release() throws IOException {
        request("/control/release", "");
    }

    private JsonNode calls() throws IOException {
        return request("/control/state", null);
    }

    private JsonNode request(String path, String body) throws IOException {
        HttpURLConnection connection =
                (HttpURLConnection)
                        new URL("http", server.getHost(), server.getMappedPort(8080), path)
                                .openConnection();
        connection.setConnectTimeout(5000);
        connection.setReadTimeout(5000);
        try {
            if (body != null) {
                connection.setRequestMethod("POST");
                connection.setRequestProperty("Content-Type", "application/json");
                connection.setDoOutput(true);
                try (OutputStream data = connection.getOutputStream()) {
                    data.write(body.getBytes(StandardCharsets.UTF_8));
                }
                Assertions.assertEquals(204, connection.getResponseCode());
                return null;
            }
            Assertions.assertEquals(200, connection.getResponseCode());
            try (InputStream data = connection.getInputStream()) {
                return JSON.readTree(data);
            }
        } finally {
            connection.disconnect();
        }
    }

    private static String conf(Path file) {
        return "/" + file.getFileName();
    }

    private static String newId() {
        return Long.toString(JobIdGenerator.newJobId());
    }

    private static void nextCheckpoint(TestContainer engine, String jobId) {
        long baseline = engine.getCompletedCheckpointCount(jobId);
        Awaitility.await()
                .atMost(45, TimeUnit.SECONDS)
                .until(() -> engine.getCompletedCheckpointCount(jobId) > baseline);
    }

    private static void awaitStatus(TestContainer engine, String id, String expected) {
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .until(() -> expected.equals(engine.getJobStatus(id)));
    }

    private static void cancelIfRunning(TestContainer engine, String id) throws Exception {
        String status = engine.getJobStatus(id);
        if ("SCHEDULED".equals(status) || "RUNNING".equals(status)) {
            engine.cancelJob(id);
        }
    }

    /** A short-lived test-only key exercises the engine's default certificate validation. */
    private static void generateCertificate(Path directory) throws Exception {
        Path store = directory.resolve("report.p12");
        Path keytool = Paths.get(System.getProperty("java.home"), "bin", "keytool");
        Process command =
                new ProcessBuilder(
                                keytool.toString(),
                                "-genkeypair",
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                store.toString(),
                                "-storepass",
                                "fixture-pass",
                                "-alias",
                                "adjust-fixture",
                                "-keyalg",
                                "RSA",
                                "-keysize",
                                "2048",
                                "-dname",
                                "CN=" + HOST,
                                "-ext",
                                "SAN=dns:" + HOST,
                                "-validity",
                                "2",
                                "-noprompt")
                        .inheritIO()
                        .start();
        if (!command.waitFor(30, TimeUnit.SECONDS)) {
            command.destroyForcibly();
            throw new IOException("Timed out creating HTTPS fixture certificate");
        }
        Assertions.assertEquals(0, command.exitValue());
        KeyStore material = KeyStore.getInstance("PKCS12");
        char[] password = "fixture-pass".toCharArray();
        try (InputStream in = Files.newInputStream(store)) {
            material.load(in, password);
        }
        writePem(
                directory.resolve("certificate.pem"),
                "CERTIFICATE",
                material.getCertificate("adjust-fixture").getEncoded());
        writePem(
                directory.resolve("certificate.key"),
                "PRIVATE KEY",
                material.getKey("adjust-fixture", password).getEncoded());
    }

    private static void writePem(Path file, String type, byte[] encoded) throws IOException {
        String base64 = Base64.getMimeEncoder(64, new byte[] {'\n'}).encodeToString(encoded);
        Files.write(
                file,
                ("-----BEGIN " + type + "-----\n" + base64 + "\n-----END " + type + "-----\n")
                        .getBytes(StandardCharsets.US_ASCII));
    }
}
