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

package org.apache.seatunnel.e2e.connector.bamboohr;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.seatunnel.SeaTunnelContainer;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the shipped BambooHR artifact in Java 11 Zeta with a separate HTTPS peer and
 * transactional LocalFile output.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BambooHRIT extends TestSuiteBase {
    private static final String CONF = "/bamboohr.conf";
    private static final String ROOT = "/tmp/bamboohr-https-fixture";
    private static final String OUTPUT = "/tmp/seatunnel_mnt/bamboohr-";
    private static final String META = "meta_fields_stream";
    private static final String DIRECTORY = "employees_directory_stream";
    private static final String TIME_OFF = "time_off_requests_stream";
    private static final String TIMESHEETS = "timesheet_entries";
    private static final String EMPLOYEES = "employees";
    private static final String METADATA_PATH = "/api/v1/meta/fields";
    private static final String DIRECTORY_PATH = "/api/v1/employees/directory";
    private static final ObjectMapper JSON =
            new ObjectMapper()
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    private static final List<String> TRUSTED = List.of(BambooHRPeer.MAIN, BambooHRPeer.OTHER);

    @TempDir static Path directory;
    private final ExecutorService tasks = Executors.newCachedThreadPool();
    private Engine engine;
    private Future<Container.ExecResult> peer;
    private String active;

    @BeforeAll
    void startFixture() throws Exception {
        KeyStore identity = identity("trusted", TRUSTED);
        identity("untrusted", List.of(BambooHRPeer.UNTRUSTED));
        char[] password = BambooHRPeer.PASSWORD.toCharArray();
        KeyStore trust = KeyStore.getInstance("JKS");
        trust.load(null, password);
        trust.setCertificateEntry("bamboohr-fixture", identity.getCertificate("fixture"));
        try (OutputStream stream = Files.newOutputStream(directory.resolve("trust.jks"))) {
            trust.store(stream, password);
        }
        engine = new Engine(directory);
        engine.startUp();
        peer = tasks.submit(engine::runPeer);
        Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> engine.exists("ready"));
    }

    @AfterEach
    void verifyProviderAssertions() throws Exception {
        if (active != null) {
            assertFalse(engine.exists(active + ".invalid"), engine.contents(active + ".invalid"));
        }
    }

    @AfterAll
    void stopFixture() throws Exception {
        try {
            if (engine != null) {
                try {
                    if (peer != null) {
                        engine.touch("stop");
                        success(peer.get(15, TimeUnit.SECONDS));
                    }
                } finally {
                    engine.tearDown();
                }
            }
        } finally {
            tasks.shutdownNow();
        }
    }

    @Test
    void customReportSendsAnExactReadOnlyPostAndKeepsReturnedValues() throws Exception {
        Settings options =
                new Settings("report", "custom_reports_stream")
                        .set("reportFields", "firstName,1349,customStartDate");
        ObjectNode request =
                object(
                        "{\"title\":\"Apache SeaTunnel employee extract\","
                                + "\"fields\":[\"firstName\",\"1349\",\"customStartDate\"]}");
        String response =
                "{\"title\":\"test\",\"fields\":[{\"id\":\"firstName\"}],\"employees\":["
                        + "{\"id\":\"1\",\"firstName\":\"测试\",\"1349\":90071992547409931234567,"
                        + "\"customStartDate\":null},{\"id\":\"2\",\"firstName\":\"Zoë\","
                        + "\"active\":false,\"rate\":1234567.12345678900}]}";
        ObjectNode busy =
                get("/api/v1/reports/custom", 429, "", "{\"error\":\"throttle\"}")
                        .put("method", "POST")
                        .put("retryAfter", "1");
        busy.set("body", request);
        ObjectNode ready = get("/api/v1/reports/custom", 200, "", response).put("method", "POST");
        ready.set("body", request);
        program(options, busy, ready);
        success(run(options));
        assertRows(options, JSON.readTree(response).path("employees"));
        List<JsonNode> calls = requests(options);
        assertEquals(2, calls.size());
        assertEquals(request, calls.get(0).path("body"));
        assertEquals(request, calls.get(1).path("body"));
        assertTrue(
                calls.get(1).path("nanoTime").asLong() - calls.get(0).path("nanoTime").asLong()
                        >= TimeUnit.MILLISECONDS.toNanos(950));
    }

    @Test
    void directoryAndMetadataKeepTheirIndependentPayloadsAndPublishedFields() throws Exception {
        Settings directory = new Settings("directory", DIRECTORY);
        String published =
                "{\"fields\":[{\"id\":\"firstName\"}],\"employees\":["
                        + "{\"id\":\"7\",\"firstName\":\"Mika\",\"workEmail\":null},"
                        + "{\"id\":\"8\",\"firstName\":\"Jules\"}]}";
        program(directory, get(DIRECTORY_PATH, published));
        success(run(directory));
        assertRows(directory, JSON.readTree(published).path("employees"));
        assertEquals(1, requests(directory).size());

        Settings fields = new Settings("fields", META);
        String data =
                "[{\"id\":1349,\"name\":\"数据字段\",\"alias\":\"customStartDate\","
                        + "\"deprecated\":false,\"nested\":{\"count\":9007199254740993,\"blank\":null}}]";
        program(fields, get(METADATA_PATH, 200, "gzip", data));
        success(run(fields));
        assertRows(fields, JSON.readTree(data));
        assertEquals(1, requests(fields).size());

        Settings empty = new Settings("empty-directory", DIRECTORY);
        program(empty, get(DIRECTORY_PATH, "{\"fields\":[],\"employees\":[]}"));
        success(run(empty));
        assertTrue(rows(empty).isEmpty());
        assertEquals(1, requests(empty).size());
    }

    @Test
    void employeeFanoutUsesInternalIdsAndPreservesMissingAndNullFields() throws Exception {
        Settings options =
                new Settings("employee-detail", EMPLOYEES)
                        .set("employeeFields", "firstName,1349,customStartDate");
        String two =
                "{\"id\":\"2\",\"firstName\":null,\"customStartDate\":\"2024-02-29\","
                        + "\"nested\":{\"salary\":9007199254740993.12345678}}";
        String nine = "{\"id\":\"9\",\"firstName\":\"Ana\"}";
        program(
                options,
                get(DIRECTORY_PATH, "{\"employees\":[{\"id\":\"9\"},{\"id\":\"2\"}]}"),
                get("/api/v1/employees/2?fields=firstName%2C1349%2CcustomStartDate", two),
                get("/api/v1/employees/9?fields=firstName%2C1349%2CcustomStartDate", nine));
        success(run(options));
        assertRows(options, JSON.readTree("[" + two + "," + nine + "]"));
        assertEquals(3, requests(options).size());
    }

    @Test
    void employeeChangesUseTheDocumentedMapAndEmitDeletionMetadata() throws Exception {
        Instant since = Instant.now().minusSeconds(120).truncatedTo(ChronoUnit.SECONDS);
        Settings options =
                new Settings("employee-changes", EMPLOYEES)
                        .set("employeeChangedSince", since.toString());
        ObjectNode response =
                JSON.createObjectNode().put("latest", since.plusSeconds(1).toString());
        ObjectNode changes = response.putObject("employees");
        change(changes, "5", "Updated", since);
        change(changes, "2", "Inserted", since);
        change(changes, "9", "Deleted", since.plusSeconds(1));
        change(changes, "1", "Updated", since.minusSeconds(1));
        change(changes, "7", "Updated", since.plus(1, ChronoUnit.DAYS));
        program(
                options,
                get(
                        "/api/v1/employees/changed?since="
                                + since.minusSeconds(1).toString().replace(":", "%3A"),
                        response.toString()),
                get(
                        "/api/v1/employees/2?fields=firstName%2ClastName",
                        "{\"id\":\"2\",\"firstName\":\"Ada\"}"),
                get(
                        "/api/v1/employees/5?fields=firstName%2ClastName",
                        "{\"id\":\"5\",\"firstName\":\"Lin\"}"));
        success(run(options));
        List<JsonNode> result = rows(options);
        assertEquals(3, result.size());
        List<String> actual = new ArrayList<>();
        for (JsonNode row : result) {
            JsonNode data = JSON.readTree(row.path("data").asText());
            actual.add(
                    data.toString()
                            + "|"
                            + row.path("change_type").asText()
                            + "|"
                            + row.path("last_changed").asText());
        }
        Collections.sort(actual);
        List<String> expected =
                new ArrayList<>(
                        List.of(
                                "{\"id\":\"2\",\"firstName\":\"Ada\"}|Inserted|" + since,
                                "{\"id\":\"5\",\"firstName\":\"Lin\"}|Updated|" + since,
                                "{\"id\":\"9\"}|Deleted|" + since.plusSeconds(1)));
        Collections.sort(expected);
        assertEquals(expected, actual);
        assertEquals(3, requests(options).size());
    }

    @Test
    void timeOffUsesOverlapAndCreationFiltersAndRetainsSameDateRecords() throws Exception {
        LocalDate end = LocalDate.now(ZoneOffset.UTC).minusDays(2);
        LocalDate watermark = end.minusDays(4);
        LocalDate first = watermark.minusDays(365);
        Settings options =
                new Settings("time-off", TIME_OFF)
                        .set("start", first.minusDays(30))
                        .set("end", end)
                        .set("timeOffWatermark", watermark);
        String in =
                "{\"id\":\"1\",\"created\":\""
                        + first
                        + "\",\"start\":\""
                        + first.minusDays(1)
                        + "\",\"end\":\""
                        + first
                        + "\",\"amount\":{\"amount\":4.2500},\"notes\":null}";
        String same =
                "{\"id\":\"2\",\"created\":\""
                        + first
                        + "\",\"dates\":{\""
                        + first
                        + "\":2.125},\"amount\":null}";
        String noCursor = "{\"id\":\"3\",\"created\":null,\"notes\":{\"employee\":\"假期\"}}";
        String outside = "{\"id\":\"0\",\"created\":\"" + first.minusDays(1) + "\"}";
        String future = "{\"id\":\"4\",\"created\":\"" + end.plusDays(1) + "\"}";
        program(
                options,
                get(
                        range(TIME_OFF, first, end),
                        "[" + outside + "," + in + "," + same + "," + noCursor + "," + future
                                + "]"));
        success(run(options));
        assertRows(options, JSON.readTree("[" + in + "," + same + "," + noCursor + "]"));
        assertEquals(1, requests(options).size());
    }

    @Test
    void timesheetsRespectMonthBoundariesAndKeepHourRowsWithNullTimes() throws Exception {
        LocalDate lastYear = LocalDate.now(ZoneOffset.UTC).minusMonths(2);
        LocalDate first = lastYear.withDayOfMonth(lastYear.lengthOfMonth());
        LocalDate next = first.plusDays(1);
        LocalDate end = next.plusDays(1);
        Settings options =
                new Settings("timesheets", TIMESHEETS)
                        .set("zone", "America/Los_Angeles")
                        .set("start", first + "T12:00:00-08:00")
                        .set("end", end);
        String hour =
                "{\"id\":9007199254740993,\"date\":\""
                        + first
                        + "\","
                        + "\"type\":\"hour\",\"start\":null,\"end\":null,\"hours\":3.2500}";
        String clock =
                "{\"id\":24,\"date\":\""
                        + next
                        + "\",\"type\":\"clock\",\"start\":\""
                        + next
                        + "T23:30:00-07:00\",\"hours\":0.500,\"projectInfo\":{\"task\":null}}";
        String legacy = "{\"id\":25,\"date\":null,\"start\":null,\"hours\":1.5}";
        String legacyOffset =
                "{\"id\":27,\"start\":\"" + next.plusDays(1) + "T05:00:00Z\",\"hours\":2.5}";
        String wrongWindow = "{\"id\":26,\"date\":\"" + next + "\"}";
        program(
                options,
                get(range(TIMESHEETS, first, first), "[" + hour + "," + wrongWindow + "]"),
                get(
                        range(TIMESHEETS, next, end),
                        "[" + clock + "," + legacy + "," + legacyOffset + "]"));
        success(run(options));
        assertRows(
                options,
                JSON.readTree("[" + hour + "," + clock + "," + legacy + "," + legacyOffset + "]"));
        assertEquals(2, requests(options).size());
    }

    @Test
    void timesheetLookbackStartsAtTheFirstRetainedDate() throws Exception {
        LocalDate floor = LocalDate.now(ZoneOffset.UTC).minusDays(364);
        LocalDate end = floor.plusDays(1);
        Settings options =
                new Settings("retention", TIMESHEETS)
                        .set("start", floor.minusYears(2))
                        .set("end", end)
                        .set("timesheetWatermark", end);
        String data = "{\"id\":41,\"date\":\"" + end + "\",\"start\":null,\"hours\":7}";
        if (floor.getMonth() == end.getMonth()) {
            program(options, get(range(TIMESHEETS, floor, end), "[" + data + "]"));
        } else {
            program(
                    options,
                    get(range(TIMESHEETS, floor, floor), "[]"),
                    get(range(TIMESHEETS, end, end), "[" + data + "]"));
        }
        success(run(options));
        assertRows(options, JSON.readTree("[" + data + "]"));
        assertEquals(floor.getMonth() == end.getMonth() ? 1 : 2, requests(options).size());
    }

    @Test
    void employeeAccessAndWrongIdsFailInsteadOfBorrowingDirectoryValues() throws Exception {
        for (int status : new int[] {403, 404}) {
            Settings options = new Settings("employee-status-" + status, EMPLOYEES);
            String directory =
                    "{\"employees\":[{\"id\":\"1\",\"firstName\":\"Do not synthesize\"}]}";
            program(
                    options,
                    get(DIRECTORY_PATH, directory),
                    get(
                            "/api/v1/employees/1?fields=firstName%2ClastName",
                            status, "", "{\"detail\":\"" + BambooHRPeer.PRIVATE + "\"}"));
            failure(run(options), "HTTP " + status);
            assertEquals(2, requests(options).size());
            assertTrue(rows(options).isEmpty());
        }
        Settings wrongId = new Settings("employee-wrong-id", EMPLOYEES);
        program(
                wrongId,
                get(DIRECTORY_PATH, "{\"employees\":[{\"id\":\"3\"}]}"),
                get("/api/v1/employees/3?fields=firstName%2ClastName", "{\"id\":\"4\"}"));
        failure(run(wrongId), "does not match");
        assertTrue(rows(wrongId).isEmpty());
    }

    @Test
    void tlsHostnameAndRedirectFailuresDoNotForwardTheCredential() throws Exception {
        Settings redirected = new Settings("redirect", META);
        program(redirected, get(METADATA_PATH, 302, "", "redirect"));
        failure(run(redirected), "HTTP 302");
        assertEquals(1, requests(redirected).size());

        for (String host : List.of(BambooHRPeer.UNTRUSTED, BambooHRPeer.WRONG)) {
            String label = host.substring(0, host.indexOf('.'));
            Settings options = new Settings(label, META).set("company", label);
            program(options);
            failure(run(options), "HTTPS or TLS exchange failed");
            assertTrue(requests(options).isEmpty());
        }
    }

    @Test
    void rejectsInvalidBodiesAndExcessiveResponsesBeforeWritingRows() throws Exception {
        Map<String, String> malformed = new LinkedHashMap<>();
        malformed.put("duplicate-fields", "[{\"id\":1,\"id\":2}]");
        malformed.put("wrong-shape", "{\"fields\":[{\"id\":1}]}");
        malformed.put("trailing-json", "[{\"id\":1}] {}");
        for (Map.Entry<String, String> input : malformed.entrySet()) {
            Settings options = new Settings(input.getKey(), META);
            program(options, get(METADATA_PATH, input.getValue()));
            failure(run(options), "response");
            assertTrue(rows(options).isEmpty());
        }

        Settings records = new Settings("record-bound", META).set("records", 1);
        program(records, get(METADATA_PATH, "[{\"id\":1},{\"id\":2}]"));
        failure(run(records), "max_records_per_response");
        assertTrue(rows(records).isEmpty());

        for (String mode : List.of("", "gzip")) {
            Settings bytes =
                    new Settings(mode.isEmpty() ? "wire-bound" : "gzip-bound", META)
                            .set("bytes", 1024);
            program(
                    bytes,
                    get(
                            METADATA_PATH,
                            200,
                            mode,
                            "[{\"private\":\"" + BambooHRPeer.PRIVATE.repeat(120) + "\"}]"));
            failure(run(bytes), "max_response_bytes");
            assertTrue(rows(bytes).isEmpty());
        }
    }

    @Test
    void retriesAreFiniteAndRespectTheRemainingTotalBudget() throws Exception {
        Settings denied = new Settings("auth-denied", META);
        program(denied, get(METADATA_PATH, 401, "", BambooHRPeer.PRIVATE));
        failure(run(denied), "HTTP 401");
        assertEquals(1, requests(denied).size());

        Settings finite = new Settings("attempt-bound", META).set("attempts", 2);
        ObjectNode throttle =
                get(METADATA_PATH, 429, "", BambooHRPeer.PRIVATE).put("retryAfter", "0");
        ObjectNode unavailable = get(METADATA_PATH, 503, "", BambooHRPeer.PRIVATE);
        program(finite, throttle, unavailable);
        failure(run(finite), "retry limit reached after HTTP 503");
        assertEquals(2, requests(finite).size());

        Settings budget = new Settings("backoff-budget", META).set("deadline", 15000);
        program(budget, get(METADATA_PATH, 429, "", BambooHRPeer.PRIVATE).put("retryAfter", "60"));
        failure(run(budget), "exceeds request deadline");
        assertEquals(1, requests(budget).size());
    }

    @Test
    void tricklingHttpsIsClosedOnTotalDeadlineAndCancellation() throws Exception {
        Settings deadline =
                new Settings("trickle-deadline", META)
                        .set("deadline", 4000)
                        .set("readTimeout", 15000);
        program(deadline, get(METADATA_PATH, 200, "trickle", ""));
        failure(run(deadline), "request deadline exceeded");
        awaitRequests(deadline, 1);
        awaitClosed(deadline);
        assertTrue(rows(deadline).isEmpty());

        Settings cancel = new Settings("trickle-cancel", META).set("deadline", 60000);
        program(cancel, get(METADATA_PATH, 200, "trickle", ""));
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(CONF, job, cancel.variables()));
        try {
            awaitRequests(cancel, 1);
            success(engine.cancelJob(job));
            running.get(30, TimeUnit.SECONDS);
            awaitClosed(cancel);
            assertTrue(rows(cancel).isEmpty());
        } finally {
            if (!running.isDone()) {
                engine.cancelJob(job);
                running.cancel(true);
            }
        }
    }

    @Test
    void savepointResumesTheSecondEmployeeWithoutRepeatingTheFirst() throws Exception {
        Settings options = new Settings("employee-savepoint", EMPLOYEES).set("deadline", 120000);
        String parents = "{\"employees\":[{\"id\":\"10\"},{\"id\":\"2\"}]}";
        String reversed = "{\"employees\":[{\"id\":\"2\"},{\"id\":\"10\"}]}";
        String initial = "{\"id\":\"10\",\"firstName\":\"first\"}";
        String later = "{\"id\":\"2\",\"firstName\":\"second\"}";
        String ten = "/api/v1/employees/10?fields=firstName%2ClastName";
        String two = "/api/v1/employees/2?fields=firstName%2ClastName";
        program(
                options,
                get(DIRECTORY_PATH, parents),
                get(ten, initial),
                get(two, 200, "hold", later),
                get(DIRECTORY_PATH, reversed),
                get(two, later));
        resume(options, 3);
        assertEquals(5, requests(options).size());
        assertRows(options, JSON.readTree("[" + initial + "," + later + "]"));
    }

    @Test
    void savepointResumesFrozenSecondMonthAndDoesNotReapplyLookback() throws Exception {
        LocalDate month = LocalDate.now(ZoneOffset.UTC).minusMonths(2);
        LocalDate first = month.withDayOfMonth(month.lengthOfMonth());
        LocalDate second = first.plusDays(1);
        Settings options =
                new Settings("month-savepoint", TIMESHEETS)
                        .set("start", first)
                        .set("end", second)
                        .set("timesheetWatermark", second)
                        .set("deadline", 120000);
        String before = "{\"id\":1,\"date\":\"" + first + "\",\"hours\":2.5,\"start\":null}";
        String after = "{\"id\":2,\"date\":\"" + second + "\",\"hours\":5.5,\"start\":null}";
        String left = range(TIMESHEETS, first, first);
        String right = range(TIMESHEETS, second, second);
        program(
                options,
                get(left, "[" + before + "]"),
                get(right, 200, "hold", "[" + after + "]"),
                get(right, "[" + after + "]"));
        resume(options, 2);
        assertEquals(3, requests(options).size());
        assertRows(options, JSON.readTree("[" + before + "," + after + "]"));
    }

    @Test
    void invalidConfigAndParallelismFailBeforeConnecting() throws Exception {
        Settings host = new Settings("bad-company", META).set("company", "user@host:443");
        program(host);
        failure(run(host), "subdomain DNS label");
        assertTrue(requests(host).isEmpty());

        Settings fields =
                new Settings("bad-field", "custom_reports_stream")
                        .set("reportFields", "firstName,,lastName");
        program(fields);
        failure(run(fields), "invalid field identifier");
        assertTrue(requests(fields).isEmpty());

        Settings parallel = new Settings("bad-parallelism", META).set("parallelism", 2);
        program(parallel);
        failure(run(parallel), "source parallelism must be 1");
        assertTrue(requests(parallel).isEmpty());
    }

    private void resume(Settings options, int countBeforeRestore) throws Exception {
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(CONF, job, options.variables()));
        try {
            awaitRequests(options, countBeforeRestore);
            long prior = engine.getCompletedCheckpointCount(job);
            Awaitility.await()
                    .atMost(35, TimeUnit.SECONDS)
                    .until(() -> engine.getCompletedCheckpointCount(job) > prior);
            success(engine.savepointJob(job));
            running.get(30, TimeUnit.SECONDS);
            engine.touch(options.name + ".release");
            success(engine.restoreJob(CONF, job, options.variables()));
        } finally {
            if (!running.isDone()) {
                engine.cancelJob(job);
                running.cancel(true);
            }
        }
    }

    private void program(Settings options, ObjectNode... steps) throws Exception {
        verifyProviderAssertions();
        Path input = Files.createTempFile(directory, "bamboohr-steps-", ".json");
        ArrayNode data = JSON.createArrayNode().addAll(Arrays.asList(steps));
        Files.writeString(input, data.toString());
        engine.install(input, options.name + ".program");
        Path selection = Files.createTempFile(directory, "bamboohr-active-", ".txt");
        Files.writeString(selection, options.name);
        engine.install(selection, "active");
        active = options.name;
    }

    private Container.ExecResult run(Settings options) throws Exception {
        return engine.executeJob(CONF, jobId(), options.variables());
    }

    private List<JsonNode> requests(Settings options) throws Exception {
        return lines(engine.contents(options.name + ".requests"));
    }

    private List<JsonNode> rows(Settings options) throws Exception {
        return lines(engine.rows(options.name));
    }

    private void assertRows(Settings options, JsonNode expected) throws Exception {
        List<String> actual = new ArrayList<>();
        for (JsonNode row : rows(options)) {
            assertTrue(row.path("change_type").isMissingNode() || row.path("change_type").isNull());
            assertTrue(
                    row.path("last_changed").isMissingNode() || row.path("last_changed").isNull());
            actual.add(JSON.readTree(row.path("data").asText()).toString());
        }
        List<String> want = new ArrayList<>();
        expected.forEach(row -> want.add(row.toString()));
        Collections.sort(actual);
        Collections.sort(want);
        assertEquals(want, actual);
    }

    private void awaitRequests(Settings options, int count) {
        Awaitility.await()
                .atMost(35, TimeUnit.SECONDS)
                .until(() -> requests(options).size() == count);
    }

    private void awaitClosed(Settings options) {
        Awaitility.await()
                .atMost(20, TimeUnit.SECONDS)
                .until(() -> engine.exists(options.name + ".disconnect"));
    }

    private static List<JsonNode> lines(String text) throws IOException {
        List<JsonNode> result = new ArrayList<>();
        for (String line : text.split("\\r?\\n")) {
            if (!line.isBlank()) {
                result.add(JSON.readTree(line));
            }
        }
        return result;
    }

    private static ObjectNode object(String value) throws IOException {
        return (ObjectNode) JSON.readTree(value);
    }

    private static ObjectNode get(String target, String response) {
        return get(target, 200, "", response);
    }

    private static ObjectNode get(String target, int status, String mode, String response) {
        return JSON.createObjectNode()
                .put("method", "GET")
                .put("target", target)
                .put("status", status)
                .put("mode", mode)
                .put("response", response);
    }

    private static void change(ObjectNode changes, String id, String action, Instant time) {
        changes.putObject(id)
                .put("id", id)
                .put("action", action)
                .put("lastChanged", time.toString());
    }

    private static String range(String resource, LocalDate first, LocalDate last) {
        String root =
                TIME_OFF.equals(resource)
                        ? "/api/v1/time_off/requests"
                        : "/api/v1/time_tracking/timesheet_entries";
        return root + "?start=" + first + "&end=" + last;
    }

    private static String jobId() {
        return Long.toString(
                ThreadLocalRandom.current().nextLong(1_000_000_000_000L, 9_000_000_000_000L));
    }

    private static String output(Container.ExecResult result) {
        return result.getStdout() + result.getStderr();
    }

    private static void success(Container.ExecResult result) {
        assertEquals(0, result.getExitCode(), () -> output(result));
    }

    private static void failure(Container.ExecResult result, String clue) {
        String log = output(result);
        assertNotEquals(0, result.getExitCode(), log);
        assertTrue(log.contains(clue), log);
        assertFalse(log.contains(BambooHRPeer.PRIVATE), "log contains the provider error payload");
        assertFalse(log.contains(BambooHRPeer.KEY), "log contains the provider credential");
        assertFalse(
                log.contains(
                        Base64.getEncoder()
                                .encodeToString(
                                        (BambooHRPeer.KEY + ":x")
                                                .getBytes(StandardCharsets.US_ASCII))),
                "log contains the provider authorization encoding");
    }

    /**
     * Generates fixture-only trust material with each expected hostname in the certificate.
     *
     * <p>The untrusted and hostname-mismatch cases use separate names from the valid tenant.
     */
    private static KeyStore identity(String name, List<String> hosts) throws Exception {
        String keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
        Path target = directory.resolve(name + ".p12");
        String alt = hosts.stream().map(host -> "dns:" + host).collect(Collectors.joining(","));
        Process process =
                new ProcessBuilder(
                                keytool,
                                "-genkeypair",
                                "-alias",
                                "fixture",
                                "-keyalg",
                                "RSA",
                                "-keysize",
                                "2048",
                                "-validity",
                                "7",
                                "-dname",
                                "CN=" + hosts.get(0),
                                "-ext",
                                "SAN=" + alt,
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                target.toString(),
                                "-storepass",
                                BambooHRPeer.PASSWORD,
                                "-keypass",
                                BambooHRPeer.PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String log = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, process.waitFor(), log);
        KeyStore key = KeyStore.getInstance("PKCS12");
        try (InputStream stream = Files.newInputStream(target)) {
            key.load(stream, BambooHRPeer.PASSWORD.toCharArray());
        }
        return key;
    }

    /**
     * Passes per-case source variables into the ordinary Zeta CLI entry point.
     *
     * <p>Optional variables are omitted to exercise the connector's normal defaults.
     */
    private static final class Settings {
        final String name;
        private final Map<String, Object> values = new LinkedHashMap<>();

        Settings(String name, String resource) {
            this.name = name;
            set("company", "tenant-fixture");
            set("token", BambooHRPeer.KEY);
            set("resource", resource);
            set("parallelism", 1);
            set("output", OUTPUT + name);
        }

        Settings set(String field, Object value) {
            values.put(field, value);
            return this;
        }

        String[] variables() {
            return values.entrySet().stream()
                    .map(
                            entry -> {
                                Object value = entry.getValue();
                                String config =
                                        value instanceof Number
                                                ? value.toString()
                                                : JSON.getNodeFactory()
                                                        .textNode(value.toString())
                                                        .toString();
                                return "bamboo." + entry.getKey() + "=" + config;
                            })
                    .toArray(String[]::new);
        }
    }

    /**
     * Installs the independent fixture in Zeta's environment and maps all test tenant domains
     * locally. The production connector continues to enforce its normal hostname and trust rules.
     */
    private static final class Engine extends SeaTunnelContainer {
        private final Path fixtures;

        Engine(Path fixtures) {
            this.fixtures = fixtures;
        }

        @Override
        protected String getJavaToolOptions() {
            return "-Djavax.net.ssl.trustStore="
                    + ROOT
                    + "/trust.jks -Djavax.net.ssl.trustStoreType=JKS"
                    + " -Djavax.net.ssl.trustStorePassword="
                    + BambooHRPeer.PASSWORD;
        }

        @Override
        protected void executeExtraCommands(GenericContainer<?> container) throws IOException {
            TRUSTED.forEach(host -> container.withExtraHost(host, "127.0.0.1"));
            container.withExtraHost(BambooHRPeer.UNTRUSTED, "127.0.0.2");
            container.withExtraHost(BambooHRPeer.WRONG, "127.0.0.1");
            for (String file : List.of("trusted.p12", "untrusted.p12", "trust.jks")) {
                container.withCopyFileToContainer(
                        MountableFile.forHostPath(fixtures.resolve(file)), ROOT + "/" + file);
            }
            String pack = BambooHRPeer.class.getPackage().getName().replace('.', '/');
            try {
                container.withCopyFileToContainer(
                        MountableFile.forHostPath(
                                Path.of(
                                        BambooHRPeer.class
                                                .getResource("BambooHRPeer.class")
                                                .toURI())),
                        ROOT + "/classes/" + pack + "/BambooHRPeer.class");
                Path json =
                        Path.of(
                                ObjectMapper.class
                                        .getProtectionDomain()
                                        .getCodeSource()
                                        .getLocation()
                                        .toURI());
                container.withCopyFileToContainer(
                        MountableFile.forHostPath(json),
                        ROOT + (Files.isDirectory(json) ? "/jackson" : "/jackson.jar"));
            } catch (URISyntaxException e) {
                throw new IOException("Cannot stage the BambooHR HTTPS fixture", e);
            }
        }

        Container.ExecResult runPeer() throws Exception {
            return server.execInContainer(
                    "java",
                    "-cp",
                    ROOT + "/classes:" + ROOT + "/jackson:" + ROOT + "/jackson.jar",
                    BambooHRPeer.class.getName(),
                    ROOT);
        }

        void install(Path file, String name) {
            server.copyFileToContainer(MountableFile.forHostPath(file), ROOT + "/" + name);
        }

        boolean exists(String name) throws Exception {
            return server.execInContainer("test", "-f", ROOT + "/" + name).getExitCode() == 0;
        }

        void touch(String file) throws Exception {
            success(server.execInContainer("touch", ROOT + "/" + file));
        }

        String contents(String name) throws Exception {
            if (!exists(name)) {
                return "";
            }
            Container.ExecResult result = server.execInContainer("cat", ROOT + "/" + name);
            success(result);
            return result.getStdout();
        }

        String rows(String name) throws Exception {
            String dir = OUTPUT + name;
            Container.ExecResult result =
                    server.execInContainer(
                            "sh",
                            "-c",
                            "if [ -d "
                                    + dir
                                    + " ]; then find "
                                    + dir
                                    + " -maxdepth 1 -type f -name 'receipt_*'"
                                    + " -exec sh -c 'for f do cat \"$f\"; printf \"\\n\"; done' sh {} +; fi");
            success(result);
            return result.getStdout();
        }
    }
}
