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

package org.apache.seatunnel.e2e.connector.agilecrm;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.agilecrm.AgileCrmSourceFactory;
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
import java.io.Serializable;
import java.lang.reflect.Proxy;
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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Execution(ExecutionMode.SAME_THREAD)
class AgileCrmSourceIT extends TestSuiteBase implements TestResource {
    private static final ObjectMapper JSON =
            new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    private static final String HOST = "fixture-agilecrm";
    private static final String ORIGIN = "http://" + HOST + ":8080";
    private static final String USERNAME = "fixture-user@example.invalid";
    private static final String API_KEY = "fixture-agilecrm-key-527";
    private static final String PRIVATE_VALUE = "agilecrm-fixture-private-server-data-493";
    private static final String CONTACT_A = "9007199254740993";
    private static final String CONTACT_B = "9007199254740995";
    private static final String FILTER_A = "9007199254742001";
    private static final String FILTER_B = "9007199254742003";
    private static final String NEXT = "another page+中/?=";
    private static final String LAST = "final&cursor/=?";
    private GenericContainer<?> fixture;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        Path tls = Files.createTempDirectory("agilecrm-test-tls-");
        try {
            createCertificate(tls);
            fixture =
                    new GenericContainer<>(DockerImageName.parse("python:3.11-alpine"))
                            .withNetwork(NETWORK)
                            .withNetworkAliases(HOST)
                            .withExposedPorts(8080)
                            .withCopyFileToContainer(
                                    MountableFile.forHostPath(
                                            ContainerUtil.getResourcesFile("/agilecrm_fixture.py")
                                                    .toPath()),
                                    "/agilecrm_fixture.py")
                            .withCopyFileToContainer(
                                    MountableFile.forHostPath(tls.resolve("fixture.crt")),
                                    "/fixture.crt")
                            .withCopyFileToContainer(
                                    MountableFile.forHostPath(tls.resolve("fixture.key")),
                                    "/fixture.key")
                            .withCommand("python", "/agilecrm_fixture.py")
                            .waitingFor(Wait.forHttp("/health").forStatusCode(200));
            fixture.start();
        } finally {
            Files.deleteIfExists(tls.resolve("fixture.p12"));
            Files.deleteIfExists(tls.resolve("fixture.key"));
            Files.deleteIfExists(tls.resolve("fixture.crt"));
            Files.deleteIfExists(tls);
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
    void followsLastRecordCursorsAndPreservesContactValues(TestContainer engine) throws Exception {
        reset(mode("gzip"));
        List<Result> output = read(engine, options("contacts"));
        Assertions.assertEquals(2, output.size());
        Result first = output.get(0);
        Assertions.assertEquals(CONTACT_A, first.id);
        Assertions.assertEquals(CONTACT_B, output.get(1).id);
        Assertions.assertEquals("", first.parent);
        Assertions.assertEquals(CONTACT_A, first.payload.path("id").asText());
        Assertions.assertEquals("Café 中文", first.payload.path("title").asText());
        Assertions.assertTrue(first.payload.path("optional").isNull());
        Assertions.assertFalse(first.payload.at("/attributes/flag").asBoolean(true));
        Assertions.assertEquals("x", first.payload.at("/attributes/order/1").asText());
        Assertions.assertEquals(
                0,
                new BigDecimal("12345678901234567890.123456789012345678901")
                        .compareTo(first.payload.path("price").decimalValue()));
        JsonNode calls = calls();
        Assertions.assertEquals(3, calls.size());
        for (JsonNode call : calls) {
            assertExchange(call, "GET", "/contacts");
            Assertions.assertEquals("100", call.at("/query/page_size/0").asText());
        }
        Assertions.assertFalse(calls.path(0).path("query").has("cursor"));
        Assertions.assertEquals(NEXT, calls.path(1).at("/query/cursor/0").asText());
        Assertions.assertEquals(LAST, calls.path(2).at("/query/cursor/0").asText());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void sendsCompanyContinuationAsReadOnlyFormData(TestContainer engine) throws Exception {
        ObjectNode conditions = mode("deflate");
        conditions.putArray("sequence").add("429").add("retry_date");
        reset(conditions);
        List<Result> rows = read(engine, options("companies").put("page_size", 5));
        Assertions.assertEquals(Arrays.asList("71001", "71002"), ids(rows));
        JsonNode calls = calls();
        Assertions.assertEquals(4, calls.size());
        for (JsonNode call : calls) {
            assertExchange(call, "POST", "/contacts/companies/list");
            Assertions.assertTrue(call.path("query").isEmpty());
            Assertions.assertTrue(call.path("form_header").asBoolean());
            Assertions.assertEquals("5", call.at("/form/page_size/0").asText());
            Assertions.assertEquals("-created_time", call.at("/form/global_sort_key/0").asText());
        }
        for (int i = 0; i < 3; i++) {
            Assertions.assertFalse(calls.path(i).path("form").has("cursor"));
        }
        Assertions.assertEquals(NEXT, calls.path(3).at("/form/cursor/0").asText());
        Assertions.assertTrue(
                calls.path(1).path("time_ms").asLong()
                        >= calls.path(0).path("time_ms").asLong() + 1000);
        Assertions.assertTrue(
                calls.path(2).path("time_ms").asLong() + 10
                        >= calls.path(1).path("due_ms").asLong());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void readsDealsTasksEventsMilestonesCampaignsAndFilters(TestContainer engine) throws Exception {
        String[][] routes = {
            {"deals", "/opportunity", "72001", "72002"},
            {"tasks", "/tasks", "74001"},
            {"events", "/events", "75001"},
            {"milestone", "/milestone/pipelines", "76001"},
            {"campaigns", "/workflows", "73001", "73002"},
            {"ticket_filters", "/tickets/filters", FILTER_A, FILTER_B}
        };
        for (String[] route : routes) {
            reset(mode(null));
            ObjectNode opts = options(route[0]);
            if ("events".equals(route[0])) {
                opts.put("event_start", 1761955200L).put("event_end", 1764547200L);
            }
            List<Result> records = read(engine, opts);
            Assertions.assertEquals(Arrays.asList(route).subList(2, route.length), ids(records));
            JsonNode calls = calls();
            Assertions.assertEquals(
                    "deals".equals(route[0]) || "campaigns".equals(route[0]) ? 2 : 1, calls.size());
            for (JsonNode call : calls) {
                assertExchange(call, "GET", route[1]);
            }
            if (calls.size() == 2) {
                Assertions.assertEquals(NEXT, calls.path(1).at("/query/cursor/0").asText());
            }
            if ("events".equals(route[0])) {
                Assertions.assertEquals("1761955200", calls.path(0).at("/query/start/0").asText());
                Assertions.assertEquals("1764547200", calls.path(0).at("/query/end/0").asText());
                Assertions.assertEquals(1761955200L, records.get(0).payload.path("start").asLong());
            }
        }
        reset(mode("no_content"));
        Assertions.assertTrue(read(engine, options("tasks")).isEmpty());
        Assertions.assertEquals(1, calls().size());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void discoversEveryContactBeforeReadingNotesOrDocumentMetadata(TestContainer engine)
            throws Exception {
        for (String resource : Arrays.asList("notes", "documents")) {
            reset(mode("duplicate_parent"));
            List<Result> records = read(engine, options(resource));
            Assertions.assertEquals(2, records.size());
            String id = "notes".equals(resource) ? "801" : "802";
            Assertions.assertEquals(Arrays.asList(id, id), ids(records));
            Assertions.assertEquals(CONTACT_A, records.get(0).parent);
            Assertions.assertEquals(CONTACT_B, records.get(1).parent);
            JsonNode calls = calls();
            Assertions.assertEquals(5, calls.size());
            for (int i = 0; i < 3; i++) {
                assertExchange(calls.path(i), "GET", "/contacts");
                Assertions.assertFalse(calls.path(i).path("query").has("filter_id"));
                Assertions.assertFalse(calls.path(i).path("query").has("type"));
            }
            assertExchange(calls.path(3), "GET", child(resource, CONTACT_A));
            assertExchange(calls.path(4), "GET", child(resource, CONTACT_B));
            if ("documents".equals(resource)) {
                Assertions.assertEquals(
                        "https://files.invalid/sample",
                        records.get(0).payload.path("url").asText());
            }
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void readsTicketsPerFilterAndRefusesUnprovenContinuation(TestContainer engine)
            throws Exception {
        reset(mode(null));
        List<Result> tickets = read(engine, options("tickets"));
        Assertions.assertEquals(Arrays.asList("77", "77"), ids(tickets));
        Assertions.assertEquals(FILTER_A, tickets.get(0).parent);
        Assertions.assertEquals(FILTER_B, tickets.get(1).parent);
        JsonNode calls = calls();
        Assertions.assertEquals(3, calls.size());
        assertExchange(calls.path(0), "GET", "/tickets/filters");
        for (int i = 1; i <= 2; i++) {
            assertExchange(calls.path(i), "GET", "/tickets/filter");
            Assertions.assertEquals(
                    i == 1 ? FILTER_A : FILTER_B, calls.path(i).at("/query/filter_id/0").asText());
            Assertions.assertEquals("25", calls.path(i).at("/query/page_size/0").asText());
            Assertions.assertEquals(
                    "-last_updated_time", calls.path(i).at("/query/global_sort_key/0").asText());
            Assertions.assertFalse(calls.path(i).path("query").has("cursor"));
        }

        for (String boundary : Arrays.asList("ticket_full", "ticket_cursor")) {
            reset(mode(boundary));
            String log =
                    failed(
                            engine,
                            options("tickets").put("page_size", 2),
                            "potentially incomplete page");
            Assertions.assertTrue(outputs(log, "tickets").isEmpty());
            Assertions.assertEquals(2, calls().size());
            Assertions.assertFalse(calls().path(1).path("query").has("cursor"));
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void validatesWindowAndParentIDsBeforeChildRequests(TestContainer engine) throws Exception {
        reset(mode(null));
        failed(
                engine,
                options("events").put("event_start", 2).put("event_end", 1),
                "events requires nonnegative event_start");
        failed(engine, options("contacts").put("event_start", 20), "apply only to events");
        failed(
                engine,
                options("contacts").put("allow_insecure_http", false),
                "base_url must be an HTTPS origin");
        Assertions.assertTrue(calls().isEmpty());

        reset(mode("bad_parent"));
        failed(engine, options("documents"), "parent id must be a positive signed 64-bit integer");
        Assertions.assertEquals(1, calls().size());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void rejectsInvalidAndRepeatedCursorsWithoutEmittingThatPage(TestContainer engine)
            throws Exception {
        reset(mode("cycle"));
        String logs = failed(engine, options("contacts"), "pagination repeated a cursor");
        Assertions.assertEquals(
                Collections.singletonList(CONTACT_A), ids(outputs(logs, "contacts")));
        Assertions.assertEquals(2, calls().size());

        reset(mode("bad_cursor"));
        logs = failed(engine, options("contacts"), "invalid last-record cursor");
        Assertions.assertTrue(outputs(logs, "contacts").isEmpty());
        Assertions.assertEquals(1, calls().size());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void rejectsUnsafeResponsesWithoutCredentialOrBodyDisclosure(TestContainer engine)
            throws Exception {
        reset(mode(null));
        failed(
                engine,
                options("contacts").put("password", API_KEY + "-wrong"),
                "unexpected HTTP 401");
        Assertions.assertEquals(1, calls().size());
        Assertions.assertFalse(calls().path(0).path("authenticated").asBoolean());

        String[][] bad = {
            {"403", "unexpected HTTP 403"}, {"redirect", "unexpected HTTP 307"},
            {"invalid", "invalid JSON response"}, {"shape", "response must be a JSON record list"},
            {"huge", "response exceeds 8 MiB"}
        };
        for (String[] part : bad) {
            reset(mode(part[0]));
            Assertions.assertTrue(
                    outputs(failed(engine, options("contacts"), part[1]), "contacts").isEmpty());
            Assertions.assertEquals(1, calls().size());
        }
        reset(mode(null));
        failed(
                engine,
                options("contacts")
                        .put("base_url", "https://" + HOST + ":8443")
                        .put("allow_insecure_http", false),
                "TLS transport failed");
        Assertions.assertEquals(0, calls().size());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void boundsAttemptsAndDisallowsRetryAfterOutsideItsDeadline(TestContainer engine)
            throws Exception {
        reset(mode("503"));
        failed(
                engine,
                options("companies").put("max_attempts", 2),
                "HTTP 503 exhausted the configured attempts");
        Assertions.assertEquals(2, calls().size());
        for (JsonNode call : calls()) {
            assertExchange(call, "POST", "/contacts/companies/list");
        }
        reset(mode("wait_forbidden"));
        failed(
                engine,
                options("contacts").put("request_timeout_ms", 2500),
                "Retry-After or backoff exceeds");
        Assertions.assertEquals(1, calls().size());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void closesHeldAndTrickledResponsesWithinTheTotalDeadline(TestContainer engine)
            throws Exception {
        for (String response : Arrays.asList("hold", "drip")) {
            reset(mode(response));
            String logs =
                    failed(
                            engine,
                            options("contacts").put("request_timeout_ms", 1500),
                            "request deadline exceeded");
            Assertions.assertTrue(outputs(logs, "contacts").isEmpty());
            Awaitility.await()
                    .atMost(8, TimeUnit.SECONDS)
                    .until(() -> calls().path(0).path("disconnect_ms").isNumber());
            Assertions.assertEquals(1, calls().size());
            Assertions.assertTrue(calls().path(0).path("disconnect_ms").asLong() < 5000);
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void cancelClosesTheInFlightPageWithoutReturningPartialData(TestContainer engine)
            throws Exception {
        reset(mode("hold"));
        Path file = job(options("contacts").put("request_timeout_ms", 90000), false, false);
        String id = jobId();
        ExecutorService submit = Executors.newSingleThreadExecutor();
        try {
            Future<Container.ExecResult> running =
                    submit.submit(() -> engine.executeJob(resource(file), id));
            Awaitility.await()
                    .atMost(90, TimeUnit.SECONDS)
                    .until(() -> calls().path(0).path("held").asBoolean());
            successful(engine.cancelJob(id));
            waitForStatus(engine, id, "CANCELED");
            running.get(60, TimeUnit.SECONDS);
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .until(() -> calls().path(0).path("disconnect_ms").isNumber());
            Assertions.assertTrue(calls().path(0).path("disconnect_ms").asLong() < 90000);
            Assertions.assertEquals(1, calls().size());
        } finally {
            try {
                release();
                stop(engine, id);
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
    void restoresTheNextContactCursorAndRejectsAChangedPageSize(TestContainer engine)
            throws Exception {
        reset(mode(null).put("hold_path", "/dev/api/contacts").put("hold_cursor", NEXT));
        ObjectNode opts = options("contacts").put("request_timeout_ms", 120000);
        Path original = job(opts, true, false);
        Path changed = job(opts.deepCopy().put("page_size", 10), true, false);
        String first = jobId();
        String restored = jobId();
        String rejected = jobId();
        ExecutorService jobs = Executors.newSingleThreadExecutor();
        try {
            String before = engine.getServerLogs();
            Future<Container.ExecResult> run =
                    jobs.submit(() -> engine.executeJob(resource(original), first));
            Awaitility.await()
                    .atMost(90, TimeUnit.SECONDS)
                    .until(() -> calls().path(1).path("held").asBoolean());
            Awaitility.await()
                    .atMost(20, TimeUnit.SECONDS)
                    .until(() -> fresh(engine, before).contains("contacts-first"));
            awaitCheckpoint(engine, first);
            successful(engine.savepointJob(first));
            waitForStatus(engine, first, "SAVEPOINT_DONE");
            successful(run.get(60, TimeUnit.SECONDS));
            Assertions.assertEquals(2, calls().size());
            release();

            String failureLog = engine.getServerLogs();
            Container.ExecResult bad =
                    engine.restoreJobWithCheckpoint(resource(changed), first, rejected);
            Assertions.assertNotEquals(0, bad.getExitCode());
            Assertions.assertTrue(
                    merged(bad, fresh(engine, failureLog))
                            .contains(
                                    "checkpoint resource, origin, page size or event range does not match"));
            Assertions.assertEquals(2, calls().size());

            String resumeLog = engine.getServerLogs();
            Container.ExecResult good =
                    engine.restoreJobWithCheckpoint(resource(original), first, restored);
            successful(good);
            Assertions.assertEquals(
                    Collections.singletonList(CONTACT_B),
                    ids(outputs(good, fresh(engine, resumeLog), "contacts")));
            Assertions.assertEquals(4, calls().size());
            Assertions.assertEquals(NEXT, calls().path(2).at("/query/cursor/0").asText());
            Assertions.assertEquals(LAST, calls().path(3).at("/query/cursor/0").asText());
        } finally {
            try {
                release();
                stop(engine, first);
                stop(engine, restored);
                stop(engine, rejected);
            } finally {
                jobs.shutdownNow();
                Files.deleteIfExists(original);
                Files.deleteIfExists(changed);
            }
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void terminalScanStaysFinishedWhenTheStreamingJobRestores(TestContainer engine)
            throws Exception {
        reset(mode(null));
        Path path = job(options("tasks"), true, true);
        String initial = jobId();
        String restored = jobId();
        ExecutorService submit = Executors.newSingleThreadExecutor();
        try {
            String before = engine.getServerLogs();
            Future<Container.ExecResult> first =
                    submit.submit(() -> engine.executeJob(resource(path), initial));
            Awaitility.await()
                    .atMost(90, TimeUnit.SECONDS)
                    .until(() -> fresh(engine, before).contains("tasks-one"));
            awaitCheckpoint(engine, initial);
            successful(engine.savepointJob(initial));
            waitForStatus(engine, initial, "SAVEPOINT_DONE");
            successful(first.get(60, TimeUnit.SECONDS));
            Assertions.assertEquals(1, calls().size());

            Future<Container.ExecResult> second =
                    submit.submit(
                            () ->
                                    engine.restoreJobWithCheckpoint(
                                            resource(path), initial, restored));
            waitForStatus(engine, restored, "RUNNING");
            awaitCheckpoint(engine, restored);
            Assertions.assertEquals(1, calls().size());
            successful(engine.cancelJob(restored));
            waitForStatus(engine, restored, "CANCELED");
            second.get(60, TimeUnit.SECONDS);
            Assertions.assertEquals(1, calls().size());
        } finally {
            try {
                stop(engine, initial);
                stop(engine, restored);
            } finally {
                submit.shutdownNow();
                Files.deleteIfExists(path);
            }
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void unassignedCheckpointRetainsWorkAndEmptyDiscoveryDoesNotFetchChildren(TestContainer engine)
            throws Exception {
        reset(mode("empty"));
        Map<String, Object> configuration =
                JSON.convertValue(options("notes"), new TypeReference<Map<String, Object>>() {});
        TableSource<?, ?, ?> table =
                new AgileCrmSourceFactory()
                        .createSource(
                                new TableSourceFactoryContext(
                                        ReadonlyConfig.fromMap(configuration),
                                        getClass().getClassLoader()));
        assertUnassignedCheckpoint(table);
        Assertions.assertEquals(0, calls().size());

        Assertions.assertTrue(read(engine, options("notes")).isEmpty());
        Assertions.assertEquals(1, calls().size());
        assertExchange(calls().path(0), "GET", "/contacts");
    }

    /**
     * Drives actual connector lifecycle calls before assignment, when a fixture cannot pause an
     * engine split that has not made any external request.
     */
    @SuppressWarnings("unchecked")
    private static <T, S extends SourceSplit, C extends Serializable>
            void assertUnassignedCheckpoint(TableSource<T, S, C> table) throws Exception {
        SeaTunnelSource<T, S, C> source = table.createSource();
        Set<Integer> registered = new HashSet<>();
        List<S> assigned = new ArrayList<>();
        SourceSplitEnumerator.Context<S> enumerator =
                (SourceSplitEnumerator.Context<S>)
                        Proxy.newProxyInstance(
                                AgileCrmSourceIT.class.getClassLoader(),
                                new Class<?>[] {SourceSplitEnumerator.Context.class},
                                (target, method, args) -> {
                                    switch (method.getName()) {
                                        case "currentParallelism":
                                            return 1;
                                        case "registeredReaders":
                                            return registered;
                                        case "assignSplit":
                                            assigned.add((S) args[1]);
                                            return null;
                                        case "signalNoMoreSplits":
                                            return null;
                                        default:
                                            throw new AssertionError(method.getName());
                                    }
                                });
        SourceReader.Context readerContext =
                (SourceReader.Context)
                        Proxy.newProxyInstance(
                                AgileCrmSourceIT.class.getClassLoader(),
                                new Class<?>[] {SourceReader.Context.class},
                                (target, method, args) -> {
                                    if ("getBoundedness".equals(method.getName())) {
                                        return Boundedness.BOUNDED;
                                    }
                                    if ("getIndexOfSubtask".equals(method.getName())) {
                                        return 0;
                                    }
                                    throw new AssertionError(method.getName());
                                });
        C coordinatorState;
        try (SourceSplitEnumerator<S, C> fresh = source.createEnumerator(enumerator);
                SourceReader<T, S> reader = source.createReader(readerContext)) {
            fresh.open();
            reader.open();
            Assertions.assertTrue(reader.snapshotState(1).isEmpty());
            coordinatorState = fresh.snapshotState(1);
        }
        registered.add(0);
        try (SourceSplitEnumerator<S, C> restored =
                        source.restoreEnumerator(enumerator, coordinatorState);
                SourceReader<T, S> reader = source.createReader(readerContext)) {
            restored.open();
            reader.open();
            restored.registerReader(0);
            restored.run();
            Assertions.assertEquals(1, assigned.size());
            reader.addSplits(assigned);
            Assertions.assertEquals(1, reader.snapshotState(2).size());
        }
    }

    private static void assertExchange(JsonNode call, String method, String path) {
        Assertions.assertEquals(method, call.path("method").asText());
        Assertions.assertEquals("/dev/api" + path, call.path("path").asText());
        Assertions.assertTrue(call.path("authenticated").asBoolean());
        Assertions.assertTrue(call.path("json").asBoolean());
    }

    private static String child(String resource, String id) {
        return "notes".equals(resource)
                ? "/contacts/" + id + "/notes"
                : "/documents/contact/" + id + "/docs";
    }

    private static List<String> ids(List<Result> rows) {
        List<String> result = new ArrayList<>(rows.size());
        for (Result row : rows) {
            result.add(row.id);
        }
        return result;
    }

    private static List<Result> read(TestContainer engine, ObjectNode opts) throws Exception {
        String before = engine.getServerLogs();
        Container.ExecResult result = run(engine, opts);
        successful(result);
        return outputs(result, fresh(engine, before), opts.path("resource").asText());
    }

    private static String failed(TestContainer engine, ObjectNode opts, String expected)
            throws Exception {
        String before = engine.getServerLogs();
        Container.ExecResult result = run(engine, opts);
        String tail = fresh(engine, before);
        String logs = merged(result, tail);
        Assertions.assertNotEquals(0, result.getExitCode(), logs);
        Assertions.assertTrue(logs.contains(expected), () -> "Missing " + expected + " in " + logs);
        Assertions.assertFalse(logs.contains(USERNAME));
        Assertions.assertFalse(logs.contains(API_KEY));
        Assertions.assertFalse(logs.contains(PRIVATE_VALUE));
        Assertions.assertFalse(
                logs.contains(
                        Base64.getEncoder()
                                .encodeToString(
                                        (USERNAME + ":" + opts.path("password").asText())
                                                .getBytes(StandardCharsets.UTF_8))));
        return tail.contains("SeaTunnelRow#kind=") ? tail : result.getStdout();
    }

    private static List<Result> outputs(Container.ExecResult result, String tail, String resource)
            throws IOException {
        List<Result> fromServer = outputs(tail, resource);
        return fromServer.isEmpty() ? outputs(result.getStdout(), resource) : fromServer;
    }

    private static List<Result> outputs(String log, String resource) throws IOException {
        List<Result> results = new ArrayList<>();
        String marker = " : " + resource + ", ";
        for (String line : log.split("\\R")) {
            int row = line.indexOf("SeaTunnelRow#kind=");
            int fields = row < 0 ? -1 : line.indexOf(marker, row);
            if (fields >= 0) {
                String[] columns = line.substring(fields + 3).split(", ", 4);
                Assertions.assertEquals(4, columns.length, line);
                results.add(new Result(columns[1], columns[2], JSON.readTree(columns[3])));
            }
        }
        return results;
    }

    private static String fresh(TestContainer engine, String before) {
        String logs = engine.getServerLogs();
        return logs.startsWith(before) ? logs.substring(before.length()) : "";
    }

    private static String merged(Container.ExecResult result, String server) {
        return result.getStdout() + "\n" + result.getStderr() + "\n" + server;
    }

    private static void successful(Container.ExecResult result) {
        Assertions.assertEquals(0, result.getExitCode(), () -> merged(result, ""));
    }

    private static Container.ExecResult run(TestContainer engine, ObjectNode opts)
            throws Exception {
        Path file = job(opts, false, false);
        try {
            return engine.executeJob(resource(file));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    private static Path job(ObjectNode opts, boolean streaming, boolean anotherSource)
            throws IOException {
        String mode = streaming ? "STREAMING\n  checkpoint.interval = 1000" : "BATCH";
        String extra =
                anotherSource
                        ? "\n  FakeSource {\n"
                                + "    plugin_output = \"keepalive\"\n"
                                + "    schema = { fields { keepalive = string } }\n"
                                + "    row.num = 1\n"
                                + "    split.read-interval = 150\n"
                                + "    rows = [{kind = INSERT, fields = [\"unrelated-stream\"]}]\n"
                                + "  }"
                        : "";
        ObjectNode target = opts.deepCopy().put("plugin_output", "selected");
        String inputs = anotherSource ? "[\"selected\", \"keepalive\"]" : "[\"selected\"]";
        String content =
                "env {\n  parallelism = 1\n  job.retry.times = 0\n  job.mode = "
                        + mode
                        + "\n}\nsource {\n  AgileCRM "
                        + target
                        + extra
                        + "\n}\nsink {\n  Console { plugin_input = "
                        + inputs
                        + " }\n}\n";
        Path path =
                Files.createTempFile(
                        ContainerUtil.getCurrentModulePath().resolve("src/test/resources"),
                        "agilecrm-e2e-",
                        ".conf");
        Files.write(path, content.getBytes(StandardCharsets.UTF_8));
        return path;
    }

    private static ObjectNode options(String resource) {
        return JSON.createObjectNode()
                .put("resource", resource)
                .put("base_url", ORIGIN)
                .put("allow_insecure_http", true)
                .put("username", USERNAME)
                .put("password", API_KEY);
    }

    private static ObjectNode mode(String defaultMode) {
        return JSON.createObjectNode().put("mode", defaultMode);
    }

    private void reset(ObjectNode mode) throws IOException {
        fixtureControl("/control/reset", mode.toString());
    }

    private void release() throws IOException {
        fixtureControl("/control/release", "");
    }

    private JsonNode calls() throws IOException {
        return fixtureControl("/control/state", null);
    }

    private JsonNode fixtureControl(String path, String body) throws IOException {
        HttpURLConnection connection =
                (HttpURLConnection)
                        new URL("http", fixture.getHost(), fixture.getMappedPort(8080), path)
                                .openConnection();
        connection.setConnectTimeout(5000);
        connection.setReadTimeout(5000);
        try {
            if (body != null) {
                connection.setRequestMethod("POST");
                connection.setDoOutput(true);
                connection.setRequestProperty("Content-Type", "application/json");
                try (OutputStream out = connection.getOutputStream()) {
                    out.write(body.getBytes(StandardCharsets.UTF_8));
                }
                Assertions.assertEquals(204, connection.getResponseCode());
                return null;
            }
            Assertions.assertEquals(200, connection.getResponseCode());
            try (InputStream in = connection.getInputStream()) {
                return JSON.readTree(in);
            }
        } finally {
            connection.disconnect();
        }
    }

    private static String resource(Path config) {
        return "/" + config.getFileName();
    }

    private static String jobId() {
        return Long.toString(JobIdGenerator.newJobId());
    }

    private static void awaitCheckpoint(TestContainer engine, String id) {
        long before = engine.getCompletedCheckpointCount(id);
        Awaitility.await()
                .atMost(45, TimeUnit.SECONDS)
                .until(() -> engine.getCompletedCheckpointCount(id) > before);
    }

    private static void waitForStatus(TestContainer engine, String id, String expected) {
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .until(() -> expected.equals(engine.getJobStatus(id)));
    }

    private static void stop(TestContainer engine, String id) throws Exception {
        String state = engine.getJobStatus(id);
        if ("RUNNING".equals(state) || "SCHEDULED".equals(state)) {
            engine.cancelJob(id);
        }
    }

    private static void createCertificate(Path tls) throws Exception {
        Path keytool = Paths.get(System.getProperty("java.home"), "bin", "keytool");
        Path file = tls.resolve("fixture.p12");
        Process key =
                new ProcessBuilder(
                                keytool.toString(),
                                "-genkeypair",
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                file.toString(),
                                "-storepass",
                                "fixture-test-pass",
                                "-alias",
                                "agilecrm-test",
                                "-dname",
                                "CN=" + HOST,
                                "-ext",
                                "SAN=dns:" + HOST,
                                "-keyalg",
                                "RSA",
                                "-keysize",
                                "2048",
                                "-validity",
                                "2",
                                "-noprompt")
                        .inheritIO()
                        .start();
        if (!key.waitFor(30, TimeUnit.SECONDS)) {
            key.destroyForcibly();
            throw new IOException("Could not complete a fixture-only certificate");
        }
        Assertions.assertEquals(0, key.exitValue());
        KeyStore store = KeyStore.getInstance("PKCS12");
        char[] password = "fixture-test-pass".toCharArray();
        try (InputStream in = Files.newInputStream(file)) {
            store.load(in, password);
        }
        pem(
                tls.resolve("fixture.crt"),
                "CERTIFICATE",
                store.getCertificate("agilecrm-test").getEncoded());
        pem(
                tls.resolve("fixture.key"),
                "PRIVATE KEY",
                store.getKey("agilecrm-test", password).getEncoded());
    }

    private static void pem(Path path, String label, byte[] data) throws IOException {
        String value = Base64.getMimeEncoder(64, new byte[] {'\n'}).encodeToString(data);
        String text = "-----BEGIN " + label + "-----\n" + value + "\n-----END " + label + "-----\n";
        Files.write(path, text.getBytes(StandardCharsets.US_ASCII));
    }

    private static final class Result {
        private final String id;
        private final String parent;
        private final JsonNode payload;

        private Result(String id, String parent, JsonNode payload) {
            this.id = id;
            this.parent = parent;
            this.payload = payload;
        }
    }
}
