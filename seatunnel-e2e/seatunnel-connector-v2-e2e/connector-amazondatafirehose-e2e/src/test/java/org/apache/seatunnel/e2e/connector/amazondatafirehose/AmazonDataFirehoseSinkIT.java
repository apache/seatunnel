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

package org.apache.seatunnel.e2e.connector.amazondatafirehose;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.seatunnel.SeaTunnelContainer;
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
import org.testcontainers.containers.Network;
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
import java.util.Arrays;
import java.util.Base64;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Exercises captured signatures/record bytes and failure, flush, recovery and cancellation
 * boundaries. Synthetic HTTP fixtures and isolated DEBUG logging do not certify live AWS TLS or
 * final delivery.
 */
@Execution(ExecutionMode.SAME_THREAD)
class AmazonDataFirehoseSinkIT extends TestSuiteBase implements TestResource {
    private static final String ACCESS = "AKIA000000000000TEST";
    private static final String SECRET = "syntheticFirehoseSecret73+test";
    private static final String TOKEN = "syntheticFirehoseSessionToken-5981";
    private static final String STREAM = "seatunnel-test-delivery";
    private static final String TARGET = "Firehose_20150804.PutRecordBatch";
    private static final String ENDPOINT = "http://firehose-fixture:8080";
    private static final String PROVIDER_MARKER = "provider-private-marker-327";
    private static final String NESTED =
            "{\"name\":\"海 🐋\",\"object\":{\"nullable\":null,\"arr\":[true,9007199254740993]},\"decimal\":0.123456789012345678901}";
    private static final ObjectMapper JSON = new ObjectMapper();

    private GenericContainer<?> fixture;

    @BeforeAll
    @Override
    public void startUp() {
        fixture = startFixture(NETWORK);
    }

    private static GenericContainer<?> startFixture(Network network) {
        GenericContainer<?> server =
                new GenericContainer<>(DockerImageName.parse("python:3.11-alpine"))
                        .withNetwork(network)
                        .withNetworkAliases("firehose-fixture")
                        .withExposedPorts(8080)
                        .withCopyFileToContainer(
                                MountableFile.forHostPath(
                                        ContainerUtil.getResourcesFile("/firehose_fixture.py")
                                                .toPath()),
                                "/firehose_fixture.py")
                        .withCommand("python", "/firehose_fixture.py")
                        .waitingFor(Wait.forHttp("/health").forStatusCode(200));
        server.start();
        return server;
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (fixture != null) {
            fixture.close();
        }
    }

    @TestTemplate
    void writesThresholdAndFinalBatchesWithExactPayloads(TestContainer container) throws Exception {
        reset(step("gzip"), step("deflate"));
        succeeds(
                run(
                        container,
                        options().put("batch_size", 2).put("token", TOKEN),
                        "string",
                        rows(
                                row("INSERT", NESTED),
                                row("INSERT", "{\"id\":2}"),
                                row("INSERT", "{\"id\":3}"))));
        JsonNode written = state();
        Assertions.assertEquals(2, written.path("requests").size());
        assertBatch(written, 0, true, data(NESTED + "\n"), data("{\"id\":2}\n"));
        assertBatch(written, 1, true, data("{\"id\":3}\n"));
        Assertions.assertEquals(
                array(data(NESTED + "\n"), data("{\"id\":2}\n"), data("{\"id\":3}\n")),
                written.path("accepted"));

        byte[] bytes = {0, 10, 13, -1, 12};
        succeeds(
                run(
                        container,
                        options().put("format", "RAW"),
                        "bytes",
                        rows(row("INSERT", data(bytes)))));
        written = state();
        assertBatch(written, 2, false, data(bytes));
        Assertions.assertEquals(data(bytes), written.at("/accepted/3").asText());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void preservesDistinctLongJsonMemberNamesAcrossBatches(TestContainer container)
            throws Exception {
        reset();
        char[] characters = new char[4096];
        Arrays.fill(characters, 'a');
        String prefix = new String(characters);
        ArrayNode records = rows();
        ArrayNode expected = JSON.createArrayNode();
        // Modest compatibility data: this verifies exact opaque bytes, not heap/cache-size bounds.
        for (int i = 0; i < 8; i++) {
            String payload =
                    "{ \""
                            + prefix
                            + i
                            + "雪\" : { \"index\": "
                            + i
                            + ", \"value\": 1.0000000000000000001, \"text\": \"\\u96ea\" } }";
            records.add(row("INSERT", payload));
            expected.add(data(payload + "\n"));
        }
        succeeds(run(container, options().put("batch_size", 3), "string", records));
        JsonNode captured = state();
        Assertions.assertEquals(3, captured.path("requests").size());
        assertBatch(
                captured,
                0,
                false,
                expected.get(0).asText(),
                expected.get(1).asText(),
                expected.get(2).asText());
        assertBatch(
                captured,
                1,
                false,
                expected.get(3).asText(),
                expected.get(4).asText(),
                expected.get(5).asText());
        assertBatch(captured, 2, false, expected.get(6).asText(), expected.get(7).asText());
        Assertions.assertEquals(expected, captured.path("accepted"));
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void serializesTypedRowsToJsonAndDelimitedRecords(TestContainer container) throws Exception {
        reset();
        ObjectNode schema = JSON.createObjectNode().put("name", "string").put("score", "int");
        ArrayNode rows = rows(row("INSERT", "sea|\"鲸\"", 85), row("INSERT", "missing", null));
        succeeds(run(container, nativeJsonOptions(), schema, rows));
        succeeds(
                run(
                        container,
                        options().put("format", "DELIMITED").put("field_delimiter", "|"),
                        schema,
                        rows));
        JsonNode state = state();
        assertBatch(
                state,
                0,
                false,
                data("{\"name\":\"sea|\\\"鲸\\\"\",\"score\":85}\n"),
                data("{\"name\":\"missing\",\"score\":null}\n"));
        assertBatch(state, 1, false, data("\"sea|\"\"鲸\"\"\"|85\n"), data("missing|\n"));
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void rejectsExpandedNativeArraysWithoutServiceCalls(TestContainer container) throws Exception {
        reset();
        ArrayNode small = JSON.createArrayNode().add("雪").add("quote\"");
        succeeds(
                run(
                        container,
                        nativeJsonOptions().put("max_record_bytes", 512),
                        "array<string>",
                        rows(row("INSERT", small))));
        assertBatch(state(), 0, false, data("{\"payload\":[\"雪\",\"quote\\\"\"]}\n"));
        reset();
        char[] characters = new char[128];
        Arrays.fill(characters, 'a');
        String shared = new String(characters);
        ArrayNode expanded = JSON.createArrayNode();
        for (int i = 0; i < 16; i++) {
            expanded.add(shared);
        }
        fails(
                container,
                nativeJsonOptions().put("max_record_bytes", 512),
                "array<string>",
                rows(row("INSERT", expanded)),
                "max_record_bytes");
        Assertions.assertEquals(0, state().path("requests").size());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void splitsOnEncodedByteLimitAndRejectsOversizedRecords(TestContainer container)
            throws Exception {
        reset();
        byte[] full = new byte[1000 * 1024];
        Arrays.fill(full, (byte) 'a');
        String record = data(full);
        succeeds(
                run(
                        container,
                        options().put("format", "RAW").put("batch_size", 500),
                        "bytes",
                        rows(
                                row("INSERT", record),
                                row("INSERT", record),
                                row("INSERT", record),
                                row("INSERT", record))));
        JsonNode writes = state();
        Assertions.assertEquals(2, writes.path("requests").size());
        assertBatch(writes, 0, false, record, record, record);
        assertBatch(writes, 1, false, record);
        Assertions.assertEquals(4, writes.path("accepted").size());

        reset();
        fails(
                container,
                options().put("max_record_bytes", 2),
                "string",
                rows(row("INSERT", "{}")),
                "payload and delimiter exceed max_record_bytes");
        succeeds(
                run(
                        container,
                        options().put("max_record_bytes", 3),
                        "string",
                        rows(row("INSERT", "{}"))));
        assertBatch(state(), 0, false, data("{}\n"));
        Assertions.assertEquals(1, state().path("requests").size());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void emptyAndInvalidInputsNeverHitTheService(TestContainer container) throws Exception {
        reset();
        succeeds(run(container, options(), "string", rows()));
        fails(
                container,
                options(),
                "string",
                rows(row("INSERT", (Object) null)),
                "nonempty payload");
        fails(
                container,
                options(),
                "string",
                rows(row("INSERT", "{\"x\":0,\"x\":1}")),
                "single valid JSON object with unique keys");
        fails(container, options(), "string", rows(row("DELETE", "{}")), "requires INSERT rows");
        fails(
                container,
                options().put("endpoint_url", ENDPOINT + "/unexpected"),
                "string",
                rows(row("INSERT", "{}")),
                "endpoint_url must be a trusted origin");
        ObjectNode incomplete = options();
        incomplete.remove("secret_key");
        fails(
                container,
                incomplete,
                "string",
                rows(row("INSERT", "{}")),
                "provide both access_key and secret_key");
        Assertions.assertEquals(0, state().path("requests").size());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void retriesOnlyTheValidatedFailedSubset(TestContainer container) throws Exception {
        reset(
                failures(1, "ServiceUnavailableException", 3, "InternalFailure"),
                step("ok"),
                failures(0, "InternalFailure"),
                failures(0, "InternalFailure"));
        ArrayNode records =
                rows(
                        row("INSERT", "{\"r\":0}"),
                        row("INSERT", "{\"r\":1}"),
                        row("INSERT", "{\"r\":2}"),
                        row("INSERT", "{\"r\":3}"));
        succeeds(run(container, options(), "string", records));
        JsonNode state = state();
        String first = data("{\"r\":0}\n");
        String second = data("{\"r\":1}\n");
        String third = data("{\"r\":2}\n");
        String fourth = data("{\"r\":3}\n");
        assertBatch(state, 0, false, first, second, third, fourth);
        assertBatch(state, 1, false, second, fourth);
        Assertions.assertEquals(array(first, third, second, fourth), state.path("accepted"));
        long wait =
                state.at("/requests/1/time_ns").asLong() - state.at("/requests/0/time_ns").asLong();
        Assertions.assertTrue(wait >= TimeUnit.MILLISECONDS.toNanos(90), "Retry ignored backoff");

        fails(
                container,
                options().put("max_retries", 1),
                "string",
                rows(row("INSERT", "{\"r\":4}")),
                "retries exhausted for 1 failed records");
        JsonNode exhausted = state();
        assertBatch(exhausted, 2, false, data("{\"r\":4}\n"));
        assertBatch(exhausted, 3, false, data("{\"r\":4}\n"));
        Assertions.assertEquals(4, exhausted.path("requests").size());
        Assertions.assertEquals(4, exhausted.path("accepted").size());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void ambiguousAndContradictoryAcknowledgementsNeverReplay(TestContainer container)
            throws Exception {
        reset(
                step("disconnect"),
                step("status").put("status", 500),
                step("redirect"),
                step("override").put("body", "{"),
                step("override").put("body", "{\"RequestResponses\":[{\"RecordId\":\"x\"}]}"),
                step("override").put("body", "{\"FailedPutCount\":0,\"RequestResponses\":[]}"),
                step("override")
                        .put(
                                "body",
                                "{\"FailedPutCount\":0,\"RequestResponses\":[{\"RecordId\":\"x\",\"ErrorCode\":\"InternalFailure\"}]}"),
                step("override")
                        .put(
                                "body",
                                "{\"FailedPutCount\":0,\"RequestResponses\":[{\"ErrorCode\":\"InternalFailure\"}]}"),
                failures(0, "UnrecognizedSyntheticFailure"),
                step("oversize"));
        String[] failures = {
            "may have accepted records",
            "unexpected HTTP 500",
            "unexpected HTTP 307",
            "invalid JSON acknowledgement",
            "invalid acknowledgement result structure",
            "invalid acknowledgement count",
            "contradictory acknowledgement entry",
            "reported failure count",
            "unrecognized per-record error",
            "acknowledgement exceeds 1 MiB"
        };
        ArrayNode rows = rows(row("INSERT", "{}"));
        for (int i = 0; i < failures.length; i++) {
            fails(container, options().put("token", TOKEN), "string", rows, failures[i]);
            Assertions.assertEquals(i + 1, state().path("requests").size());
        }
        JsonNode state = state();
        Assertions.assertEquals(0, state.path("redirects").asInt());
        Assertions.assertEquals(6, state.path("accepted").size());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void forbiddenAndMissingStreamAreTerminal(TestContainer container) throws Exception {
        reset(step("ok"), step("status").put("status", 400));
        fails(
                container,
                options().put("secret_key", "wrongSyntheticSecret").put("token", TOKEN),
                "string",
                rows(row("INSERT", "{}")),
                "unexpected HTTP 403");
        fails(
                container,
                options().put("token", TOKEN),
                "string",
                rows(row("INSERT", "{}")),
                "unexpected HTTP 400");
        JsonNode state = state();
        Assertions.assertEquals(2, state.path("requests").size());
        Assertions.assertFalse(state.at("/requests/0/verified").asBoolean());
        assertBatch(state, 1, true, data("{}\n"));
        Assertions.assertEquals(0, state.path("accepted").size());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void closesHeldOrTricklingSocketsAtTheBatchDeadline(TestContainer container) throws Exception {
        for (String kind : Arrays.asList("hold", "trickle")) {
            reset(step(kind));
            fails(
                    container,
                    options().put("batch_timeout_ms", 1500),
                    "string",
                    rows(row("INSERT", "{}")),
                    "may have accepted records");
            Awaitility.await()
                    .atMost(5, TimeUnit.SECONDS)
                    .until(() -> state().path("disconnect_ms").isNumber());
            JsonNode state = state();
            long elapsed = state.path("disconnect_ms").asLong();
            Assertions.assertTrue(
                    elapsed >= 0 && elapsed < 5000, "Disconnect after " + elapsed + " ms");
            Assertions.assertEquals(1, state.path("requests").size());
            Assertions.assertEquals(0, state.path("accepted").size());
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void flushesAtCheckpointAndRestoresAcknowledgedProgress(TestContainer container)
            throws Exception {
        reset(step("hold"));
        Path cfg =
                config(
                        options().put("batch_timeout_ms", 60000),
                        schema("string"),
                        rows(row("INSERT", "{}")),
                        "STREAMING\n  checkpoint.interval = 1000");
        String resource = resourceName(cfg);
        String oldId = jobId();
        String newId = jobId();
        ExecutorService jobs = Executors.newSingleThreadExecutor();
        try {
            Future<Container.ExecResult> original =
                    jobs.submit(() -> container.executeJob(resource, oldId));
            Awaitility.await()
                    .atMost(90, TimeUnit.SECONDS)
                    .until(() -> state().path("requests").size() == 1);
            Assertions.assertEquals(0, state().path("accepted").size());
            long heldCount = container.getCompletedCheckpointCount(oldId);
            release();
            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .until(
                            () ->
                                    state().path("accepted").size() == 1
                                            && container.getCompletedCheckpointCount(oldId)
                                                    > heldCount);
            succeeds(container.savepointJob(oldId));
            awaitStatus(container, oldId, "SAVEPOINT_DONE");
            succeeds(original.get(60, TimeUnit.SECONDS));

            Future<Container.ExecResult> restored =
                    jobs.submit(() -> container.restoreJobWithCheckpoint(resource, oldId, newId));
            Awaitility.await()
                    .atMost(90, TimeUnit.SECONDS)
                    .until(
                            () ->
                                    "RUNNING".equals(container.getJobStatus(newId))
                                            && container.getCompletedCheckpointCount(newId) > 0);
            Assertions.assertEquals(1, state().path("requests").size());
            succeeds(container.cancelJob(newId));
            awaitStatus(container, newId, "CANCELED");
            succeeds(restored.get(60, TimeUnit.SECONDS));
            Assertions.assertEquals(array(data("{}\n")), state().path("accepted"));
        } finally {
            try {
                release();
                cancelIfRunning(container, oldId);
                cancelIfRunning(container, newId);
            } finally {
                jobs.shutdownNow();
                Files.deleteIfExists(cfg);
            }
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void cancellationClosesAnActiveBatchWithoutSendingMoreRows(TestContainer container)
            throws Exception {
        reset(step("hold"));
        Path cfg =
                config(
                        options().put("batch_size", 1).put("batch_timeout_ms", 90000),
                        schema("string"),
                        rows(row("INSERT", "{}"), row("INSERT", "{\"later\":true}")),
                        "BATCH");
        String id = jobId();
        ExecutorService jobs = Executors.newSingleThreadExecutor();
        try {
            Future<Container.ExecResult> stream =
                    jobs.submit(() -> container.executeJob(resourceName(cfg), id));
            Awaitility.await()
                    .atMost(90, TimeUnit.SECONDS)
                    .until(() -> state().path("requests").size() == 1);
            succeeds(container.cancelJob(id));
            awaitStatus(container, id, "CANCELED");
            stream.get(60, TimeUnit.SECONDS);
            Awaitility.await()
                    .atMost(15, TimeUnit.SECONDS)
                    .until(() -> state().path("disconnect_ms").isNumber());
            JsonNode state = state();
            Assertions.assertTrue(
                    state.path("disconnect_ms").asLong() < 90000,
                    "The socket must be closed before its natural deadline");
            Assertions.assertEquals(1, state.path("requests").size());
            Assertions.assertEquals(0, state.path("accepted").size());
        } finally {
            try {
                cancelIfRunning(container, id);
                release();
            } finally {
                jobs.shutdownNow();
                Files.deleteIfExists(cfg);
            }
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void zetaFlushSignalSendsBelowThresholdBeforeFirstCheckpoint(TestContainer container)
            throws Exception {
        reset();
        Path cfg =
                config(
                        options(),
                        schema("string"),
                        rows(row("INSERT", "{}")),
                        "STREAMING\n  checkpoint.interval = 120000\n  sink.flush.interval = 200");
        String id = jobId();
        ExecutorService jobs = Executors.newSingleThreadExecutor();
        try {
            Future<Container.ExecResult> stream =
                    jobs.submit(() -> container.executeJob(resourceName(cfg), id));
            Awaitility.await()
                    .atMost(90, TimeUnit.SECONDS)
                    .until(() -> state().path("accepted").size() == 1);
            Assertions.assertEquals(0L, container.getCompletedCheckpointCount(id));
            assertBatch(state(), 0, false, data("{}\n"));
            succeeds(container.cancelJob(id));
            awaitStatus(container, id, "CANCELED");
            succeeds(stream.get(60, TimeUnit.SECONDS));
            Assertions.assertEquals(1, state().path("requests").size());
        } finally {
            try {
                cancelIfRunning(container, id);
            } finally {
                jobs.shutdownNow();
                Files.deleteIfExists(cfg);
            }
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void signedSessionCredentialsStayOutOfDebugLogs(TestContainer container) throws Exception {
        GenericContainer<?> normalFixture = fixture;
        try (Network separate = Network.newNetwork();
                GenericContainer<?> debugFixture = startFixture(separate)) {
            fixture = debugFixture;
            DebugZeta debug = new DebugZeta();
            try {
                debug.startUp(separate);
                reset(step("status").put("status", 403));
                String before = debug.getServerLogs();
                fails(
                        debug,
                        options().put("token", TOKEN),
                        "string",
                        rows(row("INSERT", "{}")),
                        "unexpected HTTP 403");
                String after = debug.getServerLogs();
                Assertions.assertTrue(after.startsWith(before));
                String fresh = after.substring(before.length());
                Assertions.assertTrue(
                        fresh.contains("Sending Request:"), "SDK DEBUG request log was not active");
                assertNoSensitiveOutput(fresh);
                assertBatch(state(), 0, true, data("{}\n"));
            } finally {
                debug.stopIsolatedServer();
            }
        } finally {
            fixture = normalFixture;
        }
    }

    /**
     * Starts a separate Zeta server with DEBUG configured before startup to verify privacy
     * diagnostics. Its network, provider fixture and logs stay isolated from the ordinary
     * integration container.
     */
    private static final class DebugZeta extends SeaTunnelContainer {
        private void stopIsolatedServer() {
            if (server != null) {
                server.close();
            }
        }

        @Override
        protected void executeExtraCommands(GenericContainer<?> server)
                throws IOException, InterruptedException {
            super.executeExtraCommands(server);
            server.withCopyFileToContainer(
                    MountableFile.forHostPath(
                            ContainerUtil.getResourcesFile("/firehose-debug-log4j2.properties")
                                    .toPath()),
                    SEATUNNEL_HOME + "config/log4j2.properties");
        }
    }

    private static ObjectNode nativeJsonOptions() {
        ObjectNode options = options();
        options.remove("format");
        return options;
    }

    private static void awaitStatus(TestContainer container, String id, String expected) {
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .until(() -> expected.equals(container.getJobStatus(id)));
    }

    private static void cancelIfRunning(TestContainer container, String id) throws Exception {
        String status = container.getJobStatus(id);
        if ("RUNNING".equals(status) || "SCHEDULED".equals(status)) {
            container.cancelJob(id);
        }
    }

    private static String jobId() {
        return Long.toString(JobIdGenerator.newJobId());
    }

    private void assertBatch(JsonNode state, int index, boolean session, String... records) {
        JsonNode batch = state.path("requests").get(index);
        Assertions.assertNotNull(batch);
        Assertions.assertTrue(
                batch.path("verified").asBoolean(), "Fixture rejected SigV4 signature");
        Assertions.assertEquals(session, batch.path("session").asBoolean());
        Assertions.assertEquals(TARGET, batch.path("target").asText());
        Assertions.assertEquals(STREAM, batch.path("stream").asText());
        Assertions.assertEquals("application/x-amz-json-1.1", batch.path("content_type").asText());
        Assertions.assertTrue(batch.path("size").asInt() <= 4 * 1024 * 1024);
        Assertions.assertEquals(array(records), batch.path("records"));
    }

    private Container.ExecResult run(
            TestContainer container, ObjectNode options, String type, ArrayNode rows)
            throws Exception {
        return run(container, options, schema(type), rows);
    }

    private Container.ExecResult run(
            TestContainer container, ObjectNode options, ObjectNode schema, ArrayNode rows)
            throws Exception {
        Path file = config(options, schema, rows, "BATCH");
        try {
            return container.executeJob(resourceName(file));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    private void fails(
            TestContainer container,
            ObjectNode options,
            String type,
            ArrayNode rows,
            String message)
            throws Exception {
        String before = container.getServerLogs();
        Container.ExecResult result = run(container, options, type, rows);
        String after = container.getServerLogs();
        String logs =
                result.getStdout()
                        + result.getStderr()
                        + (after.startsWith(before) ? after.substring(before.length()) : "");
        Assertions.assertNotEquals(0, result.getExitCode());
        Assertions.assertTrue(
                logs.contains(message), () -> "Missing " + message + " in fresh logs\n" + logs);
        assertNoSensitiveOutput(logs);
    }

    private static void assertNoSensitiveOutput(String logs) {
        Assertions.assertFalse(logs.contains(SECRET));
        Assertions.assertFalse(logs.contains(ACCESS));
        Assertions.assertFalse(logs.contains(TOKEN));
        Assertions.assertFalse(logs.contains(PROVIDER_MARKER));
        Assertions.assertFalse(logs.contains("SignedHeaders="));
    }

    private static void succeeds(Container.ExecResult result) {
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
    }

    private static String resourceName(Path file) {
        return "/" + file.getFileName();
    }

    private static Path config(ObjectNode sink, ObjectNode fields, ArrayNode rows, String jobMode)
            throws IOException {
        ObjectNode schema = JSON.createObjectNode().set("fields", fields);
        String content =
                "env {\n  parallelism = 1\n  job.retry.times = 0\n  job.mode = "
                        + jobMode
                        + "\n}\n"
                        + "source {\n  FakeSource {\n    schema = "
                        + schema
                        + "\n    row.num = "
                        + rows.size()
                        + "\n    rows = "
                        + rows
                        + "\n  }\n}\n"
                        + "sink {\n  AmazonDataFirehose "
                        + sink
                        + "\n}\n";
        Path file =
                Files.createTempFile(
                        ContainerUtil.getCurrentModulePath().resolve("src/test/resources"),
                        "firehose-e2e-",
                        ".conf");
        Files.write(file, content.getBytes(StandardCharsets.UTF_8));
        return file;
    }

    private static ObjectNode schema(String type) {
        return JSON.createObjectNode().put("payload", type);
    }

    private static ObjectNode options() {
        return JSON.createObjectNode()
                .put("delivery_stream", STREAM)
                .put("region", "us-east-1")
                .put("endpoint_url", ENDPOINT)
                .put("allow_insecure_http", true)
                .put("access_key", ACCESS)
                .put("secret_key", SECRET)
                .put("format", "JSON_LINES");
    }

    private static ArrayNode rows(ObjectNode... nodes) {
        ArrayNode array = JSON.createArrayNode();
        for (ObjectNode node : nodes) {
            array.add(node);
        }
        return array;
    }

    private static ObjectNode row(String kind, Object... payload) {
        ObjectNode row = JSON.createObjectNode().put("kind", kind);
        ArrayNode fields = row.putArray("fields");
        for (Object value : payload) {
            fields.add(JSON.<JsonNode>valueToTree(value));
        }
        return row;
    }

    private static ArrayNode array(String... records) {
        ArrayNode result = JSON.createArrayNode();
        for (String record : records) {
            result.add(record);
        }
        return result;
    }

    private static String data(String value) {
        return data(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String data(byte[] bytes) {
        return Base64.getEncoder().encodeToString(bytes);
    }

    private static ObjectNode failures(int index, String code) {
        ObjectNode spec = step("ok");
        spec.putObject("failures").put(Integer.toString(index), code);
        return spec;
    }

    private static ObjectNode failures(int first, String firstCode, int second, String secondCode) {
        ObjectNode spec = failures(first, firstCode);
        ((ObjectNode) spec.path("failures")).put(Integer.toString(second), secondCode);
        return spec;
    }

    private static ObjectNode step(String kind) {
        return JSON.createObjectNode().put("kind", kind);
    }

    private void reset(ObjectNode... steps) throws IOException {
        fixtureRequest("/reset", rows(steps).toString());
    }

    private void release() throws IOException {
        fixtureRequest("/release", "");
    }

    private JsonNode state() throws IOException {
        return fixtureRequest("/state", null);
    }

    private JsonNode fixtureRequest(String path, String data) throws IOException {
        URL url = new URL("http", fixture.getHost(), fixture.getMappedPort(8080), path);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setConnectTimeout(5000);
        connection.setReadTimeout(5000);
        try {
            if (data != null) {
                connection.setRequestMethod("POST");
                connection.setDoOutput(true);
                try (OutputStream out = connection.getOutputStream()) {
                    out.write(data.getBytes(StandardCharsets.UTF_8));
                }
                Assertions.assertEquals(204, connection.getResponseCode());
                return null;
            }
            try (InputStream response = connection.getInputStream()) {
                return JSON.readTree(response);
            }
        } finally {
            connection.disconnect();
        }
    }
}
