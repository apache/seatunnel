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

package org.apache.seatunnel.e2e.connector.http;

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
import java.util.Base64;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Execution(ExecutionMode.SAME_THREAD)
class JiraSinkIT extends TestSuiteBase implements TestResource {
    private static final String ORIGIN = "http://jira-sink-fixture:8080";
    private static final String ISSUE = "/rest/api/3/issue";
    private static final String USERNAME = "fixture-王@example.test";
    private static final String TOKEN = "fixture-token-δ:791";
    private static final String ERROR_BODY_MARKER = "fixture-error-body-912";
    private static final String CREATE_FIELDS =
            "{\"project\":{\"key\":\"DEMO\"},\"issuetype\":{\"id\":\"10000\"},"
                    + "\"summary\":\"同步 🐋\",\"description\":{\"type\":\"doc\",\"version\":1,"
                    + "\"content\":[{\"type\":\"paragraph\",\"content\":[{\"type\":\"text\",\"text\":\"你好\"}]}]},"
                    + "\"assignee\":null,\"labels\":[\"测试\",\"sea\"],"
                    + "\"customfield_10010\":0.123456789012345678901234567890123456789,"
                    + "\"customfield_10011\":9223372036854775809}";
    private static final String NEXT_FIELDS =
            "{\"summary\":\"next\",\"project\":{\"id\":\"1\"},\"issuetype\":{\"name\":\"Task\"}}";
    private static final ObjectMapper JSON =
            new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    private GenericContainer<?> fixture;

    @BeforeAll
    @Override
    public void startUp() {
        fixture =
                new GenericContainer<>(DockerImageName.parse("python:3.11-alpine"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("jira-sink-fixture")
                        .withExposedPorts(8080)
                        .withCopyFileToContainer(
                                MountableFile.forHostPath(
                                        ContainerUtil.getResourcesFile("/jira_sink_fixture.py")
                                                .toPath()),
                                "/jira_sink_fixture.py")
                        .withCommand("python", "/jira_sink_fixture.py")
                        .waitingFor(Wait.forHttp("/_health").forStatusCode(200));
        fixture.start();
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (fixture != null) {
            fixture.close();
        }
    }

    @TestTemplate
    void writesExactIssuesAndCompletesEveryMutation(TestContainer container) throws Exception {
        reset(step("gzip"), step("deflate"));
        succeeds(
                container,
                options("CREATE"),
                rows(row("INSERT", CREATE_FIELDS), row("INSERT", NEXT_FIELDS)));
        JsonNode created = state();
        Assertions.assertEquals(2, created.path("issues").size());
        Assertions.assertEquals(2, created.path("requests").size());
        assertRequest(created, 0, "POST", ISSUE, CREATE_FIELDS);
        assertRequest(created, 1, "POST", ISSUE, NEXT_FIELDS);
        JsonNode fields = created.at("/issues/DEMO-100/fields");
        Assertions.assertEquals("同步 🐋", fields.path("summary").asText());
        Assertions.assertEquals("你好", fields.at("/description/content/0/content/0/text").asText());
        Assertions.assertTrue(fields.path("assignee").isNull());
        Assertions.assertEquals("[\"测试\",\"sea\"]", fields.path("labels").toString());
        Assertions.assertEquals(
                "0.123456789012345678901234567890123456789",
                fields.path("customfield_10010").asText());
        Assertions.assertEquals("9223372036854775809", fields.path("customfield_10011").asText());
        Assertions.assertEquals("100", created.at("/issues/DEMO-100/id").asText());

        String firstEdit = "{\"summary\":\"edited\",\"labels\":[]}";
        String finalEdit = "{\"summary\":\"更新\",\"description\":null}";
        succeeds(
                container,
                options("UPDATE"),
                rows(row("INSERT", "DEMO-100", firstEdit), row("INSERT", "100", finalEdit)));
        JsonNode updated = state();
        assertRequest(updated, 2, "PUT", ISSUE + "/DEMO-100", firstEdit);
        assertRequest(updated, 3, "PUT", ISSUE + "/100", finalEdit);
        Assertions.assertEquals(4, updated.path("requests").size());
        Assertions.assertEquals("更新", updated.at("/issues/DEMO-100/fields/summary").asText());
        Assertions.assertTrue(updated.at("/issues/DEMO-100/fields/description").isNull());
        Assertions.assertTrue(updated.at("/issues/DEMO-100/fields/labels").isArray());
        Assertions.assertEquals(0, updated.at("/issues/DEMO-100/fields/labels").size());
        Assertions.assertEquals("next", updated.at("/issues/DEMO-101/fields/summary").asText());

        succeeds(container, options("DELETE"), rows(row("INSERT", "DEMO-100")));
        JsonNode deleted = state();
        assertRequest(deleted, 4, "DELETE", ISSUE + "/DEMO-100?deleteSubtasks=false", null);
        Assertions.assertEquals(5, deleted.path("requests").size());
        Assertions.assertTrue(deleted.at("/issues/DEMO-100").isMissingNode());
        Assertions.assertEquals(1, deleted.path("issues").size());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void emptyInputAndInvalidCommandsNeverReachJira(TestContainer container) throws Exception {
        reset();
        succeeds(container, options("CREATE"), rows());
        fails(
                container,
                options("UPDATE"),
                rows(row("INSERT", "../DEMO-100", "{\"summary\":\"x\"}")),
                "requires issue_id_or_key to be a Jira issue ID or key");
        fails(
                container,
                options("CREATE"),
                rows(row("INSERT", CREATE_FIELDS + " {}")),
                "fields must be valid JSON");
        fails(
                container,
                options("CREATE"),
                rows(row("INSERT", "{\"summary\":\"x\",\"summary\":\"y\"}")),
                "fields must be valid JSON");
        fails(
                container,
                options("CREATE"),
                rows(row("DELETE", CREATE_FIELDS)),
                "requires INSERT command rows");
        fails(
                container,
                options("CREATE").put("base_url", ORIGIN + "/rest/api/3"),
                rows(row("INSERT", CREATE_FIELDS)),
                "base_url must be a trusted HTTPS origin");
        Assertions.assertEquals(0, state().path("requests").size());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void permissionAndPermanentErrorsFailWithoutDisclosureOrRedirect(TestContainer container)
            throws Exception {
        reset();
        fails(
                container,
                options("CREATE").put("token", TOKEN + "-wrong"),
                rows(row("INSERT", CREATE_FIELDS)),
                "rejected with HTTP 401");
        Assertions.assertFalse(state().path("requests").get(0).path("authorized").asBoolean());

        reset(step("ok"), status(403), status(400), step("redirect"));
        succeeds(container, options("CREATE"), rows(row("INSERT", CREATE_FIELDS)));
        fails(
                container,
                options("DELETE"),
                rows(row("INSERT", "DEMO-100")),
                "rejected with HTTP 403");
        fails(
                container,
                options("UPDATE"),
                rows(row("INSERT", "DEMO-100", "{\"summary\":\"invalid\"}")),
                "rejected with HTTP 400");
        fails(
                container,
                options("CREATE"),
                rows(row("INSERT", CREATE_FIELDS)),
                "unexpected HTTP status 307");
        JsonNode state = state();
        Assertions.assertEquals(4, state.path("requests").size());
        Assertions.assertEquals(0, state.path("redirects").asInt());
        Assertions.assertEquals(1, state.path("issues").size());
        Assertions.assertEquals("同步 🐋", state.at("/issues/DEMO-100/fields/summary").asText());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void retriesOnlyExplicitUpdateThrottlingWithinRateAndDeadlineBounds(TestContainer container)
            throws Exception {
        reset(
                step("ok"),
                status(429).put("retry", "1"),
                step("ok"),
                status(429).put("retry", "0"),
                status(429).put("retry", "0"),
                status(429).put("retry", "120"),
                status(429),
                status(429).put("retry", "0"));
        succeeds(container, options("CREATE"), rows(row("INSERT", CREATE_FIELDS)));
        String accepted = "{\"summary\":\"accepted after throttling\"}";
        succeeds(container, options("UPDATE"), rows(row("INSERT", "DEMO-100", accepted)));
        JsonNode state = state();
        assertRequest(state, 1, "PUT", ISSUE + "/DEMO-100", accepted);
        assertRequest(state, 2, "PUT", ISSUE + "/DEMO-100", accepted);
        long wait =
                state.at("/requests/2/time_ns").asLong() - state.at("/requests/1/time_ns").asLong();
        Assertions.assertTrue(wait >= TimeUnit.SECONDS.toNanos(1), "Retried before Retry-After");

        ArrayNode update = rows(row("INSERT", "DEMO-100", "{\"summary\":\"must not apply\"}"));
        fails(
                container,
                options("UPDATE").put("max_retries", 1),
                update,
                "maximum retries exhausted");
        Assertions.assertEquals(5, state().path("requests").size());
        fails(
                container,
                options("UPDATE").put("request_timeout_ms", 5000),
                update,
                "Retry-After exceeds the remaining request deadline");
        Assertions.assertEquals(6, state().path("requests").size());
        fails(container, options("UPDATE"), update, "a valid Retry-After header is required");
        Assertions.assertEquals(7, state().path("requests").size());
        fails(
                container,
                options("CREATE"),
                rows(row("INSERT", NEXT_FIELDS)),
                "rejected with HTTP 429");
        Assertions.assertEquals(8, state().path("requests").size());
        Assertions.assertEquals(
                "accepted after throttling",
                state().at("/issues/DEMO-100/fields/summary").asText());
        Assertions.assertEquals(1, state().path("issues").size());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void ambiguousCreateOutcomesNeverReplay(TestContainer container) throws Exception {
        reset(
                step("ack_override").put("body", "{"),
                step("ack_override").put("body", "{\"id\":\"101\"}"),
                step("ack_override")
                        .put(
                                "body",
                                "{\"id\":\"102\",\"key\":\"DEMO-102\",\"errors\":{\"summary\":\""
                                        + ERROR_BODY_MARKER
                                        + "\"}}"),
                step("disconnect"));
        ArrayNode create = rows(row("INSERT", CREATE_FIELDS), row("INSERT", NEXT_FIELDS));
        fails(container, options("CREATE"), create, "malformed CREATE acknowledgement");
        Assertions.assertEquals(1, state().path("requests").size());
        fails(
                container,
                options("CREATE"),
                create,
                "invalid or contradictory CREATE acknowledgement");
        Assertions.assertEquals(2, state().path("requests").size());
        fails(
                container,
                options("CREATE"),
                create,
                "invalid or contradictory CREATE acknowledgement");
        Assertions.assertEquals(3, state().path("requests").size());
        fails(container, options("CREATE"), create, "mutation outcome may be unknown");
        JsonNode applied = state();
        Assertions.assertEquals(4, applied.path("requests").size());
        Assertions.assertEquals(4, applied.path("issues").size());
        assertRequest(applied, 3, "POST", ISSUE, CREATE_FIELDS);
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void ambiguousUpdateStopsSameIssueOrderingAndContradictoryDeletesFail(TestContainer container)
            throws Exception {
        reset(step("ok"), step("pending_503"), step("ok"), step("contradictory_204"));
        succeeds(container, options("CREATE"), rows(row("INSERT", CREATE_FIELDS)));
        String first = "{\"summary\":\"delayed edit\"}";
        String later = "{\"summary\":\"newer edit\"}";
        fails(
                container,
                options("UPDATE"),
                rows(row("INSERT", "DEMO-100", first), row("INSERT", "DEMO-100", later)),
                "unexpected HTTP status 503");
        Assertions.assertEquals(2, state().path("requests").size());
        Assertions.assertEquals("同步 🐋", state().at("/issues/DEMO-100/fields/summary").asText());
        release();
        Assertions.assertEquals(
                "delayed edit", state().at("/issues/DEMO-100/fields/summary").asText());
        succeeds(container, options("UPDATE"), rows(row("INSERT", "DEMO-100", later)));
        Assertions.assertEquals(
                "newer edit", state().at("/issues/DEMO-100/fields/summary").asText());
        fails(
                container,
                options("DELETE"),
                rows(row("INSERT", "100")),
                "expected a bodyless HTTP 204 acknowledgement");
        JsonNode state = state();
        Assertions.assertEquals(4, state.path("requests").size());
        Assertions.assertEquals(0, state.path("issues").size());
        assertRequest(state, 3, "DELETE", ISSUE + "/100?deleteSubtasks=false", null);
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void stalledPeerIsDisconnectedWithinItsRequestBudget(TestContainer container) throws Exception {
        reset(step("hold"));
        fails(
                container,
                options("CREATE").put("request_timeout_ms", 1500),
                rows(row("INSERT", CREATE_FIELDS)),
                "mutation outcome may be unknown");
        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .until(() -> !state().path("disconnect_ms").isNull());
        JsonNode stopped = state();
        long elapsed = stopped.path("disconnect_ms").asLong();
        Assertions.assertTrue(
                elapsed >= 0 && elapsed < 5000,
                "Fixture observed socket closure at " + elapsed + " ms");
        Assertions.assertEquals(1, stopped.path("requests").size());
        Assertions.assertEquals(0, stopped.path("issues").size());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void checkpointAfterAcknowledgementRestoresWithoutReissuingTheCommand(TestContainer container)
            throws Exception {
        reset(step("hold"));
        Path config =
                writeConfig(
                        options("CREATE").put("request_timeout_ms", 60000),
                        rows(row("INSERT", CREATE_FIELDS)),
                        true);
        String name = resourceName(config);
        String jobId = id();
        String restoredId = id();
        ExecutorService jobs = Executors.newSingleThreadExecutor();
        try {
            Future<Container.ExecResult> original =
                    jobs.submit(() -> container.executeJob(name, jobId));
            Awaitility.await()
                    .atMost(90, TimeUnit.SECONDS)
                    .until(() -> state().path("requests").size() == 1);
            Assertions.assertEquals(0, state().path("issues").size());
            long heldBaseline = container.getCompletedCheckpointCount(jobId);
            release();
            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .until(
                            () ->
                                    state().path("issues").size() == 1
                                            && container.getCompletedCheckpointCount(jobId)
                                                    > heldBaseline);
            Assertions.assertEquals(0, container.savepointJob(jobId).getExitCode());
            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .until(() -> "SAVEPOINT_DONE".equals(container.getJobStatus(jobId)));
            Assertions.assertEquals(0, original.get(60, TimeUnit.SECONDS).getExitCode());

            Future<Container.ExecResult> restored =
                    jobs.submit(() -> container.restoreJobWithCheckpoint(name, jobId, restoredId));
            Awaitility.await()
                    .atMost(90, TimeUnit.SECONDS)
                    .until(
                            () ->
                                    "RUNNING".equals(container.getJobStatus(restoredId))
                                            && container.getCompletedCheckpointCount(restoredId)
                                                    > 0);
            Assertions.assertEquals(1, state().path("requests").size());
            Assertions.assertEquals(1, state().path("issues").size());
            Assertions.assertEquals(0, container.cancelJob(restoredId).getExitCode());
            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .until(() -> "CANCELED".equals(container.getJobStatus(restoredId)));
            Assertions.assertEquals(0, restored.get(60, TimeUnit.SECONDS).getExitCode());
            Assertions.assertEquals(1, state().path("requests").size());
        } finally {
            release();
            stopIfRunning(container, restoredId);
            stopIfRunning(container, jobId);
            jobs.shutdownNow();
            Files.deleteIfExists(config);
        }
    }

    private void assertRequest(JsonNode state, int index, String method, String path, String fields)
            throws IOException {
        JsonNode request = state.path("requests").get(index);
        Assertions.assertNotNull(request);
        Assertions.assertTrue(request.path("authorized").asBoolean());
        Assertions.assertEquals(method, request.path("method").asText());
        Assertions.assertEquals(path, request.path("path").asText());
        String body = request.path("body").asText();
        if (fields == null) {
            Assertions.assertTrue(body.isEmpty());
        } else {
            Assertions.assertEquals(
                    JSON.readTree("{\"fields\":" + fields + "}"), JSON.readTree(body));
        }
    }

    private void succeeds(TestContainer container, ObjectNode sink, ArrayNode rows)
            throws Exception {
        Container.ExecResult result = run(container, sink, rows);
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
    }

    private void fails(TestContainer container, ObjectNode sink, ArrayNode rows, String reason)
            throws Exception {
        String before = container.getServerLogs();
        Container.ExecResult result = run(container, sink, rows);
        String after = container.getServerLogs();
        String logs =
                result.getStdout()
                        + result.getStderr()
                        + (after.startsWith(before) ? after.substring(before.length()) : "");
        Assertions.assertNotEquals(0, result.getExitCode(), "Job unexpectedly succeeded");
        Assertions.assertTrue(
                logs.contains(reason),
                () -> "Missing " + reason + " in this job's diagnostics\n" + logs);
        Assertions.assertFalse(logs.contains(ERROR_BODY_MARKER));
        Assertions.assertFalse(logs.contains(TOKEN));
        Assertions.assertFalse(logs.contains(USERNAME));
        Assertions.assertFalse(
                logs.contains(
                        Base64.getEncoder()
                                .encodeToString(
                                        (USERNAME + ":" + sink.path("token").asText())
                                                .getBytes(StandardCharsets.UTF_8))));
    }

    private Container.ExecResult run(TestContainer container, ObjectNode sink, ArrayNode rows)
            throws Exception {
        Path config = writeConfig(sink, rows, false);
        try {
            return container.executeJob(resourceName(config));
        } finally {
            Files.deleteIfExists(config);
        }
    }

    private Path writeConfig(ObjectNode sink, ArrayNode rows, boolean streaming)
            throws IOException {
        ObjectNode schema = JSON.createObjectNode();
        ObjectNode fields = schema.putObject("fields");
        String operation = sink.path("operation").asText();
        if (!"CREATE".equals(operation)) {
            fields.put("issue_id_or_key", "string");
        }
        if (!"DELETE".equals(operation)) {
            fields.put("fields", "string");
        }
        String configuration =
                "env {\n  parallelism = 1\n  job.retry.times = 0\n  job.mode = "
                        + (streaming ? "STREAMING\n  checkpoint.interval = 1000" : "BATCH")
                        + "\n}\n"
                        + "source {\n  FakeSource {\n    schema = "
                        + schema
                        + "\n    row.num = "
                        + rows.size()
                        + "\n    rows = "
                        + rows
                        + "\n  }\n}\n"
                        + "sink {\n  Jira "
                        + sink
                        + "\n}\n";
        Path config =
                Files.createTempFile(
                        ContainerUtil.getCurrentModulePath().resolve("src/test/resources"),
                        "jira-sink-e2e-",
                        ".conf");
        Files.write(config, configuration.getBytes(StandardCharsets.UTF_8));
        return config;
    }

    private static String resourceName(Path path) {
        return "/" + path.getFileName();
    }

    private static String id() {
        return Long.toString(JobIdGenerator.newJobId());
    }

    private static void stopIfRunning(TestContainer container, String jobId) throws Exception {
        String status = container.getJobStatus(jobId);
        if ("RUNNING".equals(status) || "SCHEDULED".equals(status)) {
            container.cancelJob(jobId);
        }
    }

    private static ObjectNode options(String operation) {
        return JSON.createObjectNode()
                .put("base_url", ORIGIN)
                .put("username", USERNAME)
                .put("token", TOKEN)
                .put("operation", operation)
                .put("allow_insecure_http", true);
    }

    private static ArrayNode rows(ObjectNode... rows) {
        ArrayNode array = JSON.createArrayNode();
        for (ObjectNode row : rows) {
            array.add(row);
        }
        return array;
    }

    private static ObjectNode row(String kind, String... fields) {
        ObjectNode row = JSON.createObjectNode().put("kind", kind);
        ArrayNode values = row.putArray("fields");
        for (String field : fields) {
            values.add(field);
        }
        return row;
    }

    private static ObjectNode status(int code) {
        return JSON.createObjectNode().put("status", code);
    }

    private static ObjectNode step(String kind) {
        return JSON.createObjectNode().put("kind", kind);
    }

    private void reset(ObjectNode... steps) throws IOException {
        fixtureRequest("/_reset", rows(steps).toString());
    }

    private void release() throws IOException {
        fixtureRequest("/_release", "");
    }

    private JsonNode state() throws IOException {
        return fixtureRequest("/_state", null);
    }

    private JsonNode fixtureRequest(String path, String data) throws IOException {
        URL endpoint = new URL("http", fixture.getHost(), fixture.getMappedPort(8080), path);
        HttpURLConnection connection = (HttpURLConnection) endpoint.openConnection();
        connection.setConnectTimeout(5000);
        connection.setReadTimeout(5000);
        try {
            if (data != null) {
                connection.setRequestMethod("POST");
                connection.setDoOutput(true);
                try (OutputStream output = connection.getOutputStream()) {
                    output.write(data.getBytes(StandardCharsets.UTF_8));
                }
                Assertions.assertEquals(204, connection.getResponseCode());
                return null;
            }
            try (InputStream input = connection.getInputStream()) {
                return JSON.readTree(input);
            }
        } finally {
            connection.disconnect();
        }
    }
}
