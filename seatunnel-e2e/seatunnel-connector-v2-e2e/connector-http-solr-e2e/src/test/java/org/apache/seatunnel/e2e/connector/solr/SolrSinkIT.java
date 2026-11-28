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

package org.apache.seatunnel.e2e.connector.solr;

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
import java.net.URLEncoder;
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
class SolrSinkIT extends TestSuiteBase implements TestResource {
    private static final String SOLR = "http://solr-fixture:8983";
    private static final String PROXY = "http://solr-proxy:8080";
    private static final String CORE = "/solr/records";
    private static final String USER = "fixture-solr-admin";
    private static final String PASSWORD = "fixture-password:912";
    private static final ObjectMapper JSON = new ObjectMapper();

    private GenericContainer<?> solr;
    private GenericContainer<?> proxy;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        solr =
                new GenericContainer<>(DockerImageName.parse("solr:9.8.1"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("solr-fixture")
                        .withExposedPorts(8983)
                        .withCommand("solr-precreate", "records")
                        .waitingFor(
                                Wait.forHttp(CORE + "/schema/uniquekey?wt=json")
                                        .forStatusCode(200));
        solr.start();
        addField("title_s", "string", false);
        addField("tags_ss", "string", true);
        addField("hits_l", "plong", false);
        addField("decimal_s", "string", false);
        addField("active_b", "boolean", false);

        Path tls = Files.createTempDirectory("seatunnel-solr-tls-");
        try {
            createFixtureCertificate(tls);
            proxy =
                    new GenericContainer<>(DockerImageName.parse("python:3.11-alpine"))
                            .withNetwork(NETWORK)
                            .withNetworkAliases("solr-proxy")
                            .withExposedPorts(8080)
                            .withCopyFileToContainer(
                                    MountableFile.forHostPath(
                                            ContainerUtil.getResourcesFile("/solr_fault_proxy.py")
                                                    .toPath()),
                                    "/solr_fault_proxy.py")
                            .withCopyFileToContainer(
                                    MountableFile.forHostPath(tls.resolve("cert.pem")),
                                    "/fixture-cert.pem")
                            .withCopyFileToContainer(
                                    MountableFile.forHostPath(tls.resolve("key.pem")),
                                    "/fixture-key.pem")
                            .withCommand("python", "/solr_fault_proxy.py")
                            .waitingFor(Wait.forHttp("/health").forStatusCode(200));
            proxy.start();
        } finally {
            Files.deleteIfExists(tls.resolve("cert.pem"));
            Files.deleteIfExists(tls.resolve("key.pem"));
            Files.deleteIfExists(tls.resolve("fixture.p12"));
            Files.deleteIfExists(tls);
        }
    }

    @AfterAll
    @Override
    public void tearDown() {
        try {
            if (proxy != null) {
                proxy.close();
            }
        } finally {
            if (solr != null) {
                solr.close();
            }
        }
    }

    @TestTemplate
    void indexesMappedTypedValuesAndOmittedNulls(TestContainer container) throws Exception {
        ObjectNode schema =
                JSON.createObjectNode()
                        .put("key", "string")
                        .put("title", "string")
                        .put("tags_ss", "array<string>")
                        .put("hits_l", "bigint")
                        .put("decimal_s", "decimal(30,18)")
                        .put("active_b", "boolean");
        ObjectNode options = options(SOLR).put("batch_size", 1);
        options.putObject("field_mapping").put("key", "id").put("title", "title_s");
        String exact = "123456789.123456789123456789";
        succeeds(
                run(
                        container,
                        options,
                        schema,
                        rows(
                                row(
                                        "INSERT",
                                        "typed-日本",
                                        "数据 🐋",
                                        Arrays.asList("甲", "café"),
                                        9007199254740993L,
                                        exact,
                                        true),
                                row(
                                        "INSERT",
                                        "typed-null",
                                        null,
                                        Arrays.asList("保留", null),
                                        -10,
                                        null,
                                        false))));
        JsonNode doc = document("typed-日本");
        Assertions.assertEquals("数据 🐋", doc.path("title_s").asText());
        Assertions.assertEquals(JSON.valueToTree(Arrays.asList("甲", "café")), doc.path("tags_ss"));
        Assertions.assertEquals(9007199254740993L, doc.path("hits_l").asLong());
        Assertions.assertEquals(exact, doc.path("decimal_s").asText());
        Assertions.assertTrue(doc.path("active_b").asBoolean());
        JsonNode nulls = document("typed-null");
        Assertions.assertFalse(nulls.has("title_s"));
        Assertions.assertFalse(nulls.has("decimal_s"));
        Assertions.assertEquals(JSON.valueToTree(Arrays.asList("保留")), nulls.path("tags_ss"));
        Assertions.assertEquals(-10, nulls.path("hits_l").asLong());
        Assertions.assertTrue(nulls.path("active_b").isBoolean());
        Assertions.assertFalse(nulls.path("active_b").asBoolean());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void replacesAndDeletesOnlyExplicitKeysInOrder(TestContainer container) throws Exception {
        ObjectNode schema =
                JSON.createObjectNode()
                        .put("id", "string")
                        .put("title_s", "string")
                        .put("hits_l", "bigint");
        succeeds(
                run(
                        container,
                        options(SOLR),
                        schema,
                        rows(
                                row("INSERT", "replace", "original", 14),
                                row("INSERT", "delete", "original", 20),
                                row("INSERT", "unaffected", "unchanged", 40))));
        reset(faults(step("gzip"), step("deflate"), step("forward")));
        succeeds(
                run(
                        container,
                        options(PROXY).put("enable_delete", true),
                        schema,
                        rows(
                                row("UPDATE_AFTER", "replace", "replaced", null),
                                row("DELETE", "delete", null, null),
                                row("INSERT", "late", "after deletion", 9))));
        JsonNode replaced = document("replace");
        Assertions.assertEquals("replaced", replaced.path("title_s").asText());
        Assertions.assertFalse(
                replaced.has("hits_l"), "Full replacement should remove the old value");
        Assertions.assertNull(document("delete"));
        Assertions.assertEquals("unchanged", document("unaffected").path("title_s").asText());
        Assertions.assertEquals(9, document("late").path("hits_l").asInt());
        List<JsonNode> requests = updates();
        Assertions.assertEquals(4, requests.size());
        Assertions.assertEquals("replace", requests.get(0).at("/0/id").asText());
        Assertions.assertEquals(JSON.readTree("{\"delete\":[\"delete\"]}"), requests.get(1));
        Assertions.assertEquals("late", requests.get(2).at("/0/id").asText());
        Assertions.assertEquals(
                JSON.readTree("{\"commit\":{\"softCommit\":false,\"waitSearcher\":true}}"),
                requests.get(3));
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void failsInvalidRowsOrConfigurationBeforeMutation(TestContainer container) throws Exception {
        reset(faults());
        fails(
                container,
                options(PROXY),
                schema(),
                rows(row("DELETE", "unsafe", "x")),
                "or DELETE with enable_delete");
        fails(
                container,
                options(PROXY),
                schema(),
                rows(row("UPDATE_BEFORE", "unsafe", "x")),
                "or DELETE with enable_delete");
        fails(
                container,
                options(PROXY),
                schema(),
                rows(row("INSERT", null, "x")),
                "unique key must be nonempty");
        fails(
                container,
                options(PROXY).put("collection", ".."),
                schema(),
                rows(row("INSERT", "unsafe", "x")),
                "collection must be a valid core");
        ObjectNode duplicate = options(PROXY);
        duplicate.putObject("field_mapping").put("title_s", "id");
        fails(
                container,
                duplicate,
                schema(),
                rows(row("INSERT", "unsafe", "x")),
                "duplicate target field");
        Assertions.assertEquals(0, state().path("calls").size());

        reset(faults().put("key", "aDifferentId"));
        fails(
                container,
                options(PROXY),
                schema(),
                rows(row("INSERT", "unsafe", "x")),
                "configured unique_key does not match");
        Assertions.assertEquals(1, state().path("calls").size());
        Assertions.assertEquals(0, updates().size());
        Assertions.assertNull(document("unsafe"));
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void commitsOnlyOnSelectedBoundariesAndSkipsEmptyInputs(TestContainer container)
            throws Exception {
        reset(faults());
        succeeds(run(container, options(PROXY), schema(), rows()));
        Assertions.assertEquals(0, state().path("calls").size());
        succeeds(
                run(
                        container,
                        options(PROXY).put("commit_mode", "SOFT"),
                        schema(),
                        rows(row("INSERT", "soft", "soft result"))));
        Assertions.assertEquals(2, updates().size());
        Assertions.assertTrue(updates().get(1).at("/commit/softCommit").asBoolean());
        Assertions.assertTrue(updates().get(1).at("/commit/waitSearcher").asBoolean());
        Assertions.assertEquals("soft result", document("soft").path("title_s").asText());

        reset(faults());
        succeeds(
                run(
                        container,
                        options(PROXY).put("commit_mode", "NONE"),
                        schema(),
                        rows(row("INSERT", "none", "no connector commit"))));
        Assertions.assertEquals(1, updates().size());
        hardCommit();
        Assertions.assertEquals("no connector commit", document("none").path("title_s").asText());

        reset(faults(step("forward"), step("status").put("status", 503)));
        fails(
                container,
                options(PROXY),
                schema(),
                rows(row("INSERT", "commit-fail", "index accepted")),
                "commit unexpected HTTP 503");
        Assertions.assertEquals(2, updates().size());
        hardCommit();
        Assertions.assertEquals("index accepted", document("commit-fail").path("title_s").asText());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void partialOrLostAcknowledgementNeverResendsAppliedData(TestContainer container)
            throws Exception {
        reset(faults(step("partial")));
        fails(
                container,
                options(PROXY),
                schema(),
                rows(
                        row("INSERT", "partial-first", "received"),
                        row("INSERT", "partial-second", "rejected")),
                "failed or partial acknowledgement");
        Assertions.assertEquals(1, updates().size());
        hardCommit();
        Assertions.assertEquals("received", document("partial-first").path("title_s").asText());
        Assertions.assertNull(document("partial-second"));

        reset(faults(step("applied_disconnect")));
        fails(
                container,
                options(PROXY).put("batch_size", 1),
                schema(),
                rows(
                        row("INSERT", "unknown-first", "already applied"),
                        row("INSERT", "unknown-second", "never sent")),
                "Solr may have applied some changes");
        Assertions.assertEquals(1, updates().size());
        hardCommit();
        Assertions.assertEquals(
                "already applied", document("unknown-first").path("title_s").asText());
        Assertions.assertNull(document("unknown-second"));
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void realSolrRejectsAnInvalidDocumentWithoutBatchReplay(TestContainer container)
            throws Exception {
        reset(faults());
        ObjectNode schema = schema().put("hits_l", "string");
        fails(
                container,
                options(PROXY),
                schema,
                rows(
                        row("INSERT", "real-first", "valid", "321"),
                        row("INSERT", "real-second", "invalid", "not-a-number")),
                "unexpected HTTP 400");
        Assertions.assertEquals(1, updates().size());
        hardCommit();
        Assertions.assertEquals(321, document("real-first").path("hits_l").asInt());
        Assertions.assertNull(document("real-second"));
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void invalidAcknowledgementsAndRedirectsFailClosed(TestContainer container) throws Exception {
        reset(faults(step("invalid_json"), step("oversize"), step("redirect")));
        ArrayNode data = rows(row("INSERT", "failure-trap", "never stored"));
        for (String reason :
                Arrays.asList(
                        "invalid JSON acknowledgement", "exceeds 256 KiB", "unexpected HTTP 307")) {
            fails(container, options(PROXY), schema(), data, reason);
        }
        Assertions.assertEquals(3, updates().size());
        Assertions.assertEquals(0, state().path("redirect_hits").asInt());
        Assertions.assertNull(document("failure-trap"));
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void usesBasicAuthenticationAndRejectsUntrustedTls(TestContainer container) throws Exception {
        reset(faults().put("auth", true));
        fails(
                container,
                options(PROXY).put("username", USER).put("password", PASSWORD + "-wrong"),
                schema(),
                rows(row("INSERT", "auth", "wrong credential")),
                "schema unexpected HTTP 401");
        Assertions.assertEquals(1, state().path("calls").size());
        Assertions.assertFalse(state().at("/calls/0/verified").asBoolean());
        Assertions.assertEquals(0, updates().size());

        reset(faults().put("auth", true));
        succeeds(
                run(
                        container,
                        options(PROXY).put("username", USER).put("password", PASSWORD),
                        schema(),
                        rows(row("INSERT", "auth", "authorized"))));
        Assertions.assertEquals(3, state().path("calls").size());
        for (JsonNode call : state().path("calls")) {
            Assertions.assertTrue(call.path("verified").asBoolean());
        }
        Assertions.assertEquals("authorized", document("auth").path("title_s").asText());

        reset(faults().put("auth", true));
        fails(
                container,
                options("https://solr-proxy:8443").put("username", USER).put("password", PASSWORD),
                schema(),
                rows(row("INSERT", "tls", "certificate not trusted")),
                "schema TLS transport failed");
        Assertions.assertEquals(0, state().path("calls").size());
        Assertions.assertNull(document("tls"));
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void deadlinesCloseHeldAndTricklingResponses(TestContainer container) throws Exception {
        for (String kind : Arrays.asList("hold", "trickle")) {
            reset(faults(step(kind)));
            fails(
                    container,
                    options(PROXY).put("request_timeout_ms", 1500),
                    schema(),
                    rows(row("INSERT", "deadline-" + kind, "never accepted")),
                    "Solr may have applied some changes");
            Awaitility.await()
                    .atMost(5, TimeUnit.SECONDS)
                    .until(() -> state().path("disconnect_ms").isNumber());
            Assertions.assertTrue(state().path("disconnect_ms").asLong() < 5000);
            Assertions.assertEquals(1, updates().size());
            Assertions.assertNull(document("deadline-" + kind));
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void cancelClosesActiveWriteBeforeItsDeadline(TestContainer container) throws Exception {
        reset(faults(step("hold")));
        Path cfg =
                config(
                        options(PROXY).put("batch_size", 1).put("request_timeout_ms", 90000),
                        schema(),
                        rows(
                                row("INSERT", "cancel-first", "held"),
                                row("INSERT", "cancel-later", "never sent")),
                        "BATCH");
        String id = jobId();
        ExecutorService jobs = Executors.newSingleThreadExecutor();
        try {
            Future<Container.ExecResult> running =
                    jobs.submit(() -> container.executeJob(resourceName(cfg), id));
            Awaitility.await().atMost(90, TimeUnit.SECONDS).until(() -> updates().size() == 1);
            succeeds(container.cancelJob(id));
            awaitStatus(container, id, "CANCELED");
            running.get(60, TimeUnit.SECONDS);
            Awaitility.await()
                    .atMost(15, TimeUnit.SECONDS)
                    .until(() -> state().path("disconnect_ms").isNumber());
            Assertions.assertTrue(state().path("disconnect_ms").asLong() < 90000);
            Assertions.assertEquals(1, updates().size());
            Assertions.assertNull(document("cancel-first"));
            Assertions.assertNull(document("cancel-later"));
        } finally {
            try {
                release();
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
    void checkpointCommitsPendingDocumentsAndRestoreDoesNotReplayThem(TestContainer container)
            throws Exception {
        reset(faults(step("forward"), step("hold")));
        Path cfg =
                config(
                        options(PROXY).put("request_timeout_ms", 60000),
                        schema(),
                        rows(row("INSERT", "checkpoint", "acknowledged")),
                        "STREAMING\n  checkpoint.interval = 1000");
        String oldId = jobId();
        String newId = jobId();
        ExecutorService jobs = Executors.newSingleThreadExecutor();
        try {
            Future<Container.ExecResult> running =
                    jobs.submit(() -> container.executeJob(resourceName(cfg), oldId));
            Awaitility.await().atMost(90, TimeUnit.SECONDS).until(() -> updates().size() == 2);
            Assertions.assertTrue(updates().get(1).has("commit"));
            long heldBaseline = container.getCompletedCheckpointCount(oldId);
            release();
            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .until(() -> container.getCompletedCheckpointCount(oldId) > heldBaseline);
            Assertions.assertEquals(
                    "acknowledged", document("checkpoint").path("title_s").asText());
            succeeds(container.savepointJob(oldId));
            awaitStatus(container, oldId, "SAVEPOINT_DONE");
            succeeds(running.get(60, TimeUnit.SECONDS));

            Future<Container.ExecResult> restored =
                    jobs.submit(
                            () ->
                                    container.restoreJobWithCheckpoint(
                                            resourceName(cfg), oldId, newId));
            Awaitility.await()
                    .atMost(90, TimeUnit.SECONDS)
                    .until(
                            () ->
                                    "RUNNING".equals(container.getJobStatus(newId))
                                            && container.getCompletedCheckpointCount(newId) > 0);
            Assertions.assertEquals(2, updates().size());
            succeeds(container.cancelJob(newId));
            awaitStatus(container, newId, "CANCELED");
            restored.get(60, TimeUnit.SECONDS);
            Assertions.assertEquals(2, updates().size());
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
    void timerCommitsSparseWritesBeforeTheFirstCheckpoint(TestContainer container)
            throws Exception {
        reset(faults());
        Path cfg =
                config(
                        options(PROXY),
                        schema(),
                        rows(row("INSERT", "timer", "visible")),
                        "STREAMING\n  checkpoint.interval = 120000\n  sink.flush.interval = 200");
        String id = jobId();
        ExecutorService jobs = Executors.newSingleThreadExecutor();
        try {
            Future<Container.ExecResult> running =
                    jobs.submit(() -> container.executeJob(resourceName(cfg), id));
            Awaitility.await().atMost(90, TimeUnit.SECONDS).until(() -> document("timer") != null);
            Assertions.assertEquals(0L, container.getCompletedCheckpointCount(id));
            Assertions.assertEquals("visible", document("timer").path("title_s").asText());
            succeeds(container.cancelJob(id));
            awaitStatus(container, id, "CANCELED");
            running.get(60, TimeUnit.SECONDS);
            Assertions.assertEquals(2, updates().size());
            Assertions.assertTrue(updates().get(1).has("commit"));
        } finally {
            try {
                cancelIfRunning(container, id);
            } finally {
                jobs.shutdownNow();
                Files.deleteIfExists(cfg);
            }
        }
    }

    private static void createFixtureCertificate(Path directory) throws Exception {
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
                                "CN=solr-proxy",
                                "-ext",
                                "SAN=dns:solr-proxy",
                                "-noprompt")
                        .redirectOutput(ProcessBuilder.Redirect.INHERIT)
                        .redirectError(ProcessBuilder.Redirect.INHERIT)
                        .start();
        if (!process.waitFor(30, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            throw new IOException("Timed out generating the test TLS certificate");
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
        Base64.Encoder base64 = Base64.getMimeEncoder(64, new byte[] {'\n'});
        String text =
                "-----BEGIN "
                        + label
                        + "-----\n"
                        + base64.encodeToString(value)
                        + "\n-----END "
                        + label
                        + "-----\n";
        Files.write(file, text.getBytes(StandardCharsets.US_ASCII));
    }

    private List<JsonNode> updates() throws IOException {
        List<JsonNode> result = new ArrayList<>();
        for (JsonNode request : state().path("calls")) {
            if ("POST".equals(request.path("method").asText())) {
                result.add(JSON.readTree(request.path("body").asText()));
            }
        }
        return result;
    }

    private void addField(String name, String type, boolean many) throws IOException {
        ObjectNode schema = JSON.createObjectNode();
        schema.putObject("add-field")
                .put("name", name)
                .put("type", type)
                .put("stored", true)
                .put("indexed", true)
                .put("multiValued", many);
        JsonNode result = request(solr, 8983, CORE + "/schema?wt=json", schema.toString(), 200);
        Assertions.assertEquals(0, result.at("/responseHeader/status").asInt(-1));
    }

    private void hardCommit() throws IOException {
        request(solr, 8983, CORE + "/update?wt=json", "{\"commit\":{\"waitSearcher\":true}}", 200);
    }

    private JsonNode document(String id) throws IOException {
        String query = URLEncoder.encode("id:\"" + id + "\"", "UTF-8");
        JsonNode docs =
                request(solr, 8983, CORE + "/select?q=" + query + "&wt=json", null, 200)
                        .path("response")
                        .path("docs");
        Assertions.assertTrue(docs.isArray());
        Assertions.assertTrue(docs.size() <= 1, "Document identity must be unique");
        return docs.get(0);
    }

    private static String jobId() {
        return Long.toString(JobIdGenerator.newJobId());
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

    private Container.ExecResult run(
            TestContainer container, ObjectNode sink, ObjectNode fields, ArrayNode rows)
            throws Exception {
        Path path = config(sink, fields, rows, "BATCH");
        try {
            return container.executeJob(resourceName(path));
        } finally {
            Files.deleteIfExists(path);
        }
    }

    private void fails(
            TestContainer container,
            ObjectNode sink,
            ObjectNode fields,
            ArrayNode rows,
            String reason)
            throws Exception {
        String baseline = container.getServerLogs();
        Container.ExecResult result = run(container, sink, fields, rows);
        String actual = container.getServerLogs();
        String logs =
                result.getStdout()
                        + result.getStderr()
                        + (actual.startsWith(baseline) ? actual.substring(baseline.length()) : "");
        Assertions.assertNotEquals(0, result.getExitCode());
        Assertions.assertTrue(
                logs.contains(reason),
                () -> "Expected " + reason + " in this job's diagnostics\n" + logs);
        Assertions.assertFalse(logs.contains("private-solr-response-533"));
        Assertions.assertFalse(logs.contains(USER));
        Assertions.assertFalse(logs.contains(PASSWORD));
        String credentials = sink.path("username").asText() + ":" + sink.path("password").asText();
        if (sink.has("username")) {
            Assertions.assertFalse(
                    logs.contains(
                            Base64.getEncoder()
                                    .encodeToString(credentials.getBytes(StandardCharsets.UTF_8))));
        }
    }

    private static void succeeds(Container.ExecResult result) {
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
    }

    private static String resourceName(Path config) {
        return "/" + config.getFileName();
    }

    private static Path config(ObjectNode sink, ObjectNode fields, ArrayNode rows, String mode)
            throws IOException {
        ObjectNode schema = JSON.createObjectNode().set("fields", fields);
        String content =
                "env {\n  parallelism = 1\n  job.retry.times = 0\n  job.mode = "
                        + mode
                        + "\n}\n"
                        + "source {\n  FakeSource {\n    schema = "
                        + schema
                        + "\n    row.num = "
                        + rows.size()
                        + "\n    rows = "
                        + rows
                        + "\n  }\n}\n"
                        + "sink {\n  Solr "
                        + sink
                        + "\n}\n";
        Path config =
                Files.createTempFile(
                        ContainerUtil.getCurrentModulePath().resolve("src/test/resources"),
                        "solr-e2e-",
                        ".conf");
        Files.write(config, content.getBytes(StandardCharsets.UTF_8));
        return config;
    }

    private static ObjectNode options(String origin) {
        return JSON.createObjectNode()
                .put("base_url", origin)
                .put("allow_insecure_http", true)
                .put("collection", "records");
    }

    private static ObjectNode schema() {
        return JSON.createObjectNode().put("id", "string").put("title_s", "string");
    }

    private static ObjectNode row(String kind, Object... fields) {
        ObjectNode row = JSON.createObjectNode().put("kind", kind);
        ArrayNode values = row.putArray("fields");
        for (Object field : fields) {
            values.add(JSON.<JsonNode>valueToTree(field));
        }
        return row;
    }

    private static ArrayNode rows(ObjectNode... rows) {
        ArrayNode list = JSON.createArrayNode();
        for (ObjectNode row : rows) {
            list.add(row);
        }
        return list;
    }

    private static ObjectNode faults(ObjectNode... list) {
        return JSON.createObjectNode().set("steps", rows(list));
    }

    private static ObjectNode step(String kind) {
        return JSON.createObjectNode().put("kind", kind);
    }

    private void reset(ObjectNode faults) throws IOException {
        request(proxy, 8080, "/reset", faults.toString(), 204);
    }

    private void release() throws IOException {
        request(proxy, 8080, "/release", "", 204);
    }

    private JsonNode state() throws IOException {
        return request(proxy, 8080, "/state", null, 200);
    }

    private static JsonNode request(
            GenericContainer<?> server, int port, String path, String body, int expected)
            throws IOException {
        URL url = new URL("http", server.getHost(), server.getMappedPort(port), path);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setConnectTimeout(5000);
        connection.setReadTimeout(5000);
        try {
            if (body != null) {
                connection.setRequestMethod("POST");
                connection.setRequestProperty("Content-Type", "application/json");
                connection.setDoOutput(true);
                try (OutputStream out = connection.getOutputStream()) {
                    out.write(body.getBytes(StandardCharsets.UTF_8));
                }
            }
            Assertions.assertEquals(expected, connection.getResponseCode(), path);
            if (expected == 204) {
                return null;
            }
            try (InputStream data = connection.getInputStream()) {
                return JSON.readTree(data);
            }
        } finally {
            connection.disconnect();
        }
    }
}
