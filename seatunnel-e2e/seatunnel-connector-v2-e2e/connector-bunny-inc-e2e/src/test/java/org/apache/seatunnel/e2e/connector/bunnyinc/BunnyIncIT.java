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

package org.apache.seatunnel.e2e.connector.bunnyinc;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.bunnyinc.BunnyIncRecoveryProbe;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.seatunnel.SeaTunnelContainer;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

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
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
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
 * Exercises the shipped BunnyInc artifact in Java 11 Zeta.
 *
 * <p>A separate HTTPS peer checks the complete read-only operation and scoped authorization. Rows
 * are committed to a transactional LocalFile sink and compared by value.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BunnyIncIT extends TestSuiteBase {
    private static final String CONF = "/bunny-inc.conf";
    private static final String ROOT = "/tmp/bunnyinc-https-fixture";
    private static final String OUTPUT = "/tmp/seatunnel_mnt/bunnyinc-";
    private static final String TERMINAL_CONF = "/bunny-inc-terminal.conf";
    private static final String MATRIX = "/bunny-inc-projection.tsv";
    private static final ObjectMapper JSON =
            new ObjectMapper()
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));

    @TempDir static Path directory;
    private final ExecutorService tasks = Executors.newCachedThreadPool();
    private Engine engine;
    private Future<Container.ExecResult> peer;
    private String active;

    @BeforeAll
    void startFixture() throws Exception {
        char[] password = BunnyIncPeer.PASSWORD.toCharArray();
        KeyStore identities = KeyStore.getInstance("PKCS12");
        identities.load(null, password);
        KeyStore trust = KeyStore.getInstance("JKS");
        trust.load(null, password);
        Map<String, List<String>> names =
                Map.of(
                        "trusted",
                                List.of(
                                        BunnyIncPeer.HOST,
                                        BunnyIncPeer.SECOND,
                                        BunnyIncPeer.REDIRECT),
                        "untrusted", List.of(BunnyIncPeer.HOST),
                        "wrong", List.of(BunnyIncPeer.MISMATCH));
        for (Map.Entry<String, List<String>> entry : names.entrySet()) {
            String alias = entry.getKey();
            KeyStore identity = identity(alias, entry.getValue());
            identities.setKeyEntry(
                    alias,
                    identity.getKey(alias, password),
                    password,
                    identity.getCertificateChain(alias));
            if (!"untrusted".equals(alias)) {
                trust.setCertificateEntry(alias, identity.getCertificate(alias));
            }
            if ("trusted".equals(alias)) {
                String pem =
                        "-----BEGIN CERTIFICATE-----\n"
                                + Base64.getMimeEncoder(64, new byte[] {'\n'})
                                        .encodeToString(identity.getCertificate(alias).getEncoded())
                                + "\n-----END CERTIFICATE-----\n";
                Files.writeString(directory.resolve("trusted.pem"), pem);
            }
        }
        try (OutputStream out = Files.newOutputStream(directory.resolve("identities.p12"))) {
            identities.store(out, password);
        }
        try (OutputStream out = Files.newOutputStream(directory.resolve("trust.jks"))) {
            trust.store(out, password);
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
    void queriesEachOfTheFifteenResourcesWithTheCompleteBillingProjection() throws Exception {
        Map<String, Contract> contracts = contracts();
        assertEquals(15, contracts.size());
        for (Contract contract : contracts.values()) {
            Settings settings = new Settings("catalog-" + contract.resource, contract.resource);
            program(settings, reply(contract, 10, null, page(contract, false, null, contract.row)));
            success(run(settings));
            assertRows(
                    settings,
                    expected(JSON.readTree(contract.row).path("id").asText(), contract.row));
            assertEquals(1, requests(settings).size());
        }
        assertEquals("quoteCharges", contracts.get("quote_charges").field);
        assertTrue(
                contracts.get("subscriptions").fields.contains("cancelationDate:cancellationDate"));
    }

    @Test
    void preservesOpaqueCursorsAndContinuesAfterAnEmptyPage() throws Exception {
        Contract contract = contracts().get("invoices");
        Settings settings = new Settings("opaque-cursor", contract.resource).set("pageSize", 2);
        String a = "{\"id\":\"id-a\",\"amount\":1.2300}";
        String b = "{\"id\":\"id-b\",\"voided\":null}";
        String first = "opaque==/中文-\"quoted\"\n";
        String second = "next+%=/€";
        program(
                settings,
                reply(contract, 2, null, page(contract, true, first, a)),
                reply(contract, 2, first, page(contract, true, second)),
                reply(contract, 2, second, page(contract, false, "unused-final", b)));
        success(run(settings));
        assertRows(settings, expected("id-a", a), expected("id-b", b));
        List<JsonNode> calls = requests(settings);
        assertEquals(3, calls.size());
        assertTrue(calls.get(0).path("after").isNull());
        assertEquals(first, calls.get(1).path("after").asText());
        assertEquals(second, calls.get(2).path("after").asText());
    }

    @Test
    void rejectsMissingNonStringOrCyclicCursorsAndThePageBudget() throws Exception {
        Contract contract = contracts().get("accounts");
        for (String mode : List.of("missing", "empty", "numeric", "oversized", "boolean")) {
            Settings settings = new Settings("bad-cursor-" + mode, contract.resource);
            String payload =
                    "{\"data\":{\"accounts\":{\"nodes\":[],\"pageInfo\":{\"hasNextPage\":"
                            + ("boolean".equals(mode) ? "\"true\"" : "true")
                            + ("missing".equals(mode)
                                    ? ""
                                    : ",\"endCursor\":"
                                            + ("numeric".equals(mode)
                                                    ? "8"
                                                    : "\""
                                                            + ("oversized".equals(mode)
                                                                    ? "a".repeat(8193)
                                                                    : "")
                                                            + "\""))
                            + "}}}}";
            program(settings, reply(contract, 10, null, payload));
            failure(run(settings), "GraphQL");
            assertTrue(rows(settings).isEmpty());
        }

        Settings cycle = new Settings("cursor-cycle", contract.resource);
        program(
                cycle,
                reply(contract, 10, null, page(contract, true, "first")),
                reply(contract, 10, "first", page(contract, true, "second")),
                reply(contract, 10, "second", page(contract, true, "first")));
        failure(run(cycle), "repeated cursor");
        assertEquals(3, requests(cycle).size());

        Settings repeat = new Settings("cursor-repeat", contract.resource);
        program(
                repeat,
                reply(contract, 10, null, page(contract, true, "same")),
                reply(contract, 10, "same", page(contract, true, "same")));
        failure(run(repeat), "repeated cursor");
        assertEquals(2, requests(repeat).size());

        Settings cap = new Settings("page-budget", contract.resource).set("maxPages", 1);
        program(cap, reply(contract, 10, null, page(contract, true, "another")));
        failure(run(cap), "max_pages");
        assertEquals(1, requests(cap).size());
    }

    @Test
    void rejectsGraphQlPartialAndErrorOnlyResponsesBeforeAnyRowsAppear() throws Exception {
        Contract contract = contracts().get("contacts");
        String valid = page(contract, false, null, "{\"id\":\"must-not-be-output\"}");
        String errors = "\"errors\":[{\"message\":\"" + BunnyIncPeer.PRIVATE + "\"}]";
        List<String> cases =
                List.of(
                        "{" + errors + "}",
                        "{" + errors + "," + valid.substring(1),
                        "{\"errors\":null,\"data\":{}}");
        for (int index = 0; index < cases.size(); index++) {
            Settings settings = new Settings("graphql-errors-" + index, contract.resource);
            program(settings, reply(contract, 10, null, cases.get(index)));
            failure(run(settings), "GraphQL query returned errors");
            assertTrue(rows(settings).isEmpty());
        }
        Settings clean = new Settings("empty-errors", contract.resource);
        program(clean, reply(contract, 10, null, "{\"errors\":[]," + valid.substring(1)));
        success(run(clean));
        assertRows(clean, expected("must-not-be-output", "{\"id\":\"must-not-be-output\"}"));
    }

    @Test
    void rejectsMalformedJsonIncorrectConnectionsAndOversizedNodes() throws Exception {
        Contract contract = contracts().get("plans");
        List<String> cases =
                List.of(
                        "{}",
                        "{\"data\":{\"other\":{\"nodes\":[],\"pageInfo\":{}}}}",
                        "{\"data\":{\"plans\":{\"nodes\":[null],\"pageInfo\":{\"hasNextPage\":false}}}}",
                        "{\"data\":{\"plans\":{\"nodes\":[],\"nodes\":[],\"pageInfo\":{\"hasNextPage\":false}}}}",
                        page(contract, false, null, "{\"id\":{}}"),
                        page(contract, false, null, "{\"id\":\"a\"}", "{\"id\":\"b\"}") + "{}");
        for (int index = 0; index < cases.size(); index++) {
            Settings settings = new Settings("bad-payload-" + index, contract.resource);
            program(settings, reply(contract, 10, null, cases.get(index)));
            failure(run(settings), "GraphQL");
            assertTrue(rows(settings).isEmpty());
        }

        Settings cap = new Settings("nodes-cap", contract.resource).set("pageSize", 1);
        program(
                cap,
                reply(
                        contract,
                        1,
                        null,
                        page(contract, false, null, "{\"id\":\"a\"}", "{\"id\":\"b\"}")));
        failure(run(cap), "oversized GraphQL nodes");
        assertTrue(rows(cap).isEmpty());
    }

    @Test
    void preservesNullIdsWideNumericIdsAndNestedPrecision() throws Exception {
        Contract contract = contracts().get("invoiceItems");
        Settings settings = new Settings("scalar-exactness", contract.resource);
        String a =
                "{\"id\":90071992547409931234567,\"priceDecimals\":9,\"quantity\":0.0000000012300}";
        String b = "{\"id\":null,\"bill\":{\"value\":0.123456789012345600,\"list\":[null,false]}}";
        String c = "{\"description\":\"非ASCII 😀\",\"quantity\":null,\"newField\":{\"x\":true}}";
        program(settings, reply(contract, 10, null, page(contract, false, null, a, b, c)));
        success(run(settings));
        assertRows(
                settings,
                expected("90071992547409931234567", a),
                expected(null, b),
                expected(null, c));
    }

    @Test
    void isolatesTheBearerTokenAndOriginForEachConfiguredTenant() throws Exception {
        Contract contract = contracts().get("tenants");
        Settings east = new Settings("east-tenant", contract.resource);
        String a = "{\"id\":\"east\",\"subdomain\":\"fixture-east\"}";
        program(east, reply(contract, 10, null, page(contract, false, null, a)));
        success(run(east));
        assertRows(east, expected("east", a));

        Settings west =
                new Settings("west-tenant", contract.resource)
                        .set("subdomain", "FiXtUrE-WeSt")
                        .set("token", BunnyIncPeer.SECOND_KEY);
        String b = "{\"id\":\"west\",\"subdomain\":\"fixture-west\"}";
        program(
                west,
                reply(contract, 10, null, page(contract, false, null, b))
                        .put("host", BunnyIncPeer.SECOND)
                        .put("key", BunnyIncPeer.SECOND_KEY));
        success(run(west));
        assertRows(west, expected("west", b));
        assertEquals(1, requests(east).size());
        assertEquals(1, requests(west).size());
    }

    @Test
    void retriesOnlyTheSameBoundedReadAndDoesNotExposeProviderErrors() throws Exception {
        Contract contract = contracts().get("accountBalances");
        String id = "{\"id\":\"saved\",\"balance\":9007199254740993.100}";
        Settings retry = new Settings("retry-same-read", contract.resource);
        program(
                retry,
                reply(contract, 10, null, BunnyIncPeer.PRIVATE)
                        .put("status", 429)
                        .put("retryAfter", "1"),
                reply(contract, 10, null, BunnyIncPeer.PRIVATE).put("status", 503),
                reply(contract, 10, null, page(contract, false, null, id)));
        success(run(retry));
        assertRows(retry, expected("saved", id));
        List<JsonNode> calls = requests(retry);
        assertEquals(3, calls.size());
        assertTrue(
                calls.get(1).path("nanoTime").longValue()
                                - calls.get(0).path("nanoTime").longValue()
                        >= TimeUnit.MILLISECONDS.toNanos(950));

        Settings cap = new Settings("retry-capped", contract.resource).set("attempts", 2);
        program(
                cap,
                reply(contract, 10, null, BunnyIncPeer.PRIVATE).put("status", 408),
                reply(contract, 10, null, BunnyIncPeer.PRIVATE).put("status", 503));
        failure(run(cap), "retry limit reached after HTTP 503");
        assertEquals(2, requests(cap).size());

        Settings delay = new Settings("retry-deadline", contract.resource).set("deadline", 10000);
        program(
                delay,
                reply(contract, 10, null, BunnyIncPeer.PRIVATE)
                        .put("status", 429)
                        .put("retryAfter", "60"));
        failure(run(delay), "retry delay exceeds request deadline");
        assertEquals(1, requests(delay).size());

        for (int status : List.of(401, 403)) {
            Settings failure = new Settings("auth-" + status, contract.resource);
            program(failure, reply(contract, 10, null, BunnyIncPeer.PRIVATE).put("status", status));
            failure(run(failure), "HTTP " + status);
            assertEquals(1, requests(failure).size());
        }
    }

    @Test
    void rejectsRedirectsOrCertificateFailuresBeforeSendingASecondRequest() throws Exception {
        Contract contract = contracts().get("entities");
        Settings redirect = new Settings("redirect-rejected", contract.resource);
        program(redirect, reply(contract, 10, null, "redirect").put("status", 302));
        failure(run(redirect), "HTTP 302");
        assertEquals(1, requests(redirect).size());

        try {
            for (String certificate : List.of("untrusted", "wrong")) {
                certificates(certificate);
                Settings settings = new Settings("tls-" + certificate, contract.resource);
                program(settings);
                failure(run(settings), "HTTPS or TLS exchange failed");
                assertTrue(requests(settings).isEmpty());
            }
        } finally {
            certificates("trusted");
        }
    }

    @Test
    void rejectsBadUtf8AndBoundsCompressedAndUncompressedBodies() throws Exception {
        Contract contract = contracts().get("quote_charges");
        String item = "{\"id\":\"quote\",\"priceDecimals\":4,\"amount\":1.234500}";
        Settings good = new Settings("gzip-success", contract.resource);
        program(
                good,
                reply(contract, 10, null, page(contract, false, null, item)).put("mode", "gzip"));
        success(run(good));
        assertRows(good, expected("quote", item));

        for (String mode : List.of("", "gzip")) {
            Settings settings =
                    new Settings(mode.isEmpty() ? "wire-cap" : "expanded-cap", contract.resource)
                            .set("bytes", 1024);
            String excessive =
                    "{\"id\":\"quote\",\"text\":\"" + BunnyIncPeer.PRIVATE.repeat(120) + "\"}";
            program(
                    settings,
                    reply(contract, 10, null, page(contract, false, null, excessive))
                            .put("mode", mode));
            failure(run(settings), "max_response_bytes");
            assertTrue(rows(settings).isEmpty());
        }
        Settings encoding = new Settings("invalid-utf8", contract.resource);
        program(
                encoding,
                reply(contract, 10, null, "")
                        .put(
                                "bodyBase64",
                                Base64.getEncoder()
                                        .encodeToString(new byte[] {(byte) 0xc3, 0x28})));
        failure(run(encoding), "UTF-8");
    }

    @Test
    void boundsTricklingResponsesAndCancelsTheActualEngineRequest() throws Exception {
        Contract contract = contracts().get("transactions");
        Settings limit =
                new Settings("trickle-deadline", contract.resource)
                        .set("deadline", 4000)
                        .set("readTimeout", 15000);
        program(limit, reply(contract, 10, null, "").put("mode", "trickle"));
        failure(run(limit), "request deadline exceeded");
        awaitClosed(limit);
        assertTrue(rows(limit).isEmpty());

        Settings canceled = new Settings("trickle-cancel", contract.resource);
        program(canceled, reply(contract, 10, null, "").put("mode", "trickle"));
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(CONF, job, canceled.variables()));
        try {
            awaitRequests(canceled, 1);
            success(engine.cancelJob(job));
            running.get(30, TimeUnit.SECONDS);
            awaitClosed(canceled);
            assertTrue(rows(canceled).isEmpty());
        } finally {
            if (!running.isDone()) {
                engine.cancelJob(job);
                running.cancel(true);
            }
        }
    }

    @Test
    void restoresTheSavedCursorAndDoesNotReEmitAlreadyCommittedRows() throws Exception {
        Contract contract = contracts().get("subscriptionCharges");
        Settings settings =
                new Settings("cursor-savepoint", contract.resource)
                        .set("deadline", 120000)
                        .set("pageSize", 2);
        String a = "{\"id\":\"first\",\"price\":1.0}";
        String b = "{\"id\":\"second\",\"price\":2.0}";
        String c = "{\"id\":\"third\",\"price\":3.0}";
        program(
                settings,
                reply(contract, 2, null, page(contract, true, "resume", a, b)),
                reply(contract, 2, "resume", page(contract, false, null, c)).put("mode", "hold"),
                reply(contract, 2, "resume", page(contract, false, null, c)));
        resume(settings, 2, null);
        assertRows(settings, expected("first", a), expected("second", b), expected("third", c));
        assertEquals(3, requests(settings).size());
    }

    @Test
    void rejectsUnsafeTenantsOversizedPageOptionsAndParallelismBeforeRequests() throws Exception {
        Contract contract = contracts().get("accounts");
        Settings host =
                new Settings("bad-tenant", contract.resource).set("subdomain", "fake.example");
        program(host);
        failure(run(host), "one Bunny tenant DNS label");
        assertTrue(requests(host).isEmpty());

        Settings size = new Settings("bad-page-size", contract.resource).set("pageSize", 11);
        program(size);
        failure(run(size), "page_size");
        assertTrue(requests(size).isEmpty());

        Settings parallel =
                new Settings("bad-parallelism", contract.resource).set("parallelism", 2);
        program(parallel);
        failure(run(parallel), "source parallelism must be 1");
        assertTrue(requests(parallel).isEmpty());
    }

    @Test
    void restoresATerminalSourceAlongsideAnActivePipelineWithoutMoreQueries() throws Exception {
        Contract contract = contracts().get("accounts");
        Settings settings = new Settings("terminal-first", contract.resource);
        String data = "{\"id\":\"committed-only-once\",\"name\":\"Final account\"}";
        String expected = expected("committed-only-once", data);
        program(settings, reply(contract, 10, null, page(contract, false, null, data)));
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(TERMINAL_CONF, job, settings.variables()));
        try {
            Awaitility.await()
                    .atMost(35, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertRows(settings, expected));
            success(engine.savepointJob(job));
            running.get(30, TimeUnit.SECONDS);
        } finally {
            if (!running.isDone()) {
                engine.cancelJob(job);
                running.cancel(true);
            }
        }
        assertEquals(1, requests(settings).size());

        Settings restored = new Settings("terminal-restore-peer", contract.resource);
        program(restored);
        Future<Container.ExecResult> pending =
                tasks.submit(() -> engine.restoreJob(TERMINAL_CONF, job, settings.variables()));
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> "RUNNING".equals(engine.getJobStatus(job)));
            long prior = engine.completedAcrossPipelines(job);
            Awaitility.await()
                    .atMost(35, TimeUnit.SECONDS)
                    .until(() -> engine.completedAcrossPipelines(job) > prior);
            assertTrue(requests(restored).isEmpty());
            assertRows(settings, expected);
        } finally {
            if (!pending.isDone()) {
                engine.cancelJob(job);
            }
            pending.get(30, TimeUnit.SECONDS);
        }
    }

    @Test
    void verifiesSerializedMidPageStateUsingThePackagedReaderAndRealHttps() throws Exception {
        Contract contract = contracts().get("accounts");
        String[] data = new String[5];
        for (int index = 0; index < data.length; index++) {
            data[index] = "{\"id\":\"row-" + index + "\",\"n\":" + index + ".000}";
        }
        String response = page(contract, false, null, data);
        Settings resume = new Settings("midpage-resume", contract.resource);
        program(resume, reply(contract, 5, null, response), reply(contract, 5, null, response));
        success(engine.runProbe("resume"));
        assertEquals(2, requests(resume).size());

        Settings changed = new Settings("midpage-changed", contract.resource);
        data[4] = "{\"id\":\"changed\",\"n\":99}";
        program(
                changed,
                reply(contract, 5, null, response),
                reply(contract, 5, null, page(contract, false, null, data)));
        success(engine.runProbe("changed"));
        assertEquals(2, requests(changed).size());

        Settings empty = new Settings("no-recovery-traffic", contract.resource);
        program(empty);
        success(engine.runProbe("zero"));
        assertTrue(requests(empty).isEmpty());
    }

    private void resume(Settings options, int countBeforeRestore, String failureClue)
            throws Exception {
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
            Container.ExecResult result = engine.restoreJob(CONF, job, options.variables());
            if (failureClue == null) {
                success(result);
            } else {
                failure(result, failureClue);
            }
        } finally {
            if (!running.isDone()) {
                engine.cancelJob(job);
                running.cancel(true);
            }
        }
    }

    private void program(Settings options, ObjectNode... steps) throws Exception {
        verifyProviderAssertions();
        Path program = Files.createTempFile(directory, "bunnyinc-steps-", ".json");
        Files.writeString(program, JSON.createArrayNode().addAll(Arrays.asList(steps)).toString());
        engine.install(program, options.name + ".program");
        installText("active", options.name);
        active = options.name;
    }

    private void certificates(String alias) throws Exception {
        installText("certificate", alias);
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> alias.equals(engine.contents("certificate-applied")));
    }

    private void installText(String path, String content) throws IOException {
        Path source = Files.createTempFile(directory, "bunnyinc-fixture-", ".txt");
        Files.writeString(source, content);
        engine.install(source, path);
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

    private void assertRows(Settings options, String... wanted) throws Exception {
        List<String> actual = new ArrayList<>();
        for (JsonNode row : rows(options)) {
            JsonNode id = row.path("entity_id");
            actual.add(
                    expected(
                            id.isNull() || id.isMissingNode() ? null : id.asText(),
                            row.path("data").asText()));
        }
        List<String> target = new ArrayList<>(Arrays.asList(wanted));
        Collections.sort(target);
        Collections.sort(actual);
        assertEquals(target, actual);
    }

    private static String expected(String id, String row) throws IOException {
        return JSON.createArrayNode().add(id).add(JSON.readTree(row)).toString();
    }

    private void awaitRequests(Settings options, int count) {
        Awaitility.await()
                .atMost(count > 100 ? 100 : 35, TimeUnit.SECONDS)
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

    private static ObjectNode reply(Contract contract, int size, String after, String response) {
        return JSON.createObjectNode()
                .put("host", BunnyIncPeer.HOST)
                .put("field", contract.field)
                .put("fields", contract.fields)
                .put("first", size)
                .put("after", after)
                .put("response", response)
                .put("status", 200)
                .put("mode", "");
    }

    private static String page(Contract contract, boolean more, String end, String... rows) {
        return "{\"data\":{\""
                + contract.field
                + "\":{\"nodes\":["
                + String.join(",", rows)
                + "],\"pageInfo\":{\"hasNextPage\":"
                + more
                + ",\"endCursor\":"
                + JSON.getNodeFactory().textNode(end)
                + "}}}}";
    }

    /**
     * Loads the independently prepared GraphQL projection matrix.
     *
     * <p>Tests never derive their expected wire field set from the production Java catalog.
     */
    private static Map<String, Contract> contracts() throws IOException {
        LinkedHashMap<String, Contract> result = new LinkedHashMap<>();
        try (InputStream source = BunnyIncIT.class.getResourceAsStream(MATRIX)) {
            if (source == null) {
                throw new IOException("Missing Bunny billing contract fixture");
            }
            for (String line :
                    new String(source.readAllBytes(), StandardCharsets.UTF_8).split("\n")) {
                if (line.startsWith("#") || line.isBlank()) {
                    continue;
                }
                String[] columns = line.split("\t", -1);
                assertEquals(4, columns.length);
                result.put(
                        columns[0], new Contract(columns[0], columns[1], columns[2], columns[3]));
            }
        }
        return result;
    }

    /**
     * Names a fixture connection separately from its configured SeaTunnel resource.
     *
     * <p>The projection includes the wire alias used by the subscription endpoint.
     */
    private static final class Contract {
        final String resource;
        final String field;
        final String fields;
        final String row;

        Contract(String resource, String field, String fields, String row) {
            this.resource = resource;
            this.field = field;
            this.fields = fields;
            this.row = row;
        }
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
        assertFalse(log.contains(BunnyIncPeer.PRIVATE), "log contains provider response data");
        assertFalse(log.contains(BunnyIncPeer.KEY), "log contains the provider credential");
    }

    /**
     * Generates an identity for the fixture's trusted, untrusted or mismatched handshake.
     *
     * @param name the entry name in the combined server key store
     * @param hosts all names included in this certificate
     * @return the generated private-key entry and certificate
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
                                name,
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
                                BunnyIncPeer.PASSWORD,
                                "-keypass",
                                BunnyIncPeer.PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String log = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, process.waitFor(), log);
        KeyStore key = KeyStore.getInstance("PKCS12");
        try (InputStream stream = Files.newInputStream(target)) {
            key.load(stream, BunnyIncPeer.PASSWORD.toCharArray());
        }
        return key;
    }

    /**
     * Supplies per-job values through the standard command line.
     *
     * <p>Each job chooses one export and uses the normal connector configuration.
     */
    private static final class Settings {
        final String name;
        private final Map<String, Object> values = new LinkedHashMap<>();

        Settings(String name, String resource) {
            this.name = name;
            set("token", BunnyIncPeer.KEY);
            set("resource", resource);
            set("subdomain", "fixture-east");
            set("parallelism", 1);
            set("output", name);
            set("pageSize", 10);
            set("maxPages", 2000);
            set("readTimeout", 30000);
            set("deadline", 60000);
            set("bytes", 8 * 1024 * 1024);
            set("attempts", 4);
        }

        Settings set(String field, Object value) {
            values.put(field, value);
            return this;
        }

        String[] variables() {
            return values.entrySet().stream()
                    .map(entry -> "bunnyinc." + entry.getKey() + "=" + entry.getValue())
                    .toArray(String[]::new);
        }
    }

    /**
     * Installs the independent fixture and prevents external provider DNS in tests.
     *
     * <p>The same production URL, trust-store mechanism and packaged connector remain in use.
     */
    private static final class Engine extends SeaTunnelContainer {
        private final Path fixtures;
        private final HttpClient checkpoints = HttpClient.newHttpClient();

        Engine(Path fixtures) {
            this.fixtures = fixtures;
        }

        @Override
        protected String getJavaToolOptions() {
            return "-Djavax.net.ssl.trustStore="
                    + ROOT
                    + "/trust.jks -Djavax.net.ssl.trustStoreType=JKS"
                    + " -Djavax.net.ssl.trustStorePassword="
                    + BunnyIncPeer.PASSWORD;
        }

        @Override
        protected void executeExtraCommands(GenericContainer<?> container) throws IOException {
            for (String host : List.of(BunnyIncPeer.HOST, BunnyIncPeer.SECOND)) {
                container.withExtraHost(host, "127.0.0.1");
            }
            container.withExtraHost(BunnyIncPeer.REDIRECT, "127.0.0.1");
            for (String file : List.of("identities.p12", "trust.jks", "trusted.pem")) {
                container.withCopyFileToContainer(
                        MountableFile.forHostPath(fixtures.resolve(file)), ROOT + "/" + file);
            }
            List<Class<?>> fixtureClasses =
                    new ArrayList<>(
                            List.of(
                                    BunnyIncPeer.class,
                                    BunnyIncPeer.SwitchingKeyManager.class,
                                    BunnyIncRecoveryProbe.class));
            fixtureClasses.addAll(Arrays.asList(BunnyIncRecoveryProbe.class.getDeclaredClasses()));
            for (Class<?> type : fixtureClasses) {
                String path = type.getName().replace('.', '/') + ".class";
                try {
                    container.withCopyFileToContainer(
                            MountableFile.forHostPath(
                                    Path.of(type.getResource("/" + path).toURI())),
                            ROOT + "/classes/" + path);
                } catch (URISyntaxException e) {
                    throw new IOException("Cannot stage the BunnyInc HTTPS fixture", e);
                }
            }
            try {
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
                throw new IOException("Cannot stage the BunnyInc fixture JSON library", e);
            }
        }

        Container.ExecResult runPeer() throws Exception {
            return server.execInContainer(
                    "java",
                    "-cp",
                    ROOT + "/classes:" + ROOT + "/jackson:" + ROOT + "/jackson.jar",
                    BunnyIncPeer.class.getName(),
                    ROOT);
        }

        Container.ExecResult runProbe(String mode) throws Exception {
            ContainerUtil.copyAllConnectorJarToContainer(
                    server,
                    getConnectorModulePath(),
                    getConnectorNamePrefix(),
                    getConnectorType(),
                    SEATUNNEL_HOME);
            return server.execInContainer(
                    "java",
                    "-cp",
                    ROOT
                            + "/classes:"
                            + ROOT
                            + "/jackson:"
                            + ROOT
                            + "/jackson.jar:"
                            + "/tmp/seatunnel/lib/*:/tmp/seatunnel/connectors/*",
                    BunnyIncRecoveryProbe.class.getName(),
                    ROOT + "/trusted.pem",
                    mode);
        }

        private long completedAcrossPipelines(String id) throws Exception {
            URI uri =
                    new URI(
                            "http",
                            null,
                            server.getHost(),
                            server.getMappedPort(8080),
                            "/jobs/checkpoints/" + id,
                            null,
                            null);
            HttpResponse<String> response =
                    checkpoints.send(
                            HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(5)).build(),
                            HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                return 0L;
            }
            long completed = 0L;
            for (JsonNode pipeline : JSON.readTree(response.body()).path("pipelines")) {
                completed += pipeline.path("counts").path("completed").asLong();
            }
            return completed;
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
