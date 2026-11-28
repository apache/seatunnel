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

package org.apache.seatunnel.e2e.connector.bigmailer;

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
import java.util.ArrayList;
import java.util.Arrays;
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
 * Exercises the shipped BigMailer artifact in Java 11 Zeta.
 *
 * <p>A separate HTTPS peer checks exact paths and GET-only requests. Rows are committed to a
 * transactional LocalFile sink and compared by value.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BigMailerIT extends TestSuiteBase {
    private static final String CONF = "/bigmailer.conf";
    private static final String ROOT = "/tmp/bigmailer-https-fixture";
    private static final String OUTPUT = "/tmp/seatunnel_mnt/bigmailer-";
    private static final String FIRST_BRAND = "10000000-0000-4000-8000-000000000001";
    private static final String SECOND_BRAND = "20000000-0000-4000-8000-000000000002";
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
        char[] password = BigMailerPeer.PASSWORD.toCharArray();
        KeyStore identities = KeyStore.getInstance("PKCS12");
        identities.load(null, password);
        KeyStore trust = KeyStore.getInstance("JKS");
        trust.load(null, password);
        Map<String, List<String>> names =
                Map.of(
                        "trusted", List.of(BigMailerPeer.HOST, BigMailerPeer.REDIRECT),
                        "untrusted", List.of(BigMailerPeer.HOST),
                        "wrong", List.of(BigMailerPeer.MISMATCH));
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
    void readsBothAccountCollectionsWithoutCrossingTheOtherCollection() throws Exception {
        Settings brands = new Settings("account-brands", "brands");
        String brand =
                "{\"id\":\""
                        + FIRST_BRAND
                        + "\",\"name\":\"中文\","
                        + "\"engagement\":{\"count\":90071992547409931234567,\"fraction\":0.1112223334},"
                        + "\"connection_id\":null,\"filter_soft_bounces\":false}";
        program(brands, reply(account("brands"), page(false, "terminal-do-not-use", brand)));
        success(run(brands));
        assertRows(brands, expected(FIRST_BRAND, brand));
        assertEquals(1, requests(brands).size());

        Settings users = new Settings("account-users", "users");
        String admin =
                "{\"id\":\"user-1\",\"email\":\"synthetic@example.test\","
                        + "\"is_owner\":false,\"role\":\"ADMIN\",\"updated\":null}";
        String restricted = "{\"id\":\"user-2\",\"role\":\"ANALYST\",\"nested\":{\"level\":3}}";
        program(
                users,
                reply(account("users"), page(true, "user+/=", admin)),
                reply(
                        account("users") + "&cursor=user%2B%2F%3D",
                        page(false, "terminal-user", restricted)));
        success(run(users));
        assertRows(users, expected(null, admin), expected(null, restricted));
        assertEquals(2, requests(users).size());
    }

    @Test
    void readsAllEightBrandCollectionsWithExactGetPathsAndValues() throws Exception {
        // Wire names and payload fields are fixture data, independent from the factory enum.
        Map<String, String> paths = new LinkedHashMap<>();
        paths.put("contacts", "contacts");
        paths.put("lists", "lists");
        paths.put("fields", "fields");
        paths.put("message-types", "message-types");
        paths.put("segments", "segments");
        paths.put("bulk_campaigns", "bulk-campaigns");
        paths.put("transactional_campaigns", "transactional-campaigns");
        paths.put("suppression_lists", "suppression-lists");
        Map<String, String> fields =
                Map.of(
                        "contacts",
                                "\"email\":\"个人@example.test\",\"list_ids\":[null,\"membership\"],\"unsubscribe_all\":false",
                        "lists", "\"name\":\"研究\",\"all\":false",
                        "fields", "\"merge_tag_name\":\"FIELD_1\",\"type\":\"string\"",
                        "message-types", "\"type\":\"transactional\",\"name\":\"receipt\"",
                        "segments",
                                "\"name\":\"new users\",\"filter\":{\"threshold\":123.4567890123}",
                        "bulk_campaigns", "\"status\":\"draft\",\"num_clicks\":9007199254740993",
                        "transactional_campaigns",
                                "\"status\":\"enabled\",\"track_opens\":true,\"num_sent\":14",
                        "suppression_lists",
                                "\"file_name\":\"example.csv\",\"file_size\":90071992547409931234567");
        for (Map.Entry<String, String> resource : paths.entrySet()) {
            Settings options =
                    new Settings("brand-collection-" + resource.getValue(), resource.getKey());
            String row =
                    "{\"id\":\"row-1\",\"other\":null,\"brand_id\":\""
                            + FIRST_BRAND
                            + "\","
                            + fields.get(resource.getKey())
                            + "}";
            program(
                    options,
                    reply(account("brands"), page(false, null, brand(FIRST_BRAND))),
                    reply(
                            child(FIRST_BRAND, resource.getValue()),
                            200,
                            "gzip",
                            page(false, null, row)));
            success(run(options));
            assertRows(options, expected(FIRST_BRAND, row));
            List<JsonNode> calls = requests(options);
            assertEquals(2, calls.size());
            assertTrue(
                    calls.stream()
                            .allMatch(request -> "GET".equals(request.path("method").asText())));
        }
    }

    @Test
    void followsShortAndEmptyPagesAndKeepsSameIdAcrossBrands() throws Exception {
        Settings options = new Settings("two-brand-contacts", "contacts");
        String first = "{\"id\":\"shared\",\"email\":\"a@example.test\",\"tier\":7}";
        String second =
                "{\"id\":\"shared\",\"brand_id\":\""
                        + SECOND_BRAND
                        + "\",\"email\":\"b@example.test\",\"tier\":8}";
        program(
                options,
                reply(account("brands"), page(true, "parent-page-2", brand(FIRST_BRAND))),
                reply(child(FIRST_BRAND, "contacts"), page(true, "contact-empty", first)),
                reply(
                        child(FIRST_BRAND, "contacts") + "&cursor=contact-empty",
                        page(true, "contact-end")),
                reply(
                        child(FIRST_BRAND, "contacts") + "&cursor=contact-end",
                        page(false, "ignore")),
                reply(account("brands") + "&cursor=parent-page-2", page(true, "parent-page-3")),
                reply(
                        account("brands") + "&cursor=parent-page-3",
                        page(false, "ignore", brand(SECOND_BRAND))),
                reply(child(SECOND_BRAND, "contacts"), page(false, null, second)));
        success(run(options));
        assertRows(options, expected(FIRST_BRAND, first), expected(SECOND_BRAND, second));
        List<JsonNode> calls = requests(options);
        assertEquals(7, calls.size());
        for (int i = 1; i < calls.size(); i++) {
            assertTrue(
                    calls.get(i).path("nanoTime").asLong()
                                    - calls.get(i - 1).path("nanoTime").asLong()
                            >= TimeUnit.MILLISECONDS.toNanos(90));
        }
    }

    @Test
    void readsExactlyOneHundredRowsThenHonorsTheContinuation() throws Exception {
        Settings options = new Settings("full-hundred", "users");
        ArrayNode full = JSON.createArrayNode();
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            ObjectNode row = JSON.createObjectNode().put("id", "user-" + i).put("ordinal", i);
            full.add(row);
            expected.add(expected(null, row.toString()));
        }
        ObjectNode first = JSON.createObjectNode().put("has_more", true).put("cursor", "second");
        first.set("data", full);
        String following = "{\"id\":\"user-100\",\"ordinal\":100,\"value\":null}";
        expected.add(expected(null, following));
        program(
                options,
                reply(account("users"), first.toString()),
                reply(account("users") + "&cursor=second", page(false, "ignored", following)));
        success(run(options));
        assertRows(options, expected.toArray(String[]::new));
        assertEquals(2, requests(options).size());
    }

    @Test
    void distinguishesEmptyParentFromForbiddenDiscoveryAndChild() throws Exception {
        Settings empty = new Settings("empty-discovery", "contacts");
        program(empty, reply(account("brands"), page(false, "ignored")));
        success(run(empty));
        assertTrue(rows(empty).isEmpty());
        assertEquals(1, requests(empty).size());

        Settings noBrands = new Settings("forbidden-discovery", "lists");
        program(noBrands, reply(account("brands"), 403, "", BigMailerPeer.PRIVATE));
        failure(run(noBrands), "HTTP 403");
        assertEquals(1, requests(noBrands).size());
        assertTrue(rows(noBrands).isEmpty());

        Settings noContacts = new Settings("forbidden-child", "contacts");
        program(
                noContacts,
                reply(account("brands"), page(false, null, brand(FIRST_BRAND))),
                reply(child(FIRST_BRAND, "contacts"), 403, "", BigMailerPeer.PRIVATE));
        failure(run(noContacts), "HTTP 403");
        assertEquals(2, requests(noContacts).size());
        assertTrue(rows(noContacts).isEmpty());
    }

    @Test
    void rejectsMissingCursorCyclesAndProviderExpiredCursor() throws Exception {
        Settings missing = new Settings("missing-cursor", "users");
        program(missing, reply(account("users"), page(true, null, "{\"id\":\"unsafe\"}")));
        failure(run(missing), "has_more requires a valid continuation cursor");
        assertTrue(rows(missing).isEmpty());

        Settings invalidUnicode = new Settings("invalid-cursor-unicode", "users");
        program(
                invalidUnicode,
                reply(account("users"), "{\"has_more\":true,\"cursor\":\"\\ud800\",\"data\":[]}"));
        failure(run(invalidUnicode), "has_more requires a valid continuation cursor");
        assertEquals(1, requests(invalidUnicode).size());

        Settings cycle = new Settings("cursor-cycle", "users");
        program(
                cycle,
                reply(account("users"), page(true, "loop-a")),
                reply(account("users") + "&cursor=loop-a", page(true, "loop-b")),
                reply(account("users") + "&cursor=loop-b", page(true, "loop-a")));
        failure(run(cycle), "repeated continuation cursor");
        assertEquals(3, requests(cycle).size());

        Settings expired = new Settings("invalid-cursor", "users");
        program(
                expired,
                reply(account("users"), page(true, "expired-opaque")),
                reply(account("users") + "&cursor=expired-opaque", 400, "", BigMailerPeer.PRIVATE));
        failure(run(expired), "HTTP 400");
        assertEquals(2, requests(expired).size());
    }

    @Test
    void capsPaginationAndAcceptsExactlyTheConfiguredTerminalPage() throws Exception {
        Settings cap = new Settings("cap", "users").set("maxPages", 1);
        program(cap, reply(account("users"), page(true, "overflow", "{\"id\":\"excluded\"}")));
        failure(run(cap), "requires more than max_pages");
        assertTrue(rows(cap).isEmpty());
        assertEquals(1, requests(cap).size());

        Settings exact = new Settings("exact-cap", "users").set("maxPages", 1).set("pageSize", 1);
        String row = "{\"id\":\"allowed\"}";
        program(exact, reply("/v1/users?limit=1", page(false, "ignored", row)));
        success(run(exact));
        assertRows(exact, expected(null, row));
        assertEquals(1, requests(exact).size());
    }

    @Test
    void rejectsMalformedSuccessPagesAndUnsafeBrandOwnershipBeforeEmitting() throws Exception {
        Map<String, String> malformed = new LinkedHashMap<>();
        malformed.put("missing-has-more", "{\"data\":[]}");
        malformed.put("numeric-has-more", "{\"has_more\":1,\"data\":[]}");
        malformed.put("duplicate-json", "{\"has_more\":false,\"data\":[{\"id\":1,\"id\":2}]}");
        malformed.put("bad-array", "{\"has_more\":false,\"data\":[null]}");
        malformed.put("error-as-success", "{\"message\":\"" + BigMailerPeer.PRIVATE + "\"}");
        for (Map.Entry<String, String> result : malformed.entrySet()) {
            Settings options = new Settings(result.getKey(), "users");
            program(options, reply(account("users"), result.getValue()));
            failure(run(options), "response");
            assertTrue(rows(options).isEmpty());
        }

        Settings invalidBrand = new Settings("invalid-brand-identifier", "fields");
        program(invalidBrand, reply(account("brands"), page(false, null, "{\"id\":\"../users\"}")));
        failure(run(invalidBrand), "invalid brand UUID");
        assertEquals(1, requests(invalidBrand).size());

        Settings crossed = new Settings("cross-brand", "contacts");
        program(
                crossed,
                reply(account("brands"), page(false, null, brand(FIRST_BRAND))),
                reply(
                        child(FIRST_BRAND, "contacts"),
                        page(
                                false,
                                null,
                                "{\"id\":\"bad\",\"brand_id\":\"" + SECOND_BRAND + "\"}")));
        failure(run(crossed), "does not match the requested brand");
        assertTrue(rows(crossed).isEmpty());
    }

    @Test
    void limitsWireBytesDecompressedBytesAndResponseRows() throws Exception {
        for (String mode : List.of("", "gzip")) {
            Settings options =
                    new Settings(mode.isEmpty() ? "wire-cap" : "decoded-cap", "users")
                            .set("bytes", 1024);
            String large =
                    "{\"id\":\"value\",\"private\":\"" + BigMailerPeer.PRIVATE.repeat(120) + "\"}";
            program(options, reply(account("users"), 200, mode, page(false, null, large)));
            failure(run(options), "max_response_bytes");
            assertTrue(rows(options).isEmpty());
        }

        Settings options = new Settings("page-count", "users").set("pageSize", 1);
        program(options, reply("/v1/users?limit=1", page(false, null, "{\"id\":1}", "{\"id\":2}")));
        failure(run(options), "response data");
        assertTrue(rows(options).isEmpty());
    }

    @Test
    void honorsRetryAfterAndBoundsPermanentErrorsAttemptsAndBudget() throws Exception {
        Settings denied = new Settings("invalid-key", "users");
        program(denied, reply(account("users"), 401, "", BigMailerPeer.PRIVATE));
        failure(run(denied), "HTTP 401");
        assertEquals(1, requests(denied).size());

        Settings retry = new Settings("retry", "users");
        String row = "{\"id\":\"resumed\",\"note\":null}";
        program(
                retry,
                reply(account("users"), 429, "", BigMailerPeer.PRIVATE).put("retryAfter", "1"),
                reply(account("users"), page(false, null, row)));
        success(run(retry));
        assertRows(retry, expected(null, row));
        List<JsonNode> calls = requests(retry);
        assertEquals(2, calls.size());
        assertTrue(
                calls.get(1).path("nanoTime").asLong() - calls.get(0).path("nanoTime").asLong()
                        >= TimeUnit.MILLISECONDS.toNanos(950));

        Settings attempts = new Settings("retry-limit", "users").set("attempts", 2);
        program(
                attempts,
                reply(account("users"), 429, "", BigMailerPeer.PRIVATE),
                reply(account("users"), 503, "", BigMailerPeer.PRIVATE));
        failure(run(attempts), "retry limit reached after HTTP 503");
        assertEquals(2, requests(attempts).size());

        Settings budget = new Settings("retry-budget", "users").set("deadline", 15000);
        program(
                budget,
                reply(account("users"), 429, "", BigMailerPeer.PRIVATE).put("retryAfter", "60"));
        failure(run(budget), "exceeds request deadline");
        assertEquals(1, requests(budget).size());
    }

    @Test
    void closesSlowResponsesAtTheTotalDeadlineAndOnJobCancellation() throws Exception {
        Settings deadline =
                new Settings("trickle-deadline", "users")
                        .set("deadline", 4000)
                        .set("readTimeout", 15000);
        program(deadline, reply(account("users"), 200, "trickle", ""));
        failure(run(deadline), "request deadline exceeded");
        awaitRequests(deadline, 1);
        awaitClosed(deadline);
        assertTrue(rows(deadline).isEmpty());

        Settings cancel = new Settings("trickle-cancel", "users").set("deadline", 60000);
        program(cancel, reply(account("users"), 200, "trickle", ""));
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
    void refusesRedirectUntrustedTlsAndTrustedWrongHostnameBeforeSendingMoreCredentials()
            throws Exception {
        Settings redirected = new Settings("no-redirect", "users");
        program(redirected, reply(account("users"), 302, "", "redirect"));
        failure(run(redirected), "HTTP 302");
        assertEquals(1, requests(redirected).size());

        try {
            for (String certificate : List.of("untrusted", "wrong")) {
                certificates(certificate);
                Settings options = new Settings("bad-tls-" + certificate, "users");
                program(options);
                failure(run(options), "HTTPS or TLS exchange failed");
                assertTrue(requests(options).isEmpty());
            }
        } finally {
            certificates("trusted");
        }
    }

    @Test
    void restoresTheActiveChildCursorAndContinuesToTheNextBrand() throws Exception {
        Settings options = new Settings("savepoint-child", "contacts").set("deadline", 120000);
        String a = "{\"id\":\"a\",\"email\":\"a@example.test\"}";
        String b = "{\"id\":\"b\",\"email\":\"b@example.test\"}";
        String c = "{\"id\":\"c\",\"email\":\"c@example.test\"}";
        String parents = page(false, null, brand(FIRST_BRAND), brand(SECOND_BRAND));
        String changedNames =
                page(
                        false,
                        "unneeded-terminal-cursor",
                        "{\"id\":\"" + FIRST_BRAND + "\",\"name\":\"renamed\"}",
                        brand(SECOND_BRAND));
        String childNext = child(FIRST_BRAND, "contacts") + "&cursor=second-child-page";
        program(
                options,
                reply(account("brands"), parents),
                reply(child(FIRST_BRAND, "contacts"), page(true, "second-child-page", a)),
                reply(childNext, 200, "hold", page(false, null, b)),
                reply(account("brands"), changedNames),
                reply(childNext, page(false, null, b)),
                reply(child(SECOND_BRAND, "contacts"), page(false, null, c)));
        resume(options, 3, null);
        assertEquals(6, requests(options).size());
        assertRows(
                options,
                expected(FIRST_BRAND, a),
                expected(FIRST_BRAND, b),
                expected(SECOND_BRAND, c));
    }

    @Test
    void changedParentOrderFailsInsteadOfUsingTheSavedChildCursorInAnotherBrand() throws Exception {
        Settings options = new Settings("changed-parent", "contacts").set("deadline", 120000);
        String first = "{\"id\":\"stored\",\"email\":\"original@example.test\"}";
        String parents = page(false, null, brand(FIRST_BRAND), brand(SECOND_BRAND));
        String reordered = page(false, null, brand(SECOND_BRAND), brand(FIRST_BRAND));
        program(
                options,
                reply(account("brands"), parents),
                reply(child(FIRST_BRAND, "contacts"), page(false, null, first)),
                reply(child(SECOND_BRAND, "contacts"), 200, "hold", page(false, null)),
                reply(account("brands"), reordered));
        resume(options, 3, "saved response page changed");
        assertEquals(4, requests(options).size());
        assertRows(options, expected(FIRST_BRAND, first));
    }

    @Test
    void invalidOptionsOrReaderParallelismFailBeforeAnyRequest() throws Exception {
        Settings resource = new Settings("bad-resource", "nonexistent");
        program(resource);
        failure(run(resource), "supported BigMailer collection");
        assertTrue(requests(resource).isEmpty());

        Settings limit = new Settings("bad-page-size", "brands").set("pageSize", 101);
        program(limit);
        failure(run(limit), "page_size");
        assertTrue(requests(limit).isEmpty());

        Settings parallel = new Settings("bad-parallelism", "users").set("parallelism", 2);
        program(parallel);
        failure(run(parallel), "source parallelism must be 1");
        assertTrue(requests(parallel).isEmpty());
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
        Path program = Files.createTempFile(directory, "bigmailer-steps-", ".json");
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
        Path source = Files.createTempFile(directory, "bigmailer-fixture-", ".txt");
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
            JsonNode value = row.path("brand_id");
            String brand = value.isNull() || value.isMissingNode() ? null : value.asText();
            actual.add(expected(brand, row.path("data").asText()));
        }
        List<String> target = new ArrayList<>(Arrays.asList(wanted));
        Collections.sort(target);
        Collections.sort(actual);
        assertEquals(target, actual);
    }

    private static String expected(String brand, String row) throws IOException {
        return (brand == null ? "<no-brand>" : brand) + "|" + JSON.readTree(row).toString();
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

    private static ObjectNode reply(String target, String response) {
        return reply(target, 200, "", response);
    }

    private static ObjectNode reply(String target, int status, String mode, String response) {
        return JSON.createObjectNode()
                .put("target", target)
                .put("status", status)
                .put("mode", mode)
                .put("response", response);
    }

    private static String account(String name) {
        return "/v1/" + name + "?limit=100";
    }

    private static String child(String brand, String name) {
        return "/v1/brands/" + brand + "/" + name + "?limit=100";
    }

    private static String brand(String id) {
        return "{\"id\":\"" + id + "\",\"name\":\"Fixture Company\"}";
    }

    private static String page(boolean more, String token, String... data) {
        return "{\"has_more\":"
                + more
                + (token == null ? "" : ",\"cursor\":\"" + token + "\"")
                + ",\"data\":["
                + String.join(",", data)
                + "]}";
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
        assertFalse(log.contains(BigMailerPeer.PRIVATE), "log contains provider response data");
        assertFalse(log.contains(BigMailerPeer.KEY), "log contains the provider credential");
        assertFalse(log.contains("expired-opaque"), "log contains a private provider cursor");
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
                                BigMailerPeer.PASSWORD,
                                "-keypass",
                                BigMailerPeer.PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String log = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, process.waitFor(), log);
        KeyStore key = KeyStore.getInstance("PKCS12");
        try (InputStream stream = Files.newInputStream(target)) {
            key.load(stream, BigMailerPeer.PASSWORD.toCharArray());
        }
        return key;
    }

    /**
     * Supplies per-job values through the standard command line.
     *
     * <p>Most cases use the connector's true default page size and request settings.
     */
    private static final class Settings {
        final String name;
        private final Map<String, Object> values = new LinkedHashMap<>();

        Settings(String name, String resource) {
            this.name = name;
            set("token", BigMailerPeer.KEY);
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
                                return "bigmailer." + entry.getKey() + "=" + config;
                            })
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

        Engine(Path fixtures) {
            this.fixtures = fixtures;
        }

        @Override
        protected String getJavaToolOptions() {
            return "-Djavax.net.ssl.trustStore="
                    + ROOT
                    + "/trust.jks -Djavax.net.ssl.trustStoreType=JKS"
                    + " -Djavax.net.ssl.trustStorePassword="
                    + BigMailerPeer.PASSWORD;
        }

        @Override
        protected void executeExtraCommands(GenericContainer<?> container) throws IOException {
            container.withExtraHost(BigMailerPeer.HOST, "127.0.0.1");
            container.withExtraHost(BigMailerPeer.REDIRECT, "127.0.0.1");
            for (String file : List.of("identities.p12", "trust.jks")) {
                container.withCopyFileToContainer(
                        MountableFile.forHostPath(fixtures.resolve(file)), ROOT + "/" + file);
            }
            for (Class<?> type :
                    List.of(BigMailerPeer.class, BigMailerPeer.SwitchingKeyManager.class)) {
                String path = type.getName().replace('.', '/') + ".class";
                try {
                    container.withCopyFileToContainer(
                            MountableFile.forHostPath(
                                    Path.of(type.getResource("/" + path).toURI())),
                            ROOT + "/classes/" + path);
                } catch (URISyntaxException e) {
                    throw new IOException("Cannot stage the BigMailer HTTPS fixture", e);
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
                throw new IOException("Cannot stage the BigMailer fixture JSON library", e);
            }
        }

        Container.ExecResult runPeer() throws Exception {
            return server.execInContainer(
                    "java",
                    "-cp",
                    ROOT + "/classes:" + ROOT + "/jackson:" + ROOT + "/jackson.jar",
                    BigMailerPeer.class.getName(),
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
