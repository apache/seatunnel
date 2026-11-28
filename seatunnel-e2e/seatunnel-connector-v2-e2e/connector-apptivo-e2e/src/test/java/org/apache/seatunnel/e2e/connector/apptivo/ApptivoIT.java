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

package org.apache.seatunnel.e2e.connector.apptivo;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;

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
import java.net.HttpURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs provider-contract scenarios through the real Zeta engine and transactional JSON files.
 *
 * <p>The independent Java HTTPS peer verifies every path, query and credential header. Assertions
 * inspect committed rows rather than diagnostic console output or connector internals.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ApptivoIT extends TestSuiteBase {
    private static final String ONE = "/apptivo-one.conf";
    private static final String ALL = "/apptivo-all.conf";
    private static final String FIRM = "/apptivo-firm.conf";
    private static final String TERMINAL = "/apptivo-terminal.conf";
    private static final String EXTRA =
            ",\"unknown\":{\"unicode\":\"数据🌏\",\"null\":null,\"items\":[false,null,[1,2]],"
                    + "\"integer\":9223372036854775808,\"decimal\":123456789012345678.123456789}";
    private static final String[] RESOURCES = {
        "customers", "contacts", "cases", "leads", "opportunities"
    };
    private static final String[] IDS = {"customerId", "contactId", "", "id", "opportunityId"};
    private static final ObjectMapper JSON =
            new ObjectMapper()
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    @TempDir static Path directory;
    private static String trust;
    private final ExecutorService tasks = Executors.newCachedThreadPool();
    private Engine engine;
    private Future<Container.ExecResult> peer;
    private String active;

    @BeforeAll
    void startEngineAndIndependentHttpsPeer() throws Exception {
        KeyStore trusted = identity("trusted");
        identity("untrusted");
        trust = Base64.getEncoder().encodeToString(trusted.getCertificate("api").getEncoded());
        String certificate =
                Base64.getMimeEncoder(64, new byte[] {'\n'})
                        .encodeToString(trusted.getCertificate("api").getEncoded());
        Files.writeString(
                directory.resolve("trusted.pem"),
                "-----BEGIN CERTIFICATE-----\n" + certificate + "\n-----END CERTIFICATE-----\n");
        engine = new Engine(directory);
        engine.startUp();
        peer = tasks.submit(engine::runPeer);
        Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> engine.exists("ready"));
    }

    @AfterEach
    void rejectsUnexpectedProviderTraffic() throws Exception {
        if (active != null) {
            assertFalse(engine.exists(active + ".invalid"), active);
        }
    }

    @AfterAll
    void stopOwnedPeerAndEngine() throws Exception {
        try {
            if (engine != null) {
                try {
                    if (peer != null) {
                        engine.touch("stop");
                        success(peer.get(20, TimeUnit.SECONDS));
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
    void readsAllFiveDefaultResourcesAndKeepsExactNativeJson() throws Exception {
        String name = "all-five";
        List<String> script = new ArrayList<>();
        Map<String, String> expected = new HashMap<>();
        for (int index = 0; index < RESOURCES.length; index++) {
            String resource = RESOURCES[index];
            String record =
                    index == 2
                            ? caseConfig("101", EXTRA)
                            : entity(IDS[index], "9223372036854775808", ",\"firmId\":101" + EXTRA);
            script.add(
                    ok(
                            path(resource),
                            query(resource, 0),
                            index == 2 ? record : page(0, 1, record)));
            expected.put(key(resource, index == 2 ? "case-config" : "9223372036854775808"), record);
        }
        program(name, script);
        success(run(ALL, new Settings(name, "customers")));
        assertRows(name, expected);
        assertEquals(5, requests(name).size());
        for (String request : requests(name)) {
            assertFalse(request.contains("apiKey"));
            assertFalse(request.contains("accessKey"));
        }
    }

    @Test
    void paginatesAllFourEntityListsWithTheirOwnIdentifiers() throws Exception {
        for (int index : new int[] {0, 1, 3, 4}) {
            String resource = RESOURCES[index];
            String name = "pages-" + resource;
            String a = entity(IDS[index], "1", EXTRA), b = entity(IDS[index], "2", "");
            String c = entity(IDS[index], "3", ",\"firmId\":\"101\"");
            program(
                    name,
                    List.of(
                            ok(path(resource), query(resource, 0), page(0, 3, a, b)),
                            ok(path(resource), query(resource, 2), page(2, 3, c))));
            success(run(ONE, new Settings(name, resource)));
            assertRows(
                    name,
                    Map.of(key(resource, "1"), a, key(resource, "2"), b, key(resource, "3"), c));
            assertEquals(2, requests(name).size());
        }
    }

    @Test
    void continuesFullPagesWithoutTotalsAndStopsOnShortOrEmptyPages() throws Exception {
        for (boolean emptyLast : List.of(false, true)) {
            String name = "unknown-total-" + emptyLast;
            String a = entity("customerId", "1", ""), b = entity("customerId", "2", "");
            String c = entity("customerId", "3", "");
            program(
                    name,
                    List.of(
                            ok(path("customers"), query("customers", 0), page(0, -1, a, b)),
                            ok(
                                    path("customers"),
                                    query("customers", 2),
                                    emptyLast ? page(2, -1) : page(2, -1, c))));
            success(run(ONE, new Settings(name, "customers")));
            Map<String, String> expected =
                    new HashMap<>(Map.of(key("customers", "1"), a, key("customers", "2"), b));
            if (!emptyLast) {
                expected.put(key("customers", "3"), c);
            }
            assertRows(name, expected);
            assertEquals(2, requests(name).size());
        }
    }

    @Test
    void readsEmptyEntityListsAndOneCaseConfigurationWithoutPagingIt() throws Exception {
        String name = "empty-resources";
        List<String> script = new ArrayList<>();
        String config =
                "{\"status\":\"SUCCESS\",\"appName\":\"Cases\",\"statuses\":[],\"types\":[],\"priorities\":[],\"assignees\":[]}";
        for (String resource : RESOURCES) {
            script.add(
                    ok(
                            path(resource),
                            query(resource, 0),
                            resource.equals("cases") ? config : page(0, 0)));
        }
        program(name, script);
        success(run(ALL, new Settings(name, "customers")));
        assertRows(name, Map.of(key("cases", "case-config"), config));
        assertEquals(5, requests(name).size());
    }

    @Test
    void enforcesTheConfiguredTenantForRowsAndConfiguration() throws Exception {
        String record = entity("contactId", "12", ",\"firmId\":101");
        program(
                "tenant-match",
                List.of(ok(path("contacts"), query("contacts", 0), page(0, 1, record))));
        success(run(FIRM, new Settings("tenant-match", "contacts")));
        assertRows("tenant-match", Map.of(key("contacts", "12"), record));
        for (String resource : List.of("contacts", "cases")) {
            String name = "tenant-mismatch-" + resource;
            String body =
                    resource.equals("cases")
                            ? caseConfig("999", "")
                            : page(0, 1, entity("contactId", "12", ",\"firmId\":\"999\""));
            program(name, List.of(ok(path(resource), query(resource, 0), body)));
            failure(run(FIRM, new Settings(name, resource)), "Apptivo");
            assertRows(name, Map.of());
            assertEquals(1, requests(name).size());
        }
    }

    @Test
    void treatsFailureInsideHttp200AndPermanentStatusAsVisibleErrors() throws Exception {
        String resource = "contacts";
        for (int status : new int[] {200, 400, 401, 402, 403}) {
            String name = "provider-failure-" + status;
            program(
                    name,
                    List.of(
                            step(
                                    path(resource),
                                    query(resource, 0),
                                    status,
                                    "fixed",
                                    "",
                                    privateError())));
            failure(
                    run(ONE, new Settings(name, resource)),
                    status == 200 ? "provider response reported FAILURE" : "HTTP " + status);
            assertEquals(1, requests(name).size());
            assertRows(name, Map.of());
        }
    }

    @Test
    void retriesOnlyWithinTheBudgetAndHonorsTheProviderWait() throws Exception {
        String name = "retry-success", resource = "leads";
        String row = entity("id", "1", "");
        program(
                name,
                List.of(
                        step(path(resource), query(resource, 0), 429, "fixed", "1", ""),
                        step(path(resource), query(resource, 0), 503, "fixed", "0", ""),
                        ok(path(resource), query(resource, 0), page(0, 1, row))));
        success(run(ONE, new Settings(name, resource)));
        assertRows(name, Map.of(key(resource, "1"), row));
        assertEquals(3, requests(name).size());
        List<String> times = engine.lines(name + ".times");
        assertEquals(3, times.size());
        assertTrue(Long.parseLong(times.get(1)) - Long.parseLong(times.get(0)) >= 950L);
        name = "retry-exhausted";
        program(
                name,
                Collections.nCopies(
                        3, step(path(resource), query(resource, 0), 429, "fixed", "0", "")));
        failure(run(ONE, new Settings(name, resource)), "Apptivo");
        assertEquals(3, requests(name).size());
        name = "retry-beyond-deadline";
        program(name, List.of(step(path(resource), query(resource, 0), 429, "fixed", "120", "")));
        Settings deadline = new Settings(name, resource);
        deadline.deadline = 1500;
        deadline.timeout = 1000;
        failure(run(ONE, deadline), "Apptivo");
        assertEquals(1, requests(name).size());
    }

    @Test
    void rejectsContradictoryPaginationAndBoundsEndlessFullPages() throws Exception {
        String resource = "customers";
        String a = entity("customerId", "1", ""), b = entity("customerId", "2", "");
        for (String second :
                List.of(
                        page(1, 3, entity("customerId", "3", "")),
                        page(2, 4, entity("customerId", "3", "")),
                        page(2, 3))) {
            String name = "invalid-counters-" + jobId();
            program(
                    name,
                    List.of(
                            ok(path(resource), query(resource, 0), page(0, 3, a, b)),
                            ok(path(resource), query(resource, 2), second)));
            failure(
                    run(ONE, new Settings(name, resource)),
                    "pagination offset or total is inconsistent");
            assertEquals(2, requests(name).size());
        }
        String name = "page-limit";
        program(
                name,
                List.of(
                        ok(path(resource), query(resource, 0), page(0, -1, a, b)),
                        ok(
                                path(resource),
                                query(resource, 2),
                                page(
                                        2,
                                        -1,
                                        entity("customerId", "3", ""),
                                        entity("customerId", "4", "")))));
        Settings bounded = new Settings(name, resource);
        bounded.maxPages = 2;
        failure(run(ONE, bounded), "Apptivo");
        assertEquals(2, requests(name).size());
    }

    @Test
    void rejectsMalformedOrNonObjectRecordsBeforePublishingThem() throws Exception {
        String resource = "opportunities";
        for (String body :
                List.of(
                        "[]",
                        "{\"data\":{}}",
                        "{\"data\":[null]}",
                        "{\"data\":[{}]}",
                        "{\"data\":[],\"data\":[]}",
                        "{\"data\":[]} trailing")) {
            String name = "malformed-" + jobId();
            program(name, List.of(ok(path(resource), query(resource, 0), body)));
            failure(run(ONE, new Settings(name, resource)), "Apptivo");
            assertRows(name, Map.of());
            assertEquals(1, requests(name).size());
        }
    }

    @Test
    void decodesGzipAndAppliesTheLimitToDecodedResponseBytes() throws Exception {
        String resource = "customers";
        String row = entity("customerId", "1", EXTRA);
        program(
                "gzip",
                List.of(
                        step(
                                path(resource),
                                query(resource, 0),
                                200,
                                "gzip",
                                "",
                                page(0, 1, row))));
        success(run(ONE, new Settings("gzip", resource)));
        assertRows("gzip", Map.of(key(resource, "1"), row));
        for (String mode : List.of("fixed", "gzip")) {
            String name = "oversized-" + mode;
            String huge = entity("customerId", "1", ",\"text\":\"" + "x".repeat(16000) + "\"");
            program(
                    name,
                    List.of(
                            step(
                                    path(resource),
                                    query(resource, 0),
                                    200,
                                    mode,
                                    "",
                                    page(0, 1, huge))));
            Settings limit = new Settings(name, resource);
            limit.bytes = 1024;
            failure(run(ONE, limit), "Apptivo");
            assertRows(name, Map.of());
        }
    }

    @Test
    void neverForwardsQueryCredentialsThroughRedirectsOrUntrustedTls() throws Exception {
        String resource = "customers";
        program(
                "redirect",
                List.of(step(path(resource), query(resource, 0), 302, "redirect", "", "")));
        failure(run(ONE, new Settings("redirect", resource)), "HTTP 302");
        assertEquals(1, requests("redirect").size());
        for (String endpoint :
                List.of(
                        "https://" + ApptivoPeer.HOST + ":" + ApptivoPeer.UNTRUSTED_PORT,
                        "https://localhost:" + ApptivoPeer.PORT)) {
            String name = "bad-tls-" + jobId();
            program(name, List.of(ok(path(resource), query(resource, 0), page(0, 0))));
            Settings tls = new Settings(name, resource);
            tls.endpoint = endpoint;
            tls.retries = 0;
            failure(run(ONE, tls), "HTTPS certificate verification or negotiation failed");
            assertTrue(requests(name).isEmpty());
        }
    }

    @Test
    void stopsTricklingBodiesOnDeadlineAndEngineCancellation() throws Exception {
        String name = "deadline-stream", resource = "customers";
        program(name, List.of(step(path(resource), query(resource, 0), 200, "stream", "", "")));
        Settings limit = new Settings(name, resource);
        limit.deadline = 1500;
        limit.timeout = 1000;
        limit.retries = 0;
        failure(run(ONE, limit), "Apptivo");
        awaitDisconnect(name);
        long started = Long.parseLong(engine.lines(name + ".started").get(0));
        long ended = Long.parseLong(engine.lines(name + ".disconnect").get(0));
        assertTrue(ended - started < 8000L);
        name = "cancel-stream";
        program(name, List.of(step(path(resource), query(resource, 0), 200, "stream", "", "")));
        Settings cancel = new Settings(name, resource);
        cancel.timeout = 180000;
        cancel.deadline = 240000;
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(ONE, job, cancel.variables()));
        try {
            awaitRequests(name, 1);
            success(engine.cancelJob(job));
            running.get(30, TimeUnit.SECONDS);
            awaitDisconnect(name);
            assertEquals(1, requests(name).size());
        } finally {
            stopIfRunning(job, running);
        }
    }

    @Test
    void restoresTheNextPageWithoutRepeatingCommittedEntityRows() throws Exception {
        String name = "resume-page", resource = "customers";
        String a = entity("customerId", "1", ""), b = entity("customerId", "2", "");
        String c = entity("customerId", "3", "");
        program(
                name,
                List.of(
                        ok(path(resource), query(resource, 0), page(0, 3, a, b)),
                        step(path(resource), query(resource, 2), 200, "hold", "", page(2, 3, c)),
                        ok(path(resource), query(resource, 2), page(2, 3, c))));
        Settings options = held(name, resource);
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(ONE, job, options.variables()));
        try {
            awaitRequests(name, 2);
            savepoint(job, running);
            engine.touch(name + ".release");
            success(engine.restoreJob(ONE, job, options.variables()));
            assertRows(
                    name,
                    Map.of(key(resource, "1"), a, key(resource, "2"), b, key(resource, "3"), c));
            assertEquals(
                    List.of(
                            path(resource) + "?" + query(resource, 0),
                            path(resource) + "?" + query(resource, 2),
                            path(resource) + "?" + query(resource, 2)),
                    requests(name));
        } finally {
            engine.touch(name + ".release");
            stopIfRunning(job, running);
        }
    }

    @Test
    void restoresAnInterruptedResponseWithoutPublishingItsPartialJson() throws Exception {
        String name = "resume-response", resource = "contacts";
        String row = entity("contactId", "7", EXTRA);
        String body = page(0, 1, row);
        program(
                name,
                List.of(
                        step(path(resource), query(resource, 0), 200, "partial", "", body),
                        ok(path(resource), query(resource, 0), body)));
        Settings options = held(name, resource);
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(ONE, job, options.variables()));
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> engine.exists(name + ".started"));
            savepoint(job, running);
            engine.touch(name + ".release");
            success(engine.restoreJob(ONE, job, options.variables()));
            assertRows(name, Map.of(key(resource, "7"), row));
            assertEquals(
                    Collections.nCopies(2, path(resource) + "?" + query(resource, 0)),
                    requests(name));
        } finally {
            engine.touch(name + ".release");
            stopIfRunning(job, running);
        }
    }

    @Test
    void refusesChangedScanOptionsBeforeSendingRestoredRequests() throws Exception {
        String name = "resume-options", resource = "customers";
        program(
                name,
                List.of(
                        step(
                                path(resource),
                                query(resource, 0),
                                200,
                                "hold",
                                "",
                                page(0, 1, entity("customerId", "1", "")))));
        Settings options = held(name, resource);
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(ONE, job, options.variables()));
        try {
            awaitRequests(name, 1);
            savepoint(job, running);
            engine.touch(name + ".release");
            options.pageSize = 3;
            failure(
                    engine.restoreJob(ONE, job, options.variables()),
                    "checkpoint has incompatible scan options or state");
            assertEquals(1, requests(name).size());
            assertRows(name, Map.of());
        } finally {
            engine.touch(name + ".release");
            stopIfRunning(job, running);
        }
    }

    @Test
    void restoresTerminalConfigurationWithoutCallingTheProviderAgain() throws Exception {
        String name = "resume-terminal", resource = "cases";
        String config = caseConfig("101", "");
        program(name, List.of(ok(path(resource), query(resource, 0), config)));
        Settings options = new Settings(name, resource);
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(TERMINAL, job, options.variables()));
        try {
            awaitRequests(name, 1);
            savepoint(job, running);
            Future<Container.ExecResult> restored =
                    tasks.submit(() -> engine.restoreJob(TERMINAL, job, options.variables()));
            try {
                long baseline = engine.getCompletedCheckpointCount(job);
                Awaitility.await()
                        .atMost(35, TimeUnit.SECONDS)
                        .until(() -> engine.getCompletedCheckpointCount(job) > baseline + 1);
                assertEquals(1, requests(name).size());
                assertRows(name, Map.of(key(resource, "case-config"), config));
            } finally {
                stopIfRunning(job, restored);
            }
        } finally {
            stopIfRunning(job, running);
        }
    }

    @Test
    void rotatesCredentialsWithoutLosingTheCheckpointTenantGuard() throws Exception {
        for (boolean conflictingFirm : List.of(false, true)) {
            String name = "rotate-credentials-" + conflictingFirm, resource = "customers";
            String a = entity("customerId", "1", ",\"firmId\":101");
            String b = entity("customerId", "2", ",\"firmId\":101");
            String c = entity("customerId", "3", ",\"firmId\":" + (conflictingFirm ? 999 : 101));
            String original = tenantPage("101", 0, 3, a, b);
            String heldPage = tenantPage("101", 2, 3, entity("customerId", "3", ",\"firmId\":101"));
            String restoredPage = tenantPage(conflictingFirm ? "999" : "101", 2, 3, c);
            program(
                    name,
                    List.of(
                            ok(path(resource), query(resource, 0), original),
                            step(path(resource), query(resource, 2), 200, "hold", "", heldPage),
                            ok(path(resource), query(resource, 2), restoredPage)));
            Settings options = held(name, resource);
            String job = jobId();
            Future<Container.ExecResult> running =
                    tasks.submit(() -> engine.executeJob(ONE, job, options.variables()));
            try {
                awaitRequests(name, 2);
                savepoint(job, running);
                engine.touch(name + ".release");
                options.token = "synthetic-apptivo-rotated-api-key";
                options.access = "synthetic-apptivo-rotated-access-key";
                Path keys = Files.createTempFile(directory, "rotated-keys-", ".txt");
                Files.write(keys, List.of(options.token, options.access), StandardCharsets.UTF_8);
                engine.install(keys, name + ".credentials");
                Container.ExecResult restored = engine.restoreJob(ONE, job, options.variables());
                if (conflictingFirm) {
                    failure(restored, "response firmId does not match the scan tenant");
                    assertRows(name, Map.of(key(resource, "1"), a, key(resource, "2"), b));
                } else {
                    success(restored);
                    assertRows(
                            name,
                            Map.of(
                                    key(resource, "1"),
                                    a,
                                    key(resource, "2"),
                                    b,
                                    key(resource, "3"),
                                    c));
                }
                assertEquals(3, requests(name).size());
            } finally {
                engine.touch(name + ".release");
                stopIfRunning(job, running);
            }
        }
    }

    @Test
    void restoresAcrossResourceBoundariesWithoutRediscoveringCompletedResources() throws Exception {
        String name = "resume-resource";
        String customer = entity("customerId", "1", ",\"firmId\":101");
        String contact = entity("contactId", "2", ",\"firmId\":101");
        String config = caseConfig("101", "");
        String lead = entity("id", "3", "");
        String opportunity = entity("opportunityId", "4", "");
        program(
                name,
                List.of(
                        ok(path("customers"), query("customers", 0), page(0, 1, customer)),
                        step(
                                path("contacts"),
                                query("contacts", 0),
                                200,
                                "hold",
                                "",
                                page(0, 1, contact)),
                        ok(path("contacts"), query("contacts", 0), page(0, 1, contact)),
                        ok(path("cases"), query("cases", 0), config),
                        ok(path("leads"), query("leads", 0), page(0, 1, lead)),
                        ok(
                                path("opportunities"),
                                query("opportunities", 0),
                                page(0, 1, opportunity))));
        Settings options = held(name, "customers");
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(ALL, job, options.variables()));
        try {
            awaitRequests(name, 2);
            savepoint(job, running);
            engine.touch(name + ".release");
            success(engine.restoreJob(ALL, job, options.variables()));
            assertRows(
                    name,
                    Map.of(
                            key("customers", "1"),
                            customer,
                            key("contacts", "2"),
                            contact,
                            key("cases", "case-config"),
                            config,
                            key("leads", "3"),
                            lead,
                            key("opportunities", "4"),
                            opportunity));
            assertEquals(6, requests(name).size());
            assertEquals(
                    1L,
                    requests(name).stream()
                            .filter(line -> line.startsWith(path("customers") + "?"))
                            .count());
        } finally {
            engine.touch(name + ".release");
            stopIfRunning(job, running);
        }
    }

    private void assertRows(String name, Map<String, String> expected) throws Exception {
        Map<String, JsonNode> actual = new HashMap<>();
        for (String line : engine.rows(name)) {
            JsonNode row = JSON.readTree(line);
            assertEquals(Set.of("resource", "id", "record"), fieldNames(row));
            String id = key(row.path("resource").asText(), row.path("id").asText());
            assertNull(actual.put(id, row), "duplicate committed record");
        }
        assertEquals(expected.keySet(), actual.keySet());
        for (Map.Entry<String, String> entry : expected.entrySet()) {
            String record = actual.get(entry.getKey()).path("record").asText();
            assertEquals(JSON.readTree(entry.getValue()), JSON.readTree(record));
            if (entry.getValue().contains("9223372036854775808")) {
                assertTrue(record.contains("9223372036854775808"));
                assertTrue(record.contains("123456789012345678.123456789"));
            }
        }
    }

    private static Set<String> fieldNames(JsonNode row) {
        Set<String> names = new HashSet<>();
        row.fieldNames().forEachRemaining(names::add);
        return names;
    }

    private static Settings held(String name, String resource) {
        Settings settings = new Settings(name, resource);
        settings.timeout = 180000;
        settings.deadline = 240000;
        return settings;
    }

    private static String key(String resource, String id) {
        return resource + "/" + id;
    }

    private static String path(String resource) {
        return "/app/dao/v6/" + resource;
    }

    private static String query(String resource, int offset) {
        return resource.equals("cases")
                ? "a=getConfigData"
                : "a=getAll&numRecords=2&startIndex=" + offset;
    }

    private static String entity(String idField, String id, String extra) {
        return "{\"" + idField + "\":" + id + extra + "}";
    }

    private static String caseConfig(String firm, String extra) {
        return "{\"status\":\"SUCCESS\",\"appName\":\"Cases\",\"firmId\":"
                + firm
                + ",\"statuses\":[{\"statusId\":1,\"statusName\":\"Open\"}],\"types\":[],"
                + "\"priorities\":[],\"assignees\":[]"
                + extra
                + "}";
    }

    private static String page(int offset, int total, String... rows) {
        return "{\"status\":\"SUCCESS\",\"startIndex\":"
                + offset
                + (total < 0 ? "" : ",\"totalRecords\":" + total)
                + ",\"data\":["
                + String.join(",", rows)
                + "]}";
    }

    private static String tenantPage(String firm, int offset, int total, String... rows) {
        String body = page(offset, total, rows);
        return body.substring(0, body.length() - 1) + ",\"firmId\":" + firm + "}";
    }

    private Container.ExecResult run(String config, Settings options) throws Exception {
        return engine.executeJob(config, jobId(), options.variables());
    }

    private void program(String name, List<String> lines) throws Exception {
        rejectsUnexpectedProviderTraffic();
        Path script = Files.createTempFile(directory, "steps-", ".txt");
        Files.write(script, lines, StandardCharsets.UTF_8);
        engine.install(script, name + ".program");
        Path current = Files.createTempFile(directory, "active-", ".txt");
        Files.writeString(current, name);
        engine.install(current, "active");
        active = name;
    }

    private List<String> requests(String name) throws Exception {
        return engine.lines(name + ".requests");
    }

    private void awaitRequests(String name, int count) {
        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> requests(name).size() == count);
    }

    private void awaitDisconnect(String name) {
        Awaitility.await()
                .atMost(15, TimeUnit.SECONDS)
                .until(() -> engine.exists(name + ".disconnect"));
    }

    private void savepoint(String job, Future<Container.ExecResult> running) throws Exception {
        long baseline = engine.getCompletedCheckpointCount(job);
        Awaitility.await()
                .atMost(35, TimeUnit.SECONDS)
                .until(() -> engine.getCompletedCheckpointCount(job) > baseline);
        success(engine.savepointJob(job));
        running.get(30, TimeUnit.SECONDS);
    }

    private void stopIfRunning(String job, Future<Container.ExecResult> running) throws Exception {
        if (!running.isDone()) {
            engine.cancelJob(job);
            running.cancel(true);
        }
    }

    private static String privateError() {
        return "{\"status\":\"FAILURE\",\"reason\":\"private-provider-message "
                + ApptivoPeer.TOKEN
                + " "
                + ApptivoPeer.ACCESS_KEY
                + "\"}";
    }

    private static String ok(String path, String query, String body) {
        return step(path, query, 200, "fixed", "", body);
    }

    private static String step(
            String path, String query, int status, String mode, String retry, String body) {
        return String.join(
                "\t",
                path,
                query,
                Integer.toString(status),
                mode,
                retry,
                Base64.getEncoder().encodeToString(body.getBytes(StandardCharsets.UTF_8)));
    }

    private static String jobId() {
        return Long.toString(
                ThreadLocalRandom.current().nextLong(1_000_000_000_000L, 9_000_000_000_000L));
    }

    private static String text(Container.ExecResult result) {
        return result.getStdout() + result.getStderr();
    }

    private static void success(Container.ExecResult result) {
        assertEquals(0, result.getExitCode(), () -> text(result));
    }

    private static void failure(Container.ExecResult result, String fragment) {
        assertNotEquals(0, result.getExitCode(), () -> text(result));
        assertTrue(text(result).contains(fragment), () -> text(result));
        assertFalse(text(result).contains("synthetic-apptivo"));
        assertFalse(text(result).contains("private-provider-message"));
    }

    private static KeyStore identity(String name) throws Exception {
        String keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
        Path file = directory.resolve(name + ".p12");
        Process process =
                new ProcessBuilder(
                                keytool,
                                "-genkeypair",
                                "-alias",
                                "api",
                                "-keyalg",
                                "RSA",
                                "-keysize",
                                "2048",
                                "-validity",
                                "7",
                                "-dname",
                                "CN=" + ApptivoPeer.HOST,
                                "-ext",
                                "SAN=dns:" + ApptivoPeer.HOST,
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                file.toString(),
                                "-storepass",
                                ApptivoPeer.PASSWORD,
                                "-keypass",
                                ApptivoPeer.PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, process.waitFor(), output);
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(file)) {
            store.load(input, ApptivoPeer.PASSWORD.toCharArray());
        }
        return store;
    }

    /**
     * Supplies scoped read limits and one scenario's output location.
     *
     * <p>Credentials contain reserved URI characters so the peer validates actual query encoding.
     */
    private static final class Settings {
        private final String name;
        private final String resource;
        private String endpoint = "https://" + ApptivoPeer.HOST + ":" + ApptivoPeer.PORT;
        private String firm = "101";
        private String token = ApptivoPeer.TOKEN;
        private String access = ApptivoPeer.ACCESS_KEY;
        private int bytes = 4194304;
        private int maxPages = 1000;
        private int retries = 2;
        private int timeout = 30000;
        private int deadline = 120000;
        private int pageSize = 2;

        private Settings(String name, String resource) {
            this.name = name;
            this.resource = resource;
        }

        private String[] variables() {
            return new String[] {
                "api.output=" + name,
                "api.resource=" + resource,
                "api.token=" + token,
                "api.access_key=" + access,
                "api.origin=" + endpoint,
                "api.trust=" + trust,
                "api.firm=" + firm,
                "api.page_size=" + pageSize,
                "api.max_pages=" + maxPages,
                "api.max_bytes=" + bytes,
                "api.retries=" + retries,
                "api.timeout=" + timeout,
                "api.deadline=" + deadline
            };
        }
    }

    /**
     * Stages the independently compiled peer in the ordinary Java 11 engine container.
     *
     * <p>The connector receives an explicit PEM anchor; the JVM's global trust store is unchanged.
     */
    private static final class Engine extends SeaTunnelContainer {
        private static final String ROOT = "/tmp/apptivo-peer";
        private final Path fixtures;

        private Engine(Path fixtures) {
            this.fixtures = fixtures;
        }

        /**
         * Preserves reserved credential characters as argv without logging command values.
         *
         * <p>The inherited shell join would interpret the ampersand in this fixture's keys.
         */
        @Override
        protected Container.ExecResult executeCommand(
                GenericContainer<?> container, List<String> command)
                throws IOException, InterruptedException {
            return container.execInContainer(command.toArray(new String[0]));
        }

        @Override
        protected void executeExtraCommands(GenericContainer<?> container)
                throws IOException, InterruptedException {
            super.executeExtraCommands(container);
            container.withExtraHost(ApptivoPeer.HOST, "127.0.0.1");
            for (String name : List.of("trusted.p12", "untrusted.p12", "trusted.pem")) {
                container.withCopyFileToContainer(
                        MountableFile.forHostPath(fixtures.resolve(name)), ROOT + "/" + name);
            }
            try {
                String file = "ApptivoPeer.class";
                Path source = Path.of(ApptivoPeer.class.getResource(file).toURI());
                container.withCopyFileToContainer(
                        MountableFile.forHostPath(source),
                        ROOT
                                + "/classes/"
                                + ApptivoPeer.class.getPackage().getName().replace('.', '/')
                                + "/"
                                + file);
            } catch (URISyntaxException e) {
                throw new IOException("Cannot stage the Java HTTPS fixture", e);
            }
        }

        private Container.ExecResult runPeer() throws Exception {
            return server.execInContainer(
                    "java", "-cp", ROOT + "/classes", ApptivoPeer.class.getName(), ROOT);
        }

        /**
         * Includes checkpoints from the still-running anchor pipeline after the source finishes.
         *
         * <p>The base helper reads only the first pipeline, which is terminal in this scenario.
         */
        @Override
        public long getCompletedCheckpointCount(String jobId) {
            HttpURLConnection connection = null;
            try {
                URL url =
                        new URL(
                                "http",
                                server.getHost(),
                                server.getMappedPort(8080),
                                "/jobs/checkpoints/" + jobId);
                connection = (HttpURLConnection) url.openConnection();
                connection.setConnectTimeout(5000);
                connection.setReadTimeout(5000);
                if (connection.getResponseCode() != 200) {
                    return 0L;
                }
                try (InputStream input = connection.getInputStream()) {
                    long completed = 0L;
                    for (JsonNode pipeline : JSON.readTree(input).path("pipelines")) {
                        completed += pipeline.path("counts").path("completed").asLong();
                    }
                    return completed;
                }
            } catch (IOException e) {
                throw new IllegalStateException("Cannot inspect the test job's checkpoints", e);
            } finally {
                if (connection != null) {
                    connection.disconnect();
                }
            }
        }

        private void install(Path source, String name) {
            server.copyFileToContainer(MountableFile.forHostPath(source), ROOT + "/" + name);
        }

        private boolean exists(String name) throws Exception {
            return server.execInContainer("test", "-f", ROOT + "/" + name).getExitCode() == 0;
        }

        private void touch(String name) throws Exception {
            success(server.execInContainer("touch", ROOT + "/" + name));
        }

        private List<String> lines(String name) throws Exception {
            if (!exists(name)) {
                return Collections.emptyList();
            }
            Container.ExecResult result = server.execInContainer("cat", ROOT + "/" + name);
            success(result);
            return split(result.getStdout());
        }

        private List<String> rows(String name) throws Exception {
            String path = "/tmp/seatunnel_mnt/apptivo-" + name;
            Container.ExecResult result =
                    server.execInContainer(
                            "sh",
                            "-c",
                            "if [ -d "
                                    + path
                                    + " ]; then find "
                                    + path
                                    + " -maxdepth 1 -type f -name 'receipt_*'"
                                    + " -exec sh -c 'for f do cat \"$f\"; printf \"\\n\"; done' sh {} +; fi");
            success(result);
            return split(result.getStdout());
        }

        private static List<String> split(String value) {
            List<String> lines = new ArrayList<>();
            for (String line : value.split("\\r?\\n")) {
                if (!line.isBlank()) {
                    lines.add(line);
                }
            }
            return lines;
        }
    }
}
