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

package org.apache.seatunnel.e2e.connector.auth0;

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
import java.util.Locale;
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
class Auth0IT extends TestSuiteBase {
    private static final String ONE = "/auth0-one.conf";
    private static final String ALL = "/auth0-all.conf";
    private static final String OAUTH = "/auth0-oauth.conf";
    private static final String TERMINAL = "/auth0-terminal.conf";
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

    private static final String EXTRA =
            ",\"unknown\":{\"unicode\":\"数据🌏\",\"null\":null,\"items\":[false,null,[1,2]],"
                    + "\"integer\":9223372036854775808,\"decimal\":123456789012345678.123456789}";

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
    void readsAllFiveResourcesWithExactNullableContextsAndPayloads() throws Exception {
        String name = "all-five";
        String client =
                "{\"client_id\":\"client-1\",\"client_secret\":\"selected-export-field\""
                        + EXTRA
                        + "}";
        String user = user("auth0|user-1", 1);
        String org = organization("org_1");
        String member =
                "{\"user_id\":\"auth0|user-1\",\"email\":\"one@example.invalid\"" + EXTRA + "}";
        String role = "{\"id\":\"rol_1\",\"name\":\"Reader\"" + EXTRA + "}";
        program(
                name,
                List.of(
                        ok("/api/v2/clients", offset(0, 2), array(client)),
                        ok("/api/v2/users", users(0, 3, 0, 0, true), count(1)),
                        ok("/api/v2/users", users(0, 3, 0, 2, false), array(user)),
                        ok("/api/v2/organizations", "take=2", envelope("organizations", null, org)),
                        ok(
                                "/api/v2/organizations/org_1/members",
                                "take=2",
                                envelope("members", null, member)),
                        ok(
                                "/api/v2/organizations/org_1/members/auth0%7Cuser-1/roles",
                                offset(0, 2), array(role))));
        success(run(ALL, new Settings(name, "clients")));
        assertRows(
                name,
                Map.of(
                        key("clients", "client-1", null, null),
                        expected(null, null, client),
                        key("users", "auth0|user-1", null, null),
                        expected(null, null, user),
                        key("organizations", "org_1", null, null),
                        expected(null, null, org),
                        key("organization_members", "auth0|user-1", "org_1", "auth0|user-1"),
                        expected("org_1", "auth0|user-1", member),
                        key("organization_member_roles", "rol_1", "org_1", "auth0|user-1"),
                        expected("org_1", "auth0|user-1", role)));
        assertEquals(6, requests(name).size());
    }

    @Test
    void keepsOrganizationAndMemberLineageAcrossEmptyCheckpointPages() throws Exception {
        String name = "nested-memberships";
        String one = "{\"user_id\":\"auth0|user/1\"}";
        String two = "{\"user_id\":\"auth0|user-2\"}";
        String role = "{\"id\":\"rol_shared\",\"name\":\"Shared\"}";
        String other = "{\"id\":\"rol_other\",\"name\":\"Other\"}";
        program(
                name,
                List.of(
                        ok(
                                "/api/v2/organizations",
                                "take=2",
                                envelope(
                                        "organizations",
                                        null,
                                        organization("org_1"),
                                        organization("org_2"))),
                        ok(
                                "/api/v2/organizations/org_1/members",
                                "take=2",
                                envelope("members", "cursor+/=", one)),
                        ok(
                                "/api/v2/organizations/org_1/members/auth0%7Cuser%2F1/roles",
                                offset(0, 2), array(role)),
                        ok(
                                "/api/v2/organizations/org_1/members",
                                "from=cursor%2B%2F%3D&take=2",
                                envelope("members", "after-empty")),
                        ok(
                                "/api/v2/organizations/org_1/members",
                                "from=after-empty&take=2",
                                envelope("members", null, two)),
                        ok(
                                "/api/v2/organizations/org_1/members/auth0%7Cuser-2/roles",
                                offset(0, 2), array(other)),
                        ok(
                                "/api/v2/organizations/org_2/members",
                                "take=2",
                                envelope("members", null, one)),
                        ok(
                                "/api/v2/organizations/org_2/members/auth0%7Cuser%2F1/roles",
                                offset(0, 2), array(role))));
        success(run(ONE, new Settings(name, "organization_member_roles")));
        assertRows(
                name,
                Map.of(
                        key("organization_member_roles", "rol_shared", "org_1", "auth0|user/1"),
                        expected("org_1", "auth0|user/1", role),
                        key("organization_member_roles", "rol_other", "org_1", "auth0|user-2"),
                        expected("org_1", "auth0|user-2", other),
                        key("organization_member_roles", "rol_shared", "org_2", "auth0|user/1"),
                        expected("org_2", "auth0|user/1", role)));
        assertEquals(8, requests(name).size());
    }

    @Test
    void readsMoreThanOneThousandOrganizationsAndMembersUsingCheckpointPagination()
            throws Exception {
        for (boolean members : List.of(false, true)) {
            String name = "large-" + (members ? "members" : "organizations");
            String resource = members ? "organization_members" : "organizations";
            List<String> script = new ArrayList<>();
            Map<String, List<String>> expected = new HashMap<>();
            if (members) {
                script.add(
                        ok(
                                "/api/v2/organizations",
                                "take=100",
                                envelope("organizations", null, organization("org_parent"))));
            }
            for (int page = 0; page < 11; page++) {
                List<String> rows = new ArrayList<>();
                for (int index = page * 100; index < Math.min(1001, (page + 1) * 100); index++) {
                    String id = members ? "auth0|member-" + index : "org_" + index;
                    String row =
                            members
                                    ? "{\"user_id\":\"" + id + "\",\"name\":\"Member\"}"
                                    : organization(id);
                    rows.add(row);
                    String org = members ? "org_parent" : null, user = members ? id : null;
                    expected.put(key(resource, id, org, user), expected(org, user, row));
                }
                script.add(
                        ok(
                                members
                                        ? "/api/v2/organizations/org_parent/members"
                                        : "/api/v2/organizations",
                                "take=100" + (page == 0 ? "" : "&from=page-" + page),
                                envelope(
                                        members ? "members" : "organizations",
                                        page == 10 ? null : "page-" + (page + 1),
                                        rows.toArray(new String[0]))));
            }
            program(name, script);
            Settings options = new Settings(name, resource);
            options.pageSize = 100;
            success(run(ONE, options));
            assertRows(name, expected);
            assertEquals(members ? 12 : 11, requests(name).size());
        }
    }

    @Test
    void bisectsSaturatedUserWindowsAndIncludesEveryMillisecondBoundary() throws Exception {
        String name = "split-users";
        List<String> script = new ArrayList<>();
        Map<String, List<String>> expected = new HashMap<>();
        script.add(ok("/api/v2/users", users(0, 3, 0, 0, true), count(1000)));
        for (int half = 0; half < 2; half++) {
            int first = half * 2;
            script.add(ok("/api/v2/users", users(first, first + 1, 0, 0, true), count(500)));
            for (int page = 0; page < 5; page++) {
                List<String> rows = new ArrayList<>();
                for (int index = page * 100; index < (page + 1) * 100; index++) {
                    String id = "auth0|" + (half * 500 + index);
                    String row = user(id, first + (index >= 250 ? 1 : 0));
                    rows.add(row);
                    expected.put(key("users", id, null, null), expected(null, null, row));
                }
                script.add(
                        ok(
                                "/api/v2/users",
                                users(first, first + 1, page, 100, false),
                                array(rows.toArray(new String[0]))));
            }
        }
        program(name, script);
        Settings options = new Settings(name, "users");
        options.pageSize = 100;
        success(run(ONE, options));
        assertRows(name, expected);
        assertEquals(13, requests(name).size());
    }

    @Test
    void rejectsOneMillisecondDensityAtTheSearchCapWithoutPublishingATruncatedSubset()
            throws Exception {
        String name = "dense-millisecond";
        program(name, List.of(ok("/api/v2/users", users(0, 0, 0, 0, true), count(1000))));
        Settings options = new Settings(name, "users");
        options.end = options.start;
        failure(
                run(ONE, options),
                "user search reaches the 1000-result cap within one millisecond");
        assertRows(name, Map.of());
        assertEquals(1, requests(name).size());
    }

    @Test
    void bindsUserPageCountsAndUpdatedTimesToTheProbedWindow() throws Exception {
        String a = user("auth0|a", 0), b = user("auth0|b", 1), c = user("auth0|c", 3);
        String name = "user-pages";
        program(
                name,
                List.of(
                        ok("/api/v2/users", users(0, 3, 0, 0, true), count(3)),
                        ok("/api/v2/users", users(0, 3, 0, 2, false), array(a, b)),
                        ok("/api/v2/users", users(0, 3, 1, 2, false), array(c))));
        success(run(ONE, new Settings(name, "users")));
        assertRows(
                name,
                Map.of(
                        key("users", "auth0|a", null, null),
                        expected(null, null, a),
                        key("users", "auth0|b", null, null),
                        expected(null, null, b),
                        key("users", "auth0|c", null, null),
                        expected(null, null, c)));
        for (String body : List.of(array(user("auth0|out", 4)), "[]")) {
            name = "bad-user-page-" + jobId();
            program(
                    name,
                    List.of(
                            ok("/api/v2/users", users(0, 3, 0, 0, true), count(1)),
                            ok("/api/v2/users", users(0, 3, 0, 2, false), body)));
            failure(run(ONE, new Settings(name, "users")), "Auth0");
            assertRows(name, Map.of());
        }
    }

    @Test
    void issuesTheClientCredentialsGrantWithAudienceAndRenewsOnlyOnceAfterUnauthorized()
            throws Exception {
        String name = "oauth-renewal";
        String client = "{\"client_id\":\"client-1\"}";
        program(
                name,
                List.of(
                        ok("/oauth/token", "", grant(Auth0Peer.TOKEN, 3600)),
                        step("/api/v2/clients", offset(0, 2), 401, "fixed", "", privateError()),
                        ok("/oauth/token", "", grant(Auth0Peer.RENEWED, 3600)),
                        renewed(ok("/api/v2/clients", offset(0, 2), array(client)))));
        success(run(OAUTH, new Settings(name, "clients")));
        assertRows(
                name, Map.of(key("clients", "client-1", null, null), expected(null, null, client)));
        assertEquals(
                List.of(
                        "POST /oauth/token",
                        "/api/v2/clients?" + offset(0, 2),
                        "POST /oauth/token",
                        "/api/v2/clients?" + offset(0, 2)),
                requests(name));
        name = "oauth-rejected-twice";
        program(
                name,
                List.of(
                        ok("/oauth/token", "", grant(Auth0Peer.TOKEN, 3600)),
                        step("/api/v2/clients", offset(0, 2), 401, "fixed", "", privateError()),
                        ok("/oauth/token", "", grant(Auth0Peer.RENEWED, 3600)),
                        renewed(
                                step(
                                        "/api/v2/clients",
                                        offset(0, 2),
                                        401,
                                        "fixed",
                                        "",
                                        privateError()))));
        failure(run(OAUTH, new Settings(name, "clients")), "HTTP 401 after OAuth renewal");
        assertEquals(4, requests(name).size());
    }

    @Test
    void retriesTheTokenGrantOnlyAfterAnExplicitRateLimit() throws Exception {
        String rejected = "grant-503";
        program(rejected, List.of(step("/oauth/token", "", 503, "fixed", "", privateError())));
        failure(run(OAUTH, new Settings(rejected, "clients")), "Auth0 token exchange: HTTP 503");
        assertEquals(List.of("POST /oauth/token"), requests(rejected));
        assertRows(rejected, Map.of());

        String throttled = "grant-429";
        String row = client("client-1");
        program(
                throttled,
                List.of(
                        step("/oauth/token", "", 429, "fixed", "0", ""),
                        ok("/oauth/token", "", grant(Auth0Peer.TOKEN, 3600)),
                        ok("/api/v2/clients", offset(0, 2), array(row))));
        success(run(OAUTH, new Settings(throttled, "clients")));
        assertEquals(
                List.of(
                        "POST /oauth/token",
                        "POST /oauth/token",
                        "/api/v2/clients?" + offset(0, 2)),
                requests(throttled));
        assertRows(
                throttled,
                Map.of(key("clients", "client-1", null, null), expected(null, null, row)));
    }

    @Test
    void rejectsMalformedOauthResponsesBeforeCallingTheManagementApi() throws Exception {
        for (String body :
                List.of(
                        "{}",
                        grant(Auth0Peer.TOKEN, 0),
                        "{\"access_token\":\""
                                + Auth0Peer.TOKEN
                                + "\",\"token_type\":\"Basic\",\"expires_in\":3600}")) {
            String name = "invalid-grant-" + jobId();
            program(name, List.of(ok("/oauth/token", "", body)));
            failure(
                    run(OAUTH, new Settings(name, "clients")),
                    "invalid client credentials response");
            assertRows(name, Map.of());
            assertEquals(List.of("POST /oauth/token"), requests(name));
        }
    }

    @Test
    void renewsAnExpiredGrantBeforeReadingTheNextManagementPage() throws Exception {
        String name = "oauth-expiry";
        String a = client("client-1"), b = client("client-2"), c = client("client-3");
        program(
                name,
                List.of(
                        ok("/oauth/token", "", grant(Auth0Peer.TOKEN, 1)),
                        step("/api/v2/clients", offset(0, 2), 200, "delay", "", array(a, b)),
                        ok("/oauth/token", "", grant(Auth0Peer.RENEWED, 3600)),
                        renewed(ok("/api/v2/clients", offset(1, 2), array(c)))));
        success(run(OAUTH, new Settings(name, "clients")));
        assertRows(
                name,
                Map.of(
                        key("clients", "client-1", null, null),
                        expected(null, null, a),
                        key("clients", "client-2", null, null),
                        expected(null, null, b),
                        key("clients", "client-3", null, null),
                        expected(null, null, c)));
        assertEquals(4, requests(name).size());
    }

    @Test
    void reportsPermanentErrorsWithoutRenewingAStaticTokenOrLeakingPayloads() throws Exception {
        for (int status : new int[] {200, 400, 401, 403, 404}) {
            String name = "permanent-" + status;
            program(
                    name,
                    List.of(
                            step(
                                    "/api/v2/clients",
                                    offset(0, 2),
                                    status,
                                    "fixed",
                                    "",
                                    privateError())));
            failure(
                    run(ONE, new Settings(name, "clients")),
                    status == 200 ? "Auth0" : "HTTP " + status);
            assertRows(name, Map.of());
            assertEquals(1, requests(name).size());
        }
    }

    @Test
    void retriesOnlyWithinTheBudgetAndHonorsRetryAfter() throws Exception {
        String name = "retry-success";
        String row = client("client-1");
        program(
                name,
                List.of(
                        step("/api/v2/clients", offset(0, 2), 429, "fixed", "1", ""),
                        step("/api/v2/clients", offset(0, 2), 503, "fixed", "0", ""),
                        ok("/api/v2/clients", offset(0, 2), array(row))));
        success(run(ONE, new Settings(name, "clients")));
        assertRows(name, Map.of(key("clients", "client-1", null, null), expected(null, null, row)));
        List<String> times = engine.lines(name + ".times");
        assertEquals(3, times.size());
        assertTrue(Long.parseLong(times.get(1)) - Long.parseLong(times.get(0)) >= 950L);
        name = "retry-exhausted";
        program(
                name,
                Collections.nCopies(
                        3, step("/api/v2/clients", offset(0, 2), 429, "fixed", "0", "")));
        failure(run(ONE, new Settings(name, "clients")), "Auth0");
        assertEquals(3, requests(name).size());
        name = "retry-deadline";
        program(name, List.of(step("/api/v2/clients", offset(0, 2), 429, "fixed", "120", "")));
        Settings options = new Settings(name, "clients");
        options.timeout = 1000;
        options.deadline = 1500;
        failure(run(ONE, options), "Auth0");
        assertEquals(1, requests(name).size());
    }

    @Test
    void decodesGzipAndBoundsDecodedBytesBeforePublishingRecords() throws Exception {
        String name = "gzip", row = client("client-1");
        program(name, List.of(step("/api/v2/clients", offset(0, 2), 200, "gzip", "", array(row))));
        success(run(ONE, new Settings(name, "clients")));
        assertRows(name, Map.of(key("clients", "client-1", null, null), expected(null, null, row)));
        for (String mode : List.of("fixed", "gzip")) {
            name = "oversized-" + mode;
            String huge = "{\"client_id\":\"client-1\",\"padding\":\"" + "x".repeat(16000) + "\"}";
            program(
                    name,
                    List.of(step("/api/v2/clients", offset(0, 2), 200, mode, "", array(huge))));
            Settings bounded = new Settings(name, "clients");
            bounded.bytes = 1024;
            failure(run(ONE, bounded), "max_response_bytes");
            assertRows(name, Map.of());
        }
    }

    @Test
    void neverFollowsRedirectsOrDisablesCertificateAndHostnameVerification() throws Exception {
        String name = "redirect";
        program(name, List.of(step("/api/v2/clients", offset(0, 2), 302, "redirect", "", "")));
        failure(run(ONE, new Settings(name, "clients")), "HTTP 302");
        assertEquals(1, requests(name).size());
        for (String endpoint :
                List.of(
                        "https://" + Auth0Peer.HOST + ":" + Auth0Peer.UNTRUSTED_PORT,
                        "https://localhost:" + Auth0Peer.PORT)) {
            name = "bad-tls-" + jobId();
            program(name, List.of(ok("/api/v2/clients", offset(0, 2), "[]")));
            Settings tls = new Settings(name, "clients");
            tls.endpoint = endpoint;
            tls.retries = 0;
            failure(run(ONE, tls), "HTTPS certificate verification or negotiation failed");
            assertTrue(requests(name).isEmpty());
        }
    }

    @Test
    void closesTricklingResponsesAtDeadlineAndOnEngineCancellation() throws Exception {
        String name = "deadline-stream";
        program(name, List.of(step("/api/v2/clients", offset(0, 2), 200, "stream", "", "")));
        Settings limit = new Settings(name, "clients");
        limit.timeout = 1000;
        limit.deadline = 1500;
        limit.retries = 0;
        failure(run(ONE, limit), "Auth0");
        awaitDisconnect(name);
        long start = Long.parseLong(engine.lines(name + ".started").get(0));
        long end = Long.parseLong(engine.lines(name + ".disconnect").get(0));
        assertTrue(end - start < 8000L);
        name = "cancel-stream";
        program(name, List.of(step("/api/v2/clients", offset(0, 2), 200, "stream", "", "")));
        Settings cancel = held(name, "clients");
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
    void restoresTheNextClientPageWithoutRepeatingCommittedClients() throws Exception {
        String name = "resume-clients";
        String a = client("client-1"), b = client("client-2"), c = client("client-3");
        program(
                name,
                List.of(
                        ok("/api/v2/clients", offset(0, 2), array(a, b)),
                        step("/api/v2/clients", offset(1, 2), 200, "hold", "", array(c)),
                        ok("/api/v2/clients", offset(1, 2), array(c))));
        Settings options = held(name, "clients");
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
                    Map.of(
                            key("clients", "client-1", null, null),
                            expected(null, null, a),
                            key("clients", "client-2", null, null),
                            expected(null, null, b),
                            key("clients", "client-3", null, null),
                            expected(null, null, c)));
            assertEquals(
                    List.of(
                            "/api/v2/clients?" + offset(0, 2),
                            "/api/v2/clients?" + offset(1, 2),
                            "/api/v2/clients?" + offset(1, 2)),
                    requests(name));
        } finally {
            engine.touch(name + ".release");
            stopIfRunning(job, running);
        }
    }

    @Test
    void restoresTheNextUserPageWithoutRecountingItsActiveWindow() throws Exception {
        String name = "resume-user-page";
        String first = user("auth0|first", 0);
        String second = user("auth0|second", 1);
        String last = user("auth0|last", 3);
        String next = users(0, 3, 1, 2, false);
        program(
                name,
                List.of(
                        ok("/api/v2/users", users(0, 3, 0, 0, true), count(3)),
                        ok("/api/v2/users", users(0, 3, 0, 2, false), array(first, second)),
                        step("/api/v2/users", next, 200, "hold", "", array(last)),
                        ok("/api/v2/users", next, array(last))));
        Settings options = held(name, "users");
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(ONE, job, options.variables()));
        try {
            awaitRequests(name, 3);
            savepoint(job, running);
            engine.touch(name + ".release");
            success(engine.restoreJob(ONE, job, options.variables()));
            assertRows(
                    name,
                    Map.of(
                            key("users", "auth0|first", null, null), expected(null, null, first),
                            key("users", "auth0|second", null, null), expected(null, null, second),
                            key("users", "auth0|last", null, null), expected(null, null, last)));
            assertEquals(
                    List.of(
                            "/api/v2/users?" + users(0, 3, 0, 0, true),
                            "/api/v2/users?" + users(0, 3, 0, 2, false),
                            "/api/v2/users?" + next,
                            "/api/v2/users?" + next),
                    requests(name));
        } finally {
            engine.touch(name + ".release");
            stopIfRunning(job, running);
        }
    }

    @Test
    void restoresRoleReadsWithTheSameOrganizationAndMemberAncestors() throws Exception {
        String name = "resume-roles";
        String org = envelope("organizations", null, organization("org_1"));
        String member = envelope("members", null, "{\"user_id\":\"auth0|user-1\"}");
        String role = "{\"id\":\"rol_1\",\"name\":\"Reader\"}";
        String rolePath = "/api/v2/organizations/org_1/members/auth0%7Cuser-1/roles";
        program(
                name,
                List.of(
                        ok("/api/v2/organizations", "take=2", org),
                        ok("/api/v2/organizations/org_1/members", "take=2", member),
                        step(rolePath, offset(0, 2), 200, "hold", "", array(role)),
                        ok("/api/v2/organizations", "take=2", org),
                        ok("/api/v2/organizations/org_1/members", "take=2", member),
                        ok(rolePath, offset(0, 2), array(role))));
        Settings options = held(name, "organization_member_roles");
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(ONE, job, options.variables()));
        try {
            awaitRequests(name, 3);
            savepoint(job, running);
            engine.touch(name + ".release");
            success(engine.restoreJob(ONE, job, options.variables()));
            assertRows(
                    name,
                    Map.of(
                            key("organization_member_roles", "rol_1", "org_1", "auth0|user-1"),
                            expected("org_1", "auth0|user-1", role)));
            assertEquals(6, requests(name).size());
        } finally {
            engine.touch(name + ".release");
            stopIfRunning(job, running);
        }
    }

    @Test
    void restoresDuringAnIncompleteOauthResponseWithoutSavingItsCredentials() throws Exception {
        String name = "resume-grant";
        String row = client("client-1");
        String token = grant(Auth0Peer.TOKEN, 3600);
        program(
                name,
                List.of(
                        step("/oauth/token", "", 200, "partial", "", token),
                        ok("/oauth/token", "", token),
                        ok("/api/v2/clients", offset(0, 2), array(row))));
        Settings options = held(name, "clients");
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(OAUTH, job, options.variables()));
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> engine.exists(name + ".started"));
            savepoint(job, running);
            engine.touch(name + ".release");
            success(engine.restoreJob(OAUTH, job, options.variables()));
            assertRows(
                    name,
                    Map.of(key("clients", "client-1", null, null), expected(null, null, row)));
            assertEquals(
                    List.of(
                            "POST /oauth/token",
                            "POST /oauth/token",
                            "/api/v2/clients?" + offset(0, 2)),
                    requests(name));
        } finally {
            engine.touch(name + ".release");
            stopIfRunning(job, running);
        }
    }

    @Test
    void rejectsChangedTenantOrAudienceBeforeMakingRestoredOauthRequests() throws Exception {
        for (boolean audience : List.of(false, true)) {
            String name = "resume-scope-" + audience;
            program(
                    name,
                    List.of(
                            ok("/oauth/token", "", grant(Auth0Peer.TOKEN, 3600)),
                            step("/api/v2/clients", offset(0, 2), 200, "hold", "", "[]")));
            Settings options = held(name, "clients");
            String job = jobId();
            Future<Container.ExecResult> running =
                    tasks.submit(() -> engine.executeJob(OAUTH, job, options.variables()));
            try {
                awaitRequests(name, 2);
                savepoint(job, running);
                engine.touch(name + ".release");
                if (audience) {
                    options.audience = "https://other-tenant.auth0.com/api/v2/";
                } else {
                    options.endpoint = "https://localhost:" + Auth0Peer.PORT;
                }
                failure(engine.restoreJob(OAUTH, job, options.variables()), "checkpoint");
                assertRows(name, Map.of());
                assertEquals(2, requests(name).size());
            } finally {
                engine.touch(name + ".release");
                stopIfRunning(job, running);
            }
        }
    }

    @Test
    void restoresTerminalSourceWithoutRepeatingManagementRequests() throws Exception {
        String name = "resume-terminal", row = client("client-1");
        program(name, List.of(ok("/api/v2/clients", offset(0, 2), array(row))));
        Settings options = new Settings(name, "clients");
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
                assertRows(
                        name,
                        Map.of(key("clients", "client-1", null, null), expected(null, null, row)));
            } finally {
                stopIfRunning(job, restored);
            }
        } finally {
            stopIfRunning(job, running);
        }
    }

    @Test
    void rejectsMalformedPagesRepeatedCursorsAndExhaustedPageBudgets() throws Exception {
        for (String body :
                List.of(
                        "{}",
                        "[null]",
                        "[{}]",
                        "[] trailing",
                        "[{\"client_id\":\"one\",\"client_id\":\"two\"}]")) {
            String name = "invalid-page-" + jobId();
            program(name, List.of(ok("/api/v2/clients", offset(0, 2), body)));
            failure(run(ONE, new Settings(name, "clients")), "Auth0");
            assertRows(name, Map.of());
        }
        String name = "cursor-cycle";
        program(
                name,
                List.of(
                        ok(
                                "/api/v2/organizations",
                                "take=2",
                                envelope("organizations", "cursor-one")),
                        ok(
                                "/api/v2/organizations",
                                "take=2&from=cursor-one",
                                envelope("organizations", "cursor-one"))));
        failure(run(ONE, new Settings(name, "organizations")), "pagination repeated a cursor");
        assertEquals(2, requests(name).size());
        name = "page-budget";
        program(
                name,
                List.of(ok("/api/v2/clients", offset(0, 2), array(client("one"), client("two")))));
        Settings bounded = new Settings(name, "clients");
        bounded.maxPages = 1;
        failure(run(ONE, bounded), "exceeded max_pages_per_resource");
        assertEquals(1, requests(name).size());
    }

    @Test
    void restoresPendingBisectedUserWindowWithoutReplayingTheCompletedHalf() throws Exception {
        String name = "resume-bisected-users";
        List<String> script = new ArrayList<>();
        Map<String, List<String>> expected = new HashMap<>();
        script.add(ok("/api/v2/users", users(0, 3, 0, 0, true), count(1000)));
        script.add(ok("/api/v2/users", users(0, 1, 0, 0, true), count(500)));
        addUserWindowPages(script, expected, 0);
        script.add(step("/api/v2/users", users(2, 3, 0, 0, true), 200, "hold", "", count(500)));
        script.add(ok("/api/v2/users", users(2, 3, 0, 0, true), count(500)));
        addUserWindowPages(script, expected, 1);
        program(name, script);
        Settings options = held(name, "users");
        options.pageSize = 100;
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(ONE, job, options.variables()));
        try {
            awaitRequests(name, 8);
            savepoint(job, running);
            engine.touch(name + ".release");
            success(engine.restoreJob(ONE, job, options.variables()));
            assertRows(name, expected);
            assertEquals(14, requests(name).size());
        } finally {
            engine.touch(name + ".release");
            stopIfRunning(job, running);
        }
    }

    private static void addUserWindowPages(
            List<String> script, Map<String, List<String>> expected, int half) {
        int first = half * 2;
        for (int page = 0; page < 5; page++) {
            List<String> rows = new ArrayList<>();
            for (int index = page * 100; index < (page + 1) * 100; index++) {
                String id = "auth0|" + (half * 500 + index);
                String row = user(id, first + (index >= 250 ? 1 : 0));
                rows.add(row);
                expected.put(key("users", id, null, null), expected(null, null, row));
            }
            script.add(
                    ok(
                            "/api/v2/users",
                            users(first, first + 1, page, 100, false),
                            array(rows.toArray(new String[0]))));
        }
    }

    private void assertRows(String name, Map<String, List<String>> expected) throws Exception {
        Map<String, JsonNode> actual = new HashMap<>();
        for (String line : engine.rows(name)) {
            JsonNode row = JSON.readTree(line);
            assertEquals(
                    Set.of("resource", "id", "organization_id", "user_id", "record"),
                    fieldNames(row));
            String org =
                    row.path("organization_id").isNull()
                            ? null
                            : row.path("organization_id").asText();
            String user = row.path("user_id").isNull() ? null : row.path("user_id").asText();
            String id = key(row.path("resource").asText(), row.path("id").asText(), org, user);
            assertNull(actual.put(id, row), "duplicate committed context and identity");
        }
        assertEquals(expected.keySet(), actual.keySet());
        for (Map.Entry<String, List<String>> entry : expected.entrySet()) {
            JsonNode row = actual.get(entry.getKey());
            assertContext(entry.getValue().get(0), row.path("organization_id"));
            assertContext(entry.getValue().get(1), row.path("user_id"));
            String record = row.path("record").asText();
            assertEquals(JSON.readTree(entry.getValue().get(2)), JSON.readTree(record));
            if (entry.getValue().get(2).contains("9223372036854775808")) {
                assertTrue(record.contains("9223372036854775808"));
                assertTrue(record.contains("123456789012345678.123456789"));
            }
        }
    }

    private static void assertContext(String expected, JsonNode actual) {
        if ("~".equals(expected)) {
            assertTrue(actual.isNull());
        } else {
            assertEquals(expected, actual.asText());
        }
    }

    private static Set<String> fieldNames(JsonNode row) {
        Set<String> names = new HashSet<>();
        row.fieldNames().forEachRemaining(names::add);
        return names;
    }

    private static List<String> expected(String organization, String user, String record) {
        return List.of(
                organization == null ? "~" : organization, user == null ? "~" : user, record);
    }

    private static String key(String resource, String id, String organization, String user) {
        return resource
                + "/"
                + id
                + "/"
                + (organization == null ? "~" : organization)
                + "/"
                + (user == null ? "~" : user);
    }

    private static String array(String... rows) {
        return "[" + String.join(",", rows) + "]";
    }

    private static String envelope(String field, String next, String... rows) {
        return "{\""
                + field
                + "\":"
                + array(rows)
                + (next == null ? "" : ",\"next\":\"" + next + "\"")
                + "}";
    }

    private static String client(String id) {
        return "{\"client_id\":\"" + id + "\",\"name\":\"Client\"" + EXTRA + "}";
    }

    private static String organization(String id) {
        return "{\"id\":\"" + id + "\",\"name\":\"Organization\"" + EXTRA + "}";
    }

    private static String user(String id, int millisecond) {
        return "{\"user_id\":\""
                + id
                + "\",\"updated_at\":\""
                + date(millisecond)
                + "\",\"email\":\"user@example.invalid\""
                + EXTRA
                + "}";
    }

    private static String count(int total) {
        return "{\"start\":0,\"limit\":0,\"total\":" + total + ",\"users\":[]}";
    }

    private static String offset(int page, int size) {
        return "include_totals=false&page=" + page + "&per_page=" + size;
    }

    private static String users(int first, int last, int page, int size, boolean count) {
        return "include_totals="
                + count
                + "&page="
                + page
                + "&per_page="
                + size
                + "&q=updated_at:["
                + date(first)
                + " TO "
                + date(last)
                + "]&search_engine=v3&sort=updated_at:1";
    }

    private static String date(int millisecond) {
        return "2024-01-01T00:00:00." + String.format(Locale.ROOT, "%03d", millisecond) + "Z";
    }

    private static String grant(String token, int expires) {
        return "{\"access_token\":\""
                + token
                + "\",\"token_type\":\"Bearer\",\"expires_in\":"
                + expires
                + "}";
    }

    private static String renewed(String script) {
        return script.substring(0, script.lastIndexOf('\t') + 1) + "renewed";
    }

    private static String origin() {
        return "https://" + Auth0Peer.HOST + ":" + Auth0Peer.PORT;
    }

    private static Settings held(String name, String resource) {
        Settings settings = new Settings(name, resource);
        settings.timeout = 180000;
        settings.deadline = 240000;
        return settings;
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
        return "{\"error\":\"private-provider-message " + Auth0Peer.TOKEN + "\"}";
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
                Base64.getEncoder().encodeToString(body.getBytes(StandardCharsets.UTF_8)),
                "initial");
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
        assertFalse(text(result).contains("synthetic-auth0"));
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
                                "CN=" + Auth0Peer.HOST,
                                "-ext",
                                "SAN=dns:" + Auth0Peer.HOST,
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                file.toString(),
                                "-storepass",
                                Auth0Peer.PASSWORD,
                                "-keypass",
                                Auth0Peer.PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, process.waitFor(), output);
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(file)) {
            store.load(input, Auth0Peer.PASSWORD.toCharArray());
        }
        return store;
    }

    /**
     * Binds each job to one tenant, resource selection and a frozen millisecond search window.
     *
     * <p>Both credential modes use the same independently verified provider origin.
     */
    private static final class Settings {
        private final String name;
        private final String resource;
        private String endpoint = origin();
        private String audience = origin() + "/api/v2/";
        private String start = "2024-01-01T00:00:00.000Z";
        private String end = "2024-01-01T00:00:00.003Z";
        private int bytes = 4194304;
        private int maxPages = 10000;
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
                "api.token=" + Auth0Peer.TOKEN,
                "api.client_id=" + Auth0Peer.CLIENT,
                "api.client_secret=" + Auth0Peer.SECRET,
                "api.audience=" + audience,
                "api.tenant=" + endpoint,
                "api.trust=" + trust,
                "api.start=" + start,
                "api.end=" + end,
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
        private static final String ROOT = "/tmp/auth0-peer";
        private final Path fixtures;

        private Engine(Path fixtures) {
            this.fixtures = fixtures;
        }

        /**
         * Preserves reserved credential characters as argv without logging command values.
         *
         * <p>The inherited shell join also logs the joined command and its credential values.
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
            container.withExtraHost(Auth0Peer.HOST, "127.0.0.1");
            for (String name : List.of("trusted.p12", "untrusted.p12", "trusted.pem")) {
                container.withCopyFileToContainer(
                        MountableFile.forHostPath(fixtures.resolve(name)), ROOT + "/" + name);
            }
            try {
                String file = "Auth0Peer.class";
                Path source = Path.of(Auth0Peer.class.getResource(file).toURI());
                container.withCopyFileToContainer(
                        MountableFile.forHostPath(source),
                        ROOT
                                + "/classes/"
                                + Auth0Peer.class.getPackage().getName().replace('.', '/')
                                + "/"
                                + file);
            } catch (URISyntaxException e) {
                throw new IOException("Cannot stage the Java HTTPS fixture", e);
            }
        }

        private Container.ExecResult runPeer() throws Exception {
            return server.execInContainer(
                    "java", "-cp", ROOT + "/classes", Auth0Peer.class.getName(), ROOT);
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
            String path = "/tmp/seatunnel_mnt/auth0-" + name;
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
