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

package org.apache.seatunnel.e2e.connector.avni;

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
class AvniIT extends TestSuiteBase {
    private static final String ONE = "/avni-one.conf";
    private static final String ALL = "/avni-all.conf";
    private static final String TERMINAL = "/avni-terminal.conf";
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
    void readsAllFourResourcesWithTheirOriginalIdsAuditAndVoidedRecords() throws Exception {
        String name = "four-resources";
        List<String> script = login();
        Map<String, String> expected = new HashMap<>();
        String[] resources = {"subjects", "program_enrolments", "program_encounters", "encounters"};
        String[] paths = {
            "/api/subjects", "/api/programEnrolments", "/api/programEncounters", "/api/encounters"
        };
        for (int index = 0; index < resources.length; index++) {
            String record = record("shared-id", 1);
            script.add(ok(paths[index], query(0, 2), page(1, 2, record)));
            expected.put(resources[index] + "/shared-id", record);
        }
        program(name, script);
        success(run(ALL, new Settings(name, "subjects")));
        assertRows(name, expected);
        assertEquals(6, requests(name).size());
    }

    @Test
    void readsTheEntireSameTimestampCohortAcrossOneHundredRowPages() throws Exception {
        String name = "same-timestamp-cohort";
        List<String> script = login();
        Map<String, String> expected = new HashMap<>();
        List<String> first = new ArrayList<>();
        for (int index = 0; index < 101; index++) {
            String row = record("subject-" + index, 1);
            expected.put("subjects/subject-" + index, row);
            if (index < 100) {
                first.add(row);
            }
        }
        script.add(ok("/api/subjects", query(0, 100), page(2, 100, first.toArray(new String[0]))));
        script.add(ok("/api/subjects", query(1, 100), page(2, 100, record("subject-100", 1))));
        program(name, script);
        Settings settings = new Settings(name, "subjects");
        settings.pageSize = 100;
        success(run(ONE, settings));
        assertRows(name, expected);
        assertEquals(4, requests(name).size());
    }

    @Test
    void rejectsRecordsAtEitherExclusiveTimestampBoundary() throws Exception {
        for (String timestamp : List.of("2024-01-01T00:00:00.000Z", "2024-01-01T00:00:01.000Z")) {
            String name = "exclusive-boundary-" + jobId();
            String row = record("outside", 1).replace(date(1), timestamp);
            List<String> script = login();
            script.add(ok("/api/subjects", query(0, 2), page(1, 2, row)));
            program(name, script);
            failure(run(ONE, new Settings(name, "subjects")), "Avni");
            assertRows(name, Map.of());
            assertEquals(3, requests(name).size());
        }
    }

    @Test
    void completesAnEmptyExclusiveWindowBeforeDiscoveringOrAuthenticating() throws Exception {
        String name = "empty-window";
        program(name, List.of());
        Settings settings = new Settings(name, "subjects");
        settings.end = settings.start;
        success(run(ALL, settings));
        assertRows(name, Map.of());
        assertEquals(0, requests(name).size());
    }

    @Test
    void normalizesProviderAuditOffsetsWhilePreservingTheOriginalRecord() throws Exception {
        for (String offset :
                List.of("2024-01-01T05:30:00.001+0530", "2023-12-31T19:00:00.001-05:00")) {
            String name = "audit-offset-" + jobId();
            String original = record("one", 1).replace(date(1), offset);
            List<String> script = login();
            script.add(ok("/api/subjects", query(0, 2), page(1, 2, original)));
            program(name, script);
            success(run(ONE, new Settings(name, "subjects")));
            List<String> rows = engine.rows(name);
            assertEquals(1, rows.size());
            JsonNode row = JSON.readTree(rows.get(0));
            assertEquals("subjects", row.path("resource").asText());
            assertEquals("one", row.path("id").asText());
            assertEquals(date(1), row.path("last_modified_at").asText());
            assertEquals(JSON.readTree(original), JSON.readTree(row.path("record").asText()));
            assertEquals(3, requests(name).size());
        }
    }

    @Test
    void refreshesTheCachedIdTokenAfterItsActualExpiryBetweenPages() throws Exception {
        String name = "token-expiry",
                one = record("one", 1),
                two = record("two", 2),
                three = record("three", 3);
        program(
                name,
                List.of(
                        discovery(),
                        auth(ok("/", "", grant(AvniPeer.TOKEN, 1)), "login"),
                        step("/api/subjects", query(0, 2), 200, "delay", "", page(2, 2, one, two)),
                        auth(ok("/", "", grant(AvniPeer.RENEWED, 3600)), "login"),
                        auth(ok("/api/subjects", query(1, 2), page(2, 2, three)), "renewed")));
        success(run(ONE, new Settings(name, "subjects")));
        assertRows(name, Map.of("subjects/one", one, "subjects/two", two, "subjects/three", three));
        assertEquals(5, requests(name).size());
    }

    @Test
    void treatsEmptyCustomPagesAsCompletedScansWithoutAnExtraRequest() throws Exception {
        String name = "empty-four";
        List<String> script = login();
        for (String path :
                List.of(
                        "/api/subjects",
                        "/api/programEnrolments",
                        "/api/programEncounters",
                        "/api/encounters")) {
            script.add(ok(path, query(0, 2), page(0, 2)));
        }
        program(name, script);
        success(run(ALL, new Settings(name, "subjects")));
        assertRows(name, Map.of());
        assertEquals(6, requests(name).size());
    }

    @Test
    void renewsAnExpiredIdentityTokenOnceWithoutLeakingCredentialsAcrossOrigins() throws Exception {
        String name = "renew-once", row = record("one", 1);
        List<String> script = login();
        script.add(step("/api/subjects", query(0, 2), 401, "fixed", "", privateError()));
        script.add(auth(ok("/", "", grant(AvniPeer.RENEWED, 3600)), "login"));
        script.add(auth(ok("/api/subjects", query(0, 2), page(1, 2, row)), "renewed"));
        program(name, script);
        success(run(ONE, new Settings(name, "subjects")));
        assertRows(name, Map.of("subjects/one", row));
        assertEquals(5, requests(name).size());
    }

    @Test
    void stopsAfterASecondUnauthorizedDataResponse() throws Exception {
        String name = "renew-rejected";
        List<String> script = login();
        script.add(step("/api/subjects", query(0, 2), 401, "fixed", "", privateError()));
        script.add(auth(ok("/", "", grant(AvniPeer.RENEWED, 3600)), "login"));
        script.add(
                auth(
                        step("/api/subjects", query(0, 2), 401, "fixed", "", privateError()),
                        "renewed"));
        program(name, script);
        failure(run(ONE, new Settings(name, "subjects")), "Avni");
        assertRows(name, Map.of());
        assertEquals(5, requests(name).size());
    }

    @Test
    void rejectsChallengesAndMalformedIdentityResultsWithoutSendingDataRequests() throws Exception {
        for (String result :
                List.of(
                        "{\"ChallengeName\":\"SMS_MFA\",\"Session\":\"private-provider-message\"}",
                        "{\"ChallengeName\":\"NEW_PASSWORD_REQUIRED\"}",
                        "{}",
                        "{\"AuthenticationResult\":{\"IdToken\":\"\"}}",
                        "{\"AuthenticationResult\":{\"IdToken\":123,\"ExpiresIn\":3600}}")) {
            String name = "invalid-identity-" + jobId();
            program(name, List.of(discovery(), auth(ok("/", "", result), "login")));
            failure(run(ONE, new Settings(name, "subjects")), "Avni");
            assertRows(name, Map.of());
            assertEquals(2, requests(name).size());
        }
    }

    @Test
    void rejectsMissingDiscoveryClientsBeforeTransmittingThePassword() throws Exception {
        for (String body :
                List.of(
                        "{}",
                        "{\"cognito\":{}}",
                        "{\"cognito\":{\"clientId\":\"\"}}",
                        "{\"cognito\":{\"clientId\":123}}")) {
            String name = "invalid-discovery-" + jobId();
            program(name, List.of(auth(ok("/idp-details", "", body), "none")));
            failure(run(ONE, new Settings(name, "subjects")), "Avni");
            assertRows(name, Map.of());
            assertEquals(List.of("/idp-details"), requests(name));
        }
    }

    @Test
    void doesNotReplayAnAmbiguousIdentityPostAfterAServerFailure() throws Exception {
        String name = "identity-no-replay";
        program(
                name,
                List.of(
                        discovery(),
                        auth(step("/", "", 503, "fixed", "0", privateError()), "login")));
        failure(run(ONE, new Settings(name, "subjects")), "Avni");
        assertRows(name, Map.of());
        assertEquals(List.of("/idp-details", "POST /"), requests(name));
    }

    @Test
    void distinguishesCognitoThrottlingFromPermanentAuthenticationFailures() throws Exception {
        String name = "cognito-throttle", row = record("one", 1);
        program(
                name,
                List.of(
                        discovery(),
                        auth(
                                step(
                                        "/",
                                        "",
                                        400,
                                        "fixed",
                                        "1",
                                        "{\"__type\":\"com.amazonaws.cognitoidentityprovider#TooManyRequestsException\"}"),
                                "login"),
                        auth(ok("/", "", grant(AvniPeer.TOKEN, 3600)), "login"),
                        ok("/api/subjects", query(0, 2), page(1, 2, row))));
        success(run(ONE, new Settings(name, "subjects")));
        assertRows(name, Map.of("subjects/one", row));
        List<String> times = engine.lines(name + ".times");
        assertTrue(Long.parseLong(times.get(2)) - Long.parseLong(times.get(1)) >= 900);
        for (String type :
                List.of(
                        "NotAuthorizedException",
                        "UserNotFoundException",
                        "PasswordResetRequiredException",
                        "UserNotConfirmedException")) {
            name = "cognito-permanent-" + type;
            program(
                    name,
                    List.of(
                            discovery(),
                            auth(
                                    step(
                                            "/",
                                            "",
                                            400,
                                            "fixed",
                                            "",
                                            "{\"__type\":\""
                                                    + type
                                                    + "\",\"message\":\"private-provider-message\"}"),
                                    "login")));
            failure(run(ONE, new Settings(name, "subjects")), "Avni");
            assertRows(name, Map.of());
            assertEquals(2, requests(name).size());
        }
    }

    @Test
    void neverTreatsDataPermissionsOrMissingResourcesAsEmptyCollections() throws Exception {
        for (int status : List.of(400, 403, 404)) {
            String name = "permanent-data-" + status;
            List<String> script = login();
            script.add(step("/api/subjects", query(0, 2), status, "fixed", "", privateError()));
            program(name, script);
            failure(run(ONE, new Settings(name, "subjects")), "Avni");
            assertRows(name, Map.of());
            assertEquals(3, requests(name).size());
        }
    }

    @Test
    void retriesReadThrottlingWithItsDelayAndStopsAtTheConfiguredBudget() throws Exception {
        String name = "data-throttle", row = record("one", 1);
        List<String> script = login();
        script.add(step("/api/subjects", query(0, 2), 429, "fixed", "1", privateError()));
        script.add(ok("/api/subjects", query(0, 2), page(1, 2, row)));
        program(name, script);
        success(run(ONE, new Settings(name, "subjects")));
        assertRows(name, Map.of("subjects/one", row));
        List<String> times = engine.lines(name + ".times");
        assertTrue(Long.parseLong(times.get(3)) - Long.parseLong(times.get(2)) >= 900);
        name = "retry-budget";
        script = login();
        script.add(step("/api/subjects", query(0, 2), 503, "fixed", "0", privateError()));
        script.add(step("/api/subjects", query(0, 2), 503, "fixed", "0", privateError()));
        program(name, script);
        Settings limited = new Settings(name, "subjects");
        limited.retries = 1;
        failure(run(ONE, limited), "Avni");
        assertRows(name, Map.of());
        assertEquals(4, requests(name).size());
    }

    @Test
    void validatesCustomPageMetadataAndRecordsBeforePublishingTheirRows() throws Exception {
        String row = record("one", 1);
        for (String body :
                List.of(
                        "{}",
                        "null",
                        "[]",
                        "{} trailing",
                        "{\"content\":[null],\"totalElements\":1,\"totalPages\":1,\"pageSize\":2}",
                        page(1, 2, "{\"ID\":\"missing-audit\"}"),
                        page(
                                1,
                                2,
                                "{\"ID\":null,\"audit\":{\"Last modified at\":\"2024-01-01T00:00:00.001Z\"}}"),
                        page(1, 2, row).replace("\"totalElements\":1", "\"totalElements\":9"),
                        page(2, 2, row),
                        page(1, 2, row).replace("\"totalPages\":1", "\"totalPages\":0"),
                        page(1, 2, row).replace("\"pageSize\":2", "\"pageSize\":0"))) {
            String name = "invalid-page-" + jobId();
            List<String> script = login();
            script.add(ok("/api/subjects", query(0, 2), body));
            program(name, script);
            failure(run(ONE, new Settings(name, "subjects")), "Avni");
            assertRows(name, Map.of());
            assertEquals(3, requests(name).size());
        }
    }

    @Test
    void failsRatherThanSilentlyTruncatingAtThePageBudget() throws Exception {
        String name = "page-budget";
        List<String> script = login();
        script.add(
                ok("/api/subjects", query(0, 2), page(2, 2, record("one", 1), record("two", 2))));
        program(name, script);
        Settings limited = new Settings(name, "subjects");
        limited.maxPages = 1;
        failure(run(ONE, limited), "Avni");
        assertEquals(3, requests(name).size());
    }

    @Test
    void boundsCompressedAndPlainResponsesAndRejectsRedirects() throws Exception {
        for (String mode : List.of("fixed", "gzip", "redirect")) {
            String name = "body-bound-" + mode;
            List<String> script = login();
            script.add(
                    step(
                            "/api/subjects",
                            query(0, 2),
                            "redirect".equals(mode) ? 302 : 200,
                            mode,
                            "",
                            page(1, 2, record("x".repeat(40000), 1))));
            program(name, script);
            Settings limited = new Settings(name, "subjects");
            limited.bytes = 8192;
            failure(run(ONE, limited), "Avni");
            assertRows(name, Map.of());
            assertEquals(3, requests(name).size());
        }
    }

    @Test
    void rejectsAnUntrustedApiCertificateBeforeAnyCredentialsAreTransmitted() throws Exception {
        for (boolean identity : List.of(false, true)) {
            String name = "untrusted-tls-" + identity;
            program(name, identity ? List.of(discovery()) : List.of());
            Settings settings = new Settings(name, "subjects");
            if (identity) {
                settings.identity =
                        "https://" + AvniPeer.IDENTITY_HOST + ":" + AvniPeer.UNTRUSTED_PORT;
            } else {
                settings.endpoint = "https://" + AvniPeer.HOST + ":" + AvniPeer.UNTRUSTED_PORT;
            }
            settings.retries = 0;
            failure(run(ONE, settings), "Avni");
            assertRows(name, Map.of());
            assertEquals(identity ? 1 : 0, requests(name).size());
        }
    }

    @Test
    void enforcesAnOperationDeadlineEvenWhileTheProviderKeepsSendingBytes() throws Exception {
        String name = "trickle-deadline";
        List<String> script = login();
        script.add(step("/api/subjects", query(0, 2), 200, "stream", "", "{}"));
        program(name, script);
        Settings limited = new Settings(name, "subjects");
        limited.timeout = 1200;
        limited.deadline = 1600;
        limited.retries = 0;
        failure(run(ONE, limited), "Avni");
        awaitDisconnect(name);
        assertRows(name, Map.of());
        assertEquals(3, requests(name).size());
    }

    @Test
    void cancelsAnActiveResponseAndClosesTheProviderConnection() throws Exception {
        String name = "cancel-stream";
        List<String> script = login();
        script.add(step("/api/subjects", query(0, 2), 200, "stream", "", "{}"));
        program(name, script);
        Settings settings = held(name, "subjects");
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(ONE, job, settings.variables()));
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> engine.exists(name + ".started"));
            success(engine.cancelJob(job));
            running.get(30, TimeUnit.SECONDS);
            awaitDisconnect(name);
            assertRows(name, Map.of());
            assertEquals(3, requests(name).size());
        } finally {
            stopIfRunning(job, running);
        }
    }

    @Test
    void restoresTheFrozenWindowAtTheNextPageWithoutReplayingCommittedRows() throws Exception {
        String name = "resume-next-page",
                one = record("one", 1),
                two = record("two", 1),
                three = record("three", 1);
        List<String> script = login();
        script.add(ok("/api/subjects", query(0, 2), page(2, 2, one, two)));
        script.add(step("/api/subjects", query(1, 2), 200, "hold", "", page(2, 2, three)));
        script.addAll(login());
        script.add(ok("/api/subjects", query(1, 2), page(2, 2, three)));
        program(name, script);
        Settings settings = held(name, "subjects");
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(ONE, job, settings.variables()));
        try {
            awaitRequests(name, 4);
            savepoint(job, running);
            engine.touch(name + ".release");
            success(engine.restoreJob(ONE, job, settings.variables()));
            assertRows(
                    name,
                    Map.of("subjects/one", one, "subjects/two", two, "subjects/three", three));
            assertEquals(7, requests(name).size());
        } finally {
            engine.touch(name + ".release");
            stopIfRunning(job, running);
        }
    }

    @Test
    void restoresAnIncompleteIdentityResponseWithoutPersistingTokens() throws Exception {
        String name = "resume-identity", row = record("one", 1);
        program(
                name,
                List.of(
                        discovery(),
                        auth(
                                step("/", "", 200, "partial", "", grant(AvniPeer.TOKEN, 3600)),
                                "login"),
                        discovery(),
                        auth(ok("/", "", grant(AvniPeer.TOKEN, 3600)), "login"),
                        ok("/api/subjects", query(0, 2), page(1, 2, row))));
        Settings settings = held(name, "subjects");
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(ONE, job, settings.variables()));
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> engine.exists(name + ".started"));
            savepoint(job, running);
            engine.touch(name + ".release");
            success(engine.restoreJob(ONE, job, settings.variables()));
            assertRows(name, Map.of("subjects/one", row));
            assertEquals(5, requests(name).size());
        } finally {
            engine.touch(name + ".release");
            stopIfRunning(job, running);
        }
    }

    @Test
    void restoresAtTheNextResourceWithoutRepeatingCompletedSubjectRows() throws Exception {
        String name = "resume-next-resource", row = record("shared", 1);
        List<String> script = login();
        script.add(ok("/api/subjects", query(0, 2), page(1, 2, row)));
        script.add(step("/api/programEnrolments", query(0, 2), 200, "hold", "", page(1, 2, row)));
        script.addAll(login());
        script.add(ok("/api/programEnrolments", query(0, 2), page(1, 2, row)));
        script.add(ok("/api/programEncounters", query(0, 2), page(1, 2, row)));
        script.add(ok("/api/encounters", query(0, 2), page(1, 2, row)));
        program(name, script);
        Settings settings = held(name, "subjects");
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(ALL, job, settings.variables()));
        try {
            awaitRequests(name, 4);
            savepoint(job, running);
            engine.touch(name + ".release");
            success(engine.restoreJob(ALL, job, settings.variables()));
            assertRows(
                    name,
                    Map.of(
                            "subjects/shared",
                            row,
                            "program_enrolments/shared",
                            row,
                            "program_encounters/shared",
                            row,
                            "encounters/shared",
                            row));
            assertEquals(9, requests(name).size());
        } finally {
            engine.touch(name + ".release");
            stopIfRunning(job, running);
        }
    }

    @Test
    void rejectsChangedUsernameOrWindowBeforeAnyRestoredAuthentication() throws Exception {
        for (boolean account : List.of(false, true)) {
            String name = "resume-scope-" + account;
            List<String> script = login();
            script.add(step("/api/subjects", query(0, 2), 200, "hold", "", page(0, 2)));
            program(name, script);
            Settings settings = held(name, "subjects");
            String job = jobId();
            Future<Container.ExecResult> running =
                    tasks.submit(() -> engine.executeJob(ONE, job, settings.variables()));
            try {
                awaitRequests(name, 3);
                savepoint(job, running);
                engine.touch(name + ".release");
                if (account) {
                    settings.username = "other-account@example.invalid";
                } else {
                    settings.end = "2024-01-01T00:00:02.000Z";
                }
                failure(engine.restoreJob(ONE, job, settings.variables()), "checkpoint");
                assertRows(name, Map.of());
                assertEquals(3, requests(name).size());
            } finally {
                engine.touch(name + ".release");
                stopIfRunning(job, running);
            }
        }
    }

    @Test
    void restoresTerminalSourcesWithoutRediscoveryOrAnotherLogin() throws Exception {
        String name = "resume-terminal", row = record("one", 1);
        List<String> script = login();
        script.add(ok("/api/subjects", query(0, 2), page(1, 2, row)));
        program(name, script);
        Settings settings = new Settings(name, "subjects");
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(TERMINAL, job, settings.variables()));
        try {
            awaitRequests(name, 3);
            savepoint(job, running);
            Future<Container.ExecResult> restored =
                    tasks.submit(() -> engine.restoreJob(TERMINAL, job, settings.variables()));
            try {
                long baseline = engine.getCompletedCheckpointCount(job);
                Awaitility.await()
                        .atMost(35, TimeUnit.SECONDS)
                        .until(() -> engine.getCompletedCheckpointCount(job) > baseline + 1);
                assertEquals(3, requests(name).size());
                assertRows(name, Map.of("subjects/one", row));
            } finally {
                stopIfRunning(job, restored);
            }
        } finally {
            stopIfRunning(job, running);
        }
    }

    private static String origin() {
        return "https://" + AvniPeer.HOST + ":" + AvniPeer.PORT;
    }

    private static String date(int millisecond) {
        return "2024-01-01T00:00:00." + String.format(Locale.ROOT, "%03d", millisecond) + "Z";
    }

    private static String query(int page, int size) {
        return "page="
                + page
                + "&size="
                + size
                + "&lastModifiedDateTime="
                + date(0)
                + "&now=2024-01-01T00:00:01.000Z";
    }

    private static String record(String id, int millisecond) {
        return "{\"ID\":\""
                + id
                + "\",\"voided\":true,\"audit\":{\"Last modified at\":\""
                + date(millisecond)
                + "\"}"
                + EXTRA
                + "}";
    }

    private static String page(int pages, int size, String... rows) {
        return "{\"content\":["
                + String.join(",", rows)
                + "],\"totalElements\":"
                + rows.length
                + ",\"totalPages\":"
                + pages
                + ",\"pageSize\":"
                + size
                + "}";
    }

    private static String grant(String token, int expires) {
        return "{\"AuthenticationResult\":{\"IdToken\":\""
                + token
                + "\",\"ExpiresIn\":"
                + expires
                + "}}";
    }

    private static String discovery() {
        return auth(
                ok("/idp-details", "", "{\"cognito\":{\"clientId\":\"" + AvniPeer.CLIENT + "\"}}"),
                "none");
    }

    private static List<String> login() {
        return new ArrayList<>(
                List.of(discovery(), auth(ok("/", "", grant(AvniPeer.TOKEN, 3600)), "login")));
    }

    private static String auth(String step, String mode) {
        return step.substring(0, step.lastIndexOf('\t') + 1) + mode;
    }

    private static String privateError() {
        return "{\"message\":\"private-provider-message " + AvniPeer.TOKEN + "\"}";
    }

    private static Settings held(String name, String resource) {
        Settings settings = new Settings(name, resource);
        settings.timeout = 180000;
        settings.deadline = 240000;
        return settings;
    }

    private void assertRows(String name, Map<String, String> expected) throws Exception {
        Map<String, JsonNode> actual = new HashMap<>();
        for (String line : engine.rows(name)) {
            JsonNode row = JSON.readTree(line);
            Set<String> names = new HashSet<>();
            row.fieldNames().forEachRemaining(names::add);
            assertEquals(Set.of("resource", "id", "last_modified_at", "record"), names);
            assertNull(
                    actual.put(row.path("resource").asText() + "/" + row.path("id").asText(), row),
                    "duplicate committed identity");
        }
        assertEquals(expected.keySet(), actual.keySet());
        for (Map.Entry<String, String> entry : expected.entrySet()) {
            JsonNode row = actual.get(entry.getKey());
            JsonNode original = JSON.readTree(entry.getValue());
            assertEquals(
                    original.path("audit").path("Last modified at").asText(),
                    row.path("last_modified_at").asText());
            String record = row.path("record").asText();
            assertEquals(original, JSON.readTree(record));
            if (entry.getValue().contains("9223372036854775808")) {
                assertTrue(record.contains("9223372036854775808"));
                assertTrue(record.contains("123456789012345678.123456789"));
            }
        }
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
        assertFalse(text(result).contains("synthetic-avni"));
        assertFalse(text(result).contains(AvniPeer.SECRET));
        assertFalse(text(result).contains(AvniPeer.USERNAME));
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
                                "CN=" + AvniPeer.HOST,
                                "-ext",
                                "SAN=dns:" + AvniPeer.HOST + ",dns:" + AvniPeer.IDENTITY_HOST,
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                file.toString(),
                                "-storepass",
                                AvniPeer.PASSWORD,
                                "-keypass",
                                AvniPeer.PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, process.waitFor(), output);
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(file)) {
            store.load(input, AvniPeer.PASSWORD.toCharArray());
        }
        return store;
    }

    /**
     * Binds each job to one account, resource selection and a frozen millisecond audit window.
     *
     * <p>Independent API and identity origins verify the two authentication stages.
     */
    private static final class Settings {
        private final String name;
        private final String resource;
        private String username = AvniPeer.USERNAME;
        private String endpoint = origin();
        private String identity =
                "https://" + AvniPeer.IDENTITY_HOST + ":" + AvniPeer.IDENTITY_PORT;
        private String start = "2024-01-01T00:00:00.000Z";
        private String end = "2024-01-01T00:00:01.000Z";
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
                "api.username=" + username,
                "api.password=" + AvniPeer.SECRET,
                "api.identity=" + identity,
                "api.endpoint=" + endpoint,
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
        private static final String ROOT = "/tmp/avni-peer";
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
            container.withExtraHost(AvniPeer.HOST, "127.0.0.1");
            container.withExtraHost(AvniPeer.IDENTITY_HOST, "127.0.0.1");
            for (String name : List.of("trusted.p12", "untrusted.p12", "trusted.pem")) {
                container.withCopyFileToContainer(
                        MountableFile.forHostPath(fixtures.resolve(name)), ROOT + "/" + name);
            }
            try {
                String file = "AvniPeer.class";
                Path source = Path.of(AvniPeer.class.getResource(file).toURI());
                container.withCopyFileToContainer(
                        MountableFile.forHostPath(source),
                        ROOT
                                + "/classes/"
                                + AvniPeer.class.getPackage().getName().replace('.', '/')
                                + "/"
                                + file);
            } catch (URISyntaxException e) {
                throw new IOException("Cannot stage the Java HTTPS fixture", e);
            }
        }

        private Container.ExecResult runPeer() throws Exception {
            return server.execInContainer(
                    "java", "-cp", ROOT + "/classes", AvniPeer.class.getName(), ROOT);
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
            String path = "/tmp/seatunnel_mnt/avni-" + name;
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
