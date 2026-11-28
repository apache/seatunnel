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
package org.apache.seatunnel.e2e.connector.brex;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.brex.BrexRecoveryProbe;
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
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
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
 * Runs all six Brex read resources through Zeta and a transactional LocalFile sink.
 *
 * <p>An isolated HTTPS peer verifies exact wire requests and returns independently authored
 * records.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BrexIT extends TestSuiteBase {
    private static final ObjectMapper JSON =
            new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    private static final String OPEN = "2024-01-01T10:00:00Z";
    private static final String CLOSE = "2024-04-01T12:00:00Z";
    @TempDir static Path directory;
    private static String trust;
    private static String wrongTrust;
    private final ExecutorService tasks = Executors.newCachedThreadPool();
    private Engine engine;
    private Future<Container.ExecResult> peer;
    private String active;

    @BeforeAll
    void startsAnOwnedJava11EngineAndIndependentTlsPeer() throws Exception {
        trust = identity("trusted");
        wrongTrust = identity("untrusted");
        Files.writeString(directory.resolve("trusted.pem"), trust);
        engine = new Engine(directory);
        engine.startUp();
        peer = tasks.submit(engine::peer);
        Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> engine.exists("ready"));
    }

    @AfterEach
    void rejectsUnexpectedOrCredentialBearingRequests() throws Exception {
        if (active != null) {
            assertFalse(engine.exists(active + ".invalid"), active);
        }
    }

    @AfterAll
    void stopsOnlyOwnedResources() throws Exception {
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

    private static final String TRANSACTIONS = "/v2/transactions/card/primary?limit=100";
    private static final String USERS = "/v2/users?limit=100";
    private static final String DEPARTMENTS = "/v2/departments?limit=100";
    private static final String VENDORS = "/v1/vendors?limit=100";
    private static final String EXPENSES = "/v1/expenses?limit=100";
    private static final String BUDGETS = "/v2/budgets?limit=100";

    @Test
    void readsAllSixResourcesAndPreservesExactKeysAndRawMoney() throws Exception {
        String name = "all-six";
        String transaction =
                "{\"id\":\"tx-1\",\"type\":\"REFUND\",\"description\":\"退款 🌍\","
                        + "\"posted_at_date\":\"2024-01-01\",\"initiated_at_date\":\"2023-12-31\","
                        + "\"card_id\":\"card_01\",\"amount\":{\"amount\":-9007199254740993,"
                        + "\"currency\":\"JPY\"},\"merchant\":{\"country\":null,\"mcc\":\"0517\","
                        + "\"raw_descriptor\":\"Café/東京\"},\"future\":{\"numbers\":[0,1.234567890123456789]}}";
        String user = "{\"id\":\"usr-1\",\"first_name\":\"Zoë\",\"status\":\"ACTIVE\"}";
        String department = "{\"id\":\"dept-1\",\"name\":\"Research & Finance\"}";
        String vendor = "{\"id\":\"vendor-1\",\"company_name\":\"Øne\",\"payment_accounts\":[]}";
        String expense =
                "{\"id\":\"exp-1\",\"expense_type\":\"CARD\",\"purchased_at\":\""
                        + CLOSE
                        + "\",\"original_amount\":{\"amount\":9223372036854775807,"
                        + "\"currency\":\"USD\"},\"custom_fields\":[{\"nested\":null}]}";
        String budget =
                "{\"budget_id\":\"budget-1\",\"name\":\"January\",\"amount\":{\"amount\":123456,"
                        + "\"currency\":\"EUR\"},\"users\":[\"usr-1\"]}";
        program(
                name,
                List.of(
                        reply(transactionTarget(OPEN), page(null, transaction)).put("mode", "gzip"),
                        reply(USERS, page(null, user)),
                        reply(DEPARTMENTS, page(null, department)),
                        reply(VENDORS, page(null, vendor)),
                        reply(expenseTarget(EXPENSES, OPEN, CLOSE), page(null, expense)),
                        reply(BUDGETS, page(null, budget))));
        success(run("all", new Settings(name, "users")));
        Map<String, JsonNode> expected = new HashMap<>();
        expect(expected, "transactions", "tx-1", "2024-01-01", transaction);
        expect(expected, "users", "usr-1", null, user);
        expect(expected, "departments", "dept-1", null, department);
        expect(expected, "vendors", "vendor-1", null, vendor);
        expect(expected, "expenses", "exp-1", CLOSE, expense);
        expect(expected, "budgets", "budget-1", null, budget);
        assertRows(name, expected);
        consumed(name, 6);
    }

    @Test
    void continuesPastShortAndEmptyPagesAndEncodesAnOpaqueCursor() throws Exception {
        String name = "opaque-cursor";
        String cursor = "https://credential-trap.invalid/a?postal=+%&/é";
        String encoded = "https%3A%2F%2Fcredential-trap.invalid%2Fa%3Fpostal%3D%2B%25%26%2F%C3%A9";
        String one = "{\"id\":\"vendor-1\"}";
        String two = "{\"id\":\"vendor-2\"}";
        program(
                name,
                List.of(
                        reply(VENDORS, page(cursor, one)),
                        reply(VENDORS + "&cursor=" + encoded, page("p2")),
                        reply(VENDORS + "&cursor=p2", "{\"items\":[" + two + "]}")));
        success(run("one", new Settings(name, "vendors")));
        Map<String, JsonNode> expected = new HashMap<>();
        expect(expected, "vendors", "vendor-1", null, one);
        expect(expected, "vendors", "vendor-2", null, two);
        assertRows(name, expected);
        consumed(name, 3);
    }

    @Test
    void readsMoreThanOneThousandUsersWithoutAnExtraIdentityRequest() throws Exception {
        String name = "many-users";
        Settings settings = new Settings(name, "users");
        settings.pageSize = 1000;
        List<String> first = new ArrayList<>(1000);
        Map<String, JsonNode> expected = new HashMap<>();
        for (int i = 0; i < 1001; ++i) {
            String id = "u" + i;
            String row = "{\"id\":\"" + id + "\",\"status\":\"ACTIVE\"}";
            expect(expected, "users", id, null, row);
            if (i < 1000) {
                first.add(row);
            }
        }
        program(
                name,
                List.of(
                        reply("/v2/users?limit=1000", page("last", first.toArray(new String[0]))),
                        reply(
                                "/v2/users?limit=1000&cursor=last",
                                page(null, "{\"id\":\"u1000\",\"status\":\"ACTIVE\"}"))));
        success(run("one", settings));
        assertRows(name, expected);
        consumed(name, 2);
    }

    @Test
    void capsExpensePagesAtOneHundredAndTraversesAllExpenseRows() throws Exception {
        String name = "expense-native-cap";
        Settings settings = new Settings(name, "expenses");
        settings.pageSize = 1000;
        List<ObjectNode> script = new ArrayList<>();
        Map<String, JsonNode> expected = new HashMap<>();
        for (int i = 0; i < 11; ++i) {
            String target = EXPENSES + (i == 0 ? "" : "&cursor=p" + i);
            List<String> items = new ArrayList<>();
            int count = i == 10 ? 1 : 100;
            for (int row = 0; row < count; ++row) {
                String id = "e" + (100 * i + row);
                String raw = expense(id, OPEN);
                items.add(raw);
                expect(expected, "expenses", id, OPEN, raw);
            }
            script.add(
                    reply(
                            expenseTarget(target, OPEN, CLOSE),
                            page(i == 10 ? null : "p" + (i + 1), items.toArray(new String[0]))));
        }
        program(name, script);
        success(run("one", settings));
        assertRows(name, expected);
        consumed(name, 11);
    }

    @Test
    void sendsOnlyExplicitHistoricalUserStatuses() throws Exception {
        String name = "historical-users";
        String inactive = "{\"id\":\"archived\",\"status\":\"ARCHIVED\"}";
        String active = "{\"id\":\"active\",\"status\":\"ACTIVE\"}";
        program(
                name,
                List.of(
                        reply(
                                USERS + "&status%5B%5D=ACTIVE&status%5B%5D=ARCHIVED",
                                page(null, inactive, active))));
        success(run("users", new Settings(name, "users")));
        Map<String, JsonNode> expected = new HashMap<>();
        expect(expected, "users", "archived", null, inactive);
        expect(expected, "users", "active", null, active);
        assertRows(name, expected);
        consumed(name, 1);
    }

    @Test
    void replaysOneUtcDayAndOneCalendarMonthAndHandlesEqualTimestamps() throws Exception {
        String name = "calendar-lookbacks";
        Settings settings = new Settings(name, "transactions");
        String tx = "{\"id\":\"late-posting\",\"posted_at_date\":\"2024-03-30\"}";
        String sameDay = "{\"id\":\"end-day\",\"posted_at_date\":\"2024-04-01\"}";
        String exp = expense("local-offset", "2024-02-29T07:00:00+01:00");
        String equal = expense("same-time", "2024-02-29T06:00:00Z");
        String nanos = expense("nanoseconds", "2024-04-01T11:59:59.999999999Z");
        String finalExpense = expense("end-inclusive", CLOSE);
        String txRequest = transactionTarget("2024-03-30T00:00:00Z");
        program(
                name,
                List.of(
                        reply(
                                txRequest,
                                page(
                                        "past-future",
                                        "{\"id\":\"future\",\"posted_at_date\":\"2024-04-02\"}")),
                        reply(
                                TRANSACTIONS
                                        + "&cursor=past-future"
                                        + "&posted_at_start=2024-03-30T00%3A00%3A00Z",
                                page(
                                        null,
                                        tx,
                                        sameDay,
                                        "{\"id\":\"before-replay\",\"posted_at_date\":\"2024-03-29\"}")),
                        reply(
                                expenseTarget(EXPENSES, "2024-02-29T06:00:00Z", CLOSE),
                                page(
                                        null,
                                        exp,
                                        equal,
                                        nanos,
                                        finalExpense,
                                        expense("outside-before", "2024-02-29T05:59:59.999999999Z"),
                                        expense(
                                                "outside-after",
                                                "2024-04-01T12:00:00.000000001Z")))));
        success(run("windows", settings));
        Map<String, JsonNode> expected = new HashMap<>();
        expect(expected, "transactions", "late-posting", "2024-03-30", tx);
        expect(expected, "transactions", "end-day", "2024-04-01", sameDay);
        expect(expected, "expenses", "local-offset", "2024-02-29T06:00:00Z", exp);
        expect(expected, "expenses", "same-time", "2024-02-29T06:00:00Z", equal);
        expect(expected, "expenses", "nanoseconds", "2024-04-01T11:59:59.999999999Z", nanos);
        expect(expected, "expenses", "end-inclusive", CLOSE, finalExpense);
        assertRows(name, expected);
        consumed(name, 3);
    }

    @Test
    void clipsBothLookbacksToTheConfiguredStartingInstant() throws Exception {
        String name = "lookback-floor";
        Settings settings = new Settings(name, "transactions");
        settings.start = "2024-03-31T02:05:06+01:30";
        settings.transactionResume = "2024-04-01";
        settings.expenseResume = "2024-04-01T00:00:00Z";
        String start = "2024-03-31T00:35:06Z";
        program(
                name,
                List.of(
                        reply(transactionTarget(start), page(null)),
                        reply(expenseTarget(EXPENSES, start, CLOSE), page(null))));
        success(run("windows", settings));
        assertRows(name, Collections.emptyMap());
        consumed(name, 2);
    }

    @Test
    void failsCyclesAndFinitePageBudgetsBeforeAnotherRequest() throws Exception {
        String cycle = "cursor-cycle";
        program(
                cycle,
                List.of(
                        reply(USERS, page("a")),
                        reply(USERS + "&cursor=a", page("b")),
                        reply(USERS + "&cursor=b", page("a"))));
        failure(run("one", new Settings(cycle, "users")), "repeated next_cursor");
        assertRows(cycle, Collections.emptyMap());
        consumed(cycle, 3);

        String bounded = "page-budget";
        Settings settings = new Settings(bounded, "users");
        settings.pages = 1;
        program(bounded, List.of(reply(USERS, page("a"))));
        failure(run("one", settings), "max_pages_per_resource");
        consumed(bounded, 1);
    }

    @Test
    void rejectsAmbiguousPageShapesMissingIdentityAndInvalidCursorTime() throws Exception {
        List<String> bodies =
                List.of(
                        "{\"items\":{},\"next_cursor\":null}",
                        "{\"items\":[],\"next_cursor\":17}",
                        "{\"items\":[],\"next_cursor\":\"\"}",
                        "{\"items\":[],\"next_cursor\":\"private-provider-message\\r\\n\"}",
                        "{\"items\":[null]}",
                        "{\"items\":[{\"id\":\"wrong-key\"}]}");
        List<String> reasons =
                List.of(
                        "items array",
                        "invalid next_cursor",
                        "invalid next_cursor",
                        "invalid next_cursor",
                        "expected an object",
                        "budget_id");
        for (int i = 0; i < bodies.size(); ++i) {
            String name = "invalid-shape-" + i;
            program(name, List.of(reply(BUDGETS, bodies.get(i))));
            failure(run("one", new Settings(name, "budgets")), reasons.get(i));
            assertRows(name, Collections.emptyMap());
            consumed(name, 1);
        }

        String badDate = "bad-transaction-date";
        program(
                badDate,
                List.of(
                        reply(
                                transactionTarget(OPEN),
                                page(null, "{\"id\":\"bad\",\"posted_at_date\":\"2024-02-30\"}"))));
        failure(run("one", new Settings(badDate, "transactions")), "invalid posted_at_date");
        consumed(badDate, 1);

        String badExpense = "bad-expense-time";
        program(
                badExpense,
                List.of(
                        reply(
                                expenseTarget(EXPENSES, OPEN, CLOSE),
                                page(
                                        null,
                                        "{\"id\":\"bad\",\"purchased_at\":\"2024-03-31T23:00:00\"}"))));
        failure(run("one", new Settings(badExpense, "expenses")), "invalid purchased_at");
        consumed(badExpense, 1);
    }

    @Test
    void detectsOverfilledPagesAndNonCardExpenseResponses() throws Exception {
        String name = "overfilled";
        Settings settings = new Settings(name, "departments");
        settings.pageSize = 1;
        program(
                name,
                List.of(
                        reply(
                                "/v2/departments?limit=1",
                                page(null, "{\"id\":\"a\"}", "{\"id\":\"b\"}"))));
        failure(run("one", settings), "item count exceeds");
        consumed(name, 1);

        String wrong = "not-a-card";
        program(
                wrong,
                List.of(
                        reply(
                                expenseTarget(EXPENSES, OPEN, CLOSE),
                                page(
                                        null,
                                        "{\"id\":\"r\",\"expense_type\":\"REIMBURSEMENT\",\"purchased_at\":\""
                                                + OPEN
                                                + "\"}"))));
        failure(run("one", new Settings(wrong, "expenses")), "does not match the card filter");
        consumed(wrong, 1);
    }

    @Test
    void distinguishesAuthenticationAndPermissionErrorsFromEmptyResources() throws Exception {
        for (int status : List.of(401, 403, 404)) {
            String name = "http-auth-" + status;
            Settings settings = new Settings(name, "vendors");
            settings.retries = 3;
            program(
                    name,
                    List.of(
                            reply(
                                            VENDORS,
                                            "{\"message\":\"private-provider-message "
                                                    + BrexPeer.TOKEN
                                                    + "\"}")
                                    .put("status", status)));
            failure(run("one", settings), "HTTP " + status);
            assertRows(name, Collections.emptyMap());
            consumed(name, 1);
        }
    }

    @Test
    void honorsRetryAfterAndStopsWhenTheHintOrRetryBudgetWouldBeExceeded() throws Exception {
        String name = "bounded-rate-retry";
        Settings settings = new Settings(name, "users");
        settings.retries = 2;
        settings.interval = 50;
        String row = "{\"id\":\"resumed\"}";
        program(
                name,
                List.of(
                        reply(USERS, "").put("status", 429).put("retryAfter", "1"),
                        reply(USERS, "").put("status", 503),
                        reply(USERS, page(null, row))));
        success(run("one", settings));
        List<String> times = engine.lines(name + ".times");
        assertTrue(Long.parseLong(times.get(1)) - Long.parseLong(times.get(0)) >= 850);
        assertTrue(Long.parseLong(times.get(2)) - Long.parseLong(times.get(1)) >= 400);
        Map<String, JsonNode> expected = new HashMap<>();
        expect(expected, "users", "resumed", null, row);
        assertRows(name, expected);
        consumed(name, 3);

        String hint = "oversized-retry-hint";
        Settings limited = new Settings(hint, "users");
        limited.retries = 2;
        limited.retryDelay = 500;
        program(hint, List.of(reply(USERS, "").put("status", 429).put("retryAfter", "2")));
        failure(run("one", limited), "Retry-After exceeds max_retry_delay_ms");
        consumed(hint, 1);

        String exhausted = "retry-exhausted";
        Settings once = new Settings(exhausted, "users");
        once.retries = 1;
        program(
                exhausted,
                List.of(reply(USERS, "").put("status", 502), reply(USERS, "").put("status", 502)));
        failure(run("one", once), "HTTP 502 after 2 attempts");
        consumed(exhausted, 2);
    }

    @Test
    void boundsUncompressedAndCompressedResponsesAndRejectsInvalidJson() throws Exception {
        String payload = "{\"items\":[{\"id\":\"long\",\"memo\":\"" + "ø".repeat(2048) + "\"}]}";
        for (String mode :
                List.of("json", "gzip", "bad-gzip", "invalid-json", "duplicate-key", "deep")) {
            String name = "body-" + mode;
            Settings settings = new Settings(name, "users");
            if ("json".equals(mode) || "gzip".equals(mode)) {
                settings.bytes = 256;
            }
            String data = payload;
            if ("invalid-json".equals(mode)) {
                data = "{\"items\":[]} {\"unexpected\":true}";
            } else if ("duplicate-key".equals(mode)) {
                data = "{\"items\":[],\"items\":[{\"id\":\"misleading\"}]}";
            } else if ("deep".equals(mode)) {
                data =
                        "{\"items\":[],\"unknown\":"
                                + "[".repeat(100)
                                + "null"
                                + "]".repeat(100)
                                + "}";
            }
            program(name, List.of(reply(USERS, data).put("mode", mode)));
            String expectedReason =
                    mode.equals("json") || mode.equals("gzip")
                            ? "response exceeds max_response_bytes"
                            : mode.equals("bad-gzip")
                                    ? "invalid gzip response"
                                    : "invalid JSON response";
            failure(run("one", settings), expectedReason);
            assertRows(name, Collections.emptyMap());
            consumed(name, 1);
        }
    }

    @Test
    void verifiesServerCertificatesAndWillNotFollowARedirectToATrustedTrap() throws Exception {
        String untrusted = "untrusted-peer";
        Settings wrong = new Settings(untrusted, "users");
        wrong.trust = wrongTrust;
        program(untrusted, List.of());
        failure(run("one", wrong), "certificate verification");
        consumed(untrusted, 0);

        String redirected = "redirect-to-trap";
        program(redirected, List.of(reply(USERS, "").put("mode", "redirect").put("status", 302)));
        failure(run("one", new Settings(redirected, "users")), "HTTP 302");
        consumed(redirected, 1);
    }

    @Test
    void closesAPartialJsonBodyAtTheOperationDeadline() throws Exception {
        String name = "body-operation-deadline";
        Settings settings = new Settings(name, "users");
        settings.request = 1500;
        settings.operation = 1500;
        ObjectNode slow =
                reply(USERS, page(null, "{\"id\":\"never-delivered\"}")).put("mode", "partial");
        program(name, List.of(slow));
        failure(run("one", settings), "operation deadline");
        await(name + ".disconnect");
        assertRows(name, Collections.emptyMap());
        consumed(name, 1);
    }

    @Test
    void cancelsABlockedReadWithoutPublishingItsIncompletePage() throws Exception {
        String name = "cancel-active-json";
        Settings settings = new Settings(name, "users");
        settings.request = 60000;
        settings.operation = 70000;
        program(
                name,
                List.of(
                        reply(USERS, page(null, "{\"id\":\"not-committed\"}"))
                                .put("mode", "partial")));
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob("/brex-one.conf", job, settings.variables()));
        try {
            await(name + ".started");
            success(engine.cancelJob(job));
            running.get(30, TimeUnit.SECONDS);
            await(name + ".disconnect");
        } finally {
            engine.touch(name + ".release");
            if (!running.isDone()) {
                engine.cancelJob(job);
                running.cancel(true);
            }
        }
        assertRows(name, Collections.emptyMap());
        consumed(name, 1);
    }

    @Test
    void restoresAnOpaqueCursorAfterCompletedRowsAndADifferentFinishedResource() throws Exception {
        String name = "cursor-savepoint";
        Settings settings = new Settings(name, "users");
        settings.second = "vendors";
        settings.request = 120000;
        settings.operation = 180000;
        String user = "{\"id\":\"completed-user\",\"status\":\"ACTIVE\"}";
        String vendor1 = "{\"id\":\"vendor-first\"}";
        String vendor2 = "{\"id\":\"vendor-second\"}";
        ObjectNode pending = reply(VENDORS + "&cursor=resume", page(null, vendor2));
        List<ObjectNode> before =
                List.of(
                        reply(USERS, page(null, user)),
                        reply(VENDORS, page("resume", vendor1)),
                        pending.deepCopy().put("mode", "partial"));
        program(name, before);
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob("/brex-next.conf", job, settings.variables()));
        try {
            await(name + ".started");
            long initial = engine.getCompletedCheckpointCount(job);
            Awaitility.await()
                    .atMost(35, TimeUnit.SECONDS)
                    .until(() -> engine.getCompletedCheckpointCount(job) > initial);
            success(engine.savepointJob(job));
            running.get(30, TimeUnit.SECONDS);
        } finally {
            engine.touch(name + ".release");
            if (!running.isDone()) {
                engine.cancelJob(job);
                running.cancel(true);
            }
        }
        consumed(name, 3);
        String after = "cursor-restored";
        program(after, List.of(pending));
        success(engine.restoreJob("/brex-next.conf", job, settings.variables()));
        Map<String, JsonNode> expected = new HashMap<>();
        expect(expected, "users", "completed-user", null, user);
        expect(expected, "vendors", "vendor-first", null, vendor1);
        expect(expected, "vendors", "vendor-second", null, vendor2);
        assertRows(name, expected);
        consumed(after, 1);
    }

    @Test
    void restoresATerminalSourceWithoutReopeningTheApi() throws Exception {
        String name = "terminal-savepoint";
        Settings settings = new Settings(name, "departments");
        String row = "{\"id\":\"finished\",\"name\":\"Completed\"}";
        program(name, List.of(reply(DEPARTMENTS, page(null, row))));
        String job = jobId();
        Future<Container.ExecResult> original =
                tasks.submit(
                        () -> engine.executeJob("/brex-terminal.conf", job, settings.variables()));
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> engine.lines(name + ".requests").size() == 1);
            long first = engine.getCompletedCheckpointCount(job);
            Awaitility.await()
                    .atMost(35, TimeUnit.SECONDS)
                    .until(() -> engine.getCompletedCheckpointCount(job) > first + 1);
            success(engine.savepointJob(job));
            original.get(30, TimeUnit.SECONDS);
            Future<Container.ExecResult> resumed =
                    tasks.submit(
                            () ->
                                    engine.restoreJob(
                                            "/brex-terminal.conf", job, settings.variables()));
            try {
                long restored = engine.getCompletedCheckpointCount(job);
                Awaitility.await()
                        .atMost(35, TimeUnit.SECONDS)
                        .until(() -> engine.getCompletedCheckpointCount(job) > restored + 1);
                Map<String, JsonNode> expected = new HashMap<>();
                expect(expected, "departments", "finished", null, row);
                assertRows(name, expected);
                consumed(name, 1);
            } finally {
                if (!resumed.isDone()) {
                    engine.cancelJob(job);
                    resumed.get(30, TimeUnit.SECONDS);
                }
            }
        } finally {
            if (!original.isDone()) {
                engine.cancelJob(job);
                original.cancel(true);
            }
        }
    }

    @Test
    void restoresExactPackagedReaderRowAndRejectsChangedPageCredentialOrScope() throws Exception {
        for (String mode : List.of("resume", "changed", "zero")) {
            String name = "packaged-reader-" + mode;
            String first = "{\"id\":\"first\",\"first_name\":\"initial\"}";
            String second = "{\"id\":\"second\",\"first_name\":\"second\"}";
            String replay = "{\"id\":\"second\",\"first_name\":\"mutated\"}";
            List<ObjectNode> script =
                    "zero".equals(mode)
                            ? List.of()
                            : List.of(
                                    reply(USERS, page(null, first, second)),
                                    reply(
                                            USERS,
                                            page(
                                                    null,
                                                    first,
                                                    "changed".equals(mode) ? replay : second)));
            program(name, script);
            success(engine.probe(mode));
            consumed(name, script.size());
        }
    }

    private static String transactionTarget(String start) {
        return TRANSACTIONS + "&posted_at_start=" + start.replace(":", "%3A");
    }

    private static String expenseTarget(String base, String start, String end) {
        return base
                + "&expense_type%5B%5D=CARD"
                + "&purchased_at_start="
                + start.replace(":", "%3A")
                + "&purchased_at_end="
                + end.replace(":", "%3A");
    }

    private static String expense(String id, String date) {
        return "{\"id\":\""
                + id
                + "\",\"expense_type\":\"CARD\",\"purchased_at\":\""
                + date
                + "\"}";
    }

    private static String page(String cursor, String... rows) {
        String next = cursor == null ? "null" : JSON.getNodeFactory().textNode(cursor).toString();
        return "{\"items\":[" + String.join(",", rows) + "],\"next_cursor\":" + next + "}";
    }

    private static ObjectNode reply(String target, String body) {
        return JSON.createObjectNode().put("target", target).put("response", body);
    }

    private static void expect(
            Map<String, JsonNode> expected, String resource, String id, String cursor, String raw)
            throws Exception {
        ObjectNode row = JSON.createObjectNode().put("resource", resource).put("id", id);
        if (cursor == null) {
            row.putNull("cursor");
        } else {
            row.put("cursor", cursor);
        }
        row.set("record", JSON.readTree(raw));
        assertNull(expected.put(resource + "/" + id, row));
    }

    private void assertRows(String name, Map<String, JsonNode> expected) throws Exception {
        Map<String, JsonNode> actual = new HashMap<>();
        for (String line : engine.rows(name)) {
            ObjectNode row = (ObjectNode) JSON.readTree(line);
            Set<String> fields = new HashSet<>();
            row.fieldNames().forEachRemaining(fields::add);
            assertEquals(Set.of("resource", "id", "cursor", "record"), fields);
            assertTrue(
                    row.path("record").isTextual(),
                    "the typed record is encoded in one string column");
            row.set("record", JSON.readTree(row.path("record").asText()));
            assertNull(
                    actual.put(row.path("resource").asText() + "/" + row.path("id").asText(), row));
        }
        assertEquals(expected, actual);
    }

    private void program(String name, List<ObjectNode> steps) throws Exception {
        rejectsUnexpectedOrCredentialBearingRequests();
        ArrayNode script = JSON.createArrayNode();
        steps.forEach(script::add);
        Path file = Files.createTempFile(directory, "script-", ".json");
        Files.writeString(file, script.toString());
        engine.install(file, name + ".json");
        Path current = Files.createTempFile(directory, "active-", ".txt");
        Files.writeString(current, name);
        engine.install(current, "active");
        active = name;
    }

    private void consumed(String name, int count) throws Exception {
        assertFalse(engine.exists(name + ".invalid"), name);
        assertEquals(count, engine.lines(name + ".requests").size(), name);
    }

    private void await(String marker) {
        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> engine.exists(marker));
    }

    private Container.ExecResult run(String kind, Settings settings) throws Exception {
        return engine.executeJob("/brex-" + kind + ".conf", jobId(), settings.variables());
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

    private static void failure(Container.ExecResult result, String reason) {
        assertNotEquals(0, result.getExitCode(), () -> text(result));
        assertTrue(text(result).contains(reason), () -> text(result));
        for (String secret :
                List.of(BrexPeer.TOKEN, BrexPeer.OTHER_TOKEN, "private-provider-message")) {
            assertFalse(
                    text(result).contains(secret),
                    "gateway credential or server text reached a diagnostic");
        }
    }

    private static String identity(String name) throws Exception {
        Path store = directory.resolve(name + ".p12");
        Process keytool =
                new ProcessBuilder(
                                Path.of(System.getProperty("java.home"), "bin", "keytool")
                                        .toString(),
                                "-genkeypair",
                                "-alias",
                                "gateway",
                                "-keyalg",
                                "RSA",
                                "-keysize",
                                "2048",
                                "-validity",
                                "7",
                                "-dname",
                                "CN=" + BrexPeer.HOST,
                                "-ext",
                                "SAN=dns:" + BrexPeer.HOST + ",dns:" + BrexPeer.TRAP,
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                store.toString(),
                                "-storepass",
                                BrexPeer.PASSWORD,
                                "-keypass",
                                BrexPeer.PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String output = new String(keytool.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, keytool.waitFor(), output);
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(store)) {
            keyStore.load(input, BrexPeer.PASSWORD.toCharArray());
        }
        return Base64.getEncoder().encodeToString(keyStore.getCertificate("gateway").getEncoded());
    }

    /**
     * Synthetic job settings with deterministic time boundaries and explicit limits.
     *
     * <p>Each invocation passes a fresh argument array to the real engine container.
     */
    private static final class Settings {
        final String name;
        final String resource;
        String second = "departments";
        String token = BrexPeer.TOKEN;
        String trust = BrexIT.trust;
        String start = OPEN;
        String end = CLOSE;
        String transactionResume = "2024-03-31";
        String expenseResume = "2024-03-31T06:00:00Z";
        int request = 20000;
        int operation = 30000;
        int interval = 0;
        int retries;
        int retryDelay = 30000;
        int bytes = 8388608;
        int pages = 10000;
        int pageSize = 100;

        Settings(String name, String resource) {
            this.name = name;
            this.resource = resource;
        }

        String[] variables() {
            return new String[] {
                "scenario=" + name,
                "resource=" + resource,
                "second=" + second,
                "token=" + token,
                "trust=" + trust,
                "start=" + start,
                "end=" + end,
                "transactionResume=" + transactionResume,
                "expenseResume=" + expenseResume,
                "connect=1000",
                "request=" + request,
                "operation=" + operation,
                "interval=" + interval,
                "retries=" + retries,
                "retryDelay=" + retryDelay,
                "bytes=" + bytes,
                "pages=" + pages,
                "pageSize=" + pageSize
            };
        }
    }

    /**
     * Stages the independent peer and existing shaded JSON libraries in the test engine.
     *
     * <p>Host aliases and test certificates belong only to this owned container.
     */
    private static final class Engine extends SeaTunnelContainer {
        private static final String ROOT = "/tmp/brex-peer";
        private final Path files;

        private Engine(Path files) {
            this.files = files;
        }

        /**
         * Preserves individual argv values without logging the synthetic credential settings.
         *
         * <p>The engine receives test variables as individual arguments.
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
            for (String host : List.of(BrexPeer.HOST, BrexPeer.TRAP)) {
                container.withExtraHost(host, "127.0.0.1");
            }
            container.withCopyFileToContainer(
                    MountableFile.forHostPath(files.resolve("trusted.p12")), ROOT + "/trusted.p12");
            container.withCopyFileToContainer(
                    MountableFile.forHostPath(files.resolve("trusted.pem")), ROOT + "/trusted.pem");
            try {
                stageClasses(container, BrexPeer.class);
                stageClasses(container, BrexRecoveryProbe.class);
                Set<Path> libraries = new HashSet<>();
                for (Class<?> type :
                        List.of(ObjectMapper.class, JsonFactory.class, JsonProperty.class)) {
                    Path jar =
                            Path.of(
                                    type.getProtectionDomain()
                                            .getCodeSource()
                                            .getLocation()
                                            .toURI());
                    if (libraries.add(jar)) {
                        container.withCopyFileToContainer(
                                MountableFile.forHostPath(jar),
                                ROOT + "/libs/" + jar.getFileName());
                    }
                }
            } catch (URISyntaxException e) {
                throw new IOException("Cannot stage the independent Java HTTPS peer", e);
            }
        }

        private static void stageClasses(GenericContainer<?> container, Class<?> type)
                throws IOException, URISyntaxException {
            Path parent = Path.of(type.getResource("").toURI());
            try (DirectoryStream<Path> files =
                    Files.newDirectoryStream(parent, type.getSimpleName() + "*.class")) {
                for (Path file : files) {
                    container.withCopyFileToContainer(
                            MountableFile.forHostPath(file),
                            ROOT
                                    + "/classes/"
                                    + type.getPackage().getName().replace('.', '/')
                                    + "/"
                                    + file.getFileName());
                }
            }
        }

        private Container.ExecResult peer() throws Exception {
            return server.execInContainer(
                    "java",
                    "-cp",
                    ROOT + "/classes:" + ROOT + "/libs/*",
                    BrexPeer.class.getName(),
                    ROOT);
        }

        private Container.ExecResult probe(String mode) throws Exception {
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
                            + "/libs/*:/tmp/seatunnel/lib/*:/tmp/seatunnel/connectors/*",
                    BrexRecoveryProbe.class.getName(),
                    ROOT + "/trusted.pem",
                    mode);
        }

        private void install(Path file, String name) {
            server.copyFileToContainer(MountableFile.forHostPath(file), ROOT + "/" + name);
        }

        private void touch(String name) throws Exception {
            success(server.execInContainer("touch", ROOT + "/" + name));
        }

        private boolean exists(String name) throws Exception {
            return server.execInContainer("test", "-f", ROOT + "/" + name).getExitCode() == 0;
        }

        private List<String> lines(String name) throws Exception {
            if (!exists(name)) {
                return Collections.emptyList();
            }
            Container.ExecResult result = server.execInContainer("cat", ROOT + "/" + name);
            success(result);
            return linesOf(result.getStdout());
        }

        private List<String> rows(String name) throws Exception {
            String path = "/tmp/seatunnel_mnt/brex-" + name;
            Container.ExecResult result =
                    server.execInContainer(
                            "sh",
                            "-c",
                            "if [ -d "
                                    + path
                                    + " ]; then find "
                                    + path
                                    + " -maxdepth 1 -type f -name 'receipt_*' -exec sh -c 'for f do cat \"$f\"; printf \"\\n\"; done' sh {} +; fi");
            success(result);
            return linesOf(result.getStdout());
        }

        private static List<String> linesOf(String value) {
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
