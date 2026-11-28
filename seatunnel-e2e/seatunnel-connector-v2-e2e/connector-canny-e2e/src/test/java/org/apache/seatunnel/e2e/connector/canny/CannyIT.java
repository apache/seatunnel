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
package org.apache.seatunnel.e2e.connector.canny;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.canny.CannyRecoveryProbe;
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
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
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
 * Runs all ten Canny resources through Zeta and a transactional LocalFile sink.
 *
 * <p>An isolated HTTPS peer verifies exact wire requests and returns independently authored
 * records.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CannyIT extends TestSuiteBase {
    private static final ObjectMapper JSON =
            new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
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

    private static final String BOARDS = "/api/v1/boards/list";
    private static final String CATEGORIES = "/api/v1/categories/list";
    private static final String ENTRIES = "/api/v1/entries/list";
    private static final String COMMENTS = "/api/v2/comments/list";
    private static final String COMPANIES = "/api/v2/companies/list";
    private static final String POSTS = "/api/v1/posts/list";
    private static final String STATUS = "/api/v2/status_changes/list";
    private static final String TAGS = "/api/v1/tags/list";
    private static final String USERS = "/api/v2/users/list";
    private static final String VOTES = "/api/v2/votes/list";
    private static final String A = "board-A";
    private static final String B = "board-B";

    @Test
    void readsAllTenResourcesUsingTheirNativeContractsAndExactRows() throws Exception {
        Settings settings = new Settings("all");
        String board =
                "{\"id\":\"board-A\",\"name\":\"海豚 🐬\",\"token\":\"public-widget-id\",\"url\":\"https://credential-trap.invalid/board\"}";
        String category =
                "{\"id\":\"category-1\",\"board\":{\"id\":\"board-A\"},\"name\":\"News\",\"parentID\":null}";
        String entry =
                "{\"id\":\"entry-1\",\"title\":\"Release\",\"types\":[\"new\"],\"publishedAt\":null}";
        String comment =
                "{\"id\":\"comment-1\",\"board\":{\"id\":\"board-A\"},\"post\":{\"id\":\"post-1\"},\"value\":\"first\\nsecond\"}";
        String company =
                "{\"id\":\"company-1\",\"monthlySpend\":123456789.0000000123456789,\"customFields\":{\"count\":18446744073709551617,\"enabled\":true}}";
        String post =
                "{\"id\":\"post-1\",\"board\":{\"id\":\"board-A\"},\"score\":1,\"status\":\"planned\"}";
        String change =
                "{\"id\":\"change-1\",\"post\":{\"id\":\"post-1\"},\"status\":\"complete\"}";
        String tag = "{\"id\":\"tag-1\",\"board\":{\"id\":\"board-A\"},\"name\":\"QA\"}";
        String user = "{\"id\":\"user-1\",\"alias\":\"林\",\"customFields\":{\"nullable\":null}}";
        String vote =
                "{\"id\":\"vote-1\",\"board\":{\"id\":\"board-A\"},\"post\":{\"id\":\"post-1\"},\"by\":null}";
        program(
                settings.name,
                List.of(
                        boards(board),
                        categories(A, 100, 0, false, category),
                        offset(ENTRIES, "entries", 1000, 0, false, entry),
                        cursor(COMMENTS, "items", 100, null, false, null, comment),
                        cursor(COMPANIES, "companies", 100, null, false, null, company),
                        offset(POSTS, "posts", 100, 0, false, post),
                        cursor(STATUS, "items", 100, null, false, "terminal-cursor", change),
                        offset(TAGS, "tags", 100, 0, false, tag),
                        cursor(USERS, "users", 100, null, false, null, user),
                        cursor(VOTES, "items", 100, null, false, null, vote)));
        Map<String, JsonNode> expected = new HashMap<>();
        expect(expected, "boards", "board-A", A, null, board);
        expect(expected, "categories", "category-1", A, null, category);
        expect(expected, "changelog_entries", "entry-1", null, null, entry);
        expect(expected, "comments", "comment-1", A, "post-1", comment);
        expect(expected, "companies", "company-1", null, null, company);
        expect(expected, "posts", "post-1", A, "post-1", post);
        expect(expected, "status_changes", "change-1", null, "post-1", change);
        expect(expected, "tags", "tag-1", A, null, tag);
        expect(expected, "users", "user-1", null, null, user);
        expect(expected, "votes", "vote-1", A, "post-1", vote);
        success(run(settings));
        consumed(settings.name, 10);
        assertRows(settings.name, expected);
        assertFalse(engine.exists("trap"));
    }

    @Test
    void discoversBoardsForCategoriesOnlyAndKeepsEqualIdsIndependent() throws Exception {
        Settings settings = new Settings("board-scope", "categories");
        String first = record("duplicate", "A");
        String second = record("duplicate", "B");
        program(
                settings.name,
                List.of(
                        boards(record(B, "Second"), record(A, "First")),
                        categories(A, 100, 0, false, first),
                        categories(B, 100, 0, false, second)));
        Map<String, JsonNode> expected = new HashMap<>();
        expect(expected, "categories", "duplicate", A, null, first);
        expect(expected, "categories", "duplicate", B, null, second);
        success(run(settings));
        consumed(settings.name, 3);
        assertRows(settings.name, expected);
    }

    @Test
    void readsTheHundredItemCursorBoundaryWithoutTreatingTheCursorAsAUrl() throws Exception {
        Settings settings = new Settings("cursor-boundary", "users");
        String next = "https://credential-trap.invalid/a?x=+/=%";
        String[] first = new String[100];
        Map<String, JsonNode> expected = new HashMap<>();
        for (int i = 0; i < first.length; i++) {
            first[i] = record("u" + i, "用户 " + i);
            expect(expected, "users", "u" + i, null, null, first[i]);
        }
        String last = record("u100", "after");
        expect(expected, "users", "u100", null, null, last);
        program(
                settings.name,
                List.of(
                        cursor(USERS, "users", 100, null, true, next, first),
                        cursor(USERS, "users", 100, next, false, "ignored-terminal", last)));
        success(run(settings));
        consumed(settings.name, 2);
        assertRows(settings.name, expected);
        assertFalse(engine.exists("trap"));
    }

    @Test
    void advancesTheOffsetByTheRequestedLimitAfterShortAndEmptyPages() throws Exception {
        Settings settings = new Settings("offset-empty", "tags");
        settings.pageSize = 3;
        String first = record("first", "yes");
        String last = record("last", "end");
        program(
                settings.name,
                List.of(
                        offset(TAGS, "tags", 3, 0, true, first),
                        offset(TAGS, "tags", 3, 3, true),
                        offset(TAGS, "tags", 3, 6, false, last)));
        Map<String, JsonNode> expected = new HashMap<>();
        expect(expected, "tags", "first", null, null, first);
        expect(expected, "tags", "last", null, null, last);
        success(run(settings));
        consumed(settings.name, 3);
        assertRows(settings.name, expected);
    }

    @Test
    void followsAnEmptyCursorPageOnlyWhenTheServerSuppliesTheContinuation() throws Exception {
        Settings settings = new Settings("cursor-empty", "comments");
        String comment =
                "{\"id\":\"after-empty\",\"post\":{\"id\":\"p\",\"board\":{\"id\":\"b\"}}}";
        program(
                settings.name,
                List.of(
                        cursor(COMMENTS, "items", 100, null, true, "token-A"),
                        cursor(COMMENTS, "items", 100, "token-A", false, null, comment)));
        Map<String, JsonNode> expected = new HashMap<>();
        expect(expected, "comments", "after-empty", "b", "p", comment);
        success(run(settings));
        consumed(settings.name, 2);
        assertRows(settings.name, expected);
    }

    @Test
    void rejectsNonBooleanPaginationAndMissingOrMalformedRequiredCursors() throws Exception {
        List<String> responses =
                List.of(
                        "{\"users\":[],\"hasNextPage\":\"false\"}",
                        "{\"users\":[],\"hasNextPage\":true}",
                        "{\"users\":[],\"hasNextPage\":true,\"cursor\":9}",
                        "{\"users\":[],\"hasNextPage\":true,\"cursor\":\"line\\nbreak\"}",
                        "{\"items\":[],\"hasNextPage\":false}");
        for (int i = 0; i < responses.size(); i++) {
            Settings settings = new Settings("invalid-metadata-" + i, "users");
            program(settings.name, List.of(reply(USERS, limit(100), responses.get(i))));
            failure(run(settings), i == 4 ? "users array" : i == 0 ? "hasNextPage" : "cursor");
            consumed(settings.name, 1);
        }
    }

    @Test
    void stopsRepeatedCursorAndOffsetPagesWithoutIssuingAnotherRequest() throws Exception {
        Settings cursorSettings = new Settings("cursor-cycle", "users");
        program(
                cursorSettings.name,
                List.of(
                        cursor(USERS, "users", 100, null, true, "same"),
                        cursor(USERS, "users", 100, "same", true, "same")));
        failure(run(cursorSettings), "repeated cursor");
        consumed(cursorSettings.name, 2);
        Settings offsetSettings = new Settings("offset-cycle", "posts");
        String repeated = record("same", "unchanged");
        program(
                offsetSettings.name,
                List.of(
                        offset(POSTS, "posts", 100, 0, true, repeated),
                        offset(POSTS, "posts", 100, 100, false, repeated)));
        failure(run(offsetSettings), "repeated offset page");
        consumed(offsetSettings.name, 2);
    }

    @Test
    void failsExplicitlyAtTheNativeChangelogOffsetCeiling() throws Exception {
        Settings settings = new Settings("entry-ceiling", "changelog_entries");
        program(
                settings.name,
                List.of(
                        offset(ENTRIES, "entries", 1000, 0, true, record("entry-1", "first")),
                        offset(ENTRIES, "entries", 10000, 1000, true, record("entry-2", "next"))));
        failure(run(settings), "changelog offset exceeds the Canny limit of 1000");
        consumed(settings.name, 2);
    }

    @Test
    void readsPastTheChangelogOffsetCeilingUsingTheLastAllowedRequest() throws Exception {
        Settings settings = new Settings("entries-native-window", "changelog_entries");
        String[] first = new String[1000];
        Map<String, JsonNode> expected = new HashMap<>();
        for (int i = 0; i < first.length; i++) {
            first[i] = record("entry-" + i, "First page");
            expect(expected, "changelog_entries", "entry-" + i, null, null, first[i]);
        }
        String last = record("entry-1000", "Last page");
        expect(expected, "changelog_entries", "entry-1000", null, null, last);
        program(
                settings.name,
                List.of(
                        offset(ENTRIES, "entries", 1000, 0, true, first),
                        offset(ENTRIES, "entries", 10000, 1000, false, last)));
        success(run(settings));
        assertRows(settings.name, expected);
        consumed(settings.name, 2);
    }

    @Test
    void rejectsExcessRowsPagesAndScanRequestsIndependently() throws Exception {
        Settings count = new Settings("row-bound", "users");
        count.pageSize = 1;
        program(
                count.name,
                List.of(
                        cursor(
                                USERS,
                                "users",
                                1,
                                null,
                                false,
                                null,
                                record("1", "one"),
                                record("2", "two"))));
        failure(run(count), "configured record count");
        consumed(count.name, 1);
        Settings boards = new Settings("board-bound", "boards");
        boards.items = 1;
        program(boards.name, List.of(boards(record(A, "a"), record(B, "b"))));
        failure(run(boards), "configured record count");
        consumed(boards.name, 1);
        Settings pages = new Settings("pages-bound", "tags");
        pages.pages = 1;
        program(pages.name, List.of(offset(TAGS, "tags", 100, 0, true)));
        failure(run(pages), "max_pages_per_collection");
        consumed(pages.name, 1);
        Settings scan = new Settings("scan-bound", "categories");
        scan.scanPages = 1;
        program(scan.name, List.of(boards(record(A, "a"))));
        failure(run(scan), "max_pages_per_scan");
        consumed(scan.name, 1);
    }

    @Test
    void rejectsMismatchedCategoryBoardsAndDuplicateBoardIdentities() throws Exception {
        Settings mismatch = new Settings("wrong-board", "categories");
        program(
                mismatch.name,
                List.of(
                        boards(record(A, "one")),
                        categories(
                                A,
                                100,
                                0,
                                false,
                                "{\"id\":\"category\",\"board\":{\"id\":\"board-B\"}}")));
        failure(run(mismatch), "different board");
        consumed(mismatch.name, 2);
        Settings duplicate = new Settings("duplicate-board", "categories");
        program(duplicate.name, List.of(boards(record(A, "one"), record(A, "two"))));
        failure(run(duplicate), "duplicate board ID");
        consumed(duplicate.name, 1);
    }

    @Test
    void distinguishesEmptyPermissionAndErrorEnvelopesAndKeepsSecretsOutOfDiagnostics()
            throws Exception {
        Settings empty = new Settings("empty", "votes");
        program(empty.name, List.of(cursor(VOTES, "items", 100, null, false, null)));
        success(run(empty));
        consumed(empty.name, 1);
        assertRows(empty.name, Map.of());
        for (int status : List.of(200, 401, 403, 404)) {
            Settings settings = new Settings("status-" + status, "votes");
            settings.retries = 2;
            program(
                    settings.name,
                    List.of(
                            reply(
                                            VOTES,
                                            limit(100),
                                            "{\"error\":\"private-provider-message "
                                                    + CannyPeer.TOKEN
                                                    + "\"}")
                                    .put("status", status)));
            failure(run(settings), status == 200 ? "list response" : "HTTP " + status);
            consumed(settings.name, 1);
        }
    }

    @Test
    void honorsRelativeRetryAfterAndUnixResetHintsAndCapsRetryWaits() throws Exception {
        ObjectNode ok = cursor(USERS, "users", 100, null, false, null);
        for (String hint : List.of("retryAfter", "rateReset")) {
            Settings settings = new Settings(hint, "users");
            settings.retries = 1;
            program(
                    settings.name,
                    List.of(
                            reply(USERS, limit(100), "")
                                    .put("status", 429)
                                    .put(hint, hint.equals("rateReset") ? 2 : 1),
                            ok.deepCopy()));
            success(run(settings));
            consumed(settings.name, 2);
            List<String> times = engine.lines(settings.name + ".times");
            assertTrue(Long.parseLong(times.get(1)) - Long.parseLong(times.get(0)) >= 900);
        }
        Settings cap = new Settings("retry-cap", "users");
        cap.retries = 2;
        cap.retryDelay = 250;
        program(
                cap.name,
                List.of(reply(USERS, limit(100), "").put("status", 429).put("retryAfter", 3)));
        failure(run(cap), "max_retry_delay_ms");
        consumed(cap.name, 1);
        Settings retry = new Settings("retry-read", "users");
        retry.retries = 1;
        program(
                retry.name,
                List.of(reply(USERS, limit(100), "").put("status", 503), ok.deepCopy()));
        success(run(retry));
        consumed(retry.name, 2);
    }

    @Test
    void boundsWireAndDecodedBodiesAndRequiresValidJsonAndGzip() throws Exception {
        String item = record("u", "ok");
        Settings gzip = new Settings("gzip", "users");
        program(
                gzip.name,
                List.of(cursor(USERS, "users", 100, null, false, null, item).put("mode", "gzip")));
        success(run(gzip));
        consumed(gzip.name, 1);
        Map<String, JsonNode> expected = new HashMap<>();
        expect(expected, "users", "u", null, null, item);
        assertRows(gzip.name, expected);
        for (String mode : List.of("json", "gzip")) {
            Settings settings = new Settings("over-" + mode, "users");
            settings.bytes = 256;
            program(
                    settings.name,
                    List.of(
                            cursor(
                                            USERS,
                                            "users",
                                            100,
                                            null,
                                            false,
                                            null,
                                            record("large", "x".repeat(600)))
                                    .put("mode", mode)));
            failure(run(settings), "max_response_bytes");
            consumed(settings.name, 1);
        }
        Settings badGzip = new Settings("bad-gzip", "users");
        program(
                badGzip.name,
                List.of(cursor(USERS, "users", 100, null, false, null).put("mode", "bad-gzip")));
        failure(run(badGzip), "gzip");
        consumed(badGzip.name, 1);
        Settings malformed = new Settings("bad-json", "users");
        program(malformed.name, List.of(reply(USERS, limit(100), "{\"users\":[],\"users\":[]}")));
        failure(run(malformed), "invalid JSON");
        consumed(malformed.name, 1);
    }

    @Test
    void verifiesCertificateTrustAndNeverPostsTheKeyToARedirectTarget() throws Exception {
        Settings untrusted = new Settings("untrusted", "users");
        untrusted.trust = wrongTrust;
        program(untrusted.name, List.of());
        failure(run(untrusted), "certificate verification");
        consumed(untrusted.name, 0);
        Settings redirect = new Settings("redirect", "users");
        program(
                redirect.name,
                List.of(reply(USERS, limit(100), "").put("mode", "redirect").put("status", 307)));
        failure(run(redirect), "HTTP 307");
        consumed(redirect.name, 1);
        assertFalse(engine.exists("trap"));
    }

    @Test
    void enforcesAnAttemptDeadlineAndCancelsAStreamingBody() throws Exception {
        String slowBody = "{\"users\":[" + " ".repeat(300) + "],\"hasNextPage\":false}";
        Settings deadline = new Settings("deadline", "users");
        deadline.request = 1800;
        deadline.operation = 1800;
        program(deadline.name, List.of(reply(USERS, limit(100), slowBody).put("mode", "trickle")));
        failure(run(deadline), "deadline exceeded");
        await(deadline.name + ".disconnect");
        consumed(deadline.name, 1);

        Settings cancel = new Settings("cancel", "users");
        program(cancel.name, List.of(reply(USERS, limit(100), slowBody).put("mode", "partial")));
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob("/canny.conf", job, cancel.variables()));
        try {
            await(cancel.name + ".started");
            success(engine.cancelJob(job));
            await(cancel.name + ".disconnect");
            running.get(30, TimeUnit.SECONDS);
            assertRows(cancel.name, Map.of());
        } finally {
            if (!running.isDone()) {
                running.cancel(true);
            }
        }
        consumed(cancel.name, 1);
    }

    @Test
    void restoresTheSavedCategoryOffsetEvenIfTheBoardListReorders() throws Exception {
        Settings settings = new Settings("child-restore", "categories");
        settings.pageSize = 2;
        String boardA = record(A, "First");
        String boardB = record(B, "Second");
        String first = record("first", "first record");
        String middle = record("middle", "next board");
        String last = record("last", "final record");
        ObjectNode pending = categories(B, 2, 2, false, last);
        String job =
                savepointWhilePending(
                        settings,
                        List.of(
                                boards(boardA, boardB),
                                categories(A, 2, 0, false, first),
                                categories(B, 2, 0, true, middle),
                                pending.deepCopy().put("mode", "partial")));
        String after = "child-restored";
        program(after, List.of(boards(boardB, boardA), pending));
        success(engine.restoreJob("/canny.conf", job, settings.variables()));
        Map<String, JsonNode> expected = new HashMap<>();
        expect(expected, "categories", "first", A, null, first);
        expect(expected, "categories", "middle", B, null, middle);
        expect(expected, "categories", "last", B, null, last);
        assertRows(settings.name, expected);
        consumed(after, 2);
    }

    @Test
    void rejectsAChangedRestoredBoardBeforeRequestingItsCategories() throws Exception {
        Settings settings = new Settings("saved-board", "categories");
        String job =
                savepointWhilePending(
                        settings,
                        List.of(
                                boards(record(A, "First")),
                                categories(A, 100, 0, false).put("mode", "partial")));
        String after = "changed-board";
        program(after, List.of(boards(record(A, "Changed"))));
        failure(
                engine.restoreJob("/canny.conf", job, settings.variables()),
                "checkpoint page changed");
        consumed(after, 1);
        assertRows(settings.name, Map.of());
    }

    @Test
    void restoresACursorAfterAPartialResponseAndRetainsItsContinuationHistory() throws Exception {
        Settings settings = new Settings("saved-cursor", "users");
        String first = record("first", "before checkpoint");
        String later = record("later", "after checkpoint");
        ObjectNode pending = cursor(USERS, "users", 100, "cursor-0", true, "cursor-1", later);
        String job =
                savepointWhilePending(
                        settings,
                        List.of(
                                cursor(USERS, "users", 100, null, true, "cursor-0", first),
                                pending.deepCopy().put("mode", "partial")));
        String after = "cursor-restored";
        String last = record("last", "last");
        program(
                after,
                List.of(pending, cursor(USERS, "users", 100, "cursor-1", false, null, last)));
        success(engine.restoreJob("/canny.conf", job, settings.variables()));
        Map<String, JsonNode> expected = new HashMap<>();
        expect(expected, "users", "first", null, null, first);
        expect(expected, "users", "later", null, null, later);
        expect(expected, "users", "last", null, null, last);
        assertRows(settings.name, expected);
        consumed(after, 2);
    }

    @Test
    void rejectsAChangedCredentialBeforeResumingTheSavedCursor() throws Exception {
        Settings settings = new Settings("saved-token", "users");
        String job =
                savepointWhilePending(
                        settings,
                        List.of(
                                cursor(USERS, "users", 100, null, true, "next"),
                                cursor(USERS, "users", 100, "next", false, null)
                                        .put("mode", "partial")));
        String after = "changed-token";
        program(after, List.of());
        settings.token = CannyPeer.OTHER_TOKEN;
        failure(engine.restoreJob("/canny.conf", job, settings.variables()), "checkpoint");
        consumed(after, 0);
    }

    @Test
    void restoresTerminalPositionWhileAnotherPipelineKeepsTheBatchAlive() throws Exception {
        Settings settings = new Settings("terminal", "boards");
        String board = record(A, "Terminal");
        program(settings.name, List.of(boards(board)));
        Map<String, JsonNode> expected = new HashMap<>();
        expect(expected, "boards", A, A, null, board);
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(
                        () -> engine.executeJob("/canny-terminal.conf", job, settings.variables()));
        try {
            Awaitility.await()
                    .atMost(35, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertRows(settings.name, expected));
            success(engine.savepointJob(job));
            running.get(30, TimeUnit.SECONDS);
        } finally {
            if (!running.isDone()) {
                engine.cancelJob(job);
                running.cancel(true);
            }
        }
        consumed(settings.name, 1);
        String after = "terminal-restored";
        program(after, List.of());
        Future<Container.ExecResult> resumed =
                tasks.submit(
                        () -> engine.restoreJob("/canny-terminal.conf", job, settings.variables()));
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> "RUNNING".equals(engine.getJobStatus(job)));
            long baseline = engine.completedAcrossPipelines(job);
            Awaitility.await()
                    .atMost(35, TimeUnit.SECONDS)
                    .until(() -> engine.completedAcrossPipelines(job) > baseline);
            assertRows(settings.name, expected);
            consumed(after, 0);
        } finally {
            if (!resumed.isDone()) {
                engine.cancelJob(job);
            }
            resumed.get(30, TimeUnit.SECONDS);
        }
    }

    @Test
    void serializesAnExactReaderRowAndRejectsChangedPageAndInvalidState() throws Exception {
        String first = record("category-1", "initial");
        String second = record("category-2", "second");
        String changed = record("category-2", "mutated");
        ObjectNode root = boards(record(A, "First"));
        for (String mode : List.of("resume", "changed", "zero")) {
            String name = "packaged-" + mode;
            List<ObjectNode> steps =
                    mode.equals("zero")
                            ? List.of()
                            : List.of(
                                    root,
                                    categories(A, 100, 0, false, first, second),
                                    root.deepCopy(),
                                    categories(
                                            A,
                                            100,
                                            0,
                                            false,
                                            first,
                                            mode.equals("changed") ? changed : second));
            program(name, steps);
            success(engine.probe(mode));
            consumed(name, steps.size());
        }
    }

    private String savepointWhilePending(Settings settings, List<ObjectNode> scripted)
            throws Exception {
        settings.request = 120000;
        settings.operation = 180000;
        program(settings.name, scripted);
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob("/canny.conf", job, settings.variables()));
        try {
            await(settings.name + ".started");
            long initial = engine.getCompletedCheckpointCount(job);
            Awaitility.await()
                    .atMost(35, TimeUnit.SECONDS)
                    .until(() -> engine.getCompletedCheckpointCount(job) > initial);
            success(engine.savepointJob(job));
            running.get(30, TimeUnit.SECONDS);
        } finally {
            engine.touch(settings.name + ".release");
            if (!running.isDone()) {
                engine.cancelJob(job);
                running.cancel(true);
            }
        }
        consumed(settings.name, scripted.size());
        return job;
    }

    private static String record(String id, String name) {
        return JSON.createObjectNode().put("id", id).put("name", name).toString();
    }

    private static String array(String... items) {
        return "[" + String.join(",", items) + "]";
    }

    private static ObjectNode boards(String... values) {
        return reply(BOARDS, JSON.createObjectNode(), "{\"boards\":" + array(values) + "}");
    }

    private static ObjectNode categories(
            String board, int size, int skip, boolean next, String... values) {
        ObjectNode result = offset(CATEGORIES, "categories", size, skip, next, values);
        ((ObjectNode) result.path("request")).put("boardID", board);
        return result;
    }

    private static ObjectNode offset(
            String path, String root, int size, int skip, boolean next, String... rows) {
        String body = "{\"" + root + "\":" + array(rows) + ",\"hasMore\":" + next + "}";
        return reply(path, limit(size).put("skip", skip), body);
    }

    private static ObjectNode cursor(
            String path,
            String root,
            int size,
            String requestCursor,
            boolean next,
            String responseCursor,
            String... rows) {
        ObjectNode request = limit(size);
        if (requestCursor != null) {
            request.put("cursor", requestCursor);
        }
        ObjectNode metadata = JSON.createObjectNode().put("hasNextPage", next);
        if (responseCursor != null) {
            metadata.put("cursor", responseCursor);
        }
        String body = metadata.toString();
        return reply(
                path,
                request,
                body.substring(0, body.length() - 1) + ",\"" + root + "\":" + array(rows) + "}");
    }

    private static ObjectNode limit(int size) {
        return JSON.createObjectNode().put("limit", size);
    }

    private static ObjectNode reply(String target, ObjectNode request, String response) {
        ObjectNode step = JSON.createObjectNode().put("target", target).put("response", response);
        step.set("request", request);
        return step;
    }

    private static void expect(
            Map<String, JsonNode> expected,
            String resource,
            String id,
            String board,
            String post,
            String raw)
            throws Exception {
        ObjectNode row = JSON.createObjectNode().put("resource", resource).put("id", id);
        row.put("board_id", board);
        row.put("post_id", post);
        row.set("record", JSON.readTree(raw));
        assertNull(expected.put(rowKey(row), row));
    }

    private void assertRows(String name, Map<String, JsonNode> expected) throws Exception {
        Map<String, JsonNode> actual = new HashMap<>();
        for (String line : engine.rows(name)) {
            ObjectNode row = (ObjectNode) JSON.readTree(line);
            Set<String> fields = new HashSet<>();
            row.fieldNames().forEachRemaining(fields::add);
            assertEquals(Set.of("resource", "id", "board_id", "post_id", "record"), fields);
            assertTrue(
                    row.path("record").isTextual(),
                    "the typed record is encoded in one string column");
            row.set("record", JSON.readTree(row.path("record").asText()));

            assertNull(actual.put(rowKey(row), row));
        }
        assertEquals(expected, actual);
    }

    private static String rowKey(ObjectNode row) {
        return row.path("resource").asText()
                + "/"
                + row.path("id").asText()
                + "/"
                + row.path("board_id").toString();
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

    private Container.ExecResult run(Settings settings) throws Exception {
        return engine.executeJob("/canny.conf", jobId(), settings.variables());
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
                List.of(CannyPeer.TOKEN, CannyPeer.OTHER_TOKEN, "private-provider-message")) {
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
                                "CN=" + CannyPeer.HOST,
                                "-ext",
                                "SAN=dns:" + CannyPeer.HOST + ",dns:" + CannyPeer.TRAP,
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                store.toString(),
                                "-storepass",
                                CannyPeer.PASSWORD,
                                "-keypass",
                                CannyPeer.PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String output = new String(keytool.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, keytool.waitFor(), output);
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(store)) {
            keyStore.load(input, CannyPeer.PASSWORD.toCharArray());
        }
        return Base64.getEncoder().encodeToString(keyStore.getCertificate("gateway").getEncoded());
    }

    /**
     * Synthetic job settings with explicit paging and transport limits.
     *
     * <p>Each invocation passes a fresh argument array to the real engine container.
     */
    private static final class Settings {
        final String name;
        final String[] resources;
        String trust = CannyIT.trust;
        String token = CannyPeer.TOKEN;
        int request = 60000;
        int operation = 70000;
        int retries;
        int retryDelay = 60000;
        int bytes = 8388608;
        int pages = 10000;
        int scanPages = 100000;
        int items = 10000;
        int pageSize = 100;

        Settings(String name, String... resources) {
            this.name = name;
            this.resources = resources;
        }

        String[] variables() {
            ArrayNode selection = JSON.createArrayNode();
            for (String resource : resources) {
                selection.add(resource);
            }
            List<String> values =
                    new ArrayList<>(
                            List.of(
                                    "api.output=" + name,
                                    "api.resources=" + selection,
                                    "api.token=" + token,
                                    "api.certificate=" + trust,
                                    "api.request_timeout=" + request,
                                    "api.operation_timeout=" + operation,
                                    "api.retries=" + retries,
                                    "api.retry_delay=" + retryDelay,
                                    "api.bytes=" + bytes,
                                    "api.pages=" + pages,
                                    "api.scan_pages=" + scanPages,
                                    "api.items=" + items,
                                    "api.page_size=" + pageSize));
            return values.toArray(new String[0]);
        }
    }

    /**
     * Stages the independent peer and existing shaded JSON libraries in the test engine.
     *
     * <p>Host aliases and test certificates belong only to this owned container.
     */
    private static final class Engine extends SeaTunnelContainer {
        private static final String ROOT = "/tmp/canny-peer";
        private final Path files;
        private final HttpClient checkpoints =
                HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

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
            for (String host : List.of(CannyPeer.HOST, CannyPeer.TRAP)) {
                container.withExtraHost(host, "127.0.0.1");
            }
            container.withCopyFileToContainer(
                    MountableFile.forHostPath(files.resolve("trusted.p12")), ROOT + "/trusted.p12");
            container.withCopyFileToContainer(
                    MountableFile.forHostPath(files.resolve("trusted.pem")), ROOT + "/trusted.pem");
            try {
                stageClasses(container, CannyPeer.class);
                stageClasses(container, CannyRecoveryProbe.class);
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
                    CannyPeer.class.getName(),
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
                    CannyRecoveryProbe.class.getName(),
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
            String path = "/tmp/seatunnel_mnt/canny-" + name;
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
                            BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                return 0L;
            }
            long completed = 0L;
            for (JsonNode pipeline : JSON.readTree(response.body()).path("pipelines")) {
                completed += pipeline.path("counts").path("completed").asLong();
            }
            return completed;
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
