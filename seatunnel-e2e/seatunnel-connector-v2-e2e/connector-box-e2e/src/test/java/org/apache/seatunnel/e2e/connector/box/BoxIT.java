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
package org.apache.seatunnel.e2e.connector.box;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.box.BoxRecoveryProbe;
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
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
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
import java.util.LinkedHashMap;
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
 * Runs Box catalog and extraction contracts through the real Zeta engine and transactional files.
 *
 * <p>The independently scripted TLS peer validates request order, pagination, scopes and bodies.
 * Only committed rows count as output; HTTP success and console text are not data assertions.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class BoxIT extends TestSuiteBase {
    private static final ObjectMapper JSON =
            new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    private static final String FILE = "9007199254740993";
    private static final String FOLDER = "9007199254740995";
    private static final String SECOND_FILE = "9007199254740997";
    private static final String COLLECTION = "501";
    private static final String ROOT = "/2.0/folders/0/items";
    private static final String EXTRA =
            ",\"extra\":{\"unicode\":\"数据🌏\",\"null\":null,\"items\":[false,null,[1,2]],\"integer\":9223372036854775808,\"decimal\":123456789012345678.123456789}";
    private static final String TEXT = "Exact extracted text\n数据🌏 & quoted \"value\"\n";
    private static final String ASK = "What is the total?";
    private static final String EXTRACT = "Extract fields as JSON.";
    private static final String STRUCTURED =
            "[{\"key\":\"risk\",\"description\":\"Risk rating\",\"displayName\":\"Risk\",\"type\":\"string\",\"prompt\":\"Return risk\",\"options\":[{\"key\":\"high\"},{\"key\":\"low\"}]},{\"key\":\"owner\",\"description\":\"Owner\",\"displayName\":\"Owner\",\"type\":\"string\",\"prompt\":\"Return owner\",\"options\":[{\"key\":\"none\"}]},{\"key\":\"amount\",\"description\":\"Amount\",\"displayName\":\"Amount\",\"type\":\"string\",\"prompt\":\"Return amount\"}]";
    @TempDir static Path directory;
    private static String trust;
    private static String wrongTrust;
    private final ExecutorService tasks = Executors.newCachedThreadPool();
    private Engine engine;
    private Future<Container.ExecResult> peer;
    private String active;

    @BeforeAll
    void startsTheOwnedEngineAndTwoTlsOrigins() throws Exception {
        trust = identity("trusted");
        wrongTrust = identity("untrusted");
        Files.writeString(directory.resolve("trusted.pem"), trust);
        engine = new Engine(directory);
        engine.startUp();
        peer = tasks.submit(engine::peer);
        Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> engine.exists("ready"));
    }

    @AfterEach
    void rejectsUnexpectedTraffic() throws Exception {
        if (active != null) {
            assertFalse(engine.exists(active + ".invalid"), active);
        }
    }

    @AfterAll
    void stopsOnlyOwnedServices() throws Exception {
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
    void readsAll19ResourcesWithFullPayloadsAndIndependentStructuredFieldOptions()
            throws Exception {
        String name = "all-19";
        List<ObjectNode> steps = new ArrayList<>();
        Map<String, JsonNode> expected = new LinkedHashMap<>();
        steps.add(grant("user", BoxPeer.TOKEN));
        String event = event("event-1"), admin = event("admin-event-1");
        steps.add(events("all", "0", "event opaque +/=", event));
        steps.add(events("all", "event opaque +/=", "event terminal", null));
        expect(expected, "events", "event-1", null, event, null);
        String template = item("sign-template-1", "sign_template");
        steps.add(get("/2.0/sign_templates", "limit=2", marker(null, template)));
        expect(expected, "sign_templates", "sign-template-1", null, template, null);
        String collection = item(COLLECTION, "collection"), collected = item(FILE, "file");
        steps.add(get("/2.0/collections", "offset=0&limit=2", offset(0, 2, 1, collection)));
        steps.add(
                get("/2.0/collections/501/items", "offset=0&limit=2", offset(0, 2, 1, collected)));
        expect(expected, "collections", COLLECTION, null, collection, null);
        expect(expected, "collection_items", FILE, COLLECTION, collected, null);
        String sign = item("sign-request-1", "sign_request");
        steps.add(get("/2.0/sign_requests", "limit=2", marker(null, sign)));
        expect(expected, "sign_request", "sign-request-1", null, sign, null);
        steps.add(events("admin_logs", "0", "admin opaque", admin));
        steps.add(events("admin_logs", "admin opaque", "admin terminal", null));
        expect(expected, "admin_logs", "admin-event-1", null, admin, null);
        String file = item(FILE, "file"), folder = item(FOLDER, "folder");
        steps.add(get(ROOT, "usemarker=true&limit=2", marker(null, file, folder)));
        expect(expected, "files", FILE, "0", file, null);
        expect(expected, "folders", FOLDER, "0", folder, null);
        String fileCollaboration = item("601", "collaboration"),
                folderCollaboration = item("602", "collaboration");
        String comment = item("701", "comment"), task = item("801", "task");
        steps.add(
                get(
                        "/2.0/files/" + FILE + "/collaborations",
                        "limit=2",
                        marker(null, fileCollaboration)));
        steps.add(
                get(
                        "/2.0/files/" + FILE + "/comments",
                        "offset=0&limit=2",
                        offset(0, 2, 1, comment)));
        steps.add(get("/2.0/files/" + FILE + "/tasks", "", entries(task)));
        steps.add(
                get(
                        "/2.0/folders/" + FOLDER + "/collaborations",
                        "limit=2",
                        marker(null, folderCollaboration)));
        expect(expected, "file_collaborations", "601", FILE, fileCollaboration, null);
        expect(expected, "file_comments", "701", FILE, comment, null);
        expect(expected, "file_tasks", "801", FILE, task, null);
        expect(expected, "folder_collaborations", "602", FOLDER, folderCollaboration, null);
        String recent =
                "{\"type\":\"recent_item\",\"interaction_type\":\"item_preview\",\"item\":"
                        + file
                        + EXTRA
                        + "}";
        steps.add(get("/2.0/recent_items", "limit=2", marker(null, recent)));
        expect(expected, "recent_items", "file:" + FILE, null, recent, null);
        String trashed = item("901", "file"), user = item("1001", "user");
        steps.add(get("/2.0/folders/trash/items", "usemarker=true&limit=2", marker(null, trashed)));
        steps.add(get("/2.0/users", "usemarker=true&limit=2", marker(null, user)));
        expect(expected, "trashed_items", "901", null, trashed, null);
        expect(expected, "users", "1001", null, user, null);
        steps.add(get(ROOT, "usemarker=true&limit=2", marker(null, file, folder)));
        String detail = detail(FILE);
        steps.add(get("/2.0/files/" + FILE, "", detail));
        steps.add(representation(FILE, "none", contentTemplate(FILE)));
        steps.add(get(info(FILE), "", "{\"status\":{\"state\":\"pending\"}}"));
        steps.add(get(info(FILE), "", "{\"status\":{\"state\":\"success\"}}"));
        steps.add(content(FILE, TEXT));
        expect(expected, "text_representation", FILE, "0", detail, TEXT);
        String answer = "Total: 123456789012345678.123456789\n数据🌏";
        steps.add(get("/2.0/files/" + FILE, "", detail));
        steps.add(ai("ask", FILE, JSON.createObjectNode().put("answer", answer).toString()));
        expect(expected, "ai_ask", FILE, "0", detail, answer);
        String extracted = "{\"amount\":\"123456789012345678.123456789\"}";
        steps.add(get("/2.0/files/" + FILE, "", detail));
        steps.add(ai("extract", FILE, JSON.createObjectNode().put("answer", extracted).toString()));
        expect(expected, "ai_extract", FILE, "0", detail, extracted);
        String structured =
                "{\"answer\":{\"risk\":\"high\",\"owner\":null,\"amount\":123456789012345678.123456789},\"citations\":[{\"type\":\"file\",\"id\":\""
                        + FILE
                        + "\",\"content\":\"原文\"}],\"confidence\":0.99}";
        steps.add(get("/2.0/files/" + FILE, "", detail));
        steps.add(ai("extract_structured", FILE, structured));
        expect(expected, "ai_extract_structured", FILE, "0", detail, structured);
        program(name, steps);
        success(run("all", new Settings(name, "events")));
        assertEquals(19, expected.size());
        assertRows(name, expected);
        consumed(name, steps.size());
    }

    @Test
    void keepsEmptyAndShortMarkerPagesUntilTheProviderEndsTheMarkerChain() throws Exception {
        String name = "marker-empty-and-full-terminal";
        String one = item("101", "sign_template"),
                two = item("102", "sign_template"),
                three = item("103", "sign_template");
        List<ObjectNode> steps =
                List.of(
                        get("/2.0/sign_templates", "limit=2", marker("m +/=&", one)),
                        get(
                                "/2.0/sign_templates",
                                "limit=2&marker=m%20%2B%2F%3D%26",
                                marker("next")),
                        get(
                                "/2.0/sign_templates",
                                "limit=2&marker=next",
                                marker(null, two, three)));
        program(name, steps);
        success(run("one", new Settings(name, "sign_templates")));
        Map<String, JsonNode> expected = new HashMap<>();
        expect(expected, "sign_templates", "101", null, one, null);
        expect(expected, "sign_templates", "102", null, two, null);
        expect(expected, "sign_templates", "103", null, three, null);
        assertRows(name, expected);
        consumed(name, steps.size());
    }

    @Test
    void advancesOffsetByReturnedLimitAcrossAnEmptyHoleAndFetchesHiddenParents() throws Exception {
        String name = "offset-empty-hole";
        String parent = item(COLLECTION, "collection"), child = item(FILE, "file");
        List<ObjectNode> steps =
                List.of(
                        get("/2.0/collections", "offset=0&limit=2", offset(0, 2, 1, parent)),
                        get("/2.0/collections/501/items", "offset=0&limit=2", offset(0, 2, 3)),
                        get(
                                "/2.0/collections/501/items",
                                "offset=2&limit=2",
                                offset(2, 2, 3, child)));
        program(name, steps);
        success(run("one", new Settings(name, "collection_items")));
        Map<String, JsonNode> expected = new HashMap<>();
        expect(expected, "collection_items", FILE, COLLECTION, child, null);
        assertRows(name, expected);
        consumed(name, steps.size());
    }

    @Test
    void continuesShortEventChunksAndStopsAtTheFirstZeroChunk() throws Exception {
        String name = "event-positions";
        String first = event("event-first"), second = event("event-second");
        List<ObjectNode> steps =
                List.of(
                        events("all", "0", "opaque +/=&", first),
                        events("all", "opaque +/=&", "9007199254740999", second),
                        events("all", "9007199254740999", "final", null));
        program(name, steps);
        success(run("one", new Settings(name, "events")));
        Map<String, JsonNode> expected = new HashMap<>();
        expect(expected, "events", "event-first", null, first, null);
        expect(expected, "events", "event-second", null, second, null);
        assertRows(name, expected);
        consumed(name, steps.size());
    }

    @Test
    void recursivelyExtractsOnlyFilesAndRetainsTheContainingFolder() throws Exception {
        String name = "recursive-text";
        List<ObjectNode> steps = new ArrayList<>();
        steps.add(
                get(
                        ROOT,
                        "usemarker=true&limit=2",
                        marker(null, item(FOLDER, "folder"), item("71", "web_link"))));
        steps.add(
                get(
                        "/2.0/folders/" + FOLDER + "/items",
                        "usemarker=true&limit=2",
                        marker(null, item(SECOND_FILE, "file"))));
        steps.add(get("/2.0/files/" + SECOND_FILE, "", detail(SECOND_FILE)));
        steps.add(representation(SECOND_FILE, "success", contentTemplate(SECOND_FILE)));
        steps.add(content(SECOND_FILE, TEXT));
        program(name, steps);
        Settings settings = new Settings(name, "text_representation");
        settings.recursive = true;
        success(run("one", settings));
        Map<String, JsonNode> expected = new HashMap<>();
        expect(expected, "text_representation", SECOND_FILE, FOLDER, detail(SECOND_FILE), TEXT);
        assertRows(name, expected);
        consumed(name, steps.size());
    }

    @Test
    void renewsCcgForAnEnterpriseSubjectOnceWhileStaticBearerDoesNotRenew() throws Exception {
        String name = "enterprise-ccg-renewal", row = item("101", "sign_template");
        List<ObjectNode> steps =
                List.of(
                        grant("enterprise", BoxPeer.TOKEN),
                        get(
                                        "/2.0/sign_templates",
                                        "limit=2",
                                        "{\"message\":\"private-provider-message\"}")
                                .put("status", 401),
                        grant("enterprise", BoxPeer.RENEWED),
                        get("/2.0/sign_templates", "limit=2", marker(null, row))
                                .put("token", BoxPeer.RENEWED));
        program(name, steps);
        Settings settings = new Settings(name, "sign_templates");
        settings.subjectType = "enterprise";
        success(run("ccg", settings));
        Map<String, JsonNode> expected = new HashMap<>();
        expect(expected, "sign_templates", "101", null, row, null);
        assertRows(name, expected);
        consumed(name, steps.size());
        name = "bearer-rejection";
        program(
                name,
                List.of(
                        get(
                                        "/2.0/sign_templates",
                                        "limit=2",
                                        "{\"message\":\"private-provider-message\"}")
                                .put("status", 401)));
        failure(run("one", new Settings(name, "sign_templates")), "401");
        consumed(name, 1);
        assertRows(name, Collections.emptyMap());
    }

    @Test
    void retriesAReadRateLimitButNeverReplaysAnUncertainAiRequest() throws Exception {
        String name = "catalog-rate-limit", row = item("101", "sign_template");
        program(
                name,
                List.of(
                        get(
                                        "/2.0/sign_templates",
                                        "limit=2",
                                        "{\"message\":\"private-provider-message\"}")
                                .put("status", 429)
                                .put("retry", "0"),
                        get("/2.0/sign_templates", "limit=2", marker(null, row))));
        success(run("one", new Settings(name, "sign_templates")));
        Map<String, JsonNode> expected = new HashMap<>();
        expect(expected, "sign_templates", "101", null, row, null);
        assertRows(name, expected);
        consumed(name, 2);
        name = "ai-uncertain-503";
        List<ObjectNode> steps = extractStart(FILE);
        steps.add(ai("ask", FILE, "{\"message\":\"private-provider-message\"}").put("status", 503));
        program(name, steps);
        failure(run("one", new Settings(name, "ai_ask")), "503");
        consumed(name, steps.size());
        assertRows(name, Collections.emptyMap());
    }

    @Test
    void emitsEmptyTextWhenThereIsNoExtractedRepresentation() throws Exception {
        String name = "no-text-representation";
        List<ObjectNode> steps = extractStart(FILE);
        steps.add(
                get(
                                "/2.0/files/" + FILE,
                                "fields=name%2Crepresentations",
                                "{\"id\":\""
                                        + FILE
                                        + "\",\"type\":\"file\",\"representations\":{\"entries\":[]}}")
                        .put("hint", "[extracted_text]"));
        program(name, steps);
        success(run("one", new Settings(name, "text_representation")));
        Map<String, JsonNode> expected = new HashMap<>();
        expect(expected, "text_representation", FILE, "0", detail(FILE), "");
        assertRows(name, expected);
        consumed(name, steps.size());
    }

    @Test
    void projectsFilePermissionsAndMetadataWithoutLosingRequiredIdentityFields() throws Exception {
        String name = "file-field-projection";
        String projected =
                "{\"id\":\""
                        + FILE
                        + "\",\"type\":\"file\",\"name\":\"合同.pdf\",\"description\":\"raw description\",\"permissions\":{\"can_download\":true}}";
        List<ObjectNode> steps = extractStart(FILE);
        steps.set(
                1,
                get(
                        "/2.0/files/" + FILE,
                        "fields=id%2Ctype%2Cname%2Cdescription%2Cpermissions",
                        projected));
        steps.add(representation(FILE, "success", contentTemplate(FILE)));
        steps.add(content(FILE, TEXT));
        program(name, steps);
        success(run("projection", new Settings(name, "text_representation")));
        Map<String, JsonNode> expected = new HashMap<>();
        expect(expected, "text_representation", FILE, "0", projected, TEXT);
        assertRows(name, expected);
        consumed(name, steps.size());
    }

    @Test
    void rejectsFailedUnboundedOrForeignRepresentationDestinationsBeforeCredentialEgress()
            throws Exception {
        for (String mode :
                List.of(
                        "provider-error",
                        "poll-budget",
                        "foreign-content",
                        "wrong-file-content",
                        "wrong-version-content",
                        "missing-placeholder",
                        "invalid-utf8")) {
            String name = "representation-" + mode;
            List<ObjectNode> steps = extractStart(FILE);
            Settings settings = new Settings(name, "text_representation");
            String template = contentTemplate(FILE);
            if (mode.equals("foreign-content")) {
                template = "https://box-trap.test/private/{+asset_path}";
            } else if (mode.equals("wrong-file-content")) {
                template = contentTemplate(SECOND_FILE);
            } else if (mode.equals("wrong-version-content")) {
                template = template.replace("/versions/99/", "/versions/100/");
            } else if (mode.equals("missing-placeholder")) {
                template = template.replace("{+asset_path}", "");
            }
            steps.add(
                    representation(
                            FILE,
                            mode.equals("provider-error")
                                    ? "error"
                                    : mode.equals("poll-budget") ? "pending" : "success",
                            template));
            if (mode.equals("poll-budget")) {
                settings.polls = 2;
                steps.add(get(info(FILE), "", "{\"status\":{\"state\":\"pending\"}}"));
                steps.add(get(info(FILE), "", "{\"status\":{\"state\":\"pending\"}}"));
            } else if (mode.equals("invalid-utf8")) {
                steps.add(content(FILE, "").put("bytes", "/w=="));
            }
            program(name, steps);
            failure(
                    run("one", settings),
                    mode.equals("poll-budget")
                            ? "max_representation_polls"
                            : mode.equals("invalid-utf8") ? "UTF-8" : "Box");
            consumed(name, steps.size());
            assertRows(name, Collections.emptyMap());
        }
    }

    @Test
    void detectsPagingCyclesAndTraversalDepthWithoutReturningTruncatedSuccess() throws Exception {
        String name = "marker-cycle";
        program(
                name,
                List.of(
                        get("/2.0/sign_templates", "limit=2", marker("repeat")),
                        get("/2.0/sign_templates", "limit=2&marker=repeat", marker("repeat"))));
        failure(run("one", new Settings(name, "sign_templates")), "Box");
        consumed(name, 2);
        assertRows(name, Collections.emptyMap());
        name = "extraction-depth";
        program(
                name,
                List.of(get(ROOT, "usemarker=true&limit=2", marker(null, item(FOLDER, "folder")))));
        Settings depth = new Settings(name, "text_representation");
        depth.recursive = true;
        depth.depth = 0;
        failure(run("one", depth), "max_depth");
        consumed(name, 1);
        assertRows(name, Collections.emptyMap());
        name = "extraction-cycle";
        program(
                name,
                List.of(
                        get(ROOT, "usemarker=true&limit=2", marker(null, item(FOLDER, "folder"))),
                        get(
                                "/2.0/folders/" + FOLDER + "/items",
                                "usemarker=true&limit=2",
                                marker(null, item("0", "folder")))));
        Settings cycle = new Settings(name, "text_representation");
        cycle.recursive = true;
        failure(run("one", cycle), "Box");
        consumed(name, 2);
        assertRows(name, Collections.emptyMap());
    }

    @Test
    void enforcesTheWholeTextReadDeadlineAndCancelsTheActiveNetworkRequest() throws Exception {
        String name = "text-deadline";
        List<ObjectNode> steps = extractStart(FILE);
        steps.add(representation(FILE, "success", contentTemplate(FILE)));
        steps.add(content(FILE, TEXT).put("mode", "trickle"));
        program(name, steps);
        Settings settings = new Settings(name, "text_representation");
        settings.timeout = 3000;
        settings.attempts = 1;
        failure(run("one", settings), "Box");
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> engine.exists(name + ".disconnect"));
        long start = Long.parseLong(engine.lines(name + ".started").get(0));
        long end = Long.parseLong(engine.lines(name + ".disconnect").get(0));
        assertTrue(
                end - start < 7000,
                "trickling bytes must not reset the complete response deadline");
        consumed(name, steps.size());
        assertRows(name, Collections.emptyMap());
    }

    @Test
    void rejectsWrongTrustAndRedirectsWithoutFollowingAnUnapprovedOrigin() throws Exception {
        String name = "wrong-trust";
        program(name, List.of());
        Settings settings = new Settings(name, "sign_templates");
        settings.trust = wrongTrust;
        settings.attempts = 1;
        failure(run("one", settings), "Box");
        consumed(name, 0);
        name = "api-redirect";
        program(
                name,
                List.of(
                        get("/2.0/sign_templates", "limit=2", "")
                                .put("status", 302)
                                .put("mode", "redirect")));
        failure(run("one", new Settings(name, "sign_templates")), "302");
        consumed(name, 1);
        assertRows(name, Collections.emptyMap());
    }

    @Test
    void savesTheAcceptedParentBeforeAChildRequestAndRestoresOnlyTheRemainingChild()
            throws Exception {
        String name = "parent-savepoint",
                parent = item(COLLECTION, "collection"),
                child = item(FILE, "file");
        List<ObjectNode> steps =
                List.of(
                        get("/2.0/collections", "offset=0&limit=2", offset(0, 2, 1, parent)),
                        get(
                                        "/2.0/collections/501/items",
                                        "offset=0&limit=2",
                                        offset(0, 2, 1, child))
                                .put("mode", "partial"));
        program(name, steps);
        Settings settings = new Settings(name, "collections");
        settings.timeout = 180000;
        settings.attempts = 1;
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(
                        () -> engine.executeJob("/box-parent.conf", job, settings.variables()));
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> engine.exists(name + ".started"));
            long baseline = engine.getCompletedCheckpointCount(job);
            Awaitility.await()
                    .atMost(35, TimeUnit.SECONDS)
                    .until(() -> engine.getCompletedCheckpointCount(job) > baseline);
            success(engine.savepointJob(job));
            running.get(30, TimeUnit.SECONDS);
        } finally {
            engine.touch(name + ".release");
            if (!running.isDone()) {
                engine.cancelJob(job);
                running.cancel(true);
            }
        }
        consumed(name, 2);
        String restored = "parent-restored";
        program(
                restored,
                List.of(
                        get(
                                "/2.0/collections/501/items",
                                "offset=0&limit=2",
                                offset(0, 2, 1, child))));
        success(engine.restoreJob("/box-parent.conf", job, settings.variables()));
        Map<String, JsonNode> expected = new HashMap<>();
        expect(expected, "collections", COLLECTION, null, parent, null);
        expect(expected, "collection_items", FILE, COLLECTION, child, null);
        assertRows(name, expected);
        consumed(restored, 1);
    }

    @Test
    void restoresACompletedBoxPipelineWithoutRepeatingItsRequestOrCommittedRow() throws Exception {
        String name = "terminal-original";
        String restored = "terminal-restored";
        String id = "701";
        String template = item(id, "sign_template");
        Map<String, JsonNode> expected = new HashMap<>();
        expect(expected, "sign_templates", id, null, template, null);
        program(name, List.of(get("/2.0/sign_templates", "limit=2", marker(null, template))));
        Settings settings = new Settings(name, "sign_templates");
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(
                        () -> engine.executeJob("/box-terminal.conf", job, settings.variables()));
        try {
            Awaitility.await()
                    .atMost(35, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertRows(name, expected));
            success(engine.savepointJob(job));
            running.get(30, TimeUnit.SECONDS);
        } finally {
            if (!running.isDone()) {
                engine.cancelJob(job);
                running.cancel(true);
            }
        }
        consumed(name, 1);

        program(restored, List.of());
        Future<Container.ExecResult> resumed =
                tasks.submit(
                        () -> engine.restoreJob("/box-terminal.conf", job, settings.variables()));
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> "RUNNING".equals(engine.getJobStatus(job)));
            long baseline = engine.completedAcrossPipelines(job);
            Awaitility.await()
                    .atMost(35, TimeUnit.SECONDS)
                    .until(() -> engine.completedAcrossPipelines(job) > baseline);
            assertRows(name, expected);
            consumed(restored, 0);
        } finally {
            if (!resumed.isDone()) {
                engine.cancelJob(job);
            }
            resumed.get(30, TimeUnit.SECONDS);
        }
    }

    @Test
    void cancelsAnActiveRepresentationDownloadWithoutCommittingContent() throws Exception {
        String name = "text-cancel";
        List<ObjectNode> steps = extractStart(FILE);
        steps.add(representation(FILE, "success", contentTemplate(FILE)));
        steps.add(content(FILE, TEXT).put("mode", "trickle"));
        program(name, steps);
        Settings settings = new Settings(name, "text_representation");
        settings.timeout = 180000;
        settings.attempts = 1;
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob("/box-one.conf", job, settings.variables()));
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> engine.exists(name + ".started"));
            success(engine.cancelJob(job));
            running.get(30, TimeUnit.SECONDS);
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .until(() -> engine.exists(name + ".disconnect"));
            consumed(name, steps.size());
            assertRows(name, Collections.emptyMap());
        } finally {
            if (!running.isDone()) {
                engine.cancelJob(job);
                running.cancel(true);
            }
        }
    }

    @Test
    void resumesTheExact32RowChunkRejectsChangedPagesAndRestoresTerminalReadersQuietly()
            throws Exception {
        List<String> records = new ArrayList<>();
        for (int index = 0; index < 33; index++) {
            records.add(item(Integer.toString(10000 + index), "sign_template"));
        }
        String page = marker(null, records.toArray(new String[0]));
        for (String mode : List.of("resume", "changed")) {
            String name = "reader-chunk-" + mode;
            program(
                    name,
                    List.of(
                            get("/2.0/sign_templates", "limit=33", page),
                            get(
                                    "/2.0/sign_templates",
                                    "limit=33",
                                    mode.equals("changed")
                                            ? page.replace("exact metadata", "changed metadata")
                                            : page)));
            success(engine.probe(mode));
            consumed(name, 2);
        }
        String zero = "reader-zero-network";
        program(zero, List.of());
        success(engine.probe("zero"));
        consumed(zero, 0);
    }

    @Test
    void readsEmptyCatalogsAndCompressedResponsesWithoutIssuingChildRequests() throws Exception {
        String name = "empty-all";
        List<ObjectNode> steps =
                List.of(
                        grant("user", BoxPeer.TOKEN),
                        events("all", "0", "terminal", null),
                        get("/2.0/sign_templates", "limit=2", marker(null)).put("mode", "gzip"),
                        get("/2.0/collections", "offset=0&limit=2", offset(0, 2, 0)),
                        get("/2.0/sign_requests", "limit=2", marker(null)),
                        events("admin_logs", "0", "terminal-admin", null),
                        get(ROOT, "usemarker=true&limit=2", marker(null)),
                        get("/2.0/recent_items", "limit=2", marker(null)),
                        get("/2.0/folders/trash/items", "usemarker=true&limit=2", marker(null)),
                        get("/2.0/users", "usemarker=true&limit=2", marker(null)),
                        get(ROOT, "usemarker=true&limit=2", marker(null)));
        program(name, steps);
        success(run("all", new Settings(name, "events")));
        assertRows(name, Collections.emptyMap());
        consumed(name, steps.size());
    }

    @Test
    void boundsDiscoveryAndResponseSizeAndRejectsMalformedAiAnswers() throws Exception {
        for (String mode : List.of("requests", "pages", "items", "bytes")) {
            String name = "budget-" + mode;
            Settings settings = new Settings(name, "sign_templates");
            List<ObjectNode> steps;
            if (mode.equals("requests")) {
                settings = new Settings(name, "collection_items");
                settings.requests = 1;
                steps =
                        List.of(
                                get(
                                        "/2.0/collections",
                                        "offset=0&limit=2",
                                        offset(0, 2, 1, item(COLLECTION, "collection"))));
            } else if (mode.equals("pages")) {
                settings.pages = 1;
                steps = List.of(get("/2.0/sign_templates", "limit=2", marker("again")));
            } else if (mode.equals("items")) {
                settings = new Settings(name, "text_representation");
                settings.items = 1;
                steps =
                        List.of(
                                get(
                                        ROOT,
                                        "usemarker=true&limit=2",
                                        marker(
                                                null,
                                                item(FILE, "file"),
                                                item(SECOND_FILE, "file"))));
            } else {
                settings.bytes = 1024;
                settings.attempts = 1;
                String oversized =
                        "{\"id\":\"101\",\"type\":\"sign_template\",\"description\":\""
                                + "x".repeat(2048)
                                + "\"}";
                steps = List.of(get("/2.0/sign_templates", "limit=2", marker(null, oversized)));
            }
            program(name, steps);
            failure(run("one", settings), "Box");
            assertRows(name, Collections.emptyMap());
            consumed(name, steps.size());
        }
        for (String method : List.of("ask", "extract", "extract_structured")) {
            String name = "invalid-ai-" + method;
            List<ObjectNode> steps = extractStart(FILE);
            steps.add(ai(method, FILE, "{\"answer\":null}"));
            program(name, steps);
            failure(run("one", new Settings(name, "ai_" + method)), "answer");
            assertRows(name, Collections.emptyMap());
            consumed(name, steps.size());
        }
    }

    private static List<ObjectNode> extractStart(String file) {
        return new ArrayList<>(
                List.of(
                        get(ROOT, "usemarker=true&limit=2", marker(null, item(file, "file"))),
                        get("/2.0/files/" + file, "", detail(file))));
    }

    private static ObjectNode grant(String subject, String token) {
        return get(
                        "/oauth2/token",
                        "",
                        "{\"access_token\":\""
                                + token
                                + "\",\"expires_in\":3600,\"token_type\":\"bearer\"}")
                .put("method", "POST")
                .put("subject_type", subject);
    }

    private static ObjectNode get(String path, String query, String response) {
        return JSON.createObjectNode()
                .put("host", BoxPeer.API)
                .put("path", path)
                .put("query", query)
                .put("method", "GET")
                .put("status", 200)
                .put("response", response);
    }

    private static ObjectNode events(String kind, String position, String next, String row) {
        ObjectNode response =
                JSON.createObjectNode()
                        .put("chunk_size", row == null ? 0 : 1)
                        .put("next_stream_position", next);
        ArrayNode entries = response.putArray("entries");
        if (row != null) {
            try {
                entries.add(JSON.readTree(row));
            } catch (IOException e) {
                throw new IllegalArgumentException("Invalid independent event fixture", e);
            }
        }
        return get(
                "/2.0/events",
                "stream_type=" + kind + "&stream_position=" + encode(position) + "&limit=2",
                response.toString());
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private static String marker(String next, String... rows) {
        ObjectNode response = envelope(rows);
        if (next == null) {
            response.putNull("next_marker");
        } else {
            response.put("next_marker", next);
        }
        return response.toString();
    }

    private static String offset(int offset, int limit, int total, String... rows) {
        return envelope(rows)
                .put("offset", offset)
                .put("limit", limit)
                .put("total_count", total)
                .toString();
    }

    private static String entries(String... rows) {
        return envelope(rows).toString();
    }

    private static ObjectNode envelope(String... rows) {
        ObjectNode response = JSON.createObjectNode();
        ArrayNode entries = response.putArray("entries");
        for (String row : rows) {
            try {
                entries.add(JSON.readTree(row));
            } catch (IOException e) {
                throw new IllegalArgumentException("Invalid independent Box fixture", e);
            }
        }
        return response;
    }

    private static String item(String id, String type) {
        return "{\"id\":\""
                + id
                + "\",\"type\":\""
                + type
                + "\",\"name\":\"exact metadata\""
                + EXTRA
                + "}";
    }

    private static String event(String id) {
        return "{\"event_id\":\""
                + id
                + "\",\"event_type\":\"ITEM_UPLOAD\",\"created_at\":\"2024-01-01T12:00:00-05:00\""
                + EXTRA
                + "}";
    }

    private static String detail(String id) {
        return "{\"id\":\""
                + id
                + "\",\"type\":\"file\",\"name\":\"合同.pdf\",\"file_version\":{\"id\":\"99\"},\"description\":\"raw description\",\"permissions\":{\"can_download\":true,\"can_annotate\":false},\"size\":9223372036854775808"
                + EXTRA
                + "}";
    }

    private static String info(String id) {
        return "/2.0/internal_files/" + id + "/versions/99/representations/extracted_text";
    }

    private static String contentTemplate(String id) {
        return "https://" + BoxPeer.CONTENT + "/api" + info(id) + "/content/{+asset_path}";
    }

    private static ObjectNode representation(String id, String state, String template) {
        ObjectNode body = JSON.createObjectNode().put("id", id).put("type", "file");
        ObjectNode representation =
                body.putObject("representations")
                        .putArray("entries")
                        .addObject()
                        .put("representation", "extracted_text");
        representation.putObject("status").put("state", state);
        representation.putObject("info").put("url", "https://" + BoxPeer.API + info(id));
        representation.putObject("content").put("url_template", template);
        return get("/2.0/files/" + id, "fields=name%2Crepresentations", body.toString())
                .put("hint", "[extracted_text]");
    }

    private static ObjectNode content(String id, String value) {
        return get("/api" + info(id) + "/content/", "", value)
                .put("host", BoxPeer.CONTENT)
                .put("content_type", "text/plain; charset=utf-8");
    }

    private static ObjectNode ai(String method, String id, String response) throws Exception {
        ObjectNode body = JSON.createObjectNode();
        body.putArray("items").addObject().put("type", "file").put("id", id);
        if (method.equals("ask")) {
            body.put("mode", "single_item_qa").put("prompt", ASK);
        } else if (method.equals("extract")) {
            body.put("prompt", EXTRACT);
        } else {
            body.set("fields", JSON.readTree(STRUCTURED));
        }
        ObjectNode step = get("/2.0/ai/" + method, "", response).put("method", "POST");
        step.set("body", body);
        return step;
    }

    private static void expect(
            Map<String, JsonNode> rows,
            String resource,
            String id,
            String parent,
            String payload,
            String content)
            throws Exception {
        ObjectNode row = JSON.createObjectNode().put("resource", resource).put("record_id", id);
        if (parent == null) {
            row.putNull("parent_id");
        } else {
            row.put("parent_id", parent);
        }
        row.set("payload", JSON.readTree(payload));
        if (content == null) {
            row.putNull("content");
        } else if (resource.equals("ai_extract_structured")) {
            row.set("content", JSON.readTree(content));
        } else {
            row.put("content", content);
        }
        assertNull(rows.put(key(resource, id, parent), row));
    }

    private static String key(String resource, String id, String parent) {
        return resource + "/" + id + "/" + (parent == null ? "~" : parent);
    }

    private void assertRows(String name, Map<String, JsonNode> expected) throws Exception {
        Map<String, JsonNode> actual = new HashMap<>();
        for (String line : engine.rows(name)) {
            ObjectNode row = (ObjectNode) JSON.readTree(line);
            Set<String> fields = new HashSet<>();
            row.fieldNames().forEachRemaining(fields::add);
            assertEquals(
                    Set.of("resource", "record_id", "parent_id", "payload", "content"), fields);
            assertTrue(row.path("payload").isTextual());
            row.set("payload", JSON.readTree(row.path("payload").asText()));
            if (row.path("resource").asText().equals("ai_extract_structured")) {
                assertTrue(row.path("content").isTextual());
                row.set("content", JSON.readTree(row.path("content").asText()));
            }
            String parent = row.path("parent_id").isNull() ? null : row.path("parent_id").asText();
            assertNull(
                    actual.put(
                            key(
                                    row.path("resource").asText(),
                                    row.path("record_id").asText(),
                                    parent),
                            row),
                    "duplicate committed identity within parent");
        }
        assertEquals(expected, actual);
    }

    private void program(String name, List<ObjectNode> steps) throws Exception {
        rejectsUnexpectedTraffic();
        ArrayNode script = JSON.createArrayNode();
        steps.forEach(script::add);
        Path file = Files.createTempFile(directory, "script-", ".json");
        Files.writeString(file, script.toString());
        engine.install(file, name + ".json");
        Path activeFile = Files.createTempFile(directory, "active-", ".txt");
        Files.writeString(activeFile, name);
        engine.install(activeFile, "active");
        active = name;
    }

    private void consumed(String name, int count) throws Exception {
        assertFalse(engine.exists(name + ".invalid"), name);
        assertEquals(count, engine.lines(name + ".requests").size(), name);
    }

    private Container.ExecResult run(String config, Settings settings) throws Exception {
        return engine.executeJob("/box-" + config + ".conf", jobId(), settings.variables());
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

    private static void failure(Container.ExecResult result, String reason) {
        assertNotEquals(0, result.getExitCode(), () -> output(result));
        assertTrue(output(result).contains(reason), () -> output(result));
        for (String value :
                List.of(
                        BoxPeer.TOKEN,
                        BoxPeer.RENEWED,
                        BoxPeer.SECRET,
                        "private-provider-message")) {
            assertFalse(
                    output(result).contains(value), "sensitive request or provider detail leaked");
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
                                "api",
                                "-keyalg",
                                "RSA",
                                "-keysize",
                                "2048",
                                "-validity",
                                "7",
                                "-dname",
                                "CN=" + BoxPeer.API,
                                "-ext",
                                "SAN=dns:" + BoxPeer.API + ",dns:" + BoxPeer.CONTENT,
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                store.toString(),
                                "-storepass",
                                BoxPeer.PASSWORD,
                                "-keypass",
                                BoxPeer.PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String result = new String(keytool.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, keytool.waitFor(), result);
        KeyStore keys = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(store)) {
            keys.load(input, BoxPeer.PASSWORD.toCharArray());
        }
        return Base64.getEncoder().encodeToString(keys.getCertificate("api").getEncoded());
    }

    /**
     * Defines one source scan's independent request and traversal limits.
     *
     * <p>Failure scenarios change only the budget or option whose boundary is under test.
     */
    private static final class Settings {
        private final String name;
        private final String resource;
        private String trust = BoxIT.trust;
        private String subjectType = "user";
        private boolean recursive;
        private int pages = 1000;
        private int requests = 50000;
        private int items = 10000;
        private int depth = 32;
        private int polls = 8;
        private int attempts = 2;
        private int timeout = 30000;
        private int bytes = 8388608;

        private Settings(String name, String resource) {
            this.name = name;
            this.resource = resource;
        }

        private String[] variables() {
            return new String[] {
                "api.output=" + name,
                "api.resource=" + resource,
                "api.endpoint=https://" + BoxPeer.API,
                "api.download=https://" + BoxPeer.CONTENT,
                "api.token=" + BoxPeer.TOKEN,
                "api.client=" + BoxPeer.CLIENT,
                "api.secret=" + BoxPeer.SECRET,
                "api.subject=" + BoxPeer.SUBJECT,
                "api.subject_type=" + subjectType,
                "api.trust=" + trust,
                "api.recursive=" + recursive,
                "api.pages=" + pages,
                "api.requests=" + requests,
                "api.items=" + items,
                "api.depth=" + depth,
                "api.polls=" + polls,
                "api.attempts=" + attempts,
                "api.timeout=" + timeout,
                "api.bytes=" + bytes
            };
        }
    }

    /**
     * Hosts the independent HTTPS fixture beside a normal Java 11 SeaTunnel worker.
     *
     * <p>Only this container gets DNS aliases and the explicit local source trust anchor.
     */
    private static final class Engine extends SeaTunnelContainer {
        private static final String PEER = "/tmp/box-peer";
        private final Path fixtures;
        private final HttpClient checkpoints =
                HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

        private Engine(Path fixtures) {
            this.fixtures = fixtures;
        }

        /**
         * Executes argv without shell interpolation or logging synthetic credential values.
         *
         * <p>The ordinary wrapper's joined command would expose reserved secret characters.
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
            for (String host : List.of(BoxPeer.API, BoxPeer.CONTENT, "box-trap.test")) {
                container.withExtraHost(host, "127.0.0.1");
            }
            for (String name : List.of("trusted.p12", "trusted.pem")) {
                container.withCopyFileToContainer(
                        MountableFile.forHostPath(fixtures.resolve(name)), PEER + "/" + name);
            }
            try {
                stage(container, BoxPeer.class);
                stage(container, BoxRecoveryProbe.class);
                Set<Path> jars = new HashSet<>();
                for (Class<?> type :
                        List.of(ObjectMapper.class, JsonFactory.class, JsonProperty.class)) {
                    Path jar =
                            Path.of(
                                    type.getProtectionDomain()
                                            .getCodeSource()
                                            .getLocation()
                                            .toURI());
                    if (jars.add(jar)) {
                        container.withCopyFileToContainer(
                                MountableFile.forHostPath(jar),
                                PEER + "/libs/" + jar.getFileName());
                    }
                }
            } catch (URISyntaxException e) {
                throw new IOException("Cannot stage the Java HTTPS fixture", e);
            }
        }

        private static void stage(GenericContainer<?> container, Class<?> type)
                throws IOException, URISyntaxException {
            Path folder = Path.of(type.getResource("").toURI());
            try (DirectoryStream<Path> files =
                    Files.newDirectoryStream(folder, type.getSimpleName() + "*.class")) {
                for (Path file : files) {
                    container.withCopyFileToContainer(
                            MountableFile.forHostPath(file),
                            PEER
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
                    PEER + "/classes:" + PEER + "/libs/*",
                    BoxPeer.class.getName(),
                    PEER);
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
                    PEER
                            + "/classes:"
                            + PEER
                            + "/libs/*:/tmp/seatunnel/lib/*:/tmp/seatunnel/connectors/*",
                    BoxRecoveryProbe.class.getName(),
                    PEER + "/trusted.pem",
                    mode);
        }

        private void install(Path file, String name) {
            server.copyFileToContainer(MountableFile.forHostPath(file), PEER + "/" + name);
        }

        private boolean exists(String name) throws Exception {
            return server.execInContainer("test", "-f", PEER + "/" + name).getExitCode() == 0;
        }

        private void touch(String name) throws Exception {
            success(server.execInContainer("touch", PEER + "/" + name));
        }

        private List<String> lines(String name) throws Exception {
            if (!exists(name)) {
                return Collections.emptyList();
            }
            Container.ExecResult result = server.execInContainer("cat", PEER + "/" + name);
            success(result);
            return linesOf(result.getStdout());
        }

        private List<String> rows(String name) throws Exception {
            String path = "/tmp/seatunnel_mnt/box-" + name;
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

        /**
         * Observes a new checkpoint even after the Box pipeline has already finished.
         *
         * <p>The first pipeline counter alone cannot identify progress of the live anchor.
         */
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
            HttpRequest get = HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(5)).build();
            HttpResponse<String> response =
                    checkpoints.send(get, HttpResponse.BodyHandlers.ofString());
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
