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
package org.apache.seatunnel.e2e.connector.buildkite;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.buildkite.BuildkiteRecoveryProbe;
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
import java.net.http.HttpResponse.BodyHandlers;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies native Buildkite exports through the real Java 11 Zeta engine and HTTPS.
 *
 * <p>Independent literal provider fixtures validate every request and compare committed full rows,
 * including their parent context. No production catalog supplies expected paths or record fields.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class BuildkiteIT extends TestSuiteBase {
    private static final ObjectMapper JSON =
            new ObjectMapper()
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    private static final String START = "2024-01-01T00:00:00Z";
    private static final String END = "2024-01-02T00:00:00Z";
    private static final String EXTRA =
            ",\"extra\":{\"unicode\":\"数据🌏\",\"null\":null,\"array\":[false,null,[1,2]],\"integer\":9223372036854775808,\"decimal\":123456789012345678.12345678900}";
    private static String trust, wrongTrust, wrongNameTrust;
    @TempDir static Path directory;
    private final ExecutorService tasks = Executors.newCachedThreadPool();
    private Engine engine;
    private Future<Container.ExecResult> peer;
    private String active;

    @BeforeAll
    void startsOnlyOwnedTlsAndEngineServices() throws Exception {
        trust = identity("trusted", BuildkitePeer.HOST);
        wrongTrust = identity("untrusted", BuildkitePeer.HOST);
        wrongNameTrust = identity("wrong-name", "unrelated.invalid");
        engine = new Engine(directory);
        engine.startUp();
        peer = tasks.submit(engine::peer);
        Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> engine.exists("ready"));
    }

    @AfterEach
    void rejectsUnexpectedTrafficAndRestoresTheNormalCertificate() throws Exception {
        if (active != null) {
            assertFalse(engine.exists(active + ".invalid"), active);
        }
        selectIdentity("trusted");
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
    void exportsAll12ResourcesWithExactParentContextAndPrecisePayloads() throws Exception {
        String name = "all-12";
        String org = item("org-1", ",\"slug\":\"alpha\",\"created_at\":\"" + START + "\"");
        String suite =
                item("suite-1", ",\"api_token\":\"private-metadata-token\",\"name\":\"suite\"");
        String pipeline =
                item(
                        "pipeline-1",
                        ",\"slug\":\"shared\",\"created_at\":\"2024-01-01 00:00:00 UTC\"");
        String cluster = item("cluster-1", ",\"created_at\":\"" + START + "\"");
        String queue = item("queue-1", ",\"created_at\":\"" + START + "\"");
        String agent =
                item(
                        "agent-1",
                        ",\"created_at\":\""
                                + START
                                + "\",\"token\":\"private-metadata-token\",\"scopes\":[\"read_agents\"]");
        String pipelineBuild =
                item("pipeline-build", ",\"created_at\":\"2024-01-01T08:00:00+08:00\"");
        String orgBuild = item("org-build", ",\"created_at\":\"" + START + "\"");
        String build = item("global-build", ",\"created_at\":\"" + START + "\"");
        String emoji =
                "{\"name\":\"party\",\"aliases\":[\"celebration\"],\"url\":\"https://buildkite-trap.invalid/image\""
                        + EXTRA
                        + "}";
        String access =
                "{\"uuid\":\"access-1\",\"token_value\":\"private-metadata-token\",\"scopes\":[\"read_builds\"],\"description\":\"read only\""
                        + EXTRA
                        + "}";
        String user = item("user-1", ",\"created_at\":\"" + START + "\"");
        program(
                name,
                List.of(
                        get("/v2/organizations", page(1, 100), array(org)),
                        get("/v2/analytics/organizations/alpha/suites", "", array(suite)),
                        get("/v2/organizations/alpha/pipelines", page(1, 100), array(pipeline)),
                        get(
                                "/v2/organizations/alpha/pipelines/shared/builds",
                                builds(1),
                                array(pipelineBuild)),
                        get("/v2/organizations/alpha/clusters", page(1, 100), array(cluster)),
                        get(
                                "/v2/organizations/alpha/clusters/cluster-1/queues",
                                page(1, 100),
                                array(queue)),
                        get(
                                "/v2/organizations/alpha/clusters/cluster-1/tokens",
                                page(1, 100),
                                array(agent)),
                        get("/v2/organizations/alpha/builds", builds(1), array(orgBuild)),
                        get("/v2/organizations/alpha/emojis", "", array(emoji)),
                        get("/v2/access-token", "", access),
                        get("/v2/builds", builds(1), array(build)),
                        get("/v2/user", "", user)));
        success(run(new Settings(name)));
        List<JsonNode> expected =
                List.of(
                        expected("organizations", "org-1", "alpha", null, null, START, org),
                        expected(
                                "analytics_organizations_suites",
                                "suite-1",
                                "alpha",
                                null,
                                null,
                                null,
                                without(suite, "api_token")),
                        expected(
                                "organizations_pipelines",
                                "pipeline-1",
                                "alpha",
                                "shared",
                                null,
                                START,
                                pipeline),
                        expected(
                                "organizations_pipelines_builds",
                                "pipeline-build",
                                "alpha",
                                "shared",
                                null,
                                START,
                                pipelineBuild),
                        expected(
                                "organizations_clusters",
                                "cluster-1",
                                "alpha",
                                null,
                                "cluster-1",
                                START,
                                cluster),
                        expected(
                                "organizations_clusters_queues",
                                "queue-1",
                                "alpha",
                                null,
                                "cluster-1",
                                START,
                                queue),
                        expected(
                                "organizations_clusters_tokens",
                                "agent-1",
                                "alpha",
                                null,
                                "cluster-1",
                                START,
                                without(agent, "token")),
                        expected(
                                "organizations_builds",
                                "org-build",
                                "alpha",
                                null,
                                null,
                                START,
                                orgBuild),
                        expected("organizations_emojis", "party", "alpha", null, null, null, emoji),
                        expected(
                                "access-token",
                                "access-1",
                                null,
                                null,
                                null,
                                null,
                                without(access, "token_value")),
                        expected("builds", "global-build", null, null, null, START, build),
                        expected("user", "user-1", null, null, null, START, user));
        assertRows(name, expected);
        consumed(name, 12);
        for (String line : engine.rows(name)) {
            assertTrue(line.contains("123456789012345678.12345678900"));
            assertFalse(line.contains("private-metadata-token"));
        }
    }

    @Test
    void keepsSameNamedPipelinesAndClustersWithinTheirOwnOrganization() throws Exception {
        String name = "two-organizations";
        Settings settings =
                new Settings(
                        name, "organizations_pipelines_builds", "organizations_clusters_queues");
        List<ObjectNode> steps = new ArrayList<>();
        steps.add(
                get(
                        "/v2/organizations",
                        page(1, 100),
                        array(
                                "{\"id\":\"oa\",\"slug\":\"alpha\",\"created_at\":\"1999-01-01T00:00:00Z\"}",
                                "{\"id\":\"ob\",\"slug\":\"beta\"}")));
        List<JsonNode> expected = new ArrayList<>();
        for (String org : List.of("alpha", "beta")) {
            String build = item(org + "-build", ",\"created_at\":\"" + START + "\"");
            String queue = item(org + "-queue", ",\"created_at\":\"" + START + "\"");
            steps.add(
                    get(
                            "/v2/organizations/" + org + "/pipelines",
                            page(1, 100),
                            array(
                                    "{\"id\":\"pipeline\",\"slug\":\"same\",\"created_at\":\"1999-01-01T00:00:00Z\"}")));
            steps.add(
                    get(
                            "/v2/organizations/" + org + "/pipelines/same/builds",
                            builds(1),
                            array(build)));
            steps.add(
                    get(
                            "/v2/organizations/" + org + "/clusters",
                            page(1, 100),
                            array("{\"id\":\"same\"}")));
            steps.add(
                    get(
                            "/v2/organizations/" + org + "/clusters/same/queues",
                            page(1, 100),
                            array(queue)));
            expected.add(
                    expected(
                            "organizations_pipelines_builds",
                            org + "-build",
                            org,
                            "same",
                            null,
                            START,
                            build));
            expected.add(
                    expected(
                            "organizations_clusters_queues",
                            org + "-queue",
                            org,
                            null,
                            "same",
                            START,
                            queue));
        }
        program(name, steps);
        success(run(settings));
        assertRows(name, expected);
        consumed(name, 9);
    }

    @Test
    void readsMoreThan100SameInstantBuildsAndAppliesAnExclusiveUpperBoundary() throws Exception {
        String name = "201-builds";
        List<String> first = new ArrayList<>(), second = new ArrayList<>();
        List<JsonNode> expected = new ArrayList<>();
        for (int index = 0; index < 200; index++) {
            String record = item("build-" + index, ",\"created_at\":\"" + START + "\"");
            (index < 100 ? first : second).add(record);
            expected.add(expected("builds", "build-" + index, null, null, null, START, record));
        }
        String last = item("build-200", ",\"created_at\":\"2024-01-01T23:59:59.999999999Z\"");
        String excluded = item("at-end", ",\"created_at\":\"" + END + "\"");
        expected.add(
                expected(
                        "builds",
                        "build-200",
                        null,
                        null,
                        null,
                        "2024-01-01T23:59:59.999999999Z",
                        last));
        program(
                name,
                List.of(
                        get("/v2/builds", builds(1), array(first.toArray(new String[0])))
                                .put(
                                        "Link",
                                        next("/v2/builds?page=2&api_key=private-legacy-query")),
                        get("/v2/builds", builds(2), array(second.toArray(new String[0])))
                                .put("Link", next("?page=3")),
                        get("/v2/builds", builds(3), array(last, excluded))));
        success(run(new Settings(name, "builds")));
        assertRows(name, expected);
        consumed(name, 3);
    }

    @Test
    void followsShortAndEmptyPagesWithNextAndStopsFullPagesWithoutNext() throws Exception {
        String name = "link-authority";
        String one = item("o1", ",\"slug\":\"one\",\"created_at\":\"" + START + "\"");
        String two = item("o2", ",\"slug\":\"two\",\"created_at\":\"" + START + "\"");
        String three = item("o3", ",\"slug\":\"three\",\"created_at\":\"" + START + "\"");
        Settings settings = new Settings(name, "organizations");
        settings.pageSize = 2;
        program(
                name,
                List.of(
                        get("/v2/organizations", page(1, 2), array(one))
                                .put(
                                        "Link",
                                        "</v2/organizations?page=1>; rel=prev, <?page=2>; title=\"a,b;c\"; rel=\"alternate next\""),
                        get("/v2/organizations", page(2, 2), "[]")
                                .put(
                                        "Link",
                                        next(
                                                "https://api.buildkite.com/v2/organizations?page=3&per_page=2")),
                        get("/v2/organizations", page(3, 2), array(two, three))));
        success(run(settings));
        assertRows(
                name,
                List.of(
                        expected("organizations", "o1", "one", null, null, START, one),
                        expected("organizations", "o2", "two", null, null, START, two),
                        expected("organizations", "o3", "three", null, null, START, three)));
        consumed(name, 3);
    }

    @Test
    void supportsSuiteDefaultPaginationAndFullEmojiListsLargerThan100() throws Exception {
        String name = "suite-and-emoji";
        Settings settings =
                new Settings(name, "analytics_organizations_suites", "organizations_emojis");
        String suite = "{\"id\":\"s1\",\"name\":\"suite\"}";
        List<String> emojis = new ArrayList<>();
        List<JsonNode> expected = new ArrayList<>();
        expected.add(
                expected("analytics_organizations_suites", "s1", "alpha", null, null, null, suite));
        for (int index = 0; index < 101; index++) {
            String emoji =
                    "{\"name\":\"emoji-"
                            + index
                            + "\",\"aliases\":[],\"url\":\"https://buildkite-trap.invalid/image\"}";
            emojis.add(emoji);
            expected.add(
                    expected(
                            "organizations_emojis",
                            "emoji-" + index,
                            "alpha",
                            null,
                            null,
                            null,
                            emoji));
        }
        program(
                name,
                List.of(
                        get(
                                "/v2/organizations",
                                page(1, 100),
                                array("{\"id\":\"o1\",\"slug\":\"alpha\"}")),
                        get("/v2/analytics/organizations/alpha/suites", "", array(suite))
                                .put("Link", next("?page=2&per_page=30")),
                        get("/v2/analytics/organizations/alpha/suites", "page=2", "[]"),
                        get(
                                "/v2/organizations/alpha/emojis",
                                "",
                                array(emojis.toArray(new String[0])))));
        success(run(settings));
        assertRows(name, expected);
        consumed(name, 4);
    }

    @Test
    void rejectsEveryLinkThatChangesScopeOrDoesNotAdvanceExactlyOnePage() throws Exception {
        List<String> links =
                List.of(
                        next("https://buildkite-trap.invalid/v2/builds?page=2"),
                        next("/v2/user?page=2"),
                        next("?page=1"),
                        next("?page=3"),
                        next("?page=2&page=2"),
                        next("?page=2&per_page=99"),
                        next("?page=2&created_from=2023-01-01T00%3A00%3A00Z"),
                        next("?page=2&unrecognized=wide"),
                        next("?page=2#fragment"),
                        "<?page=2>; rel=next; anchor=\"/v2/builds\"",
                        "<?page=2>; rel=\"next next\"",
                        "<?page=2>; rel=next, <?page=2>; rel=next");
        for (int index = 0; index < links.size(); index++) {
            Settings settings = new Settings("unsafe-link-" + index, "builds");
            fails(
                    settings,
                    List.of(get("/v2/builds", builds(1), "[]").put("Link", links.get(index))),
                    "Link");
        }
    }

    @Test
    void preservesAllFrozenDateFiltersWhenLinkOmitsThemOrUsesEquivalentOffsets() throws Exception {
        String name = "equivalent-date";
        String link =
                "?page=2&created_from="
                        + enc("2024-01-01T08:00:00+08:00")
                        + "&created_to="
                        + enc("2024-01-02T08:00:00+08:00");
        program(
                name,
                List.of(
                        get("/v2/builds", builds(1), "[]").put("Link", next(link)),
                        get("/v2/builds", builds(2), "[]")));
        success(run(new Settings(name, "builds")));
        consumed(name, 2);
        assertRows(name, List.of());
    }

    @Test
    void honorsExhaustedRateLimitScopesWithoutApplyingAnUnexhaustedScopeDelay() throws Exception {
        Settings settings = new Settings("rate-limit", "access-token");
        settings.retries = 1;
        settings.retryDelay = 2000;
        String access = "{\"uuid\":\"a1\",\"scopes\":[\"read_builds\"]}";
        program(
                settings.name,
                List.of(
                        get("/v2/access-token", "", "private-provider-message")
                                .put("status", 429)
                                .put("Retry-After", "0")
                                .put("RateLimit-Remaining", "10")
                                .put("RateLimit-Reset", "100")
                                .put("RateLimit-User-Remaining", "0")
                                .put("RateLimit-User-Reset", "1"),
                        get("/v2/access-token", "", access)));
        success(run(settings));
        consumed(settings.name, 2);
        List<String> times = engine.lines(settings.name + ".times");
        assertTrue(Long.parseLong(times.get(1)) - Long.parseLong(times.get(0)) >= 1000L);
        assertRows(
                settings.name,
                List.of(expected("access-token", "a1", null, null, null, null, access)));
        Settings tooLong = new Settings("rate-over-cap", "access-token");
        tooLong.retries = 1;
        tooLong.retryDelay = 500;
        fails(
                tooLong,
                List.of(
                        get("/v2/access-token", "", "private-provider-message")
                                .put("status", 429)
                                .put("RateLimit-DeepBuildsList-Remaining", "0")
                                .put("RateLimit-DeepBuildsList-Reset", "2")),
                "max_retry_delay_ms");
    }

    @Test
    void failsExplicitlyOnPermissionsPlansAndProviderErrorsWithoutLeakingTokens() throws Exception {
        for (int status : List.of(400, 401, 403, 404, 429, 503)) {
            fails(
                    new Settings("status-" + status, "access-token"),
                    List.of(
                            get(
                                            "/v2/access-token",
                                            "",
                                            "private-provider-message " + BuildkitePeer.TOKEN)
                                    .put("status", status)),
                    "HTTP " + status);
        }
        fails(
                new Settings("logical-error", "builds"),
                List.of(get("/v2/builds", builds(1), "{\"message\":\"private-provider-message\"}")),
                "array");
    }

    @Test
    void validatesTrustAndHostnameSeparatelyAndNeverFollowsRedirects() throws Exception {
        Settings wrong = new Settings("untrusted", "access-token");
        wrong.certificate = wrongTrust;
        fails(wrong, List.of(), "certificate");
        selectIdentity("wrong-name");
        Settings name = new Settings("wrong-san", "access-token");
        name.certificate = wrongNameTrust;
        fails(name, List.of(), "certificate");
        selectIdentity("trusted");
        fails(
                new Settings("redirect", "access-token"),
                List.of(get("/v2/access-token", "", "").put("status", 302).put("mode", "redirect")),
                "302");
    }

    @Test
    void checksWireAndDecodedGzipLimitsAndStrictJson() throws Exception {
        String name = "gzip-success", access = item("unused", ",\"uuid\":\"a1\"");
        program(name, List.of(get("/v2/access-token", "", access).put("mode", "gzip")));
        success(run(new Settings(name, "access-token")));
        consumed(name, 1);
        assertRows(name, List.of(expected("access-token", "a1", null, null, null, null, access)));
        for (String mode : List.of("json", "gzip")) {
            Settings limit = new Settings("bytes-" + mode, "access-token");
            limit.bytes = 256;
            fails(
                    limit,
                    List.of(
                            get(
                                            "/v2/access-token",
                                            "",
                                            "{\"uuid\":\"a1\",\"value\":\""
                                                    + "x".repeat(4000)
                                                    + "\"}")
                                    .put("mode", mode)),
                    "max_response_bytes");
        }
        for (String body :
                List.of(
                        "{\"uuid\":\"a1\",\"uuid\":\"a2\"}",
                        "{\"uuid\":\"a1\"}{}",
                        "{\"uuid\":null}")) {
            fails(
                    new Settings("json-" + Math.abs(body.hashCode()), "access-token"),
                    List.of(get("/v2/access-token", "", body)),
                    "Buildkite");
        }
        String deep =
                "{\"uuid\":\"a1\",\"value\":" + "[".repeat(100) + "null" + "]".repeat(100) + "}";
        fails(
                new Settings("depth", "access-token"),
                List.of(get("/v2/access-token", "", deep)),
                "JSON");
    }

    @Test
    void rejectsInvalidUtf8AndBrokenCompressedDataWithoutCommittingPartialRecords()
            throws Exception {
        byte[] prefix = "{\"uuid\":\"a1\",\"description\":\"".getBytes(StandardCharsets.UTF_8);
        byte[] suffix = "\"}".getBytes(StandardCharsets.UTF_8);
        byte[] invalid = new byte[prefix.length + suffix.length + 2];
        System.arraycopy(prefix, 0, invalid, 0, prefix.length);
        invalid[prefix.length] = (byte) 0xc3;
        invalid[prefix.length + 1] = (byte) 0x28;
        System.arraycopy(suffix, 0, invalid, prefix.length + 2, suffix.length);
        fails(
                new Settings("invalid-utf8", "access-token"),
                List.of(
                        get("/v2/access-token", "", "")
                                .put("bytes", Base64.getEncoder().encodeToString(invalid))),
                "JSON");
        assertRows("invalid-utf8", List.of());
        fails(
                new Settings("broken-gzip", "access-token"),
                List.of(get("/v2/access-token", "", "not-a-gzip-stream").put("encoding", "gzip")),
                "gzip");
        assertRows("broken-gzip", List.of());
    }

    @Test
    void enforcesPerCollectionScanAndUnpaginatedItemBudgets() throws Exception {
        Settings pages = new Settings("pages-budget", "builds");
        pages.pages = 1;
        fails(
                pages,
                List.of(get("/v2/builds", builds(1), "[]").put("Link", next("?page=2"))),
                "max_pages_per_collection");
        Settings scan = new Settings("scan-budget", "builds");
        scan.scanPages = 1;
        fails(
                scan,
                List.of(get("/v2/builds", builds(1), "[]").put("Link", next("?page=2"))),
                "max_pages_per_scan");
        Settings unpaged = new Settings("unpaged-budget", "analytics_organizations_suites");
        unpaged.items = 1;
        fails(
                unpaged,
                List.of(
                        get(
                                "/v2/organizations",
                                page(1, 100),
                                array("{\"id\":\"o1\",\"slug\":\"alpha\"}")),
                        get(
                                "/v2/analytics/organizations/alpha/suites",
                                "",
                                array("{\"id\":\"s1\"}", "{\"id\":\"s2\"}"))),
                "item limit");
    }

    @Test
    void rejectsInvalidOptionsAndSkipsAnEmptyCreationWindowWithoutHttps() throws Exception {
        Settings invalid = new Settings("invalid-page-size", "access-token");
        invalid.pageSize = 101;
        fails(invalid, List.of(), "page_size");
        Settings reverse = new Settings("invalid-window", "builds");
        reverse.end = "2023-01-01T00:00:00Z";
        fails(reverse, List.of(), "end_date");
        Settings empty = new Settings("empty-window", "builds");
        empty.end = START;
        program(empty.name, List.of());
        success(run(empty));
        assertRows(empty.name, List.of());
        consumed(empty.name, 0);
    }

    @Test
    void boundsATricklingAttemptAndClosesTheSocketWhenTheEngineCancels() throws Exception {
        Settings deadline = new Settings("deadline", "access-token");
        deadline.requestTimeout = 1500;
        deadline.operationTimeout = 1500;
        fails(
                deadline,
                List.of(get("/v2/access-token", "", "{\"uuid\":\"a1\"}").put("mode", "trickle")),
                "deadline");
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> engine.exists(deadline.name + ".disconnect"));
        Settings settings = new Settings("cancel", "access-token");
        program(
                settings.name,
                List.of(get("/v2/access-token", "", "{\"uuid\":\"a1\"}").put("mode", "trickle")));
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob("/buildkite.conf", job, settings.variables()));
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> engine.exists(settings.name + ".started"));
            success(engine.cancelJob(job));
            running.get(30, TimeUnit.SECONDS);
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .until(() -> engine.exists(settings.name + ".disconnect"));
            consumed(settings.name, 1);
            assertRows(settings.name, List.of());
        } finally {
            if (!running.isDone()) {
                engine.cancelJob(job);
                running.cancel(true);
            }
        }
    }

    @Test
    void restoresAThirdLevelParentSavepointWithoutRepeatingAncestorsOrCrossingScope()
            throws Exception {
        Settings settings = new Settings("parent-original", "organizations_clusters_queues");
        String org = "{\"id\":\"o1\",\"slug\":\"alpha\"}", cluster = "{\"id\":\"cluster-1\"}";
        String queue = item("queue-1", ",\"created_at\":\"" + START + "\"");
        List<ObjectNode> steps =
                List.of(
                        get("/v2/organizations", page(1, 100), array(org)),
                        get("/v2/organizations/alpha/clusters", page(1, 100), array(cluster)),
                        get(
                                        "/v2/organizations/alpha/clusters/cluster-1/queues",
                                        page(1, 100),
                                        array(queue))
                                .put("mode", "partial"));
        String job = pendingSavepoint(settings, steps);
        program(
                "parent-restored",
                List.of(
                        get("/v2/organizations", page(1, 100), array(org)),
                        get("/v2/organizations/alpha/clusters", page(1, 100), array(cluster)),
                        get(
                                "/v2/organizations/alpha/clusters/cluster-1/queues",
                                page(1, 100),
                                array(queue))));
        success(engine.restoreJob("/buildkite.conf", job, settings.variables()));
        assertRows(
                settings.name,
                List.of(
                        expected(
                                "organizations_clusters_queues",
                                "queue-1",
                                "alpha",
                                null,
                                "cluster-1",
                                START,
                                queue)));
        consumed("parent-restored", 3);
    }

    @Test
    void rejectsAChangedRecoveredParentBeforeContactingItsSavedChild() throws Exception {
        Settings settings =
                new Settings("changed-parent-original", "organizations_clusters_queues");
        String job =
                pendingSavepoint(
                        settings,
                        List.of(
                                get(
                                        "/v2/organizations",
                                        page(1, 100),
                                        array("{\"id\":\"o1\",\"slug\":\"alpha\"}")),
                                get(
                                        "/v2/organizations/alpha/clusters",
                                        page(1, 100),
                                        array("{\"id\":\"cluster-1\"}")),
                                get(
                                                "/v2/organizations/alpha/clusters/cluster-1/queues",
                                                page(1, 100),
                                                "[]")
                                        .put("mode", "partial")));
        program(
                "parent-changed",
                List.of(
                        get(
                                "/v2/organizations",
                                page(1, 100),
                                array("{\"id\":\"o1\",\"slug\":\"beta\"}"))));
        Container.ExecResult result =
                engine.restoreJob("/buildkite.conf", job, settings.variables());
        assertNotEquals(0, result.getExitCode(), result.getStdout() + result.getStderr());
        assertTrue((result.getStdout() + result.getStderr()).contains("checkpoint page changed"));
        consumed("parent-changed", 1);
        assertRows(settings.name, List.of());
    }

    @Test
    void restoresTheNextBuildPageWithoutDuplicatingCommittedRecords() throws Exception {
        Settings settings = new Settings("page-original", "builds");
        String first = item("first", ",\"created_at\":\"" + START + "\""),
                last = item("last", ",\"created_at\":\"" + START + "\"");
        String job =
                pendingSavepoint(
                        settings,
                        List.of(
                                get("/v2/builds", builds(1), array(first))
                                        .put("Link", next("?page=2")),
                                get("/v2/builds", builds(2), array(last)).put("mode", "partial")));
        program("page-restored", List.of(get("/v2/builds", builds(2), array(last))));
        success(engine.restoreJob("/buildkite.conf", job, settings.variables()));
        consumed("page-restored", 1);
        assertRows(
                settings.name,
                List.of(
                        expected("builds", "first", null, null, null, START, first),
                        expected("builds", "last", null, null, null, START, last)));
    }

    @Test
    void restoresACompletedPipelineWithNoNetworkOrDuplicateCommittedRow() throws Exception {
        Settings settings = new Settings("terminal-original", "access-token");
        String access = "{\"uuid\":\"a1\",\"description\":\"read only\"}";
        List<JsonNode> expected =
                List.of(expected("access-token", "a1", null, null, null, null, access));
        program(settings.name, List.of(get("/v2/access-token", "", access)));
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(
                        () ->
                                engine.executeJob(
                                        "/buildkite-terminal.conf", job, settings.variables()));
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
        program("terminal-restored", List.of());
        Future<Container.ExecResult> resumed =
                tasks.submit(
                        () ->
                                engine.restoreJob(
                                        "/buildkite-terminal.conf", job, settings.variables()));
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> "RUNNING".equals(engine.getJobStatus(job)));
            long baseline = engine.completedAcrossPipelines(job);
            Awaitility.await()
                    .atMost(35, TimeUnit.SECONDS)
                    .until(() -> engine.completedAcrossPipelines(job) > baseline);
            assertRows(settings.name, expected);
            consumed("terminal-restored", 0);
        } finally {
            if (!resumed.isDone()) {
                engine.cancelJob(job);
            }
            resumed.get(30, TimeUnit.SECONDS);
        }
    }

    @Test
    void serializesAnEmittedRowBeforeAdvancementAndRejectsChangedReplayPages() throws Exception {
        String first = "{\"id\":\"probe-1\",\"created_at\":\"" + START + "\"}",
                second = "{\"id\":\"probe-2\",\"created_at\":\"" + START + "\"}";
        for (String mode : List.of("resume", "changed")) {
            String name = "reader-" + mode;
            program(
                    name,
                    List.of(
                            get("/v2/builds", builds(1), array(first, second)),
                            get(
                                    "/v2/builds",
                                    builds(1),
                                    array(
                                            first,
                                            mode.equals("changed")
                                                    ? second.replace("probe-2", "changed")
                                                    : second))));
            success(engine.probe(mode));
            consumed(name, 2);
        }
        program("reader-zero", List.of());
        success(engine.probe("zero"));
        consumed("reader-zero", 0);
    }

    /**
     * Saves actual Zeta state after a completed checkpoint while a later body is incomplete.
     *
     * <p>Only the current job and owned response latch are canceled or released on failure.
     */
    private String pendingSavepoint(Settings settings, List<ObjectNode> steps) throws Exception {
        program(settings.name, steps);
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob("/buildkite.conf", job, settings.variables()));
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> engine.exists(settings.name + ".started"));
            long baseline = engine.getCompletedCheckpointCount(job);
            Awaitility.await()
                    .atMost(35, TimeUnit.SECONDS)
                    .until(() -> engine.getCompletedCheckpointCount(job) > baseline);
            success(engine.savepointJob(job));
            running.get(30, TimeUnit.SECONDS);
        } finally {
            engine.touch(settings.name + ".release");
            if (!running.isDone()) {
                engine.cancelJob(job);
                running.cancel(true);
            }
        }
        consumed(settings.name, steps.size());
        return job;
    }

    private static String item(String id, String fields) {
        return "{\"id\":\"" + id + "\"" + fields + EXTRA + "}";
    }

    private static String without(String record, String key) throws IOException {
        ObjectNode result = (ObjectNode) JSON.readTree(record);
        result.remove(key);
        return result.toString();
    }

    private static String array(String... records) {
        return "[" + String.join(",", records) + "]";
    }

    private static String page(int page, int size) {
        return "page=" + page + "&per_page=" + size;
    }

    private static String builds(int page) {
        return page(page, 100) + "&created_from=" + enc(START) + "&created_to=" + enc(END);
    }

    private static String enc(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private static String next(String uri) {
        return "<" + uri + ">; rel=\"next\"";
    }

    private static ObjectNode get(String path, String query, String response) {
        return JSON.createObjectNode()
                .put("path", path)
                .put("query", query)
                .put("response", response);
    }

    private static JsonNode expected(
            String resource,
            String id,
            String org,
            String pipeline,
            String cluster,
            String created,
            String record)
            throws IOException {
        ObjectNode row = JSON.createObjectNode();
        row.put("resource", resource)
                .put("id", id)
                .put("organization_slug", org)
                .put("pipeline_slug", pipeline)
                .put("cluster_id", cluster)
                .put("created_at", created);
        row.set("record", JSON.readTree(record));
        return row;
    }

    private void assertRows(String name, List<JsonNode> expected) throws Exception {
        Map<JsonNode, Integer> actual = new HashMap<>();
        for (String line : engine.rows(name)) {
            JsonNode parsed = JSON.readTree(line);
            assertTrue(
                    parsed.isObject() && parsed.size() == 7, "exact seven-column schema: " + line);
            assertTrue(
                    parsed.path("record").isTextual(), "record must remain JSON stored in STRING");
            ObjectNode row = (ObjectNode) parsed;
            row.set("record", JSON.readTree(parsed.path("record").textValue()));
            actual.merge(row, 1, Integer::sum);
        }
        Map<JsonNode, Integer> wanted = new HashMap<>();
        expected.forEach(row -> wanted.merge(row, 1, Integer::sum));
        assertEquals(wanted, actual, name);
    }

    private void program(String name, List<ObjectNode> steps) throws Exception {
        ArrayNode script = JSON.createArrayNode();
        steps.forEach(script::add);
        Path file = Files.createTempFile(directory, "script-", ".json");
        Files.writeString(file, script.toString());
        engine.install(file, name + ".json");
        Path selected = Files.createTempFile(directory, "active-", ".txt");
        Files.writeString(selected, name);
        engine.install(selected, "active");
        active = name;
    }

    private void selectIdentity(String identity) throws Exception {
        if (identity.equals("trusted") && !engine.exists("identity")) {
            return;
        }
        Path selected = Files.createTempFile(directory, "identity-", ".txt");
        Files.writeString(selected, identity);
        engine.install(selected, "identity");
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> engine.lines("identity-ready").equals(List.of(identity)));
    }

    private void consumed(String name, int count) throws Exception {
        assertFalse(engine.exists(name + ".invalid"), name);
        assertEquals(count, engine.lines(name + ".requests").size(), name);
    }

    private Container.ExecResult run(Settings settings) throws Exception {
        return engine.executeJob("/buildkite.conf", jobId(), settings.variables());
    }

    private void fails(Settings settings, List<ObjectNode> steps, String reason) throws Exception {
        program(settings.name, steps);
        Container.ExecResult result = run(settings);
        String output = result.getStdout() + result.getStderr();
        assertNotEquals(0, result.getExitCode(), output);
        assertTrue(output.contains(reason), output);
        assertFalse(output.contains(BuildkitePeer.TOKEN), "API key leaked");
        assertFalse(output.contains("private-provider-message"), "provider payload leaked");
        consumed(settings.name, steps.size());
    }

    private static String jobId() {
        return Long.toString(
                ThreadLocalRandom.current().nextLong(1_000_000_000_000L, 9_000_000_000_000L));
    }

    private static void success(Container.ExecResult result) {
        assertEquals(0, result.getExitCode(), result.getStdout() + result.getStderr());
    }

    private static String identity(String name, String hostname) throws Exception {
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
                                "CN=" + hostname,
                                "-ext",
                                "SAN=dns:" + hostname,
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                store.toString(),
                                "-storepass",
                                BuildkitePeer.PASSWORD,
                                "-keypass",
                                BuildkitePeer.PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String output = new String(keytool.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, keytool.waitFor(), output);
        KeyStore keys = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(store)) {
            keys.load(input, BuildkitePeer.PASSWORD.toCharArray());
        }
        return Base64.getEncoder().encodeToString(keys.getCertificate("api").getEncoded());
    }

    /**
     * Supplies exact source options with isolated output and bounded I/O.
     *
     * <p>Each scenario changes only the option whose boundary is being exercised.
     */
    private static final class Settings {
        private final String name;
        private final String[] resources;
        private String certificate = trust, end = END;
        private int pageSize = 100,
                pages = 1000,
                scanPages = 10000,
                items = 10000,
                requestTimeout = 120000,
                operationTimeout = 180000,
                retries,
                retryDelay = 60000,
                bytes = 8388608;

        private Settings(String name, String... resources) {
            this.name = name;
            this.resources = resources;
        }

        private String[] variables() {
            ArrayNode selected = JSON.createArrayNode();
            for (String resource : resources) {
                selected.add(resource);
            }
            return new String[] {
                "api.output=" + name,
                "api.resources=" + selected,
                "api.token=" + BuildkitePeer.TOKEN,
                "api.certificate=" + certificate,
                "api.start=" + START,
                "api.end=" + end,
                "api.page_size=" + pageSize,
                "api.pages=" + pages,
                "api.scan_pages=" + scanPages,
                "api.items=" + items,
                "api.request_timeout=" + requestTimeout,
                "api.operation_timeout=" + operationTimeout,
                "api.retries=" + retries,
                "api.retry_delay=" + retryDelay,
                "api.bytes=" + bytes
            };
        }
    }

    /**
     * Hosts normal packaged plugins and independent Java TLS fixtures in one owned worker.
     *
     * <p>DNS and certificate trust overrides apply only to this integration-test container.
     */
    private static final class Engine extends SeaTunnelContainer {
        private static final String PEER = "/tmp/buildkite-peer";
        private final Path fixtures;
        private final HttpClient checkpoints =
                HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

        private Engine(Path fixtures) {
            this.fixtures = fixtures;
        }

        /**
         * Passes entry-point variables as argv rather than a logged shell command.
         *
         * <p>This also preserves reserved characters in test credentials and timestamp options.
         */
        @Override
        protected Container.ExecResult executeCommand(
                GenericContainer<?> container, List<String> command)
                throws IOException, InterruptedException {
            return container.execInContainer(command.toArray(new String[0]));
        }

        /**
         * Stages only the owned peer, certificates and existing shaded JSON libraries.
         *
         * <p>The production API alias resolves to loopback only inside this container.
         */
        @Override
        protected void executeExtraCommands(GenericContainer<?> container)
                throws IOException, InterruptedException {
            super.executeExtraCommands(container);
            container.withExtraHost(BuildkitePeer.HOST, "127.0.0.1");
            container.withExtraHost("buildkite-trap.invalid", "127.0.0.1");
            for (String name : List.of("trusted.p12", "wrong-name.p12")) {
                container.withCopyFileToContainer(
                        MountableFile.forHostPath(fixtures.resolve(name)), PEER + "/" + name);
            }
            try {
                stage(container, BuildkitePeer.class);
                stage(container, BuildkiteRecoveryProbe.class);
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
            } catch (URISyntaxException failure) {
                throw new IOException("Cannot stage the Java Buildkite fixture", failure);
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
                    BuildkitePeer.class.getName(),
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
                    BuildkiteRecoveryProbe.class.getName(),
                    trust,
                    mode);
        }

        private void install(Path file, String name) {
            server.copyFileToContainer(MountableFile.forHostPath(file), PEER + "/" + name);
        }

        private void touch(String name) throws Exception {
            success(server.execInContainer("touch", PEER + "/" + name));
        }

        private boolean exists(String name) throws Exception {
            return server.execInContainer("test", "-f", PEER + "/" + name).getExitCode() == 0;
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
            String path = "/tmp/seatunnel_mnt/buildkite-" + name;
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

        private static List<String> linesOf(String output) {
            List<String> result = new ArrayList<>();
            for (String line : output.split("\\R")) {
                if (!line.isBlank()) {
                    result.add(line);
                }
            }
            return result;
        }
    }
}
