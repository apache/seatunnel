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

package org.apache.seatunnel.e2e.connector.blogger;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
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
 * Exercises the shipped Blogger artifact in Java 11 Zeta.
 *
 * <p>A separate HTTPS peer verifies both OAuth grants and read-only API requests. Rows are
 * committed to a transactional LocalFile sink and compared by value.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BloggerIT extends TestSuiteBase {
    private static final String CONF = "/blogger.conf";
    private static final String ROOT = "/tmp/blogger-https-fixture";
    private static final String OUTPUT = "/tmp/seatunnel_mnt/blogger-";
    private static final String SELF = "/v3/users/self/blogs";
    private static final String BLOG_A = "90071992547409931234567";
    private static final String BLOG_B = "90071992547409939876543";
    private static final String POST_A = "18583759836732561751";
    private static final String POST_B = "18483659423189563223";
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
        char[] password = BloggerPeer.PASSWORD.toCharArray();
        KeyStore identities = KeyStore.getInstance("PKCS12");
        identities.load(null, password);
        KeyStore trust = KeyStore.getInstance("JKS");
        trust.load(null, password);
        Map<String, List<String>> names =
                Map.of(
                        "trusted",
                                List.of(BloggerPeer.API, BloggerPeer.OAUTH, BloggerPeer.REDIRECT),
                        "untrusted", List.of(BloggerPeer.OAUTH),
                        "wrong", List.of(BloggerPeer.MISMATCH));
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
    void listsTheAuthenticatedUsersBlogsAndPreservesOpaqueContent() throws Exception {
        Settings options = new Settings("users", "users");
        String first =
                "{\"id\":\""
                        + BLOG_A
                        + "\",\"name\":\"博客\",\"locale\":{\"language\":\"zh\"},"
                        + "\"description\":null,\"pages\":{\"totalItems\":90071992547409931234567},"
                        + "\"url\":\"https://never-fetch.example/about\"}";
        String second = "{\"id\":\"" + BLOG_B + "\",\"customMetaData\":\"{}\"}";
        program(options, grant(), api(SELF, list("blog", null, first, second)));
        success(run(options));
        assertRows(options, expected(BLOG_A, null, first), expected(BLOG_B, null, second));
        assertEquals(2, requests(options).size());
    }

    @Test
    void listsFullPostsThroughOpaqueShortAndEmptyPages() throws Exception {
        Settings options = new Settings("blogs", "blogs");
        String first =
                "{\"id\":\""
                        + POST_A
                        + "\",\"blog\":{\"id\":\""
                        + BLOG_A
                        + "\"},"
                        + "\"content\":\"<img src=\\\"https://never-fetch.example/photo\\\">中文\","
                        + "\"replies\":{\"totalItems\":9007199254740993},\"labels\":[null,\"a\"]}";
        String second =
                "{\"id\":\""
                        + POST_A
                        + "\",\"blog\":{\"id\":\""
                        + BLOG_B
                        + "\"},"
                        + "\"content\":null,\"images\":[{\"url\":\"https://never-fetch.example/private\"}]}";
        program(
                options,
                grant(),
                api(SELF, twoBlogs()),
                api(posts(BLOG_A, true), list("post", "a+/=", first)),
                api(posts(BLOG_A, true) + "&pageToken=a%2B%2F%3D", list("post", "skip")),
                api(posts(BLOG_A, true) + "&pageToken=skip", list("post", null)),
                api(posts(BLOG_B, true), list("post", null, second)));
        success(run(options));
        assertRows(options, expected(BLOG_A, POST_A, first), expected(BLOG_B, POST_A, second));
        assertEquals(6, requests(options).size());
    }

    @Test
    void fetchesPostDetailUnderItsOwningBlogWithoutAListBodyOrAPostCrossProduct() throws Exception {
        Settings options = new Settings("posts", "posts");
        String first = postDetail(BLOG_A, POST_A, "first detail");
        String second = postDetail(BLOG_B, POST_A, "second detail");
        String third = postDetail(BLOG_B, POST_B, "third detail");
        program(
                options,
                grant(),
                api(SELF, twoBlogs()),
                api(posts(BLOG_A, false), list("post", null, id(POST_A))),
                api(detailPath(BLOG_A, POST_A), first),
                api(posts(BLOG_B, false), list("post", "next", id(POST_A))),
                api(detailPath(BLOG_B, POST_A), second),
                api(posts(BLOG_B, false) + "&pageToken=next", list("post", null, id(POST_B))),
                api(detailPath(BLOG_B, POST_B), third));
        success(run(options));
        assertRows(
                options,
                expected(BLOG_A, POST_A, first),
                expected(BLOG_B, POST_A, second),
                expected(BLOG_B, POST_B, third));
        assertEquals(8, requests(options).size());
    }

    @Test
    void readsPagesForBothBlogsAndKeepsGzipPayloadsAndHtml() throws Exception {
        Settings options = new Settings("pages", "pages");
        String first =
                "{\"id\":\"11\",\"blog\":{\"id\":\""
                        + BLOG_A
                        + "\"},"
                        + "\"content\":\"<p>中文</p>\",\"status\":null,\"custom\":{\"value\":4.125000000009}}";
        String second = "{\"id\":\"22\",\"blog\":{\"id\":\"" + BLOG_B + "\"},\"title\":\"第二页\"}";
        program(
                options,
                grant(),
                api(SELF, twoBlogs()),
                api(pages(BLOG_A), list("page", "next", first)),
                api(pages(BLOG_A) + "&pageToken=next", list("page", null)),
                api(pages(BLOG_B), 200, "gzip", list("page", null, second)));
        success(run(options));
        assertRows(options, expected(BLOG_A, null, first), expected(BLOG_B, null, second));
        assertEquals(5, requests(options).size());
    }

    @Test
    void readsCommentsByBlogAndPostAndIgnoresPreviousNavigation() throws Exception {
        Settings options = new Settings("comments", "comments");
        String first = comment(BLOG_A, POST_A, "17", "第一条");
        String second = comment(BLOG_A, POST_B, "17", "another post");
        String third =
                comment(BLOG_B, POST_A, "17", "<a href='https://never-fetch.example/'>fixture</a>");
        String noNext =
                "{\"kind\":\"blogger#commentList\",\"prevPageToken\":\"do-not-send\",\"items\":["
                        + first
                        + "]}";
        program(
                options,
                grant(),
                api(SELF, twoBlogs()),
                api(posts(BLOG_A, false), list("post", null, id(POST_A), id(POST_B))),
                api(comments(BLOG_A, POST_A), list("comment", "comment+/=")),
                api(comments(BLOG_A, POST_A) + "&pageToken=comment%2B%2F%3D", noNext),
                api(comments(BLOG_A, POST_B), list("comment", null, second)),
                api(posts(BLOG_B, false), list("post", null, id(POST_A))),
                api(comments(BLOG_B, POST_A), list("comment", null, third)));
        success(run(options));
        assertRows(
                options,
                expected(BLOG_A, POST_A, first),
                expected(BLOG_A, POST_B, second),
                expected(BLOG_B, POST_A, third));
        assertEquals(8, requests(options).size());
    }

    @Test
    void distinguishesTypedEmptyAndPermissionFailureDuringDiscoveryOrRead() throws Exception {
        Settings empty = new Settings("empty-account", "comments");
        program(empty, grant(), api(SELF, "{\"kind\":\"blogger#blogList\"}"));
        success(run(empty));
        assertTrue(rows(empty).isEmpty());
        assertEquals(2, requests(empty).size());

        Settings emptyPages = new Settings("empty-pages", "pages");
        program(
                emptyPages,
                grant(),
                api(SELF, list("blog", null, id(BLOG_A))),
                api(pages(BLOG_A), "{\"kind\":\"blogger#pageList\"}"));
        success(run(emptyPages));
        assertTrue(rows(emptyPages).isEmpty());
        assertEquals(3, requests(emptyPages).size());

        Settings denied = new Settings("forbidden-blog", "comments");
        program(
                denied,
                grant(),
                api(SELF, list("blog", null, id(BLOG_A))),
                api(posts(BLOG_A, false), 403, "", apiError("insufficientPermissions")));
        failure(run(denied), "HTTP 403");
        assertTrue(rows(denied).isEmpty());
        assertEquals(3, requests(denied).size());
    }

    @Test
    void renewsARejectedBearerOnceWithoutChangingThePageRequest() throws Exception {
        Settings options = new Settings("stale-bearer", "users");
        program(
                options,
                grant(),
                api(SELF, 401, "", apiError("authError")),
                grant(BloggerPeer.NEXT_ACCESS, 3600),
                api(SELF, list("blog", null, id(BLOG_A))).put("bearer", BloggerPeer.NEXT_ACCESS));
        success(run(options));
        assertRows(options, expected(BLOG_A, null, id(BLOG_A)));
        assertEquals(4, requests(options).size());

        Settings repeated = new Settings("second-unauthorized", "users");
        program(
                repeated,
                grant(),
                api(SELF, 401, "", apiError("authError")),
                grant(BloggerPeer.NEXT_ACCESS, 3600),
                api(SELF, 401, "", apiError("authError")).put("bearer", BloggerPeer.NEXT_ACCESS));
        failure(run(repeated), "HTTP 401");
        assertEquals(4, requests(repeated).size());
        assertTrue(rows(repeated).isEmpty());
    }

    @Test
    void renewsAOneSecondTokenBeforeTheNextPage() throws Exception {
        Settings options = new Settings("expires-between-pages", "pages");
        String stored = "{\"id\":\"77\",\"content\":\"cached token expired\"}";
        program(
                options,
                grant(BloggerPeer.TOKEN, 1),
                api(SELF, list("blog", null, id(BLOG_A))).put("delayMs", 1600),
                grant(BloggerPeer.NEXT_ACCESS, 3600),
                api(pages(BLOG_A), list("page", null, stored))
                        .put("bearer", BloggerPeer.NEXT_ACCESS));
        success(run(options));
        assertRows(options, expected(BLOG_A, null, stored));
        assertEquals(4, requests(options).size());
    }

    @Test
    void failsOnRevokedOrInvalidOAuthGrantsBeforeCallingBlogger() throws Exception {
        Settings revoked = new Settings("revoked-grant", "users");
        program(
                revoked,
                token(
                        400,
                        "",
                        "{\"error\":\"invalid_grant\",\"error_description\":\""
                                + BloggerPeer.PRIVATE
                                + "\"}"));
        failure(run(revoked), "OAuth refresh returned HTTP 400");
        assertEquals(1, requests(revoked).size());

        Settings malformed = new Settings("invalid-token-body", "users");
        program(
                malformed,
                token(
                        200,
                        "",
                        "{\"access_token\":\""
                                + BloggerPeer.TOKEN
                                + "\",\"token_type\":\"Bearer\",\"expires_in\":0}"));
        failure(run(malformed), "invalid OAuth token response");
        assertEquals(1, requests(malformed).size());

        Settings scope = new Settings("insufficient-scope", "users");
        program(
                scope,
                token(
                        200,
                        "",
                        "{\"access_token\":\""
                                + BloggerPeer.TOKEN
                                + "\",\"token_type\":\"Bearer\",\"expires_in\":3600,\"scope\":\"profile\"}"));
        failure(run(scope), "does not have blogger.readonly scope");
        assertEquals(1, requests(scope).size());
    }

    @Test
    void retriesOnlyTransientQuotaResponsesAndHonorsTheTotalBudget() throws Exception {
        Settings recover = new Settings("quota-retry", "users");
        program(
                recover,
                grant(),
                api(SELF, 403, "", apiError("userRateLimitExceeded")),
                api(SELF, 429, "", apiError("rateLimitExceeded")).put("retryAfter", "1"),
                api(SELF, list("blog", null, id(BLOG_A))));
        success(run(recover));
        assertRows(recover, expected(BLOG_A, null, id(BLOG_A)));
        List<JsonNode> calls = requests(recover);
        assertEquals(4, calls.size());
        assertTrue(
                calls.get(3).path("nanoTime").asLong() - calls.get(2).path("nanoTime").asLong()
                        >= TimeUnit.MILLISECONDS.toNanos(950));

        Settings quota = new Settings("quota-permanent", "users");
        program(quota, grant(), api(SELF, 403, "", apiError("dailyLimitExceeded")));
        failure(run(quota), "HTTP 403");
        assertEquals(2, requests(quota).size());

        Settings budget = new Settings("grant-and-api-budget", "users").set("deadline", 15000);
        program(
                budget,
                grant(),
                api(SELF, 429, "", apiError("rateLimitExceeded")).put("retryAfter", "60"));
        failure(run(budget), "exceeds request deadline");
        assertEquals(2, requests(budget).size());

        Settings finite = new Settings("finite-attempts", "users").set("attempts", 2);
        program(
                finite,
                grant(),
                api(SELF, 503, "", apiError("backendError")),
                api(SELF, 503, "", apiError("backendError")));
        failure(run(finite), "retry limit reached after HTTP 503");
        assertEquals(3, requests(finite).size());
    }

    @Test
    void rejectsMalformedListsWrongParentsAndNonPaginationTokens() throws Exception {
        Map<String, String> bad = new LinkedHashMap<>();
        bad.put("wrong-envelope", "{\"message\":\"" + BloggerPeer.PRIVATE + "\"}");
        bad.put("invalid-items", "{\"kind\":\"blogger#blogList\",\"items\":null}");
        bad.put(
                "unexpected-next",
                "{\"kind\":\"blogger#blogList\",\"nextPageToken\":\"impossible\"}");
        bad.put("duplicates", "{\"kind\":\"blogger#blogList\",\"kind\":\"blogger#blogList\"}");
        bad.put("invalid-blog-id", list("blog", null, "{\"id\":\"../escape\"}"));
        for (Map.Entry<String, String> row : bad.entrySet()) {
            Settings options = new Settings(row.getKey(), "users");
            program(options, grant(), api(SELF, row.getValue()));
            failure(run(options), "Blogger");
            assertTrue(rows(options).isEmpty());
        }

        Settings wrongDetail = new Settings("wrong-post-detail", "posts");
        program(
                wrongDetail,
                grant(),
                api(SELF, list("blog", null, id(BLOG_A))),
                api(posts(BLOG_A, false), list("post", null, id(POST_A))),
                api(detailPath(BLOG_A, POST_A), postDetail(BLOG_A, POST_B, "wrong row")));
        failure(run(wrongDetail), "does not match its requested post ID");
        assertTrue(rows(wrongDetail).isEmpty());

        Settings wrongComment = new Settings("wrong-owning-post", "comments");
        program(
                wrongComment,
                grant(),
                api(SELF, list("blog", null, id(BLOG_A))),
                api(posts(BLOG_A, false), list("post", null, id(POST_A))),
                api(
                        comments(BLOG_A, POST_A),
                        list("comment", null, comment(BLOG_A, POST_B, "18", "wrong"))));
        failure(run(wrongComment), "association differs from its requested parent");
        assertTrue(rows(wrongComment).isEmpty());
    }

    @Test
    void failsCyclesExpiredTokensAndPageCapsWithoutRestarting() throws Exception {
        Settings cycle = new Settings("repeated-cursor", "pages");
        program(
                cycle,
                grant(),
                api(SELF, list("blog", null, id(BLOG_A))),
                api(pages(BLOG_A), list("page", "loop-a")),
                api(pages(BLOG_A) + "&pageToken=loop-a", list("page", "loop-b")),
                api(pages(BLOG_A) + "&pageToken=loop-b", list("page", "loop-a")));
        failure(run(cycle), "repeated continuation cursor");
        assertEquals(5, requests(cycle).size());

        Settings expired = new Settings("expired-cursor", "pages");
        program(
                expired,
                grant(),
                api(SELF, list("blog", null, id(BLOG_A))),
                api(pages(BLOG_A), list("page", "expired-opaque")),
                api(pages(BLOG_A) + "&pageToken=expired-opaque", 400, "", apiError("badRequest")));
        failure(run(expired), "HTTP 400");
        assertEquals(4, requests(expired).size());

        Settings cap = new Settings("page-cap", "pages").set("maxPages", 1);
        program(
                cap,
                grant(),
                api(SELF, list("blog", null, id(BLOG_A))),
                api(pages(BLOG_A), list("page", "another", id("99"))));
        failure(run(cap), "requires more than max_pages");
        assertTrue(rows(cap).isEmpty());
    }

    @Test
    void boundsWireBytesExpandedBodiesAndTheAccountBlogList() throws Exception {
        for (String mode : List.of("", "gzip")) {
            Settings options =
                    new Settings(mode.isEmpty() ? "wire-cap" : "expanded-cap", "users")
                            .set("bytes", 1024);
            String large =
                    "{\"id\":\""
                            + BLOG_A
                            + "\",\"private\":\""
                            + BloggerPeer.PRIVATE.repeat(120)
                            + "\"}";
            program(options, grant(), api(SELF, 200, mode, list("blog", null, large)));
            failure(run(options), "max_response_bytes");
            assertTrue(rows(options).isEmpty());
        }

        Settings cap = new Settings("blog-count", "posts").set("maxBlogs", 1);
        program(cap, grant(), api(SELF, twoBlogs()));
        failure(run(cap), "oversized items collection");
        assertEquals(2, requests(cap).size());
    }

    @Test
    void closesSlowTokenAndDataConnectionsOnDeadlineOrCancellation() throws Exception {
        Settings deadline =
                new Settings("oauth-trickle", "users")
                        .set("deadline", 4000)
                        .set("readTimeout", 15000);
        program(deadline, token(200, "trickle", ""));
        failure(run(deadline), "request deadline exceeded");
        awaitRequests(deadline, 1);
        awaitClosed(deadline);
        assertTrue(rows(deadline).isEmpty());

        Settings cancel = new Settings("data-cancel", "users").set("deadline", 60000);
        program(cancel, grant(), api(SELF, 200, "trickle", ""));
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(CONF, job, cancel.variables()));
        try {
            awaitRequests(cancel, 2);
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
    void keepsOAuthAndBloggerHeadersOnTheCorrectTlsHostsAndRejectsRedirects() throws Exception {
        Settings oauth = new Settings("oauth-redirect", "users");
        program(oauth, token(307, "", "unexpected redirect"));
        failure(run(oauth), "HTTP 307");
        assertEquals(1, requests(oauth).size());

        Settings data = new Settings("data-redirect", "users");
        program(data, grant(), api(SELF, 302, "", "unexpected redirect"));
        failure(run(data), "HTTP 302");
        assertEquals(2, requests(data).size());

        try {
            for (String alias : List.of("untrusted", "wrong")) {
                certificates(alias);
                Settings badTls = new Settings("tls-" + alias, "users");
                program(badTls);
                failure(run(badTls), "HTTPS or TLS exchange failed");
                assertTrue(requests(badTls).isEmpty());
            }
        } finally {
            certificates("trusted");
        }
    }

    @Test
    void savepointRestoresTheExactBlogPostAndCommentCursor() throws Exception {
        Settings options = new Settings("nested-savepoint", "comments").set("deadline", 120000);
        String first = comment(BLOG_A, POST_A, "1", "first");
        String second = comment(BLOG_A, POST_A, "2", "resumed");
        String third = comment(BLOG_B, POST_A, "3", "another blog");
        String page2 = comments(BLOG_A, POST_A) + "&pageToken=saved";
        String sameIds =
                list(
                        "blog",
                        null,
                        "{\"id\":\"" + BLOG_A + "\",\"name\":\"edited blog label\"}",
                        id(BLOG_B));
        program(
                options,
                grant(),
                api(SELF, twoBlogs()),
                api(posts(BLOG_A, false), list("post", null, id(POST_A))),
                api(comments(BLOG_A, POST_A), list("comment", "saved", first)),
                api(page2, 200, "hold", list("comment", null, second)),
                grant(),
                api(SELF, sameIds),
                api(
                        posts(BLOG_A, false),
                        list(
                                "post",
                                null,
                                "{\"id\":\"" + POST_A + "\",\"title\":\"edited post label\"}")),
                api(page2, list("comment", null, second)),
                api(posts(BLOG_B, false), list("post", null, id(POST_A))),
                api(comments(BLOG_B, POST_A), list("comment", null, third)));
        resume(options, 5, null);
        assertEquals(11, requests(options).size());
        assertRows(
                options,
                expected(BLOG_A, POST_A, first),
                expected(BLOG_A, POST_A, second),
                expected(BLOG_B, POST_A, third));
    }

    @Test
    void rejectsChangedParentOrderBeforeAttachingSavedPostProgressToAnotherBlog() throws Exception {
        Settings options = new Settings("savepoint-parent-drift", "posts").set("deadline", 120000);
        String kept = postDetail(BLOG_A, POST_A, "saved row");
        program(
                options,
                grant(),
                api(SELF, twoBlogs()),
                api(posts(BLOG_A, false), list("post", null, id(POST_A))),
                api(detailPath(BLOG_A, POST_A), kept),
                api(posts(BLOG_B, false), 200, "hold", list("post", null, id(POST_A))),
                grant(),
                api(SELF, list("blog", null, id(BLOG_B), id(BLOG_A))));
        resume(options, 5, "saved response page changed");
        assertEquals(7, requests(options).size());
        assertRows(options, expected(BLOG_A, POST_A, kept));
    }

    @Test
    void validatesResourcePageSizeAndParallelismBeforeOAuth() throws Exception {
        Settings resource = new Settings("bad-resource", "unknown");
        program(resource);
        failure(run(resource), "supported Blogger collection");

        Settings page = new Settings("bad-page-size", "pages").set("pageSize", 101);
        program(page);
        failure(run(page), "page_size");

        Settings parallel = new Settings("bad-parallelism", "users").set("parallelism", 2);
        program(parallel);
        failure(run(parallel), "source parallelism must be 1");

        assertTrue(requests(resource).isEmpty());
        assertTrue(requests(page).isEmpty());
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
        Path program = Files.createTempFile(directory, "blogger-steps-", ".json");
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
        Path source = Files.createTempFile(directory, "blogger-fixture-", ".txt");
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
            JsonNode blog = row.path("blog_id");
            JsonNode post = row.path("post_id");
            assertTrue(blog.isTextual());
            actual.add(
                    expected(
                            blog.asText(),
                            post.isNull() || post.isMissingNode() ? null : post.asText(),
                            row.path("data").asText()));
        }
        List<String> target = new ArrayList<>(Arrays.asList(wanted));
        Collections.sort(target);
        Collections.sort(actual);
        assertEquals(target, actual);
    }

    private static String expected(String blog, String post, String row) throws IOException {
        return blog
                + "|"
                + (post == null ? "<no-post>" : post)
                + "|"
                + JSON.readTree(row).toString();
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

    private static ObjectNode token(int status, String mode, String response) {
        return step(BloggerPeer.OAUTH, "/token", status, mode, response);
    }

    private static ObjectNode grant() {
        return grant(BloggerPeer.TOKEN, 3600);
    }

    private static ObjectNode grant(String bearer, int lifetime) {
        String response =
                JSON.createObjectNode()
                        .put("access_token", bearer)
                        .put("token_type", "Bearer")
                        .put("expires_in", lifetime)
                        .put("scope", BloggerPeer.SCOPE)
                        .toString();
        return token(200, "", response);
    }

    private static ObjectNode api(String target, String response) {
        return api(target, 200, "", response);
    }

    private static ObjectNode api(String target, int status, String mode, String response) {
        return step(BloggerPeer.API, target, status, mode, response)
                .put("bearer", BloggerPeer.TOKEN);
    }

    private static ObjectNode step(
            String host, String target, int status, String mode, String response) {
        return JSON.createObjectNode()
                .put("host", host)
                .put("target", target)
                .put("status", status)
                .put("mode", mode)
                .put("response", response);
    }

    private static String posts(String blog, boolean bodies) {
        return "/v3/blogs/" + blog + "/posts?maxResults=100&fetchBodies=" + bodies;
    }

    private static String detailPath(String blog, String post) {
        return "/v3/blogs/" + blog + "/posts/" + post + "?fetchBody=true";
    }

    private static String pages(String blog) {
        return "/v3/blogs/" + blog + "/pages?maxResults=100&fetchBodies=true";
    }

    private static String comments(String blog, String post) {
        return "/v3/blogs/" + blog + "/posts/" + post + "/comments?maxResults=100&fetchBodies=true";
    }

    private static String twoBlogs() {
        return list("blog", null, id(BLOG_A), id(BLOG_B));
    }

    private static String id(String id) {
        return "{\"id\":\"" + id + "\"}";
    }

    private static String postDetail(String blog, String post, String title) {
        ObjectNode object =
                JSON.createObjectNode()
                        .put("kind", "blogger#post")
                        .put("id", post)
                        .put("title", title)
                        .put("content", "<p>detail</p>");
        object.putObject("blog").put("id", blog);
        return object.toString();
    }

    private static String comment(String blog, String post, String comment, String content) {
        ObjectNode object = JSON.createObjectNode().put("id", comment).put("content", content);
        object.putObject("blog").put("id", blog);
        object.putObject("post").put("id", post);
        return object.toString();
    }

    private static String list(String kind, String token, String... rows) {
        return "{\"kind\":\"blogger#"
                + kind
                + "List\""
                + (token == null ? "" : ",\"nextPageToken\":\"" + token + "\"")
                + ",\"items\":["
                + String.join(",", rows)
                + "]}";
    }

    private static String apiError(String reason) {
        return "{\"error\":{\"message\":\""
                + BloggerPeer.PRIVATE
                + "\",\"errors\":[{\"reason\":\""
                + reason
                + "\"}]}}";
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
        assertFalse(log.contains(BloggerPeer.PRIVATE), "log contains provider response data");
        for (String secret :
                List.of(
                        BloggerPeer.CLIENT_SECRET,
                        BloggerPeer.REFRESH,
                        BloggerPeer.TOKEN,
                        BloggerPeer.NEXT_ACCESS)) {
            assertFalse(log.contains(secret), "log contains an OAuth credential");
        }
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
                                BloggerPeer.PASSWORD,
                                "-keypass",
                                BloggerPeer.PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String log = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, process.waitFor(), log);
        KeyStore key = KeyStore.getInstance("PKCS12");
        try (InputStream stream = Files.newInputStream(target)) {
            key.load(stream, BloggerPeer.PASSWORD.toCharArray());
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
            set("clientId", BloggerPeer.CLIENT_ID);
            set("clientSecret", BloggerPeer.CLIENT_SECRET);
            set("token", BloggerPeer.REFRESH);
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
                                return "blogger." + entry.getKey() + "=" + config;
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
                    + BloggerPeer.PASSWORD;
        }

        @Override
        protected void executeExtraCommands(GenericContainer<?> container) throws IOException {
            container.withExtraHost(BloggerPeer.API, "127.0.0.1");
            container.withExtraHost(BloggerPeer.OAUTH, "127.0.0.1");
            container.withExtraHost(BloggerPeer.REDIRECT, "127.0.0.1");
            for (String file : List.of("identities.p12", "trust.jks")) {
                container.withCopyFileToContainer(
                        MountableFile.forHostPath(fixtures.resolve(file)), ROOT + "/" + file);
            }
            for (Class<?> type :
                    List.of(BloggerPeer.class, BloggerPeer.SwitchingKeyManager.class)) {
                String path = type.getName().replace('.', '/') + ".class";
                try {
                    container.withCopyFileToContainer(
                            MountableFile.forHostPath(
                                    Path.of(type.getResource("/" + path).toURI())),
                            ROOT + "/classes/" + path);
                } catch (URISyntaxException e) {
                    throw new IOException("Cannot stage the Blogger HTTPS fixture", e);
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
                throw new IOException("Cannot stage the Blogger fixture JSON library", e);
            }
        }

        Container.ExecResult runPeer() throws Exception {
            return server.execInContainer(
                    "java",
                    "-cp",
                    ROOT + "/classes:" + ROOT + "/jackson:" + ROOT + "/jackson.jar",
                    BloggerPeer.class.getName(),
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
