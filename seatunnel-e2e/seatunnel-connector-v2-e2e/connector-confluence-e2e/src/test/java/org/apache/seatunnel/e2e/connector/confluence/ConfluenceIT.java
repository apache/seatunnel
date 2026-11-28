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

package org.apache.seatunnel.e2e.connector.confluence;

import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.seatunnel.SeaTunnelContainer;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.seatunnel.e2e.connector.confluence.ConfluenceFixture.gzip;
import static org.apache.seatunnel.e2e.connector.confluence.ConfluenceFixture.json;
import static org.apache.seatunnel.e2e.connector.confluence.ConfluenceFixture.status;

@Timeout(value = 4, unit = TimeUnit.MINUTES)
@Execution(ExecutionMode.SAME_THREAD)
class ConfluenceIT extends TestSuiteBase {
    private static final String CONF = "/confluence_to_console.conf";
    private static final String PAGE = "/wiki/api/v2/pages?limit=2&body-format=storage";
    private static final String FIRST =
            "{\"id\":\"first\",\"body\":{\"storage\":{\"value\":\"<p>雪🦊</p>\"}},\"null\":null,\"empty\":\"\"}";
    private static final String TAIL =
            "{\"id\":\"tail\",\"body\":{\"atlas_doc_format\":{\"value\":\"{\\\"text\\\":\\\"é\\\"}\"}},\"items\":[]}";
    private static final String GROUP = "/wiki/rest/api/group?limit=2&start=";
    private static final String AUDIT = "/wiki/rest/api/audit?limit=2&start=";
    private static final AtomicLong JOB_IDS = new AtomicLong(System.currentTimeMillis());

    private Path directory;

    private ConfluenceFixture fixture;
    private SeaTunnelContainer engine;
    private ExecutorService commands;

    @BeforeAll
    void startZeta() throws Exception {
        // A private engine instance installs the fixture trust store before the worker starts.
        Path fixtureDirectory = Files.createTempDirectory("confluence-fixture-");
        directory = fixtureDirectory;
        fixture = new ConfluenceFixture(fixtureDirectory);
        commands = Executors.newCachedThreadPool();
        engine =
                new SeaTunnelContainer() {
                    @Override
                    protected String getJavaToolOptions() {
                        return "-Djavax.net.ssl.trustStore=/tmp/confluence-test-trust.jks "
                                + "-Djavax.net.ssl.trustStorePassword="
                                + ConfluenceFixture.PASSWORD;
                    }

                    @Override
                    protected void executeExtraCommands(GenericContainer<?> container) {
                        container.withCopyFileToContainer(
                                MountableFile.forHostPath(fixtureDirectory.resolve("trust.jks")),
                                "/tmp/confluence-test-trust.jks");
                    }
                };
        engine.startUp();
    }

    @AfterAll
    void stopZeta() throws Exception {
        try {
            if (engine != null) {
                engine.tearDown();
            }
        } finally {
            if (commands != null) {
                commands.shutdownNow();
                Assertions.assertTrue(commands.awaitTermination(30, TimeUnit.SECONDS));
            }
            if (fixture != null) {
                fixture.close();
            }
            if (directory != null) {
                Files.deleteIfExists(directory.resolve("trust.jks"));
                Files.deleteIfExists(directory.resolve("key.jks"));
                Files.deleteIfExists(directory.resolve("keytool.log"));
                Files.deleteIfExists(directory);
            }
        }
    }

    @Test
    void readsAllFiveResourcesAndPreservesExactRows() throws Exception {
        String first =
                "{\"id\":922337203685477580812345,\"title\":\"雪🦊\",\"body\":{\"storage\":{\"value\":\"<p>π</p>\"}},\"null\":null,\"empty\":\"\"}";
        ConfluenceFixture.Scenario pages =
                new ConfluenceFixture.Scenario(
                        (i, exchange) -> {
                            if (i == 0) {
                                exchange.getResponseHeaders()
                                        .add(
                                                "Link",
                                                "</wiki/api/v2/pages?cursor=a%2Bb%3D>; rel=\"next\", </wiki/api/v2/pages>; rel=\"self\"");
                                gzip(exchange, results(first));
                            } else if (i == 1) {
                                json(
                                        exchange,
                                        next(
                                                "",
                                                fixture.origin()
                                                        + "/wiki/api/v2/pages?cursor=last"));
                            } else {
                                json(exchange, results(TAIL));
                            }
                        });
        succeeds("pages", pages, PAGE, PAGE + "&cursor=a%2Bb%3D", PAGE + "&cursor=last");
        expectRows("pages, 922337203685477580812345, , " + first, "pages, tail, , " + TAIL);

        String blog = "{\"id\":\"post\",\"body\":{\"atlas_doc_format\":{\"value\":\"[]\"}}}";
        ConfluenceFixture.Scenario posts =
                new ConfluenceFixture.Scenario(
                        (i, exchange) -> {
                            if (i == 0) {
                                exchange.getResponseHeaders()
                                        .set("Link", "<?cursor=next>; rel=\"next\"");
                                json(exchange, next(blog, "/wiki/api/v2/blogposts?cursor=next"));
                            } else {
                                json(exchange, results(""));
                            }
                        });
        String blogPath = "/wiki/api/v2/blogposts?limit=2&body-format=atlas_doc_format";
        succeeds(
                "blog_posts",
                posts,
                variables("blog_posts", 2, 120000, 4, 16777216, 10000, 0, "atlas_doc_format"),
                blogPath,
                blogPath + "&cursor=next");
        expectRows("blog_posts, post, , " + blog);

        String space =
                "{\"id\":\"space\",\"description\":{\"plain\":{\"value\":\"\"}},\"icon\":null}";
        String spacePath = "/wiki/api/v2/spaces?limit=2&description-format=plain&include-icon=true";
        succeeds(
                "space",
                new ConfluenceFixture.Scenario((i, exchange) -> json(exchange, results(space))),
                spacePath);
        expectRows("space, space, , " + space);

        String group1 = "{\"id\":\"group-one\"}";
        String group2 = "{\"id\":\"group-two\"}";
        ConfluenceFixture.Scenario groups =
                new ConfluenceFixture.Scenario(
                        (i, exchange) -> {
                            if (i == 0) {
                                json(
                                        exchange,
                                        "{\"results\":["
                                                + group1
                                                + "],\"start\":0,\"limit\":1,\"size\":1}");
                            } else if (i == 1) {
                                json(exchange, next("", "/rest/api/group?start=2&limit=2"));
                            } else {
                                json(exchange, results(group2));
                            }
                        });
        succeeds("group", groups, GROUP + "0", GROUP + "1", GROUP + "2");
        expectRows("group, group-one, , " + group1, "group, group-two, , " + group2);

        String firstAudit = "{\"creationDate\":9,\"author\":null,\"summary\":\"before\"}";
        String secondAudit = "{\"creationDate\":9,\"summary\":\"after\"}";
        ConfluenceFixture.Scenario audit =
                new ConfluenceFixture.Scenario(
                        (i, exchange) ->
                                json(
                                        exchange,
                                        i == 0
                                                ? results(firstAudit + "," + secondAudit)
                                                : results("")));
        succeeds("audit", audit, AUDIT + "0", AUDIT + "2");
        expectRows("audit, , 9, " + firstAudit, "audit, , 9, " + secondAudit);
    }

    @Test
    void retriesSamePageWithBothRetryAfterForms() throws Exception {
        ConfluenceFixture.Scenario script =
                new ConfluenceFixture.Scenario(
                        (i, exchange) -> {
                            if (i == 0) {
                                exchange.getResponseHeaders().set("Retry-After", "1");
                                status(exchange, 429);
                            } else if (i == 1) {
                                String date =
                                        DateTimeFormatter.RFC_1123_DATE_TIME.format(
                                                Instant.now()
                                                        .plusSeconds(3)
                                                        .atZone(ZoneOffset.UTC));
                                exchange.getResponseHeaders().set("Retry-After", date);
                                status(exchange, 503);
                            } else {
                                json(exchange, results(FIRST));
                            }
                        });
        succeeds("pages", script, PAGE, PAGE, PAGE);
        expectRows("pages, first, , " + FIRST);
        Assertions.assertTrue(
                elapsedMillis(script, 0, 1) >= 1000, "429 retry must honor Retry-After");
        Assertions.assertTrue(
                elapsedMillis(script, 1, 2) >= 1800, "503 retry must honor HTTP-date hint");
    }

    @Test
    void retriesAnIncompleteTransportResponseWithoutEmittingPartialRows() throws Exception {
        ConfluenceFixture.Scenario script =
                new ConfluenceFixture.Scenario(
                        (i, exchange) -> {
                            if (i == 0) {
                                exchange.sendResponseHeaders(200, 1000);
                                exchange.getResponseBody()
                                        .write(
                                                results(FIRST)
                                                        .getBytes(
                                                                java.nio.charset.StandardCharsets
                                                                        .UTF_8));
                            } else {
                                json(exchange, results(TAIL));
                            }
                        });
        succeeds("pages", script, PAGE, PAGE);
        expectRows("pages, tail, , " + TAIL);
    }

    @Test
    void failsPermanentStatusesAndExhaustsOnlyTheCurrentPosition() throws Exception {
        for (int code : Arrays.asList(401, 403, 404, 410, 302)) {
            ConfluenceFixture.Scenario script =
                    new ConfluenceFixture.Scenario(
                            (i, exchange) -> {
                                if (code == 302) {
                                    exchange.getResponseHeaders()
                                            .set("Location", fixture.origin() + "/unexpected");
                                }
                                status(exchange, code);
                            });
            fails(
                    code == 403 ? "audit" : "pages",
                    script,
                    "HTTP " + code,
                    code == 403 ? AUDIT + "0" : PAGE);
        }
        for (int code : Arrays.asList(429, 503)) {
            ConfluenceFixture.Scenario script =
                    new ConfluenceFixture.Scenario((i, exchange) -> status(exchange, code));
            fails(
                    "pages",
                    script,
                    "request attempts exhausted after HTTP " + code,
                    variables("pages", 2, 10000, 2, 16777216, 10000, 0, "storage"),
                    PAGE,
                    PAGE);
        }
        ConfluenceFixture.Scenario tooLong =
                new ConfluenceFixture.Scenario(
                        (i, exchange) -> {
                            exchange.getResponseHeaders().set("Retry-After", "1800");
                            status(exchange, 429);
                        });
        fails("pages", tooLong, "page deadline cannot accommodate retry", PAGE);
    }

    @Test
    void rejectsUnsafeOrCyclicContinuations() throws Exception {
        for (String link :
                Arrays.asList(
                        "https://other.example.test/wiki/api/v2/pages?cursor=a",
                        "https://" + ConfluenceFixture.HOST + ":1/wiki/api/v2/pages?cursor=a",
                        fixture.origin().replace("https://", "http://")
                                + "/wiki/api/v2/pages?cursor=a",
                        fixture.origin().replace("https://", "https://someone@")
                                + "/wiki/api/v2/pages?cursor=a",
                        "/wiki/api/v2/%2e%2e/v2/pages?cursor=a",
                        "/wiki/api/v2/blogposts?cursor=a")) {
            fails(
                    "pages",
                    new ConfluenceFixture.Scenario(
                            (i, exchange) -> json(exchange, next(FIRST, link))),
                    "pagination",
                    PAGE);
        }
        fails(
                "pages",
                new ConfluenceFixture.Scenario(
                        (i, exchange) -> json(exchange, next(FIRST, "?cursor=a&cursor=a"))),
                "duplicate parameter",
                PAGE);
        fails(
                "pages",
                new ConfluenceFixture.Scenario(
                        (i, exchange) -> {
                            exchange.getResponseHeaders().set("Link", "<?cursor=a>; rel=next");
                            json(exchange, next(FIRST, "?cursor=b"));
                        }),
                "conflicting pagination links",
                PAGE);
        fails(
                "pages",
                new ConfluenceFixture.Scenario(
                        (i, exchange) ->
                                json(exchange, next("", "?cursor=" + (i == 1 ? "b" : "a")))),
                "cursor pagination cycle",
                PAGE,
                PAGE + "&cursor=a",
                PAGE + "&cursor=b");
        fails(
                "pages",
                new ConfluenceFixture.Scenario(
                        (i, exchange) -> json(exchange, next(FIRST, "?cursor=a"))),
                "scan exceeds max_pages",
                variables("pages", 2, 120000, 4, 16777216, 1, 0, "storage"),
                PAGE);
        fails(
                "group",
                new ConfluenceFixture.Scenario(
                        (i, exchange) -> json(exchange, next("", "?start=0"))),
                "next offset",
                GROUP + "0");
    }

    @Test
    void rejectsMalformedAndOversizedPagesBeforeAnyRows() throws Exception {
        for (String payload :
                Arrays.asList(
                        "{\"results\":[",
                        "{\"results\":[]}{}",
                        "{\"results\":null}",
                        "{\"results\":[{\"id\":\"would-be-emitted\"},42]}",
                        "{}")) {
            fails(
                    "pages",
                    new ConfluenceFixture.Scenario((i, exchange) -> json(exchange, payload)),
                    "Confluence pages:",
                    PAGE);
        }
        String deep =
                String.join("", Collections.nCopies(70, "["))
                        + "0"
                        + String.join("", Collections.nCopies(70, "]"));
        fails(
                "pages",
                new ConfluenceFixture.Scenario(
                        (i, exchange) ->
                                json(exchange, "{\"results\":[{\"nested\":" + deep + "}]}")),
                "malformed JSON",
                PAGE);
        String over =
                "{\"results\":[{\"long\":\""
                        + String.join("", Collections.nCopies(2048, "a"))
                        + "\"}]}";
        for (boolean compressed : Arrays.asList(false, true)) {
            ConfluenceFixture.Scenario script =
                    new ConfluenceFixture.Scenario(
                            (i, exchange) -> {
                                if (compressed) {
                                    gzip(exchange, over);
                                } else {
                                    json(exchange, over);
                                }
                            });
            fails(
                    "pages",
                    script,
                    "max_response_bytes",
                    variables("pages", 2, 120000, 4, 128, 10000, 0, "storage"),
                    PAGE);
        }
    }

    @Test
    void pageDeadlineAbortsTricklingBody() throws Exception {
        ConfluenceFixture.Hold hold = new ConfluenceFixture.Hold();
        ConfluenceFixture.Scenario script =
                new ConfluenceFixture.Scenario((i, exchange) -> hold.send(exchange, results(TAIL)));
        try {
            fails(
                    "pages",
                    script,
                    "page deadline exceeded",
                    variables("pages", 2, 5000, 1, 16777216, 10000, 0, "storage"),
                    PAGE);
            Assertions.assertTrue(hold.entered.await(1, TimeUnit.SECONDS));
            Assertions.assertTrue(hold.disconnected.await(5, TimeUnit.SECONDS));
        } finally {
            hold.release.countDown();
        }
    }

    @Test
    void cancelsHeldIoAndDoesNotDeliverLateRows() throws Exception {
        ConfluenceFixture.Hold hold = new ConfluenceFixture.Hold();
        ConfluenceFixture.Scenario script =
                new ConfluenceFixture.Scenario((i, exchange) -> hold.send(exchange, results(TAIL)));
        String job = nextJob();
        begin(script);
        Future<Container.ExecResult> original = launch(job, defaults("pages"));
        try {
            Assertions.assertTrue(hold.entered.await(45, TimeUnit.SECONDS));
            success(engine.cancelJob(job));
            assertJobStatus(job, "CANCELED");
            original.get(30, TimeUnit.SECONDS);
            Assertions.assertTrue(hold.disconnected.await(5, TimeUnit.SECONDS));
            Assertions.assertEquals(Collections.singletonList(PAGE), script.requests());
            expectRows();
        } finally {
            hold.release.countDown();
            cancelIfRunning(job, original);
        }
    }

    @Test
    void noRequestAfterCancellationDuringBackoff() throws Exception {
        CountDownLatch firstAttempt = new CountDownLatch(1);
        ConfluenceFixture.Scenario script =
                new ConfluenceFixture.Scenario(
                        (i, exchange) -> {
                            exchange.getResponseHeaders().set("Retry-After", "15");
                            status(exchange, 429);
                            firstAttempt.countDown();
                        });
        String job = nextJob();
        begin(script);
        Future<Container.ExecResult> original = launch(job, defaults("pages"));
        try {
            Assertions.assertTrue(firstAttempt.await(45, TimeUnit.SECONDS));
            awaitNextCheckpoint(job);
            success(engine.cancelJob(job));
            assertJobStatus(job, "CANCELED");
            original.get(30, TimeUnit.SECONDS);
            long since = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - script.times.get(0));
            Assertions.assertTrue(since < 15000, "cancel before provider delay elapsed");
            Awaitility.await()
                    .atMost(20, TimeUnit.SECONDS)
                    .until(
                            () ->
                                    TimeUnit.NANOSECONDS.toMillis(
                                                    System.nanoTime() - script.times.get(0))
                                            >= 15500);
            Assertions.assertEquals(Collections.singletonList(PAGE), script.requests());
            expectRows();
        } finally {
            cancelIfRunning(job, original);
        }
    }

    @Test
    void stopWithSavepointWhileCursorRequestIsHeldAndRestoreExactTail() throws Exception {
        restoreAcrossHeldPage(true);
    }

    @Test
    void restoreV1OffsetFromAnObservedPeriodicCheckpoint() throws Exception {
        restoreAcrossHeldPage(false);
    }

    @Test
    void stopAndRestoreInsideAPageWithoutSkippingOrRepeatingCommittedRows() throws Exception {
        restoreMidPage(false);
    }

    @Test
    void changedCommittedPrefixFailsInsteadOfSkippingRows() throws Exception {
        restoreMidPage(true);
    }

    private void restoreMidPage(boolean changePrefix) throws Exception {
        List<String> payloads = new ArrayList<>();
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            String id = "mid-" + i;
            String value = "{\"id\":\"" + id + "\"}";
            payloads.add(value);
            expected.add("pages, " + id + ", , " + value);
        }
        String payload = results(String.join(",", payloads));
        ConfluenceFixture.Scenario script =
                new ConfluenceFixture.Scenario(
                        (i, exchange) ->
                                json(
                                        exchange,
                                        i > 0 && changePrefix
                                                ? payload.replace("mid-0", "changed")
                                                : payload));
        String job = nextJob();
        begin(script);
        Future<Container.ExecResult> original =
                launch(job, variables("pages", 100, 120000, 4, 16777216, 10000, 250, "storage"));
        try {
            Awaitility.await().atMost(45, TimeUnit.SECONDS).until(() -> !rows().isEmpty());
            success(engine.savepointJob(job));
            assertJobStatus(job, "SAVEPOINT_DONE");
            success(original.get(40, TimeUnit.SECONDS));
            List<String> joined = new ArrayList<>(rows());
            Assertions.assertTrue(
                    !joined.isEmpty() && joined.size() < expected.size(),
                    "savepoint acted within the result page, with no second request");
            Assertions.assertEquals(
                    Collections.singletonList(PAGE.replace("limit=2", "limit=100")),
                    script.requests());
            markLogs();
            Container.ExecResult restored =
                    engine.restoreJob(
                            CONF,
                            job,
                            variables("pages", 100, 120000, 4, 16777216, 10000, 0, "storage"));
            if (changePrefix) {
                assertJobStatus(job, "FAILED");
                Assertions.assertTrue(
                        (logs() + restored.getStdout() + restored.getStderr())
                                .contains("restored page changed"));
                expectRows();
            } else {
                success(restored);
                assertJobStatus(job, "FINISHED");
                joined.addAll(rows());
                Assertions.assertEquals(expected, joined);
            }
            Assertions.assertEquals(2, script.requests().size());
            Assertions.assertEquals(script.requests().get(0), script.requests().get(1));
        } finally {
            cancelIfRunning(job, original);
        }
    }

    private void restoreAcrossHeldPage(boolean savepoint) throws Exception {
        String resource = savepoint ? "pages" : "group";
        String firstTarget = savepoint ? PAGE : GROUP + "0";
        String nextTarget = savepoint ? PAGE + "&cursor=next" : GROUP + "1";
        String nextLink = savepoint ? "?cursor=next" : "?start=1";
        ConfluenceFixture.Hold hold = new ConfluenceFixture.Hold();
        ConfluenceFixture.Scenario script =
                new ConfluenceFixture.Scenario(
                        (i, exchange) -> {
                            if (i == 0) {
                                json(exchange, next(FIRST, nextLink));
                            } else if (i == 1) {
                                hold.send(exchange, results(TAIL));
                            } else {
                                json(exchange, results(TAIL));
                            }
                        });
        String job = nextJob();
        begin(script);
        Future<Container.ExecResult> original = launch(job, defaults(resource));
        try {
            Assertions.assertTrue(hold.entered.await(45, TimeUnit.SECONDS));
            Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> rows().size() == 1);
            awaitNextCheckpoint(job);
            if (savepoint) {
                success(engine.savepointJob(job));
                assertJobStatus(job, "SAVEPOINT_DONE");
            } else {
                engine.stopJob(job);
                assertJobStatus(job, "CANCELED");
            }
            original.get(40, TimeUnit.SECONDS);
            Assertions.assertEquals(
                    1, hold.release.getCount(), "terminal state before remote release");
            expectRows(resource + ", first, , " + FIRST);
            Assertions.assertEquals(Arrays.asList(firstTarget, nextTarget), script.requests());
            markLogs();
            if (savepoint) {
                success(engine.restoreJob(CONF, job, defaults(resource)));
            } else {
                String restore = nextJob();
                success(
                        engine.restoreJobWithCheckpoint(
                                CONF, job, restore, Arrays.asList(defaults(resource))));
                assertJobStatus(restore, "FINISHED");
            }
            expectRows(resource + ", tail, , " + TAIL);
            Assertions.assertEquals(
                    Arrays.asList(firstTarget, nextTarget, nextTarget), script.requests());
        } finally {
            hold.release.countDown();
            cancelIfRunning(job, original);
        }
    }

    private int logOffset;

    private void begin(ConfluenceFixture.Scenario script) {
        fixture.serve(script);
        markLogs();
    }

    private void markLogs() {
        logOffset = engine.getServerLogs().length();
    }

    private String logs() {
        String logs = engine.getServerLogs();
        Assertions.assertTrue(
                logs.length() >= logOffset, "server log should not rotate during one case");
        return logs.substring(logOffset);
    }

    private List<String> rows() {
        List<String> rows = new ArrayList<>();
        for (String line : logs().split("\\R")) {
            int marker = line.indexOf("SeaTunnelRow#kind=INSERT : ");
            if (marker >= 0) {
                rows.add(line.substring(marker + "SeaTunnelRow#kind=INSERT : ".length()));
            }
        }
        return rows;
    }

    private void expectRows(String... expected) {
        Assertions.assertEquals(Arrays.asList(expected), rows());
    }

    private void succeeds(String resource, ConfluenceFixture.Scenario script, String... requests)
            throws Exception {
        succeeds(resource, script, defaults(resource), requests);
    }

    private void succeeds(
            String resource,
            ConfluenceFixture.Scenario script,
            String[] variables,
            String... requests)
            throws Exception {
        String job = nextJob();
        begin(script);
        success(engine.executeJob(CONF, job, variables));
        assertJobStatus(job, "FINISHED");
        Assertions.assertEquals(Arrays.asList(requests), script.requests(), resource);
        assertCredentialsRedacted(logs());
    }

    private void fails(
            String resource, ConfluenceFixture.Scenario script, String message, String... requests)
            throws Exception {
        fails(resource, script, message, defaults(resource), requests);
    }

    private void fails(
            String resource,
            ConfluenceFixture.Scenario script,
            String message,
            String[] variables,
            String... requests)
            throws Exception {
        String job = nextJob();
        begin(script);
        Container.ExecResult result = engine.executeJob(CONF, job, variables);
        assertJobStatus(job, "FAILED");
        String diagnostics = logs() + result.getStdout() + result.getStderr();
        Assertions.assertTrue(
                diagnostics.contains(message), "Expected failure for " + resource + ": " + message);
        Assertions.assertEquals(Arrays.asList(requests), script.requests(), resource);
        expectRows();
        assertCredentialsRedacted(diagnostics);
    }

    private Future<Container.ExecResult> launch(String job, String[] variables) {
        return commands.submit(() -> engine.executeJob(CONF, job, variables));
    }

    private void cancelIfRunning(String job, Future<?> operation) throws Exception {
        if (!operation.isDone()) {
            try {
                engine.cancelJob(job);
            } finally {
                operation.cancel(true);
            }
        }
    }

    private void assertJobStatus(String job, String expected) {
        Awaitility.await()
                .atMost(20, TimeUnit.SECONDS)
                .untilAsserted(() -> Assertions.assertEquals(expected, engine.getJobStatus(job)));
    }

    private void awaitNextCheckpoint(String job) {
        long previous = engine.getCompletedCheckpointCount(job);
        Awaitility.await()
                .atMost(15, TimeUnit.SECONDS)
                .until(() -> engine.getCompletedCheckpointCount(job) > previous);
    }

    private String[] defaults(String resource) {
        return variables(resource, 2, 120000, 4, 16777216, 10000, 0, "storage");
    }

    private String[] variables(
            String resource,
            int pageSize,
            int timeout,
            int attempts,
            int maxBytes,
            int maxPages,
            int sinkDelay,
            String format) {
        return new String[] {
            "confluence.url=" + fixture.origin(),
            "confluence.resource=" + resource,
            "confluence.size=" + pageSize,
            "confluence.timeout=" + timeout,
            "confluence.attempts=" + attempts,
            "confluence.bytes=" + maxBytes,
            "confluence.pages=" + maxPages,
            "confluence.sink.delay=" + sinkDelay,
            "confluence.body.format=" + format
        };
    }

    private static String nextJob() {
        return Long.toString(JOB_IDS.incrementAndGet());
    }

    private static void success(Container.ExecResult result) {
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
    }

    private static long elapsedMillis(ConfluenceFixture.Scenario script, int from, int to) {
        return TimeUnit.NANOSECONDS.toMillis(script.times.get(to) - script.times.get(from));
    }

    private static String results(String records) {
        return "{\"results\":[" + records + "]}";
    }

    private static String next(String records, String link) {
        return "{\"results\":[" + records + "],\"_links\":{\"next\":\"" + link + "\"}}";
    }

    private static void assertCredentialsRedacted(String diagnostics) {
        Assertions.assertFalse(diagnostics.contains(ConfluenceFixture.TOKEN), "Token leaked");
        Assertions.assertFalse(
                diagnostics.contains(ConfluenceFixture.AUTHORIZATION), "Basic header leaked");
        Assertions.assertFalse(diagnostics.contains(ConfluenceFixture.USERNAME), "Username leaked");
    }
}
