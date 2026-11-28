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
package org.apache.seatunnel.e2e.connector.ashby;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.ashby.AshbyCheckpointProbe;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.seatunnel.SeaTunnelContainer;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the installed connector through Zeta against a separate Java HTTPS peer.
 *
 * <p>Every successful test checks both the provider requests and the exact rows in committed
 * LocalFile JSON. Asynchronous tests stop or savepoint the actual Zeta job before restoring it.
 */
class AshbyIT extends TestSuiteBase {
    private static final ObjectMapper JSON =
            new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    private static final String USERS = "user.list";
    private static final String APPLICATIONS = "application.list";
    private static final String HISTORY = "application.listHistory";
    private static final String CRITERIA = "application.listCriteriaEvaluations";
    private static final String USER_INPUT = "{\"limit\":2,\"includeDeactivated\":true}";
    private static final String TLS = "https://ashby-peer:8444";
    private static final String SECRET = "ashby-fixture-auth";
    private static final String PRIVATE_DATA = "candidate-pii-private@invalid.example";
    private static final String DISCONNECT = "disconnect\t/";
    private static final String START = "2024-01-02T03:04:05.123Z";
    private static final String OLD = "2023-12-31T00:00:00Z";

    private final Engine engine = new Engine();
    private final HttpClient admin =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final ExecutorService jobs = Executors.newSingleThreadExecutor();
    private GenericContainer<?> peer;

    private Path certificates;

    /**
     * Starts one isolated provider and then the default JDK 11 Zeta container.
     *
     * <p>Only the test certificate is shared with Zeta, which resolves the peer over the test
     * network.
     */
    @BeforeAll
    void startProviderAndEngine(@TempDir Path certificateDirectory) throws Exception {
        certificates = certificateDirectory;
        String host = DockerClientFactory.instance().dockerHostIpAddress();
        String runnerSan =
                host.matches("[0-9.]+") || host.contains(":") ? "ip:" + host : "dns:" + host;
        String generate =
                "keytool -genkeypair -alias api -keyalg RSA -keysize 2048 "
                        + "-storetype PKCS12 -storepass ashby-fixture -validity 30 "
                        + "-dname CN=ashby-peer -ext SAN=dns:ashby-peer,"
                        + runnerSan
                        + " -keystore ";
        String export = "keytool -exportcert -rfc -alias api -storepass ashby-fixture -keystore ";
        String startPeer =
                generate
                        + "/tmp/api.p12 && "
                        + export
                        + "/tmp/api.p12 -file /tmp/api.pem && "
                        + generate
                        + "/tmp/unrelated.p12 && "
                        + export
                        + "/tmp/unrelated.p12 -file /tmp/unrelated.pem && "
                        + "exec java --add-modules jdk.httpserver -cp /tmp "
                        + AshbyPeer.class.getName()
                        + " /tmp/api.p12";
        String peerClass = "/" + AshbyPeer.class.getName().replace('.', '/');
        peer =
                new GenericContainer<>(DockerImageName.parse("eclipse-temurin:11-jdk"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("ashby-peer", "ashby-bad-host")
                        .withExposedPorts(8080, 8444)
                        .withCopyToContainer(
                                Transferable.of(classBytes(peerClass + ".class")),
                                "/tmp" + peerClass + ".class")
                        .withCopyToContainer(
                                Transferable.of(classBytes(peerClass + "$Reply.class")),
                                "/tmp" + peerClass + "$Reply.class")
                        .withCommand("sh", "-c", startPeer)
                        .waitingFor(
                                Wait.forHttp("/trace")
                                        .forPort(8080)
                                        .withStartupTimeout(Duration.ofMinutes(2)));
        peer.start();
        engine.startUp();
        installCertificate("/tmp/api.pem");
        installCertificate("/tmp/unrelated.pem");
    }

    /**
     * Stops outstanding client calls and both owned containers after all scenarios.
     *
     * <p>Provider cleanup runs after a failure to stop Zeta as well as on successful test
     * completion.
     */
    @AfterAll
    void stopProviderAndEngine() throws Exception {
        jobs.shutdownNow();
        try {
            engine.tearDown();
        } finally {
            if (peer != null) {
                peer.stop();
            }
        }
    }

    @Test
    void readsAllDirectResourcesWithExactProviderArgumentsAndLosslessOutput() throws Exception {
        String[][] paged = {
            {"applications", APPLICATIONS},
            {"application_feedback", "applicationFeedback.list"},
            {"candidate_tags", "candidateTag.list", "includeArchived"},
            {"candidates", "candidate.list"},
            {"custom_fields", "customField.list", "includeArchived"},
            {"departments", "department.list", "includeArchived"},
            {"feedback_form_definitions", "feedbackFormDefinition.list", "includeArchived"},
            {"interview_schedules", "interviewSchedule.list"},
            {"interviews", "interview.list", "includeArchived", "includeNonSharedInterviews"},
            {"jobs", "job.list"},
            {"locations", "location.list", "includeArchived"},
            {"offers", "offer.list"},
            {"users", USERS, "includeDeactivated"},
            {"interview_plans", "interviewPlan.list", "includeArchived"},
            {
                "interviewer_pools",
                "interviewerPool.list",
                "includeArchivedPools",
                "includeArchivedTrainingStages"
            },
            {"survey_form_definitions", "surveyFormDefinition.list"},
            {"source_tracking_links", "sourceTrackingLink.list", "includeDisabled"},
            {"openings", "opening.list"},
            {"job_templates", "jobTemplate.list"},
            {"approvals", "approval.list"},
            {"projects", "project.list"}
        };
        String[][] single = {
            {"archive_reasons", "archiveReason.list", "includeArchived"},
            {"sources", "source.list", "includeArchived"},
            {"job_postings", "jobPosting.list"},
            {"job_boards", "jobBoard.list"},
            {"interview_stage_groups", "interviewStageGroup.list"}
        };
        for (String[] resource : paged) {
            assertDirect(resource, true);
        }
        for (String[] resource : single) {
            assertDirect(resource, false);
        }
    }

    @Test
    void preservesApplicationWindowAndAdvancesAnEmptyPageOnProviderCursor() throws Exception {
        String old = "{\"id\":\"outside\",\"updatedAt\":\"" + OLD + "\"}";
        String bound = "{\"id\":\"inclusive\",\"updatedAt\":\"" + START + "\"}";
        String noTimestamp = "{\"id\":\"unknown\",\"deletedAt\":null}";
        program(
                response(APPLICATIONS, page(true, "empty-follow", "unused", "")),
                response(APPLICATIONS, page(true, "second", null, old + "," + bound)),
                response(APPLICATIONS, page(false, null, "complete", noTimestamp)));
        try (Job job = job("applications", options().put("start_date", START), false, false)) {
            assertSuccess(engine.executeJob(job.name));
            assertRows(
                    job,
                    row("applications", "inclusive", null, bound),
                    row("applications", "unknown", null, noTimestamp));
        }
        ObjectNode expected = options().put("limit", 2).put("createdAfter", 1704164645123L);
        assertRequests(
                request(APPLICATIONS, expected.toString()),
                request(APPLICATIONS, expected.deepCopy().put("cursor", "empty-follow").toString()),
                request(APPLICATIONS, expected.deepCopy().put("cursor", "second").toString()));
    }

    @Test
    void appliesExactCreatedAfterOnlyToDocumentedDirectMethods() throws Exception {
        for (String[] resource :
                new String[][] {
                    {"application_feedback", "applicationFeedback.list"},
                    {"interview_schedules", "interviewSchedule.list"}
                }) {
            String entry = "{\"id\":\"legacy-without-updatedAt\"}";
            program(response(resource[1], page(false, null, "done", entry)));
            try (Job job = job(resource[0], options().put("start_date", START), false, false)) {
                assertSuccess(engine.executeJob(job.name));
                assertRows(job, row(resource[0], "legacy-without-updatedAt", null, entry));
            }
            assertRequests(request(resource[1], "{\"limit\":2,\"createdAfter\":1704164645123}"));
        }
    }

    @Test
    void discoversAllCriteriaParentsAndRestartsChildCursorForEachApplication() throws Exception {
        String parentA = "{\"id\":\"application-a\",\"updatedAt\":\"" + OLD + "\"}";
        String parentB = "{\"id\":\"application-b\",\"updatedAt\":\"" + OLD + "\"}";
        String entry = "{\"id\":\"criterion\",\"score\":null}";
        program(
                response(APPLICATIONS, page(true, "parent-more", null, parentA)),
                response(CRITERIA, page(true, "child-more", "ignored", "")),
                response(CRITERIA, page(false, null, "ignored", entry)),
                response(APPLICATIONS, page(false, null, null, parentB)),
                response(CRITERIA, page(false, null, null, entry)));
        try (Job job = job("application_criteria_evaluations", options(), false, false)) {
            assertSuccess(engine.executeJob(job.name));
            assertRows(
                    job,
                    row(
                            "application_criteria_evaluations",
                            "application-a/criterion",
                            "application-a",
                            "{\"id\":\"criterion\",\"score\":null,\"application_id\":\"application-a\"}"),
                    row(
                            "application_criteria_evaluations",
                            "application-b/criterion",
                            "application-b",
                            "{\"id\":\"criterion\",\"score\":null,\"application_id\":\"application-b\"}"));
        }
        assertRequests(
                request(APPLICATIONS, "{\"limit\":2}"),
                request(CRITERIA, "{\"limit\":2,\"applicationId\":\"application-a\"}"),
                request(
                        CRITERIA,
                        "{\"limit\":2,\"applicationId\":\"application-a\",\"cursor\":\"child-more\"}"),
                request(APPLICATIONS, "{\"limit\":2,\"cursor\":\"parent-more\"}"),
                request(CRITERIA, "{\"limit\":2,\"applicationId\":\"application-b\"}"));
    }

    @Test
    void discoversBothInterviewPlansWithExactUnpaginatedStageRequest() throws Exception {
        String rpc = "interviewStage.list";
        program(
                response(
                        "interviewPlan.list",
                        page(
                                true,
                                "another-plan",
                                null,
                                "{\"id\":\"plan-a\",\"isArchived\":true}")),
                response(rpc, single("{\"id\":\"stage-a\",\"interviewPlanId\":\"plan-a\"}")),
                response("interviewPlan.list", page(false, null, null, "{\"id\":\"plan-b\"}")),
                response(rpc, single("{\"id\":\"stage-b\"}")));
        try (Job job = job("interview_stages", options(), false, false)) {
            assertSuccess(engine.executeJob(job.name));
            assertRows(
                    job,
                    row(
                            "interview_stages",
                            "stage-a",
                            "plan-a",
                            "{\"id\":\"stage-a\",\"interviewPlanId\":\"plan-a\",\"interview_plan_is_archived\":true}"),
                    row(
                            "interview_stages",
                            "stage-b",
                            "plan-b",
                            "{\"id\":\"stage-b\",\"interview_plan_is_archived\":null}"));
        }
        assertRequests(
                request("interviewPlan.list", "{\"limit\":2,\"includeArchived\":true}"),
                request(rpc, "{\"interviewPlanId\":\"plan-a\"}"),
                request(
                        "interviewPlan.list",
                        "{\"limit\":2,\"includeArchived\":true,\"cursor\":\"another-plan\"}"),
                request(rpc, "{\"interviewPlanId\":\"plan-b\"}"));
    }

    @Test
    void failsTheRunWhenAChildBelongsToADifferentParent() throws Exception {
        program(
                response(APPLICATIONS, page(false, null, null, "{\"id\":\"app-a\"}")),
                response(
                        CRITERIA,
                        page(
                                false,
                                null,
                                null,
                                "{\"id\":\"wrong-result\",\"applicationId\":\"app-b\"}")));
        try (Job job = job("application_criteria_evaluations", options(), false, false)) {
            assertFailure(
                    engine.executeJob(job.name), "provider child belongs to a different parent");
        }
        assertRequests(
                request(APPLICATIONS, "{\"limit\":2}"),
                request(CRITERIA, "{\"limit\":2,\"applicationId\":\"app-a\"}"));
    }

    @Test
    void commitsOnlyTheTerminalDeltaTokenAndPreservesReportedDeletionFields() throws Exception {
        String first = "{\"id\":\"user-a\",\"isDeactivated\":true}";
        String second = "{\"id\":\"user-b\",\"deletedAt\":\"" + START + "\"}";
        program(
                response(USERS, page(true, "delta-cursor", "must-not-promote", first)),
                response(USERS, page(false, null, "next-pass", second)),
                slow(USERS));
        try (Job job =
                job(
                        "users",
                        syncOptions("initial-pass").put("poll_interval_ms", 1200),
                        true,
                        false)) {
            String id = jobId();
            Future<Container.ExecResult> run = start(job, id);
            try {
                await(() -> requests().size() >= 3, "the next delta pass");
                awaitRows(
                        job,
                        row("users", "user-a", null, first),
                        row("users", "user-b", null, second));
                assertRequests(
                        request(USERS, userInput().put("syncToken", "initial-pass").toString()),
                        request(
                                USERS,
                                userInput()
                                        .put("syncToken", "initial-pass")
                                        .put("cursor", "delta-cursor")
                                        .toString()),
                        request(USERS, userInput().put("syncToken", "next-pass").toString()));
            } finally {
                stop(id, run);
            }
            await(
                    () -> trace().contains(DISCONNECT + USERS),
                    "downstream cancellation closes the response");
        }
    }

    @Test
    void keepsTheHistoryParentTokenPendingAcrossSavepointUntilEveryChildPageIsEmitted()
            throws Exception {
        String stale = "{\"id\":\"too-old\",\"updatedAt\":\"" + OLD + "\"}";
        String matching =
                "{\"id\":\"old-created-app\",\"status\":\"Hired\","
                        + "\"createdAt\":\""
                        + OLD
                        + "\",\"updatedAt\":\""
                        + START
                        + "\"}";
        String unknown = "{\"id\":\"undated-app\"}";
        String first = "{\"id\":\"event-1\"}";
        String second = "{\"id\":\"event-2\",\"sequence\":2}";
        String third = "{\"id\":\"event-3\"}";
        program(
                response(APPLICATIONS, page(true, "outer-page", "discard", stale)),
                response(
                        APPLICATIONS, page(false, null, "promote-later", matching + "," + unknown)),
                response(HISTORY, page(true, "inner-page", "invalid-child-token", first)),
                slow(HISTORY));
        ObjectNode settings =
                syncOptions("settled")
                        .put("start_date", START)
                        .put("poll_interval_ms", 1500)
                        .put("request_timeout_ms", 60000);
        ObjectNode firstPayload = historyPayload(first, "old-created-app", "Hired", OLD, START);
        ObjectNode secondPayload = historyPayload(second, "old-created-app", "Hired", OLD, START);
        ObjectNode thirdPayload = historyPayload(third, "undated-app", null, null, null);
        ObjectNode firstRow =
                row("application_history", "event-1", "old-created-app", firstPayload.toString());
        try (Job job = job("application_history", settings, true, false)) {
            String id = jobId();
            Future<Container.ExecResult> original = start(job, id);
            try {
                await(() -> requests().size() >= 4, "the held child continuation");
                awaitRows(job, firstRow);
                assertSuccess(engine.savepointJob(id));
                original.get(60, TimeUnit.SECONDS);
                await(
                        () -> trace().contains(DISCONNECT + HISTORY),
                        "release of original child connection");
                assertRequests(
                        request(APPLICATIONS, "{\"limit\":2,\"syncToken\":\"settled\"}"),
                        request(
                                APPLICATIONS,
                                "{\"limit\":2,\"syncToken\":\"settled\",\"cursor\":\"outer-page\"}"),
                        request(HISTORY, "{\"limit\":2,\"applicationId\":\"old-created-app\"}"),
                        request(
                                HISTORY,
                                "{\"limit\":2,\"applicationId\":\"old-created-app\",\"cursor\":\"inner-page\"}"));
            } finally {
                if (!original.isDone()) {
                    stop(id, original);
                }
            }

            append(
                    response(HISTORY, page(false, null, null, second)),
                    response(HISTORY, page(false, null, null, third)),
                    slow(APPLICATIONS));
            Future<Container.ExecResult> resumed = restore(job, id);
            try {
                await(() -> requests().size() >= 7, "the fully committed parent token");
                awaitRows(
                        job,
                        firstRow,
                        row(
                                "application_history",
                                "event-2",
                                "old-created-app",
                                secondPayload.toString()),
                        row(
                                "application_history",
                                "event-3",
                                "undated-app",
                                thirdPayload.toString()));
                List<WireRequest> calls = requests();
                assertEquals(7, calls.size());
                assertEquals(
                        request(
                                HISTORY,
                                "{\"limit\":2,\"applicationId\":\"old-created-app\",\"cursor\":\"inner-page\"}"),
                        calls.get(4).signature());
                assertEquals(
                        request(HISTORY, "{\"limit\":2,\"applicationId\":\"undated-app\"}"),
                        calls.get(5).signature());
                assertEquals(
                        request(APPLICATIONS, "{\"limit\":2,\"syncToken\":\"promote-later\"}"),
                        calls.get(6).signature());
            } finally {
                stop(id, resumed);
            }
        }
    }

    @Test
    void savesAnInFlightDirectPageAndRejectsChangedResumeSettings() throws Exception {
        String first = "{\"id\":\"first\"}";
        String last = "{\"id\":\"last\"}";
        program(response(USERS, page(true, "saved-cursor", null, first)), slow(USERS));
        try (Job job = job("users", options().put("request_timeout_ms", 60000), true, false);
                Job changed =
                        job(
                                "users",
                                options().put("request_timeout_ms", 60000).put("page_size", 3),
                                true,
                                false)) {
            String id = jobId();
            Future<Container.ExecResult> run = start(job, id);
            try {
                await(() -> requests().size() >= 2, "the pending continuation");
                awaitRows(job, row("users", "first", null, first));
                assertSuccess(engine.savepointJob(id));
                run.get(60, TimeUnit.SECONDS);
            } finally {
                if (!run.isDone()) {
                    stop(id, run);
                }
            }
            await(() -> trace().contains(DISCONNECT + USERS), "the saved request to close");

            append(response(USERS, page(false, null, "complete", last)));
            assertSuccess(engine.restoreJob(job.name, id));
            assertRows(job, row("users", "first", null, first), row("users", "last", null, last));
            assertRequests(
                    request(USERS, USER_INPUT),
                    request(USERS, userInput().put("cursor", "saved-cursor").toString()),
                    request(USERS, userInput().put("cursor", "saved-cursor").toString()));

            // Exercise a distinct persisted savepoint so a failed restore cannot consume the first.
            program(slow(USERS));
            String changingId = jobId();
            Future<Container.ExecResult> changingRun = start(job, changingId);
            try {
                await(() -> requests().size() == 1, "the first request in the new job");
                assertSuccess(engine.savepointJob(changingId));
                changingRun.get(60, TimeUnit.SECONDS);
                assertFailure(
                        engine.restoreJob(changed.name, changingId),
                        "resource or read settings changed");
                assertRequests(request(USERS, USER_INPUT));
            } finally {
                if (!changingRun.isDone()) {
                    stop(changingId, changingRun);
                }
            }
        }
    }

    @Test
    void restoresACompletedSplitWithoutReadingAgainWhileAnotherPipelineIsRunning()
            throws Exception {
        String terminal = "{\"id\":\"once\"}";
        program(response(USERS, page(false, null, "end", terminal)));
        try (Job job = job("users", options(), true, true)) {
            String id = jobId();
            Future<Container.ExecResult> original = start(job, id);
            try {
                awaitRows(job, row("users", "once", null, terminal));
                assertSuccess(engine.savepointJob(id));
                original.get(60, TimeUnit.SECONDS);
            } finally {
                if (!original.isDone()) {
                    stop(id, original);
                }
            }
            Future<Container.ExecResult> resumed = restore(job, id);
            try {
                await(
                        () -> "RUNNING".equals(engine.getJobStatus(id)),
                        "the restored anchor pipeline");
                long baseline = engine.completedCheckpoints(id);
                await(
                        () -> engine.completedCheckpoints(id) > baseline,
                        "a checkpoint in the live restored pipeline");
                assertRequests(request(USERS, USER_INPUT));
                assertRows(job, row("users", "once", null, terminal));
            } finally {
                stop(id, resumed);
            }
        }
    }

    @Test
    void rejectsMalformedAndContradictoryCursorEnvelopes() throws Exception {
        assertPageFailure("users", USERS, single(""), "moreDataAvailable");
        assertPageFailure("users", USERS, page(true, null, null, ""), "usable nextCursor");
        assertPageFailure(
                "users", USERS, page(false, "fake-next", "final", ""), "contradictory nextCursor");
        assertPageFailure("users", USERS, page(false, null, "", ""), "usable syncToken");
        assertPageFailure(
                "archive_reasons",
                "archiveReason.list",
                page(true, "unused", null, ""),
                "single-response method");
    }

    @Test
    void failsCursorLoopsAndBoundedRequestExhaustionWithoutReplayingThePass() throws Exception {
        program(
                response(USERS, page(true, "repeat", null, "")),
                response(USERS, page(true, "repeat", null, "")));
        try (Job job = job("users", options(), false, false)) {
            assertFailure(engine.executeJob(job.name), "provider repeated a page cursor");
        }
        assertRequests(
                request(USERS, USER_INPUT),
                request(USERS, userInput().put("cursor", "repeat").toString()));

        program(response(USERS, page(true, "not-requested", null, "")));
        try (Job job = job("users", options().put("max_requests_per_pass", 1), false, false)) {
            assertFailure(engine.executeJob(job.name), "max_requests_per_pass exhausted");
        }
        assertRequests(request(USERS, USER_INPUT));
    }

    @Test
    void surfacesExplicitIncidentalAndProviderPermissionFailuresWithoutLoggingPrivateValues()
            throws Exception {
        for (int status : new int[] {401, 403}) {
            program(
                    reply(
                            USERS,
                            status,
                            "",
                            "plain",
                            "{\"success\":false,\"errors\":[\"" + PRIVATE_DATA + "\"]}"));
            try (Job job = job("users", options(), false, false)) {
                assertFailure(
                        engine.executeJob(job.name),
                        "HTTP " + status + " requires an active key with organizationRead");
            }
            assertRequests(request(USERS, USER_INPUT));
        }

        assertPageFailure(
                "users",
                USERS,
                "{\"success\":false,\"errors\":[\"missing_endpoint_permission\",\""
                        + PRIVATE_DATA
                        + "\"]}",
                "missing endpoint permission organizationRead");
        assertPageFailure(
                "users",
                USERS,
                "{\"success\":false,\"errors\":[\"" + PRIVATE_DATA + "\"]}",
                "success=false");
        assertPageFailure(
                "users",
                USERS,
                "{\"success\":true,\"results\":[],\"errors\":[\"" + PRIVATE_DATA + "\"]}",
                "contradictory success and error");
    }

    @Test
    void failsAnExpiredOrOversizedDeltaWithoutSilentlyReenteringFullSync() throws Exception {
        for (String code :
                List.of(
                        "sync_token_expired",
                        "sync_token_invalid",
                        "next_cursor_expired",
                        "cursor_invalid",
                        "invalid_next_cursor",
                        "incremental_sync_too_large")) {
            program(
                    response(
                            USERS,
                            "{\"success\":false,\"errorInfo\":{\"code\":\""
                                    + code
                                    + "\",\"detail\":\""
                                    + PRIVATE_DATA
                                    + "\"}}"));
            try (Job job = job("users", syncOptions("old-private-sync"), false, false)) {
                String log = assertFailure(engine.executeJob(job.name), code);
                assertFalse(log.contains("old-private-sync"));
            }
            assertRequests(
                    request(USERS, userInput().put("syncToken", "old-private-sync").toString()));
        }
    }

    @Test
    void respectsRetryAfterAndRetriesAReadWithoutChangingItsInputs() throws Exception {
        String record = "{\"id\":\"retried\"}";
        program(
                reply(USERS, 429, "1", "plain", "{\"error\":\"" + PRIVATE_DATA + "\"}"),
                reply(USERS, 503, "", "plain", "{}"),
                response(USERS, page(false, null, "complete", record)));
        try (Job job = job("users", options().put("max_attempts", 3), false, false)) {
            assertSuccess(engine.executeJob(job.name));
            assertRows(job, row("users", "retried", null, record));
        }
        assertRequests(
                request(USERS, USER_INPUT), request(USERS, USER_INPUT), request(USERS, USER_INPUT));
        List<WireRequest> requests = requests();
        assertTrue(
                TimeUnit.NANOSECONDS.toMillis(requests.get(1).arrived - requests.get(0).arrived)
                        >= 950);
        assertTrue(
                TimeUnit.NANOSECONDS.toMillis(requests.get(2).arrived - requests.get(1).arrived)
                        >= 100);
    }

    @Test
    void boundsDecodedGzipAndCannotFollowARedirectWithItsCredentials() throws Exception {
        String entry = "{\"id\":\"gz\",\"name\":\"雪é\"}";
        program(reply(USERS, 200, "", "gzip", page(false, null, "done", entry)));
        try (Job job = job("users", options().put("max_response_bytes", 1024), false, false)) {
            assertSuccess(engine.executeJob(job.name));
            assertRows(job, row("users", "gz", null, entry));
        }
        assertRequests(request(USERS, USER_INPUT));

        String big = "{\"id\":\"large\",\"content\":\"" + "z".repeat(4096) + "\"}";
        program(reply(USERS, 200, "", "gzip", page(false, null, "done", big)));
        try (Job job = job("users", options().put("max_response_bytes", 1024), false, false)) {
            assertFailure(engine.executeJob(job.name), "response exceeds max_response_bytes");
        }
        assertRequests(request(USERS, USER_INPUT));

        program(reply(USERS, 307, "", "redirect", "{}"));
        try (Job job = job("users", options(), false, false)) {
            assertFailure(engine.executeJob(job.name), "unexpected HTTP 307");
        }
        assertRequests(request(USERS, USER_INPUT));
    }

    @Test
    void refusesIndependentlyAnUntrustedCertificateAndAnIncorrectTlsHostname() throws Exception {
        program();
        try (Job job =
                job(
                        "users",
                        options().put("trust_certificate", "/tmp/unrelated.pem"),
                        false,
                        false)) {
            assertFailure(
                    engine.executeJob(job.name), "HTTPS certificate or hostname validation failed");
        }
        try (Job job =
                job(
                        "users",
                        options().put("endpoint", "https://ashby-bad-host:8444"),
                        false,
                        false)) {
            assertFailure(
                    engine.executeJob(job.name), "HTTPS certificate or hostname validation failed");
        }
        assertRequests();
    }

    @Test
    void closesAContinuouslyTricklingConnectionAtDeadlineAndDoesNotOverrunRetryBudget()
            throws Exception {
        program(slow(USERS));
        try (Job job = job("users", options().put("request_timeout_ms", 1000), false, false)) {
            assertFailure(engine.executeJob(job.name), "request deadline");
        }
        await(() -> trace().contains(DISCONNECT + USERS), "peer sees deadline disconnect");
        assertRequests(request(USERS, USER_INPUT));

        program(reply(USERS, 429, "60", "plain", "{}"));
        try (Job job = job("users", options().put("request_timeout_ms", 1500), false, false)) {
            assertFailure(engine.executeJob(job.name), "wait exceeds the request deadline");
        }
        assertRequests(request(USERS, USER_INPUT));
    }

    @Test
    void checkpointsTheExactPartialResponseAndRefusesAChangedSuffix() throws Exception {
        String route = "archiveReason.list";
        List<String> ids =
                IntStream.range(0, 300)
                        .mapToObj(index -> "reason-" + index)
                        .collect(Collectors.toList());
        String rows =
                ids.stream().map(id -> "{\"id\":\"" + id + "\"}").collect(Collectors.joining(","));
        program(
                response(route, single(rows)),
                response(route, single(rows)),
                response(route, single(rows.replace("reason-299", "different-reason"))));
        AshbyCheckpointProbe probe = checkpointProbe();
        List<String> prefix = new ArrayList<>();
        byte[] state = probe.saveFirstChunk(prefix);
        assertEquals(ids.subList(0, 256), prefix);
        List<String> suffix = new ArrayList<>();
        probe.resume(state, suffix);
        assertEquals(ids.subList(256, 300), suffix);

        List<String> rejected = new ArrayList<>();
        Exception failure = assertThrows(Exception.class, () -> probe.resume(state, rejected));
        assertTrue(failure.getMessage().contains("provider page changed after checkpoint"));
        assertTrue(rejected.isEmpty());
        assertRequests(
                request(route, "{\"includeArchived\":true}"),
                request(route, "{\"includeArchived\":true}"),
                request(route, "{\"includeArchived\":true}"));
    }

    @Test
    void cancellationBeforeSplitAssignmentStaysEffective() throws Exception {
        program();
        checkpointProbe().cancelBeforeAssignment();
        assertRequests();
    }

    private AshbyCheckpointProbe checkpointProbe() throws Exception {
        URI origin =
                new URI("https", null, peer.getHost(), peer.getMappedPort(8444), null, null, null);
        return new AshbyCheckpointProbe(
                origin.toString(), certificates.resolve("api.pem").toString(), SECRET);
    }

    private void assertDirect(String[] resource, boolean paged) throws Exception {
        String full =
                "{\"id\":\"bedc9169-e8b5-4052-a8f4-82530fdbbead\",\"name\":\"候補者 🦦\","
                        + "\"nullable\":null,\"amount\":12345678901234567890.000000000000000001,"
                        + "\"large\":9223372036854775808123,\"nested\":{\"flags\":[true,null,\"\"]}}";
        String another = "{\"id\":\"last\",\"isArchived\":true}";
        ObjectNode body = options();
        if (paged) {
            body.put("limit", 2);
        }
        for (int i = 2; i < resource.length; i++) {
            body.put(resource[i], true);
        }

        ObjectNode expected = row(resource[0], "bedc9169-e8b5-4052-a8f4-82530fdbbead", null, full);
        if (paged) {
            program(
                    response(resource[1], page(true, "provider-next", "do-not-send", full)),
                    response(resource[1], page(false, null, "terminal-sync", another)));
            try (Job job = job(resource[0], syncOptions("known-sync"), false, false)) {
                assertSuccess(engine.executeJob(job.name));
                assertRows(job, expected, row(resource[0], "last", null, another));
            }
            body.put("syncToken", "known-sync");
            assertRequests(
                    request(resource[1], body.toString()),
                    request(
                            resource[1],
                            body.deepCopy().put("cursor", "provider-next").toString()));
        } else {
            program(response(resource[1], single(full)));
            try (Job job = job(resource[0], options(), false, false)) {
                assertSuccess(engine.executeJob(job.name));
                assertRows(job, expected);
            }
            assertRequests(request(resource[1], body.toString()));
        }
    }

    private void assertPageFailure(String resource, String rpc, String body, String expected)
            throws Exception {
        program(response(rpc, body));
        try (Job job = job(resource, options(), false, false)) {
            assertFailure(engine.executeJob(job.name), expected);
        }
        String requestBody =
                resource.equals("archive_reasons") ? "{\"includeArchived\":true}" : USER_INPUT;
        assertRequests(request(rpc, requestBody));
    }

    private static ObjectNode historyPayload(
            String json, String parent, String status, String created, String updated)
            throws Exception {
        return ((ObjectNode) JSON.readTree(json))
                .put("application_id", parent)
                .put("application_status", status)
                .put("application_created_at", created)
                .put("application_updated_at", updated);
    }

    private static ObjectNode userInput() {
        return options().put("limit", 2).put("includeDeactivated", true);
    }

    private static ObjectNode syncOptions(String token) {
        ObjectNode config = options();
        config.putObject("sync").put("token", token);
        return config;
    }

    private static ObjectNode options() {
        return JSON.createObjectNode();
    }

    private static byte[] classBytes(String name) throws Exception {
        try (InputStream content = AshbyPeer.class.getResourceAsStream(name)) {
            if (content == null) {
                throw new IllegalStateException("Missing fixture class " + name);
            }
            return content.readAllBytes();
        }
    }

    private void installCertificate(String path) throws Exception {
        Container.ExecResult pem = peer.execInContainer("cat", path);
        assertEquals(0, pem.getExitCode(), pem.getStderr());
        engine.installCertificate(pem.getStdout(), path);
        Files.writeString(certificates.resolve(Path.of(path).getFileName()), pem.getStdout());
    }

    private static String jobId() {
        return Long.toString(UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE);
    }

    private Future<Container.ExecResult> start(Job job, String id) {
        return jobs.submit(() -> engine.start(job.name, id));
    }

    private Future<Container.ExecResult> restore(Job job, String id) {
        return jobs.submit(() -> engine.restoreJob(job.name, id));
    }

    private void stop(String id, Future<Container.ExecResult> run) throws Exception {
        try {
            if (!run.isDone()) {
                engine.stopJob(id);
            }
            run.get(45, TimeUnit.SECONDS);
        } finally {
            if (!run.isDone()) {
                run.cancel(true);
            }
        }
    }

    private static Job job(String resource, ObjectNode changes, boolean streaming, boolean anchor)
            throws Exception {
        String name = "ashby-" + UUID.randomUUID();
        String path = "/tmp/seatunnel_mnt/" + name;
        ObjectNode settings =
                options()
                        .put("resource", resource)
                        .put("token", SECRET)
                        .put("endpoint", TLS)
                        .put("trust_certificate", "/tmp/api.pem")
                        .put("page_size", 2)
                        .put("plugin_output", "events");
        settings.setAll(changes);
        String anchorSource =
                anchor
                        ? "FakeSource {\n plugin_output=anchor\n row.num=1000\n split.num=1000\n"
                                + " split.read-interval=250\n schema.fields.value=\"string\"\n}\n"
                        : "";
        String anchorSink = anchor ? "Console {\n plugin_input=anchor\n}\n" : "";
        String config =
                "env {\n job.mode="
                        + (streaming ? "STREAMING" : "BATCH")
                        + "\n parallelism=1\n checkpoint.interval=350\n}\n"
                        + "source {\n Ashby "
                        + settings
                        + "\n"
                        + anchorSource
                        + "}\n"
                        + "sink {\n LocalFile {\n plugin_input=events\n path=\""
                        + path
                        + "\"\n file_format_type=json\n is_enable_transaction=true\n custom_filename=true\n"
                        + " file_name_expression=\"receipt_$"
                        + "{transactionId}\"\n }\n"
                        + anchorSink
                        + "}\n";
        Path configDir = ContainerUtil.getCurrentModulePath().resolve("src/test/resources");
        Files.createDirectories(configDir);
        Path file = configDir.resolve(name + ".conf");
        Files.writeString(file, config, StandardCharsets.UTF_8);
        return new Job("/" + file.getFileName(), file, path);
    }

    private static ObjectNode row(String resource, String id, String parent, String payload)
            throws Exception {
        ObjectNode row =
                options().put("resource", resource).put("record_id", id).put("parent_id", parent);
        row.set("payload", JSON.readTree(payload));
        return row;
    }

    private void assertRows(Job job, ObjectNode... expected) throws Exception {
        assertEquals(multiset(List.of(expected)), multiset(engine.receipts(job.path)));
    }

    private void awaitRows(Job job, ObjectNode... expected) throws Exception {
        Map<ObjectNode, Integer> wanted = multiset(List.of(expected));
        await(
                () -> wanted.equals(multiset(engine.receipts(job.path))),
                "the exact committed output rows");
    }

    private static Map<ObjectNode, Integer> multiset(List<ObjectNode> rows) {
        Map<ObjectNode, Integer> counts = new HashMap<>();
        rows.forEach(row -> counts.merge(row, 1, Integer::sum));
        return counts;
    }

    private static void assertSuccess(Container.ExecResult result) {
        assertEquals(0, result.getExitCode(), () -> result.getStdout() + result.getStderr());
    }

    private String assertFailure(Container.ExecResult result, String expected) {
        assertNotEquals(0, result.getExitCode(), expected);
        String output = result.getStdout() + result.getStderr() + engine.getServerLogs();
        assertTrue(output.contains(expected), () -> "Expected diagnostic: " + expected);
        assertFalse(output.contains(PRIVATE_DATA), "provider result appears in diagnostics");
        assertFalse(output.contains(SECRET), "API key appears in diagnostics");
        assertFalse(
                output.contains(
                        Base64.getEncoder()
                                .encodeToString((SECRET + ":").getBytes(StandardCharsets.UTF_8))),
                "Basic credentials appear in diagnostics");
        return output;
    }

    private static String single(String rows) {
        return "{\"success\":true,\"results\":[" + rows + "]}";
    }

    private static String page(boolean more, String cursor, String token, String rows) {
        String prefix = "{\"success\":true,\"moreDataAvailable\":" + more;
        if (cursor != null) {
            prefix += ",\"nextCursor\":\"" + cursor + "\"";
        }
        if (token != null) {
            prefix += ",\"syncToken\":\"" + token + "\"";
        }
        return prefix + ",\"results\":[" + rows + "]}";
    }

    private static String response(String rpc, String json) {
        return reply(rpc, 200, "", "plain", json);
    }

    private static String slow(String rpc) {
        return reply(rpc, 200, "", "slow", "");
    }

    private static String reply(String rpc, int status, String retry, String mode, String body) {
        return String.join(
                "\t",
                "/" + rpc,
                Integer.toString(status),
                retry,
                mode,
                Base64.getEncoder().encodeToString(body.getBytes(StandardCharsets.UTF_8)));
    }

    private void program(String... entries) throws Exception {
        configure("/program", entries);
    }

    private void append(String... entries) throws Exception {
        configure("/append", entries);
    }

    private URI adminUri(String path) {
        return URI.create("http://" + peer.getHost() + ":" + peer.getMappedPort(8080) + path);
    }

    private void configure(String path, String... entries) throws Exception {
        HttpRequest request =
                HttpRequest.newBuilder(adminUri(path))
                        .timeout(Duration.ofSeconds(5))
                        .POST(HttpRequest.BodyPublishers.ofString(String.join("\n", entries)))
                        .build();
        assertEquals(200, admin.send(request, HttpResponse.BodyHandlers.discarding()).statusCode());
    }

    private String trace() throws Exception {
        HttpRequest request =
                HttpRequest.newBuilder(adminUri("/trace")).timeout(Duration.ofSeconds(5)).build();
        HttpResponse<String> response = admin.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        return response.body();
    }

    private static List<Object> request(String route, String body) throws Exception {
        return List.of("/" + route, JSON.readTree(body));
    }

    @SafeVarargs
    private final void assertRequests(List<Object>... expected) throws Exception {
        assertEquals(
                List.of(expected),
                requests().stream().map(WireRequest::signature).collect(Collectors.toList()));
    }

    private List<WireRequest> requests() throws Exception {
        List<WireRequest> calls = new ArrayList<>();
        for (String line : trace().split("\n")) {
            if (line.startsWith("request\t")) {
                WireRequest call = new WireRequest(line);
                assertTrue(call.authenticated, "expected Basic API key and empty password");
                assertTrue(call.notImpersonating, "connector must not send an actor header");
                assertEquals("POST", call.method);
                calls.add(call);
            }
        }
        return calls;
    }

    private static void await(Condition ready, String purpose) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
        do {
            if (ready.poll()) {
                return;
            }
            Thread.sleep(100);
        } while (System.nanoTime() - deadline < 0L);
        throw new AssertionError("Timed out awaiting " + purpose);
    }

    /**
     * A single call recorded through the actual TLS socket.
     *
     * <p>The arrival timestamp comes from the peer and is comparable within one test run.
     */
    private static final class WireRequest {
        private final String path;
        private final String method;
        private final boolean authenticated;
        private final boolean notImpersonating;
        private final long arrived;
        private final JsonNode body;

        private WireRequest(String line) throws Exception {
            String[] parts = line.split("\t", -1);
            path = parts[1];
            method = parts[2];
            authenticated = Boolean.parseBoolean(parts[3]);
            arrived = Long.parseLong(parts[4]);
            body = JSON.readTree(Base64.getDecoder().decode(parts[5]));
            notImpersonating = Boolean.parseBoolean(parts[6]);
        }

        private List<Object> signature() {
            return List.of(path, body);
        }
    }

    /**
     * A temporary configuration consumed by the regular job submission path.
     *
     * <p>The sink output location belongs to the job and is reused during savepoint recovery.
     */
    private static final class Job implements AutoCloseable {
        private final String name;
        private final Path config;
        private final String path;

        private Job(String name, Path config, String path) {
            this.name = name;
            this.config = config;
            this.path = path;
        }

        @Override
        public void close() throws Exception {
            Files.deleteIfExists(config);
        }
    }

    /**
     * A checked observation polled against the running engine or peer.
     *
     * <p>An unexpected I/O failure is reported immediately instead of appearing as a timeout.
     */
    @FunctionalInterface
    private interface Condition {
        boolean poll() throws Exception;
    }

    /**
     * Exposes only this module's committed sink files and the engine's checkpoint overview.
     *
     * <p>Reading the files avoids counting a client or server log message as a delivered row.
     */
    private static final class Engine extends SeaTunnelContainer {
        private final HttpClient rest = HttpClient.newHttpClient();

        private void installCertificate(String contents, String path) {
            server.copyFileToContainer(Transferable.of(contents), path);
        }

        private Container.ExecResult start(String conf, String jobId) throws Exception {
            return executeJob(server, conf, jobId, List.of());
        }

        private List<ObjectNode> receipts(String path) throws Exception {
            Container.ExecResult result =
                    server.execInContainer(
                            "sh",
                            "-c",
                            "if [ -d '"
                                    + path
                                    + "' ]; then find '"
                                    + path
                                    + "' -maxdepth 1 -type f -name 'receipt_*' -exec cat {} +; fi");
            assertEquals(0, result.getExitCode(), result.getStderr());
            List<ObjectNode> rows = new ArrayList<>();
            for (String line : result.getStdout().split("\\R")) {
                if (!line.isBlank()) {
                    ObjectNode row = (ObjectNode) JSON.readTree(line);
                    assertTrue(row.path("payload").isTextual());
                    row.set("payload", JSON.readTree(row.path("payload").textValue()));
                    rows.add(row);
                }
            }
            return rows;
        }

        private long completedCheckpoints(String id) throws Exception {
            URI uri =
                    URI.create(
                            "http://"
                                    + server.getHost()
                                    + ":"
                                    + server.getMappedPort(8080)
                                    + "/jobs/checkpoints/"
                                    + id);
            HttpResponse<String> response =
                    rest.send(
                            HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(5)).build(),
                            HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                return 0L;
            }
            long count = 0L;
            for (JsonNode pipeline : JSON.readTree(response.body()).path("pipelines")) {
                count += pipeline.path("counts").path("completed").asLong();
            }
            return count;
        }
    }
}
