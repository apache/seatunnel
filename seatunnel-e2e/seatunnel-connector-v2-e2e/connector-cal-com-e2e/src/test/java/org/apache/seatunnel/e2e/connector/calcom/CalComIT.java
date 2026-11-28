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
package org.apache.seatunnel.e2e.connector.calcom;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.calcom.CalComRecoveryProbe;
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
 * Runs all Cal.com v2 resources through Zeta and a transactional LocalFile sink.
 *
 * <p>An isolated HTTPS peer verifies exact wire requests and returns independently authored
 * records.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CalComIT extends TestSuiteBase {
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

    private static final String EVENTS = "/v2/event-types";
    private static final String ME = "/v2/me";
    private static final String SCHEDULES = "/v2/schedules";
    private static final String CALENDARS = "/v2/calendars";
    private static final String BOOKINGS = "/v2/bookings";
    private static final String CONFERENCING = "/v2/conferencing";
    private static final String GROUPED = "2024-04-15";
    private static final String FLAT = "2026-06-12";
    private static final String SCHEDULE = "2024-06-11";
    private static final String OFFSET = "2024-08-13";
    private static final String CURSOR = "2026-05-01";

    @Test
    void readsAllSixResourcesWithOriginalGroupsCalendarConnectionsAndPreciseRows()
            throws Exception {
        String name = "all-six";
        String owner = "{\"groupName\":\"東京\",\"profile\":{\"teamId\":13,\"name\":null}}";
        String team = "{\"groupName\":\"Shared\",\"profile\":{\"teamId\":28}}";
        String eventOne =
                "{\"id\":9007199254740993,\"title\":\"Coaching 🌍\","
                        + "\"metadata\":{\"ratio\":1.00000000000000000012900,\"future\":null},"
                        + "\"prices\":[null,{\"currency\":\"EUR\",\"label\":\"Café\"}]}";
        String eventTwo = "{\"id\":9007199254740993,\"title\":\"Same event id in another group\"}";
        String profile =
                "{\"id\":101,\"username\":\"zoë\",\"weekStart\":\"Monday\",\"timeFormat\":24}";
        String day =
                "{\"id\":254,\"availability\":[{\"days\":[\"Monday\"],\"startTime\":\"17:00\","
                        + "\"endTime\":\"19:00\"}],\"overrides\":[{\"date\":\"2026-10-25\"}],\"timeZone\":\"Europe/Rome\"}";
        String google =
                "{\"credentialId\":41,\"integration\":{\"slug\":\"google-calendar\",\"name\":\"Google\"}}";
        String outlook =
                "{\"credentialId\":42,\"integration\":{\"slug\":\"office365-calendar\",\"name\":\"Outlook\"}}";
        String calendarOne =
                "{\"externalId\":\"shared@example.test\",\"readOnly\":false,\"primary\":null}";
        String calendarTwo =
                "{\"externalId\":\"shared@example.test\",\"readOnly\":true,\"isSelected\":false}";
        String booking =
                "{\"id\":123,\"uid\":\"opaque_book\",\"status\":\"cancelled\","
                        + "\"start\":\"2026-10-25T02:30:00.123456789+02:00\","
                        + "\"end\":\"2026-10-25T02:30:00+01:00\",\"metadata\":{\"value\":9223372036854775807}}";
        String conference =
                "{\"id\":55,\"type\":\"google_video\",\"userId\":101,\"appId\":\"google-meet\","
                        + "\"invalid\":null,\"key\":{\"refresh_token\":\"private-provider-message\"},\"unknown\":{\"safe\":true}}";
        program(
                name,
                List.of(
                        reply(
                                EVENTS,
                                GROUPED,
                                groups(
                                        group(owner, "eventTypes", eventOne),
                                        group("{\"groupName\":\"empty\"}", "eventTypes"),
                                        group(team, "eventTypes", eventTwo))),
                        reply(ME, null, successData(profile)),
                        reply(SCHEDULES, SCHEDULE, successData(array(day))),
                        reply(
                                CALENDARS,
                                null,
                                successData(
                                        "{\"connectedCalendars\":"
                                                + array(
                                                        group(google, "calendars", calendarOne),
                                                        group(outlook, "calendars", calendarTwo))
                                                + "}")),
                        reply(BOOKINGS + "?limit=100", CURSOR, cursorPage(null, booking))
                                .put("mode", "gzip"),
                        reply(CONFERENCING, null, successData(array(conference)))));
        success(run(new Settings(name)));
        Map<String, JsonNode> expected = new HashMap<>();
        expect(expected, "event_types", "9007199254740993", owner, eventOne);
        expect(expected, "event_types", "9007199254740993", team, eventTwo);
        expect(expected, "my_profile", "101", null, profile);
        expect(expected, "schedules", "254", null, day);
        expect(expected, "calendars", "shared@example.test", google, calendarOne);
        expect(expected, "calendars", "shared@example.test", outlook, calendarTwo);
        expect(expected, "bookings", "123", null, booking);
        expect(
                expected,
                "conferencing",
                "55",
                null,
                "{\"id\":55,\"type\":\"google_video\",\"userId\":101,\"appId\":\"google-meet\","
                        + "\"invalid\":null,\"unknown\":{\"safe\":true}}");
        assertRows(name, expected);
        assertTrue(
                engine.rows(name).stream()
                        .anyMatch(line -> line.contains("1.00000000000000000012900")));
        assertTrue(
                engine.rows(name).stream()
                        .noneMatch(line -> line.contains("private-provider-message")));
        consumed(name, 6);
    }

    @Test
    void readsCurrentFlatEventsAndFullSchedulesWithoutGuessingPagination() throws Exception {
        String name = "full-lists";
        Settings settings = new Settings(name, "event_types", "schedules");
        settings.eventVersion = FLAT;
        List<String> events = new ArrayList<>();
        List<String> schedules = new ArrayList<>();
        Map<String, JsonNode> expected = new HashMap<>();
        for (int id = 1; id <= 113; ++id) {
            String event = "{\"id\":" + id + ",\"teamId\":24,\"title\":\"event\"}";
            String schedule = "{\"id\":" + id + ",\"name\":\"availability\"}";
            events.add(event);
            schedules.add(schedule);
            expect(expected, "event_types", Integer.toString(id), null, event);
            expect(expected, "schedules", Integer.toString(id), null, schedule);
        }
        program(
                name,
                List.of(
                        reply(EVENTS, FLAT, successData(array(events.toArray(new String[0])))),
                        reply(
                                SCHEDULES,
                                SCHEDULE,
                                successData(array(schedules.toArray(new String[0]))))));
        success(run(settings));
        assertRows(name, expected);
        consumed(name, 2);
    }

    @Test
    void acceptsEmptyNestedListsAndPreservesTheExactScopeOfTheProfileCall() throws Exception {
        String name = "empty-nested";
        Settings settings = new Settings(name, "event_types", "calendars", "my_profile");
        String user = "{\"id\":789,\"defaultScheduleId\":null}";
        program(
                name,
                List.of(
                        reply(EVENTS, GROUPED, groups()),
                        reply(ME, null, successData(user)),
                        reply(
                                CALENDARS,
                                null,
                                successData(
                                        "{\"connectedCalendars\":[{\"credentialId\":99,\"calendars\":[]}]}"))));
        success(run(settings));
        Map<String, JsonNode> expected = new HashMap<>();
        expect(expected, "my_profile", "789", null, user);
        assertRows(name, expected);
        consumed(name, 3);
    }

    @Test
    void followsAnOpaqueCursorThroughAFullAndAnEmptyPageToTheLastBooking() throws Exception {
        String name = "cursor-more-than-100";
        Settings settings = new Settings(name, "bookings");
        String escaped = "https://credential-trap.invalid/a?opaque=+%&/é";
        String query = "https%3A%2F%2Fcredential-trap.invalid%2Fa%3Fopaque%3D%2B%25%26%2F%C3%A9";
        List<String> first = new ArrayList<>();
        Map<String, JsonNode> expected = new HashMap<>();
        for (int id = 1; id <= 101; ++id) {
            String row = booking(id, "pending");
            if (id <= 100) {
                first.add(row);
            }
            expect(expected, "bookings", Integer.toString(id), null, row);
        }
        program(
                name,
                List.of(
                        reply(
                                BOOKINGS + "?limit=100",
                                CURSOR,
                                cursorPage(escaped, first.toArray(new String[0]))),
                        reply(BOOKINGS + "?limit=100&cursor=" + query, CURSOR, cursorPage("tail")),
                        reply(
                                BOOKINGS + "?limit=100&cursor=tail",
                                CURSOR,
                                cursorPage(null, booking(101, "pending")))));
        success(run(settings));
        assertRows(name, expected);
        consumed(name, 3);
    }

    @Test
    void usesLegacyOffsetMetadataEvenWhenServerHydrationOmitsManySelectedRows() throws Exception {
        String name = "legacy-offset-200";
        Settings settings = new Settings(name, "bookings");
        settings.bookingVersion = OFFSET;
        String first = booking(1, "accepted");
        String last = booking(201, "cancelled");
        program(
                name,
                List.of(
                        reply(
                                BOOKINGS + "?skip=0&take=100",
                                OFFSET,
                                offsetPage(100, 201, 100, 101, 1, 3, true, false, first)),
                        reply(
                                BOOKINGS + "?skip=100&take=100",
                                OFFSET,
                                offsetPage(100, 201, 100, 1, 2, 3, true, true)),
                        reply(
                                BOOKINGS + "?skip=200&take=100",
                                OFFSET,
                                offsetPage(1, 201, 100, 0, 3, 3, false, true, last))));
        success(run(settings));
        Map<String, JsonNode> expected = new HashMap<>();
        expect(expected, "bookings", "1", null, first);
        expect(expected, "bookings", "201", null, last);
        assertRows(name, expected);
        consumed(name, 3);
    }

    @Test
    void rejectsHalfSpecifiedContinuationInconsistentMetadataAndCursorCycles() throws Exception {
        for (String pagination :
                List.of(
                        "{\"hasMore\":true,\"nextCursor\":null}",
                        "{\"hasMore\":\"true\",\"nextCursor\":\"next\"}",
                        "{\"hasMore\":false,\"nextCursor\":\"next\"}",
                        "{\"hasMore\":true,\"nextCursor\":\"\"}")) {
            String name = "bad-cursor-" + Math.abs(pagination.hashCode());
            program(
                    name,
                    List.of(
                            reply(
                                    BOOKINGS + "?limit=100",
                                    CURSOR,
                                    "{\"status\":\"success\",\"data\":[],\"pagination\":"
                                            + pagination
                                            + "}")));
            failure(run(new Settings(name, "bookings")), "pagination");
            assertRows(name, Collections.emptyMap());
            consumed(name, 1);
        }
        String cycle = "cursor-cycle";
        program(
                cycle,
                List.of(
                        reply(BOOKINGS + "?limit=100", CURSOR, cursorPage("a")),
                        reply(BOOKINGS + "?limit=100&cursor=a", CURSOR, cursorPage("b")),
                        reply(BOOKINGS + "?limit=100&cursor=b", CURSOR, cursorPage("a"))));
        failure(run(new Settings(cycle, "bookings")), "repeated nextCursor");
        consumed(cycle, 3);

        String offset = "offset-meta-mismatch";
        Settings settings = new Settings(offset, "bookings");
        settings.bookingVersion = OFFSET;
        program(
                offset,
                List.of(
                        reply(
                                BOOKINGS + "?skip=0&take=100",
                                OFFSET,
                                offsetPage(100, 201, 100, 101, 1, 4, true, false))));
        failure(run(settings), "inconsistent offset pagination metadata");
        assertRows(offset, Collections.emptyMap());
        consumed(offset, 1);
    }

    @Test
    void checksBothPagedAndNestedBudgetsBeforeFollowingOrPublishingExtraRows() throws Exception {
        String paged = "page-cap";
        Settings pages = new Settings(paged, "bookings");
        pages.pages = 1;
        program(paged, List.of(reply(BOOKINGS + "?limit=100", CURSOR, cursorPage("never"))));
        failure(run(pages), "max_pages_per_resource");
        consumed(paged, 1);

        String overfilled = "page-overfilled";
        Settings one = new Settings(overfilled, "bookings");
        one.pageSize = 1;
        program(
                overfilled,
                List.of(
                        reply(
                                BOOKINGS + "?limit=1",
                                CURSOR,
                                cursorPage(null, booking(1, "accepted"), booking(2, "accepted")))));
        failure(run(one), "configured record count");
        assertRows(overfilled, Collections.emptyMap());
        consumed(overfilled, 1);

        String nested = "full-list-cap";
        Settings cap = new Settings(nested, "event_types");
        cap.items = 1;
        program(
                nested,
                List.of(
                        reply(
                                EVENTS,
                                GROUPED,
                                groups(
                                        group(
                                                "{\"groupName\":\"large\"}",
                                                "eventTypes",
                                                "{\"id\":1}",
                                                "{\"id\":2}")))));
        failure(run(cap), "configured record count");
        assertRows(nested, Collections.emptyMap());
        consumed(nested, 1);
    }

    @Test
    void rejectsSuccessEnumsWithErrorPayloadsAndWrongResourceShapes() throws Exception {
        List<String> bad =
                List.of(
                        "{\"status\":\"error\",\"data\":{\"id\":123},\"error\":{\"message\":\"private-provider-message\"}}",
                        "{\"status\":\"success\",\"data\":{\"id\":123},\"errors\":[{\"message\":\"private-provider-message\"}]}",
                        "{\"status\":\"success\",\"data\":[{\"id\":1}]}",
                        "{\"status\":\"success\",\"data\":{\"id\":1.25}}");
        for (int i = 0; i < bad.size(); ++i) {
            String name = "bad-envelope-" + i;
            program(name, List.of(reply(ME, null, bad.get(i))));
            failure(
                    run(new Settings(name, "my_profile")),
                    i < 2 ? "successful response" : "Cal.com");
            assertRows(name, Collections.emptyMap());
            consumed(name, 1);
        }

        String version = "wrong-event-shape";
        program(version, List.of(reply(EVENTS, GROUPED, successData("[{\"id\":1}]"))));
        failure(run(new Settings(version, "event_types")), "eventTypeGroups");
        consumed(version, 1);
    }

    @Test
    void rejectsUnsupportedVersionsAndSurfacesRetiredProviderPathsWithoutFallback()
            throws Exception {
        String unsupported = "invalid-version-option";
        Settings invalid = new Settings(unsupported, "event_types");
        invalid.eventVersion = "latest";
        program(unsupported, List.of());
        failure(run(invalid), "event_types_api_version");
        consumed(unsupported, 0);

        String unavailable = "provider-version-rejected";
        Settings legacy = new Settings(unavailable, "bookings");
        legacy.bookingVersion = OFFSET;
        legacy.retries = 3;
        program(
                unavailable,
                List.of(
                        reply(BOOKINGS + "?skip=0&take=100", OFFSET, "private-provider-message")
                                .put("status", 404)));
        failure(run(legacy), "HTTP 404");
        consumed(unavailable, 1);
    }

    @Test
    void reportsCredentialAndPermissionErrorsWithoutASecondRequest() throws Exception {
        for (int code : List.of(401, 403)) {
            String name = "auth-" + code;
            Settings settings = new Settings(name, "my_profile");
            settings.retries = 3;
            program(
                    name,
                    List.of(
                            reply(ME, null, "private-provider-message " + CalComPeer.TOKEN)
                                    .put("status", code)));
            failure(run(settings), "HTTP " + code);
            assertRows(name, Collections.emptyMap());
            consumed(name, 1);
        }
    }

    @Test
    void honorsRetryAfterAndBoundsTheTotalRetryBudget() throws Exception {
        String name = "pacing";
        Settings settings = new Settings(name, "my_profile");
        settings.retries = 2;
        String profile = "{\"id\":123}";
        program(
                name,
                List.of(
                        reply(ME, null, "").put("status", 429).put("retryAfter", "1"),
                        reply(ME, null, "").put("status", 503),
                        reply(ME, null, successData(profile))));
        success(run(settings));
        List<String> times = engine.lines(name + ".times");
        assertTrue(Long.parseLong(times.get(1)) - Long.parseLong(times.get(0)) >= 850);
        assertTrue(Long.parseLong(times.get(2)) - Long.parseLong(times.get(1)) >= 400);
        consumed(name, 3);
        Map<String, JsonNode> expected = new HashMap<>();
        expect(expected, "my_profile", "123", null, profile);
        assertRows(name, expected);

        String cap = "retry-over-budget";
        Settings limited = new Settings(cap, "my_profile");
        limited.retries = 2;
        limited.retryDelay = 500;
        program(cap, List.of(reply(ME, null, "").put("status", 429).put("retryAfter", "2")));
        failure(run(limited), "Retry-After exceeds max_retry_delay_ms");
        consumed(cap, 1);
    }

    @Test
    void appliesTheBodyLimitBeforeAndAfterGzipAndRejectsMalformedJson() throws Exception {
        String huge = successData("{\"id\":13,\"note\":\"" + "é".repeat(2000) + "\"}");
        for (String mode :
                List.of("json", "gzip", "bad-gzip", "trailing", "duplicate", "too-deep")) {
            String name = "body-" + mode;
            Settings settings = new Settings(name, "my_profile");
            String body = huge;
            if (mode.equals("json") || mode.equals("gzip")) {
                settings.bytes = 256;
            } else if (mode.equals("trailing")) {
                body = successData("{\"id\":1}") + "{}";
            } else if (mode.equals("duplicate")) {
                body = successData("{\"id\":1,\"id\":2}");
            } else if (mode.equals("too-deep")) {
                body =
                        successData(
                                "{\"id\":1,\"nested\":"
                                        + "[".repeat(100)
                                        + "null"
                                        + "]".repeat(100)
                                        + "}");
            }
            program(name, List.of(reply(ME, null, body).put("mode", mode)));
            String reason =
                    mode.equals("json") || mode.equals("gzip")
                            ? "max_response_bytes"
                            : mode.equals("bad-gzip")
                                    ? "invalid gzip response"
                                    : "invalid JSON response";
            failure(run(settings), reason);
            assertRows(name, Collections.emptyMap());
            consumed(name, 1);
        }
    }

    @Test
    void verifiesTlsAndRejectsARedirectToATrustedTrap() throws Exception {
        String wrong = "wrong-trust";
        Settings refused = new Settings(wrong, "my_profile");
        refused.trust = wrongTrust;
        program(wrong, List.of());
        failure(run(refused), "certificate verification");
        consumed(wrong, 0);

        String redirect = "no-redirect";
        program(redirect, List.of(reply(ME, null, "").put("mode", "redirect").put("status", 302)));
        failure(run(new Settings(redirect, "my_profile")), "HTTP 302");
        consumed(redirect, 1);
    }

    @Test
    void boundsATricklingResponseByTheDeadlineAndCancelsAnIncompleteRead() throws Exception {
        String slow = "slow-deadline";
        Settings deadline = new Settings(slow, "my_profile");
        deadline.request = 1500;
        deadline.operation = 1500;
        program(slow, List.of(reply(ME, null, successData("{\"id\":123}")).put("mode", "partial")));
        failure(run(deadline), "operation deadline");
        await(slow + ".disconnect");
        assertRows(slow, Collections.emptyMap());
        consumed(slow, 1);

        String stop = "cancel-active-page";
        Settings cancel = new Settings(stop, "my_profile");
        program(stop, List.of(reply(ME, null, successData("{\"id\":123}")).put("mode", "partial")));
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob("/calcom.conf", job, cancel.variables()));
        try {
            await(stop + ".started");
            success(engine.cancelJob(job));
            running.get(30, TimeUnit.SECONDS);
            await(stop + ".disconnect");
        } finally {
            engine.touch(stop + ".release");
            if (!running.isDone()) {
                engine.cancelJob(job);
                running.cancel(true);
            }
        }
        assertRows(stop, Collections.emptyMap());
        consumed(stop, 1);
    }

    @Test
    void restoresTheNativeCursorAfterACompletedResourceAndCommittedBookingPage() throws Exception {
        String name = "cursor-savepoint";
        Settings settings = new Settings(name, "schedules", "bookings");
        settings.request = 120000;
        settings.operation = 180000;
        String schedule = "{\"id\":6,\"name\":\"Already complete\"}";
        String first = booking(71, "accepted");
        String last = booking(72, "accepted");
        ObjectNode target =
                reply(BOOKINGS + "?limit=100&cursor=next", CURSOR, cursorPage(null, last));
        String job =
                savepointWhilePending(
                        settings,
                        List.of(
                                reply(SCHEDULES, SCHEDULE, successData(array(schedule))),
                                reply(BOOKINGS + "?limit=100", CURSOR, cursorPage("next", first)),
                                target.deepCopy().put("mode", "partial")));
        String restored = "cursor-restored";
        program(restored, List.of(target));
        success(engine.restoreJob("/calcom.conf", job, settings.variables()));
        Map<String, JsonNode> expected = new HashMap<>();
        expect(expected, "schedules", "6", null, schedule);
        expect(expected, "bookings", "71", null, first);
        expect(expected, "bookings", "72", null, last);
        assertRows(name, expected);
        consumed(restored, 1);
    }

    @Test
    void restoresLegacyOffsetWithoutRequeryingItsAcceptedPreviousPage() throws Exception {
        String name = "legacy-savepoint";
        Settings settings = new Settings(name, "bookings");
        settings.bookingVersion = OFFSET;
        settings.pageSize = 1;
        settings.request = 120000;
        settings.operation = 180000;
        String first = booking(1, "accepted"), second = booking(2, "cancelled");
        ObjectNode target =
                reply(
                        BOOKINGS + "?skip=1&take=1",
                        OFFSET,
                        offsetPage(1, 2, 1, 0, 2, 2, false, true, second));
        String job =
                savepointWhilePending(
                        settings,
                        List.of(
                                reply(
                                        BOOKINGS + "?skip=0&take=1",
                                        OFFSET,
                                        offsetPage(1, 2, 1, 1, 1, 2, true, false, first)),
                                target.deepCopy().put("mode", "partial")));
        String restored = "legacy-restored";
        program(restored, List.of(target));
        success(engine.restoreJob("/calcom.conf", job, settings.variables()));
        Map<String, JsonNode> expected = new HashMap<>();
        expect(expected, "bookings", "1", null, first);
        expect(expected, "bookings", "2", null, second);
        assertRows(name, expected);
        consumed(restored, 1);
    }

    @Test
    void restoresACompletedPipelineWithNoApiRequestsOrDuplicateRows() throws Exception {
        String name = "terminal-source";
        Settings settings = new Settings(name, "my_profile");
        String profile = "{\"id\":904,\"username\":\"complete\"}";
        program(name, List.of(reply(ME, null, successData(profile))));
        String job = jobId();
        Map<String, JsonNode> expected = new HashMap<>();
        expect(expected, "my_profile", "904", null, profile);
        Future<Container.ExecResult> running =
                tasks.submit(
                        () ->
                                engine.executeJob(
                                        "/calcom-terminal.conf", job, settings.variables()));
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
        String restored = "terminal-restored";
        program(restored, List.of());
        Future<Container.ExecResult> resumed =
                tasks.submit(
                        () ->
                                engine.restoreJob(
                                        "/calcom-terminal.conf", job, settings.variables()));
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
    void restoresPackagedReaderAfterAnExactGroupRowAndRejectsChangedPages() throws Exception {
        String first =
                group(
                        "{\"groupName\":\"team-1\"}",
                        "eventTypes",
                        "{\"id\":11,\"title\":\"initial\"}");
        String other =
                group(
                        "{\"groupName\":\"team-2\"}",
                        "eventTypes",
                        "{\"id\":12,\"title\":\"second\"}");
        String variant =
                group(
                        "{\"groupName\":\"team-3\"}",
                        "eventTypes",
                        "{\"id\":12,\"title\":\"second\"}");
        for (String mode : List.of("resume", "changed", "zero")) {
            String name = "packaged-" + mode;
            List<ObjectNode> steps =
                    mode.equals("zero")
                            ? List.of()
                            : List.of(
                                    reply(EVENTS, GROUPED, groups(first, other)),
                                    reply(
                                            EVENTS,
                                            GROUPED,
                                            groups(
                                                    first,
                                                    mode.equals("changed") ? variant : other)));
            program(name, steps);
            success(engine.probe(mode));
            consumed(name, steps.size());
        }
    }

    private String savepointWhilePending(Settings settings, List<ObjectNode> steps)
            throws Exception {
        program(settings.name, steps);
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob("/calcom.conf", job, settings.variables()));
        try {
            await(settings.name + ".started");
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

    private static ObjectNode reply(String target, String version, String response) {
        ObjectNode step = JSON.createObjectNode().put("target", target).put("response", response);
        if (version != null) {
            step.put("version", version);
        }
        return step;
    }

    private static String booking(int id, String status) {
        return "{\"id\":"
                + id
                + ",\"uid\":\"b"
                + id
                + "\",\"status\":\""
                + status
                + "\",\"start\":\"2026-10-25T02:30:00.123456789+02:00\"}";
    }

    private static String array(String... elems) {
        return "[" + String.join(",", elems) + "]";
    }

    private static String successData(String raw) {
        return "{\"status\":\"success\",\"data\":" + raw + "}";
    }

    private static String group(String metadata, String field, String... rows) {
        String head = metadata.substring(0, metadata.length() - 1);
        return head + ",\"" + field + "\":" + array(rows) + "}";
    }

    private static String groups(String... groups) {
        return successData("{\"eventTypeGroups\":" + array(groups) + "}");
    }

    private static String cursorPage(String cursor, String... rows) {
        ObjectNode page = JSON.createObjectNode().put("hasMore", cursor != null);
        if (cursor == null) {
            page.putNull("nextCursor");
        } else {
            page.put("nextCursor", cursor);
        }
        return "{\"status\":\"success\",\"data\":" + array(rows) + ",\"pagination\":" + page + "}";
    }

    private static String offsetPage(
            int returned,
            int total,
            int take,
            int remaining,
            int page,
            int pages,
            boolean next,
            boolean previous,
            String... rows) {
        ObjectNode meta =
                JSON.createObjectNode()
                        .put("returnedItems", returned)
                        .put("totalItems", total)
                        .put("itemsPerPage", take)
                        .put("remainingItems", remaining)
                        .put("currentPage", page)
                        .put("totalPages", pages)
                        .put("hasNextPage", next)
                        .put("hasPreviousPage", previous);
        return "{\"status\":\"success\",\"data\":" + array(rows) + ",\"pagination\":" + meta + "}";
    }

    private static void expect(
            Map<String, JsonNode> expected, String resource, String id, String parent, String raw)
            throws Exception {
        ObjectNode row = JSON.createObjectNode().put("resource", resource).put("id", id);
        if (parent == null) {
            row.putNull("parent");
        } else {
            row.set("parent", JSON.readTree(parent));
        }
        row.set("record", JSON.readTree(raw));
        assertNull(expected.put(rowKey(row), row));
    }

    private void assertRows(String name, Map<String, JsonNode> expected) throws Exception {
        Map<String, JsonNode> actual = new HashMap<>();
        for (String line : engine.rows(name)) {
            ObjectNode row = (ObjectNode) JSON.readTree(line);
            Set<String> fields = new HashSet<>();
            row.fieldNames().forEachRemaining(fields::add);
            assertEquals(Set.of("resource", "id", "parent", "record"), fields);
            assertTrue(
                    row.path("record").isTextual(),
                    "the typed record is encoded in one string column");
            row.set("record", JSON.readTree(row.path("record").asText()));
            if (!row.path("parent").isNull()) {
                assertTrue(row.path("parent").isTextual());
                row.set("parent", JSON.readTree(row.path("parent").asText()));
            }
            assertNull(actual.put(rowKey(row), row));
        }
        assertEquals(expected, actual);
    }

    private static String rowKey(ObjectNode row) {
        return row.path("resource").asText()
                + "/"
                + row.path("id").asText()
                + "/"
                + row.path("parent").toString();
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
        return engine.executeJob("/calcom.conf", jobId(), settings.variables());
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
                List.of(CalComPeer.TOKEN, CalComPeer.OTHER_TOKEN, "private-provider-message")) {
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
                                "CN=" + CalComPeer.HOST,
                                "-ext",
                                "SAN=dns:" + CalComPeer.HOST + ",dns:" + CalComPeer.TRAP,
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                store.toString(),
                                "-storepass",
                                CalComPeer.PASSWORD,
                                "-keypass",
                                CalComPeer.PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String output = new String(keytool.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, keytool.waitFor(), output);
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(store)) {
            keyStore.load(input, CalComPeer.PASSWORD.toCharArray());
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
        final String[] resources;
        String trust = CalComIT.trust;
        String eventVersion = "2024-04-15";
        String bookingVersion = "2026-05-01";
        int request = 60000;
        int operation = 70000;
        int retries;
        int retryDelay = 30000;
        int bytes = 8388608;
        int pages = 10000;
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
            return new String[] {
                "api.output=" + name,
                "api.resources=" + selection,
                "api.token=" + CalComPeer.TOKEN,
                "api.certificate=" + trust,
                "api.events=" + eventVersion,
                "api.bookings=" + bookingVersion,
                "api.request_timeout=" + request,
                "api.operation_timeout=" + operation,
                "api.retries=" + retries,
                "api.retry_delay=" + retryDelay,
                "api.bytes=" + bytes,
                "api.pages=" + pages,
                "api.items=" + items,
                "api.page_size=" + pageSize
            };
        }
    }

    /**
     * Stages the independent peer and existing shaded JSON libraries in the test engine.
     *
     * <p>Host aliases and test certificates belong only to this owned container.
     */
    private static final class Engine extends SeaTunnelContainer {
        private static final String ROOT = "/tmp/calcom-peer";
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
            for (String host : List.of(CalComPeer.HOST, CalComPeer.TRAP)) {
                container.withExtraHost(host, "127.0.0.1");
            }
            container.withCopyFileToContainer(
                    MountableFile.forHostPath(files.resolve("trusted.p12")), ROOT + "/trusted.p12");
            container.withCopyFileToContainer(
                    MountableFile.forHostPath(files.resolve("trusted.pem")), ROOT + "/trusted.pem");
            try {
                stageClasses(container, CalComPeer.class);
                stageClasses(container, CalComRecoveryProbe.class);
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
                    CalComPeer.class.getName(),
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
                    CalComRecoveryProbe.class.getName(),
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
            String path = "/tmp/seatunnel_mnt/calcom-" + name;
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
