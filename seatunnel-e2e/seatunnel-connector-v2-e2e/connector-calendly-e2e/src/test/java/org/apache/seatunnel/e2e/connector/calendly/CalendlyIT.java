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
package org.apache.seatunnel.e2e.connector.calendly;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.calendly.CalendlyRecoveryProbe;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs independent Calendly API and durable OAuth contracts through a real Java 11 Zeta worker.
 *
 * <p>Every provider response and expected row is a literal fixture. The token authority is separate
 * from the data authority, and only transactional LocalFile rows count as successful extraction.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class CalendlyIT extends TestSuiteBase {
    private static final ObjectMapper JSON =
            new ObjectMapper()
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    private static final String START = "2024-01-01T00:00:00Z", END = "2024-01-02T00:00:00Z";
    private static final String PREFIX = "https://api.calendly.com";
    private static final String USER = "00000000-0000-4000-8000-000000000001";
    private static final String ORG = "00000000-0000-4000-8000-000000000002";
    private static final String EVENT = "00000000-0000-4000-8000-000000000003";
    private static final String TYPE = "00000000-0000-4000-8000-000000000004";
    private static final String GROUP = "00000000-0000-4000-8000-000000000005";
    private static final String MEMBER = "00000000-0000-4000-8000-000000000006";
    private static final String OTHER_USER = "00000000-0000-4000-8000-000000000007";
    private static final String FORM = "00000000-0000-4000-8000-000000000008";
    private static final String ROW = "00000000-0000-4000-8000-000000000009";
    private static final String EXTRA =
            ",\"extra\":{\"unicode\":\"数据🌏\",\"null\":null,\"array\":[false,null,[1,2]],\"integer\":9223372036854775808,\"decimal\":123456789012345678.12345678900}";
    private static String trust, wrongTrust, wrongNameTrust;
    @TempDir static Path directory;
    private final ExecutorService tasks = Executors.newCachedThreadPool();
    private Engine engine;
    private Future<Container.ExecResult> peer;
    private String active;

    @BeforeAll
    void startsOnlyOwnedDualOriginTlsAndEngineServices() throws Exception {
        trust = identity("trusted", CalendlyPeer.HOST);
        wrongTrust = identity("untrusted", CalendlyPeer.HOST);
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
    void exportsAll20ResourcesWithExactIdentityParentTimeAndFullPayload() throws Exception {
        List<Contract> catalog = contracts();
        assertEquals(20, catalog.size());
        for (Contract contract : catalog) {
            String name = "catalog-" + contract.name;
            List<ObjectNode> steps = new ArrayList<>();
            steps.add(me());
            if (!contract.parent.equals("-")) {
                steps.add(parent(contract.parent));
            }
            String original = sample(contract);
            if (!contract.name.equals("api_user")) {
                steps.add(get(path(contract), query(contract, null), collection(null, original)));
            }
            program(name, steps);
            success(run(new Settings(name, contract.name)));
            ObjectNode payload = (ObjectNode) JSON.readTree(original);
            String id =
                    contract.identity.equals("user")
                            ? USER
                            : contract.identity.equals("event")
                                    ? EVENT
                                    : contract.identity.equals("membership")
                                            ? MEMBER
                                            : contract.identity.equals("location")
                                                            || contract.identity.equals("outgoing")
                                                    ? null
                                                    : ROW;
            if (contract.identity.equals("event") || contract.identity.equals("invitee")) {
                payload.put("id", id);
            }
            if (contract.identity.equals("invitee")) {
                payload.put("scheduled_event_id", EVENT);
            }
            if (contract.identity.equals("location")) {
                payload.put("user", uri("users", OTHER_USER));
            }
            if (contract.identity.equals("rule")) {
                payload.put("availability_rule_uri", uri("user_availability_schedules", ROW));
            }
            assertRows(
                    name,
                    List.of(
                            expected(
                                    id,
                                    parentId(contract),
                                    contract.time.equals("-") ? null : START,
                                    payload)));
            consumed(name, steps.size());
            assertTrue(engine.rows(name).get(0).contains("123456789012345678.12345678900"));
        }
    }

    @Test
    void followsEmptyAndShortOpaqueCursorPagesAndNeverDereferencesResponseUrls() throws Exception {
        String name = "opaque-cursor", token = "next +/&=%中文", second = "second+opaque";
        Contract contract = named("contacts");
        String record = sample(contract);
        ObjectNode first = (ObjectNode) JSON.readTree(collection(token));
        ((ObjectNode) first.path("pagination"))
                .put("next_page", "https://calendly-trap.invalid/private");
        program(
                name,
                List.of(
                        me(),
                        get("/contacts", query(contract, null), first.toString()),
                        get("/contacts", query(contract, token), collection(second, record)),
                        get("/contacts", query(contract, second), collection(null))));
        success(run(new Settings(name, "contacts")));
        assertRows(name, List.of(expected(ROW, null, null, JSON.readTree(record))));
        consumed(name, 4);
    }

    @Test
    void retainsDistinctInviteesUnderTwoOldEventsWithTheSameBareInviteeId() throws Exception {
        String name = "two-old-events", secondEvent = "00000000-0000-4000-8000-000000000010";
        Contract contract = named("event_invitees");
        String first = sample(contract), second = first.replace(EVENT, secondEvent);
        String
                oldOne =
                        "{\"uri\":\""
                                + uri("scheduled_events", EVENT)
                                + "\",\"updated_at\":\"2010-01-01T00:00:00Z\"}",
                oldTwo = oldOne.replace(EVENT, secondEvent);
        String inventory =
                "count=2&organization="
                        + enc(uri("organizations", ORG))
                        + "&sort="
                        + enc("start_time:desc");
        program(
                name,
                List.of(
                        me(),
                        get("/scheduled_events", inventory, collection(null, oldOne, oldTwo)),
                        get(path(contract), query(contract, null), collection(null, first)),
                        get(
                                path(contract).replace(EVENT, secondEvent),
                                query(contract, null),
                                collection(null, second))));
        success(run(new Settings(name, "event_invitees")));
        ObjectNode one = (ObjectNode) JSON.readTree(first);
        one.put("id", ROW).put("scheduled_event_id", EVENT);
        ObjectNode two = (ObjectNode) JSON.readTree(second);
        two.put("id", ROW).put("scheduled_event_id", secondEvent);
        assertRows(
                name,
                List.of(expected(ROW, EVENT, START, one), expected(ROW, secondEvent, START, two)));
        consumed(name, 4);
    }

    @Test
    void readsAllUpdatedEventPagesDespiteStartTimeOrderingAndAnOlderIntermediateUpdate()
            throws Exception {
        Settings settings = new Settings("unordered-updates", "scheduled_events");
        settings.incrementalMode = true;
        Contract contract = named("scheduled_events");
        String first = sample(contract),
                old =
                        first.replace(EVENT, "00000000-0000-4000-8000-000000000011")
                                .replace("2024-01-01T08:00:00+08:00", "2019-01-01T00:00:00Z");
        String last = first.replace(EVENT, "00000000-0000-4000-8000-000000000012");
        program(
                settings.name,
                List.of(
                        me(),
                        get(path(contract), query(contract, null), collection("next", first, old)),
                        get(path(contract), query(contract, "next"), collection(null, last))));
        success(run(settings));
        ObjectNode one = (ObjectNode) JSON.readTree(first);
        one.put("id", EVENT);
        ObjectNode two = (ObjectNode) JSON.readTree(last);
        two.put("id", "00000000-0000-4000-8000-000000000012");
        assertRows(
                settings.name,
                List.of(
                        expected(EVENT, ORG, START, one),
                        expected("00000000-0000-4000-8000-000000000012", ORG, START, two)));
        consumed(settings.name, 3);
    }

    @Test
    void includesBothExactCursorBoundsAndPreservesTiedRecords() throws Exception {
        Contract contract = named("event_types");
        Settings settings = new Settings("cursor-bounds", "event_types");
        settings.incrementalMode = true;
        String first = sample(contract),
                last =
                        first.replace(ROW, "00000000-0000-4000-8000-000000000013")
                                .replace("2024-01-01T08:00:00+08:00", END);
        String excluded =
                first.replace(ROW, "00000000-0000-4000-8000-000000000014")
                        .replace("2024-01-01T08:00:00+08:00", "2024-01-02T00:00:00.000000001Z");
        program(
                settings.name,
                List.of(
                        me(),
                        get(path(contract), query(contract, null), collection("more", first, last)),
                        get(path(contract), query(contract, "more"), collection(null, excluded))));
        success(run(settings));
        assertRows(
                settings.name,
                List.of(
                        expected(ROW, ORG, START, JSON.readTree(first)),
                        expected(
                                "00000000-0000-4000-8000-000000000013",
                                ORG,
                                END,
                                JSON.readTree(last))));
        consumed(settings.name, 3);
    }

    @Test
    void appliesFiveMinuteScheduledOverlapAndThreeDayRecapOverlap() throws Exception {
        Settings scheduled = new Settings("scheduled-overlap", "scheduled_events");
        scheduled.incrementalMode = true;
        scheduled.incremental = "2024-01-01T01:00:00Z";
        Contract events = named("scheduled_events");
        String
                atLower =
                        sample(events).replace("2024-01-01T08:00:00+08:00", "2024-01-01T00:55:00Z"),
                tooEarly =
                        atLower.replace(EVENT, "00000000-0000-4000-8000-000000000015")
                                .replace("00:55:00Z", "00:54:59.999999999Z");
        program(
                scheduled.name,
                List.of(
                        me(),
                        get(
                                path(events),
                                query(events, null),
                                collection(null, atLower, tooEarly))));
        success(run(scheduled));
        ObjectNode event = (ObjectNode) JSON.readTree(atLower);
        event.put("id", EVENT);
        assertRows(scheduled.name, List.of(expected(EVENT, ORG, "2024-01-01T00:55:00Z", event)));
        consumed(scheduled.name, 2);
        Settings recap = new Settings("recap-overlap", "meeting_recaps");
        recap.incrementalMode = true;
        recap.incremental = "2024-01-05T00:00:00Z";
        recap.end = "2024-01-10T00:00:00Z";
        String record =
                sample(named("meeting_recaps"))
                        .replace("2024-01-01T08:00:00+08:00", "2024-01-02T00:00:00Z");
        program(
                recap.name,
                List.of(
                        me(),
                        get(
                                "/meeting_recaps",
                                "count=2&start_time="
                                        + enc("2024-01-02T00:00:00Z")
                                        + "&end_time="
                                        + enc(recap.end),
                                collection(null, record))));
        success(run(recap));
        assertRows(
                recap.name,
                List.of(expected(ROW, null, "2024-01-02T00:00:00Z", JSON.readTree(record))));
        consumed(recap.name, 2);
    }

    @Test
    void keepsCustomAvailabilityWithNullIdentityAndFollowsOnlyItsSupportedOpaqueCursor()
            throws Exception {
        Contract contract = named("event_type_availability_schedules");
        String custom =
                "{\"event_type\":\""
                        + uri("event_types", TYPE)
                        + "\",\"availability_rule\":{\"uri\":null,\"name\":null,\"timezone\":\"Europe/London\",\"rules\":[]}}";
        String named = sample(contract);
        program(
                "custom-availability",
                List.of(
                        me(),
                        parent("type"),
                        get(path(contract), query(contract, null), collection("next", custom)),
                        get(path(contract), query(contract, "next"), collection(null, named))));
        success(run(new Settings("custom-availability", contract.name)));
        ObjectNode customData = (ObjectNode) JSON.readTree(custom);
        customData.putNull("availability_rule_uri");
        ObjectNode namedData = (ObjectNode) JSON.readTree(named);
        namedData.put("availability_rule_uri", uri("user_availability_schedules", ROW));
        assertRows(
                "custom-availability",
                List.of(
                        expected(null, TYPE, null, customData),
                        expected(ROW, TYPE, null, namedData)));
        consumed("custom-availability", 4);
    }

    @Test
    void rejectsCursorCyclesUnpagedContinuationsAndTheFinitePageBudget() throws Exception {
        Contract contacts = named("contacts");
        fails(
                new Settings("cursor-cycle", "contacts"),
                List.of(
                        me(),
                        get("/contacts", query(contacts, null), collection("same")),
                        get("/contacts", query(contacts, "same"), collection("same"))),
                "repeated cursor");
        Contract user = named("user_availability_schedules");
        fails(
                new Settings("unpaged-continuation", user.name),
                List.of(
                        me(),
                        parent("user"),
                        get(path(user), query(user, null), collection("unmodeled"))),
                "unpaged");
        Settings budget = new Settings("page-budget", "contacts");
        budget.pages = 1;
        fails(
                budget,
                List.of(me(), get("/contacts", query(contacts, null), collection("more"))),
                "max_pages");
    }

    @Test
    void rotatesRefreshTokensTwiceAcrossSeparateEngineProcessesBeforeUsingEachAccessToken()
            throws Exception {
        String store = tokenStore("two-rotations", CalendlyPeer.R1, 0600);
        for (int index = 0; index < 2; index++) {
            String previous = index == 0 ? CalendlyPeer.R1 : CalendlyPeer.R2,
                    next = index == 0 ? CalendlyPeer.R2 : CalendlyPeer.R3,
                    access = "access-" + (index + 1);
            Settings settings = oauth("rotation-" + index, store);
            program(
                    settings.name,
                    List.of(
                            token(previous, access, next),
                            me().put("authorization", "Bearer " + access)));
            success(run(settings));
            consumed(settings.name, 2);
            assertRows(settings.name, List.of(expected(USER, null, null, JSON.readTree(meData()))));
            assertStore(store, next, false);
            assertEquals("600", engine.mode(store));
            assertEquals("600", engine.mode(store + ".lock"));
        }
    }

    @Test
    void refreshesOnceAfterAData401AndUsesOnlyThePersistedRotatedSuccessor() throws Exception {
        String store = tokenStore("renew-after-401", CalendlyPeer.R1, 0600);
        Settings settings = oauth("renew-after-401", store);
        program(
                settings.name,
                List.of(
                        token(CalendlyPeer.R1, "access-1", CalendlyPeer.R2),
                        me().put("authorization", "Bearer access-1")
                                .put("status", 401)
                                .put("response", "private-provider-message"),
                        token(CalendlyPeer.R2, "access-2", CalendlyPeer.R3),
                        me().put("authorization", "Bearer access-2")));
        success(run(settings));
        consumed(settings.name, 4);
        assertStore(store, CalendlyPeer.R3, false);
        assertRows(settings.name, List.of(expected(USER, null, null, JSON.readTree(meData()))));
        String deniedStore = tokenStore("second-data-401", CalendlyPeer.R1, 0600);
        Settings denied = oauth("second-data-401", deniedStore);
        fails(
                denied,
                List.of(
                        token(CalendlyPeer.R1, "access-1", CalendlyPeer.R2),
                        me().put("authorization", "Bearer access-1")
                                .put("status", 401)
                                .put("response", "private-provider-message"),
                        token(CalendlyPeer.R2, "access-2", CalendlyPeer.R3),
                        me().put("authorization", "Bearer access-2")
                                .put("status", 401)
                                .put("response", "private-provider-message")),
                "401");
        assertStore(deniedStore, CalendlyPeer.R3, false);
    }

    @Test
    void persistsUncertainTokenPostOutcomeAndNeverReusesItAfterRestart() throws Exception {
        String store = tokenStore("unknown-rotation", CalendlyPeer.R1, 0600);
        Settings settings = oauth("unknown-rotation", store);
        settings.deadline = 1500;
        settings.readTimeout = 10000;
        settings.attempts = 4;
        fails(
                settings,
                List.of(token(CalendlyPeer.R1, "access-1", CalendlyPeer.R2).put("mode", "trickle")),
                "deadline");
        assertStore(store, null, true);
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> engine.exists(settings.name + ".disconnect"));
        Settings restored = oauth("unknown-restarted", store);
        restored.attempts = 4;
        fails(restored, List.of(), "outcome is unknown");
        assertStore(store, null, true);
    }

    @Test
    void rejectsInvalidGrantAndMalformedTokenResponsesWithoutRetryOrApiTraffic() throws Exception {
        List<ObjectNode> failures =
                List.of(
                        token(CalendlyPeer.R1, "access-1", CalendlyPeer.R2)
                                .put("status", 400)
                                .put(
                                        "response",
                                        "{\"error\":\"invalid_grant\",\"message\":\"private-provider-message\"}"),
                        token(CalendlyPeer.R1, "access-1", CalendlyPeer.R1),
                        token(CalendlyPeer.R1, "access-1", CalendlyPeer.R2)
                                .put(
                                        "response",
                                        "{\"access_token\":\"access-1\",\"refresh_token\":\""
                                                + CalendlyPeer.R2
                                                + "\",\"token_type\":\"Bearer\",\"expires_in\":0.5}"));
        for (int index = 0; index < failures.size(); index++) {
            String store = tokenStore("bad-rotation-" + index, CalendlyPeer.R1, 0600);
            Settings settings = oauth("bad-rotation-" + index, store);
            settings.attempts = 4;
            fails(settings, List.of(failures.get(index)), "OAuth");
            assertStore(store, null, true);
            fails(oauth("bad-rotation-restart-" + index, store), List.of(), "outcome is unknown");
        }
    }

    @Test
    void rejectsInsecureOrAliasedTokenStoresBeforeAnyTokenPost() throws Exception {
        String insecure = tokenStore("insecure-store", CalendlyPeer.R1, 0644);
        fails(oauth("insecure-store", insecure), List.of(), "0600");
        String target = tokenStore("linked-target", CalendlyPeer.R1, 0600);
        String soft = target + "-symlink";
        engine.link(target, soft, true);
        fails(oauth("symlink-store", soft), List.of(), "OAuth");
        String hard = target + "-hardlink";
        engine.link(target, hard, false);
        fails(oauth("hardlink-store", hard), List.of(), "OAuth");
    }

    private Settings oauth(String name, String store) {
        Settings settings = new Settings(name, "api_user");
        settings.oauth = true;
        settings.tokenFile = store;
        settings.attempts = 2;
        return settings;
    }

    private String tokenStore(String name, String refresh, int mode) throws Exception {
        Path file = Files.createTempFile(directory, "refresh-", ".json");
        Files.writeString(
                file,
                JSON.createObjectNode().put("version", 1).put("refresh_token", refresh).toString());
        return engine.store(file, name, mode);
    }

    private void assertStore(String path, String refresh, boolean pending) throws Exception {
        JsonNode state = JSON.readTree(engine.read(path));
        assertEquals(2, state.size());
        assertEquals(1, state.path("version").intValue());
        if (pending) {
            assertTrue(state.path("pending").isBoolean() && state.path("pending").booleanValue());
            assertFalse(state.has("refresh_token"));
        } else {
            assertEquals(refresh, state.path("refresh_token").textValue());
            assertFalse(state.has("access_token"));
        }
    }

    private static ObjectNode token(String previous, String access, String next) {
        String basic =
                "Basic "
                        + Base64.getEncoder()
                                .encodeToString(
                                        (CalendlyPeer.CLIENT + ":" + CalendlyPeer.SECRET)
                                                .getBytes(StandardCharsets.UTF_8));
        String form =
                "client_id="
                        + enc(CalendlyPeer.CLIENT)
                        + "&grant_type=refresh_token&refresh_token="
                        + enc(previous);
        String body =
                JSON.createObjectNode()
                        .put("access_token", access)
                        .put("refresh_token", next)
                        .put("token_type", "Bearer")
                        .put("expires_in", 3600)
                        .toString();
        return get("/oauth/token", "", body)
                .put("host", CalendlyPeer.AUTH)
                .put("method", "POST")
                .put("authorization", basic)
                .put("form", form);
    }

    @Test
    void skipsOnlyAnInitiallyDisappearedFanOutChildAndNeverHidesPermissionsOrLaterPageErrors()
            throws Exception {
        Contract invitees = named("event_invitees");
        program(
                "deleted-child",
                List.of(
                        me(),
                        parent("event"),
                        get(path(invitees), query(invitees, null), "{}").put("status", 404)));
        success(run(new Settings("deleted-child", "event_invitees")));
        assertRows("deleted-child", List.of());
        consumed("deleted-child", 3);
        Contract locations = named("locations");
        String departed = "{\"message\":\"This user is not in your organization\"}";
        program(
                "departed-user",
                List.of(
                        me(),
                        parent("user"),
                        get(path(locations), query(locations, null), departed).put("status", 403)));
        success(run(new Settings("departed-user", "locations")));
        assertRows("departed-user", List.of());
        consumed("departed-user", 3);
        fails(
                new Settings("missing-child-scope", "locations"),
                List.of(
                        me(),
                        parent("user"),
                        get(
                                        path(locations),
                                        query(locations, null),
                                        "{\"message\":\"This user is not in your organization\",\"required_scopes\":[\"availability:read\"]}")
                                .put("status", 403)),
                "403");
        fails(
                new Settings("later-child-404", "event_invitees"),
                List.of(
                        me(),
                        parent("event"),
                        get(
                                path(invitees),
                                query(invitees, null),
                                collection("more", sample(invitees))),
                        get(path(invitees), query(invitees, "more"), "{}").put("status", 404)),
                "404");
        Contract contacts = named("contacts");
        fails(
                new Settings("direct-404", "contacts"),
                List.of(me(), get(path(contacts), query(contacts, null), "{}").put("status", 404)),
                "404");
    }

    @Test
    void rejectsPermissionAndSubscriptionErrorsAndHonorsRelativeRateReset() throws Exception {
        for (int status : List.of(401, 403, 429, 503)) {
            Settings settings = new Settings("pat-status-" + status, "api_user");
            settings.attempts = 3;
            int attempts = status == 429 || status == 503 ? 3 : 1;
            List<ObjectNode> steps = new ArrayList<>();
            for (int index = 0; index < attempts; index++) {
                steps.add(
                        me().put("status", status)
                                .put("response", "private-provider-message " + CalendlyPeer.TOKEN)
                                .put("Retry-After", "0"));
            }
            fails(settings, steps, "HTTP " + status);
        }
        Settings settings = new Settings("relative-reset", "api_user");
        settings.attempts = 2;
        program(
                settings.name,
                List.of(
                        me().put("status", 429)
                                .put("response", "private-provider-message")
                                .put("X-RateLimit-Reset", "1"),
                        me()));
        success(run(settings));
        consumed(settings.name, 2);
        List<String> times = engine.lines(settings.name + ".times");
        assertTrue(Long.parseLong(times.get(1)) - Long.parseLong(times.get(0)) >= 1000L);
        assertRows(settings.name, List.of(expected(USER, null, null, JSON.readTree(meData()))));
    }

    @Test
    void verifiesTrustAndHostnameAndRejectsRedirectAndForeignCanonicalReferences()
            throws Exception {
        Settings wrong = new Settings("wrong-trust", "api_user");
        wrong.certificate = wrongTrust;
        fails(wrong, List.of(), "TLS");
        selectIdentity("wrong-name");
        Settings name = new Settings("wrong-san", "api_user");
        name.certificate = wrongNameTrust;
        fails(name, List.of(), "TLS");
        selectIdentity("trusted");
        fails(
                new Settings("redirect", "api_user"),
                List.of(me().put("status", 302).put("mode", "redirect")),
                "302");
        fails(
                new Settings("foreign-identity", "contacts"),
                List.of(
                        me().put(
                                        "response",
                                        "{\"resource\":"
                                                + meData().replace(
                                                                PREFIX,
                                                                "https://calendly-trap.invalid")
                                                + "}")),
                "out-of-scope");
        Contract contacts = named("contacts");
        fails(
                new Settings("foreign-record", "contacts"),
                List.of(
                        me(),
                        get(
                                path(contacts),
                                query(contacts, null),
                                collection(
                                        null,
                                        sample(contacts)
                                                .replace(
                                                        PREFIX, "https://calendly-trap.invalid")))),
                "out-of-scope");
    }

    @Test
    void rejectsMalformedJsonUtf8AndScopeWhileBoundingGzipAndRawBodies() throws Exception {
        for (String body :
                List.of(
                        "{\"resource\":{},\"resource\":{}}",
                        "{\"resource\":{}}{}",
                        "{\"resource\":null}",
                        "{\"message\":\"private-provider-message\"}")) {
            fails(
                    new Settings("bad-json-" + Math.abs(body.hashCode()), "api_user"),
                    List.of(me().put("response", body)),
                    "Calendly");
        }
        byte[] prefix = "{\"resource\":{\"name\":\"".getBytes(StandardCharsets.UTF_8),
                suffix = "\"}}".getBytes(StandardCharsets.UTF_8);
        byte[] invalid = new byte[prefix.length + suffix.length + 2];
        System.arraycopy(prefix, 0, invalid, 0, prefix.length);
        invalid[prefix.length] = (byte) 0xc3;
        invalid[prefix.length + 1] = (byte) 0x28;
        System.arraycopy(suffix, 0, invalid, prefix.length + 2, suffix.length);
        fails(
                new Settings("invalid-utf8", "api_user"),
                List.of(me().put("bytes", Base64.getEncoder().encodeToString(invalid))),
                "UTF-8");
        String oversized = "{\"resource\":{\"name\":\"" + "x".repeat(8000) + "\"}}";
        for (String mode : List.of("json", "gzip")) {
            Settings limit = new Settings("body-limit-" + mode, "api_user");
            limit.bytes = 1024;
            fails(
                    limit,
                    List.of(me().put("mode", mode).put("response", oversized)),
                    "max_response_bytes");
        }
        program("gzip-success", List.of(me().put("mode", "gzip")));
        success(run(new Settings("gzip-success", "api_user")));
        consumed("gzip-success", 1);
        assertRows("gzip-success", List.of(expected(USER, null, null, JSON.readTree(meData()))));
        Contract invitees = named("event_invitees");
        String foreign = sample(invitees).replace(EVENT, "00000000-0000-4000-8000-000000000099");
        fails(
                new Settings("foreign-event", "event_invitees"),
                List.of(
                        me(),
                        parent("event"),
                        get(path(invitees), query(invitees, null), collection(null, foreign))),
                "out-of-scope");
    }

    @Test
    void rejectsInvalidOptionsBeforeAuthenticationAndCancelsAnActiveDataRequest() throws Exception {
        Settings invalid = new Settings("invalid-size", "contacts");
        invalid.size = 101;
        fails(invalid, List.of(), "page_size");
        Settings notDated = new Settings("invalid-incremental", "contacts");
        notDated.incrementalMode = true;
        fails(notDated, List.of(), "incremental_start");
        Settings deadline = new Settings("data-deadline", "api_user");
        deadline.deadline = 1500;
        deadline.readTimeout = 10000;
        fails(deadline, List.of(me().put("mode", "trickle")), "deadline");
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> engine.exists(deadline.name + ".disconnect"));
        Settings settings = new Settings("data-cancel", "api_user");
        program(settings.name, List.of(me().put("mode", "trickle")));
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob("/calendly.conf", job, settings.variables()));
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> engine.exists(settings.name + ".started"));
            success(engine.cancelJob(job));
            running.get(30, TimeUnit.SECONDS);
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .until(() -> engine.exists(settings.name + ".disconnect"));
            assertRows(settings.name, List.of());
            consumed(settings.name, 1);
        } finally {
            if (!running.isDone()) {
                engine.cancelJob(job);
                running.cancel(true);
            }
        }
    }

    @Test
    void restoresInsideAParentPageAndRequestsOnlyTheRemainingInviteeCollection() throws Exception {
        Settings settings = new Settings("parent-original", "event_invitees");
        Contract contract = named("event_invitees");
        String secondEvent = "00000000-0000-4000-8000-000000000010";
        String
                oldOne =
                        "{\"uri\":\""
                                + uri("scheduled_events", EVENT)
                                + "\",\"updated_at\":\"2010-01-01T00:00:00Z\"}",
                oldTwo = oldOne.replace(EVENT, secondEvent);
        String inventory =
                "count=2&organization="
                        + enc(uri("organizations", ORG))
                        + "&sort="
                        + enc("start_time:desc");
        String first = sample(contract), second = first.replace(EVENT, secondEvent);
        String job =
                pendingSavepoint(
                        settings,
                        List.of(
                                me(),
                                get(
                                        "/scheduled_events",
                                        inventory,
                                        collection(null, oldOne, oldTwo)),
                                get(path(contract), query(contract, null), collection(null, first)),
                                get(
                                                path(contract).replace(EVENT, secondEvent),
                                                query(contract, null),
                                                collection(null, second))
                                        .put("mode", "partial")));
        program(
                "parent-restored",
                List.of(
                        me(),
                        get("/scheduled_events", inventory, collection(null, oldOne, oldTwo)),
                        get(
                                path(contract).replace(EVENT, secondEvent),
                                query(contract, null),
                                collection(null, second))));
        success(engine.restoreJob(jobConfig(settings), job, settings.variables()));
        ObjectNode one = (ObjectNode) JSON.readTree(first);
        one.put("id", ROW).put("scheduled_event_id", EVENT);
        ObjectNode two = (ObjectNode) JSON.readTree(second);
        two.put("id", ROW).put("scheduled_event_id", secondEvent);
        assertRows(
                settings.name,
                List.of(expected(ROW, EVENT, START, one), expected(ROW, secondEvent, START, two)));
        consumed("parent-restored", 3);
    }

    @Test
    void restoresTheOpaqueNextPageWithPatOrRotatedOAuthWithoutDuplicatingCommittedRows()
            throws Exception {
        Contract contract = named("contacts");
        String first = sample(contract),
                secondId = "00000000-0000-4000-8000-000000000020",
                second = first.replace(ROW, secondId);
        for (boolean oauth : List.of(false, true)) {
            Settings settings = new Settings("page-original-" + oauth, "contacts");
            settings.oauth = oauth;
            settings.attempts = 2;
            List<ObjectNode> original = new ArrayList<>();
            if (oauth) {
                settings.tokenFile = tokenStore("page-oauth", CalendlyPeer.R1, 0600);
                original.add(token(CalendlyPeer.R1, "access-1", CalendlyPeer.R2));
            }
            String auth = oauth ? "Bearer access-1" : "Bearer " + CalendlyPeer.TOKEN;
            original.add(me().put("authorization", auth));
            original.add(
                    get(
                                    path(contract),
                                    query(contract, null),
                                    collection("remaining+opaque", first))
                            .put("authorization", auth));
            original.add(
                    get(
                                    path(contract),
                                    query(contract, "remaining+opaque"),
                                    collection(null, second))
                            .put("authorization", auth)
                            .put("mode", "partial"));
            String job = pendingSavepoint(settings, original);
            List<ObjectNode> resumed = new ArrayList<>();
            if (oauth) {
                assertStore(settings.tokenFile, CalendlyPeer.R2, false);
                resumed.add(token(CalendlyPeer.R2, "access-2", CalendlyPeer.R3));
            }
            String newAuth = oauth ? "Bearer access-2" : "Bearer " + CalendlyPeer.TOKEN;
            resumed.add(me().put("authorization", newAuth));
            resumed.add(
                    get(
                                    path(contract),
                                    query(contract, "remaining+opaque"),
                                    collection(null, second))
                            .put("authorization", newAuth));
            String name = "page-restored-" + oauth;
            program(name, resumed);
            success(engine.restoreJob(jobConfig(settings), job, settings.variables()));
            consumed(name, resumed.size());
            assertRows(
                    settings.name,
                    List.of(
                            expected(ROW, null, null, JSON.readTree(first)),
                            expected(secondId, null, null, JSON.readTree(second))));
            if (oauth) {
                assertStore(settings.tokenFile, CalendlyPeer.R3, false);
            }
        }
    }

    @Test
    void rejectsAChangedAuthenticatedOrganizationBeforeRestoringItsPendingPage() throws Exception {
        Settings settings = new Settings("identity-original", "contacts");
        Contract contract = named("contacts");
        String job =
                pendingSavepoint(
                        settings,
                        List.of(
                                me(),
                                get(path(contract), query(contract, null), collection(null))
                                        .put("mode", "partial")));
        String otherOrganization = "00000000-0000-4000-8000-000000000099";
        program(
                "identity-changed",
                List.of(
                        me().put(
                                        "response",
                                        "{\"resource\":"
                                                + meData().replace(ORG, otherOrganization)
                                                + "}")));
        Container.ExecResult result =
                engine.restoreJob(jobConfig(settings), job, settings.variables());
        assertNotEquals(0, result.getExitCode(), result.getStdout() + result.getStderr());
        assertTrue((result.getStdout() + result.getStderr()).contains("changed during restore"));
        consumed("identity-changed", 1);
        assertRows(settings.name, List.of());
    }

    @Test
    void restoresATerminalPipelineWithoutRepeatingIdentityOrAuthentication() throws Exception {
        Settings settings = new Settings("terminal-original", "api_user");
        List<JsonNode> expected = List.of(expected(USER, null, null, JSON.readTree(meData())));
        program(settings.name, List.of(me()));
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(
                        () ->
                                engine.executeJob(
                                        "/calendly-terminal.conf", job, settings.variables()));
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
        Future<Container.ExecResult> restored =
                tasks.submit(
                        () ->
                                engine.restoreJob(
                                        "/calendly-terminal.conf", job, settings.variables()));
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
            if (!restored.isDone()) {
                engine.cancelJob(job);
            }
            restored.get(30, TimeUnit.SECONDS);
        }
    }

    /**
     * Saves actual Zeta state while a later response is incomplete and checkpointing still
     * advances.
     *
     * <p>Only this job and its owned response latch are stopped or released during cleanup.
     */
    private String pendingSavepoint(Settings settings, List<ObjectNode> steps) throws Exception {
        program(settings.name, steps);
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(
                        () -> engine.executeJob(jobConfig(settings), job, settings.variables()));
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

    @Test
    void serializes32RowsAndRejectsReplayChangesEvenInFilteredProviderRecords() throws Exception {
        List<String> records = new ArrayList<>();
        for (int index = 0; index < 33; index++) {
            String id =
                    "00000000-0000-4000-8000-" + String.format(Locale.ROOT, "%012d", 1000 + index);
            records.add(
                    "{\"uri\":\""
                            + uri("event_types", id)
                            + "\",\"updated_at\":\""
                            + START
                            + "\"}");
        }
        records.add(
                "{\"uri\":\""
                        + uri("event_types", "00000000-0000-4000-8000-000000002000")
                        + "\",\"updated_at\":\"2019-01-01T00:00:00Z\"}");
        String body = collection(null, records.toArray(new String[0]));
        String query =
                "count=34&organization="
                        + enc(uri("organizations", ORG))
                        + "&sort="
                        + enc("updated_at:desc");
        for (String mode : List.of("resume", "changed", "changed-filtered")) {
            String replay =
                    mode.equals("changed")
                            ? body.replace("000000001032", "000000003032")
                            : mode.equals("changed-filtered")
                                    ? body.replace("000000002000", "000000003000")
                                    : body;
            String name = "reader-" + mode;
            program(
                    name,
                    List.of(
                            me(),
                            get("/event_types", query, body),
                            me(),
                            get("/event_types", query, replay)));
            success(engine.probe(mode));
            consumed(name, 4);
        }
        program("reader-zero", List.of());
        success(engine.probe("zero"));
        consumed("reader-zero", 0);
    }

    private static List<Contract> contracts() throws IOException {
        List<Contract> result = new ArrayList<>();
        try (InputStream input = CalendlyIT.class.getResourceAsStream("/calendly-contracts.tsv")) {
            assertTrue(input != null, "independent resource matrix is missing");
            for (String line :
                    new String(input.readAllBytes(), StandardCharsets.UTF_8).split("\\R")) {
                if (!line.isBlank() && !line.startsWith("#")) {
                    result.add(new Contract(line.split("\\t", -1)));
                }
            }
        }
        return result;
    }

    private static Contract named(String resource) throws IOException {
        return contracts().stream()
                .filter(value -> value.name.equals(resource))
                .findFirst()
                .orElseThrow();
    }

    private static String uri(String family, String id) {
        return PREFIX + "/" + family + "/" + id;
    }

    private static String enc(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private static String path(Contract contract) {
        return contract.path.replace("{event}", EVENT).replace("{org}", ORG);
    }

    private static String query(Contract contract, String cursor) {
        List<String> values = new ArrayList<>();
        if (contract.counted) {
            values.add("count=2");
        }
        if (!contract.scope.equals("-")) {
            String value =
                    contract.scope.equals("organization")
                            ? uri("organizations", ORG)
                            : contract.scope.equals("event_type")
                                    ? uri("event_types", TYPE)
                                    : contract.scope.equals("group")
                                            ? uri("groups", GROUP)
                                            : contract.scope.equals("user")
                                                    ? uri("users", OTHER_USER)
                                                    : uri("routing_forms", FORM);
            values.add(contract.scope + "=" + enc(value));
        }
        if (!contract.sort.equals("-")) {
            values.add("sort=" + enc(contract.sort));
        }
        if (contract.name.equals("scheduled_events")) {
            values.add("min_start_time=" + enc(START));
        }
        if (contract.name.equals("outgoing_communications")) {
            values.add("min_created_at=" + enc(START));
            values.add("max_created_at=" + enc(END));
        }
        if (contract.name.equals("activity_log_entries")) {
            values.add("min_occurred_at=" + enc(START));
            values.add("max_occurred_at=" + enc(END));
        }
        if (contract.name.equals("meeting_recaps")) {
            values.add("start_time=" + enc(START));
            values.add("end_time=" + enc(END));
        }
        if (cursor != null) {
            values.add("page_token=" + enc(cursor));
        }
        return String.join("&", values);
    }

    private static ObjectNode parent(String kind) throws Exception {
        String path, query, value;
        switch (kind) {
            case "event":
                path = "/scheduled_events";
                query =
                        "count=2&organization="
                                + enc(uri("organizations", ORG))
                                + "&sort="
                                + enc("start_time:desc");
                value =
                        "{\"uri\":\""
                                + uri("scheduled_events", EVENT)
                                + "\",\"start_time\":\"2010-01-01T00:00:00Z\",\"updated_at\":\"2010-01-01T00:00:00Z\"}";
                break;
            case "type":
                path = "/event_types";
                query =
                        "count=2&organization="
                                + enc(uri("organizations", ORG))
                                + "&sort="
                                + enc("updated_at:desc");
                value =
                        "{\"uri\":\""
                                + uri("event_types", TYPE)
                                + "\",\"updated_at\":\"2010-01-01T00:00:00Z\"}";
                break;
            case "group":
                path = "/groups";
                query = "count=2&organization=" + enc(uri("organizations", ORG));
                value = "{\"uri\":\"" + uri("groups", GROUP) + "\"}";
                break;
            case "user":
                path = "/organization_memberships";
                query = "count=2&organization=" + enc(uri("organizations", ORG));
                value =
                        "{\"uri\":\""
                                + uri("organization_memberships", MEMBER)
                                + "\",\"user\":{\"uri\":\""
                                + uri("users", OTHER_USER)
                                + "\"}}";
                break;
            case "form":
                path = "/routing_forms";
                query = "count=2&organization=" + enc(uri("organizations", ORG));
                value = "{\"uri\":\"" + uri("routing_forms", FORM) + "\"}";
                break;
            default:
                throw new AssertionError("Unmodeled provider parent " + kind);
        }
        return get(path, query, collection(null, value));
    }

    private static String parentId(Contract contract) {
        switch (contract.parent) {
            case "event":
                return EVENT;
            case "type":
                return TYPE;
            case "group":
                return GROUP;
            case "user":
                return OTHER_USER;
            case "form":
                return FORM;
            default:
                return contract.scope.equals("organization")
                                || contract.identity.equals("invitation")
                        ? ORG
                        : null;
        }
    }

    private static String sample(Contract contract) {
        if (contract.identity.equals("user")) {
            return meData();
        }
        String fields;
        switch (contract.identity) {
            case "event":
                fields =
                        "\"uri\":\""
                                + uri("scheduled_events", EVENT)
                                + "\",\"start_time\":\"2024-01-01T00:00:00Z\"";
                break;
            case "invitee":
                fields = "\"uri\":\"" + uri("scheduled_events", EVENT) + "/invitees/" + ROW + "\"";
                break;
            case "membership":
                fields =
                        "\"uri\":\""
                                + uri("organization_memberships", MEMBER)
                                + "\",\"user\":{\"uri\":\""
                                + uri("users", OTHER_USER)
                                + "\"}";
                break;
            case "invitation":
                fields = "\"uri\":\"" + uri("organizations", ORG) + "/invitations/" + ROW + "\"";
                break;
            case "rule":
                fields =
                        "\"event_type\":\""
                                + uri("event_types", TYPE)
                                + "\",\"availability_setting\":\"host\",\"availability_rule\":{\"uri\":\""
                                + uri("user_availability_schedules", ROW)
                                + "\",\"timezone\":\"UTC\",\"rules\":[]}";
                break;
            case "location":
                fields = "\"kind\":\"physical\",\"location\":\"Office & café\"";
                break;
            case "outgoing":
                fields =
                        "\"type\":\"email\",\"body\":\"Reminder 🌏\",\"sender_uri\":\""
                                + uri("users", USER)
                                + "\"";
                break;
            default:
                fields = "\"uri\":\"" + uri(contract.name, ROW) + "\"";
        }
        if (!contract.time.equals("-")) {
            fields += ",\"" + contract.time + "\":\"2024-01-01T08:00:00+08:00\"";
        }
        return "{" + fields + EXTRA + "}";
    }

    private static String meData() {
        return "{\"uri\":\""
                + uri("users", USER)
                + "\",\"current_organization\":\""
                + uri("organizations", ORG)
                + "\",\"name\":\"Test user\""
                + EXTRA
                + "}";
    }

    private static ObjectNode me() {
        return get("/users/me", "", "{\"resource\":" + meData() + "}");
    }

    private static String collection(String token, String... records) {
        ObjectNode pagination = JSON.createObjectNode().put("next_page_token", token);
        return "{\"collection\":["
                + String.join(",", records)
                + "],\"pagination\":"
                + pagination
                + "}";
    }

    private static ObjectNode get(String path, String query, String response) {
        return JSON.createObjectNode()
                .put("path", path)
                .put("query", query)
                .put("response", response);
    }

    private static JsonNode expected(String id, String parent, String time, JsonNode data) {
        ObjectNode row =
                JSON.createObjectNode()
                        .put("id", id)
                        .put("parent_id", parent)
                        .put("updated_at", time);
        row.set("data", data);
        return row;
    }

    private void assertRows(String name, List<JsonNode> expected) throws Exception {
        Map<JsonNode, Integer> actual = new HashMap<>(), wanted = new HashMap<>();
        for (String line : engine.rows(name)) {
            JsonNode parsed = JSON.readTree(line);
            assertTrue(parsed.isObject() && parsed.size() == 4, line);
            assertTrue(parsed.path("data").isTextual(), line);
            ObjectNode row = (ObjectNode) parsed;
            row.set("data", JSON.readTree(parsed.path("data").textValue()));
            actual.merge(row, 1, Integer::sum);
        }
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
        return engine.executeJob(jobConfig(settings), jobId(), settings.variables());
    }

    private static String jobConfig(Settings settings) {
        return settings.oauth
                ? "/calendly-oauth.conf"
                : settings.incrementalMode ? "/calendly-incremental.conf" : "/calendly.conf";
    }

    private void fails(Settings settings, List<ObjectNode> steps, String reason) throws Exception {
        program(settings.name, steps);
        Container.ExecResult result = run(settings);
        String output = result.getStdout() + result.getStderr();
        assertNotEquals(0, result.getExitCode(), output);
        assertTrue(output.contains(reason), output);
        for (String secret :
                List.of(
                        CalendlyPeer.TOKEN,
                        CalendlyPeer.SECRET,
                        CalendlyPeer.R1,
                        CalendlyPeer.R2,
                        CalendlyPeer.R3,
                        "private-provider-message")) {
            assertFalse(output.contains(secret), "credential/provider payload leaked");
        }
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
        String san =
                "SAN=dns:"
                        + hostname
                        + (name.equals("wrong-name") ? "" : ",dns:" + CalendlyPeer.AUTH);
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
                                san,
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                store.toString(),
                                "-storepass",
                                CalendlyPeer.PASSWORD,
                                "-keypass",
                                CalendlyPeer.PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String output = new String(keytool.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, keytool.waitFor(), output);
        KeyStore keys = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(store)) {
            keys.load(input, CalendlyPeer.PASSWORD.toCharArray());
        }
        Files.writeString(
                directory.resolve(name + ".pem"),
                "-----BEGIN CERTIFICATE-----\n"
                        + Base64.getMimeEncoder(64, new byte[] {'\n'})
                                .encodeToString(keys.getCertificate("api").getEncoded())
                        + "\n-----END CERTIFICATE-----\n");
        return Engine.PEER + "/" + name + ".pem";
    }

    /**
     * Holds independently documented wire facts from the literal provider matrix.
     *
     * <p>No production resource catalog or serializer supplies this integration oracle.
     */
    private static final class Contract {
        private final String name, path, scope, sort, parent, time, identity;
        private final boolean counted;

        private Contract(String[] fields) {
            assertEquals(8, fields.length);
            name = fields[0];
            path = fields[1];
            scope = fields[2];
            sort = fields[3];
            parent = fields[4];
            counted = fields[5].equals("yes");
            time = fields[6];
            identity = fields[7];
        }
    }

    /**
     * Supplies one bounded resource scan with isolated files and finite HTTPS budgets.
     *
     * <p>PAT and rotating OAuth options use separate ordinary entry-point templates.
     */
    private static final class Settings {
        private final String name, resource;
        private String certificate = trust,
                start = START,
                end = END,
                incremental = START,
                tokenFile = Engine.PEER + "/private/unused.json";
        private boolean oauth, incrementalMode;
        private int size = 2,
                pages = 100,
                bytes = 8388608,
                readTimeout = 120000,
                deadline = 180000,
                attempts = 1,
                lookback;

        private Settings(String name, String resource) {
            this.name = name;
            this.resource = resource;
        }

        private String[] variables() {
            return new String[] {
                "api.output=" + name,
                "api.resource=" + resource,
                "api.token=" + CalendlyPeer.TOKEN,
                "api.client=" + CalendlyPeer.CLIENT,
                "api.secret=" + CalendlyPeer.SECRET,
                "api.token_file=" + tokenFile,
                "api.certificate=" + certificate,
                "api.start=" + start,
                "api.end=" + end,
                "api.incremental=" + incremental,
                "api.lookback=" + lookback,
                "api.page_size=" + size,
                "api.pages=" + pages,
                "api.bytes=" + bytes,
                "api.read_timeout=" + readTimeout,
                "api.deadline=" + deadline,
                "api.attempts=" + attempts
            };
        }
    }

    /**
     * Hosts normal packaged plugins and independent Java TLS fixtures in one owned worker.
     *
     * <p>DNS and certificate trust overrides apply only to this integration-test container.
     */
    private static final class Engine extends SeaTunnelContainer {
        private static final String PEER = "/tmp/calendly-peer";
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
            container.withExtraHost(CalendlyPeer.HOST, "127.0.0.1");
            container.withExtraHost(CalendlyPeer.AUTH, "127.0.0.1");
            container.withExtraHost("calendly-trap.invalid", "127.0.0.1");
            for (String name :
                    List.of(
                            "trusted.p12",
                            "wrong-name.p12",
                            "trusted.pem",
                            "untrusted.pem",
                            "wrong-name.pem")) {
                container.withCopyFileToContainer(
                        MountableFile.forHostPath(fixtures.resolve(name)), PEER + "/" + name);
            }
            try {
                stage(container, CalendlyPeer.class);
                stage(container, CalendlyRecoveryProbe.class);
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
                throw new IOException("Cannot stage the Java Calendly fixture", failure);
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
                    CalendlyPeer.class.getName(),
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
                    CalendlyRecoveryProbe.class.getName(),
                    trust,
                    mode);
        }

        private void install(Path file, String name) {
            server.copyFileToContainer(MountableFile.forHostPath(file), PEER + "/" + name);
        }

        private String store(Path file, String name, int mode) throws Exception {
            success(server.execInContainer("mkdir", "-p", "-m", "700", PEER + "/private"));
            String target = PEER + "/private/" + name + ".json";
            server.copyFileToContainer(MountableFile.forHostPath(file, mode), target);
            return target;
        }

        private String read(String path) throws Exception {
            Container.ExecResult result = server.execInContainer("cat", path);
            success(result);
            return result.getStdout();
        }

        private String mode(String path) throws Exception {
            Container.ExecResult result = server.execInContainer("stat", "-c", "%a", path);
            success(result);
            return result.getStdout().trim();
        }

        private void link(String target, String alias, boolean symbolic) throws Exception {
            success(
                    symbolic
                            ? server.execInContainer("ln", "-s", target, alias)
                            : server.execInContainer("ln", target, alias));
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
            String path = "/tmp/seatunnel_mnt/calendly-" + name;
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
