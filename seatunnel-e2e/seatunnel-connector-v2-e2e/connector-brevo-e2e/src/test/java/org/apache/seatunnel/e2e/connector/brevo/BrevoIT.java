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
package org.apache.seatunnel.e2e.connector.brevo;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.brevo.BrevoRecoveryProbe;
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
 * Exercises all export shapes and recovery through a Java 11 Zeta worker and HTTPS.
 *
 * <p>The request matrix is a literal provider-contract fixture independent of production catalogs.
 * Transactional LocalFile rows are compared as multisets, preserving duplicate event observations.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class BrevoIT extends TestSuiteBase {
    private static final ObjectMapper JSON =
            new ObjectMapper()
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    private static final String PARENT = "9007199254740993";
    private static final String START = "2024-01-01T00:00:00Z";
    private static final String END = "2024-01-01T00:00:01Z";
    private static final String ORIGINAL_TIME = "2024-01-01T08:00:00+08:00";
    private static final String EXTRA =
            ",\"extra\":{\"unicode\":\"数据🌏\",\"null\":null,\"array\":[false,null,[1,2]],\"integer\":9223372036854775808,\"decimal\":123456789012345678.12345678900}";
    @TempDir static Path directory;
    private final ExecutorService tasks = Executors.newCachedThreadPool();
    private Engine engine;
    private Future<Container.ExecResult> peer;
    private String active;

    @BeforeAll
    void startsOnlyTheOwnedEngineAndItsExplicitTlsTrust() throws Exception {
        identity("trusted");
        identity("untrusted");
        engine = new Engine(directory);
        engine.startUp();
        peer = tasks.submit(engine::peer);
        Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> engine.exists("ready"));
    }

    @AfterEach
    void rejectsUnexpectedOrCredentialBearingTraffic() throws Exception {
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
    void exportsAll29ResourcesAndBothAliasesWithExactFourColumnPayloads() throws Exception {
        List<Contract> contracts = contracts();
        assertEquals(29, contracts.size());
        for (Contract contract : contracts) {
            singleExport(contract, contract.name);
        }
        singleExport(named("emailCampaigns"), "campaigns");
        singleExport(named("email_templates"), "templates");
    }

    private void singleExport(Contract contract, String selection) throws Exception {
        String name = "catalog-" + selection;
        String record = record(contract, ORIGINAL_TIME, "1");
        List<ObjectNode> steps = new ArrayList<>();
        if (contract.nested()) {
            steps.add(
                    get(
                            contract.parentPath,
                            page(50, "offset", 0),
                            envelope(contract.parentEnvelope, "{\"id\":" + PARENT + "}")));
        }
        Settings settings = new Settings(name, selection);
        settings.unpaged = contract.paging.equals("none");
        settings.bounded = contract.name.endsWith("email_events");
        steps.add(
                get(
                        contract.path.replace("{parent}", PARENT),
                        query(contract, settings, 0),
                        envelope(contract.envelope, record)));
        if (contract.name.equals("companies") || contract.name.equals("crm_deals")) {
            steps.add(
                    get(contract.path, query(contract, settings, 1), envelope(contract.envelope)));
        }
        program(name, steps);
        success(run(settings));
        ObjectNode payload = (ObjectNode) JSON.readTree(record);
        String id = payload.path(contract.identity).asText();
        if (contract.nested()) {
            payload.put(contract.parentField, Long.parseLong(PARENT));
        }
        if (contract.time.startsWith("attributes.")) {
            payload.set(
                    "last_updated_at", payload.path("attributes").get(contract.time.substring(11)));
        }
        assertRows(
                name,
                List.of(
                        expected(
                                id,
                                contract.nested() ? PARENT : null,
                                contract.time.equals("-") ? null : START,
                                payload)));
        consumed(name, steps.size());
        assertTrue(
                engine.rows(name).get(0).contains("123456789012345678.12345678900"),
                "decimal scale must survive the STRING data field");
    }

    @Test
    void advancesRawOffsetsAcrossEntirelyFilteredPagesAndKeepsInclusiveBoundaries()
            throws Exception {
        Contract contract = named("contacts");
        Settings settings = new Settings("raw-filter", "contacts");
        settings.bounded = true;
        String before = record(contract, "2023-12-31T23:59:59.999Z", "1");
        String older = record(contract, "2023-12-31T23:59:59Z", "2");
        String first = record(contract, START, "3");
        String last = record(contract, END, "4");
        program(
                settings.name,
                List.of(
                        get(
                                contract.path,
                                query(contract, settings, 0),
                                envelope("contacts", before, older)),
                        get(
                                contract.path,
                                query(contract, settings, 2),
                                envelope("contacts", first, last)),
                        get(contract.path, query(contract, settings, 4), envelope("contacts"))));
        success(run(settings));
        assertRows(
                settings.name,
                List.of(
                        expected("3", null, START, JSON.readTree(first)),
                        expected("4", null, END, JSON.readTree(last))));
        consumed(settings.name, 3);
    }

    @Test
    void probesShortCompanyAndDealPagesWithoutAssumingTheirUndocumentedCap() throws Exception {
        for (String resource : List.of("companies", "crm_deals")) {
            Contract contract = named(resource);
            Settings settings = new Settings("short-" + resource, resource);
            String first = record(contract, START, "1"), second = record(contract, END, "2");
            program(
                    settings.name,
                    List.of(
                            get(
                                    contract.path,
                                    query(contract, settings, 0),
                                    envelope("items", first)),
                            get(
                                    contract.path,
                                    query(contract, settings, 1),
                                    envelope("items", second)),
                            get(contract.path, query(contract, settings, 2), envelope("items"))));
            success(run(settings));
            List<JsonNode> rows = new ArrayList<>();
            for (String value : List.of(first, second)) {
                ObjectNode payload = (ObjectNode) JSON.readTree(value);
                payload.set(
                        "last_updated_at",
                        payload.path("attributes").get(contract.time.substring(11)));
                rows.add(
                        expected(
                                payload.path("id").asText(),
                                null,
                                value.equals(first) ? START : END,
                                payload));
            }
            assertRows(settings.name, rows);
            consumed(settings.name, 3);
        }
    }

    @Test
    void scansEveryParentAndResetsChildOffsetsWithoutFilteringOldParents() throws Exception {
        String name = "all-parents";
        String first = "{\"id\":101}", second = "{\"id\":102}";
        program(
                name,
                List.of(
                        get(
                                "/contacts/folders",
                                page(50, "offset", 0),
                                envelope(
                                        "folders",
                                        "{\"id\":701,\"createdAt\":\"1999-01-01T00:00:00Z\"}",
                                        "{\"id\":702}")),
                        get(
                                "/contacts/folders/701/lists",
                                page(2, "offset", 0),
                                envelope("lists", first)),
                        get(
                                "/contacts/folders/702/lists",
                                page(2, "offset", 0),
                                envelope("lists", second))));
        success(run(new Settings(name, "contacts_folders_lists")));
        assertRows(
                name,
                List.of(
                        expected(
                                "101", "701", null, JSON.readTree("{\"id\":101,\"folderId\":701}")),
                        expected(
                                "102",
                                "702",
                                null,
                                JSON.readTree("{\"id\":102,\"folderId\":702}"))));
        consumed(name, 3);
    }

    @Test
    void normalizesCrmEpochSecondsMillisecondsAndIsoWithoutPushingSentOrDueFilters()
            throws Exception {
        for (String resource :
                List.of("crm_tasks", "crm_notes", "emailCampaigns", "smsCampaigns")) {
            Contract contract = named(resource);
            Settings settings = new Settings("local-time-" + resource, resource);
            settings.bounded = true;
            String first = record(contract, START, "1"), last = record(contract, END, "2");
            if (resource.startsWith("crm_")) {
                first = first.replace("\"" + START + "\"", "1704067200");
                last = last.replace("\"" + END + "\"", "\"1704067201000\"");
            }
            program(
                    settings.name,
                    List.of(
                            get(
                                    contract.path,
                                    page(2, "offset", 0),
                                    envelope(contract.envelope, first, last)),
                            get(contract.path, page(2, "offset", 2), envelope(contract.envelope))));
            success(run(settings));
            assertRows(
                    settings.name,
                    List.of(
                            expected(
                                    JSON.readTree(first).path(contract.identity).asText(),
                                    null,
                                    START,
                                    JSON.readTree(first)),
                            expected(
                                    JSON.readTree(last).path(contract.identity).asText(),
                                    null,
                                    END,
                                    JSON.readTree(last))));
            consumed(settings.name, 2);
        }
    }

    @Test
    void partitionsBothEventWindowsAndPreservesDuplicateMessageObservations() throws Exception {
        for (String resource : List.of("inbound_email_events", "transactional_email_events")) {
            Contract contract = named(resource);
            Settings settings = new Settings("windows-" + resource, resource);
            settings.bounded = true;
            settings.end = "2024-01-31T00:00:00Z";
            String first = record(contract, START, "1"), last = record(contract, settings.end, "2");
            String firstQuery =
                    page(2, "offset", 0)
                            + (resource.startsWith("inbound")
                                    ? "&startDate="
                                            + enc("2024-01-01T00:00:00.000Z")
                                            + "&endDate="
                                            + enc("2024-01-30T23:59:59.999Z")
                                    : "&startDate=2024-01-01&endDate=2024-01-30");
            String secondQuery =
                    page(2, "offset", 0)
                            + (resource.startsWith("inbound")
                                    ? "&startDate="
                                            + enc("2024-01-31T00:00:00.000Z")
                                            + "&endDate="
                                            + enc("2024-01-31T00:00:00.000Z")
                                    : "&startDate=2024-01-31&endDate=2024-01-31");
            List<ObjectNode> steps = new ArrayList<>();
            List<JsonNode> rows = new ArrayList<>();
            if (resource.startsWith("transactional")) {
                steps.add(get(contract.path, firstQuery, envelope("events", first, first)));
                steps.add(
                        get(
                                contract.path,
                                firstQuery.replace("offset=0", "offset=2"),
                                envelope("events", first, first)));
                steps.add(
                        get(
                                contract.path,
                                firstQuery.replace("offset=0", "offset=4"),
                                envelope("events")));
                for (int i = 0; i < 4; i++) {
                    rows.add(
                            expected(
                                    JSON.readTree(first).path(contract.identity).asText(),
                                    null,
                                    START,
                                    JSON.readTree(first)));
                }
            } else {
                steps.add(get(contract.path, firstQuery, envelope("events", first)));
                rows.add(
                        expected(
                                JSON.readTree(first).path(contract.identity).asText(),
                                null,
                                START,
                                JSON.readTree(first)));
            }
            steps.add(get(contract.path, secondQuery, envelope("events", last)));
            rows.add(
                    expected(
                            JSON.readTree(last).path(contract.identity).asText(),
                            null,
                            settings.end,
                            JSON.readTree(last)));
            program(settings.name, steps);
            success(run(settings));
            assertRows(settings.name, rows);
            consumed(settings.name, steps.size());
        }
    }

    @Test
    void rejectsTruncatedDomainsAndBoundedMissingTimesInsteadOfSilentlyLosingData()
            throws Exception {
        Settings domains = new Settings("domains-truncated", "domains");
        domains.unpaged = true;
        fails(
                domains,
                List.of(
                        get(
                                "/senders/domains",
                                "",
                                "{\"domains\":[{\"id\":1}],\"current_page\":1,\"total_pages\":2}")),
                "additional pages");
        Settings missing = new Settings("missing-time", "contacts");
        missing.bounded = true;
        fails(
                missing,
                List.of(
                        get(
                                "/contacts",
                                query(named("contacts"), missing, 0),
                                envelope("contacts", "{\"id\":1}"))),
                "time");
    }

    @Test
    void usesRawCountMetadataAndFailsRepeatedPagesOrExhaustedBudgets() throws Exception {
        String one = "{\"id\":1}";
        String two = "{\"id\":2}";
        String name = "count-short";
        program(
                name,
                List.of(
                        get(
                                "/processes",
                                page(2, "offset", 0),
                                "{\"processes\":[" + one + "],\"count\":2}"),
                        get(
                                "/processes",
                                page(2, "offset", 1),
                                "{\"processes\":[" + two + "],\"count\":2}")));
        success(run(new Settings(name, "processes")));
        assertRows(
                name,
                List.of(
                        expected("1", null, null, JSON.readTree(one)),
                        expected("2", null, null, JSON.readTree(two))));
        consumed(name, 2);
        String repeated = envelope("processes", one, two);
        fails(
                new Settings("repeated", "processes"),
                List.of(
                        get("/processes", page(2, "offset", 0), repeated),
                        get("/processes", page(2, "offset", 2), repeated)),
                "repeated page");
        Settings exhausted = new Settings("max-pages", "processes");
        exhausted.maxPages = 1;
        fails(exhausted, List.of(get("/processes", page(2, "offset", 0), repeated)), "max_pages");
    }

    @Test
    void retriesThrottlingUsingRelativeResetAndNeverExposesProviderSecrets() throws Exception {
        Settings settings = new Settings("relative-reset", "processes");
        settings.attempts = 2;
        program(
                settings.name,
                List.of(
                        get("/processes", page(2, "offset", 0), "private-provider-message")
                                .put("status", 429)
                                .put("Retry-After", "0")
                                .put("x-sib-ratelimit-reset", "1"),
                        get("/processes", page(2, "offset", 0), envelope("processes"))));
        success(run(settings));
        consumed(settings.name, 2);
        List<String> times = engine.lines(settings.name + ".times");
        assertTrue(Long.parseLong(times.get(1)) - Long.parseLong(times.get(0)) >= 1000L);
        assertRows(settings.name, List.of());
        for (int status : List.of(401, 403, 429)) {
            fails(
                    new Settings("http-" + status, "processes"),
                    List.of(
                            get(
                                            "/processes",
                                            page(2, "offset", 0),
                                            "private-provider-message " + BrevoPeer.TOKEN)
                                    .put("status", status)),
                    "HTTP " + status);
        }
        fails(
                new Settings("logical-error", "processes"),
                List.of(
                        get(
                                "/processes",
                                page(2, "offset", 0),
                                "{\"code\":\"unauthorized\",\"message\":\"private-provider-message\"}")),
                "error object");
    }

    @Test
    void rejectsRedirectsWrongTlsMalformedJsonAndOversizedBodies() throws Exception {
        fails(
                new Settings("redirect", "processes"),
                List.of(
                        get("/processes", page(2, "offset", 0), "")
                                .put("mode", "redirect")
                                .put("status", 302)),
                "302");
        Settings wrong = new Settings("wrong-trust", "processes");
        wrong.certificate = Engine.PEER + "/untrusted.pem";
        fails(wrong, List.of(), "TLS");
        for (String body :
                List.of(
                        "{\"processes\":[],\"processes\":[]}",
                        "{\"processes\":[]}{}",
                        "{\"processes\":[null]}",
                        "{\"processes\":\"wrong\"}")) {
            fails(
                    new Settings("bad-json-" + Math.abs(body.hashCode()), "processes"),
                    List.of(get("/processes", page(2, "offset", 0), body)),
                    "Brevo");
        }
        Settings large = new Settings("oversized", "processes");
        large.bytes = 1024;
        fails(
                large,
                List.of(
                        get(
                                "/processes",
                                page(2, "offset", 0),
                                envelope(
                                        "processes",
                                        "{\"id\":1,\"value\":\"" + "x".repeat(1500) + "\"}"))),
                "max_response_bytes");
        String deep =
                "{\"processes\":[{\"id\":1,\"value\":"
                        + "[".repeat(90)
                        + "null"
                        + "]".repeat(90)
                        + "}]}";
        fails(
                new Settings("depth", "processes"),
                List.of(get("/processes", page(2, "offset", 0), deep)),
                "JSON");
    }

    @Test
    void readsGzipAndBoundsItsExpandedPayloadAndInvalidUtf8() throws Exception {
        String name = "gzip-success";
        String body = "{\"id\":1,\"text\":\"数据🌏\"}";
        program(
                name,
                List.of(
                        get("/processes", page(2, "offset", 0), envelope("processes", body))
                                .put("mode", "gzip")));
        success(run(new Settings(name, "processes")));
        consumed(name, 1);
        assertRows(name, List.of(expected("1", null, null, JSON.readTree(body))));
        Settings expanded = new Settings("gzip-expanded", "processes");
        expanded.bytes = 1024;
        fails(
                expanded,
                List.of(
                        get(
                                        "/processes",
                                        page(2, "offset", 0),
                                        envelope(
                                                "processes",
                                                "{\"id\":1,\"text\":\"" + "x".repeat(8000) + "\"}"))
                                .put("mode", "gzip")),
                "max_response_bytes");
        byte[] prefix = "{\"processes\":[{\"id\":1,\"text\":\"".getBytes(StandardCharsets.UTF_8);
        byte[] suffix = "\"}]}".getBytes(StandardCharsets.UTF_8);
        byte[] invalid = new byte[prefix.length + suffix.length + 2];
        System.arraycopy(prefix, 0, invalid, 0, prefix.length);
        invalid[prefix.length] = (byte) 0xc3;
        invalid[prefix.length + 1] = (byte) 0x28;
        System.arraycopy(suffix, 0, invalid, prefix.length + 2, suffix.length);
        fails(
                new Settings("invalid-utf8", "processes"),
                List.of(
                        get("/processes", page(2, "offset", 0), "")
                                .put("bytes", Base64.getEncoder().encodeToString(invalid))),
                "UTF-8");
    }

    @Test
    void preservesOptionalMissingIdentitiesAndFullScanNullTimes() throws Exception {
        String name = "nullable-fields";
        String first = "{\"id\":null,\"modifiedAt\":null,\"unknown\":{\"nested\":true}}";
        String second = "{\"extra\":[null,false]}";
        program(
                name,
                List.of(
                        get("/contacts", page(2, "offset", 0), envelope("contacts", first, second)),
                        get("/contacts", page(2, "offset", 2), envelope("contacts"))));
        success(run(new Settings(name, "contacts")));
        assertRows(
                name,
                List.of(
                        expected(null, null, null, JSON.readTree(first)),
                        expected(null, null, null, JSON.readTree(second))));
        consumed(name, 2);
    }

    @Test
    void rejectsInvalidOptionsBeforeAnyProviderRequest() throws Exception {
        Settings size = new Settings("invalid-size", "contacts_folders");
        size.pageSize = 51;
        fails(size, List.of(), "page_size");
        Settings noTime = new Settings("invalid-nondated", "processes");
        noTime.bounded = true;
        fails(noTime, List.of(), "date options");
        Settings reversed = new Settings("invalid-range", "contacts");
        reversed.bounded = true;
        reversed.end = "2023-12-31T23:59:59Z";
        fails(reversed, List.of(), "start_date");
        Settings subMillis = new Settings("invalid-submillis", "contacts");
        subMillis.bounded = true;
        subMillis.end = "2024-01-01T00:00:00.000001Z";
        fails(subMillis, List.of(), "end_date");
    }

    @Test
    void boundsTheWholeTricklingRequestAndCancelsActiveSockets() throws Exception {
        Settings deadline = new Settings("trickle-deadline", "processes");
        deadline.deadline = 2000;
        deadline.readTimeout = 10000;
        fails(
                deadline,
                List.of(
                        get("/processes", page(2, "offset", 0), envelope("processes"))
                                .put("mode", "trickle")),
                "deadline");
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> engine.exists(deadline.name + ".disconnect"));
        Settings settings = new Settings("cancel", "processes");
        program(
                settings.name,
                List.of(
                        get("/processes", page(2, "offset", 0), envelope("processes"))
                                .put("mode", "trickle")));
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(
                        () -> engine.executeJob("/brevo-full.conf", job, settings.variables()));
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
    void restoresAnActualSavepointAndValidatesTheParentBeforeReplayingItsPendingChild()
            throws Exception {
        Settings settings = new Settings("parent-savepoint", "contacts_folders_lists");
        program(
                settings.name,
                List.of(
                        get(
                                "/contacts/folders",
                                page(50, "offset", 0),
                                envelope("folders", "{\"id\":701}")),
                        get(
                                        "/contacts/folders/701/lists",
                                        page(2, "offset", 0),
                                        envelope("lists", "{\"id\":101}"))
                                .put("mode", "partial")));
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(
                        () -> engine.executeJob("/brevo-full.conf", job, settings.variables()));
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
        consumed(settings.name, 2);
        program(
                "parent-restored",
                List.of(
                        get(
                                "/contacts/folders",
                                page(50, "offset", 0),
                                envelope("folders", "{\"id\":701}")),
                        get(
                                "/contacts/folders/701/lists",
                                page(2, "offset", 0),
                                envelope("lists", "{\"id\":101}"))));
        success(engine.restoreJob("/brevo-full.conf", job, settings.variables()));
        assertRows(
                settings.name,
                List.of(
                        expected(
                                "101",
                                "701",
                                null,
                                JSON.readTree("{\"id\":101,\"folderId\":701}"))));
        consumed("parent-restored", 2);
    }

    @Test
    void restoresTheRawNextPageWithoutRepeatingPreviouslyCommittedRows() throws Exception {
        Settings settings = new Settings("page-savepoint", "processes");
        program(
                settings.name,
                List.of(
                        get(
                                "/processes",
                                page(2, "offset", 0),
                                envelope("processes", "{\"id\":1}", "{\"id\":2}")),
                        get("/processes", page(2, "offset", 2), envelope("processes", "{\"id\":3}"))
                                .put("mode", "partial")));
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(
                        () -> engine.executeJob("/brevo-full.conf", job, settings.variables()));
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
        consumed(settings.name, 2);
        program(
                "page-restored",
                List.of(
                        get(
                                "/processes",
                                page(2, "offset", 2),
                                envelope("processes", "{\"id\":3}"))));
        success(engine.restoreJob("/brevo-full.conf", job, settings.variables()));
        assertRows(
                settings.name,
                List.of(
                        expected("1", null, null, JSON.readTree("{\"id\":1}")),
                        expected("2", null, null, JSON.readTree("{\"id\":2}")),
                        expected("3", null, null, JSON.readTree("{\"id\":3}"))));
        consumed("page-restored", 1);
    }

    @Test
    void restoresATerminalPipelineWithoutRepeatingRequestsOrCommittedRows() throws Exception {
        Settings settings = new Settings("terminal-original", "processes");
        List<JsonNode> expected = List.of(expected("1", null, null, JSON.readTree("{\"id\":1}")));
        program(
                settings.name,
                List.of(
                        get(
                                "/processes",
                                page(2, "offset", 0),
                                envelope("processes", "{\"id\":1}"))));
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(
                        () -> engine.executeJob("/brevo-terminal.conf", job, settings.variables()));
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
                        () -> engine.restoreJob("/brevo-terminal.conf", job, settings.variables()));
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
    void serializesTheExact16RowPositionRejectsChangedPagesAndFinishesQuietly() throws Exception {
        List<String> records = new ArrayList<>();
        for (int index = 0; index < 17; index++) {
            records.add("{\"id\":" + (1000 + index) + "}");
        }
        String page = envelope("processes", records.toArray(new String[0]));
        for (String mode : List.of("resume", "changed")) {
            String name = "reader-" + mode;
            String replay = mode.equals("changed") ? page.replace("1016", "2016") : page;
            program(
                    name,
                    List.of(
                            get("/processes", page(17, "offset", 0), page),
                            get("/processes", page(17, "offset", 0), replay),
                            get("/processes", page(17, "offset", 17), envelope("processes"))));
            success(engine.probe(mode));
            consumed(name, mode.equals("resume") ? 3 : 2);
        }
        program("reader-zero", List.of());
        success(engine.probe("zero"));
        consumed("reader-zero", 0);
    }

    private static List<Contract> contracts() throws IOException {
        List<Contract> values = new ArrayList<>();
        try (InputStream input = BrevoIT.class.getResourceAsStream("/brevo-contracts.tsv")) {
            assertTrue(input != null, "independent contract matrix missing");
            for (String line :
                    new String(input.readAllBytes(), StandardCharsets.UTF_8).split("\\R")) {
                if (!line.isBlank() && !line.startsWith("#")) {
                    values.add(new Contract(line.split("\\t", -1)));
                }
            }
        }
        return values;
    }

    private static Contract named(String name) throws IOException {
        return contracts().stream()
                .filter(value -> value.name.equals(name))
                .findFirst()
                .orElseThrow();
    }

    private static String record(Contract contract, String time, String index) throws Exception {
        String id;
        switch (contract.identity) {
            case "email":
                id = "user" + index + "@example.invalid";
                break;
            case "uuid":
                id = "00000000-0000-4000-8000-" + String.format("%012d", Integer.parseInt(index));
                break;
            case "messageId":
                id = "message-" + index + "@example.invalid";
                break;
            case "name":
            case "internalName":
            case "pipeline":
                id = "field-" + index;
                break;
            default:
                id = index;
        }
        String identity =
                contract.identity.equals("id") || contract.identity.equals("organization_id")
                        ? index
                        : JSON.writeValueAsString(id);
        String value = "{\"" + contract.identity + "\":" + identity;
        if (contract.time.startsWith("attributes.")) {
            value += ",\"attributes\":{\"" + contract.time.substring(11) + "\":\"" + time + "\"}";
        } else if (!contract.time.equals("-")) {
            value += ",\"" + contract.time + "\":\"" + time + "\"";
        }
        return value + EXTRA + "}";
    }

    private static String query(Contract contract, Settings settings, int position) {
        String value =
                contract.paging.equals("none")
                        ? ""
                        : page(
                                2,
                                contract.paging,
                                contract.paging.equals("page") ? position + 1 : position);
        if (!settings.bounded) {
            return value;
        }
        if (List.of(
                        "contacts",
                        "contacts_lists_contacts",
                        "companies",
                        "crm_deals",
                        "products",
                        "categories")
                .contains(contract.name)) {
            return value + "&modifiedSince=" + enc("2023-12-31T23:59:59.999Z");
        }
        if (contract.name.equals("inbound_email_events")) {
            return value
                    + "&startDate="
                    + enc("2024-01-01T00:00:00.000Z")
                    + "&endDate="
                    + enc("2024-01-01T00:00:01.000Z");
        }
        if (contract.name.equals("transactional_email_events")) {
            return value + "&startDate=2024-01-01&endDate=2024-01-01";
        }
        return value;
    }

    private static String page(int limit, String kind, int position) {
        return "limit=" + limit + "&" + kind + "=" + position + "&sort=asc";
    }

    private static String enc(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private static String envelope(String key, String... records) {
        String array = "[" + String.join(",", records) + "]";
        return key.equals("[]")
                ? array
                : key.equals("$") ? records[0] : "{\"" + key + "\":" + array + "}";
    }

    private static ObjectNode get(String path, String query, String body) {
        return JSON.createObjectNode()
                .put("path", "/v3" + path)
                .put("query", query)
                .put("response", body);
    }

    private static ObjectNode expected(String id, String parent, String time, JsonNode payload) {
        ObjectNode row = JSON.createObjectNode();
        row.put("entity_id", id).put("parent_id", parent).put("modified_at", time);
        row.set("data", payload);
        return row;
    }

    private void assertRows(String name, List<JsonNode> expected) throws Exception {
        Map<JsonNode, Integer> actual = new HashMap<>();
        for (String line : engine.rows(name)) {
            JsonNode parsed = JSON.readTree(line);
            assertTrue(
                    parsed.isObject() && parsed.size() == 4, "exact four-column schema: " + line);
            assertTrue(parsed.path("data").isTextual(), "data must remain JSON stored in STRING");
            ObjectNode row = (ObjectNode) parsed;
            row.set("data", JSON.readTree(parsed.path("data").textValue()));
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

    private void consumed(String name, int count) throws Exception {
        assertFalse(engine.exists(name + ".invalid"), name);
        assertEquals(count, engine.lines(name + ".requests").size(), name);
    }

    private Container.ExecResult run(Settings settings) throws Exception {
        return engine.executeJob(
                "/brevo-"
                        + (settings.unpaged ? "unpaged" : settings.bounded ? "bounded" : "full")
                        + ".conf",
                jobId(),
                settings.variables());
    }

    private void fails(Settings settings, List<ObjectNode> steps, String reason) throws Exception {
        program(settings.name, steps);
        Container.ExecResult result = run(settings);
        String output = result.getStdout() + result.getStderr();
        assertNotEquals(0, result.getExitCode(), output);
        assertTrue(output.contains(reason), output);
        assertFalse(output.contains(BrevoPeer.TOKEN), "API key leaked");
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

    private static void identity(String name) throws Exception {
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
                                "CN=" + BrevoPeer.HOST,
                                "-ext",
                                "SAN=dns:" + BrevoPeer.HOST,
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                store.toString(),
                                "-storepass",
                                BrevoPeer.PASSWORD,
                                "-keypass",
                                BrevoPeer.PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String output = new String(keytool.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, keytool.waitFor(), output);
        KeyStore keys = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(store)) {
            keys.load(input, BrevoPeer.PASSWORD.toCharArray());
        }
        Files.writeString(
                directory.resolve(name + ".pem"),
                "-----BEGIN CERTIFICATE-----\n"
                        + Base64.getMimeEncoder(64, new byte[] {'\n'})
                                .encodeToString(keys.getCertificate("api").getEncoded())
                        + "\n-----END CERTIFICATE-----\n");
    }

    /**
     * Holds independent documented examples rather than importing production resource definitions.
     *
     * <p>All request shapes, envelope names and identity fields originate in the checked-in matrix.
     */
    private static final class Contract {
        private final String name,
                path,
                envelope,
                paging,
                identity,
                time,
                parentPath,
                parentEnvelope,
                parentField;

        private Contract(String[] fields) {
            assertEquals(10, fields.length);
            name = fields[0];
            path = fields[1];
            envelope = fields[2];
            paging = fields[3];
            identity = fields[5];
            time = fields[6];
            parentPath = fields[7];
            parentEnvelope = fields[8];
            parentField = fields[9];
        }

        private boolean nested() {
            return !parentPath.equals("-");
        }
    }

    /**
     * Supplies ordinary entry-point variables with isolated output and finite request budgets.
     *
     * <p>Only the option whose failure boundary is under test is changed by each scenario.
     */
    private static final class Settings {
        private final String name, resource;
        private String end = END;
        private String certificate = Engine.PEER + "/trusted.pem";
        private boolean unpaged, bounded;
        private int pageSize = 2,
                maxPages = 100,
                bytes = 8388608,
                readTimeout = 120000,
                deadline = 180000,
                attempts = 1;

        private Settings(String name, String resource) {
            this.name = name;
            this.resource = resource;
        }

        private String[] variables() {
            return new String[] {
                "api.output=" + name,
                "api.resource=" + resource,
                "api.token=" + BrevoPeer.TOKEN,
                "api.certificate=" + certificate,
                "api.page_size=" + pageSize,
                "api.max_pages=" + maxPages,
                "api.max_bytes=" + bytes,
                "api.read_timeout=" + readTimeout,
                "api.deadline=" + deadline,
                "api.attempts=" + attempts,
                "api.start=" + START,
                "api.end=" + end
            };
        }
    }

    /**
     * Hosts normal packaged plugins and independent Java TLS fixtures in one owned worker.
     *
     * <p>DNS and certificate trust overrides apply only to this integration-test container.
     */
    private static final class Engine extends SeaTunnelContainer {
        private static final String PEER = "/tmp/brevo-peer";
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
            container.withExtraHost(BrevoPeer.HOST, "127.0.0.1");
            container.withExtraHost("brevo-trap.invalid", "127.0.0.1");
            for (String name : List.of("trusted.p12", "trusted.pem", "untrusted.pem")) {
                container.withCopyFileToContainer(
                        MountableFile.forHostPath(fixtures.resolve(name)), PEER + "/" + name);
            }
            try {
                stage(container, BrevoPeer.class);
                stage(container, BrevoRecoveryProbe.class);
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
                throw new IOException("Cannot stage the Java Brevo fixture", failure);
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
                    BrevoPeer.class.getName(),
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
                    BrevoRecoveryProbe.class.getName(),
                    PEER + "/trusted.pem",
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
            String path = "/tmp/seatunnel_mnt/brevo-" + name;
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
