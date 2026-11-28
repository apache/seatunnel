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
package org.apache.seatunnel.e2e.connector.callrail;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.callrail.CallRailRecoveryProbe;
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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs CallRail through a real SeaTunnel Zeta engine and an isolated Java TLS account.
 *
 * <p>Tests compare page numbers and bounds in captured GET requests and inspect LocalFile's
 * committed JSON rows. The provider peer never calls the real billable API.
 */
class CallRailIT extends TestSuiteBase {
    private static final ObjectMapper JSON =
            new ObjectMapper()
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    private static final String TOKEN = "callrail-fixture-private+/=?";
    private static final String HIDDEN_MESSAGE = "private-provider-response-token";
    private static final String HOST = "callrail-peer";
    private static final String PEM = "/tmp/callrail.pem";
    private static final String OTHER_PEM = "/tmp/unrelated-callrail.pem";
    private static final String ENDPOINT = "https://" + HOST + ":18443";
    private static final String START = "2025-01-01T00:00:00.100000001Z";
    private static final String END = "2025-04-15T00:00:00.123456789Z";
    private static final String ACCOUNT = "227799611";
    private static final String MASKED = "ACCab09c7d401";
    private static final String PREFIX = "/v3/a/" + ACCOUNT;
    private static final String CALLS = PREFIX + "/calls.json";
    private static final String CALLS_NEXT = PREFIX + "/calls";
    private static final String CONVERSATIONS = PREFIX + "/text-messages.json";
    private static final String USERS = PREFIX + "/users.json";
    private static final String COMPANIES = PREFIX + "/companies.json";
    private static final String IN_WINDOW = "2025-01-15T12:30:30Z";
    private static final List<String> ALL = List.of("calls", "conversations", "users", "companies");
    private static final String CALL_FIELDS =
            String.join(
                    ",",
                    "agent_email",
                    "call_highlights",
                    "call_type",
                    "campaign",
                    "company_id",
                    "company_name",
                    "company_time_zone",
                    "created_at",
                    "device_type",
                    "fbclid",
                    "first_call",
                    "formatted_business_phone_number",
                    "formatted_call_type",
                    "formatted_customer_location",
                    "formatted_customer_name",
                    "formatted_customer_name_or_phone_number",
                    "formatted_customer_phone_number",
                    "formatted_duration",
                    "formatted_tracking_phone_number",
                    "formatted_tracking_source",
                    "formatted_value",
                    "ga",
                    "gclid",
                    "good_lead_call_id",
                    "good_lead_call_time",
                    "keypad_entries",
                    "keywords",
                    "keywords_spotted",
                    "landing_page_url",
                    "last_requested_url",
                    "lead_status",
                    "medium",
                    "milestones",
                    "msclkid",
                    "note",
                    "prior_calls",
                    "referrer_domain",
                    "referring_url",
                    "source",
                    "source_name",
                    "speaker_percent",
                    "tags",
                    "timeline_url",
                    "total_calls",
                    "tracker_id",
                    "utm_campaign",
                    "utm_content",
                    "utm_medium",
                    "utm_source",
                    "utm_term",
                    "value",
                    "waveforms");
    private static final String CONVERSATION_FIELDS =
            String.join(
                    ",",
                    "company_id",
                    "company_time_zone",
                    "current_tracker_id",
                    "current_tracking_number",
                    "customer_name",
                    "customer_phone_number",
                    "formatted_current_tracking_number",
                    "formatted_customer_name",
                    "formatted_customer_phone_number",
                    "formatted_initial_tracking_number",
                    "id",
                    "initial_tracker_id",
                    "initial_tracking_number",
                    "last_message_at",
                    "recent_messages",
                    "state");

    private final Engine engine = new Engine();
    private final ExecutorService submissions = Executors.newSingleThreadExecutor();
    private final HttpClient testControl =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private GenericContainer<?> peer;
    private Path certificate;

    /**
     * Provisions independent certificates, the protocol peer and the engine.
     *
     * <p>The unrelated certificate lets a negative test use a genuine trust failure with the
     * expected HTTPS endpoint still running.
     */
    @BeforeAll
    void boot(@TempDir Path testFiles) throws Exception {
        String runner = DockerClientFactory.instance().dockerHostIpAddress();
        assertTrue(runner.matches("[A-Za-z0-9.:-]+"));
        String runnerType = runner.matches("[0-9.]+") || runner.contains(":") ? "ip:" : "dns:";
        String create =
                "keytool -genkeypair -alias provider -storetype PKCS12 -storepass callrail-fixture "
                        + "-keyalg RSA -keysize 2048 -validity 30 -dname CN="
                        + HOST
                        + " -ext SAN=dns:"
                        + HOST
                        + ",dns:callrail-redirect-host,"
                        + runnerType
                        + runner
                        + " -keystore ";
        String export =
                "keytool -exportcert -rfc -alias provider -storepass callrail-fixture -keystore ";
        String entry =
                create
                        + "/tmp/provider.p12 && "
                        + export
                        + "/tmp/provider.p12 -file "
                        + PEM
                        + " && "
                        + create
                        + "/tmp/unrelated.p12 && "
                        + export
                        + "/tmp/unrelated.p12 -file "
                        + OTHER_PEM
                        + " && exec java --add-modules jdk.httpserver -cp /tmp "
                        + CallRailPeer.class.getName()
                        + " /tmp/provider.p12";
        String providerClass = "/" + CallRailPeer.class.getName().replace('.', '/') + ".class";
        byte[] bytecode;
        try (InputStream bits = CallRailPeer.class.getResourceAsStream(providerClass)) {
            if (bits == null) {
                throw new IllegalStateException("Missing packaged HTTPS fixture");
            }
            bytecode = bits.readAllBytes();
        }
        peer =
                new GenericContainer<>(DockerImageName.parse("eclipse-temurin:11-jdk"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST, "callrail-other-host", "callrail-redirect-host")
                        .withExposedPorts(18080, 18443)
                        .withCopyToContainer(Transferable.of(bytecode), "/tmp" + providerClass)
                        .withCommand("sh", "-c", entry)
                        .waitingFor(
                                Wait.forHttp("/requests")
                                        .forPort(18080)
                                        .withStartupTimeout(Duration.ofMinutes(2)));
        peer.start();
        engine.startUp();
        String testedCertificate = copyCertificate(PEM);
        copyCertificate(OTHER_PEM);
        certificate = Files.writeString(testFiles.resolve("fixture.pem"), testedCertificate);
    }

    /**
     * Stops both owned containers and async submissions after the suite.
     *
     * <p>The provider is still released if teardown of Zeta reports an error.
     */
    @AfterAll
    void stop() throws Exception {
        submissions.shutdownNow();
        try {
            engine.tearDown();
        } finally {
            if (peer != null) {
                peer.stop();
            }
        }
    }

    @Test
    void readsAllFourCollectionsWithExactAccountScopeAndProviderFields() throws Exception {
        String firstDay = "2025-01-01";
        String lastDay = "2025-01-31";
        Seen callFirst = timed(CALLS, 2, 0, true, firstDay, lastDay);
        Seen callSecond = nextCalls(2, 1, firstDay, lastDay);
        callSecond.query.put("action", "index");
        callSecond.query.put("agency_id", ACCOUNT);
        callSecond.query.put("api_sort[][start_time]", "desc");
        callSecond.query.put("api_sort[]", "start_time");
        callSecond.query.put("controller", "api/v3/calls");
        callSecond.query.put("format", "json");
        callSecond.query.put("no_one_to_many_associations", "true");
        callSecond.query.put("page", "1");
        Seen callThird = nextCalls(2, 2, firstDay, lastDay);

        ObjectNode a =
                record("CAL-a")
                        .put("start_time", "2024-12-31T19:00:00.100000001-05:00")
                        .put("customer_name", "访客 🧭")
                        .putNull("agent_email");
        a.put("recording", "https://callrail-redirect-host:18443/never-fetch");
        a.put("legacy_cookie_payload", "preserve-if-present");
        a.put("utma", "legacy-returned-by-provider");
        a.putObject("milestones").put("metadata", new BigInteger("9223372036854775808"));
        ObjectNode b =
                record("CAL-b")
                        .put("start_time", START)
                        .put("value", new BigDecimal("123456789012345678.123456789000"));
        ObjectNode older = record("CAL-before").put("start_time", "2025-01-01T00:00:00.100000000Z");
        ObjectNode upper = record("CAL-after").put("start_time", "2025-02-01T00:00:00Z");
        ObjectNode sms =
                record("CON-sms").put("last_message_at", IN_WINDOW).put("state", "archived");
        sms.putArray("recent_messages")
                .addObject()
                .put("content", "<p>保留原值</p>")
                .putArray("media_urls")
                .add("https://callrail-redirect-host:18443/never-fetch-mms");
        ObjectNode user =
                options()
                        .put("id", new BigInteger("9223372036854775809"))
                        .put("created_at", IN_WINDOW)
                        .putNull("email");
        ObjectNode company = record("COM-masked").put("status", "disabled").putNull("created_at");
        company.put("script_url", "https://callrail-redirect-host:18443/never-fetch-script");

        Seen smsFirst = timed(CONVERSATIONS, 2, 1, false, firstDay, lastDay);
        Seen smsSecond = timed(CONVERSATIONS, 2, 2, false, firstDay, lastDay);
        Seen smsThird = timed(CONVERSATIONS, 2, 3, false, firstDay, lastDay);
        reset(
                ok(CALLS, relative(rows(a, older), continuation(callSecond, true))),
                ok(CALLS_NEXT, relative(rows(), continuation(callThird, false))),
                ok(CALLS_NEXT, relative(rows(b, upper), null)),
                ok(CONVERSATIONS, numbered("conversations", 1, 2, 3, 1, rows())),
                ok(CONVERSATIONS, numbered("conversations", 2, 2, 3, 1, rows(sms))),
                ok(CONVERSATIONS, numbered("conversations", 3, 2, 3, 1, rows())),
                ok(USERS, numbered("users", 1, 2, 1, 1, rows(user))),
                ok(COMPANIES, numbered("companies", 1, 2, 1, 1, rows(company))));
        try (Job job = job(ALL, options().put("page_size", 2), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(
                    job,
                    List.of(
                            out("calls", "CAL-a", ACCOUNT, START, a),
                            out("calls", "CAL-b", ACCOUNT, START, b),
                            out("conversations", "CON-sms", ACCOUNT, IN_WINDOW, sms),
                            out("users", "9223372036854775809", ACCOUNT, IN_WINDOW, user),
                            out("companies", "COM-masked", ACCOUNT, null, company)));
        }
        assertTrace(
                callFirst,
                callSecond,
                callThird,
                smsFirst,
                smsSecond,
                smsThird,
                wire(USERS, 2, 1),
                wire(COMPANIES, 2, 1));
    }

    @Test
    void usesNonoverlappingHundredDayUtcSlicesAndTestsTheExclusiveUpperNanosecond()
            throws Exception {
        ObjectNode beforeEnd = record("before").put("start_time", "2025-04-10T23:59:59.999999999Z");
        ObjectNode sameBoundary = record("boundary").put("start_time", "2025-04-11T00:00:00Z");
        ObjectNode last = record("last").put("start_time", "2025-04-15T00:00:00.123456788Z");
        ObjectNode exactEnd = record("excluded").put("start_time", END);
        reset(
                ok(CALLS, numbered("calls", 1, 100, 1, 2, rows(beforeEnd, sameBoundary))),
                ok(CALLS, numbered("calls", 1, 100, 1, 3, rows(sameBoundary, last, exactEnd))),
                ok(CONVERSATIONS, numbered("conversations", 1, 100, 0, 0, rows())),
                ok(CONVERSATIONS, numbered("conversations", 1, 100, 1, 0, rows())));
        try (Job job =
                job(
                        List.of("calls", "conversations"),
                        options().put("calls_pagination", "offset").put("end_date", END),
                        false,
                        false)) {
            good(engine.executeJob(job.configName));
            assertOutput(
                    job,
                    List.of(
                            out(
                                    "calls",
                                    "before",
                                    ACCOUNT,
                                    beforeEnd.path("start_time").asText(),
                                    beforeEnd),
                            out(
                                    "calls",
                                    "boundary",
                                    ACCOUNT,
                                    sameBoundary.path("start_time").asText(),
                                    sameBoundary),
                            out("calls", "last", ACCOUNT, last.path("start_time").asText(), last)));
        }
        assertTrace(
                timed(CALLS, 100, 1, false, "2025-01-01", "2025-04-10"),
                timed(CALLS, 100, 1, false, "2025-04-11", "2025-04-15"),
                timed(CONVERSATIONS, 100, 1, false, "2025-01-01", "2025-04-10"),
                timed(CONVERSATIONS, 100, 1, false, "2025-04-11", "2025-04-15"));
    }

    @Test
    void keepsUniformLocalInventoryFilteringWithoutSendingDateParameters() throws Exception {
        ObjectNode createdLate = record("US-new").put("created_at", IN_WINDOW);
        ObjectNode createdEarly = record("US-old").put("created_at", "2024-10-01T00:00:00Z");
        ObjectNode missingDate = record("US-missing").put("name", "created_at unavailable");
        ObjectNode nullDate = record("COM-null").putNull("created_at").put("status", "disabled");
        reset(
                ok(USERS, numbered("users", 1, 2, 2, 3, rows(createdEarly))),
                ok(USERS, numbered("users", 2, 2, 2, 3, rows(createdLate, missingDate))),
                ok(COMPANIES, numbered("companies", 1, 2, 1, 1, rows(nullDate))));
        try (Job job =
                job(List.of("users", "companies"), options().put("page_size", 2), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(
                    job,
                    List.of(
                            out("users", "US-new", ACCOUNT, IN_WINDOW, createdLate),
                            out("users", "US-missing", ACCOUNT, null, missingDate),
                            out("companies", "COM-null", ACCOUNT, null, nullDate)));
        }
        assertTrace(wire(USERS, 2, 1), wire(USERS, 2, 2), wire(COMPANIES, 2, 1));
    }

    @Test
    void honorsResourceOrderAndSelectsNumberedCallsForAMaskedAccount() throws Exception {
        String companies = "/v3/a/" + MASKED + "/companies.json";
        String calls = "/v3/a/" + MASKED + "/calls.json";
        ObjectNode company = record("COM-one").putNull("created_at");
        reset(
                ok(companies, numbered("companies", 1, 250, 1, 1, rows(company))),
                ok(calls, numbered("calls", 1, 250, 1, 0, rows())));
        ObjectNode settings =
                options()
                        .put("account_id", MASKED)
                        .put("page_size", 250)
                        .put("start_date", "2025-01-01");
        try (Job job = job(List.of("companies", "calls"), settings, false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of(out("companies", "COM-one", MASKED, null, company)));
        }
        assertTrace(
                wire(companies, 250, 1), timed(calls, 250, 1, false, "2025-01-01", "2025-01-31"));
    }

    @Test
    void readsAll250RowsWithACommonTimestampFromOneRelativePage() throws Exception {
        ArrayNode page = rows();
        List<ObjectNode> output = new ArrayList<>();
        for (int i = 1; i <= 250; i++) {
            ObjectNode call = record("CAL-" + i).put("start_time", IN_WINDOW);
            page.add(call);
            output.add(out("calls", "CAL-" + i, ACCOUNT, IN_WINDOW, call));
        }
        reset(ok(CALLS, relative(page, null)));
        try (Job job = job(List.of("calls"), options().put("page_size", 250), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, output);
        }
        assertTrace(timed(CALLS, 250, 0, true, "2025-01-01", "2025-01-31"));
    }

    @Test
    void rejectsRelativeContinuationsAcrossOriginAccountOrResourceBoundaries() throws Exception {
        Seen initial = timed(CALLS, 100, 0, true, "2025-01-01", "2025-01-31");
        String next = continuation(nextCalls(100, 1, "2025-01-01", "2025-01-31"), true);
        for (String invalid :
                List.of(
                        next.replace(HOST, "callrail-redirect-host"),
                        next.replace(":18443", ":18444"),
                        next.replace("https:", "http:"),
                        next.replace(ACCOUNT, "outside"),
                        next.replace(CALLS_NEXT, USERS),
                        next.replace("https://", "https://name@"),
                        next + "#hidden")) {
            rejectOne(
                    ok(
                            CALLS,
                            relative(
                                    rows(record("CAL-first").put("start_time", IN_WINDOW)),
                                    invalid)),
                    List.of("calls"),
                    options(),
                    initial,
                    "relative continuation");
        }
    }

    @Test
    void rejectsUnscopedFrozenAndDuplicateQueryChangesBeforeEmission() throws Exception {
        Seen initial = timed(CALLS, 100, 0, true, "2025-01-01", "2025-01-31");
        String valid = continuation(nextCalls(100, 1, "2025-01-01", "2025-01-31"), false);
        for (String link :
                List.of(
                        valid.replace("offset=1", "offset=0"),
                        valid.replace("offset=1", "offset=2"),
                        valid.replace("end_date=2025-01-31", "end_date=2025-02-28"),
                        valid.replace("per_page=100", "per_page=99"),
                        valid.replace("time_zone=Etc%2FUTC", "time_zone=America%2FNew_York"),
                        valid.replace("relative_pagination=true", "relative_pagination=false"),
                        valid.replaceAll("fields=[^&]+", "fields=id"),
                        valid + "&offset=1",
                        valid + "&agency_id=private",
                        valid + "&auth_token=" + HIDDEN_MESSAGE)) {
            rejectOne(
                    ok(
                            CALLS,
                            relative(rows(record("CAL-first").put("start_time", IN_WINDOW)), link)),
                    List.of("calls"),
                    options(),
                    initial,
                    "continuation");
        }
    }

    @Test
    void neverMixesRelativeAndNumberedResponseModes() throws Exception {
        rejectOne(
                ok(CALLS, numbered("calls", 1, 100, 1, 0, rows())),
                List.of("calls"),
                options(),
                timed(CALLS, 100, 0, true, "2025-01-01", "2025-01-31"),
                "missing relative pagination flag");
        rejectOne(
                ok(CALLS, relative(rows(), null)),
                List.of("calls"),
                options().put("calls_pagination", "offset"),
                timed(CALLS, 100, 1, false, "2025-01-01", "2025-01-31"),
                "received relative metadata");
        rejectOne(
                ok(
                        CALLS,
                        relative(rows(), null).put("next_page", "https://callrail-redirect-host/")),
                List.of("calls"),
                options(),
                timed(CALLS, 100, 0, true, "2025-01-01", "2025-01-31"),
                "unexpected next_page on a terminal response");
    }

    @Test
    void validatesNumberedPageAndEffectiveSizeWithoutSkippingRows() throws Exception {
        rejectOne(
                ok(USERS, numbered("users", 2, 100, 3, 1, rows(record("US-one")))),
                List.of("users"),
                options(),
                wire(USERS, 100, 1),
                "inconsistent numbered pagination metadata");
        rejectOne(
                ok(USERS, numbered("users", 1, 100, 1, 0, rows(record("US-one")))),
                List.of("users"),
                options(),
                wire(USERS, 100, 1),
                "total_records is smaller");
        rejectOne(
                ok(
                        USERS,
                        numbered("users", 1, 100, 1, 1, rows(record("US-one")))
                                .put("total_pages", "one")),
                List.of("users"),
                options(),
                wire(USERS, 100, 1),
                "invalid numbered metadata total_pages");

        reset(
                ok(USERS, numbered("users", 1, 50, 2, 0, rows())),
                ok(USERS, numbered("users", 2, 25, 2, 0, rows())));
        failJob(List.of("users"), options(), "inconsistent numbered pagination metadata");
        assertTrace(wire(USERS, 100, 1), wire(USERS, 100, 2));
    }

    @Test
    void rejectsAdjacentDuplicatesRepeatedEarlierPagesAndBeyondBudgetWork() throws Exception {
        ObjectNode a = record("a");
        ObjectNode b = record("b");
        ObjectNode c = record("c");
        rejectOne(
                ok(USERS, numbered("users", 1, 100, 1, 2, rows(a, a))),
                List.of("users"),
                options(),
                wire(USERS, 100, 1),
                "duplicate identifier");
        reset(
                ok(USERS, numbered("users", 1, 100, 2, 4, rows(a, b))),
                ok(USERS, numbered("users", 2, 100, 2, 4, rows(b, c))));
        failJob(List.of("users"), options(), "duplicate identifier");
        assertTrace(wire(USERS, 100, 1), wire(USERS, 100, 2));

        reset(
                ok(USERS, numbered("users", 1, 100, 3, 5, rows(a, b))),
                ok(USERS, numbered("users", 2, 100, 3, 5, rows(c))),
                ok(USERS, numbered("users", 3, 100, 3, 5, rows(b, a))));
        failJob(List.of("users"), options(), "provider repeated an earlier page");
        assertTrace(wire(USERS, 100, 1), wire(USERS, 100, 2), wire(USERS, 100, 3));

        reset(ok(USERS, numbered("users", 1, 100, 2, 0, rows())));
        failJob(List.of("users"), options().put("max_pages_per_scope", 1), "max_pages_per_scope");
        assertTrace(wire(USERS, 100, 1));

        reset(ok(USERS, numbered("users", 1, 100, 1, 0, rows())));
        failJob(List.of("users", "companies"), options().put("max_requests", 1), "max_requests");
        assertTrace(wire(USERS, 100, 1));
    }

    @Test
    void validatesWholePagesAndTimestampFieldsBeforePublishingRows() throws Exception {
        ObjectNode valid = record("US-good").put("created_at", IN_WINDOW);
        rejectOne(
                ok(
                        USERS,
                        numbered(
                                "users",
                                1,
                                100,
                                1,
                                2,
                                rows(
                                        valid,
                                        record("US-bad")
                                                .put("created_at", "2025-02-30T00:00:00Z")))),
                List.of("users"),
                options(),
                wire(USERS, 100, 1),
                "record has no valid created_at timestamp");
        rejectOne(
                ok(USERS, numbered("users", 1, 100, 1, 2, rows(valid, options().put("id", -1)))),
                List.of("users"),
                options(),
                wire(USERS, 100, 1),
                "expected a textual or positive integral provider ID");
        String dateMissing = "record has no valid last_message_at timestamp";
        rejectOne(
                ok(
                        CONVERSATIONS,
                        numbered("conversations", 1, 100, 1, 1, rows(record("CON-missing")))),
                List.of("conversations"),
                options(),
                timed(CONVERSATIONS, 100, 1, false, "2025-01-01", "2025-01-31"),
                dateMissing);
        rejectOne(
                ok(USERS, options().put("error", HIDDEN_MESSAGE)),
                List.of("users"),
                options(),
                wire(USERS, 100, 1),
                "expected a successful account list object");
        rejectOne(
                ok(USERS, numbered("users", 1, 100, 1, 1, rows(valid)).put("users", "invalid")),
                List.of("users"),
                options(),
                wire(USERS, 100, 1),
                "expected a resource array");
        rejectOne(
                frame(
                        USERS,
                        200,
                        "plain",
                        "",
                        "{\"users\":[],\"users\":[]}".getBytes(StandardCharsets.UTF_8)),
                List.of("users"),
                options(),
                wire(USERS, 100, 1),
                "expected a single valid UTF-8 JSON");
    }

    @Test
    void rejectsUnauthorizedAndUnexpectedHttpResponsesWithoutLoggingPayloads() throws Exception {
        for (int status : List.of(400, 401, 403, 404, 204, 206)) {
            rejectOne(
                    frame(
                            USERS,
                            status,
                            "binary",
                            "",
                            HIDDEN_MESSAGE.getBytes(StandardCharsets.UTF_8)),
                    List.of("users"),
                    options(),
                    wire(USERS, 100, 1),
                    "HTTP " + status);
        }
    }

    @Test
    void respectsRetryAfterAndExponentialFallbackWithoutHidden503Replay() throws Exception {
        ObjectNode empty = numbered("users", 1, 100, 1, 0, rows());
        for (String timing : List.of("1", "")) {
            reset(reply(USERS, 429, "plain", timing, options()), ok(USERS, empty));
            try (Job job = job(List.of("users"), options().put("max_attempts", 2), false, false)) {
                good(engine.executeJob(job.configName));
                assertOutput(job, List.of());
            }
            assertTrace(wire(USERS, 100, 1), wire(USERS, 100, 1));
            List<Seen> actual = seen();
            assertTrue(TimeUnit.NANOSECONDS.toMillis(actual.get(1).at - actual.get(0).at) >= 970L);
        }
        rejectOne(
                reply(USERS, 429, "plain", "90", options()),
                List.of("users"),
                options(),
                wire(USERS, 100, 1),
                "retry delay exceeds request deadline");
        rejectOne(
                reply(USERS, 503, "plain", "0", options()),
                List.of("users"),
                options().put("max_attempts", 1),
                wire(USERS, 100, 1),
                "HTTP 503 retry budget exhausted");
    }

    @Test
    void handlesNegotiatedGzipAndRejectsInvalidEncodingTypeAndSize() throws Exception {
        ObjectNode user = record("US-ok").put("name", "团队");
        ObjectNode page = numbered("users", 1, 100, 1, 1, rows(user));
        reset(reply(USERS, 200, "gzip", "", page));
        try (Job job = job(List.of("users"), options(), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of(out("users", "US-ok", ACCOUNT, null, user)));
        }
        assertTrace(wire(USERS, 100, 1));
        ObjectNode big =
                numbered(
                        "users",
                        1,
                        100,
                        1,
                        1,
                        rows(record("US-large").put("bio", "x".repeat(8192))));
        rejectOne(
                reply(USERS, 200, "gzip", "", big),
                List.of("users"),
                options().put("max_response_bytes", 1024),
                wire(USERS, 100, 1),
                "response exceeds max_response_bytes");
        rejectOne(
                reply(USERS, 200, "unsupported", "", page),
                List.of("users"),
                options(),
                wire(USERS, 100, 1),
                "unsupported response compression");
        rejectOne(
                reply(USERS, 200, "binary", "", page),
                List.of("users"),
                options(),
                wire(USERS, 100, 1),
                "expected Content-Type application/json");
        rejectOne(
                frame(USERS, 200, "plain", "", new byte[] {'{', (byte) 0xFF, '}'}),
                List.of("users"),
                options(),
                wire(USERS, 100, 1),
                "single valid UTF-8 JSON");
    }

    @Test
    void retriesATruncatedGetWithoutEmittingItsFirstBody() throws Exception {
        ObjectNode omitted = record("US-prefix");
        ObjectNode accepted = record("US-complete");
        reset(
                reply(USERS, 200, "truncated", "", numbered("users", 1, 100, 1, 1, rows(omitted))),
                ok(USERS, numbered("users", 1, 100, 1, 1, rows(accepted))));
        try (Job job = job(List.of("users"), options().put("max_attempts", 2), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of(out("users", "US-complete", ACCOUNT, null, accepted)));
        }
        assertTrace(wire(USERS, 100, 1), wire(USERS, 100, 1));
    }

    @Test
    void rejectsWrongCertificatesHostnameAndRedirects() throws Exception {
        reset();
        failJob(
                List.of("users"),
                options().put("trust_certificate", OTHER_PEM),
                "TLS certificate or hostname");
        failJob(
                List.of("users"),
                options().put("endpoint", "https://callrail-other-host:18443"),
                "TLS certificate or hostname");
        assertTrace();
        rejectOne(
                reply(USERS, 302, "redirect", "", options()),
                List.of("users"),
                options(),
                wire(USERS, 100, 1),
                "HTTP 302");
    }

    @Test
    void abandonsAStreamingBodyAtTheTotalDeadline() throws Exception {
        rejectOne(
                stall(USERS),
                List.of("users"),
                options().put("request_timeout_ms", 2500),
                wire(USERS, 100, 1),
                "request deadline exceeded");
        await(() -> trace().contains("disconnect\t" + USERS), "deadline closed transfer");
    }

    @Test
    void cancelsTheFirstListWithoutRequestingTheLaterResource() throws Exception {
        reset(stall(USERS));
        try (Job job =
                job(
                        List.of("users", "companies"),
                        options().put("request_timeout_ms", 60000),
                        true,
                        false)) {
            Future<Container.ExecResult> running = start(job, job.id);
            try {
                await(() -> seen().size() == 1, "blocked users page");
            } finally {
                cancel(job.id, running);
            }
            await(() -> trace().contains("disconnect\t" + USERS), "cancel closed transfer");
            assertOutput(job, List.of());
        }
        assertTrace(wire(USERS, 100, 1));
    }

    @Test
    void resumesTheExactRelativePageAfterASameTimestampSavepoint() throws Exception {
        Seen next = nextCalls(100, 1, "2025-01-01", "2025-01-31");
        ObjectNode first = record("CAL-before-savepoint").put("start_time", START);
        ObjectNode second = record("CAL-after-savepoint").put("start_time", START);
        ObjectNode before = out("calls", "CAL-before-savepoint", ACCOUNT, START, first);
        ObjectNode after = out("calls", "CAL-after-savepoint", ACCOUNT, START, second);
        reset(ok(CALLS, relative(rows(first), continuation(next, true))), stall(CALLS_NEXT));
        try (Job job =
                job(List.of("calls"), options().put("request_timeout_ms", 60000), true, false)) {
            saveActive(job, 2, List.of(before), CALLS_NEXT);
            queue(ok(CALLS_NEXT, relative(rows(second), null)));
            good(engine.restoreJob(job.configName, job.id));
            assertOutput(job, List.of(before, after));
        }
        assertTrace(timed(CALLS, 100, 0, true, "2025-01-01", "2025-01-31"), next, next);
    }

    @Test
    void resumesTheSecondDateSliceAfterCommittingTheFirst() throws Exception {
        ObjectNode first =
                record("call-first-slice").put("start_time", "2025-04-10T23:59:59.999999999Z");
        ObjectNode second = record("call-next-slice").put("start_time", "2025-04-11T00:00:00Z");
        ObjectNode before =
                out("calls", "call-first-slice", ACCOUNT, first.path("start_time").asText(), first);
        ObjectNode after =
                out(
                        "calls",
                        "call-next-slice",
                        ACCOUNT,
                        second.path("start_time").asText(),
                        second);
        reset(ok(CALLS, numbered("calls", 1, 100, 1, 1, rows(first))), stall(CALLS));
        ObjectNode settings =
                options()
                        .put("request_timeout_ms", 60000)
                        .put("end_date", END)
                        .put("calls_pagination", "offset");
        try (Job job = job(List.of("calls"), settings, true, false)) {
            saveActive(job, 2, List.of(before), CALLS);
            queue(ok(CALLS, numbered("calls", 1, 100, 1, 1, rows(second))));
            good(engine.restoreJob(job.configName, job.id));
            assertOutput(job, List.of(before, after));
        }
        Seen latest = timed(CALLS, 100, 1, false, "2025-04-11", "2025-04-15");
        assertTrace(timed(CALLS, 100, 1, false, "2025-01-01", "2025-04-10"), latest, latest);
    }

    @Test
    void rejectsChangedAccountScopeBeforeStartingAResumedRequest() throws Exception {
        reset(stall(USERS));
        try (Job original =
                        job(
                                List.of("users"),
                                options().put("request_timeout_ms", 60000),
                                true,
                                false);
                Job changed =
                        job(List.of("users"), options().put("account_id", MASKED), true, false)) {
            saveActive(original, 1, List.of(), USERS);
            bad(engine.restoreJob(changed.configName, original.id), "read scope changed");
        }
        assertTrace(wire(USERS, 100, 1));
    }

    @Test
    void preservesTheTerminalListWhenAnIndependentPipelineIsStillActive() throws Exception {
        ObjectNode user = record("US-terminal");
        ObjectNode expected = out("users", "US-terminal", ACCOUNT, null, user);
        reset(ok(USERS, numbered("users", 1, 100, 1, 1, rows(user))));
        try (Job job = job(List.of("users"), options(), true, true)) {
            saveActive(job, 1, List.of(expected), null);
            Future<Container.ExecResult> restored =
                    submissions.submit(() -> engine.restoreJob(job.configName, job.id));
            try {
                await(
                        () -> "RUNNING".equals(engine.getJobStatus(job.id)),
                        "restored active anchor");
                long before = engine.checkpointCount(job.id);
                await(() -> engine.checkpointCount(job.id) > before, "new active checkpoint");
                assertOutput(job, List.of(expected));
                assertTrace(wire(USERS, 100, 1));
            } finally {
                cancel(job.id, restored);
            }
        }
    }

    @Test
    void rejectsInvalidConfigBeforeOpeningProviderTransport() throws Exception {
        reset();
        failJob(
                ALL,
                options().put("account_id", "wrong/path"),
                "expected a numeric or masked account identifier");
        failJob(ALL, options().put("calls_pagination", "page"), "choose relative or offset");
        failJob(ALL, options().put("start_date", "2025-02-30"), "expected an ISO date or RFC 3339");
        failJob(ALL, options().put("end_date", "2024-01-01"), "must be earlier than the scan end");
        failJob(ALL, options().put("page_size", 251), "expected a value from 1 to 250");
        failJob(List.of("users", "users"), options(), "choose each resource only once");
        failJob(List.of("invented"), options(), "unknown resource name");
        failJob(
                ALL,
                options().put("endpoint", "http://callrail-peer:18443"),
                "expected a bare HTTPS origin");
        assertTrace();
    }

    @Test
    void serializesExactPartialRowsAndRejectsChangedFilteredDataOrContinuation() throws Exception {
        List<String> wanted = new ArrayList<>();
        ArrayNode items = rows(record("filtered").put("created_at", "2024-06-01T00:00:00Z"));
        for (int n = 1; n <= 33; n++) {
            String id = "user-" + n;
            wanted.add(id);
            items.add(record(id));
        }
        ArrayNode modified = items.deepCopy();
        modified.set(0, record("filtered").put("created_at", "2024-06-02T00:00:00Z"));
        ObjectNode complete = numbered("users", 1, 100, 1, 34, items);
        reset(
                ok(USERS, complete),
                ok(USERS, complete),
                ok(USERS, numbered("users", 1, 100, 1, 34, modified)),
                ok(USERS, numbered("users", 1, 100, 2, 34, items)));
        CallRailRecoveryProbe reader = recovery(List.of("users"));
        List<String> emitted = new ArrayList<>();
        byte[] snapshot = reader.firstChunk(emitted);
        assertEquals(wanted.subList(0, 32), emitted);
        String serialized = new String(snapshot, StandardCharsets.ISO_8859_1);
        assertFalse(serialized.contains(TOKEN) || serialized.contains("user-1"));
        List<String> suffix = new ArrayList<>();
        reader.consume(snapshot, suffix);
        assertEquals(wanted.subList(32, 33), suffix);
        for (int n = 0; n < 2; n++) {
            List<String> rejected = new ArrayList<>();
            Exception changed =
                    assertThrows(Exception.class, () -> reader.consume(snapshot, rejected));
            assertTrue(changed.getMessage().contains("provider page changed since checkpoint"));
            assertTrue(rejected.isEmpty());
        }
        assertTrace(
                wire(USERS, 100, 1), wire(USERS, 100, 1), wire(USERS, 100, 1), wire(USERS, 100, 1));
    }

    @Test
    void restoresCoordinatorAndFinishesEmptyOrCancelledReadersWithoutNetwork() throws Exception {
        reset();
        CallRailRecoveryProbe probe = recovery(List.of("users"));
        byte[] pending = probe.recoverUnassigned();
        probe.finishWithoutAssignment();
        List<String> output = new ArrayList<>();
        probe.stopBeforeAssignment(output);
        assertTrue(output.isEmpty());
        assertTrace();

        queue(ok(USERS, numbered("users", 1, 100, 1, 1, rows(record("US-coordinator")))));
        probe.consume(pending, output);
        assertEquals(List.of("US-coordinator"), output);
        assertTrace(wire(USERS, 100, 1));
    }

    private void rejectOne(
            String scripted,
            List<String> resources,
            ObjectNode settings,
            Seen expected,
            String diagnostic)
            throws Exception {
        reset(scripted);
        try (Job job = job(resources, settings, false, false)) {
            bad(engine.executeJob(job.configName), diagnostic);
            assertOutput(job, List.of());
        }
        assertTrace(expected);
    }

    private void failJob(List<String> resources, ObjectNode values, String diagnostic)
            throws Exception {
        try (Job job = job(resources, values, false, false)) {
            bad(engine.executeJob(job.configName), diagnostic);
        }
    }

    private void saveActive(Job job, int calls, List<ObjectNode> committed, String stalledPath)
            throws Exception {
        Future<Container.ExecResult> running = start(job, job.id);
        try {
            await(() -> seen().size() == calls, "expected provider request");
            awaitOutput(job, committed);
            good(engine.savepointJob(job.id));
            running.get(45, TimeUnit.SECONDS);
        } finally {
            if (!running.isDone()) {
                cancel(job.id, running);
            }
        }
        if (stalledPath != null) {
            await(() -> trace().contains("disconnect\t" + stalledPath), "savepoint closed request");
        }
    }

    private CallRailRecoveryProbe recovery(List<String> resources) throws Exception {
        URI url =
                new URI("https", null, peer.getHost(), peer.getMappedPort(18443), null, null, null);
        return new CallRailRecoveryProbe(url.toString(), certificate.toString(), TOKEN, resources);
    }

    private Future<Container.ExecResult> start(Job job, String id) {
        return submissions.submit(() -> engine.start(job.configName, id));
    }

    private void cancel(String id, Future<Container.ExecResult> running) throws Exception {
        try {
            if (!running.isDone()) {
                engine.stopJob(id);
            }
            running.get(45, TimeUnit.SECONDS);
        } finally {
            if (!running.isDone()) {
                running.cancel(true);
            }
        }
    }

    private static String id() {
        return Long.toString(UUID.randomUUID().getLeastSignificantBits() & Long.MAX_VALUE);
    }

    private String copyCertificate(String path) throws Exception {
        Container.ExecResult certificate = peer.execInContainer("cat", path);
        assertEquals(0, certificate.getExitCode(), certificate.getStderr());
        engine.putCertificate(path, certificate.getStdout());
        return certificate.getStdout();
    }

    private static Job job(
            List<String> selected, ObjectNode overrides, boolean stream, boolean anchor)
            throws Exception {
        String name = "callrail-" + UUID.randomUUID();
        String path = "/tmp/seatunnel_mnt/" + name;
        ObjectNode settings =
                options()
                        .put("plugin_output", "callrail_data")
                        .put("token", TOKEN)
                        .put("account_id", ACCOUNT)
                        .put("start_date", START)
                        .put("end_date", "2025-02-01")
                        .put("endpoint", ENDPOINT)
                        .put("trust_certificate", PEM);
        ArrayNode resources = settings.putArray("resources");
        selected.forEach(resources::add);
        settings.setAll(overrides);
        String anchorInput =
                anchor
                        ? " FakeSource {\n plugin_output=anchor\n row.num=1000\n split.num=1000\n"
                                + " split.read-interval=250\n schema.fields.name=\"string\"\n}\n"
                        : "";
        String anchorOutput = anchor ? " Console { plugin_input=anchor }\n" : "";
        String contents =
                "env {\n job.mode="
                        + (stream ? "STREAMING" : "BATCH")
                        + "\n parallelism=1\n checkpoint.interval=350\n}\n"
                        + "source {\n CallRail "
                        + settings
                        + "\n"
                        + anchorInput
                        + "}\n"
                        + "sink {\n LocalFile {\n plugin_input=callrail_data\n path=\""
                        + path
                        + "\"\n"
                        + " file_format_type=json\n is_enable_transaction=true\n custom_filename=true\n"
                        + " file_name_expression=\"callrail_$"
                        + "{transactionId}\"\n}\n"
                        + anchorOutput
                        + "}\n";
        Path directory = ContainerUtil.getCurrentModulePath().resolve("src/test/resources");
        Files.createDirectories(directory);
        return new Job(
                Files.writeString(
                        directory.resolve(name + ".conf"), contents, StandardCharsets.UTF_8),
                path);
    }

    private static ObjectNode options() {
        return JSON.createObjectNode();
    }

    private static ObjectNode record(String id) {
        return options().put("id", id);
    }

    private static ObjectNode numbered(
            String resource, int page, int size, int pages, long total, ArrayNode records) {
        ObjectNode response =
                options()
                        .put("page", page)
                        .put("per_page", size)
                        .put("total_pages", pages)
                        .put("total_records", total);
        response.set(resource, records);
        return response;
    }

    private static ObjectNode relative(ArrayNode items, String next) {
        ObjectNode response = options().put("has_next_page", next != null).put("next_page", next);
        response.set("calls", items);
        return response;
    }

    private static ArrayNode rows(JsonNode... records) {
        ArrayNode array = JSON.createArrayNode();
        for (JsonNode record : records) {
            array.add(record);
        }
        return array;
    }

    private static ObjectNode out(
            String resource, String id, String account, String time, ObjectNode payload) {
        ObjectNode row =
                options()
                        .put("resource", resource)
                        .put("record_id", id)
                        .put("account_id", account)
                        .put("record_time", time);
        row.set("payload", payload);
        return row;
    }

    private static String reply(
            String path,
            int status,
            String mode,
            String retryAfter,
            JsonNode body,
            String... links) {
        return frame(
                path,
                status,
                mode,
                retryAfter,
                body.toString().getBytes(StandardCharsets.UTF_8),
                links);
    }

    private static String frame(
            String path, int status, String mode, String retryAfter, byte[] body, String... links) {
        String encoded = Base64.getEncoder().encodeToString(body);
        String link =
                Base64.getEncoder()
                        .encodeToString(String.join("\n", links).getBytes(StandardCharsets.UTF_8));
        return String.join("\t", Integer.toString(status), mode, retryAfter, encoded, path, link);
    }

    private static String ok(String path, JsonNode body, String... links) {
        return reply(path, 200, "plain", "", body, links);
    }

    private static String stall(String path) {
        return reply(path, 200, "chunked", "", options());
    }

    private void reset(String... replies) throws Exception {
        prepare("/reset", replies);
    }

    private void queue(String... replies) throws Exception {
        prepare("/queue", replies);
    }

    private URI fixture(String path) throws Exception {
        return new URI("http", null, peer.getHost(), peer.getMappedPort(18080), path, null, null);
    }

    private void prepare(String path, String[] replies) throws Exception {
        HttpRequest request =
                HttpRequest.newBuilder(fixture(path))
                        .timeout(Duration.ofSeconds(5))
                        .POST(HttpRequest.BodyPublishers.ofString(String.join("\n", replies)))
                        .build();
        assertEquals(
                200,
                testControl.send(request, HttpResponse.BodyHandlers.discarding()).statusCode());
    }

    private String trace() throws Exception {
        HttpRequest get =
                HttpRequest.newBuilder(fixture("/requests")).timeout(Duration.ofSeconds(5)).build();
        HttpResponse<String> result = testControl.send(get, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, result.statusCode());
        return result.body();
    }

    private List<Seen> seen() throws Exception {
        List<Seen> requests = new ArrayList<>();
        for (String frame : trace().split("\n")) {
            if (!frame.startsWith("call\t")) {
                continue;
            }
            String[] parts = frame.split("\t", -1);
            assertEquals("GET", parts[1]);
            assertEquals("true", parts[4], "expected the documented quoted token authorization");
            assertEquals("true", parts[5], "the connector must ask for JSON");
            URI request = URI.create(parts[2]);
            assertEquals("true", parts[6], "expected the connector application User-Agent");
            Map<String, String> query = new TreeMap<>();
            if (request.getRawQuery() != null) {
                for (String param : request.getRawQuery().split("&")) {
                    String[] pair = param.split("=", 2);
                    assertEquals(2, pair.length);
                    assertNull(
                            query.put(pair[0], URLDecoder.decode(pair[1], StandardCharsets.UTF_8)));
                }
            }
            requests.add(new Seen(request.getRawPath(), query, Long.parseLong(parts[3])));
        }
        return requests;
    }

    private static Seen wire(String path, int size, int page) {
        return new Seen(
                path, Map.of("per_page", String.valueOf(size), "page", String.valueOf(page)), 0L);
    }

    private static Seen timed(
            String path,
            int size,
            int position,
            boolean relative,
            String firstDay,
            String lastDay) {
        String fields = path.contains("calls") ? CALL_FIELDS : CONVERSATION_FIELDS;
        Map<String, String> query = new TreeMap<>();
        query.put("per_page", String.valueOf(size));
        query.put("fields", fields);
        query.put("start_date", firstDay);
        query.put("end_date", lastDay);
        query.put("time_zone", "Etc/UTC");
        if (relative) {
            query.put("relative_pagination", "true");
            query.put("offset", String.valueOf(position));
        } else {
            query.put("page", String.valueOf(position));
        }
        return new Seen(path, query, 0L);
    }

    private static Seen nextCalls(int size, int offset, String firstDay, String lastDay) {
        return timed(CALLS_NEXT, size, offset, true, firstDay, lastDay);
    }

    private static String continuation(Seen request, boolean absolute) {
        String query =
                request.query.entrySet().stream()
                        .map(
                                item ->
                                        URLEncoder.encode(item.getKey(), StandardCharsets.UTF_8)
                                                + "="
                                                + URLEncoder.encode(
                                                        item.getValue(), StandardCharsets.UTF_8))
                        .collect(Collectors.joining("&"));
        return (absolute ? ENDPOINT : "") + request.route + "?" + query;
    }

    private void assertTrace(Seen... expected) throws Exception {
        List<Seen> actual = seen();
        assertEquals(
                expected.length,
                actual.size(),
                () ->
                        actual.stream()
                                .map(item -> item.route + item.query)
                                .collect(Collectors.joining(", ")));
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i].route, actual.get(i).route, "route at index " + i);
            assertEquals(expected[i].query, actual.get(i).query, "query at index " + i);
        }
    }

    private void assertOutput(Job job, List<ObjectNode> expected) throws Exception {
        assertEquals(frequencies(expected), frequencies(engine.outputs(job.path)));
    }

    private void awaitOutput(Job job, List<ObjectNode> expected) throws Exception {
        await(
                () -> frequencies(engine.outputs(job.path)).equals(frequencies(expected)),
                "committed local output");
    }

    private static Map<ObjectNode, Long> frequencies(List<ObjectNode> values) {
        return values.stream()
                .collect(Collectors.groupingBy(value -> value, Collectors.counting()));
    }

    private static void good(Container.ExecResult result) {
        assertEquals(0, result.getExitCode(), () -> result.getStdout() + result.getStderr());
    }

    private void bad(Container.ExecResult result, String diagnostic) {
        assertNotEquals(0, result.getExitCode(), diagnostic);
        String visible = result.getStdout() + result.getStderr() + engine.getServerLogs();
        assertTrue(visible.contains(diagnostic), "Missing diagnostic " + diagnostic);
        assertFalse(visible.contains(TOKEN), "access key leaked");
        assertFalse(visible.contains(HIDDEN_MESSAGE), "provider error text leaked");
    }

    private static void await(Probe check, String purpose) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
        do {
            if (check.ready()) {
                return;
            }
            Thread.sleep(100L);
        } while (System.nanoTime() < until);
        throw new AssertionError("Timed out waiting for " + purpose);
    }

    /**
     * An observed wire request with a provider-side timestamp.
     *
     * <p>The timing is comparable within this fixture process and never uses wall-clock time.
     */
    private static final class Seen {
        private final String route;
        private final Map<String, String> query;
        private final long at;

        private Seen(String route, Map<String, String> query, long at) {
            this.route = route;
            this.query = query;
            this.at = at;
        }
    }

    /**
     * Files for a job submitted via the regular Zeta CLI.
     *
     * <p>The same output folder survives restoration so tests can detect duplicated writes.
     */
    private static final class Job implements AutoCloseable {
        private final Path config;
        private final String configName;
        private final String path;
        private final String id = CallRailIT.id();

        private Job(Path config, String path) {
            this.config = config;
            this.configName = "/" + config.getFileName();
            this.path = path;
        }

        @Override
        public void close() throws Exception {
            Files.deleteIfExists(config);
        }
    }

    /**
     * Checked condition against the active fixture or engine.
     *
     * <p>Unexpected read failures are exposed immediately rather than hidden behind a wait timeout.
     */
    @FunctionalInterface
    private interface Probe {
        boolean ready() throws Exception;
    }

    /**
     * Exposes the committed LocalFile results and live checkpoint counters for this suite.
     *
     * <p>Fixture tests use the actual starter, LocalFile committer and Zeta REST status.
     */
    private static final class Engine extends SeaTunnelContainer {
        private final HttpClient rest = HttpClient.newHttpClient();

        private void putCertificate(String path, String contents) {
            server.copyFileToContainer(Transferable.of(contents), path);
        }

        private Container.ExecResult start(String config, String id) throws Exception {
            return executeJob(server, config, id, List.of());
        }

        private List<ObjectNode> outputs(String directory) throws Exception {
            Container.ExecResult files =
                    server.execInContainer(
                            "sh",
                            "-c",
                            "if [ -d '"
                                    + directory
                                    + "' ]; then find '"
                                    + directory
                                    + "' -maxdepth 1 -type f -name 'callrail_*' -exec cat {} +; fi");
            assertEquals(0, files.getExitCode(), files.getStderr());
            List<ObjectNode> output = new ArrayList<>();
            for (String line : files.getStdout().split("\\R")) {
                if (!line.isBlank()) {
                    ObjectNode row = (ObjectNode) JSON.readTree(line);
                    assertTrue(row.path("payload").isTextual());
                    row.set("payload", JSON.readTree(row.path("payload").textValue()));
                    output.add(row);
                }
            }
            return output;
        }

        private long checkpointCount(String id) throws Exception {
            URI address =
                    new URI(
                            "http",
                            null,
                            server.getHost(),
                            server.getMappedPort(8080),
                            "/jobs/checkpoints/" + id,
                            null,
                            null);
            HttpRequest query =
                    HttpRequest.newBuilder(address).timeout(Duration.ofSeconds(5)).build();
            HttpResponse<String> response = rest.send(query, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                return 0;
            }
            // The CallRail pipeline can already have ended. Observe all active pipeline counters.
            long completed = 0;
            for (JsonNode pipeline : JSON.readTree(response.body()).path("pipelines")) {
                completed += pipeline.path("counts").path("completed").asLong();
            }
            return completed;
        }
    }
}
