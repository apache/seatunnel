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
package org.apache.seatunnel.e2e.connector.campayn;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.campayn.CampaynRecoveryProbe;
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
 * Runs Campayn through a real SeaTunnel Zeta engine and an isolated Java TLS account.
 *
 * <p>Tests compare page numbers and bounds in captured GET requests and inspect LocalFile's
 * committed JSON rows. The provider peer never calls the real billable API.
 */
class CampaynIT extends TestSuiteBase {
    private static final ObjectMapper JSON =
            new ObjectMapper()
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    private static final String TOKEN = "campayn-fixture-private+/=?";
    private static final String HIDDEN_MESSAGE = "private-provider-response-token";
    private static final String HOST = "campayn-peer";
    private static final String PEM = "/tmp/campayn.pem";
    private static final String OTHER_PEM = "/tmp/unrelated-campayn.pem";
    private static final String ENDPOINT = "https://" + HOST + ":18443";
    private static final String TENANT = "tenant-1";
    private static final String LISTS = "/api/v1/lists.json";
    private static final String EMAILS = "/api/v1/emails.json";
    private static final String REPORTS = "/api/v1/reports/calendar.json";
    private static final List<String> ALL =
            List.of("lists", "forms", "contacts", "emails", "reports");
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
                "keytool -genkeypair -alias provider -storetype PKCS12 -storepass campayn-fixture "
                        + "-keyalg RSA -keysize 2048 -validity 30 -dname CN="
                        + HOST
                        + " -ext SAN=dns:"
                        + HOST
                        + ",dns:campayn-redirect-host,"
                        + runnerType
                        + runner
                        + " -keystore ";
        String export =
                "keytool -exportcert -rfc -alias provider -storepass campayn-fixture -keystore ";
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
                        + CampaynPeer.class.getName()
                        + " /tmp/provider.p12";
        String providerClass = "/" + CampaynPeer.class.getName().replace('.', '/') + ".class";
        byte[] bytecode;
        try (InputStream bits = CampaynPeer.class.getResourceAsStream(providerClass)) {
            if (bits == null) {
                throw new IllegalStateException("Missing packaged HTTPS fixture");
            }
            bytecode = bits.readAllBytes();
        }
        peer =
                new GenericContainer<>(DockerImageName.parse("eclipse-temurin:11-jdk"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST, "campayn-other-host", "campayn-redirect-host")
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
    void readsEveryDocumentedCollectionWithExactTenantAndListIdentity() throws Exception {
        ObjectNode first = record("101").put("list_name", "客户 🧭").putNull("tags");
        first.put("contact_count", new BigInteger("9223372036854775808"));
        ObjectNode second = options().put("id", 202).put("list_name", "Follow-up");
        ObjectNode formA =
                record("shared-form")
                        .put("contact_list_id", "101")
                        .put("form_html", "<form>原样</form>");
        ObjectNode formB = record("shared-form").put("contact_list_id", 202).put("form_html", "");
        ObjectNode contactA =
                record("contact").putNull("first_name").put("email", "sample-one@example.invalid");
        contactA.put("image_url", "https://campayn-redirect-host:18443/never-fetch-image");
        ObjectNode contactB = record("contact").put("first_name", "用户");
        contactB.putArray("custom_fields").addObject().putNull("value");
        ObjectNode email = record("email-1").put("send_now", "0").putNull("campaign_title");
        email.put("percent_views", new BigDecimal("123456789012345678.123456789000"));
        email.put("preview_thumb", "https://campayn-redirect-host:18443/never-fetch-preview");
        ObjectNode scheduled =
                record("email-1")
                        .put("scheduled_date", "2030-03-20 13:45:05")
                        .put("status", "scheduled")
                        .putNull("report_url");
        ObjectNode sent =
                record("sent")
                        .put("scheduled_date", "2015-02-01 00:00:00")
                        .put("status", "sent")
                        .put("report_url", "https://campayn-redirect-host:18443/not-fetched");
        reset(
                ok(LISTS, rows(first, second)),
                ok(child("101", "forms"), rows(formA)),
                ok(child("202", "forms"), rows(formB)),
                ok(child("101", "contacts"), rows(contactA)),
                ok(child("202", "contacts"), rows(contactB)),
                ok(EMAILS, rows(email)),
                ok(REPORTS, rows(scheduled, sent)));
        try (Job job = job(ALL, options().put("sub_domain", "Tenant-1"), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(
                    job,
                    List.of(
                            out("lists", "101", "101", first),
                            out("lists", "202", "202", second),
                            out("forms", "shared-form", "101", formA),
                            out("forms", "shared-form", "202", formB),
                            out("contacts", "contact", "101", contactA),
                            out("contacts", "contact", "202", contactB),
                            out("emails", "email-1", null, email),
                            out("reports", "email-1", null, scheduled),
                            out("reports", "sent", null, sent)));
        }
        assertTrace(
                wire(LISTS),
                wire(child("101", "forms")),
                wire(child("202", "forms")),
                wire(child("101", "contacts")),
                wire(child("202", "contacts")),
                wire(EMAILS),
                wire(REPORTS));
    }

    @Test
    void preservesLaterParentsAfterAnEmptyChildWhenListsAreUnselected() throws Exception {
        ObjectNode later = record("contact-late");
        reset(
                ok(LISTS, rows(record("1"), record("2"))),
                ok(child("1", "contacts"), rows()),
                ok(child("2", "contacts"), rows(later)));
        try (Job job = job(List.of("contacts"), options(), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of(out("contacts", "contact-late", "2", later)));
        }
        assertTrace(wire(LISTS), wire(child("1", "contacts")), wire(child("2", "contacts")));
    }

    @Test
    void skipsListChildrenAfterEmptyDiscoveryAndKeepsIndependentCollections() throws Exception {
        ObjectNode report = record("calendar").putNull("report_url");
        reset(ok(LISTS, rows()), ok(EMAILS, rows()), ok(REPORTS, rows(report)));
        try (Job job =
                job(List.of("forms", "contacts", "emails", "reports"), options(), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of(out("reports", "calendar", null, report)));
        }
        assertTrace(wire(LISTS), wire(EMAILS), wire(REPORTS));
    }

    @Test
    void readsOnlyTheSelectedCalendarWithoutSpeculativeDateOrPageArguments() throws Exception {
        ObjectNode past = record("sent-early").put("scheduled_date", "2010-04-01 00:00:00");
        ObjectNode future =
                record("planned-later")
                        .put("scheduled_date", "2040-04-01 00:00:00")
                        .putNull("report_url");
        reset(ok(REPORTS, rows(past, future)));
        try (Job job = job(List.of("reports"), options(), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(
                    job,
                    List.of(
                            out("reports", "sent-early", null, past),
                            out("reports", "planned-later", null, future)));
        }
        assertTrace(wire(REPORTS));
    }

    @Test
    void rejectsForeignFormsAndReportsAChildPermissionFailureWithoutFetchingSiblings()
            throws Exception {
        String forms = child("10", "forms");
        reset(
                ok(LISTS, rows(record("10"), record("20"))),
                ok(
                        forms,
                        rows(
                                record("valid").put("contact_list_id", "10"),
                                record("misrouted").put("contact_list_id", "20"))));
        try (Job job = job(List.of("forms"), options(), false, false)) {
            bad(engine.executeJob(job.configName), "form does not belong to the requested list");
            assertOutput(job, List.of());
        }
        assertTrace(wire(LISTS), wire(forms));

        String contacts = child("10", "contacts");
        reset(
                ok(LISTS, rows(record("10"), record("20"))),
                reply(contacts, 403, "plain", "", options().put("error", HIDDEN_MESSAGE)));
        try (Job job = job(List.of("contacts", "reports"), options(), false, false)) {
            bad(engine.executeJob(job.configName), "contacts: access denied (HTTP 403)");
            assertOutput(job, List.of());
        }
        assertTrace(wire(LISTS), wire(contacts));
    }

    @Test
    void validatesCompleteArraysAndRejectsAmbiguousFieldsBeforeEmission() throws Exception {
        List<String> reports = List.of("reports");
        rejectOne(
                ok(REPORTS, options().put("error", HIDDEN_MESSAGE)),
                reports,
                options(),
                wire(REPORTS),
                "expected a JSON array");
        rejectOne(
                ok(REPORTS, rows(record("valid"), JSON.getNodeFactory().nullNode())),
                reports,
                options(),
                wire(REPORTS),
                "expected each collection record to be a JSON object");
        rejectOne(
                ok(REPORTS, rows(record("valid"), options().put("id", -10))),
                reports,
                options(),
                wire(REPORTS),
                "expected a textual or positive integral provider id");
        rejectOne(
                ok(
                        REPORTS,
                        rows(record("same"), options().put("id", "same").put("status", "sent"))),
                reports,
                options(),
                wire(REPORTS),
                "duplicate provider id");
        rejectOne(
                frame(
                        REPORTS,
                        200,
                        "plain",
                        "",
                        "[{\"id\":\"one\",\"id\":\"two\"}]".getBytes(StandardCharsets.UTF_8)),
                reports,
                options(),
                wire(REPORTS),
                "single valid UTF-8 JSON");
        rejectOne(
                frame(
                        REPORTS,
                        200,
                        "plain",
                        "",
                        ("[] " + HIDDEN_MESSAGE).getBytes(StandardCharsets.UTF_8)),
                reports,
                options(),
                wire(REPORTS),
                "single valid UTF-8 JSON");
    }

    @Test
    void refusesUnsafeListIdsAndBoundsDiscoveryBeforeOpeningAChild() throws Exception {
        for (String invalid : List.of("..", "a/b", "encoded%2Flist", "path?query")) {
            rejectOne(
                    ok(LISTS, rows(record("valid"), record(invalid))),
                    List.of("contacts"),
                    options(),
                    wire(LISTS),
                    "list identifier is unsafe");
        }
        rejectOne(
                ok(LISTS, rows(record("one"), record("two"))),
                List.of("forms"),
                options().put("max_lists", 1),
                wire(LISTS),
                "list discovery exceeds max_lists");
        reset(ok(LISTS, rows(record("one"))));
        failJob(List.of("contacts"), options().put("max_requests", 1), "max_requests exhausted");
        assertTrace(wire(LISTS));
    }

    @Test
    void rejectsAnUnexpectedPaginationSignalWithoutOpeningItsTarget() throws Exception {
        rejectOne(
                ok(
                        REPORTS,
                        rows(record("report")),
                        "<https://campayn-redirect-host:18443/paged>; rel=next"),
                List.of("reports"),
                options(),
                wire(REPORTS),
                "unexpected Link header for a complete list");
    }

    @Test
    void rejectsInvalidTenantLabelsAndConfigurationsWithoutAProviderCall() throws Exception {
        reset();
        for (String invalid :
                List.of(
                        "https://other.example",
                        "sub.tenant",
                        "name@other",
                        "tenant:443",
                        "a/b",
                        "-lead",
                        "tail-",
                        "mixed space",
                        "a".repeat(64))) {
            failJob(ALL, options().put("sub_domain", invalid), "expected one DNS label");
        }
        failJob(ALL, options().put("token", "TRUEREST " + TOKEN), "without a prefix");
        failJob(
                ALL,
                options().put("endpoint", "http://campayn-peer:18443"),
                "expected a bare HTTPS origin");
        failJob(List.of("lists", "lists"), options(), "choose each resource only once");
        failJob(List.of("account"), options(), "unknown resource name");
        assertTrace();
    }

    @Test
    void refusesAccessAndInvalidStatusBodiesWithoutExposingTheirContents() throws Exception {
        List<String> selection = List.of("emails");
        for (int status : List.of(400, 401, 403, 404, 204, 206)) {
            rejectOne(
                    frame(
                            EMAILS,
                            status,
                            "binary",
                            "",
                            HIDDEN_MESSAGE.getBytes(StandardCharsets.UTF_8)),
                    selection,
                    options(),
                    wire(EMAILS),
                    "HTTP " + status);
        }
    }

    @Test
    void boundsRateLimitFallbackAndHonorsAnExplicitRetryAfter() throws Exception {
        List<String> selection = List.of("emails");
        for (String retry : List.of("1", "")) {
            reset(reply(EMAILS, 429, "plain", retry, options()), ok(EMAILS, rows()));
            try (Job job = job(selection, options().put("max_attempts", 2), false, false)) {
                good(engine.executeJob(job.configName));
                assertOutput(job, List.of());
            }
            assertTrace(wire(EMAILS), wire(EMAILS));
            List<Seen> observed = seen();
            assertTrue(
                    TimeUnit.NANOSECONDS.toMillis(observed.get(1).at - observed.get(0).at) >= 970L);
        }
        rejectOne(
                reply(EMAILS, 429, "plain", "90", options()),
                selection,
                options(),
                wire(EMAILS),
                "retry delay exceeds request deadline");
        rejectOne(
                reply(EMAILS, 503, "plain", "0", options()),
                selection,
                options().put("max_attempts", 1),
                wire(EMAILS),
                "HTTP 503 retry budget exhausted");
    }

    @Test
    void enforcesDecodedBodyBoundsAndStrictJsonMimeAndUtf8() throws Exception {
        List<String> selection = List.of("emails");
        ObjectNode value = record("中文-邮件");
        reset(reply(EMAILS, 200, "gzip", "", rows(value)));
        try (Job job = job(selection, options(), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of(out("emails", "中文-邮件", null, value)));
        }
        assertTrace(wire(EMAILS));
        ObjectNode big = record("wide").put("html", "x".repeat(8192));
        rejectOne(
                reply(EMAILS, 200, "gzip", "", rows(big)),
                selection,
                options().put("max_response_bytes", 1024),
                wire(EMAILS),
                "response exceeds max_response_bytes");
        rejectOne(
                reply(EMAILS, 200, "unsupported", "", rows(value)),
                selection,
                options(),
                wire(EMAILS),
                "unsupported response compression");
        rejectOne(
                reply(EMAILS, 200, "binary", "", rows(value)),
                selection,
                options(),
                wire(EMAILS),
                "expected Content-Type application/json");
        rejectOne(
                frame(EMAILS, 200, "plain", "", new byte[] {'[', (byte) 0xFF, ']'}),
                selection,
                options(),
                wire(EMAILS),
                "single valid UTF-8 JSON");
    }

    @Test
    void restoresACompleteGetAfterATruncatedBodyWithoutEmittingThatBody() throws Exception {
        ObjectNode first = record("not-complete");
        ObjectNode second = record("complete");
        reset(reply(EMAILS, 200, "truncated", "", rows(first)), ok(EMAILS, rows(second)));
        try (Job job = job(List.of("emails"), options().put("max_attempts", 2), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of(out("emails", "complete", null, second)));
        }
        assertTrace(wire(EMAILS), wire(EMAILS));
    }

    @Test
    void verifiesTrustAndHostnameAndNeverFollowsARedirect() throws Exception {
        List<String> selection = List.of("emails");
        reset();
        failJob(
                selection,
                options().put("trust_certificate", OTHER_PEM),
                "TLS certificate or hostname");
        failJob(
                selection,
                options().put("endpoint", "https://campayn-other-host:18443"),
                "TLS certificate or hostname");
        assertTrace();
        rejectOne(
                reply(EMAILS, 302, "redirect", "", options()),
                selection,
                options(),
                wire(EMAILS),
                "HTTP 302");
    }

    @Test
    void stopsAnActiveBodyAtItsTotalDeadline() throws Exception {
        rejectOne(
                stall(EMAILS),
                List.of("emails"),
                options().put("request_timeout_ms", 2500),
                wire(EMAILS),
                "request deadline exceeded");
        await(
                () -> trace().contains("disconnect\t" + EMAILS),
                "deadline closed provider connection");
    }

    @Test
    void cancelsTheActiveListChildBeforeItCanReachAnotherScope() throws Exception {
        String next = child("1", "forms");
        reset(ok(LISTS, rows(record("1"), record("2"))), stall(next));
        try (Job job =
                job(
                        List.of("forms", "reports"),
                        options().put("request_timeout_ms", 60000),
                        true,
                        false)) {
            Future<Container.ExecResult> running = start(job, job.id);
            try {
                await(() -> seen().size() == 2, "stalled form request");
            } finally {
                cancel(job.id, running);
            }
            await(() -> trace().contains("disconnect\t" + next), "cancel closed child request");
            assertOutput(job, List.of());
        }
        assertTrace(wire(LISTS), wire(next));
    }

    @Test
    void restoresTheCurrentListChildWithoutReplayingCompletedParentsOrSiblings() throws Exception {
        String fa = child("1", "forms");
        String fb = child("2", "forms");
        String ca = child("1", "contacts");
        String cb = child("2", "contacts");
        ObjectNode first = record("first").put("contact_list_id", "1");
        ObjectNode second = record("second").put("contact_list_id", "2");
        ObjectNode one = record("1");
        ObjectNode two = record("2");
        ObjectNode contact = record("contact");
        List<ObjectNode> already =
                List.of(
                        out("lists", "1", "1", one),
                        out("lists", "2", "2", two),
                        out("forms", "first", "1", first));
        reset(ok(LISTS, rows(one, two)), ok(fa, rows(first)), stall(fb));
        try (Job job =
                job(
                        List.of("lists", "forms", "contacts"),
                        options().put("request_timeout_ms", 60000),
                        true,
                        false)) {
            saveActive(job, 3, already, fb);
            queue(ok(fb, rows(second)), ok(ca, rows()), ok(cb, rows(contact)));
            good(engine.restoreJob(job.configName, job.id));
            List<ObjectNode> expected = new ArrayList<>(already);
            expected.add(out("forms", "second", "2", second));
            expected.add(out("contacts", "contact", "2", contact));
            assertOutput(job, expected);
        }
        assertTrace(wire(LISTS), wire(fa), wire(fb), wire(fb), wire(ca), wire(cb));
    }

    @Test
    void rejectsATenantChangeBeforeAnyRecoveredProviderRequest() throws Exception {
        reset(stall(LISTS));
        try (Job job =
                        job(
                                List.of("contacts"),
                                options().put("request_timeout_ms", 60000),
                                true,
                                false);
                Job different =
                        job(
                                List.of("contacts"),
                                options().put("sub_domain", "tenant-2"),
                                true,
                                false)) {
            saveActive(job, 1, List.of(), LISTS);
            bad(engine.restoreJob(different.configName, job.id), "read scope changed");
        }
        assertTrace(wire(LISTS));
    }

    @Test
    void restoresTerminalStateBesideAnUnfinishedIndependentPipeline() throws Exception {
        ObjectNode email = record("mail-terminal");
        ObjectNode expected = out("emails", "mail-terminal", null, email);
        reset(ok(EMAILS, rows(email)));
        try (Job job = job(List.of("emails"), options(), true, true)) {
            saveActive(job, 1, List.of(expected), null);
            Future<Container.ExecResult> restored =
                    submissions.submit(() -> engine.restoreJob(job.configName, job.id));
            try {
                await(
                        () -> "RUNNING".equals(engine.getJobStatus(job.id)),
                        "active restored anchor");
                long before = engine.checkpointCount(job.id);
                await(() -> engine.checkpointCount(job.id) > before, "new anchor checkpoint");
                assertOutput(job, List.of(expected));
                assertTrace(wire(EMAILS));
            } finally {
                cancel(job.id, restored);
            }
        }
    }

    @Test
    void restoresOnlyTheUnpublishedArraySuffixAndRejectsChangedPayloadOrOrder() throws Exception {
        List<String> ids = new ArrayList<>();
        ArrayNode data = rows();
        for (int n = 1; n <= 33; n++) {
            String id = "report-" + n;
            ids.add(id);
            data.add(record(id));
        }
        ArrayNode changed = data.deepCopy();
        changed.set(32, record("report-33").put("name", "changed"));
        ArrayNode reordered = data.deepCopy();
        reordered.set(0, data.get(1));
        reordered.set(1, data.get(0));
        reset(ok(REPORTS, data), ok(REPORTS, data), ok(REPORTS, changed), ok(REPORTS, reordered));
        CampaynRecoveryProbe reader = recovery(List.of("reports"));
        List<String> initial = new ArrayList<>();
        byte[] snapshot = reader.firstChunk(initial);
        assertEquals(ids.subList(0, 32), initial);
        String saved = new String(snapshot, StandardCharsets.ISO_8859_1);
        assertFalse(saved.contains(TOKEN) || saved.contains("report-1"));
        List<String> suffix = new ArrayList<>();
        reader.consume(snapshot, suffix);
        assertEquals(ids.subList(32, 33), suffix);
        for (int n = 0; n < 2; n++) {
            List<String> invalid = new ArrayList<>();
            Exception refusal =
                    assertThrows(Exception.class, () -> reader.consume(snapshot, invalid));
            assertTrue(
                    refusal.getMessage().contains("provider collection changed since checkpoint"));
            assertTrue(invalid.isEmpty());
        }
        assertTrace(wire(REPORTS), wire(REPORTS), wire(REPORTS), wire(REPORTS));
    }

    @Test
    void restoresUnassignedCoordinatorAndFinishesEmptyOrCancelledReadersWithoutTransport()
            throws Exception {
        reset();
        CampaynRecoveryProbe reader = recovery(List.of("emails"));
        byte[] assigned = reader.recoverUnassigned();
        reader.finishWithoutAssignment();
        List<String> output = new ArrayList<>();
        reader.stopBeforeAssignment(output);
        assertTrue(output.isEmpty());
        assertTrace();
        queue(ok(EMAILS, rows(record("final"))));
        reader.consume(assigned, output);
        assertEquals(List.of("final"), output);
        assertTrace(wire(EMAILS));
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

    private CampaynRecoveryProbe recovery(List<String> resources) throws Exception {
        URI url =
                new URI("https", null, peer.getHost(), peer.getMappedPort(18443), null, null, null);
        return new CampaynRecoveryProbe(url.toString(), certificate.toString(), TOKEN, resources);
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
        String name = "campayn-" + UUID.randomUUID();
        String path = "/tmp/seatunnel_mnt/" + name;
        ObjectNode settings =
                options()
                        .put("plugin_output", "campayn_data")
                        .put("token", TOKEN)
                        .put("sub_domain", TENANT)
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
                        + "source {\n Campayn "
                        + settings
                        + "\n"
                        + anchorInput
                        + "}\n"
                        + "sink {\n LocalFile {\n plugin_input=campayn_data\n path=\""
                        + path
                        + "\"\n"
                        + " file_format_type=json\n is_enable_transaction=true\n custom_filename=true\n"
                        + " file_name_expression=\"campayn_$"
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

    private static String child(String id, String resource) {
        return "/api/v1/lists/" + id + "/" + resource + ".json";
    }

    private static ArrayNode rows(JsonNode... records) {
        ArrayNode array = JSON.createArrayNode();
        for (JsonNode record : records) {
            array.add(record);
        }
        return array;
    }

    private static ObjectNode out(String resource, String id, String list, ObjectNode payload) {
        ObjectNode row =
                options()
                        .put("resource", resource)
                        .put("record_id", id)
                        .put("sub_domain", TENANT)
                        .put("list_id", list);
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
            assertEquals("true", parts[4], "expected the complete TRUEREST authentication header");
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

    private static Seen wire(String path) {
        return new Seen(path, Map.of(), 0L);
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
        private final String id = CampaynIT.id();

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
                                    + "' -maxdepth 1 -type f -name 'campayn_*' -exec cat {} +; fi");
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
            // The Campayn pipeline can already have ended. Observe all active pipeline counters.
            long completed = 0;
            for (JsonNode pipeline : JSON.readTree(response.body()).path("pipelines")) {
                completed += pipeline.path("counts").path("completed").asLong();
            }
            return completed;
        }
    }
}
