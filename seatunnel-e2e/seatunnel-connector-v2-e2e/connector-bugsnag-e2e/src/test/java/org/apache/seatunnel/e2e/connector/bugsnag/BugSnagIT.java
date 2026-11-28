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
package org.apache.seatunnel.e2e.connector.bugsnag;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.bugsnag.BugSnagRecoveryProbe;
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
 * Runs BugSnag through a real SeaTunnel Zeta engine and an isolated Java TLS account.
 *
 * <p>Tests compare page numbers and bounds in captured GET requests and inspect LocalFile's
 * committed JSON rows. The provider peer never calls the real billable API.
 */
class BugSnagIT extends TestSuiteBase {
    private static final ObjectMapper JSON =
            new ObjectMapper()
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    private static final String TOKEN = "bugsnag-fixture-private+/=?";
    private static final String HIDDEN_MESSAGE = "private-provider-response-token";
    private static final String HOST = "bugsnag-peer";
    private static final String PEM = "/tmp/bugsnag.pem";
    private static final String OTHER_PEM = "/tmp/unrelated-bugsnag.pem";
    private static final String ENDPOINT = "https://" + HOST + ":18443";
    private static final String START = "2025-01-01T00:00:00.100000001Z";
    private static final String END = "2025-02-01T00:00:00.123456789Z";
    private static final String OLD = "2024-12-31T10:00:00Z";
    private static final String IN_WINDOW = "2025-01-15T12:30:30Z";
    private static final String ORGANIZATIONS = "/user/organizations";
    private static final String INTEGRATIONS = "/integrations";
    private static final List<String> ALL =
            List.of(
                    "organizations",
                    "projects",
                    "saved_searches",
                    "saved_searches_usage_summary",
                    "errors",
                    "events",
                    "pivots",
                    "supported_integrations",
                    "collaborators",
                    "teams",
                    "event_fields",
                    "releases",
                    "trace_fields");

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
                "keytool -genkeypair -alias provider -storetype PKCS12 -storepass bugsnag-fixture "
                        + "-keyalg RSA -keysize 2048 -validity 30 -dname CN="
                        + HOST
                        + " -ext SAN=dns:"
                        + HOST
                        + ",dns:bugsnag-redirect-host,"
                        + runnerType
                        + runner
                        + " -keystore ";
        String export =
                "keytool -exportcert -rfc -alias provider -storepass bugsnag-fixture -keystore ";
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
                        + BugSnagPeer.class.getName()
                        + " /tmp/provider.p12";
        String providerClass = "/" + BugSnagPeer.class.getName().replace('.', '/') + ".class";
        byte[] bytecode;
        try (InputStream bits = BugSnagPeer.class.getResourceAsStream(providerClass)) {
            if (bits == null) {
                throw new IllegalStateException("Missing packaged HTTPS fixture");
            }
            bytecode = bits.readAllBytes();
        }
        peer =
                new GenericContainer<>(DockerImageName.parse("eclipse-temurin:11-jdk"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST, "bugsnag-other-host", "bugsnag-redirect-host")
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
    void readsAllThirteenResourcesWithExactAncestryAndPersistedPayloads() throws Exception {
        Scenario expected = new Scenario();
        ObjectNode org = record("first").put("name", "研发 🧭");
        ObjectNode second = record("second").putNull("billing_emails");
        expected.page(
                sized(ORGANIZATIONS, 2, null),
                rows(org),
                "</ignore?offset=2>; rel=prev; title=\"previous, with punctuation\"",
                "<"
                        + ENDPOINT
                        + ORGANIZATIONS
                        + "?offset=next-organization&per_page=2&direction=desc&sort=created_at>; rel=\"next\"");
        expected.row(out("organizations", "first", "first", null, null, null, org));
        allProjectResources(expected, "first", "shared-project", IN_WINDOW, 17);
        organizationResources(expected, "first");
        expected.page(sized(ORGANIZATIONS, 2, "next-organization"), rows(second));
        expected.row(out("organizations", "second", "second", null, null, null, second));
        allProjectResources(expected, "second", "shared-project", OLD, 29);
        organizationResources(expected, "second");
        ObjectNode integration = options().put("key", "slack.like-example").putNull("fields");
        expected.page(wire(INTEGRATIONS), rows(integration));
        expected.row(
                out(
                        "supported_integrations",
                        "slack.like-example",
                        null,
                        null,
                        null,
                        null,
                        integration));
        expected.check(ALL, options().put("page_size", 2));
    }

    private static void allProjectResources(
            Scenario expected, String org, String projectId, String created, int count) {
        ObjectNode parent =
                record(projectId).put("organization_id", org).put("created_at", created);
        expected.page(sized(organization(org, "projects"), 2, null), rows(parent));
        if (IN_WINDOW.equals(created)) {
            expected.row(out("projects", projectId, org, projectId, null, created, parent));
        }

        String searchId = "shared-search";
        ObjectNode saved = record(searchId).put("project_id", projectId);
        saved.putObject("filters")
                .putArray("error.status")
                .addObject()
                .put("type", "eq")
                .put("value", "open");
        expected.page(wire(project(projectId, "saved_searches")), rows(saved));
        expected.row(out("saved_searches", searchId, org, projectId, searchId, null, saved));
        ObjectNode detail = summary(count).put("performance_monitor_count", 1);
        detail.putObject("unknown").putNull("optional");
        expected.page(wire(usage(searchId)), detail);
        expected.row(
                out(
                        "saved_searches_usage_summary",
                        searchId,
                        org,
                        projectId,
                        searchId,
                        null,
                        detail));

        ObjectNode error =
                record("error-1").put("project_id", projectId).put("last_seen", IN_WINDOW);
        error.put("event_count", new BigInteger("9223372036854775808"));
        expected.page(sized(project(projectId, "errors"), 2, null), rows(error));
        expected.row(out("errors", "error-1", org, projectId, null, IN_WINDOW, error));

        ObjectNode event =
                record("event-1").put("received_at", IN_WINDOW).put("is_full_report", true);
        event.put("score", new BigDecimal("123456789012345678.123456789000"));
        event.putObject("custom").putNull("a").put("name", "崩溃原因");
        event.put("url", "https://bugsnag-redirect-host:18443/never-follow-record-urls");
        expected.page(sized(project(projectId, "events"), 2, null), rows(event));
        expected.row(out("events", "event-1", org, projectId, null, IN_WINDOW, event));

        ObjectNode pivot =
                options().put("event_field_display_id", "u:locale").put("average", count);
        expected.page(wire(project(projectId, "pivots")), rows(pivot));
        expected.row(out("pivots", "u:locale", org, projectId, null, null, pivot));
        ObjectNode field = options().put("display_id", "event/field").putNull("values");
        expected.page(wire(project(projectId, "event_fields")), rows(field));
        expected.row(out("event_fields", "event/field", org, projectId, null, null, field));

        ObjectNode release =
                record("release-1")
                        .put("project_id", projectId)
                        .put("release_time", "2025-01-15T13:30:30+01:00");
        expected.page(sized(project(projectId, "releases"), 2, "0"), rows(release));
        expected.page(sized(project(projectId, "releases"), 2, "1"), rows());
        expected.row(out("releases", "release-1", org, projectId, null, IN_WINDOW, release));

        ObjectNode traceField = options().put("display_id", "span.attr").put("custom", true);
        expected.page(wire(project(projectId, "trace_fields")), rows(traceField));
        expected.row(out("trace_fields", "span.attr", org, projectId, null, null, traceField));
    }

    private static void organizationResources(Scenario expected, String org) {
        ObjectNode collaborator = record("member-1").put("email", "fixture@example.invalid");
        collaborator.putArray("project_ids").add("irrelevant-project");
        String route = organization(org, "collaborators");
        expected.page(
                sized(route, 2, null), rows(collaborator), "<?offset=continue-members>; rel=next");
        expected.page(sized(route, 2, "continue-members"), rows());
        expected.row(out("collaborators", "member-1", org, null, null, null, collaborator));

        ObjectNode team = record("team-1").put("name", "團隊");
        expected.page(sized(organization(org, "teams"), 2, null), rows(team));
        expected.row(out("teams", "team-1", org, null, null, null, team));
    }

    @Test
    void discoversSummaryChildrenAcrossUnselectedParentsWithReusedSearchIds() throws Exception {
        Scenario expected = new Scenario();
        expected.page(sized(ORGANIZATIONS, 50, null), rows(record("org")));
        String route = organization("org", "projects");
        expected.page(
                sized(route, 50, null),
                rows(record("p1").put("created_at", OLD)),
                "<?offset=project-two>; rel=next");
        expected.page(wire(project("p1", "saved_searches")), rows(record("same")));
        ObjectNode first = summary(1);
        expected.page(wire(usage("same")), first);
        expected.row(out("saved_searches_usage_summary", "same", "org", "p1", "same", null, first));
        expected.page(sized(route, 50, "project-two"), rows(record("p2").put("created_at", OLD)));
        expected.page(wire(project("p2", "saved_searches")), rows(record("same")));
        ObjectNode second = summary(2);
        expected.page(wire(usage("same")), second);
        expected.row(
                out("saved_searches_usage_summary", "same", "org", "p2", "same", null, second));
        expected.check(List.of("saved_searches_usage_summary"), options());
    }

    @Test
    void keepsEventReportsAndTheAdvertisedBaseAcrossRelativePagesAndNanosecondCutoffs()
            throws Exception {
        Scenario expected = new Scenario();
        expected.page(sized(ORGANIZATIONS, 2, null), rows(record("org")));
        expected.page(
                sized(organization("org", "projects"), 2, null),
                rows(record("p1").put("created_at", OLD)));
        String route = project("p1", "events");
        String base = "2025-02-01T00:00:00Z";
        ObjectNode tooOld = record("early").put("received_at", "2025-01-01T00:00:00.100000000Z");
        ObjectNode first = record("eq-1").put("received_at", "2025-01-01T01:00:00.100000001+01:00");
        ObjectNode second = record("eq-2").put("received_at", START);
        ObjectNode atEnd = record("at-end").put("received_at", END);
        expected.page(
                sized(route, 2, null),
                rows(tooOld, first),
                "<?base=" + base + "&offset=next%2B1&sort=timestamp>; rel=next; title=\"a, b\"");
        expected.page(
                withQuery(sized(route, 2, "next+1"), "base", base),
                rows(),
                "<?offset=next%2F2>; rel=\"next alternate\"");
        expected.page(withQuery(sized(route, 2, "next/2"), "base", base), rows(second, atEnd));
        expected.row(out("events", "eq-1", "org", "p1", null, START, first));
        expected.row(out("events", "eq-2", "org", "p1", null, START, second));
        expected.check(List.of("events"), options().put("page_size", 2));
    }

    @Test
    void usesReleaseCapAndRawOffsetsWithoutDroppingShortOrFilteredPages() throws Exception {
        Scenario expected = new Scenario();
        expected.page(sized(ORGANIZATIONS, 50, null), rows(record("org")));
        expected.page(sized(organization("org", "projects"), 50, null), rows(record("project")));
        String route = project("project", "releases");
        ArrayNode raw = rows();
        for (int i = 0; i < 10; i++) {
            raw.add(record("old-" + i).put("release_time", OLD));
        }
        ObjectNode included = record("current").put("release_time", IN_WINDOW);
        expected.page(sized(route, 10, "0"), raw, "<?offset=10&per_page=10>; rel=\"next\"");
        expected.page(sized(route, 10, "10"), rows(included));
        expected.page(sized(route, 10, "11"), rows());
        expected.row(out("releases", "current", "org", "project", null, IN_WINDOW, included));
        expected.check(List.of("releases"), options());
    }

    @Test
    void observesAdvertisedPagingOnAnArrayWithoutAnInitialPerPageArgument() throws Exception {
        ObjectNode value = options().put("key", "tail");
        reset(
                ok(
                        INTEGRATIONS,
                        rows(),
                        "<?offset=token%2B%2F%3D%3F&per_page=30>; rel=\"next\"; title=\"a\\\"b,c\""),
                ok(INTEGRATIONS, rows(value), "</ignored?offset=prior>; rel=previous"));
        try (Job job = job(List.of("supported_integrations"), options(), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(
                    job,
                    List.of(out("supported_integrations", "tail", null, null, null, null, value)));
        }
        assertTrace(wire(INTEGRATIONS), withQuery(wire(INTEGRATIONS), "offset", "token+/=?"));
    }

    @Test
    void skipsUnselectedBranchesAndAllowsEmptyRootAndLeafCollections() throws Exception {
        reset(ok(ORGANIZATIONS, rows()), ok(INTEGRATIONS, rows()));
        try (Job job = job(ALL, options(), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of());
        }
        assertTrace(sized(ORGANIZATIONS, 50, null), wire(INTEGRATIONS));
    }

    private static Seen withQuery(Seen request, String key, String value) {
        Map<String, String> query = new TreeMap<>(request.query);
        query.put(key, value);
        return new Seen(request.route, query, 0L);
    }

    /**
     * One deterministic tape with independently expected HTTP requests and persisted rows.
     *
     * <p>Only the isolated provider consumes the scripted responses.
     */
    private final class Scenario {
        private final List<String> tape = new ArrayList<>();
        private final List<Seen> requests = new ArrayList<>();
        private final List<ObjectNode> output = new ArrayList<>();

        void page(Seen request, JsonNode body, String... links) {
            requests.add(request);
            tape.add(ok(request.route, body, links));
        }

        void row(ObjectNode row) {
            output.add(row);
        }

        void check(List<String> selected, ObjectNode configuration) throws Exception {
            reset(tape.toArray(new String[0]));
            try (Job job = job(selected, configuration, false, false)) {
                good(engine.executeJob(job.configName));
                assertOutput(job, output);
            }
            assertTrace(requests.toArray(new Seen[0]));
        }
    }

    @Test
    void rejectsCrossOriginWrongPathAndOtherUnscopedContinuationDestinations() throws Exception {
        List<String> targets =
                List.of(
                        "https://bugsnag-redirect-host:18443" + ORGANIZATIONS + "?offset=1",
                        "http://bugsnag-peer:18443" + ORGANIZATIONS + "?offset=1",
                        "https://internals@bugsnag-peer:18443" + ORGANIZATIONS + "?offset=1",
                        ORGANIZATIONS + "?offset=1#fragment",
                        "/organizations/second/teams?offset=1",
                        "https://bugsnag-peer:18444" + ORGANIZATIONS + "?offset=1");
        for (String target : targets) {
            reset(ok(ORGANIZATIONS, rows(record("valid")), "<" + target + ">; rel=next"));
            failJob(List.of("organizations"), options(), "continuation");
            assertTrace(sized(ORGANIZATIONS, 50, null));
        }
    }

    @Test
    void rejectsAmbiguousMalformedAndScopeChangingLinksBeforePublishingThePage() throws Exception {
        String[][] cases = {
            {"<?offset=1>; rel=next, <?offset=2>; rel=next", "ambiguous next pagination links"},
            {"<?offset=1>; rel=\"next", "malformed pagination Link"},
            {"<?offset=1>; rel=next; anchor=\"/elsewhere\"", "changed its anchor"},
            {"<?offset=1&offset=2>; rel=next", "duplicate continuation parameter"},
            {
                "<?offset=1&auth_token=" + HIDDEN_MESSAGE + ">; rel=next",
                "unsupported query parameter"
            },
            {"<?offset=1&per_page=1>; rel=next", "changed the fixed request parameters"},
            {"<?offset=1&direction=asc>; rel=next", "changed the fixed request parameters"},
            {"<?sort=created_at>; rel=next", "has no valid offset"},
            {"<?offset=%0A>; rel=next", "has no valid offset"}
        };
        for (String[] item : cases) {
            rejectOne(
                    ok(ORGANIZATIONS, rows(record("valid")), item[0]),
                    List.of("organizations"),
                    options(),
                    sized(ORGANIZATIONS, 50, null),
                    item[1]);
        }
    }

    @Test
    void rejectsAChangedEventsBaseAndADowngradeToSummaryReports() throws Exception {
        String projects = organization("org", "projects");
        String events = project("p", "events");
        reset(
                ok(ORGANIZATIONS, rows(record("org"))),
                ok(projects, rows(record("p"))),
                ok(events, rows(), "<?offset=1&full_reports=false>; rel=next"));
        failJob(List.of("events"), options(), "changed the fixed request parameters");
        assertTrace(
                sized(ORGANIZATIONS, 50, null), sized(projects, 50, null), sized(events, 50, null));

        String base = "2025-02-01T00:00:00Z";
        reset(
                ok(ORGANIZATIONS, rows(record("org"))),
                ok(projects, rows(record("p"))),
                ok(events, rows(), "<?offset=1&base=" + base + ">; rel=next"),
                ok(events, rows(), "<?offset=2&base=2025-02-03T00:00:00Z>; rel=next"));
        failJob(List.of("events"), options(), "changed the fixed request parameters");
        assertTrace(
                sized(ORGANIZATIONS, 50, null),
                sized(projects, 50, null),
                sized(events, 50, null),
                withQuery(sized(events, 50, "1"), "base", base));
    }

    @Test
    void detectsOffsetCyclesRepeatedRecordsAndScanLimits() throws Exception {
        reset(
                ok(ORGANIZATIONS, rows(record("one")), "<?offset=a>; rel=next"),
                ok(ORGANIZATIONS, rows(), "<?offset=b>; rel=next"),
                ok(ORGANIZATIONS, rows(), "<?offset=a>; rel=next"));
        failJob(List.of("organizations"), options(), "repeated a continuation offset");
        assertTrace(
                sized(ORGANIZATIONS, 50, null),
                sized(ORGANIZATIONS, 50, "a"),
                sized(ORGANIZATIONS, 50, "b"));

        reset(
                ok(ORGANIZATIONS, rows(record("one")), "<?offset=valid>; rel=next"),
                ok(ORGANIZATIONS, rows(record("one"))));
        failJob(List.of("organizations"), options(), "duplicate identifier");
        assertTrace(sized(ORGANIZATIONS, 50, null), sized(ORGANIZATIONS, 50, "valid"));

        reset(ok(ORGANIZATIONS, rows(), "<?offset=second>; rel=next"));
        failJob(
                List.of("organizations"),
                options().put("max_pages_per_scope", 1),
                "max_pages_per_scope");
        assertTrace(sized(ORGANIZATIONS, 50, null));

        reset(ok(ORGANIZATIONS, rows(record("org"))));
        failJob(List.of("projects"), options().put("max_requests", 1), "max_requests");
        assertTrace(sized(ORGANIZATIONS, 50, null));
    }

    @Test
    void validatesIdsAndParentScopeWithoutPublishingPartialRecords() throws Exception {
        rejectOne(
                ok(ORGANIZATIONS, rows(record("valid"), options())),
                List.of("organizations"),
                options(),
                sized(ORGANIZATIONS, 50, null),
                "valid textual provider identifier");
        rejectOne(
                ok(ORGANIZATIONS, rows(record("a"), record("a"))),
                List.of("organizations"),
                options(),
                sized(ORGANIZATIONS, 50, null),
                "duplicate identifier");
        rejectOne(
                ok(ORGANIZATIONS, rows(record("a"), record("b"))),
                List.of("organizations"),
                options().put("page_size", 1),
                sized(ORGANIZATIONS, 1, null),
                "response record limit");
        rejectOne(
                ok(ORGANIZATIONS, rows(record("a"), record("a%2Fb"))),
                ALL,
                options(),
                sized(ORGANIZATIONS, 50, null),
                "parent identifier is unsafe");

        String projects = organization("org", "projects");
        reset(
                ok(ORGANIZATIONS, rows(record("org"))),
                ok(
                        projects,
                        rows(
                                record("valid").put("created_at", IN_WINDOW),
                                record("foreign")
                                        .put("created_at", IN_WINDOW)
                                        .put("organization_id", "other"))));
        try (Job job = job(List.of("projects"), options(), false, false)) {
            bad(
                    engine.executeJob(job.configName),
                    "record does not belong to the requested parent");
            assertOutput(job, List.of());
        }
        assertTrace(sized(ORGANIZATIONS, 50, null), sized(projects, 50, null));
    }

    @Test
    void rejectsMalformedSingletonsAndUnexpectedSummaryContinuation() throws Exception {
        String projects = organization("org", "projects");
        String searches = project("p", "saved_searches");
        for (String reply :
                List.of(
                        ok(usage("s"), rows(summary(10))),
                        ok(usage("s"), options().put("message", HIDDEN_MESSAGE)),
                        ok(usage("s"), summary(-1)),
                        ok(usage("s"), summary(1), "<?offset=1>; rel=next"))) {
            reset(
                    ok(ORGANIZATIONS, rows(record("org"))),
                    ok(projects, rows(record("p"))),
                    ok(searches, rows(record("s"))),
                    reply);
            try (Job job = job(List.of("saved_searches_usage_summary"), options(), false, false)) {
                bad(engine.executeJob(job.configName), "usage summary");
                assertOutput(job, List.of());
            }
            assertTrace(
                    sized(ORGANIZATIONS, 50, null),
                    sized(projects, 50, null),
                    wire(searches),
                    wire(usage("s")));
        }
    }

    @Test
    void rejectsInvalidTimestampAndWrongReleaseOffset() throws Exception {
        String projects = organization("org", "projects");
        reset(
                ok(ORGANIZATIONS, rows(record("org"))),
                ok(projects, rows(record("p").put("created_at", "2025-02-30T00:00:00Z"))));
        failJob(List.of("projects"), options(), "record has no valid created_at timestamp");
        assertTrace(sized(ORGANIZATIONS, 50, null), sized(projects, 50, null));

        String releases = project("p", "releases");
        reset(
                ok(ORGANIZATIONS, rows(record("org"))),
                ok(projects, rows(record("p"))),
                ok(
                        releases,
                        rows(record("r1").put("release_time", IN_WINDOW)),
                        "<?offset=9>; rel=next"));
        failJob(List.of("releases"), options(), "release continuation did not match");
        assertTrace(
                sized(ORGANIZATIONS, 50, null),
                sized(projects, 50, null),
                sized(releases, 10, "0"));
    }

    @Test
    void rejectsPermissionAndProviderContentErrorsWithoutPublishingSecrets() throws Exception {
        for (int status : List.of(400, 401, 403, 404, 204, 206)) {
            rejectOne(
                    reply(
                            INTEGRATIONS,
                            status,
                            "plain",
                            "",
                            options().put("error", HIDDEN_MESSAGE)),
                    List.of("supported_integrations"),
                    options(),
                    wire(INTEGRATIONS),
                    "HTTP " + status);
        }
        rejectOne(
                ok(INTEGRATIONS, options().put("error", HIDDEN_MESSAGE)),
                List.of("supported_integrations"),
                options(),
                wire(INTEGRATIONS),
                "expected a JSON array");
        rejectOne(
                frame(
                        INTEGRATIONS,
                        200,
                        "plain",
                        "",
                        "[{\"key\":\"a\",\"key\":\"b\"}]".getBytes(StandardCharsets.UTF_8)),
                List.of("supported_integrations"),
                options(),
                wire(INTEGRATIONS),
                "single valid UTF-8 JSON");
    }

    @Test
    void respectsTheRetryAfterLowerBoundAndExplicitAttemptLimit() throws Exception {
        List<String> selected = List.of("supported_integrations");
        reset(reply(INTEGRATIONS, 429, "plain", "1", options()), ok(INTEGRATIONS, rows()));
        try (Job job = job(selected, options().put("max_attempts", 2), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of());
        }
        assertTrace(wire(INTEGRATIONS), wire(INTEGRATIONS));
        List<Seen> actual = seen();
        assertTrue(TimeUnit.NANOSECONDS.toMillis(actual.get(1).at - actual.get(0).at) >= 970L);

        rejectOne(
                reply(INTEGRATIONS, 429, "plain", "", options()),
                selected,
                options(),
                wire(INTEGRATIONS),
                "without a confirmed Retry-After delay");
        rejectOne(
                reply(INTEGRATIONS, 429, "plain", "90", options()),
                selected,
                options(),
                wire(INTEGRATIONS),
                "retry delay exceeds request deadline");
        rejectOne(
                reply(INTEGRATIONS, 503, "plain", "0", options()),
                selected,
                options().put("max_attempts", 1),
                wire(INTEGRATIONS),
                "HTTP 503 retry budget exhausted");
    }

    @Test
    void checksJsonCompressionEncodingAndTheDecodedResponseBound() throws Exception {
        List<String> selected = List.of("supported_integrations");
        ObjectNode unicode = options().put("key", "unicode").put("name", "跟踪");
        reset(reply(INTEGRATIONS, 200, "gzip", "", rows(unicode)));
        try (Job job = job(selected, options(), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(
                    job,
                    List.of(
                            out(
                                    "supported_integrations",
                                    "unicode",
                                    null,
                                    null,
                                    null,
                                    null,
                                    unicode)));
        }
        assertTrace(wire(INTEGRATIONS));

        ObjectNode big = unicode.deepCopy().put("long", "X".repeat(8192));
        rejectOne(
                reply(INTEGRATIONS, 200, "gzip", "", rows(big)),
                selected,
                options().put("max_response_bytes", 1024),
                wire(INTEGRATIONS),
                "response exceeds max_response_bytes");
        rejectOne(
                reply(INTEGRATIONS, 200, "unsupported", "", rows(unicode)),
                selected,
                options(),
                wire(INTEGRATIONS),
                "unsupported response compression");
        rejectOne(
                reply(INTEGRATIONS, 200, "binary", "", rows(unicode)),
                selected,
                options(),
                wire(INTEGRATIONS),
                "expected Content-Type application/json");
        rejectOne(
                frame(INTEGRATIONS, 200, "plain", "", new byte[] {'[', (byte) 0xFF, ']'}),
                selected,
                options(),
                wire(INTEGRATIONS),
                "single valid UTF-8 JSON");
    }

    @Test
    void retriesATruncatedGetBeforePublishingAnyOfItsPrefix() throws Exception {
        ObjectNode discarded = options().put("key", "discarded");
        ObjectNode kept = options().put("key", "complete");
        reset(
                reply(INTEGRATIONS, 200, "truncated", "", rows(discarded)),
                ok(INTEGRATIONS, rows(kept)));
        try (Job job =
                job(
                        List.of("supported_integrations"),
                        options().put("max_attempts", 2),
                        false,
                        false)) {
            good(engine.executeJob(job.configName));
            assertOutput(
                    job,
                    List.of(
                            out(
                                    "supported_integrations",
                                    "complete",
                                    null,
                                    null,
                                    null,
                                    null,
                                    kept)));
        }
        assertTrace(wire(INTEGRATIONS), wire(INTEGRATIONS));
    }

    @Test
    void checksCertificateHostnameAndRedirectHandling() throws Exception {
        List<String> selected = List.of("supported_integrations");
        reset();
        failJob(
                selected,
                options().put("trust_certificate", OTHER_PEM),
                "TLS certificate or hostname");
        failJob(
                selected,
                options().put("endpoint", "https://bugsnag-other-host:18443"),
                "TLS certificate or hostname");
        assertTrace();
        rejectOne(
                reply(INTEGRATIONS, 302, "redirect", "", options()),
                selected,
                options(),
                wire(INTEGRATIONS),
                "HTTP 302");
    }

    @Test
    void enforcesTheTotalDeadlineEvenWhileTheBodyMakesProgress() throws Exception {
        rejectOne(
                stall(INTEGRATIONS),
                List.of("supported_integrations"),
                options().put("request_timeout_ms", 2500),
                wire(INTEGRATIONS),
                "request deadline exceeded");
        await(() -> trace().contains("disconnect\t" + INTEGRATIONS), "deadline closed response");
    }

    @Test
    void cancelsTheOutstandingChildRequestWithoutStartingItsSibling() throws Exception {
        String projects = organization("org", "projects");
        String searches = project("p", "saved_searches");
        String summary = usage("s");
        reset(
                ok(ORGANIZATIONS, rows(record("org"))),
                ok(projects, rows(record("p"))),
                ok(searches, rows(record("s"), record("later"))),
                stall(summary));
        try (Job job =
                job(
                        List.of("saved_searches_usage_summary", "supported_integrations"),
                        options().put("request_timeout_ms", 60000),
                        true,
                        false)) {
            Future<Container.ExecResult> running = start(job, job.id);
            try {
                await(() -> seen().size() == 4, "blocked nested summary");
            } finally {
                cancel(job.id, running);
            }
            await(
                    () -> trace().contains("disconnect\t" + summary),
                    "cancel closed summary request");
            assertOutput(job, List.of());
        }
        assertTrace(
                sized(ORGANIZATIONS, 50, null),
                sized(projects, 50, null),
                wire(searches),
                wire(summary));
    }

    @Test
    void restoresTheThirdLevelSummaryWithoutReplayingCommittedParentRows() throws Exception {
        String projects = organization("org", "projects");
        String searches = project("p", "saved_searches");
        ObjectNode org = record("org");
        ObjectNode oldProject = record("p").put("created_at", OLD);
        ObjectNode saved = record("s");
        ObjectNode value = summary(9);
        ObjectNode parentOutput = out("organizations", "org", "org", null, null, null, org);
        ObjectNode searchOutput = out("saved_searches", "s", "org", "p", "s", null, saved);
        reset(
                ok(ORGANIZATIONS, rows(org)),
                ok(projects, rows(oldProject)),
                ok(searches, rows(saved)),
                stall(usage("s")));
        try (Job job =
                job(
                        List.of(
                                "organizations",
                                "projects",
                                "saved_searches",
                                "saved_searches_usage_summary"),
                        options().put("request_timeout_ms", 60000),
                        true,
                        false)) {
            saveActive(job, 4, List.of(parentOutput, searchOutput), usage("s"));
            queue(ok(usage("s"), value));
            good(engine.restoreJob(job.configName, job.id));
            assertOutput(
                    job,
                    List.of(
                            parentOutput,
                            searchOutput,
                            out(
                                    "saved_searches_usage_summary",
                                    "s",
                                    "org",
                                    "p",
                                    "s",
                                    null,
                                    value)));
        }
        assertTrace(
                sized(ORGANIZATIONS, 50, null),
                sized(projects, 50, null),
                wire(searches),
                wire(usage("s")),
                wire(usage("s")));
    }

    @Test
    void restoresFromTheExactEventOffsetAcrossASameTimestampBoundary() throws Exception {
        String projects = organization("org", "projects");
        String events = project("p", "events");
        ObjectNode a = record("before-savepoint").put("received_at", START);
        ObjectNode b = record("after-savepoint").put("received_at", START);
        String base = "2025-02-01T00:00:00Z";
        reset(
                ok(ORGANIZATIONS, rows(record("org"))),
                ok(projects, rows(record("p"))),
                ok(events, rows(a), "<?offset=event-next&base=" + base + ">; rel=next"),
                stall(events));
        ObjectNode before = out("events", "before-savepoint", "org", "p", null, START, a);
        ObjectNode after = out("events", "after-savepoint", "org", "p", null, START, b);
        try (Job job =
                job(List.of("events"), options().put("request_timeout_ms", 60000), true, false)) {
            saveActive(job, 4, List.of(before), events);
            queue(ok(events, rows(b)));
            good(engine.restoreJob(job.configName, job.id));
            assertOutput(job, List.of(before, after));
        }
        Seen next = withQuery(sized(events, 50, "event-next"), "base", base);
        assertTrace(
                sized(ORGANIZATIONS, 50, null),
                sized(projects, 50, null),
                sized(events, 50, null),
                next,
                next);
    }

    @Test
    void refusesChangedWindowBeforeRequestingAnotherPageAfterRestore() throws Exception {
        reset(stall(ORGANIZATIONS));
        try (Job original = job(ALL, options().put("request_timeout_ms", 60000), true, false);
                Job changed =
                        job(ALL, options().put("end_date", "2025-03-01T00:00:00Z"), true, false)) {
            saveActive(original, 1, List.of(), ORGANIZATIONS);
            bad(engine.restoreJob(changed.configName, original.id), "read scope changed");
        }
        assertTrace(sized(ORGANIZATIONS, 50, null));
    }

    @Test
    void restoresTerminalStateWhileAnotherPipelineKeepsCheckpointing() throws Exception {
        ObjectNode value = options().put("key", "terminal");
        ObjectNode expected =
                out("supported_integrations", "terminal", null, null, null, null, value);
        reset(ok(INTEGRATIONS, rows(value)));
        try (Job job = job(List.of("supported_integrations"), options(), true, true)) {
            saveActive(job, 1, List.of(expected), null);
            Future<Container.ExecResult> restored =
                    submissions.submit(() -> engine.restoreJob(job.configName, job.id));
            try {
                await(
                        () -> "RUNNING".equals(engine.getJobStatus(job.id)),
                        "active restored anchor");
                long before = engine.checkpointCount(job.id);
                await(() -> engine.checkpointCount(job.id) > before, "next active checkpoint");
                assertOutput(job, List.of(expected));
                assertTrace(wire(INTEGRATIONS));
            } finally {
                cancel(job.id, restored);
            }
        }
    }

    @Test
    void rejectsInvalidOptionsBeforeContactingTheProvider() throws Exception {
        reset();
        failJob(ALL, options().put("start_date", "2025-02-30T00:00:00Z"), "RFC 3339 timestamp");
        failJob(ALL, options().put("start_date", END), "must be earlier than the scan end");
        failJob(ALL, options().put("token", "token " + TOKEN), "without a prefix");
        failJob(ALL, options().put("page_size", 101), "expected a value from 1 to 100");
        failJob(
                List.of("organizations", "organizations"),
                options(),
                "choose each resource only once");
        failJob(List.of("invented"), options(), "unknown resource name");
        failJob(
                ALL,
                options().put("endpoint", "http://bugsnag-peer:18443"),
                "expected a bare HTTPS origin");
        assertTrace();
    }

    @Test
    void resumesOnlyUnpublishedRowsAndRefusesChangedBodyOrContinuation() throws Exception {
        List<String> wanted = new ArrayList<>();
        ArrayNode page = rows();
        for (int n = 1; n <= 33; n++) {
            String name = "integration-" + n;
            wanted.add(name);
            page.add(options().put("key", name));
        }
        ArrayNode changed = page.deepCopy();
        changed.set(32, options().put("key", "integration-33").put("name", "changed"));
        String next = "<?offset=original>; rel=next";
        reset(
                ok(INTEGRATIONS, page, next),
                ok(INTEGRATIONS, page, next),
                ok(INTEGRATIONS, rows()),
                ok(INTEGRATIONS, changed, next),
                ok(INTEGRATIONS, page, "<?offset=changed>; rel=next"));
        BugSnagRecoveryProbe reader = recovery(List.of("supported_integrations"));
        List<String> first = new ArrayList<>();
        byte[] snapshot = reader.firstChunk(first);
        assertEquals(wanted.subList(0, 32), first);
        String serialized = new String(snapshot, StandardCharsets.ISO_8859_1);
        assertFalse(serialized.contains(TOKEN) || serialized.contains("integration-1"));
        List<String> resumed = new ArrayList<>();
        reader.consume(snapshot, resumed);
        assertEquals(wanted.subList(32, 33), resumed);
        for (int n = 0; n < 2; n++) {
            List<String> rejected = new ArrayList<>();
            Exception mismatch =
                    assertThrows(Exception.class, () -> reader.consume(snapshot, rejected));
            assertTrue(mismatch.getMessage().contains("provider page changed since checkpoint"));
            assertTrue(rejected.isEmpty());
        }
        assertTrace(
                wire(INTEGRATIONS),
                wire(INTEGRATIONS),
                withQuery(wire(INTEGRATIONS), "offset", "original"),
                wire(INTEGRATIONS),
                wire(INTEGRATIONS));
    }

    @Test
    void restoresTheUnassignedCoordinatorAndHandlesEmptyAssignmentAndStickyCancellation()
            throws Exception {
        reset();
        BugSnagRecoveryProbe reader = recovery(List.of("supported_integrations"));
        byte[] pending = reader.recoverUnassigned();
        reader.finishWithoutAssignment();
        List<String> output = new ArrayList<>();
        reader.stopBeforeAssignment(output);
        assertTrue(output.isEmpty());
        assertTrace();
        queue(ok(INTEGRATIONS, rows(options().put("key", "after-assign"))));
        reader.consume(pending, output);
        assertEquals(List.of("after-assign"), output);
        assertTrace(wire(INTEGRATIONS));
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

    private BugSnagRecoveryProbe recovery(List<String> resources) throws Exception {
        URI url =
                new URI("https", null, peer.getHost(), peer.getMappedPort(18443), null, null, null);
        return new BugSnagRecoveryProbe(url.toString(), certificate.toString(), TOKEN, resources);
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
        String name = "bugsnag-" + UUID.randomUUID();
        String path = "/tmp/seatunnel_mnt/" + name;
        ObjectNode settings =
                options()
                        .put("plugin_output", "bugsnag_data")
                        .put("token", TOKEN)
                        .put("start_date", START)
                        .put("end_date", END)
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
                        + "source {\n BugSnag "
                        + settings
                        + "\n"
                        + anchorInput
                        + "}\n"
                        + "sink {\n LocalFile {\n plugin_input=bugsnag_data\n path=\""
                        + path
                        + "\"\n"
                        + " file_format_type=json\n is_enable_transaction=true\n custom_filename=true\n"
                        + " file_name_expression=\"bugsnag_$"
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

    private static String organization(String id, String resource) {
        return "/organizations/" + id + "/" + resource;
    }

    private static String project(String id, String resource) {
        return "/projects/" + id + "/" + resource;
    }

    private static String usage(String id) {
        return "/saved_searches/" + id + "/usage_summary";
    }

    private static ObjectNode record(String id) {
        return options().put("id", id);
    }

    private static ObjectNode summary(int count) {
        return options()
                .put("project_notifications_count", count)
                .put("collaborator_email_notifications_count", 0)
                .put("current_user_using_for_email_notification", false);
    }

    private static ArrayNode rows(JsonNode... records) {
        ArrayNode array = JSON.createArrayNode();
        for (JsonNode record : records) {
            array.add(record);
        }
        return array;
    }

    private static ObjectNode out(
            String resource,
            String id,
            String org,
            String project,
            String search,
            String time,
            ObjectNode payload) {
        ObjectNode row =
                options()
                        .put("resource", resource)
                        .put("record_id", id)
                        .put("organization_id", org)
                        .put("project_id", project)
                        .put("search_id", search)
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
            assertEquals("true", parts[4], "expected the personal token authorization scheme");
            assertEquals("true", parts[5], "the connector must ask for JSON");
            URI request = URI.create(parts[2]);
            assertEquals("true", parts[6], "expected X-Version 2");
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

    private static Seen sized(String path, int size, String offset) {
        Map<String, String> query = new TreeMap<>();
        query.put("per_page", String.valueOf(size));
        if (path.endsWith("/projects")) {
            query.put("sort", "created_at");
            query.put("direction", "desc");
        } else if (path.endsWith("/errors")) {
            query.put("sort", "last_seen");
            query.put("direction", "desc");
        } else if (path.endsWith("/events")) {
            query.put("sort", "timestamp");
            query.put("direction", "desc");
            query.put("full_reports", "true");
        } else if (path.endsWith("/releases")) {
            query.put("sort", "timestamp");
            query.put("base", END);
        }
        if (offset != null) {
            query.put("offset", offset);
        }
        return new Seen(path, query, 0L);
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
        private final String id = BugSnagIT.id();

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
                                    + "' -maxdepth 1 -type f -name 'bugsnag_*' -exec cat {} +; fi");
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
            // The BugSnag pipeline can already have ended. Observe all active pipeline counters.
            long completed = 0;
            for (JsonNode pipeline : JSON.readTree(response.body()).path("pipelines")) {
                completed += pipeline.path("counts").path("completed").asLong();
            }
            return completed;
        }
    }
}
