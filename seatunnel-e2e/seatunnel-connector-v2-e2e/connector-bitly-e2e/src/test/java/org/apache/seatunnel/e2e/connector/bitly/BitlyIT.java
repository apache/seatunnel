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
package org.apache.seatunnel.e2e.connector.bitly;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.bitly.BitlyRecoveryProbe;
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
import java.net.URI;
import java.net.URLDecoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
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
 * Runs Bitly through a real SeaTunnel Zeta engine and an isolated Java TLS account.
 *
 * <p>Tests compare page numbers and bounds in captured GET requests and inspect LocalFile's
 * committed JSON rows. The provider peer never calls the real billable API.
 */
class BitlyIT extends TestSuiteBase {
    private static final ObjectMapper JSON =
            new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    private static final String TOKEN = "bitly-fixture-private+/=?";
    private static final String HIDDEN_MESSAGE = "private-provider-response-token";
    private static final String HOST = "bitly-peer";
    private static final String PEM = "/tmp/bitly.pem";
    private static final String OTHER_PEM = "/tmp/unrelated-bitly.pem";
    private static final String ENDPOINT = "https://" + HOST + ":18443";
    private static final String START = "2024-03-01T00:00:00Z";
    private static final String END = "2024-04-01T00:00:00Z";
    private static final String METRIC_REFERENCE = "2024-04-01T00:00:00+0000";
    private static final String ORGANIZATIONS = "/v4/organizations";
    private static final String GROUPS = "/v4/groups";
    private static final String LINKS = "/v4/groups/GA/bitlinks";
    private static final String QRS = "/v4/groups/GA/qr-codes";
    private static final String CAMPAIGNS = "/v4/campaigns";
    private static final String BSD = "/v4/bsds";
    private static final List<String> ALL =
            List.of(
                    "bsds",
                    "campaigns",
                    "channels",
                    "groups",
                    "group_preferences",
                    "group_shorten_counts",
                    "organizations",
                    "organization_shorten_counts",
                    "bitlinks",
                    "bitlink_clicks",
                    "qr_codes");
    private static final String COHORT = "2024-03-15T12:00:00.123456789Z";

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
                "keytool -genkeypair -alias provider -storetype PKCS12 -storepass bitly-fixture "
                        + "-keyalg RSA -keysize 2048 -validity 30 -dname CN="
                        + HOST
                        + " -ext SAN=dns:"
                        + HOST
                        + ",dns:bitly-redirect-host,"
                        + runnerType
                        + runner
                        + " -keystore ";
        String export =
                "keytool -exportcert -rfc -alias provider -storepass bitly-fixture -keystore ";
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
                        + BitlyPeer.class.getName()
                        + " /tmp/provider.p12";
        String providerClass = "/" + BitlyPeer.class.getName().replace('.', '/') + ".class";
        byte[] bytecode;
        try (InputStream bits = BitlyPeer.class.getResourceAsStream(providerClass)) {
            if (bits == null) {
                throw new IllegalStateException("Missing packaged HTTPS fixture");
            }
            bytecode = bits.readAllBytes();
        }
        peer =
                new GenericContainer<>(DockerImageName.parse("eclipse-temurin:11-jdk"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST, "bitly-other-host", "bitly-redirect-host")
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
    void loadsAllElevenResourcesThroughOrganizationGroupLinkAndQrRelations() throws Exception {
        String prefsA = "/v4/groups/GA/preferences";
        String prefsB = "/v4/groups/GB/preferences";
        String countA = "/v4/groups/GA/shorten_counts";
        String countB = "/v4/groups/GB/shorten_counts";
        String countOrgA = "/v4/organizations/OA/shorten_counts";
        String countOrgB = "/v4/organizations/OB/shorten_counts";
        String linksB = "/v4/groups/GB/bitlinks";
        String qrB = "/v4/groups/GB/qr-codes";
        String clickOld = "/v4/bitlinks/brand.example/Caf%C3%A9/clicks";
        String clickNew = "/v4/bitlinks/bit.ly/current/clicks";
        String qrDetail = "/v4/qr-codes/QA";
        String qrImage = qrDetail + "/image";
        String qrAtEnd = "/v4/qr-codes/QEnd";
        ObjectNode bsds = list("bsds", JSON.getNodeFactory().textNode("brand.example"));
        ObjectNode campaign = parent("CA", START).put("group_guid", "GA").put("name", "春季 campaña");
        ObjectNode channel = parent("CH", COHORT).put("group_guid", "GA").putNull("title");
        ObjectNode oldOrg = parent("OA", "2023-12-31T00:00:00+0000");
        ObjectNode newOrg = parent("OB", COHORT).putNull("references");
        ObjectNode oldGroup =
                parent("GA", "2024-02-29T23:59:59.999999999Z").put("organization_guid", "OA");
        ObjectNode newGroup = parent("GB", START).put("organization_guid", "OA");
        ObjectNode preferencesA =
                options().put("group_guid", "GA").put("domain_preference", "brand.example");
        ObjectNode preferencesB = options().put("group_guid", "GB").putNull("domain_preference");
        ObjectNode counted =
                list(
                        "metrics",
                        options()
                                .put("key", "2024-03-01")
                                .put("value", new BigDecimal("9223372036854775808.123456789")));
        counted.put("facet", "shorten_counts")
                .put("unit", "day")
                .put("units", -1)
                .put("unit_reference", METRIC_REFERENCE);
        ObjectNode oldLink = link("brand.example/Café", "2021-01-01T00:00:00+0000");
        ObjectNode newLink =
                link("bit.ly/current", "2024-03-01T05:30:00+0530")
                        .put("archived", true)
                        .put("title", "你好 🧭");
        newLink.putObject("references").put("private_hint", "https://destination.invalid/data");
        ObjectNode clicked =
                list("link_clicks", options().put("clicks", 23L).put("date", START))
                        .put("unit", "day")
                        .put("units", -1)
                        .put("unit_reference", METRIC_REFERENCE);
        ObjectNode qr = qr("QA", "GA", "bit.ly/current", COHORT);
        qr.putObject("render_customizations").put("background_color", "#332244");
        ObjectNode expired = qr("QEnd", "GA", "bit.ly/current", END);
        ObjectNode image = options().put("qr_code_image", "data:image/svg+xml;base64,PHN2Zy8+");

        reset(
                ok(BSD, bsds),
                ok(CAMPAIGNS, list("campaigns", campaign)),
                ok("/v4/channels", list("channels", channel)),
                ok(ORGANIZATIONS, list("organizations", oldOrg, newOrg)),
                ok(countOrgA, counted),
                ok(GROUPS, list("groups", oldGroup, newGroup)),
                ok(prefsA, preferencesA),
                ok(countA, counted),
                ok(LINKS, page("links", "", oldLink, newLink)),
                ok(clickOld, clicked),
                ok(clickNew, clicked),
                ok(QRS, page("qr_codes", "", qr, expired)),
                ok(qrDetail, qr),
                ok(qrImage, image),
                ok(qrAtEnd, expired),
                ok(prefsB, preferencesB),
                ok(countB, counted),
                ok(linksB, page("links", "", newLink)),
                ok(clickNew, clicked),
                ok(qrB, page("qr_codes", "")),
                ok(countOrgB, counted),
                ok(GROUPS, list("groups")));

        try (Job job = job(ALL, options(), false, false)) {
            good(engine.executeJob(job.configName));
            ObjectNode qrPayload = options();
            qrPayload.set("metadata", qr);
            qrPayload.set("image", image);
            assertOutput(
                    job,
                    List.of(
                            out("bsds", "bsds", null, null, null, bsds),
                            out("campaigns", "CA", null, "GA", null, campaign),
                            out("channels", "CH", null, "GA", null, channel),
                            out("organizations", "OB", "OB", null, null, newOrg),
                            out("organization_shorten_counts", "OA", "OA", null, null, counted),
                            out("organization_shorten_counts", "OB", "OB", null, null, counted),
                            out("groups", "GB", "OA", "GB", null, newGroup),
                            out("group_preferences", "GA", "OA", "GA", null, preferencesA),
                            out("group_preferences", "GB", "OA", "GB", null, preferencesB),
                            out("group_shorten_counts", "GA", "OA", "GA", null, counted),
                            out("group_shorten_counts", "GB", "OA", "GB", null, counted),
                            out(
                                    "bitlinks",
                                    "bit.ly/current",
                                    "OA",
                                    "GA",
                                    "bit.ly/current",
                                    newLink),
                            out(
                                    "bitlinks",
                                    "bit.ly/current",
                                    "OA",
                                    "GB",
                                    "bit.ly/current",
                                    newLink),
                            out(
                                    "bitlink_clicks",
                                    "brand.example/Café",
                                    "OA",
                                    "GA",
                                    "brand.example/Café",
                                    clicked),
                            out(
                                    "bitlink_clicks",
                                    "bit.ly/current",
                                    "OA",
                                    "GA",
                                    "bit.ly/current",
                                    clicked),
                            out(
                                    "bitlink_clicks",
                                    "bit.ly/current",
                                    "OA",
                                    "GB",
                                    "bit.ly/current",
                                    clicked),
                            out("qr_codes", "QA", "OA", "GA", "bit.ly/current", qrPayload)));
        }
        assertTrace(
                wire(BSD),
                wire(CAMPAIGNS),
                wire("/v4/channels"),
                wire(ORGANIZATIONS),
                metric(countOrgA),
                wire(GROUPS, Map.of("organization_guid", "OA")),
                wire(prefsA),
                metric(countA),
                links(LINKS, null, false),
                metric(clickOld),
                metric(clickNew),
                qrPage(QRS, null),
                wire(qrDetail),
                wire(qrImage, Map.of("format", "svg")),
                wire(qrAtEnd),
                wire(prefsB),
                metric(countB),
                links(linksB, null, false),
                metric(clickNew),
                qrPage(qrB, null),
                metric(countOrgB),
                wire(GROUPS, Map.of("organization_guid", "OB")));
    }

    @Test
    void preservesFiftyRecordsThenContinuesFromAnEmptyWindowUsingAnOpaqueLinkToken()
            throws Exception {
        ObjectNode first = page("links", "cursor+A/?=&");
        ((ObjectNode) first.get("pagination"))
                .put(
                        "next",
                        ENDPOINT + LINKS + "?search_after=cursor%2BA%2F%3F%3D%26&archived=both");
        List<ObjectNode> expected = new ArrayList<>();
        ArrayNode entries = (ArrayNode) first.get("links");
        for (int i = 0; i < 50; i++) {
            String id = "bit.ly/entry" + i;
            ObjectNode item = link(id, COHORT);
            entries.add(item);
            expected.add(out("bitlinks", id, "OA", "GA", id, item));
        }
        ObjectNode empty = page("links", "");
        ((ObjectNode) empty.get("pagination")).put("next", "?search_after=next%2B%26%2F%3D");
        ObjectNode tail = link("bit.ly/last", "2024-03-31T23:59:59.999999999Z");
        expected.add(out("bitlinks", "bit.ly/last", "OA", "GA", "bit.ly/last", tail));
        account(ok(LINKS, first), ok(LINKS, empty), ok(LINKS, page("links", "", tail)));
        try (Job job = job(List.of("bitlinks"), options(), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, expected);
        }
        assertAccountTrace(
                links(LINKS, null, true),
                links(LINKS, "cursor+A/?=&", true),
                links(LINKS, "next+&/=", true));
    }

    @Test
    void usesHalfOpenNanosecondBoundsAndConservativeEpochRequestValues() throws Exception {
        String start = "2024-03-01T00:00:00.000000100Z";
        String end = "2024-04-01T00:00:00.000000200Z";
        ObjectNode below = link("bit.ly/below", "2024-03-01T00:00:00.000000099+0000");
        ObjectNode first = link("bit.ly/first", "2024-03-01T05:30:00.000000100+05:30");
        ObjectNode last = link("bit.ly/last", "2024-04-01T00:00:00.000000199Z");
        ObjectNode outside = link("bit.ly/outside", end);
        account(ok(LINKS, page("links", "", below, first, last, outside)));
        try (Job job =
                job(
                        List.of("bitlinks"),
                        options().put("start_date", start).put("end_date", end),
                        false,
                        false)) {
            good(engine.executeJob(job.configName));
            assertOutput(
                    job,
                    List.of(
                            out("bitlinks", "bit.ly/first", "OA", "GA", "bit.ly/first", first),
                            out("bitlinks", "bit.ly/last", "OA", "GA", "bit.ly/last", last)));
        }
        assertAccountTrace(
                wire(
                        LINKS,
                        Map.of(
                                "size",
                                "50",
                                "archived",
                                "both",
                                "created_after",
                                String.valueOf(Instant.parse(START).getEpochSecond() - 1),
                                "created_before",
                                String.valueOf(Instant.parse(END).getEpochSecond() + 1))));
    }

    @Test
    void followsQrParentCursorAndEmitsOnlyCoupledModifiedMetadataWithItsImage() throws Exception {
        ObjectNode code = qr("QChild", "GA", "bit.ly/child", COHORT);
        ObjectNode image = options().put("qr_code_image", "data:image/svg+xml;base64,PHN2Zy8+");
        ObjectNode data = options();
        data.set("metadata", code);
        data.set("image", image);
        account(
                ok(QRS, page("qr_codes", "parent/next +")),
                ok(QRS, page("qr_codes", "", code)),
                ok("/v4/qr-codes/QChild", code),
                ok("/v4/qr-codes/QChild/image", image));
        try (Job job = job(List.of("qr_codes"), options(), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of(out("qr_codes", "QChild", "OA", "GA", "bit.ly/child", data)));
        }
        assertAccountTrace(
                qrPage(QRS, null),
                qrPage(QRS, "parent/next +"),
                wire("/v4/qr-codes/QChild"),
                wire("/v4/qr-codes/QChild/image", Map.of("format", "svg")));
    }

    @Test
    void preservesTheOptionalLegacyReadIncludingAnUnstoredInlineQr() throws Exception {
        String legacy = "/v4/bitlinks/bit.ly/old/qr";
        ObjectNode code =
                options()
                        .put("id", "QLegacy")
                        .put("group_guid", "GA")
                        .put("modified", COHORT)
                        .put("has_stored", false)
                        .put("qr_code", "data:image/svg+xml;base64,PHN2Zy8+");
        account(
                ok(LINKS, page("links", "", link("bit.ly/old", "2020-01-01T00:00:00Z"))),
                ok(legacy, code));
        try (Job job =
                job(
                        List.of("qr_codes"),
                        options().put("qr_mode", "legacy_bitlink"),
                        false,
                        false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of(out("qr_codes", "QLegacy", "OA", "GA", "bit.ly/old", code)));
        }
        assertAccountTrace(links(LINKS, null, false), wire(legacy));
    }

    @Test
    void failsOnForeignAndContradictoryContinuationUrlsWithoutSendingTheBearer() throws Exception {
        List<String> badLinks =
                List.of(
                        "https://bitly-redirect-host:18443/v4/groups/GA/bitlinks?search_after=next",
                        "/v4/groups/GB/bitlinks?search_after=next",
                        "?search_after=other",
                        "?search_after=next&archived=off",
                        "?search_after=next&search_after=next");
        for (int i = 0; i < badLinks.size(); i++) {
            ObjectNode body = page("links", "next");
            ((ObjectNode) body.get("pagination")).put("next", badLinks.get(i));
            account(ok(LINKS, body));
            String diagnostic =
                    i <= 1
                            ? "does not match the API request scope"
                            : i == 2
                                    ? "link and continuation cursor disagree"
                                    : "changes the request parameters";
            rejectCurrent(List.of("bitlinks"), options(), diagnostic);
            assertAccountTrace(links(LINKS, null, true));
        }
    }

    @Test
    void checksRepeatedCursorsAndPageIdentitiesAndEnforcesThePerParentBudget() throws Exception {
        ObjectNode a = link("bit.ly/a", COHORT);
        ObjectNode b = link("bit.ly/b", COHORT);
        account(ok(LINKS, page("links", "same")), ok(LINKS, page("links", "same")));
        rejectCurrent(List.of("bitlinks"), options(), "provider repeated a continuation cursor");
        assertAccountTrace(links(LINKS, null, true), links(LINKS, "same", true));

        account(
                ok(LINKS, page("links", "t1", a)),
                ok(LINKS, page("links", "t2", b)),
                ok(LINKS, page("links", "", a)));
        rejectCurrent(List.of("bitlinks"), options(), "provider repeated a page");
        assertAccountTrace(
                links(LINKS, null, true), links(LINKS, "t1", true), links(LINKS, "t2", true));

        account(ok(LINKS, page("links", "again", a)));
        rejectCurrent(
                List.of("bitlinks"),
                options().put("max_pages_per_parent", 1),
                "max_pages_per_parent exhausted");
        assertAccountTrace(links(LINKS, null, true));
    }

    @Test
    void neverInterpretsUrlsOrEncodedTraversalAsBitlinkIds() throws Exception {
        for (String id :
                List.of(
                        "https://bitly-redirect-host/forbidden",
                        "bit.ly/%2e%2e",
                        "bit.ly/..",
                        "bit.ly/back/slash",
                        "bit.ly/hash?next=other")) {
            account(ok(LINKS, page("links", "", link(id, COHORT))));
            rejectCurrent(List.of("bitlink_clicks"), options(), "invalid Bitlink domain and hash");
            assertAccountTrace(links(LINKS, null, false));
        }
    }

    @Test
    void validatesTheExactQrAndImageAndRejectsAnOutOfGroupCandidate() throws Exception {
        String detail = "/v4/qr-codes/Q1";
        String image = detail + "/image";
        ObjectNode candidate = qr("Q1", "GA", "bit.ly/expected", COHORT);
        account(
                ok(QRS, page("qr_codes", "", candidate)),
                ok(detail, qr("Q1", "GB", "bit.ly/expected", COHORT)));
        rejectCurrent(List.of("qr_codes"), options(), "QR detail does not match");
        assertAccountTrace(qrPage(QRS, null), wire(detail));

        account(
                ok(QRS, page("qr_codes", "", candidate)),
                ok(detail, candidate),
                frame(image, 200, "binary", "", "<svg/>"));
        rejectCurrent(List.of("qr_codes"), options(), "expected Content-Type application/json");
        assertAccountTrace(qrPage(QRS, null), wire(detail), wire(image, Map.of("format", "svg")));

        account(
                ok(QRS, page("qr_codes", "", candidate)),
                ok(detail, candidate),
                ok(image, options().put("qr_code_image", "https://bitly-redirect-host/forbidden")));
        rejectCurrent(List.of("qr_codes"), options(), "QR image response is not inline SVG data");
        assertAccountTrace(qrPage(QRS, null), wire(detail), wire(image, Map.of("format", "svg")));
    }

    @Test
    void failsClosedOnApplicationErrorAndMalformedCollectionInsteadOfReturningEmpty()
            throws Exception {
        reset(ok(CAMPAIGNS, options().put("message", HIDDEN_MESSAGE)));
        rejectCurrent(List.of("campaigns"), options(), "expected a successful Bitly JSON object");
        assertTrace(wire(CAMPAIGNS));

        reset(
                ok(
                        CAMPAIGNS,
                        list("campaigns", parent("valid", COHORT), options().put("guid", "bad"))));
        try (Job job = job(List.of("campaigns"), options(), false, false)) {
            bad(engine.executeJob(job.configName), "missing a valid provider timestamp");
            assertOutput(job, List.of());
        }
        assertTrace(wire(CAMPAIGNS));

        account(ok(LINKS, list("links", link("bit.ly/valid", COHORT))));
        rejectCurrent(List.of("bitlinks"), options(), "pagination object is missing or invalid");
        assertAccountTrace(links(LINKS, null, true));

        reset(
                ok(ORGANIZATIONS, list("organizations", options().put("guid", "OA"))),
                ok(GROUPS, list("groups", parent("G1", COHORT).put("organization_guid", "OB"))));
        rejectCurrent(List.of("groups"), options(), "group belongs to a different organization");
        assertAccountTrace();
    }

    @Test
    void distinguishesUnauthorizedForbiddenAndPermanentApiUsageExhaustion() throws Exception {
        for (int status : List.of(401, 403, 404)) {
            reset(reply(BSD, status, "plain", "1", options().put("message", HIDDEN_MESSAGE)));
            rejectCurrent(List.of("bsds"), options(), "HTTP " + status);
            assertTrace(wire(BSD));
        }
        reset(
                reply(
                        BSD,
                        429,
                        "plain",
                        "1",
                        options()
                                .put("message", "API_USAGE_LIMIT_EXCEEDED")
                                .put("description", HIDDEN_MESSAGE)));
        rejectCurrent(List.of("bsds"), options(), "monthly API quota exhausted");
        assertTrace(wire(BSD));

        reset(reply(BSD, 429, "plain", "", options().put("message", "RATE_LIMIT_EXCEEDED")));
        rejectCurrent(List.of("bsds"), options(), "without a confirmed platform retry delay");
        assertTrace(wire(BSD));
    }

    @Test
    void honorsTheConfirmedPlatformDelayAndBoundsServerRetries() throws Exception {
        ObjectNode domains = list("bsds", JSON.getNodeFactory().textNode("branded.example"));
        reset(
                reply(BSD, 429, "plain", "1", options().put("message", "RATE_LIMIT_EXCEEDED")),
                ok(BSD, domains));
        try (Job job = job(List.of("bsds"), options(), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of(out("bsds", "bsds", null, null, null, domains)));
        }
        assertTrace(wire(BSD), wire(BSD));
        List<Seen> times = seen();
        assertTrue(TimeUnit.NANOSECONDS.toMillis(times.get(1).at - times.get(0).at) >= 950L);

        reset(reply(BSD, 503, "plain", "0", options()), ok(BSD, domains));
        rejectCurrent(
                List.of("bsds"),
                options().put("max_attempts", 1),
                "HTTP 503 retry budget exhausted");
        assertTrace(wire(BSD));

        reset(reply(BSD, 503, "plain", "30", options()));
        rejectCurrent(
                List.of("bsds"),
                options().put("request_timeout_ms", 3000),
                "retry delay exceeds request deadline");
        assertTrace(wire(BSD));
    }

    @Test
    void boundsDecodedBodiesAndOneLogicalQrPair() throws Exception {
        ObjectNode domains = list("bsds", JSON.getNodeFactory().textNode("branded.example"));
        reset(reply(BSD, 200, "gzip", "", domains));
        try (Job job =
                job(List.of("bsds"), options().put("max_response_bytes", 1024), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of(out("bsds", "bsds", null, null, null, domains)));
        }
        assertTrace(wire(BSD));

        reset(reply(BSD, 200, "gzip", "", domains.deepCopy().put("extended", "a".repeat(5000))));
        rejectCurrent(
                List.of("bsds"),
                options().put("max_response_bytes", 1024),
                "response exceeds max_response_bytes");
        assertTrace(wire(BSD));

        ObjectNode qr = qr("QBudget", "GA", "bit.ly/known", COHORT);
        account(ok(QRS, page("qr_codes", "", qr)), ok("/v4/qr-codes/QBudget", qr));
        rejectCurrent(
                List.of("qr_codes"),
                options().put("max_requests", 4),
                "max_requests exhausted before QR image");
        assertAccountTrace(qrPage(QRS, null), wire("/v4/qr-codes/QBudget"));
    }

    @Test
    void refusesRedirectsAndUntrustedOrWrongHostCertificates() throws Exception {
        reset(reply(BSD, 302, "redirect", "", options()));
        rejectCurrent(List.of("bsds"), options(), "HTTP 302");
        assertTrace(wire(BSD));
        assertFalse(trace().contains("/redirect-receiver"));

        reset(ok(BSD, list("bsds")));
        rejectCurrent(
                List.of("bsds"),
                options().put("trust_certificate", OTHER_PEM),
                "TLS certificate or hostname validation failed");
        rejectCurrent(
                List.of("bsds"),
                options().put("endpoint", "https://bitly-other-host:18443"),
                "TLS certificate or hostname validation failed");
        assertTrace();
    }

    @Test
    void stopsTheTricklingTransferOnDeadlineWithoutRetry() throws Exception {
        reset(stall(BSD));
        rejectCurrent(
                List.of("bsds"),
                options().put("request_timeout_ms", 2800),
                "request deadline exceeded");
        await(() -> trace().contains("disconnect\t" + BSD), "deadline must close provider socket");
        assertTrace(wire(BSD));
    }

    @Test
    void activeJobCancellationClosesTheProviderBeforeOtherResourcesCanStart() throws Exception {
        reset(stall(BSD));
        try (Job job =
                job(
                        List.of("bsds", "campaigns"),
                        options().put("request_timeout_ms", 60000),
                        true,
                        false)) {
            Future<Container.ExecResult> running = start(job, job.id);
            try {
                await(() -> seen().size() == 1, "active Bitly HTTPS transfer");
            } finally {
                cancel(job.id, running);
            }
            await(() -> trace().contains("disconnect\t" + BSD), "provider saw stop");
            assertOutput(job, List.of());
        }
        assertTrace(wire(BSD));
    }

    @Test
    void restoresAnInFlightChildWithoutReemittingItsParentOrRediscoveringOtherParents()
            throws Exception {
        String click = "/v4/bitlinks/bit.ly/recovered/clicks";
        ObjectNode link = link("bit.ly/recovered", COHORT);
        ObjectNode metrics = list("link_clicks", options().put("clicks", 7).put("date", START));
        ObjectNode first =
                out("bitlinks", "bit.ly/recovered", "OA", "GA", "bit.ly/recovered", link);
        account(ok(LINKS, page("links", "", link)), stall(click));
        try (Job job =
                job(
                        List.of("bitlinks", "bitlink_clicks"),
                        options().put("request_timeout_ms", 60000),
                        true,
                        false)) {
            saveActive(job, 4, List.of(first), click);
            queue(ok(click, metrics));
            good(engine.restoreJob(job.configName, job.id));
            assertOutput(
                    job,
                    List.of(
                            first,
                            out(
                                    "bitlink_clicks",
                                    "bit.ly/recovered",
                                    "OA",
                                    "GA",
                                    "bit.ly/recovered",
                                    metrics)));
        }
        assertAccountTrace(links(LINKS, null, false), metric(click), metric(click));
    }

    @Test
    void resumesTheSameParentContinuationAndRejectsConfigurationDriftBeforeAnyCall()
            throws Exception {
        ObjectNode first = link("bit.ly/first", START);
        ObjectNode second = link("bit.ly/second", COHORT);
        ObjectNode committed = out("bitlinks", "bit.ly/first", "OA", "GA", "bit.ly/first", first);
        account(ok(LINKS, page("links", "active=?+", first)), stall(LINKS));
        try (Job job =
                        job(
                                List.of("bitlinks"),
                                options().put("request_timeout_ms", 60000),
                                true,
                                false);
                Job changed =
                        job(List.of("bitlinks"), options().put("page_size", 42), true, false)) {
            saveActive(job, 4, List.of(committed), LINKS);
            bad(engine.restoreJob(changed.configName, job.id), "read scope changed");
            assertAccountTrace(links(LINKS, null, true), links(LINKS, "active=?+", true));
        }

        account(ok(LINKS, page("links", "active=?+", first)), stall(LINKS));
        try (Job job =
                job(List.of("bitlinks"), options().put("request_timeout_ms", 60000), true, false)) {
            saveActive(job, 4, List.of(committed), LINKS);
            queue(ok(LINKS, page("links", "", second)));
            good(engine.restoreJob(job.configName, job.id));
            assertOutput(
                    job,
                    List.of(
                            committed,
                            out("bitlinks", "bit.ly/second", "OA", "GA", "bit.ly/second", second)));
        }
        assertAccountTrace(
                links(LINKS, null, true),
                links(LINKS, "active=?+", true),
                links(LINKS, "active=?+", true));
    }

    @Test
    void restartsAnInterruptedQrDetailImagePairWithoutPublishingHalfARow() throws Exception {
        String detail = "/v4/qr-codes/QPair";
        String imagePath = detail + "/image";
        ObjectNode metadata = qr("QPair", "GA", "bit.ly/paired", COHORT);
        ObjectNode image = options().put("qr_code_image", "data:image/svg+xml;base64,PHN2Zy8+");
        account(ok(QRS, page("qr_codes", "", metadata)), ok(detail, metadata), stall(imagePath));
        try (Job job =
                job(List.of("qr_codes"), options().put("request_timeout_ms", 60000), true, false)) {
            saveActive(job, 5, List.of(), imagePath);
            queue(ok(detail, metadata), ok(imagePath, image));
            good(engine.restoreJob(job.configName, job.id));
            ObjectNode payload = options();
            payload.set("metadata", metadata);
            payload.set("image", image);
            assertOutput(
                    job, List.of(out("qr_codes", "QPair", "OA", "GA", "bit.ly/paired", payload)));
        }
        assertAccountTrace(
                qrPage(QRS, null),
                wire(detail),
                wire(imagePath, Map.of("format", "svg")),
                wire(detail),
                wire(imagePath, Map.of("format", "svg")));
    }

    @Test
    void persistsTerminalPositionAcrossARealRestoreBesideTheAnchorPipeline() throws Exception {
        ObjectNode domains = list("bsds");
        ObjectNode committed = out("bsds", "bsds", null, null, null, domains);
        reset(ok(BSD, domains));
        try (Job job = job(List.of("bsds"), options(), true, true)) {
            saveActive(job, 1, List.of(committed), null);
            Future<Container.ExecResult> restored =
                    submissions.submit(() -> engine.restoreJob(job.configName, job.id));
            try {
                await(() -> "RUNNING".equals(engine.getJobStatus(job.id)), "restored anchor");
                long before = engine.checkpointCount(job.id);
                await(() -> engine.checkpointCount(job.id) > before, "restored checkpoint");
                assertOutput(job, List.of(committed));
                assertTrace(wire(BSD));
            } finally {
                cancel(job.id, restored);
            }
        }
    }

    @Test
    void rejectsInvalidDateOrSelectionBeforeOpeningTheProvider() throws Exception {
        reset();
        rejectCurrent(
                List.of("bsds"), options().put("start_date", END), "must be earlier than end_date");
        rejectCurrent(List.of("bsds", "bsds"), options(), "select each resource only once");
        rejectCurrent(
                List.of("bsds"),
                options().put("endpoint", "http://bitly-peer:18443"),
                "expected an HTTPS origin");
        rejectCurrent(
                List.of("bsds"),
                options().put("qr_mode", "invented"),
                "expected stored or legacy_bitlink");
        assertTrace();
    }

    @Test
    void resumesAnExactPartialResponseAndRejectsAnyChangedTailBeforeOutput() throws Exception {
        List<String> ids = new ArrayList<>();
        ObjectNode body = list("campaigns");
        ArrayNode data = (ArrayNode) body.get("campaigns");
        for (int i = 0; i < 67; i++) {
            ids.add("campaign-" + i);
            data.add(parent(ids.get(i), COHORT));
        }
        ObjectNode changed = body.deepCopy();
        ((ArrayNode) changed.get("campaigns")).set(66, parent("changed-tail", COHORT));
        reset(ok(CAMPAIGNS, body), ok(CAMPAIGNS, body), ok(CAMPAIGNS, changed));
        BitlyRecoveryProbe reader = recovery(List.of("campaigns"));
        List<String> initial = new ArrayList<>();
        byte[] state = reader.firstChunk(initial);
        assertEquals(ids.subList(0, 32), initial);
        List<String> recovered = new ArrayList<>();
        reader.consume(state, recovered);
        assertEquals(ids.subList(32, 67), recovered);
        List<String> refused = new ArrayList<>();
        Exception invalid = assertThrows(Exception.class, () -> reader.consume(state, refused));
        assertTrue(invalid.getMessage().contains("provider page changed after checkpoint"));
        assertTrue(refused.isEmpty());
        assertTrace(wire(CAMPAIGNS), wire(CAMPAIGNS), wire(CAMPAIGNS));
    }

    @Test
    void restoresPendingCoordinatorOwnershipWithoutAProbeOrReversedAssignment() throws Exception {
        reset();
        BitlyRecoveryProbe reader = recovery(List.of("bsds"));
        byte[] state = reader.recoverUnassigned();
        assertTrace();
        queue(ok(BSD, list("bsds")));
        List<String> ids = new ArrayList<>();
        reader.consume(state, ids);
        assertEquals(List.of("bsds"), ids);
        assertTrace(wire(BSD));
    }

    @Test
    void keepsCancellationStickyBeforeSplitAssignment() throws Exception {
        reset();
        List<String> ids = new ArrayList<>();
        recovery(List.of("bsds")).stopBeforeAssignment(ids);
        assertTrue(ids.isEmpty());
        assertTrace();
    }

    private void rejectCurrent(List<String> resources, ObjectNode overrides, String diagnostic)
            throws Exception {
        try (Job job = job(resources, overrides, false, false)) {
            bad(engine.executeJob(job.configName), diagnostic);
        }
    }

    private void saveActive(Job job, int calls, List<ObjectNode> committed, String stalledPath)
            throws Exception {
        Future<Container.ExecResult> running = start(job, job.id);
        try {
            await(() -> seen().size() == calls, "expected provider phase");
            awaitOutput(job, committed);
            good(engine.savepointJob(job.id));
            running.get(45, TimeUnit.SECONDS);
        } finally {
            if (!running.isDone()) {
                cancel(job.id, running);
            }
        }
        if (stalledPath != null) {
            await(
                    () -> trace().contains("disconnect\t" + stalledPath),
                    "savepoint closed previous HTTPS call");
        }
    }

    private BitlyRecoveryProbe recovery(List<String> resources) throws Exception {
        URI url =
                new URI("https", null, peer.getHost(), peer.getMappedPort(18443), null, null, null);
        return new BitlyRecoveryProbe(url.toString(), certificate.toString(), TOKEN, resources);
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
        String name = "bitly-" + UUID.randomUUID();
        String path = "/tmp/seatunnel_mnt/" + name;
        ObjectNode settings =
                options()
                        .put("plugin_output", "bitly_data")
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
                        + "source {\n Bitly "
                        + settings
                        + "\n"
                        + anchorInput
                        + "}\n"
                        + "sink {\n LocalFile {\n plugin_input=bitly_data\n path=\""
                        + path
                        + "\"\n"
                        + " file_format_type=json\n is_enable_transaction=true\n custom_filename=true\n"
                        + " file_name_expression=\"bitly_$"
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

    private static ObjectNode parent(String guid, String modified) {
        return options().put("guid", guid).put("modified", modified);
    }

    private static ObjectNode link(String id, String created) {
        return options()
                .put("id", id)
                .put("created_at", created)
                .put("long_url", "https://bitly-redirect-host:18443/forbidden");
    }

    private static ObjectNode qr(String id, String group, String link, String modified) {
        return options()
                .put("qrcode_id", id)
                .put("group_guid", group)
                .put("bitlink_id", link)
                .put("qr_code_type", "bitlink")
                .put("modified", modified)
                .put("archived", true);
    }

    private static ObjectNode list(String field, JsonNode... values) {
        ObjectNode result = options();
        ArrayNode items = result.putArray(field);
        for (JsonNode value : values) {
            items.add(value);
        }
        return result;
    }

    private static ObjectNode page(String field, String cursor, JsonNode... values) {
        ObjectNode payload = list(field, values);
        payload.putObject("pagination").put("search_after", cursor);
        return payload;
    }

    private static ObjectNode out(
            String resource, String id, String org, String group, String link, JsonNode payload) {
        ObjectNode row =
                options()
                        .put("resource", resource)
                        .put("record_id", id)
                        .put("organization_id", org)
                        .put("group_id", group)
                        .put("bitlink_id", link);
        row.set("payload", payload);
        return row;
    }

    private static String reply(
            String path, int status, String mode, String retryAfter, JsonNode body) {
        return frame(path, status, mode, retryAfter, body.toString());
    }

    private static String frame(
            String path, int status, String mode, String retryAfter, String body) {
        String encoded = Base64.getEncoder().encodeToString(body.getBytes(StandardCharsets.UTF_8));
        return String.join("\t", Integer.toString(status), mode, retryAfter, encoded, path);
    }

    private static String ok(String path, JsonNode body) {
        return reply(path, 200, "plain", "", body);
    }

    private static String stall(String path) {
        return reply(path, 200, "chunked", "", options());
    }

    private void account(String... descendants) throws Exception {
        List<String> tape = new ArrayList<>();
        tape.add(ok(ORGANIZATIONS, list("organizations", options().put("guid", "OA"))));
        tape.add(ok(GROUPS, list("groups", options().put("guid", "GA"))));
        tape.addAll(List.of(descendants));
        reset(tape.toArray(new String[0]));
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
            assertEquals("true", parts[4], "the expected Bearer credential must be present");
            assertEquals("true", parts[5], "the connector must ask for JSON");
            URI request = URI.create(parts[2]);
            assertTrue(request.getRawPath().startsWith("/v4/"));
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
        return wire(path, Map.of());
    }

    private static Seen wire(String path, Map<String, String> query) {
        return new Seen(path, query, 0L);
    }

    private static Seen metric(String path) {
        return wire(path, Map.of("unit", "day", "units", "-1", "unit_reference", METRIC_REFERENCE));
    }

    private static Seen links(String path, String cursor, boolean withCreatedFilter) {
        Map<String, String> query = new TreeMap<>(Map.of("size", "50", "archived", "both"));
        if (cursor != null) {
            query.put("search_after", cursor);
        }
        if (withCreatedFilter) {
            query.put("created_after", String.valueOf(Instant.parse(START).getEpochSecond() - 1));
            query.put("created_before", String.valueOf(Instant.parse(END).getEpochSecond()));
        }
        return wire(path, query);
    }

    private static Seen qrPage(String path, String cursor) {
        Map<String, String> query =
                new TreeMap<>(Map.of("size", "50", "archived", "both", "qrc_type", "bitlink"));
        if (cursor != null) {
            query.put("search_after", cursor);
        }
        return wire(path, query);
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

    private void assertAccountTrace(Seen... children) throws Exception {
        List<Seen> expected = new ArrayList<>();
        expected.add(wire(ORGANIZATIONS));
        expected.add(wire(GROUPS, Map.of("organization_guid", "OA")));
        expected.addAll(List.of(children));
        assertTrace(expected.toArray(new Seen[0]));
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
        private final String id = BitlyIT.id();

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
                                    + "' -maxdepth 1 -type f -name 'bitly_*' -exec cat {} +; fi");
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
            // The Bitly pipeline can already have ended. Observe all active pipeline counters.
            long completed = 0;
            for (JsonNode pipeline : JSON.readTree(response.body()).path("pipelines")) {
                completed += pipeline.path("counts").path("completed").asLong();
            }
            return completed;
        }
    }
}
