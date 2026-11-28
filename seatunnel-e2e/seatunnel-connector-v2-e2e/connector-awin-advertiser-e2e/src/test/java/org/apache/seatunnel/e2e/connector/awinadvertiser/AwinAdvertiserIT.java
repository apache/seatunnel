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
package org.apache.seatunnel.e2e.connector.awinadvertiser;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.awinadvertiser.AwinRecoveryProbe;
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
import java.net.URLDecoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
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
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises all six advertiser resources through Zeta using an independent Awin protocol peer.
 *
 * <p>The suite runs the engine in a non-UTC time zone and checks the actual wire requests and
 * committed output rather than inferring delivery from logs.
 */
class AwinAdvertiserIT extends TestSuiteBase {
    private static final ObjectMapper JSON =
            new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    private static final String TOKEN = "awin-test-token-private";
    private static final String PRIVATE_RESPONSE = "opaque-awin-error-body";
    private static final long ADVERTISER = 9007199254740993L;
    private static final String ADVERTISER_TEXT = Long.toString(ADVERTISER);
    private static final String PROGRAM = "/advertisers/" + ADVERTISER;
    private static final String CAMPAIGN = PROGRAM + "/reports/campaign";
    private static final String TRANSACTIONS = PROGRAM + "/transactions/";
    private static final String PUBLISHER_REPORT = PROGRAM + "/reports/publisher";
    private static final String CREATIVE = PROGRAM + "/reports/creative";
    private static final String PUBLISHERS = PROGRAM + "/publishers";
    private static final String ACCOUNTS = "/accounts";
    private static final String DAY = "2024-11-03";
    private static final String TOMORROW = "2024-11-04";
    private static final String HOST = "awin-peer";
    private static final String PEM = "/tmp/awin-vendor.pem";
    private static final String UNRELATED_PEM = "/tmp/unrelated-awin.pem";
    private static final String ORIGIN = "https://" + HOST + ":18443";

    private final Zeta engine = new Zeta();
    private final HttpClient control =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final ExecutorService jobs = Executors.newSingleThreadExecutor();
    private GenericContainer<?> peer;
    private Path testTrust;

    /**
     * Starts the vendor peer and the standard JDK 11 engine.
     *
     * <p>The negative trust case uses an independent valid certificate rather than a broken file.
     */
    @BeforeAll
    void start(@TempDir Path temp) throws Exception {
        String host = DockerClientFactory.instance().dockerHostIpAddress();
        assertTrue(host.matches("[A-Za-z0-9.:-]+"));
        String hostType = host.matches("[0-9.]+") || host.contains(":") ? "ip:" : "dns:";
        String issue =
                "keytool -genkeypair -alias api -keyalg RSA -keysize 2048 "
                        + "-storetype PKCS12 -storepass awin-fixture -validity 30 -dname CN="
                        + HOST
                        + " -ext SAN=dns:"
                        + HOST
                        + ","
                        + hostType
                        + host
                        + " -keystore ";
        String export = "keytool -exportcert -rfc -alias api -storepass awin-fixture -keystore ";
        String script =
                issue
                        + "/tmp/service.p12 && "
                        + export
                        + "/tmp/service.p12 -file "
                        + PEM
                        + " && "
                        + issue
                        + "/tmp/unrelated.p12 && "
                        + export
                        + "/tmp/unrelated.p12 -file "
                        + UNRELATED_PEM
                        + " && exec java --add-modules jdk.httpserver -cp /tmp "
                        + AwinAdvertiserPeer.class.getName()
                        + " /tmp/service.p12";
        String cls = "/" + AwinAdvertiserPeer.class.getName().replace('.', '/') + ".class";
        byte[] bytes;
        try (InputStream binary = AwinAdvertiserPeer.class.getResourceAsStream(cls)) {
            if (binary == null) {
                throw new IllegalStateException("HTTPS fixture binary is missing");
            }
            bytes = binary.readAllBytes();
        }
        peer =
                new GenericContainer<>(DockerImageName.parse("eclipse-temurin:11-jdk"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST, "awin-other-name")
                        .withExposedPorts(18443, 18080)
                        .withCopyToContainer(Transferable.of(bytes), "/tmp" + cls)
                        .withCommand("sh", "-c", script)
                        .waitingFor(
                                Wait.forHttp("/log")
                                        .forPort(18080)
                                        .withStartupTimeout(Duration.ofMinutes(2)));
        peer.start();
        engine.startUp();
        testTrust = Files.writeString(temp.resolve("awin.pem"), installPem(PEM));
        installPem(UNRELATED_PEM);
    }

    /**
     * Stops the fixture even if engine cleanup fails.
     *
     * <p>Async submissions belong to this suite and are cancelled at exit.
     */
    @AfterAll
    void stop() throws Exception {
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
    void readsAllSixResourcesAndKeepsExactDimensionsAcrossADstBoundary() throws Exception {
        ObjectNode campaign =
                object(
                        "{\"date\":\"2024-11-03\",\"publisherId\":17,"
                                + "\"campaign\":\"東京:promo\",\"totals\":{\"sales\":123456789012345.000000000123,\"reversed\":null}}");
        ObjectNode deleted =
                object(
                        "{\"id\":9223372036854775810,\"advertiserId\":"
                                + ADVERTISER
                                + ",\"transactionDate\":\"2024-11-03T23:59:59.999999Z\",\"commissionStatus\":\"deleted\","
                                + "\"basketProducts\":[{\"id\":\"sku\",\"saleAmount\":100.000000000000000009,\"quantity\":2}],"
                                + "\"customParameters\":[{\"key\":\"备注\",\"value\":null}]}");
        ObjectNode pending =
                value("id", "550e8400-e29b-41d4-a716-446655440000")
                        .put("commissionStatus", "pending")
                        .put("transactionDate", "2024-11-04T01:00:00+01:00");
        ObjectNode publisher =
                value("publisherId", "17").put("region", "US").put("currency", "USD");
        ObjectNode creative =
                value("publisherId", "17").put("creativeId", "cr:1").put("currency", "BRL");
        ObjectNode creativeBu = creative.deepCopy().put("currency", "USD");
        ObjectNode joined = value("id", "17").put("name", "Joined – café").putNull("primaryRegion");
        ObjectNode account =
                value("accountId", ADVERTISER_TEXT)
                        .put("accountType", "advertiser")
                        .put("userRole", "viewer");

        program(
                ok(envelope("result", array(campaign))),
                ok(array(deleted, pending)),
                ok(array(publisher)),
                ok(array()),
                ok(array(creative)),
                ok(array()),
                ok(array()),
                ok(array(creativeBu)),
                ok(array(joined)),
                ok(envelope("accounts", array(account))));
        ObjectNode settings = dates(DAY, TOMORROW);
        settings.putArray("regions").add("BR").add("BU");
        try (Job job =
                job(
                        List.of(
                                "campaign_performance",
                                "transactions",
                                "publisher_performance",
                                "creative_performance",
                                "publishers",
                                "accounts"),
                        settings,
                        false,
                        false)) {
            success(engine.executeJob(job.name));
            output(
                    job,
                    List.of(
                            row(
                                    "campaign_performance",
                                    key(DAY, "17", "東京:promo"),
                                    ADVERTISER_TEXT,
                                    null,
                                    DAY,
                                    campaign),
                            row(
                                    "transactions",
                                    "9223372036854775810",
                                    ADVERTISER_TEXT,
                                    null,
                                    DAY,
                                    deleted.deepCopy().put("is_deleted", true)),
                            row(
                                    "transactions",
                                    "550e8400-e29b-41d4-a716-446655440000",
                                    ADVERTISER_TEXT,
                                    null,
                                    TOMORROW,
                                    pending.deepCopy().put("is_deleted", false)),
                            row(
                                    "publisher_performance",
                                    key(DAY, "17", "US", "USD"),
                                    ADVERTISER_TEXT,
                                    "US",
                                    DAY,
                                    publisher.deepCopy().put("date", DAY)),
                            row(
                                    "creative_performance",
                                    key(DAY, "BR", "cr:1", "17", "BRL"),
                                    ADVERTISER_TEXT,
                                    "BR",
                                    DAY,
                                    creative.deepCopy().put("date", DAY).put("region", "BR")),
                            row(
                                    "creative_performance",
                                    key(TOMORROW, "BU", "cr:1", "17", "USD"),
                                    ADVERTISER_TEXT,
                                    "BU",
                                    TOMORROW,
                                    creativeBu
                                            .deepCopy()
                                            .put("date", TOMORROW)
                                            .put("region", "BU")),
                            row("publishers", "17", ADVERTISER_TEXT, null, null, joined),
                            row(
                                    "accounts",
                                    ADVERTISER_TEXT,
                                    ADVERTISER_TEXT,
                                    null,
                                    null,
                                    account)));
        }
        wire(
                campaign(DAY, TOMORROW),
                transactions(DAY, TOMORROW),
                publisher(DAY),
                publisher(TOMORROW),
                creative(DAY, "BR"),
                creative(TOMORROW, "BR"),
                creative(DAY, "BU"),
                creative(TOMORROW, "BU"),
                call(PUBLISHERS, Map.of()),
                accounts());
    }

    @Test
    void splitsCampaignAfterFourHundredDatesAndTransactionsAfterThirtyOneUtcDays()
            throws Exception {
        String campaignStart = "2023-01-01";
        String campaignEnd = "2024-02-06";
        ObjectNode boundary =
                value("date", "2024-02-04").put("publisherId", 2).put("campaign", "seam");
        ObjectNode following = boundary.deepCopy().put("date", "2024-02-05");
        program(ok(envelope("result", array(boundary))), ok(envelope("result", array(following))));
        try (Job job =
                job(
                        List.of("campaign_performance"),
                        dates(campaignStart, campaignEnd),
                        false,
                        false)) {
            success(engine.executeJob(job.name));
            output(
                    job,
                    List.of(
                            row(
                                    "campaign_performance",
                                    key("2024-02-04", "2", "seam"),
                                    ADVERTISER_TEXT,
                                    null,
                                    "2024-02-04",
                                    boundary),
                            row(
                                    "campaign_performance",
                                    key("2024-02-05", "2", "seam"),
                                    ADVERTISER_TEXT,
                                    null,
                                    "2024-02-05",
                                    following)));
        }
        wire(campaign(campaignStart, "2024-02-04"), campaign("2024-02-05", campaignEnd));

        ObjectNode last = value("id", "at-end").put("transactionDate", "2024-03-02T23:59:59");
        ObjectNode next = value("id", "at-start").put("transactionDate", "2024-03-03T00:00:00");
        program(ok(array(last)), ok(array(next)));
        try (Job job =
                job(List.of("transactions"), dates("2024-02-01", "2024-03-03"), false, false)) {
            success(engine.executeJob(job.name));
            output(
                    job,
                    List.of(
                            row(
                                    "transactions",
                                    "at-end",
                                    ADVERTISER_TEXT,
                                    null,
                                    "2024-03-02",
                                    last.deepCopy().put("is_deleted", false)),
                            row(
                                    "transactions",
                                    "at-start",
                                    ADVERTISER_TEXT,
                                    null,
                                    "2024-03-03",
                                    next.deepCopy().put("is_deleted", false))));
        }
        wire(transactions("2024-02-01", "2024-03-02"), transactions("2024-03-03", "2024-03-03"));
    }

    @Test
    void lookbackReplaysUpdatesAndKeepsEachCreativeRegionIndependent() throws Exception {
        String first = "2024-01-01";
        String last = "2024-01-05";
        ObjectNode sale = value("id", "sale-1").put("transactionDate", "2024-01-02T00:00:00");
        ObjectNode pending = sale.deepCopy().put("commissionStatus", "pending");
        program(ok(array(pending)));
        try (Job job = job(List.of("transactions"), dates(first, last), false, false)) {
            success(engine.executeJob(job.name));
            output(
                    job,
                    List.of(
                            row(
                                    "transactions",
                                    "sale-1",
                                    ADVERTISER_TEXT,
                                    null,
                                    "2024-01-02",
                                    pending.deepCopy().put("is_deleted", false))));
        }
        wire(transactions(first, last));

        ObjectNode changed = sale.deepCopy().put("commissionStatus", "deleted");
        ObjectNode creative =
                value("creativeId", "same:creative").put("publisherId", 42).put("currency", "USD");
        ObjectNode cfg = dates(first, last).put("lookback_days", 2);
        cfg.putArray("regions").add("GB").add("US");
        cfg.putObject("resume_from")
                .put("transactions", "2024-01-03")
                .put("creative_performance:GB", "2024-01-05")
                .put("publisher_performance", "2024-01-04");
        program(
                ok(array(changed)),
                ok(array(creative)),
                ok(array()),
                ok(array()),
                ok(array()),
                ok(array()),
                ok(array(creative)),
                ok(array()),
                ok(array()),
                ok(array()),
                ok(array()),
                ok(array()),
                ok(array()));
        try (Job job =
                job(
                        List.of("transactions", "creative_performance", "publisher_performance"),
                        cfg,
                        false,
                        false)) {
            success(engine.executeJob(job.name));
            output(
                    job,
                    List.of(
                            row(
                                    "transactions",
                                    "sale-1",
                                    ADVERTISER_TEXT,
                                    null,
                                    "2024-01-02",
                                    changed.deepCopy().put("is_deleted", true)),
                            row(
                                    "creative_performance",
                                    key("2024-01-03", "GB", "same:creative", "42", "USD"),
                                    ADVERTISER_TEXT,
                                    "GB",
                                    "2024-01-03",
                                    creative.deepCopy()
                                            .put("date", "2024-01-03")
                                            .put("region", "GB")),
                            row(
                                    "creative_performance",
                                    key("2024-01-03", "US", "same:creative", "42", "USD"),
                                    ADVERTISER_TEXT,
                                    "US",
                                    "2024-01-03",
                                    creative.deepCopy()
                                            .put("date", "2024-01-03")
                                            .put("region", "US"))));
        }
        wire(
                transactions(first, last),
                creative("2024-01-03", "GB"),
                creative("2024-01-04", "GB"),
                creative("2024-01-05", "GB"),
                creative("2024-01-01", "US"),
                creative("2024-01-02", "US"),
                creative("2024-01-03", "US"),
                creative("2024-01-04", "US"),
                creative("2024-01-05", "US"),
                publisher("2024-01-02"),
                publisher("2024-01-03"),
                publisher("2024-01-04"),
                publisher("2024-01-05"));
    }

    @Test
    void acceptsDocumentedHttpStatusWrapperAndAdvertiserAccountsWithoutAProgramId()
            throws Exception {
        ObjectNode account = value("accountId", "inactive-123").put("accountType", "advertiser");
        ObjectNode wrapper = value("statusCode", "200 OK").put("statusCodeValue", 200);
        wrapper.set("body", envelope("accounts", array(account)));
        program(ok(wrapper));
        ObjectNode cfg = options();
        cfg.putNull("advertiser_id");
        try (Job job = job(List.of("accounts"), cfg, false, false)) {
            success(engine.executeJob(job.name));
            output(
                    job,
                    List.of(row("accounts", "inactive-123", "inactive-123", null, null, account)));
        }
        wire(accounts());
    }

    @Test
    void rejectsRowsFromADifferentRegionOrAdvertiserAndUnexpectedDates() throws Exception {
        ObjectNode cfg = dates(DAY, DAY);
        cfg.putArray("regions").add("GB");
        failureAt(
                "creative_performance",
                cfg,
                ok(array(value("publisherId", "1").put("creativeId", "2").put("region", "US"))),
                "creative result belongs to a different region",
                creative(DAY, "GB"));
        failureAt(
                "transactions",
                dates(DAY, DAY),
                ok(
                        array(
                                value("id", "sale")
                                        .put("advertiserId", 2)
                                        .put("transactionDate", DAY + "T00:00:00"))),
                "record belongs to a different advertiser",
                transactions(DAY, DAY));
        failureAt(
                "publisher_performance",
                dates(DAY, DAY),
                ok(array(value("publisherId", "1").put("date", TOMORROW))),
                "report date is outside the requested UTC window",
                publisher(DAY));
        failureAt(
                "transactions",
                dates(DAY, DAY),
                ok(array(value("id", "sale").put("transactionDate", TOMORROW + "T00:00:00Z"))),
                "transactionDate is outside the requested UTC window",
                transactions(DAY, DAY));
    }

    @Test
    void rejectsErrorsAndWireTruncationWithoutEmittingANewRequest() throws Exception {
        JsonNode hidden = value("error", PRIVATE_RESPONSE).put("success", false);
        for (int status : new int[] {401, 403}) {
            failureAt(
                    "accounts",
                    options(),
                    reply(status, "plain", "", hidden),
                    "HTTP " + status + " requires a token",
                    accounts());
        }
        failureAt(
                "accounts", options(), ok(hidden), "failed or partial report envelope", accounts());
        failureAt(
                "accounts",
                options(),
                ok(value("statusCode", "206 PARTIAL_CONTENT").put("statusCodeValue", 206)),
                "failed or partial report envelope",
                accounts());
        JsonNode goodBody = envelope("accounts", array(value("accountId", "one")));
        failureAt(
                "accounts",
                options(),
                reply(206, "plain", "", goodBody),
                "partial response or continuation",
                accounts());
        failureAt(
                "accounts",
                options(),
                reply(200, "range", "", goodBody),
                "partial response or continuation",
                accounts());
        failureAt(
                "accounts",
                options(),
                reply(200, "link", "", goodBody),
                "partial response or continuation",
                accounts());
        failureAt(
                "accounts",
                options(),
                ok(array(value("accountId", "one"))),
                "missing its expected records array",
                accounts());
    }

    @Test
    void retriesThrottledSameWindowAndHonorsServerRetryAfter() throws Exception {
        ObjectNode publisher = value("id", "first-publisher");
        program(
                reply(429, "plain", "1", value("message", PRIVATE_RESPONSE)),
                reply(503, "plain", "0", options()),
                ok(array(publisher)));
        try (Job job = job(List.of("publishers"), options().put("max_attempts", 3), false, false)) {
            success(engine.executeJob(job.name));
            output(
                    job,
                    List.of(
                            row(
                                    "publishers",
                                    "first-publisher",
                                    ADVERTISER_TEXT,
                                    null,
                                    null,
                                    publisher)));
        }
        Map.Entry<String, Map<String, String>> call = call(PUBLISHERS, Map.of());
        wire(call, call, call);
        List<Wire> events = events();
        assertTrue(TimeUnit.NANOSECONDS.toMillis(events.get(1).at - events.get(0).at) >= 950L);
        assertTrue(TimeUnit.NANOSECONDS.toMillis(events.get(2).at - events.get(1).at) >= 350L);

        failureAt(
                "publishers",
                options().put("max_attempts", 1),
                reply(503, "plain", "0", options()),
                "HTTP 503 retry budget exhausted",
                call);
        failureAt(
                "publishers",
                options().put("request_timeout_ms", 1500),
                reply(429, "plain", "60", options()),
                "retry delay exceeds request deadline",
                call);
    }

    @Test
    void countsAttemptsBetweenResourcesAndKeepsQuotaWaitWithinTheDeadline() throws Exception {
        program(ok(array()));
        try (Job job =
                job(
                        List.of("publishers", "accounts"),
                        options().put("requests_per_minute", 1).put("request_timeout_ms", 2000),
                        false,
                        false)) {
            failed(engine.executeJob(job.name), "legacy rate pacing exceeds request deadline");
        }
        wire(call(PUBLISHERS, Map.of()));

        program(reply(429, "plain", "", options()));
        try (Job job =
                job(
                        List.of("accounts"),
                        options().put("requests_per_minute", 1).put("request_timeout_ms", 2000),
                        false,
                        false)) {
            failed(engine.executeJob(job.name), "legacy rate pacing exceeds request deadline");
        }
        wire(accounts());

        program(ok(array()));
        try (Job job =
                job(
                        List.of("publishers", "accounts"),
                        options().put("max_requests", 1),
                        false,
                        false)) {
            failed(engine.executeJob(job.name), "max_requests exhausted");
        }
        wire(call(PUBLISHERS, Map.of()));
    }

    @Test
    void refusesCredentialRedirectAndIndependentlyVerifiesHostnameAndTrust() throws Exception {
        failureAt(
                "publishers",
                options(),
                reply(302, "location", "", options()),
                "provider rejected the request (HTTP 302)",
                call(PUBLISHERS, Map.of()));
        program();
        try (Job job =
                job(
                        List.of("publishers"),
                        options().put("trust_certificate", UNRELATED_PEM),
                        false,
                        false)) {
            failed(engine.executeJob(job.name), "TLS certificate or hostname validation failed");
        }
        try (Job job =
                job(
                        List.of("publishers"),
                        options().put("endpoint", "https://awin-other-name:18443"),
                        false,
                        false)) {
            failed(engine.executeJob(job.name), "TLS certificate or hostname validation failed");
        }
        wire();
    }

    @Test
    void preservesGzipAndStopsReadingADecodedOversizeOrDripDeadline() throws Exception {
        Map.Entry<String, Map<String, String>> call = call(PUBLISHERS, Map.of());
        ObjectNode record = value("id", "unicode").put("name", "Zürich – 東京");
        program(reply(200, "gzip", "", array(record)));
        try (Job job =
                job(
                        List.of("publishers"),
                        options().put("max_response_bytes", 1024),
                        false,
                        false)) {
            success(engine.executeJob(job.name));
            output(job, List.of(row("publishers", "unicode", ADVERTISER_TEXT, null, null, record)));
        }
        wire(call);
        failureAt(
                "publishers",
                options().put("max_response_bytes", 1024),
                reply(200, "gzip", "", array(record.deepCopy().put("home", "h".repeat(3000)))),
                "response exceeds max_response_bytes",
                call);
        failureAt(
                "publishers",
                options().put("request_timeout_ms", 1200),
                stall(),
                "request deadline exceeded",
                call);
        await(
                () -> trace().contains("closed\t" + PUBLISHERS),
                "provider disconnect after deadline");
    }

    @Test
    void cancelsTheFirstActiveProviderTransferAndDoesNotVisitFurtherResources() throws Exception {
        program(stall());
        try (Job job =
                job(
                        List.of("publishers", "accounts"),
                        options().put("request_timeout_ms", 60000),
                        true,
                        false)) {
            String id = jobId();
            Future<Container.ExecResult> jobRun = run(job, id);
            try {
                await(() -> events().size() == 1, "the open provider response");
            } finally {
                abort(id, jobRun);
            }
        }
        await(
                () -> trace().contains("closed\t" + PUBLISHERS),
                "provider disconnect after job stop");
        wire(call(PUBLISHERS, Map.of()));
    }

    @Test
    void resumesARealSavepointInsideARegionalPlanWithoutRepeatingCompletedRows() throws Exception {
        ObjectNode build = value("publisherId", "55").put("creativeId", "creative");
        program(ok(array(build)), stall());
        ObjectNode config = dates(DAY, DAY).put("request_timeout_ms", 60000);
        config.putArray("regions").add("GB").add("US");
        ObjectNode gb =
                row(
                        "creative_performance",
                        key(DAY, "GB", "creative", "55", null),
                        ADVERTISER_TEXT,
                        "GB",
                        DAY,
                        build.deepCopy().put("date", DAY).put("region", "GB"));
        ObjectNode us =
                row(
                        "creative_performance",
                        key(DAY, "US", "creative", "55", null),
                        ADVERTISER_TEXT,
                        "US",
                        DAY,
                        build.deepCopy().put("date", DAY).put("region", "US"));
        ObjectNode accounts = value("accountId", ADVERTISER_TEXT).put("accountType", "advertiser");
        try (Job job = job(List.of("creative_performance", "accounts"), config, true, false)) {
            String id = jobId();
            Future<Container.ExecResult> running = run(job, id);
            try {
                await(() -> events().size() == 2, "the held second regional request");
                committed(job, List.of(gb));
                success(engine.savepointJob(id));
                running.get(45, TimeUnit.SECONDS);
            } finally {
                if (!running.isDone()) {
                    abort(id, running);
                }
            }
            await(
                    () -> trace().contains("closed\t" + CREATIVE),
                    "old regional call stops at savepoint");
            append(ok(array(build)), ok(envelope("accounts", array(accounts))));
            success(engine.restoreJob(job.name, id));
            output(
                    job,
                    List.of(
                            gb,
                            us,
                            row(
                                    "accounts",
                                    ADVERTISER_TEXT,
                                    ADVERTISER_TEXT,
                                    null,
                                    null,
                                    accounts)));
        }
        wire(creative(DAY, "GB"), creative(DAY, "US"), creative(DAY, "US"), accounts());
    }

    @Test
    void refusesToReuseACreativeSavepointWithNewRegionScope() throws Exception {
        program(stall());
        ObjectNode current = dates(DAY, DAY).put("request_timeout_ms", 60000);
        current.putArray("regions").add("GB");
        ObjectNode changed = current.deepCopy();
        changed.putArray("regions").add("GB").add("US");
        try (Job job = job(List.of("creative_performance"), current, true, false);
                Job other = job(List.of("creative_performance"), changed, true, false)) {
            String id = jobId();
            Future<Container.ExecResult> running = run(job, id);
            try {
                await(() -> events().size() == 1, "first regional request");
                success(engine.savepointJob(id));
                running.get(45, TimeUnit.SECONDS);
                failed(engine.restoreJob(other.name, id), "read scope changed");
            } finally {
                if (!running.isDone()) {
                    abort(id, running);
                }
            }
        }
        wire(creative(DAY, "GB"));
    }

    @Test
    void retainsATerminalScanBesideAnotherLivePipelineAfterRestore() throws Exception {
        ObjectNode account = value("accountId", ADVERTISER_TEXT).put("accountType", "advertiser");
        ObjectNode expected =
                row("accounts", ADVERTISER_TEXT, ADVERTISER_TEXT, null, null, account);
        program(ok(envelope("accounts", array(account))));
        try (Job job = job(List.of("accounts"), options(), true, true)) {
            String id = jobId();
            Future<Container.ExecResult> running = run(job, id);
            try {
                committed(job, List.of(expected));
                success(engine.savepointJob(id));
                running.get(45, TimeUnit.SECONDS);
            } finally {
                if (!running.isDone()) {
                    abort(id, running);
                }
            }
            Future<Container.ExecResult> resumed =
                    jobs.submit(() -> engine.restoreJob(job.name, id));
            try {
                await(
                        () -> "RUNNING".equals(engine.getJobStatus(id)),
                        "the restored secondary pipeline");
                long previous = engine.checkpoints(id);
                await(() -> engine.checkpoints(id) > previous, "a new restored checkpoint");
                output(job, List.of(expected));
                wire(accounts());
            } finally {
                abort(id, resumed);
            }
        }
    }

    @Test
    void validatesEmptyRegionsAndUnexpectedLookbackKeysBeforeNetworkReads() throws Exception {
        program();
        try (Job job = job(List.of("creative_performance"), dates(DAY, DAY), false, false)) {
            failed(
                    engine.executeJob(job.name),
                    "creative reporting requires distinct supported Awin regions");
        }
        ObjectNode config = dates(DAY, DAY);
        config.putArray("regions").add("GB");
        config.putObject("resume_from").put("creative_performance:US", DAY);
        try (Job job = job(List.of("creative_performance"), config, false, false)) {
            failed(engine.executeJob(job.name), "key outside the selected dated partitions");
        }
        try (Job job =
                job(
                        List.of("transactions"),
                        dates(DAY, DAY).put("transaction_window_days", 32),
                        false,
                        false)) {
            failed(engine.executeJob(job.name), "transaction_window_days");
        }
        wire();
    }

    @Test
    void resumesTheExactPartialResponseAndRejectsAMutatedSuffix() throws Exception {
        List<String> ids =
                IntStream.range(0, 152).mapToObj(i -> "pub-" + i).collect(Collectors.toList());
        ArrayNode listing = array();
        ids.forEach(id -> listing.add(value("id", id)));
        ArrayNode altered = listing.deepCopy();
        altered.set(151, value("id", "changed-tail"));
        program(ok(listing), ok(listing), ok(altered));
        AwinRecoveryProbe probe = recovery("publishers", null);
        List<String> received = new ArrayList<>();
        byte[] saved = probe.firstBatch(received);
        assertEquals(ids.subList(0, 128), received);
        List<String> recovered = new ArrayList<>();
        probe.resume(saved, recovered);
        assertEquals(ids.subList(128, 152), recovered);

        List<String> refused = new ArrayList<>();
        Exception failure = assertThrows(Exception.class, () -> probe.resume(saved, refused));
        assertTrue(failure.getMessage().contains("provider response changed after checkpoint"));
        assertTrue(refused.isEmpty());
        Map.Entry<String, Map<String, String>> each = call(PUBLISHERS, Map.of());
        wire(each, each, each);
    }

    @Test
    void preAssignmentCheckpointKeepsTheFrozenCurrentTransactionEnd() throws Exception {
        program();
        String today = LocalDate.now(ZoneOffset.UTC).toString();
        AwinRecoveryProbe probe = recovery("transactions", today);
        byte[] state = probe.assignAfterRestore();
        wire();
        String cut =
                DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss")
                        .withZone(ZoneOffset.UTC)
                        .format(AwinRecoveryProbe.cutoff(state));
        append(ok(array(value("id", "first-snapshot-row").put("transactionDate", cut))));
        List<String> output = new ArrayList<>();
        probe.resume(state, output);
        assertEquals(List.of("first-snapshot-row"), output);
        wire(
                call(
                        TRANSACTIONS,
                        Map.of(
                                "startDate",
                                today + "T00:00:00",
                                "endDate",
                                cut,
                                "timezone",
                                "UTC",
                                "dateType",
                                "transaction",
                                "showBasketProducts",
                                "true")));
    }

    @Test
    void cancellationBeforeAssignmentPreventsAllTransport() throws Exception {
        program();
        List<String> output = new ArrayList<>();
        recovery("publishers", null).cancelBeforeAssignment(output);
        assertTrue(output.isEmpty());
        wire();
    }

    private AwinRecoveryProbe recovery(String resource, String start) throws Exception {
        URI origin =
                new URI("https", null, peer.getHost(), peer.getMappedPort(18443), null, null, null);
        return new AwinRecoveryProbe(
                origin.toString(), testTrust.toString(), TOKEN, ADVERTISER, resource, start);
    }

    private void failureAt(
            String resource,
            ObjectNode overrides,
            String response,
            String message,
            Map.Entry<String, Map<String, String>> expected)
            throws Exception {
        program(response);
        try (Job job = job(List.of(resource), overrides, false, false)) {
            failed(engine.executeJob(job.name), message);
        }
        wire(expected);
    }

    private static String jobId() {
        return Long.toString(UUID.randomUUID().getLeastSignificantBits() & Long.MAX_VALUE);
    }

    private Future<Container.ExecResult> run(Job job, String id) {
        return jobs.submit(() -> engine.submit(job.name, id));
    }

    private void abort(String id, Future<Container.ExecResult> result) throws Exception {
        try {
            if (!result.isDone()) {
                engine.stopJob(id);
            }
            result.get(45, TimeUnit.SECONDS);
        } finally {
            if (!result.isDone()) {
                result.cancel(true);
            }
        }
    }

    private String installPem(String file) throws Exception {
        Container.ExecResult pem = peer.execInContainer("cat", file);
        assertEquals(0, pem.getExitCode(), pem.getStderr());
        engine.install(file, pem.getStdout());
        return pem.getStdout();
    }

    private static Job job(
            List<String> resources, ObjectNode changes, boolean streaming, boolean other)
            throws Exception {
        String local = "awin-advertiser-" + UUID.randomUUID();
        String output = "/tmp/seatunnel_mnt/" + local;
        ObjectNode settings =
                options()
                        .put("plugin_output", "advertiser_rows")
                        .put("token", TOKEN)
                        .put("advertiser_id", ADVERTISER)
                        .put("endpoint", ORIGIN)
                        .put("trust_certificate", PEM);
        ArrayNode selected = settings.putArray("resources");
        resources.forEach(selected::add);
        settings.setAll(changes);
        String actor =
                other
                        ? " FakeSource {\n plugin_output=anchor\n row.num=1000\n split.num=1000\n"
                                + " split.read-interval=250\n schema.fields.value=\"string\"\n }\n"
                        : "";
        String drain = other ? " Console { plugin_input=anchor }\n" : "";
        String hocon =
                "env {\n job.mode="
                        + (streaming ? "STREAMING" : "BATCH")
                        + "\n parallelism=1\n checkpoint.interval=350\n}\n"
                        + "source {\n AwinAdvertiser "
                        + settings
                        + "\n"
                        + actor
                        + "}\n"
                        + "sink {\n LocalFile {\n plugin_input=advertiser_rows\n path=\""
                        + output
                        + "\"\n file_format_type=json\n is_enable_transaction=true\n custom_filename=true\n"
                        + " file_name_expression=\"delivered_$"
                        + "{transactionId}\"\n}\n"
                        + drain
                        + "}\n";
        Path directory = ContainerUtil.getCurrentModulePath().resolve("src/test/resources");
        Files.createDirectories(directory);
        return new Job(Files.writeString(directory.resolve(local + ".conf"), hocon), output);
    }

    private static ObjectNode options() {
        return JSON.createObjectNode();
    }

    private static ObjectNode dates(String start, String end) {
        return options().put("start_date", start).put("end_date", end);
    }

    private static ObjectNode object(String encoded) throws Exception {
        return (ObjectNode) JSON.readTree(encoded);
    }

    private static ObjectNode value(String attribute, String value) {
        return options().put(attribute, value);
    }

    private static ArrayNode array(JsonNode... entries) {
        ArrayNode array = JSON.createArrayNode();
        for (JsonNode entry : entries) {
            array.add(entry);
        }
        return array;
    }

    private static ObjectNode envelope(String name, ArrayNode entries) {
        ObjectNode object = options();
        object.set(name, entries);
        return object;
    }

    private static String key(String... parts) throws Exception {
        ArrayNode identity = JSON.createArrayNode();
        for (String part : parts) {
            identity.add(part);
        }
        return JSON.writeValueAsString(identity);
    }

    private static ObjectNode row(
            String resource,
            String key,
            String advertiser,
            String region,
            String date,
            JsonNode body) {
        ObjectNode expected =
                options()
                        .put("resource", resource)
                        .put("record_key", key)
                        .put("advertiser_id", advertiser)
                        .put("region", region)
                        .put("report_date", date);
        expected.set("payload", body);
        return expected;
    }

    private static String reply(int status, String mode, String retry, JsonNode body) {
        return String.join(
                "\t",
                Integer.toString(status),
                mode,
                retry,
                Base64.getEncoder()
                        .encodeToString(body.toString().getBytes(StandardCharsets.UTF_8)));
    }

    private static String ok(JsonNode payload) {
        return reply(200, "plain", "", payload);
    }

    private static String stall() {
        return reply(200, "drip", "", array());
    }

    private void program(String... steps) throws Exception {
        control("/replace", steps);
    }

    private void append(String... steps) throws Exception {
        control("/add", steps);
    }

    private void control(String path, String[] steps) throws Exception {
        HttpRequest request =
                HttpRequest.newBuilder(admin(path))
                        .timeout(Duration.ofSeconds(5))
                        .POST(HttpRequest.BodyPublishers.ofString(String.join("\n", steps)))
                        .build();
        assertEquals(
                200, control.send(request, HttpResponse.BodyHandlers.discarding()).statusCode());
    }

    private URI admin(String path) throws Exception {
        return new URI("http", null, peer.getHost(), peer.getMappedPort(18080), path, null, null);
    }

    private String trace() throws Exception {
        HttpRequest request =
                HttpRequest.newBuilder(admin("/log")).timeout(Duration.ofSeconds(5)).build();
        HttpResponse<String> response = control.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        return response.body();
    }

    private List<Wire> events() throws Exception {
        List<Wire> calls = new ArrayList<>();
        for (String line : trace().split("\n")) {
            if (line.startsWith("call\t")) {
                String[] parts = line.split("\t", -1);
                assertEquals("GET", parts[2]);
                assertEquals("true", parts[3], "expected exact Bearer authentication");
                assertEquals("0", parts[5], "read endpoints receive no request body");
                URI target = URI.create(parts[1]);
                Map<String, String> query = new TreeMap<>();
                if (target.getRawQuery() != null) {
                    for (String item : target.getRawQuery().split("&")) {
                        String[] pair = item.split("=", 2);
                        assertEquals(2, pair.length);
                        assertNull(
                                query.put(
                                        pair[0],
                                        URLDecoder.decode(pair[1], StandardCharsets.UTF_8)));
                    }
                }
                calls.add(new Wire(call(target.getPath(), query), Long.parseLong(parts[4])));
            }
        }
        return calls;
    }

    @SafeVarargs
    private final void wire(Map.Entry<String, Map<String, String>>... expected) throws Exception {
        assertEquals(
                List.of(expected),
                events().stream().map(wire -> wire.request).collect(Collectors.toList()));
    }

    private static Map.Entry<String, Map<String, String>> call(
            String path, Map<String, String> query) {
        return Map.entry(path, query);
    }

    private static Map.Entry<String, Map<String, String>> accounts() {
        return call(ACCOUNTS, Map.of("type", "advertiser"));
    }

    private static Map.Entry<String, Map<String, String>> campaign(String first, String last) {
        return call(
                CAMPAIGN,
                Map.of("interval", "day", "startDate", first, "endDate", last, "timezone", "UTC"));
    }

    private static Map.Entry<String, Map<String, String>> transactions(String first, String last) {
        return call(
                TRANSACTIONS,
                Map.of(
                        "startDate",
                        first + "T00:00:00",
                        "endDate",
                        last + "T23:59:59",
                        "dateType",
                        "transaction",
                        "timezone",
                        "UTC",
                        "showBasketProducts",
                        "true"));
    }

    private static Map.Entry<String, Map<String, String>> publisher(String date) {
        return call(
                PUBLISHER_REPORT,
                Map.of(
                        "startDate",
                        date,
                        "endDate",
                        date,
                        "timezone",
                        "UTC",
                        "dateType",
                        "transaction"));
    }

    private static Map.Entry<String, Map<String, String>> creative(String date, String region) {
        return call(
                CREATIVE,
                Map.of(
                        "startDate",
                        date,
                        "endDate",
                        date,
                        "timezone",
                        "UTC",
                        "dateType",
                        "transaction",
                        "region",
                        region));
    }

    private void output(Job job, List<ObjectNode> expected) throws Exception {
        assertEquals(bag(expected), bag(engine.read(job.output)));
    }

    private void committed(Job job, List<ObjectNode> expected) throws Exception {
        await(() -> bag(expected).equals(bag(engine.read(job.output))), "committed LocalFile rows");
    }

    private static Map<ObjectNode, Long> bag(List<ObjectNode> input) {
        return input.stream().collect(Collectors.groupingBy(row -> row, Collectors.counting()));
    }

    private static void success(Container.ExecResult exit) {
        assertEquals(0, exit.getExitCode(), () -> exit.getStdout() + exit.getStderr());
    }

    private void failed(Container.ExecResult exit, String diagnosed) {
        assertNotEquals(0, exit.getExitCode(), diagnosed);
        String logs = exit.getStdout() + exit.getStderr() + engine.getServerLogs();
        assertTrue(logs.contains(diagnosed), () -> "Missing diagnostic " + diagnosed);
        assertFalse(logs.contains(TOKEN), "credential leaked");
        assertFalse(logs.contains(PRIVATE_RESPONSE), "provider body leaked");
    }

    private static void await(Condition condition, String detail) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(45L);
        do {
            if (condition.check()) {
                return;
            }
            Thread.sleep(100);
        } while (System.nanoTime() - deadline < 0L);
        throw new AssertionError("Timed out waiting for " + detail);
    }

    /**
     * A received GET and its timestamp from the peer's monotonic clock.
     *
     * <p>Comparing receipt times verifies that the configured retry lower bound reached the wire.
     */
    private static final class Wire {
        private final Map.Entry<String, Map<String, String>> request;
        private final long at;

        private Wire(Map.Entry<String, Map<String, String>> request, long at) {
            this.request = request;
            this.at = at;
        }
    }

    /**
     * A regular CLI configuration and its corresponding transactional sink directory.
     *
     * <p>Restore retains the same destination so repeated output would remain visible.
     */
    private static final class Job implements AutoCloseable {
        private final Path file;
        private final String name;
        private final String output;

        private Job(Path file, String output) {
            this.file = file;
            this.name = "/" + file.getFileName();
            this.output = output;
        }

        @Override
        public void close() throws Exception {
            Files.deleteIfExists(file);
        }
    }

    /**
     * A checked observation of the peer or engine state.
     *
     * <p>API and file exceptions fail the test directly rather than being interpreted as waiting.
     */
    @FunctionalInterface
    private interface Condition {
        boolean check() throws Exception;
    }

    /**
     * Zeta engine exposing exact committed output and REST checkpoint observations.
     *
     * <p>A non-UTC JVM setting forces date tests to rely on the connector's UTC configuration.
     */
    private static final class Zeta extends SeaTunnelContainer {
        private final HttpClient rest = HttpClient.newHttpClient();

        @Override
        protected String getJavaToolOptions() {
            return "-Duser.timezone=America/Los_Angeles";
        }

        private void install(String path, String contents) {
            server.copyFileToContainer(Transferable.of(contents), path);
        }

        private Container.ExecResult submit(String path, String jobId) throws Exception {
            return executeJob(server, path, jobId, List.of());
        }

        private List<ObjectNode> read(String directory) throws Exception {
            Container.ExecResult files =
                    server.execInContainer(
                            "sh",
                            "-c",
                            "if [ -d '"
                                    + directory
                                    + "' ]; then find '"
                                    + directory
                                    + "' -maxdepth 1 -type f -name 'delivered_*' -exec cat {} +; fi");
            assertEquals(0, files.getExitCode(), files.getStderr());
            List<ObjectNode> output = new ArrayList<>();
            for (String line : files.getStdout().split("\\R")) {
                if (!line.isBlank()) {
                    ObjectNode row = (ObjectNode) JSON.readTree(line);
                    assertTrue(
                            row.path("payload").isTextual(),
                            "catalog payload uses a STRING column");
                    row.set("payload", JSON.readTree(row.path("payload").textValue()));
                    output.add(row);
                }
            }
            return output;
        }

        private long checkpoints(String id) throws Exception {
            URI url =
                    new URI(
                            "http",
                            null,
                            server.getHost(),
                            server.getMappedPort(8080),
                            "/jobs/checkpoints/" + id,
                            null,
                            null);
            HttpResponse<String> response =
                    rest.send(
                            HttpRequest.newBuilder(url).timeout(Duration.ofSeconds(5)).build(),
                            HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                return 0L;
            }
            long completed = 0L;
            for (JsonNode pipeline : JSON.readTree(response.body()).path("pipelines")) {
                completed += pipeline.path("counts").path("completed").asLong();
            }
            return completed;
        }
    }
}
