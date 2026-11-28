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
package org.apache.seatunnel.e2e.connector.braintree;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.braintree.BraintreeRecoveryProbe;
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
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
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
 * Runs the Braintree v6 XML contract through Zeta and a transactional LocalFile sink.
 *
 * <p>An isolated HTTPS peer verifies native methods, merchant paths, Basic headers and exact
 * DateRange/ID XML requests independently of production code.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BraintreeIT extends TestSuiteBase {
    private static final ObjectMapper JSON =
            new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    private static final String OPEN = "2024-05-10T12:00:15Z";
    private static final String CLOSE = "2024-05-11T12:02:20Z";
    private static final String FIRST_MINUTE = "2024-05-10T12:00:00Z";
    private static final String FINAL_MINUTE = "2024-05-11T12:02:00Z";
    private static final String DATETIME = "2024-05-10T12:00:15Z";
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

    @Test
    void readsAllSevenResourcesWithNativeWireAndExactTypedOutput() throws Exception {
        String name = "all-resources";
        List<ObjectNode> steps = new ArrayList<>();
        Map<String, JsonNode> expected = new LinkedHashMap<>();
        String merchant1 =
                item(
                        "merchant-account",
                        "merchant-A",
                        "<status>active</status><business><legal-name>数据 &amp; Co</legal-name>"
                                + "<address><postal-code>00012</postal-code></address></business>"
                                + "<funding><account-number-last-4>0009</account-number-last-4></funding>");
        steps.add(
                step(
                        "/merchant_accounts?page=1",
                        null,
                        paged(
                                "merchant-accounts",
                                3,
                                2,
                                merchant1
                                        + item(
                                                "merchant-account",
                                                "merchant-B",
                                                "<default type=\"boolean\">false</default>"))));
        steps.add(
                step(
                        "/merchant_accounts?page=2",
                        null,
                        paged(
                                "merchant-accounts",
                                3,
                                2,
                                item("merchant-account", "merchant-C", ""))));
        expect(
                expected,
                "merchant_accounts",
                "merchant-A",
                null,
                "{\"id\":\"merchant-A\",\"status\":\"active\",\"business_details\":{\"legal_name\":\"数据 & Co\",\"address_details\":{\"postal_code\":\"00012\"}},\"funding_details\":{\"account_number_last_4\":\"0009\"}}");
        expect(
                expected,
                "merchant_accounts",
                "merchant-B",
                null,
                "{\"id\":\"merchant-B\",\"default\":false}");
        expect(expected, "merchant_accounts", "merchant-C", null, "{\"id\":\"merchant-C\"}");

        String a = "00A&<", b = "00002";
        steps.add(
                step(
                        "/customers/advanced_search_ids",
                        created(FIRST_MINUTE, FINAL_MINUTE),
                        ids(2, a, b)));
        String customerA =
                item(
                        "customer",
                        a,
                        "<created-at type=\"datetime\">"
                                + DATETIME
                                + "</created-at>"
                                + "<credit-cards type=\"array\"><credit-card><last-4>0001</last-4>"
                                + "<expired type=\"boolean\">false</expired></credit-card></credit-cards>"
                                + "<unknown-object><unknown-decimal type=\"decimal\">9007199254740993.0100</unknown-decimal>"
                                + "<unknown-null nil=\"true\"/><void-list type=\"array\"/></unknown-object>");
        String customerB = dated("customer", b, DATETIME, "<first-name>名字🌊</first-name>");
        steps.add(
                step(
                                "/customers/advanced_search",
                                created(FIRST_MINUTE, FINAL_MINUTE, a, b),
                                wrap("customers", customerB + customerA))
                        .put("mode", "gzip"));
        expect(
                expected,
                "customers",
                a,
                DATETIME,
                "{\"id\":\"00A&<\",\"created_at\":\""
                        + DATETIME
                        + "\",\"credit_cards\":[{\"last_4\":\"0001\",\"expired\":false}],"
                        + "\"unknown_object\":{\"unknown_decimal\":9007199254740993.0100,\"unknown_null\":null,\"void_list\":[]},"
                        + "\"payment_methods\":[{\"last_4\":\"0001\",\"expired\":false}]}");
        expect(
                expected,
                "customers",
                b,
                DATETIME,
                "{\"id\":\"00002\",\"created_at\":\""
                        + DATETIME
                        + "\",\"first_name\":\"名字🌊\",\"payment_methods\":[]}");

        steps.add(
                step(
                        "/discounts",
                        null,
                        wrap(
                                "discounts",
                                item(
                                        "discount",
                                        "discount-A",
                                        "<amount type=\"decimal\">123456789012345678.00001</amount><never-expires type=\"boolean\">true</never-expires>"))));
        expect(
                expected,
                "discounts",
                "discount-A",
                null,
                "{\"id\":\"discount-A\",\"amount\":123456789012345678.00001,\"never_expires\":true}");
        steps.add(
                step(
                        "/plans",
                        null,
                        wrap(
                                "plans",
                                item(
                                        "plan",
                                        "plan-A",
                                        "<price>19.9900</price><add-ons type=\"array\"><add-on><quantity type=\"integer\">2</quantity>"
                                                + "<amount>5.25</amount></add-on></add-ons><discounts type=\"array\"/>"))));
        expect(
                expected,
                "plans",
                "plan-A",
                null,
                "{\"id\":\"plan-A\",\"price\":19.9900,\"add_ons\":[{\"quantity\":2,\"amount\":5.25}],\"discounts\":[]}");

        steps.add(
                step(
                        "/transactions/advanced_search_ids",
                        created(FIRST_MINUTE, FINAL_MINUTE),
                        ids(50, "900719925474099312345")));
        steps.add(
                step(
                        "/transactions/advanced_search",
                        created(FIRST_MINUTE, FINAL_MINUTE, "900719925474099312345"),
                        wrap(
                                "credit-card-transactions",
                                dated(
                                        "transaction",
                                        "900719925474099312345",
                                        DATETIME,
                                        "<amount type=\"decimal\">9007199254740993.123456789</amount>"
                                                + "<billing><postal-code>00123</postal-code></billing>"
                                                + "<global-id>opaque-global</global-id><tax-exempt type=\"boolean\">false</tax-exempt>"
                                                + "<fields><item type=\"integer\">9223372036854775808123</item></fields>"))));
        expect(
                expected,
                "transactions",
                "900719925474099312345",
                DATETIME,
                "{\"id\":\"900719925474099312345\",\"created_at\":\""
                        + DATETIME
                        + "\",\"amount\":9007199254740993.123456789,"
                        + "\"billing_details\":{\"postal_code\":\"00123\"},\"global_id\":\"opaque-global\",\"graphql_id\":\"opaque-global\","
                        + "\"tax_exempt\":false,\"fields\":{\"item\":9223372036854775808123}}");
        steps.add(
                step(
                        "/subscriptions/advanced_search_ids",
                        created(FIRST_MINUTE, FINAL_MINUTE),
                        ids(50, "sub-A")));
        steps.add(
                step(
                        "/subscriptions/advanced_search",
                        created(FIRST_MINUTE, FINAL_MINUTE, "sub-A"),
                        wrap(
                                "subscriptions",
                                dated(
                                        "subscription",
                                        "sub-A",
                                        "2024-05-10T13:00:15+01:00",
                                        "<next-billing-date type=\"date\">2024-06-10</next-billing-date><price>10.2500</price>"
                                                + "<paused nil=\"true\"/>"))));
        expect(
                expected,
                "subscriptions",
                "sub-A",
                DATETIME,
                "{\"id\":\"sub-A\",\"created_at\":\""
                        + DATETIME
                        + "\",\"next_billing_date\":\"2024-06-10\",\"price\":10.2500,\"paused\":null}");

        String disputeQuery = received("2024-05-10T00:00:00Z", "2024-05-11T23:59:00Z");
        steps.add(
                step(
                        "/disputes/advanced_search?page=1",
                        disputeQuery,
                        paged(
                                "disputes",
                                2,
                                1,
                                dispute(
                                        "dispute-A",
                                        "2024-05-10",
                                        "<amount-disputed>28.2500</amount-disputed>"))));
        steps.add(
                step(
                        "/disputes/advanced_search?page=2",
                        disputeQuery,
                        paged(
                                "disputes",
                                2,
                                1,
                                dispute("dispute-B", "2024-05-11", "<amount-won>0</amount-won>"))));
        expect(
                expected,
                "disputes",
                "dispute-A",
                "2024-05-10",
                "{\"id\":\"dispute-A\",\"received_date\":\"2024-05-10\",\"amount_disputed\":28.2500}");
        expect(
                expected,
                "disputes",
                "dispute-B",
                "2024-05-11",
                "{\"id\":\"dispute-B\",\"received_date\":\"2024-05-11\",\"amount_won\":0}");
        program(name, steps);
        success(run("all", new Settings(name, "plans")));
        assertRows(name, expected);
        consumed(name, steps.size());
    }

    @Test
    void keepsSameSecondIdsAndFiltersTheTwoExtraMinuteFragments() throws Exception {
        String name = "exact-boundaries";
        Settings settings = new Settings(name, "customers");
        settings.end = "2024-05-10T12:00:20Z";
        String[] ids = {"old", "boundary", "same-second", "upper", "future"};
        List<ObjectNode> steps =
                List.of(
                        step(
                                "/customers/advanced_search_ids",
                                created(FIRST_MINUTE, FIRST_MINUTE),
                                ids(50, ids)),
                        step(
                                "/customers/advanced_search",
                                created(FIRST_MINUTE, FIRST_MINUTE, ids),
                                wrap(
                                        "customers",
                                        dated("customer", "old", "2024-05-10T12:00:14.999999Z", "")
                                                + dated("customer", "boundary", OPEN, "")
                                                + dated("customer", "same-second", OPEN, "")
                                                + dated("customer", "upper", settings.end, "")
                                                + dated(
                                                        "customer",
                                                        "future",
                                                        "2024-05-10T12:00:20.000001Z",
                                                        ""))));
        program(name, steps);
        success(run("one", settings));
        Map<String, JsonNode> expected = new HashMap<>();
        expectCustomer(expected, "boundary", OPEN);
        expectCustomer(expected, "same-second", OPEN);
        expectCustomer(expected, "upper", settings.end);
        assertRows(name, expected);
        consumed(name, steps.size());
    }

    @Test
    void subdividesAtTheSearchCapWithoutOverlappingMinuteWindows() throws Exception {
        String name = "minute-subdivision";
        Settings settings = new Settings(name, "customers");
        settings.end = "2024-05-10T12:03:40Z";
        settings.limit = 2;
        String left = "2024-05-10T12:01:00Z", right = "2024-05-10T12:02:00Z";
        String last = "2024-05-10T12:03:00Z";
        List<ObjectNode> steps =
                List.of(
                        step(
                                "/customers/advanced_search_ids",
                                created(FIRST_MINUTE, last),
                                ids(50, "left", "right")),
                        step(
                                "/customers/advanced_search_ids",
                                created(FIRST_MINUTE, left),
                                ids(50, "left")),
                        step(
                                "/customers/advanced_search",
                                created(FIRST_MINUTE, left, "left"),
                                wrap("customers", dated("customer", "left", left, ""))),
                        step(
                                "/customers/advanced_search_ids",
                                created(right, last),
                                ids(50, "right")),
                        step(
                                "/customers/advanced_search",
                                created(right, last, "right"),
                                wrap("customers", dated("customer", "right", right, ""))));
        program(name, steps);
        success(run("one", settings));
        Map<String, JsonNode> expected = new HashMap<>();
        expectCustomer(expected, "left", left);
        expectCustomer(expected, "right", right);
        assertRows(name, expected);
        consumed(name, steps.size());
    }

    @Test
    void failsExplicitlyWhenTheMinimumSearchWindowStillLooksTruncated() throws Exception {
        String name = "unsplittable-minute";
        Settings settings = new Settings(name, "customers");
        settings.end = "2024-05-10T12:00:58Z";
        settings.limit = 2;
        program(
                name,
                List.of(
                        step(
                                "/customers/advanced_search_ids",
                                created(FIRST_MINUTE, FIRST_MINUTE),
                                ids(50, "a", "b"))));
        failure(run("one", settings), "search limit reached within one minute");
        consumed(name, 1);
        assertRows(name, Collections.emptyMap());

        name = "unsplittable-dispute-date";
        settings = new Settings(name, "disputes");
        settings.end = "2024-05-10T23:59:59Z";
        settings.limit = 2;
        program(
                name,
                List.of(
                        step(
                                "/disputes/advanced_search?page=1",
                                received("2024-05-10T00:00:00Z", "2024-05-10T23:59:00Z"),
                                paged("disputes", 2, 1, dispute("day-1", "2024-05-10", "")))));
        failure(run("one", settings), "search limit reached within one date");
        consumed(name, 1);
        assertRows(name, Collections.emptyMap());
    }

    @Test
    void splitsDisputesByCalendarDateBeforeAcceptingAnyCappedPage() throws Exception {
        String name = "dispute-subdivision";
        Settings settings = new Settings(name, "disputes");
        settings.limit = 2;
        List<ObjectNode> steps =
                List.of(
                        step(
                                "/disputes/advanced_search?page=1",
                                received("2024-05-10T00:00:00Z", "2024-05-11T23:59:00Z"),
                                paged(
                                        "disputes",
                                        2,
                                        1,
                                        dispute("ignore-capped-page", "2024-05-10", ""))),
                        step(
                                "/disputes/advanced_search?page=1",
                                received("2024-05-10T00:00:00Z", "2024-05-10T23:59:00Z"),
                                paged("disputes", 1, 1, dispute("date-first", "2024-05-10", ""))),
                        step(
                                "/disputes/advanced_search?page=1",
                                received("2024-05-11T00:00:00Z", "2024-05-11T23:59:00Z"),
                                paged("disputes", 1, 1, dispute("date-last", "2024-05-11", ""))));
        program(name, steps);
        success(run("one", settings));
        Map<String, JsonNode> expected = new HashMap<>();
        expect(
                expected,
                "disputes",
                "date-first",
                "2024-05-10",
                "{\"id\":\"date-first\",\"received_date\":\"2024-05-10\"}");
        expect(
                expected,
                "disputes",
                "date-last",
                "2024-05-11",
                "{\"id\":\"date-last\",\"received_date\":\"2024-05-11\"}");
        assertRows(name, expected);
        consumed(name, steps.size());
    }

    @Test
    void continuesAfterAnEntireBatchDisappearsAndEnforcesRequestedIdScope() throws Exception {
        String name = "deleted-batch";
        Settings settings = new Settings(name, "customers");
        settings.batch = 1;
        List<ObjectNode> steps =
                List.of(
                        step(
                                "/customers/advanced_search_ids",
                                created(FIRST_MINUTE, FINAL_MINUTE),
                                ids(50, "deleted", "still-here")),
                        step(
                                "/customers/advanced_search",
                                created(FIRST_MINUTE, FINAL_MINUTE, "deleted"),
                                "<customers/>"),
                        step(
                                "/customers/advanced_search",
                                created(FIRST_MINUTE, FINAL_MINUTE, "still-here"),
                                wrap("customers", dated("customer", "still-here", OPEN, ""))));
        program(name, steps);
        success(run("one", settings));
        Map<String, JsonNode> expected = new HashMap<>();
        expectCustomer(expected, "still-here", OPEN);
        assertRows(name, expected);
        consumed(name, steps.size());

        name = "unexpected-id";
        steps =
                List.of(
                        step(
                                "/customers/advanced_search_ids",
                                created(FIRST_MINUTE, FINAL_MINUTE),
                                ids(50, "expected")),
                        step(
                                "/customers/advanced_search",
                                created(FIRST_MINUTE, FINAL_MINUTE, "expected"),
                                wrap("customers", dated("customer", "different", OPEN, ""))));
        program(name, steps);
        failure(run("one", new Settings(name, "customers")), "outside the requested batch");
        assertRows(name, Collections.emptyMap());
        consumed(name, steps.size());
    }

    @Test
    void rejectsDuplicateSearchIdsAndInvalidPagedMetadata() throws Exception {
        String name = "duplicate-search-ids";
        program(
                name,
                List.of(
                        step(
                                "/customers/advanced_search_ids",
                                created(FIRST_MINUTE, FINAL_MINUTE),
                                ids(50, "repeated", "repeated"))));
        failure(run("one", new Settings(name, "customers")), "duplicate IDs");
        consumed(name, 1);
        for (String mode : List.of("short", "drift", "cycle", "budget")) {
            name = "paging-" + mode;
            Settings settings = new Settings(name, "merchant_accounts");
            String item = item("merchant-account", "visible", "");
            List<ObjectNode> steps = new ArrayList<>();
            steps.add(
                    step(
                            "/merchant_accounts?page=1",
                            null,
                            paged("merchant-accounts", 2, mode.equals("short") ? 2 : 1, item)));
            if (mode.equals("budget")) {
                settings.pages = 1;
            } else if (mode.equals("drift")) {
                steps.add(
                        step(
                                "/merchant_accounts?page=2",
                                null,
                                paged(
                                        "merchant-accounts",
                                        3,
                                        1,
                                        item("merchant-account", "next", ""))));
            } else if (mode.equals("cycle")) {
                steps.add(
                        step(
                                "/merchant_accounts?page=2",
                                null,
                                paged("merchant-accounts", 2, 1, item)));
            }
            program(name, steps);
            String reason =
                    mode.equals("drift")
                            ? "metadata changed"
                            : mode.equals("cycle")
                                    ? "repeated a previously accepted page"
                                    : mode.equals("short") ? "record count" : "max_pages";
            failure(run("one", settings), reason);
            consumed(name, steps.size());
        }
    }

    @Test
    void consumesASecondIndependentCoarseWindowAfterAnEmptyFirstSearch() throws Exception {
        String name = "coarse-window";
        Settings settings = new Settings(name, "customers");
        settings.days = 1;
        String firstUntil = "2024-05-11T11:59:00Z";
        String secondFrom = "2024-05-11T12:00:00Z";
        List<ObjectNode> steps =
                List.of(
                        step(
                                "/customers/advanced_search_ids",
                                created(FIRST_MINUTE, firstUntil),
                                ids(50)),
                        step(
                                "/customers/advanced_search_ids",
                                created(secondFrom, FINAL_MINUTE),
                                ids(50, "day-two")),
                        step(
                                "/customers/advanced_search",
                                created(secondFrom, FINAL_MINUTE, "day-two"),
                                wrap("customers", dated("customer", "day-two", secondFrom, ""))));
        program(name, steps);
        success(run("one", settings));
        Map<String, JsonNode> expected = new HashMap<>();
        expectCustomer(expected, "day-two", secondFrom);
        assertRows(name, expected);
        consumed(name, steps.size());
    }

    @Test
    void rejectsUnsafeOrMalformedXmlBeforeEmittingAnyElement() throws Exception {
        String[] xml = {
            "<!DOCTYPE plans [<!ENTITY evil SYSTEM \"https://credential-trap.invalid/entity\">]><plans><plan><id>&evil;</id></plan></plans>",
            "<plans><plan><id>first</id></plan><plan><id>incomplete</id></plans>",
            "<plans/><plans/>",
            "<unrelated/>",
            "<api-error-response><message>private-provider-message</message></api-error-response>",
            "<plans><plan><id>id</id><amount type=\"decimal\">not-money</amount></plan></plans>",
            "<plans><plan><id>id</id><unknown-field>a</unknown-field><unknown_field>b</unknown_field></plan></plans>"
        };
        for (int index = 0; index < xml.length; ++index) {
            String name = "bad-xml-" + index;
            program(name, List.of(step("/plans", null, xml[index])));
            failure(run("one", new Settings(name, "plans")), "Braintree");
            assertRows(name, Collections.emptyMap());
            consumed(name, 1);
        }
    }

    @Test
    void boundsWireGzipExpansionXmlDepthAndNodeCount() throws Exception {
        for (String mode : List.of("large-wire", "large-inflated", "depth", "nodes", "bad-gzip")) {
            String name = "xml-limit-" + mode;
            Settings settings = new Settings(name, "plans");
            String body =
                    wrap(
                            "plans",
                            item(
                                    "plan",
                                    "safe",
                                    "<annotation>" + "x".repeat(7000) + "</annotation>"));
            String encoding = "xml";
            if (mode.equals("large-wire") || mode.equals("large-inflated")) {
                settings.bytes = 256;
                if (mode.equals("large-inflated")) {
                    encoding = "gzip";
                }
            } else if (mode.equals("depth")) {
                body = wrap("plans", item("plan", "safe", "<a><b><c><d>too-deep</d></c></b></a>"));
                settings.depth = 4;
            } else if (mode.equals("nodes")) {
                body = wrap("plans", item("plan", "safe", "<item>too-many</item>".repeat(25)));
                settings.nodes = 16;
            } else {
                encoding = "bad-gzip";
            }
            program(name, List.of(step("/plans", null, body).put("mode", encoding)));
            failure(
                    run("one", settings),
                    mode.equals("depth") || mode.equals("nodes")
                            ? "max_xml_"
                            : mode.equals("bad-gzip") ? "Braintree" : "max_response_bytes");
            assertRows(name, Collections.emptyMap());
            consumed(name, 1);
        }
    }

    @Test
    void scopesBasicCredentialsToTheChosenMerchantAndFixedEnvironment() throws Exception {
        String name = "production-merchant";
        Settings settings = new Settings(name, "plans");
        settings.environment = "Production";
        settings.merchant = "merchant_other";
        ObjectNode first = step("/plans", null, wrap("plans", item("plan", "production", "")));
        first.put("host", BraintreePeer.PROD).put("target", "/merchants/merchant_other/plans");
        program(name, List.of(first));
        success(run("one", settings));
        Map<String, JsonNode> expected = new HashMap<>();
        expect(expected, "plans", "production", null, "{\"id\":\"production\"}");
        assertRows(name, expected);
        consumed(name, 1);
        for (int code : List.of(401, 403, 422)) {
            name = "rejected-http-" + code;
            ObjectNode denied =
                    step(
                            "/plans",
                            null,
                            "<api-error-response>private-provider-message</api-error-response>");
            program(name, List.of(denied.put("status", code)));
            failure(run("one", new Settings(name, "plans")), "HTTP " + code);
            consumed(name, 1);
            assertRows(name, Collections.emptyMap());
        }
    }

    @Test
    void honorsBoundedRetryAfterAndKeepsReadOnlyPostReplaySafe() throws Exception {
        String name = "retry-date-post";
        ObjectNode throttle =
                step("/customers/advanced_search_ids", created(FIRST_MINUTE, FINAL_MINUTE), "");
        throttle.put("status", 429).put("retryAfter", "1");
        ObjectNode unavailable =
                step("/customers/advanced_search_ids", created(FIRST_MINUTE, FINAL_MINUTE), "");
        unavailable.put("status", 503);
        ObjectNode ready =
                step(
                        "/customers/advanced_search_ids",
                        created(FIRST_MINUTE, FINAL_MINUTE),
                        ids(50));
        List<ObjectNode> steps = List.of(throttle, unavailable, ready);
        program(name, steps);
        Settings settings = new Settings(name, "customers");
        settings.retries = 2;
        success(run("one", settings));
        consumed(name, steps.size());
        List<String> times = engine.lines(name + ".times");
        assertTrue(Long.parseLong(times.get(1)) - Long.parseLong(times.get(0)) >= 900);
        assertRows(name, Collections.emptyMap());

        name = "excess-retry-after";
        settings = new Settings(name, "customers");
        settings.retries = 2;
        settings.retryDelay = 1000;
        program(name, List.of(throttle.deepCopy().put("retryAfter", "300")));
        failure(run("one", settings), "max_retry_delay_ms");
        consumed(name, 1);
    }

    @Test
    void validatesTlsAndRefusesToSendCredentialsOnRedirects() throws Exception {
        String name = "wrong-trust-anchor";
        Settings settings = new Settings(name, "plans");
        settings.trust = wrongTrust;
        program(name, List.of());
        failure(run("one", settings), "certificate");
        consumed(name, 0);
        name = "redirect";
        program(name, List.of(step("/plans", null, "").put("status", 302).put("mode", "redirect")));
        failure(run("one", new Settings(name, "plans")), "HTTP 302");
        consumed(name, 1);
    }

    @Test
    void closesATricklingXmlDocumentAtTheOperationDeadline() throws Exception {
        String name = "trickling-xml";
        Settings options = new Settings(name, "plans");
        options.request = 1000;
        options.operation = 1000;
        String body =
                wrap("plans", item("plan", "slow", "<notes>" + "x".repeat(8000) + "</notes>"));
        program(name, List.of(step("/plans", null, body).put("mode", "trickle")));
        failure(run("one", options), "deadline");
        await(name + ".disconnect");
        assertRows(name, Collections.emptyMap());
        consumed(name, 1);
    }

    @Test
    void cancelsAnActiveGatewayReadWithoutCommittingAPartialRow() throws Exception {
        String name = "cancel-active-xml";
        Settings settings = new Settings(name, "plans");
        settings.request = 60000;
        settings.operation = 70000;
        program(
                name,
                List.of(
                        step("/plans", null, wrap("plans", item("plan", "slow-cancel", "")))
                                .put("mode", "partial")));
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(
                        () -> engine.executeJob("/braintree-one.conf", job, settings.variables()));
        try {
            await(name + ".started");
            success(engine.cancelJob(job));
            running.get(30, TimeUnit.SECONDS);
            await(name + ".disconnect");
        } finally {
            engine.touch(name + ".release");
            if (!running.isDone()) {
                engine.cancelJob(job);
                running.cancel(true);
            }
        }
        assertRows(name, Collections.emptyMap());
        consumed(name, 1);
    }

    @Test
    void resumesAConfirmedIdBatchFromASavepointWithoutReopeningCompletedResources()
            throws Exception {
        String name = "pending-batch-savepoint";
        Settings settings = new Settings(name, "customers");
        settings.request = 120000;
        settings.operation = 180000;
        ObjectNode page =
                step(
                        "/customers/advanced_search",
                        created(FIRST_MINUTE, FINAL_MINUTE, "recover"),
                        wrap("customers", dated("customer", "recover", OPEN, "")));
        List<ObjectNode> before =
                List.of(
                        step(
                                "/plans",
                                null,
                                wrap(
                                        "plans",
                                        item("plan", "already-complete", "<price>3.05</price>"))),
                        step(
                                "/customers/advanced_search_ids",
                                created(FIRST_MINUTE, FINAL_MINUTE),
                                ids(50, "recover")),
                        page.deepCopy().put("mode", "partial"));
        program(name, before);
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(
                        () -> engine.executeJob("/braintree-next.conf", job, settings.variables()));
        try {
            await(name + ".started");
            long initial = engine.getCompletedCheckpointCount(job);
            Awaitility.await()
                    .atMost(35, TimeUnit.SECONDS)
                    .until(() -> engine.getCompletedCheckpointCount(job) > initial);
            success(engine.savepointJob(job));
            running.get(30, TimeUnit.SECONDS);
        } finally {
            engine.touch(name + ".release");
            if (!running.isDone()) {
                engine.cancelJob(job);
                running.cancel(true);
            }
        }
        consumed(name, before.size());
        String after = "pending-batch-restored";
        program(after, List.of(page));
        success(engine.restoreJob("/braintree-next.conf", job, settings.variables()));
        Map<String, JsonNode> expected = new HashMap<>();
        expect(
                expected,
                "plans",
                "already-complete",
                null,
                "{\"id\":\"already-complete\",\"price\":3.05}");
        expectCustomer(expected, "recover", OPEN);
        assertRows(name, expected);
        consumed(after, 1);
    }

    @Test
    void restoresATerminalSourceInsideAnActiveJobWithoutOpeningTheGateway() throws Exception {
        String name = "terminal-savepoint";
        Settings settings = new Settings(name, "plans");
        program(name, List.of(step("/plans", null, wrap("plans", item("plan", "finished", "")))));
        String job = jobId();
        Future<Container.ExecResult> original =
                tasks.submit(
                        () ->
                                engine.executeJob(
                                        "/braintree-terminal.conf", job, settings.variables()));
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> engine.lines(name + ".requests").size() == 1);
            long first = engine.getCompletedCheckpointCount(job);
            Awaitility.await()
                    .atMost(35, TimeUnit.SECONDS)
                    .until(() -> engine.getCompletedCheckpointCount(job) > first + 1);
            success(engine.savepointJob(job));
            original.get(30, TimeUnit.SECONDS);
            Future<Container.ExecResult> resumed =
                    tasks.submit(
                            () ->
                                    engine.restoreJob(
                                            "/braintree-terminal.conf", job, settings.variables()));
            try {
                long restored = engine.getCompletedCheckpointCount(job);
                Awaitility.await()
                        .atMost(35, TimeUnit.SECONDS)
                        .until(() -> engine.getCompletedCheckpointCount(job) > restored + 1);
                Map<String, JsonNode> expected = new HashMap<>();
                expect(expected, "plans", "finished", null, "{\"id\":\"finished\"}");
                assertRows(name, expected);
                consumed(name, 1);
            } finally {
                if (!resumed.isDone()) {
                    engine.cancelJob(job);
                    resumed.get(30, TimeUnit.SECONDS);
                }
            }
        } finally {
            if (!original.isDone()) {
                engine.cancelJob(job);
                original.cancel(true);
            }
        }
    }

    @Test
    void restoresTheExactSerializedRowPositionAndRejectsChangedBatchOrScope() throws Exception {
        for (String mode : List.of("resume", "changed", "zero")) {
            String name = "packaged-reader-" + mode;
            List<ObjectNode> steps = new ArrayList<>();
            if (!mode.equals("zero")) {
                steps.add(
                        step(
                                "/customers/advanced_search_ids",
                                created(FIRST_MINUTE, FINAL_MINUTE),
                                ids(50, "first", "second")));
                String first = dated("customer", "first", OPEN, "<first-name>initial</first-name>");
                String second =
                        dated("customer", "second", CLOSE, "<first-name>second</first-name>");
                steps.add(
                        step(
                                "/customers/advanced_search",
                                created(FIRST_MINUTE, FINAL_MINUTE, "first", "second"),
                                wrap("customers", first + second)));
                steps.add(
                        step(
                                "/customers/advanced_search",
                                created(FIRST_MINUTE, FINAL_MINUTE, "first", "second"),
                                wrap(
                                        "customers",
                                        first
                                                + (mode.equals("changed")
                                                        ? dated(
                                                                "customer",
                                                                "second",
                                                                CLOSE,
                                                                "<first-name>changed</first-name>")
                                                        : second))));
            }
            program(name, steps);
            success(engine.probe(mode));
            consumed(name, steps.size());
        }
    }

    private static ObjectNode step(String target, String request, String response) {
        return JSON.createObjectNode()
                .put("method", request == null ? "GET" : "POST")
                .put("target", "/merchants/" + BraintreePeer.MERCHANT + target)
                .put("body", request == null ? "" : request)
                .put("response", response);
    }

    private static String created(String minimum, String maximum, String... ids) {
        return search("created_at", minimum, maximum, ids);
    }

    private static String received(String minimum, String maximum) {
        return search("received_date", minimum, maximum);
    }

    private static String search(String name, String minimum, String maximum, String... ids) {
        StringBuilder xml =
                new StringBuilder("<search><")
                        .append(name)
                        .append('>')
                        .append("<min type=\"datetime\">")
                        .append(minimum)
                        .append("</min>")
                        .append("<max type=\"datetime\">")
                        .append(maximum)
                        .append("</max></")
                        .append(name)
                        .append('>');
        if (ids.length != 0) {
            xml.append("<ids type=\"array\">");
            for (String id : ids) {
                xml.append("<item>").append(escape(id)).append("</item>");
            }
            xml.append("</ids>");
        }
        return xml.append("</search>").toString();
    }

    private static String ids(int size, String... values) {
        StringBuilder xml =
                new StringBuilder("<search-results><page-size type=\"integer\">")
                        .append(size)
                        .append("</page-size><ids type=\"array\">");
        for (String id : values) {
            xml.append("<item>").append(escape(id)).append("</item>");
        }
        return xml.append("</ids></search-results>").toString();
    }

    private static String dated(String tag, String id, String date, String fields) {
        return item(tag, id, "<created-at type=\"datetime\">" + date + "</created-at>" + fields);
    }

    private static String dispute(String id, String date, String fields) {
        return item(
                "dispute",
                id,
                "<received-date type=\"date\">" + date + "</received-date>" + fields);
    }

    private static String item(String tag, String id, String fields) {
        return wrap(tag, "<id>" + escape(id) + "</id>" + fields);
    }

    private static String paged(String name, int total, int size, String rows) {
        return wrap(
                name,
                "<total-items type=\"integer\">"
                        + total
                        + "</total-items>"
                        + "<page-size type=\"integer\">"
                        + size
                        + "</page-size>"
                        + rows);
    }

    private static String wrap(String name, String text) {
        return "<" + name + ">" + text + "</" + name + ">";
    }

    private static String escape(String text) {
        return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
    }

    private static void expectCustomer(Map<String, JsonNode> expected, String id, String cursor)
            throws Exception {
        ObjectNode record = JSON.createObjectNode().put("id", id).put("created_at", cursor);
        record.putArray("payment_methods");
        expect(expected, "customers", id, cursor, record.toString());
    }

    private static void expect(
            Map<String, JsonNode> expected, String resource, String id, String cursor, String raw)
            throws Exception {
        ObjectNode row = JSON.createObjectNode().put("resource", resource).put("id", id);
        if (cursor == null) {
            row.putNull("cursor");
        } else {
            row.put("cursor", cursor);
        }
        row.set("record", JSON.readTree(raw));
        assertNull(expected.put(resource + "/" + id, row));
    }

    private void assertRows(String name, Map<String, JsonNode> expected) throws Exception {
        Map<String, JsonNode> actual = new HashMap<>();
        for (String line : engine.rows(name)) {
            ObjectNode row = (ObjectNode) JSON.readTree(line);
            Set<String> fields = new HashSet<>();
            row.fieldNames().forEachRemaining(fields::add);
            assertEquals(Set.of("resource", "id", "cursor", "record"), fields);
            assertTrue(
                    row.path("record").isTextual(),
                    "the typed record is encoded in one string column");
            row.set("record", JSON.readTree(row.path("record").asText()));
            assertNull(
                    actual.put(row.path("resource").asText() + "/" + row.path("id").asText(), row));
        }
        assertEquals(expected, actual);
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

    private Container.ExecResult run(String kind, Settings settings) throws Exception {
        return engine.executeJob("/braintree-" + kind + ".conf", jobId(), settings.variables());
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
        String basic =
                Base64.getEncoder()
                        .encodeToString(
                                (BraintreePeer.ACCESS + ":" + BraintreePeer.SECRET)
                                        .getBytes(StandardCharsets.US_ASCII));
        for (String secret :
                List.of(
                        BraintreePeer.ACCESS,
                        BraintreePeer.SECRET,
                        basic,
                        "private-provider-message")) {
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
                                "CN=" + BraintreePeer.SANDBOX,
                                "-ext",
                                "SAN=dns:" + BraintreePeer.SANDBOX + ",dns:" + BraintreePeer.PROD,
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                store.toString(),
                                "-storepass",
                                BraintreePeer.PASSWORD,
                                "-keypass",
                                BraintreePeer.PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String output = new String(keytool.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, keytool.waitFor(), output);
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(store)) {
            keyStore.load(input, BraintreePeer.PASSWORD.toCharArray());
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
        final String resource;
        String environment = "Sandbox";
        String merchant = BraintreePeer.MERCHANT;
        String trust = BraintreeIT.trust;
        String end = CLOSE;
        int request = 20000;
        int operation = 30000;
        int retries;
        int retryDelay = 30000;
        int bytes = 8388608;
        int depth = 64;
        int nodes = 500000;
        int pages = 10000;
        int days = 31;
        int windows = 10000;
        int limit = 50000;
        int batch = 1000;

        Settings(String name, String resource) {
            this.name = name;
            this.resource = resource;
        }

        String[] variables() {
            return new String[] {
                "scenario=" + name,
                "resource=" + resource,
                "environment=" + environment,
                "merchant=" + merchant,
                "access=" + BraintreePeer.ACCESS,
                "secret=" + BraintreePeer.SECRET,
                "trust=" + trust,
                "start=" + OPEN,
                "end=" + end,
                "connect=1000",
                "request=" + request,
                "operation=" + operation,
                "retries=" + retries,
                "retryDelay=" + retryDelay,
                "bytes=" + bytes,
                "depth=" + depth,
                "nodes=" + nodes,
                "pages=" + pages,
                "windows=" + windows,
                "days=" + days,
                "limit=" + limit,
                "batch=" + batch
            };
        }
    }

    /**
     * Stages the independent peer and existing shaded JSON libraries in the test engine.
     *
     * <p>Host aliases and test certificates belong only to this owned container.
     */
    private static final class Engine extends SeaTunnelContainer {
        private static final String ROOT = "/tmp/braintree-peer";
        private final Path files;

        private Engine(Path files) {
            this.files = files;
        }

        /**
         * Preserves individual argv values without logging the synthetic credential settings.
         *
         * <p>The gateway test receives the reserved characters as part of the same variable.
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
            for (String host :
                    List.of(BraintreePeer.PROD, BraintreePeer.SANDBOX, BraintreePeer.TRAP)) {
                container.withExtraHost(host, "127.0.0.1");
            }
            container.withCopyFileToContainer(
                    MountableFile.forHostPath(files.resolve("trusted.p12")), ROOT + "/trusted.p12");
            container.withCopyFileToContainer(
                    MountableFile.forHostPath(files.resolve("trusted.pem")), ROOT + "/trusted.pem");
            try {
                stageClasses(container, BraintreePeer.class);
                stageClasses(container, BraintreeRecoveryProbe.class);
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
                    BraintreePeer.class.getName(),
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
                    BraintreeRecoveryProbe.class.getName(),
                    ROOT + "/trusted.pem",
                    mode,
                    OPEN,
                    CLOSE);
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
            String path = "/tmp/seatunnel_mnt/braintree-" + name;
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
