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
package org.apache.seatunnel.e2e.connector.adobecommerce;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.seatunnel.SeaTunnelContainer;
import org.apache.seatunnel.e2e.common.util.DependencyJar;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.postgresql.Driver;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests the packaged source using a wire-level HTTPS contract and rows committed by Zeta. */
class AdobeCommerceSourceIT extends TestSuiteBase {
    private static final String HOST = "tenant.commerce.test";
    private static final String URL = "https://" + HOST + ":8443/shop";
    private static final String SECRET = "test-keystore";
    private static final String PRIVATE = "provider-private-commerce-note";
    private static final String DECIMAL = "9876543210987654321.123456789012345678900";
    private static final String LARGE_ID = "1234567890123456789012345678901234567890";
    private static final String API = "/shop/rest/en_us/V1/";
    private static final String PAGE = "searchCriteria[currentPage]";
    private static final String LIMIT = "searchCriteria[pageSize]";
    private static final String STOP = "searchCriteria[filter_groups][1][filters][0][value]";

    private static final String ALL = "/commerce_catalog.conf";
    private static final String WINDOW = "/commerce_window.conf";
    private static final String ORDERS = "/commerce_orders.conf";
    private static final String RECOVERY = "/commerce_recovery.conf";
    private static final String TERMINAL = "/commerce_terminal.conf";

    private static final List<String> NAMES =
            Arrays.asList(
                    "carts",
                    "orders",
                    "coupons",
                    "invoices",
                    "products",
                    "customers",
                    "shipments",
                    "tax_rates",
                    "creditmemos",
                    "sales_rules",
                    "store_views",
                    "tax_classes",
                    "store_groups",
                    "transactions",
                    "store_websites",
                    "customer_groups",
                    "inventory_stocks",
                    "inventory_sources",
                    "directory_currency",
                    "directory_countries",
                    "inventory_source_items");
    private static final List<String> PATHS =
            Arrays.asList(
                    "carts/search",
                    "orders",
                    "coupons/search",
                    "invoices",
                    "products",
                    "customers/search",
                    "shipments",
                    "taxRates/search",
                    "creditmemos",
                    "salesRules/search",
                    "store/storeViews",
                    "taxClasses/search",
                    "store/storeGroups",
                    "transactions",
                    "store/websites",
                    "customerGroups/search",
                    "inventory/stocks",
                    "inventory/sources",
                    "directory/currency",
                    "directory/countries",
                    "inventory/source-items");
    private static final List<String> IDS =
            Arrays.asList(
                    "id",
                    "entity_id",
                    "coupon_id",
                    "entity_id",
                    "id",
                    "id",
                    "entity_id",
                    "id",
                    "entity_id",
                    "rule_id",
                    "id",
                    "class_id",
                    "id",
                    "transaction_id",
                    "id",
                    "id",
                    "stock_id",
                    "source_code",
                    "base_currency_code",
                    "id",
                    "source_code");
    private static final List<String> UNPAGED =
            Arrays.asList(
                    "store_views",
                    "store_groups",
                    "store_websites",
                    "directory_currency",
                    "directory_countries");
    private static final List<String> UPDATED =
            Arrays.asList(
                    "carts",
                    "orders",
                    "invoices",
                    "products",
                    "customers",
                    "shipments",
                    "creditmemos");
    private static final List<String> CREATED = Arrays.asList("coupons", "transactions");

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final AtomicLong JOBS = new AtomicLong(System.currentTimeMillis());

    @TempDir private static Path storage;

    private final String token = UUID.randomUUID().toString();
    private GenericContainer<?> fixture;
    private PostgreSQLContainer<?> postgres;
    private Engine engine;

    @BeforeAll
    void start() throws Exception {
        generateCertificate();
        Path logging = storage.resolve("log.properties");
        Files.writeString(
                logging,
                "appender.console.type=Console\nappender.console.name=OUT\n"
                        + "appender.console.layout.type=PatternLayout\nappender.console.layout.pattern=%p %c %m%n\n"
                        + "rootLogger.level=info\nrootLogger.appenderRef.out.ref=OUT\n"
                        + "logger.http.name=org.apache.http\nlogger.http.level=debug\n");
        fixture =
                new GenericContainer<>(DockerImageName.parse("python:3.11-alpine"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST, "wrong.commerce.test", "redirect.commerce.test")
                        .withExposedPorts(8080)
                        .withEnv("FIXTURE_TOKEN", token)
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("commerce_fixture.py"),
                                "/case.py")
                        .withCopyFileToContainer(
                                MountableFile.forHostPath(storage.resolve("cert.pem")), "/case.crt")
                        .withCopyFileToContainer(
                                MountableFile.forHostPath(storage.resolve("key.pem")), "/case.key")
                        .withCommand("python", "/case.py")
                        .waitingFor(Wait.forHttp("/ready").forPort(8080));
        fixture.start();

        postgres =
                new PostgreSQLContainer<>(DockerImageName.parse("postgres:14-alpine"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("commerce-db")
                        .withDatabaseName("test")
                        .withUsername("test")
                        .withPassword("test");
        postgres.start();
        try (Connection db = connection();
                Statement statement = db.createStatement()) {
            statement.execute("CREATE TABLE commerce_rows (resource TEXT, id TEXT, record TEXT)");
        }

        Path jdbc = DependencyJar.of(Driver.class).path();
        engine =
                new Engine() {
                    @Override
                    protected void executeExtraCommands(GenericContainer<?> server) {
                        server.withCopyFileToContainer(
                                MountableFile.forHostPath(storage.resolve("trusted.jks")),
                                "/tmp/commerce.jks");
                        server.withCopyFileToContainer(
                                MountableFile.forHostPath(logging), "/tmp/commerce-log.properties");
                        server.withCopyFileToContainer(
                                MountableFile.forHostPath(jdbc),
                                "/tmp/seatunnel/plugins/Jdbc/lib/postgresql.jar");
                    }

                    @Override
                    protected String getJavaToolOptions() {
                        return "-Djavax.net.ssl.trustStore=/tmp/commerce.jks -Djavax.net.ssl.trustStorePassword="
                                + SECRET
                                + " -Dlog4j2.configurationFile=/tmp/commerce-log.properties";
                    }
                };
        engine.startUp();
    }

    private static void generateCertificate() throws Exception {
        Path storePath = storage.resolve("site.p12");
        String keytool = Paths.get(System.getProperty("java.home"), "bin", "keytool").toString();
        Process process =
                new ProcessBuilder(
                                keytool,
                                "-genkeypair",
                                "-noprompt",
                                "-alias",
                                "site",
                                "-keyalg",
                                "RSA",
                                "-keysize",
                                "2048",
                                "-validity",
                                "2",
                                "-dname",
                                "CN=" + HOST,
                                "-ext",
                                "SAN=dns:" + HOST + ",dns:redirect.commerce.test",
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                storePath.toString(),
                                "-storepass",
                                SECRET)
                        .redirectErrorStream(true)
                        .redirectOutput(storage.resolve("generate.log").toFile())
                        .start();
        if (!process.waitFor(30, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            throw new IllegalStateException("Fixture certificate generator timed out");
        }
        assertEquals(0, process.exitValue());
        KeyStore privateKeys = KeyStore.getInstance("PKCS12");
        try (InputStream source = Files.newInputStream(storePath)) {
            privateKeys.load(source, SECRET.toCharArray());
        }
        Certificate identity = privateKeys.getCertificate("site");
        pem("CERTIFICATE", identity.getEncoded(), storage.resolve("cert.pem"));
        pem(
                "PRIVATE KEY",
                privateKeys.getKey("site", SECRET.toCharArray()).getEncoded(),
                storage.resolve("key.pem"));
        KeyStore trusted = KeyStore.getInstance("JKS");
        trusted.load(null, null);
        trusted.setCertificateEntry("site", identity);
        try (OutputStream out = Files.newOutputStream(storage.resolve("trusted.jks"))) {
            trusted.store(out, SECRET.toCharArray());
        }
    }

    private static void pem(String type, byte[] content, Path target) throws Exception {
        String data = Base64.getMimeEncoder(64, new byte[] {'\n'}).encodeToString(content);
        Files.writeString(
                target,
                "-----BEGIN " + type + "-----\n" + data + "\n-----END " + type + "-----\n",
                StandardCharsets.US_ASCII);
    }

    @AfterAll
    void stop() throws Exception {
        try {
            if (engine != null) {
                engine.tearDown();
            }
        } finally {
            try {
                if (postgres != null) {
                    postgres.stop();
                }
            } finally {
                if (fixture != null) {
                    fixture.stop();
                }
            }
        }
    }

    @Test
    void readsAllTwentyOneResourcesAndPreservesTheirWireData() throws Exception {
        select("catalog");
        int before = engine.getServerLogs().length();
        successful(run(ALL, args(URL, token, 2, 5, 5000, 65536, 100000)));
        List<String> expectedRows = new ArrayList<>();
        List<String> expectedRequests = new ArrayList<>();
        for (int i = 0; i < NAMES.size(); i++) {
            String resource = NAMES.get(i);
            int count =
                    "orders".equals(resource)
                            ? 3
                            : "directory_currency".equals(resource)
                                    ? 1
                                    : UNPAGED.contains(resource) ? 2 : 4;
            expectedRows.addAll(expected(resource, 0, count));
            expectedRequests.add(
                    API + PATHS.get(i) + ":" + (UNPAGED.contains(resource) ? "" : "1"));
            if (!UNPAGED.contains(resource)) {
                expectedRequests.add(API + PATHS.get(i) + ":2");
            }
        }
        assertRows(expectedRows);
        assertEquals(expectedRequests, requestPositions());
        for (JsonNode request : calls()) {
            assertEquals(HOST + ":8443", request.path("host").asText());
            assertTrue(request.path("authorized").asBoolean());
            assertTrue(request.path("gzip").asBoolean());
            if (request.path("query").has(LIMIT)) {
                assertEquals("2", request.path("query").path(LIMIT).path(0).asText());
            } else {
                assertTrue(request.path("query").isEmpty());
            }
        }
        privateFree(engine.getServerLogs().substring(before));
    }

    @Test
    void usesInclusiveAndExclusiveBoundsOnOnlyTheResourcesWithADateContract() throws Exception {
        select("window");
        successful(run(WINDOW, args(URL, token, 2, 5, 5000, 65536, 100000)));
        List<String> output = new ArrayList<>();
        for (String resource : NAMES) {
            if (cursor(resource) != null) {
                output.add(row(resource, 1, true));
                output.add(row(resource, 2, true));
            } else {
                output.addAll(
                        expected(
                                resource,
                                0,
                                "directory_currency".equals(resource)
                                        ? 1
                                        : UNPAGED.contains(resource) ? 2 : 1));
            }
        }
        assertRows(output);
        JsonNode observed = calls();
        assertEquals(21, observed.size());
        for (int i = 0; i < observed.size(); i++) {
            String resource = NAMES.get(i);
            JsonNode query = observed.get(i).path("query");
            if (cursor(resource) == null) {
                assertFalse(query.has(STOP));
            } else {
                assertEquals(
                        "sales_rules".equals(resource) ? "2023-07-05" : "2023-07-04 12:00:00",
                        query.path(STOP).path(0).asText());
            }
        }
        rejected(WINDOW, "bad-window", "outside the UTC scan window", 1, 5, 5000, 65536);
    }

    @Test
    void validatesTotalCountPageEchoAndFiniteProgress() throws Exception {
        rejected(ORDERS, "bad-total", "missing or invalid total_count", 1, 5, 5000, 65536);
        rejected(ORDERS, "bad-echo", "search_criteria does not match", 1, 5, 5000, 65536);
        rejected(ORDERS, "short", "page length is inconsistent", 1, 5, 5000, 65536);
        rejected(ORDERS, "empty-more", "page length is inconsistent", 1, 5, 5000, 65536);
        rejected(ORDERS, "repeat", "pagination repeated a previous page", 2, 5, 5000, 65536);
        rejected(ORDERS, "repeat", "max_pages exceeded", 1, 1, 5000, 65536);
        select("empty");
        successful(run(ORDERS, args(URL, token, 2, 5, 5000, 65536, 100000)));
        assertRows(Collections.emptyList());
        assertEquals(Collections.singletonList(API + "orders:1"), requestPositions());

        select("shrink");
        successful(run(ORDERS, args(URL, token, 2, 5, 5000, 65536, 100000)));
        assertRows(expected("orders", 0, 2));
        assertEquals(Arrays.asList(API + "orders:1", API + "orders:2"), requestPositions());
    }

    @Test
    void failsSafelyOnStatusRedirectInvalidBodyAndTlsMismatch() throws Exception {
        rejected(ORDERS, "unauthorized", "HTTP 401", 1, 5, 4000, 65536);
        rejected(ORDERS, "forbidden", "HTTP 403", 1, 5, 4000, 65536);
        rejected(ORDERS, "redirect", "HTTP 302", 1, 5, 4000, 65536);
        assertEquals(Collections.singletonList(API + "orders:1"), requestPositions());
        rejected(ORDERS, "huge", "response exceeds max_response_bytes", 1, 5, 4000, 2048);
        rejected(ORDERS, "bad-json", "invalid JSON response", 1, 5, 4000, 65536);
        rejected(
                ORDERS, "provider-error", "provider reported an error response", 1, 5, 4000, 65536);

        select("catalog");
        int before = engine.getServerLogs().length();
        failure(
                run(
                        ORDERS,
                        args(
                                "https://wrong.commerce.test:8443/shop",
                                token,
                                2,
                                5,
                                4000,
                                65536,
                                100000)),
                before,
                "TLS peer verification");
        assertTrue(calls().isEmpty());

        select("catalog");
        before = engine.getServerLogs().length();
        String wrongToken = UUID.randomUUID().toString();
        Container.ExecResult wrong = run(ORDERS, args(URL, wrongToken, 2, 5, 4000, 65536, 100000));
        failure(wrong, before, "HTTP 403");
        assertFalse(
                (engine.getServerLogs().substring(before) + wrong.getStdout() + wrong.getStderr())
                        .contains(wrongToken));
        assertFalse(calls().get(0).path("authorized").asBoolean());
    }

    @Test
    void respectsServerBackoffAndStopsAtTheAttemptAndDeadlineLimits() throws Exception {
        select("retries");
        int before = engine.getServerLogs().length();
        successful(run(ORDERS, args(URL, token, 2, 5, 7000, 65536, 100000)));
        assertRows(expected("orders", 0, 1));
        assertEquals(Collections.nCopies(3, API + "orders:1"), requestPositions());
        JsonNode requests = calls();
        assertTrue(
                requests.get(1).path("start_ms").asLong()
                                - requests.get(0).path("start_ms").asLong()
                        >= 950);
        privateFree(engine.getServerLogs().substring(before));
        rejected(ORDERS, "busy", "HTTP 500 after 3 attempts", 3, 5, 7000, 65536);
        rejected(
                ORDERS,
                "long-retry",
                "retry wait exceeds the remaining page deadline",
                1,
                5,
                2500,
                65536);
    }

    @Test
    void closesStalledAndTricklingResponsesAndCancelsAnInFlightRequest() throws Exception {
        rejected(ORDERS, "stall", "page deadline exceeded", 1, 5, 2500, 65536);
        awaitClose(0, 10000);
        rejected(ORDERS, "trickle", "page deadline exceeded", 1, 5, 2500, 65536);
        awaitClose(0, 10000);

        select("cancel");
        String id = id();
        CompletableFuture<Container.ExecResult> active =
                submit(ORDERS, id, args(URL, token, 2, 5, 60000, 65536, 100000));
        try {
            awaitHold(0);
            awaitNewCheckpoint(id);
            successful(engine.cancelJob(id));
            await().atMost(30, TimeUnit.SECONDS)
                    .until(() -> "CANCELED".equals(engine.getJobStatus(id)));
            active.get(45, TimeUnit.SECONDS);
            awaitClose(0, 45000);
            assertRows(Collections.emptyList());
        } finally {
            cleanup(id);
        }
    }

    @Test
    void restoresAtACommittedPageWithACoordinatorFrozenTimeWindow() throws Exception {
        select("boundary");
        String id = id();
        String mismatched = id();
        List<String> options = args(URL, token, 2, 5, 60000, 65536, 100000);
        CompletableFuture<Container.ExecResult> active = submit(RECOVERY, id, options);
        try {
            awaitHold(1);
            await().atMost(25, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertRows(expected("orders", 0, 2)));
            awaitNewCheckpoint(id);
            successful(engine.savepointJob(id));
            successful(active.get(60, TimeUnit.SECONDS));
            awaitClose(1, 45000);
            assertFalse(calls().get(1).path("released").asBoolean());
            String frozen = calls().get(0).path("query").path(STOP).path(0).asText();
            assertFalse(frozen.isEmpty());

            int before = engine.getServerLogs().length();
            failure(
                    engine.restoreJobWithCheckpoint(
                            RECOVERY, id, mismatched, args(URL, token, 3, 5, 60000, 65536, 100000)),
                    before,
                    "checkpoint is incompatible");
            assertEquals(2, calls().size());

            control("/release");
            successful(engine.restoreJob(RECOVERY, id, options.toArray(new String[0])));
            assertEquals(
                    Arrays.asList(API + "orders:1", API + "orders:2", API + "orders:2"),
                    requestPositions());
            assertRows(expected("orders", 0, 3));
            for (JsonNode request : calls()) {
                assertEquals(frozen, request.path("query").path(STOP).path(0).asText());
            }
        } finally {
            cleanup(id, mismatched);
        }
    }

    @Test
    void verifiesAndResumesARealMidPageCheckpoint() throws Exception {
        select("midpage");
        String id = id();
        String changed = id();
        List<String> slow = args(URL, token, 100, 2, 60000, 65536, 4);
        List<String> fast = args(URL, token, 100, 2, 60000, 65536, 100000);
        CompletableFuture<Container.ExecResult> active = submit(ORDERS, id, slow);
        try {
            await().atMost(40, TimeUnit.SECONDS).until(() -> rows().size() >= 4);
            successful(engine.savepointJob(id));
            successful(active.get(60, TimeUnit.SECONDS));
            int count = rows().size();
            assertTrue(count > 0 && count < 100, "Savepoint did not capture a partial page");
            assertEquals(Collections.singletonList(API + "orders:1"), requestPositions());

            control("/changed");
            int before = engine.getServerLogs().length();
            failure(
                    engine.restoreJobWithCheckpoint(ORDERS, id, changed, fast),
                    before,
                    "checkpointed page changed during recovery");
            assertEquals(count, rows().size());

            control("/unchanged");
            successful(engine.restoreJob(ORDERS, id, fast.toArray(new String[0])));
            assertEquals(Collections.nCopies(3, API + "orders:1"), requestPositions());
            assertRows(expected("orders", 0, 100));
        } finally {
            cleanup(id, changed);
        }
    }

    @Test
    void restoresWithTerminalSourceWithoutIssuingAnotherGet() throws Exception {
        select("terminal");
        String id = id();
        List<String> slow = args(URL, token, 2, 5, 60000, 65536, 24);
        List<String> fast = args(URL, token, 2, 5, 60000, 65536, 100000);
        CompletableFuture<Container.ExecResult> active = submit(TERMINAL, id, slow);
        try {
            await().atMost(40, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                assertRows(expected("orders", 0, 1));
                                assertEquals("RUNNING", engine.getJobStatus(id));
                                assertTrue(engine.hasCompletedPipeline(id));
                            });
            successful(engine.savepointJob(id));
            successful(active.get(60, TimeUnit.SECONDS));
            successful(engine.restoreJob(TERMINAL, id, fast.toArray(new String[0])));
            assertEquals(Collections.singletonList(API + "orders:1"), requestPositions());
            assertRows(expected("orders", 0, 1));
        } finally {
            cleanup(id);
        }
    }

    private void rejected(
            String config,
            String mode,
            String reason,
            int requestCount,
            int maxPages,
            int deadline,
            int bytes)
            throws Exception {
        select(mode);
        int mark = engine.getServerLogs().length();
        failure(run(config, args(URL, token, 2, maxPages, deadline, bytes, 100000)), mark, reason);
        assertEquals(requestCount, calls().size());
    }

    private void failure(Container.ExecResult result, int mark, String reason) {
        assertNotEquals(0, result.getExitCode());
        String actual =
                engine.getServerLogs().substring(mark) + result.getStdout() + result.getStderr();
        assertTrue(actual.contains(reason), "Expected bounded error " + reason);
        privateFree(actual);
    }

    private void privateFree(String logs) {
        assertFalse(
                logs.contains(token) || logs.contains(PRIVATE),
                "Private message or bearer was logged");
    }

    private void awaitHold(int request) {
        await().atMost(45, TimeUnit.SECONDS)
                .until(() -> calls().path(request).path("held").asBoolean());
    }

    private void awaitClose(int request, long max) {
        await().atMost(15, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            long millis = calls().path(request).path("closed_ms").asLong(-1);
                            assertTrue(
                                    millis >= 0 && millis <= max,
                                    "The provider did not observe prompt connection closure");
                        });
    }

    private void awaitNewCheckpoint(String id) {
        long baseline = engine.getCompletedCheckpointCount(id);
        await().atMost(30, TimeUnit.SECONDS)
                .until(() -> engine.getCompletedCheckpointCount(id) > baseline);
    }

    private static void successful(Container.ExecResult result) {
        assertEquals(0, result.getExitCode());
    }

    private static String id() {
        return Long.toString(JOBS.incrementAndGet());
    }

    private Container.ExecResult run(String config, List<String> args) throws Exception {
        return engine.executeJob(config, id(), args.toArray(new String[0]));
    }

    private CompletableFuture<Container.ExecResult> submit(
            String config, String job, List<String> args) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return engine.executeJob(config, job, args.toArray(new String[0]));
                    } catch (Exception ex) {
                        throw new IllegalStateException("Failed to execute fixture job", ex);
                    }
                });
    }

    private static List<String> args(
            String url, String token, int limit, int maxPages, int deadline, int bytes, int rate) {
        return Arrays.asList(
                "fixture_url=" + url,
                "fixture_token=" + token,
                "fixture_limit=" + limit,
                "fixture_max_pages=" + maxPages,
                "fixture_deadline=" + deadline,
                "fixture_bytes=" + bytes,
                "fixture_rate=" + rate);
    }

    private void cleanup(String... jobs) throws Exception {
        control("/release");
        for (String job : jobs) {
            String status = engine.getJobStatus(job);
            if ("RUNNING".equals(status)
                    || "PENDING".equals(status)
                    || "RESTARTING".equals(status)) {
                engine.cancelJob(job);
            }
        }
    }

    private void select(String mode) throws Exception {
        control("/select?mode=" + mode);
        try (Connection db = connection();
                Statement query = db.createStatement()) {
            query.execute("TRUNCATE TABLE commerce_rows");
        }
    }

    private List<String> requestPositions() throws Exception {
        List<String> seen = new ArrayList<>();
        for (JsonNode call : calls()) {
            seen.add(
                    call.path("path").asText()
                            + ":"
                            + call.path("query").path(PAGE).path(0).asText());
        }
        return seen;
    }

    private JsonNode calls() throws Exception {
        return control("/calls");
    }

    private JsonNode control(String path) throws Exception {
        return get(new URL("http", fixture.getHost(), fixture.getMappedPort(8080), path));
    }

    private static JsonNode get(URL target) throws Exception {
        HttpURLConnection client = (HttpURLConnection) target.openConnection();
        client.setConnectTimeout(5000);
        client.setReadTimeout(5000);
        try (InputStream input = client.getInputStream()) {
            return JSON.readTree(input);
        } finally {
            client.disconnect();
        }
    }

    private Connection connection() throws Exception {
        return DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }

    private List<String> rows() throws Exception {
        List<String> found = new ArrayList<>();
        try (Connection db = connection();
                Statement query = db.createStatement();
                ResultSet result =
                        query.executeQuery("SELECT resource, id, record FROM commerce_rows")) {
            while (result.next()) {
                found.add(
                        result.getString(1)
                                + "|"
                                + result.getString(2)
                                + "|"
                                + result.getString(3));
            }
        }
        Collections.sort(found);
        return found;
    }

    private void assertRows(List<String> expected) throws Exception {
        List<String> ordered = new ArrayList<>(expected);
        Collections.sort(ordered);
        assertEquals(ordered, rows());
    }

    private static List<String> expected(String resource, int from, int end) {
        return IntStream.range(from, end)
                .mapToObj(i -> row(resource, i, false))
                .collect(Collectors.toList());
    }

    private static String cursor(String resource) {
        if (UPDATED.contains(resource)) {
            return "updated_at";
        } else if (CREATED.contains(resource)) {
            return "created_at";
        } else if ("sales_rules".equals(resource)) {
            return "from_date";
        }
        return null;
    }

    private static String row(String resource, int index, boolean window) {
        String identifier = Integer.toString(index + 1);
        boolean numeric = true;
        if ("products".equals(resource) && index == 3) {
            identifier = LARGE_ID;
        } else if ("directory_currency".equals(resource)) {
            identifier = "USD";
            numeric = false;
        } else if ("inventory_sources".equals(resource)) {
            identifier = String.format(Locale.ROOT, "store-%03d", index);
            numeric = false;
        } else if ("inventory_source_items".equals(resource)) {
            identifier = "north";
            numeric = false;
        } else if ("directory_countries".equals(resource)) {
            identifier = index == 0 ? "US" : "JP";
            numeric = false;
        }

        String wireId = numeric ? identifier : quote(identifier);
        StringBuilder data =
                new StringBuilder("{\"")
                        .append(IDS.get(NAMES.indexOf(resource)))
                        .append("\":")
                        .append(wireId)
                        .append(",\"description\":\"Café 雪\"");
        if ("inventory_source_items".equals(resource)) {
            String sku = String.format(Locale.ROOT, "SKU+雪-%03d", index);
            data.append(",\"sku\":").append(quote(sku));
            identifier = "[" + wireId + "," + quote(sku) + "]";
        }
        if ("directory_currency".equals(resource)) {
            data.append(
                            ",\"available_currency_codes\":[\"USD\",\"JPY\"],\"exchange_rates\":[{\"currency_to\":\"JPY\",\"rate\":")
                    .append(DECIMAL)
                    .append("}]");
        }
        String field = cursor(resource);
        if ("from_date".equals(field)) {
            data.append(",\"from_date\":")
                    .append(window ? quote(index == 1 ? "2023-07-02" : "2023-07-04") : "null");
        } else if (field != null) {
            String time =
                    window
                            ? index == 1 ? "2023-07-01 12:00:00" : "2023-07-04 11:59:59"
                            : "2023-07-02 01:00:00";
            data.append(',').append(quote(field)).append(':').append(quote(time));
        }
        data.append(",\"extension_attributes\":{\"empty\":null,\"values\":[true,\"雪\",")
                .append(DECIMAL)
                .append("]}}");
        return resource + "|" + identifier + "|" + data;
    }

    private static String quote(String value) {
        return "\"" + value + "\"";
    }

    private static class Engine extends SeaTunnelContainer {
        boolean hasCompletedPipeline(String job) throws Exception {
            URL overview =
                    new URL(
                            "http",
                            server.getHost(),
                            server.getMappedPort(8080),
                            "/jobs/checkpoints/" + job);
            for (JsonNode pipeline : get(overview).path("pipelines")) {
                if ("completed-point"
                        .equals(pipeline.path("latestCompleted").path("checkpointType").asText())) {
                    return true;
                }
            }
            return false;
        }
    }
}
