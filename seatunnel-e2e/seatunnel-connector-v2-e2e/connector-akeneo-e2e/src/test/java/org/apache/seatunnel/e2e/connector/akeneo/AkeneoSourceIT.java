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
package org.apache.seatunnel.e2e.connector.akeneo;

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

/** Runs the packaged source on Zeta against an independent HTTPS API and real JDBC sink. */
class AkeneoSourceIT extends TestSuiteBase {
    private static final String HOST = "tenant.akeneo.test";
    private static final String ORIGIN = "https://" + HOST + ":8443";
    private static final String TRUST_PASSWORD = "fixture-trust";
    private static final String PRIVATE = "private-akeneo-provider-payload";
    private static final String TOKEN = "/api/oauth/v1/token";
    private static final String PREFIX = "/api/rest/v1/";
    private static final String PRODUCT_ROUTE = PREFIX + "products-uuid";
    private static final String CATEGORY_ROUTE = PREFIX + "categories";
    private static final String FAMILY = "tailor-é+";
    private static final String FAMILY_ROUTE = PREFIX + "families/tailor-%C3%A9%2B/variants";
    private static final String CURSOR = "opaque+/=akeneo";
    private static final String CATALOG = "/akeneo_catalog.conf";
    private static final String PRODUCTS = "/akeneo_products.conf";
    private static final String CATEGORIES = "/akeneo_categories.conf";
    private static final String VARIANTS = "/akeneo_variants.conf";
    private static final String MEASURE = "/akeneo_measure.conf";
    private static final String TERMINAL = "/akeneo_terminal.conf";
    private static final AtomicLong IDS = new AtomicLong(System.currentTimeMillis());
    private static final ObjectMapper JSON = new ObjectMapper();

    @TempDir private static Path temporary;

    private final String client = UUID.randomUUID().toString();
    private final String secret = UUID.randomUUID().toString();
    private final String username = UUID.randomUUID().toString();
    private final String password = UUID.randomUUID().toString();
    private GenericContainer<?> fixture;
    private PostgreSQLContainer<?> postgres;
    private Engine engine;

    @BeforeAll
    void startFixtures() throws Exception {
        generateCertificate();
        Path loggers = temporary.resolve("http-debug.properties");
        Files.writeString(
                loggers,
                "appender.console.type=Console\nappender.console.name=OUT\n"
                        + "appender.console.layout.type=PatternLayout\nappender.console.layout.pattern=%p %c %m%n\n"
                        + "rootLogger.level=info\nrootLogger.appenderRef.out.ref=OUT\n"
                        + "logger.http.name=org.apache.http\nlogger.http.level=debug\n");
        fixture =
                new GenericContainer<>(DockerImageName.parse("python:3.11-alpine"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST, "wrong.akeneo.test", "redirect.akeneo.test")
                        .withExposedPorts(8080)
                        .withEnv("FIXTURE_CLIENT", client)
                        .withEnv("FIXTURE_SECRET", secret)
                        .withEnv("FIXTURE_USER", username)
                        .withEnv("FIXTURE_PASSWORD", password)
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("akeneo_fixture.py"),
                                "/provider.py")
                        .withCopyFileToContainer(
                                MountableFile.forHostPath(temporary.resolve("provider.crt")),
                                "/akeneo-cert.pem")
                        .withCopyFileToContainer(
                                MountableFile.forHostPath(temporary.resolve("provider.key")),
                                "/akeneo-key.pem")
                        .withCommand("python", "/provider.py")
                        .waitingFor(Wait.forHttp("/ready").forPort(8080));
        fixture.start();

        postgres =
                new PostgreSQLContainer<>(DockerImageName.parse("postgres:14-alpine"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("akeneo-db")
                        .withDatabaseName("test")
                        .withUsername("test")
                        .withPassword("test");
        postgres.start();
        try (Connection sql = sql();
                Statement query = sql.createStatement()) {
            query.execute(
                    "CREATE TABLE akeneo_rows (resource TEXT, id TEXT, parent_id TEXT, record TEXT)");
        }
        Path jdbc = DependencyJar.of(Driver.class).path();
        engine =
                new Engine() {
                    @Override
                    protected void executeExtraCommands(GenericContainer<?> container) {
                        container.withCopyFileToContainer(
                                MountableFile.forHostPath(temporary.resolve("trust.jks")),
                                "/tmp/akeneo.jks");
                        container.withCopyFileToContainer(
                                MountableFile.forHostPath(loggers), "/tmp/akeneo-log.properties");
                        container.withCopyFileToContainer(
                                MountableFile.forHostPath(jdbc),
                                "/tmp/seatunnel/plugins/Jdbc/lib/postgresql.jar");
                    }

                    @Override
                    protected String getJavaToolOptions() {
                        return "-Djavax.net.ssl.trustStore=/tmp/akeneo.jks -Djavax.net.ssl.trustStorePassword="
                                + TRUST_PASSWORD
                                + " -Dlog4j2.configurationFile=/tmp/akeneo-log.properties";
                    }
                };
        engine.startUp();
    }

    private static void generateCertificate() throws Exception {
        Path store = temporary.resolve("tls.p12");
        Process process =
                new ProcessBuilder(
                                Paths.get(System.getProperty("java.home"), "bin", "keytool")
                                        .toString(),
                                "-genkeypair",
                                "-noprompt",
                                "-alias",
                                "provider",
                                "-keyalg",
                                "RSA",
                                "-keysize",
                                "2048",
                                "-validity",
                                "2",
                                "-dname",
                                "CN=" + HOST,
                                "-ext",
                                "SAN=dns:" + HOST + ",dns:redirect.akeneo.test",
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                store.toString(),
                                "-storepass",
                                TRUST_PASSWORD)
                        .redirectErrorStream(true)
                        .redirectOutput(temporary.resolve("keytool.log").toFile())
                        .start();
        if (!process.waitFor(30, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            throw new IllegalStateException("Fixture certificate command timed out");
        }
        assertEquals(0, process.exitValue());
        KeyStore keys = KeyStore.getInstance("PKCS12");
        try (InputStream source = Files.newInputStream(store)) {
            keys.load(source, TRUST_PASSWORD.toCharArray());
        }
        Certificate server = keys.getCertificate("provider");
        pem("CERTIFICATE", server.getEncoded(), temporary.resolve("provider.crt"));
        pem(
                "PRIVATE KEY",
                keys.getKey("provider", TRUST_PASSWORD.toCharArray()).getEncoded(),
                temporary.resolve("provider.key"));
        KeyStore trust = KeyStore.getInstance("JKS");
        trust.load(null, null);
        trust.setCertificateEntry("provider", server);
        try (OutputStream sink = Files.newOutputStream(temporary.resolve("trust.jks"))) {
            trust.store(sink, TRUST_PASSWORD.toCharArray());
        }
    }

    private static void pem(String type, byte[] bytes, Path target) throws Exception {
        String data = Base64.getMimeEncoder(64, new byte[] {'\n'}).encodeToString(bytes);
        Files.writeString(
                target, "-----BEGIN " + type + "-----\n" + data + "\n-----END " + type + "-----\n");
    }

    @AfterAll
    void shutdownFixtures() throws Exception {
        try {
            if (engine != null) {
                engine.tearDown();
            }
        } finally {
            try {
                if (fixture != null) {
                    fixture.stop();
                }
            } finally {
                if (postgres != null) {
                    postgres.stop();
                }
            }
        }
    }

    @Test
    void readsExactRowsFromAllElevenResources() throws Exception {
        select("catalog");
        success(run(CATALOG, defaults()));
        List<String> expected = new ArrayList<>(products(1, 3));
        expected.add(row("categories", "furniture"));
        expected.add(row("families", FAMILY));
        expected.add(row("families", "shirts"));
        expected.add(variant("variant-left-1", FAMILY));
        expected.add(variant("variant-left-2", FAMILY));
        expected.add(variant("variant-right-1", "shirts"));
        expected.add(row("attributes", "description"));
        expected.add(row("attribute_groups", "marketing"));
        expected.add(row("association_types", "replacements"));
        expected.add(row("channels", "ecommerce"));
        expected.add(row("locales", "en_US"));
        expected.add(row("currencies", "EUR"));
        expected.add(
                row("measure_families", "Area", null, ",\"standard_unit_code\":\"SQUARE_METER\""));
        assertRows(expected);
        assertEquals(Arrays.asList("", CURSOR), valuesFor(PRODUCT_ROUTE, "search_after"));
        assertEquals(
                Arrays.asList("search_after", "search_after"),
                valuesFor(PRODUCT_ROUTE, "pagination_type"));
        assertEquals(Arrays.asList("", "2"), valuesFor(FAMILY_ROUTE, "page"));
        assertEquals(
                Collections.singletonList(""), valuesFor(PREFIX + "measurement-families", "limit"));
        for (JsonNode request : calls()) {
            assertTrue(request.path("auth").asBoolean());
        }
    }

    @Test
    void supportsTheOriginalMeasureFamilyRouteWithOffsetPagination() throws Exception {
        select("catalog");
        success(run(MEASURE, settings(ORIGIN, secret, "legacy", 2, 5, 10000, 65536, 100000)));
        assertRows(
                Arrays.asList(
                        row("measure_families", "legacy-1", null, ",\"standard\":\"PIECE\""),
                        row("measure_families", "legacy-2", null, ",\"standard\":\"PIECE\"")));
        assertEquals(
                Arrays.asList(TOKEN, PREFIX + "measure-families", PREFIX + "measure-families"),
                paths());
        assertEquals(Arrays.asList("", "2"), valuesFor(PREFIX + "measure-families", "page"));
    }

    @Test
    void refreshesAtExpirationAndReplaysARejectedPageOnce() throws Exception {
        select("expires");
        success(run(PRODUCTS, defaults()));
        assertRows(products(1, 3));
        assertEquals(Arrays.asList("password", "get", "refresh_token", "get"), kinds());

        select("refresh-on-401");
        success(run(PRODUCTS, defaults()));
        assertRows(products(1, 3));
        assertEquals(Arrays.asList("password", "get", "get", "refresh_token", "get"), kinds());
        assertEquals(Arrays.asList("", CURSOR, CURSOR), valuesFor(PRODUCT_ROUTE, "search_after"));
    }

    @Test
    void boundsTokenRetriesAndKeepsAuthFailuresDistinct() throws Exception {
        select("auth-rate");
        success(run(CATEGORIES, defaults()));
        assertRows(Collections.singletonList(row("categories", "furniture")));
        assertEquals(Arrays.asList("password", "password", "get"), kinds());
        assertTrue(
                calls().get(1).path("start_ms").asLong() - calls().get(0).path("start_ms").asLong()
                        >= 950);

        rejected(CATEGORIES, "auth-invalid", "invalid token response", 1);
        rejected(CATEGORIES, "auth-oversize", "response exceeds max_response_bytes", 1);
        rejected(CATEGORIES, "auth-drop", "token request I/O failure", 1);
        rejected(CATEGORIES, "auth-bad-refresh", "authorization: HTTP 400", 3);
        assertEquals(Arrays.asList("password", "get", "refresh_token"), kinds());
        rejected(CATEGORIES, "auth-repeated-401", "HTTP 401 after one token renewal", 4);
        rejected(CATEGORIES, "forbidden", "categories: HTTP 403", 2);
        rejected(CATEGORIES, "missing", "categories: HTTP 404", 2);
    }

    @Test
    void validatesContinuationsWithoutUsingTheirUrlAsARequestTarget() throws Exception {
        select("empty-next");
        success(run(PRODUCTS, defaults()));
        assertRows(products(2, 3));
        assertEquals(Arrays.asList("", CURSOR), valuesFor(PRODUCT_ROUTE, "search_after"));

        rejected(PRODUCTS, "foreign", "invalid continuation origin, route or pagination scope", 2);
        rejected(
                PRODUCTS,
                "with-userinfo",
                "invalid continuation origin, route or pagination scope",
                2);
        rejected(
                PRODUCTS,
                "query-change",
                "invalid continuation origin, route or pagination scope",
                2);
        rejected(
                CATEGORIES,
                "page-skip",
                "invalid continuation origin, route or pagination scope",
                2);
        rejected(
                CATEGORIES,
                "limit-change",
                "invalid continuation origin, route or pagination scope",
                2);
        rejected(
                VARIANTS,
                "wrong-parent",
                "invalid continuation origin, route or pagination scope",
                3);

        select("cursor-loop");
        int mark = engine.getServerLogs().length();
        failure(run(PRODUCTS, defaults()), mark, "repeated search-after cursor");
        assertEquals(Arrays.asList(TOKEN, PRODUCT_ROUTE, PRODUCT_ROUTE), paths());
        select("catalog");
        mark = engine.getServerLogs().length();
        failure(
                run(PRODUCTS, settings(ORIGIN, secret, "current", 2, 1, 10000, 65536, 100000)),
                mark,
                "max_pages reached with a continuation");
        assertRows(Collections.emptyList());
        assertEquals(Arrays.asList(TOKEN, PRODUCT_ROUTE), paths());
    }

    @Test
    void verifiesTransportTrustAndDiscardsSensitiveErrorPayloads() throws Exception {
        rejected(CATEGORIES, "auth-redirect", "authorization: HTTP 302", 1);
        rejected(CATEGORIES, "redirect", "categories: HTTP 302", 2);
        rejected(CATEGORIES, "oversize", "response exceeds max_response_bytes", 2);
        rejected(CATEGORIES, "bad-json", "invalid JSON response", 2);
        rejected(CATEGORIES, "invalid-envelope", "invalid collection response", 2);

        select("catalog");
        int mark = engine.getServerLogs().length();
        String wrong = UUID.randomUUID().toString();
        Container.ExecResult badAuth =
                run(CATEGORIES, settings(ORIGIN, wrong, "current", 2, 5, 10000, 65536, 100000));
        failure(badAuth, mark, "authorization: HTTP 401");
        assertFalse(
                (engine.getServerLogs().substring(mark) + badAuth.getStdout() + badAuth.getStderr())
                        .contains(wrong));
        assertFalse(calls().get(0).path("auth").asBoolean());
        assertRows(Collections.emptyList());

        select("catalog");
        mark = engine.getServerLogs().length();
        failure(
                run(
                        CATEGORIES,
                        settings(
                                "https://wrong.akeneo.test:8443",
                                secret,
                                "current",
                                2,
                                5,
                                10000,
                                65536,
                                100000)),
                mark,
                "TLS peer verification");
        assertTrue(calls().isEmpty());
        assertRows(Collections.emptyList());
    }

    @Test
    void retriesReadOnlyFailuresAndBoundsRetryTime() throws Exception {
        select("retry");
        success(run(CATEGORIES, defaults()));
        assertRows(Collections.singletonList(row("categories", "furniture")));
        JsonNode wire = calls();
        assertEquals(4, wire.size());
        assertTrue(
                wire.get(2).path("start_ms").asLong() - wire.get(1).path("start_ms").asLong()
                        >= 950);
        rejected(CATEGORIES, "busy", "HTTP 503 after 3 attempts", 4);
        rejected(CATEGORIES, "long-retry", "retry delay exceeds the remaining deadline", 2);
    }

    @Test
    void closesStalledTricklingAndCanceledRequests() throws Exception {
        for (String mode : Arrays.asList("stall", "trickle")) {
            select(mode);
            int mark = engine.getServerLogs().length();
            failure(
                    run(CATEGORIES, settings(ORIGIN, secret, "current", 2, 5, 2700, 65536, 100000)),
                    mark,
                    "page deadline exceeded");
            awaitClose(1, 10000);
            assertRows(Collections.emptyList());
        }
        select("cancel");
        String job = nextJob();
        CompletableFuture<Container.ExecResult> running =
                submit(CATEGORIES, job, recovery(2, 100000));
        try {
            awaitHold(1);
            awaitCheckpoint(job);
            success(engine.cancelJob(job));
            await().atMost(30, TimeUnit.SECONDS)
                    .until(() -> "CANCELED".equals(engine.getJobStatus(job)));
            running.get(45, TimeUnit.SECONDS);
            awaitClose(1, 45000);
            assertRows(Collections.emptyList());
        } finally {
            cleanup(job);
        }
    }

    @Test
    void restoresTheFirstPendingRequestFromANewCheckpoint() throws Exception {
        select("hold-auth");
        String job = nextJob();
        List<String> options = recovery(2, 100000);
        CompletableFuture<Container.ExecResult> running = submit(CATEGORIES, job, options);
        try {
            awaitHold(0);
            awaitCheckpoint(job);
            assertRows(Collections.emptyList());
            success(engine.savepointJob(job));
            success(running.get(60, TimeUnit.SECONDS));
            awaitClose(0, 45000);
            control("/release");
            success(engine.restoreJob(CATEGORIES, job, options.toArray(new String[0])));
            assertRows(Collections.singletonList(row("categories", "furniture")));
            assertEquals(Arrays.asList("password", "password", "get"), kinds());
        } finally {
            cleanup(job);
        }
    }

    @Test
    void restoresAnOffsetAndRejectsAChangedSourceScope() throws Exception {
        select("hold-offset");
        String job = nextJob();
        String changed = nextJob();
        List<String> options = recovery(2, 100000);
        CompletableFuture<Container.ExecResult> running = submit(CATEGORIES, job, options);
        try {
            awaitHold(2);
            await().atMost(25, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    assertRows(
                                            Collections.singletonList(
                                                    row("categories", "category-1"))));
            awaitCheckpoint(job);
            success(engine.savepointJob(job));
            success(running.get(60, TimeUnit.SECONDS));
            awaitClose(2, 45000);
            int mark = engine.getServerLogs().length();
            failure(
                    engine.restoreJobWithCheckpoint(CATEGORIES, job, changed, recovery(3, 100000)),
                    mark,
                    "checkpoint does not match the configured source scope");
            assertEquals(3, calls().size());
            control("/release");
            success(engine.restoreJob(CATEGORIES, job, options.toArray(new String[0])));
            assertRows(
                    Arrays.asList(
                            row("categories", "category-1"), row("categories", "category-2")));
            assertEquals(Arrays.asList("", "2", "2"), valuesFor(CATEGORY_ROUTE, "page"));
            assertEquals(Arrays.asList("password", "get", "get", "password", "get"), kinds());
        } finally {
            cleanup(job, changed);
        }
    }

    @Test
    void resumesFromTheSavedOpaqueProductCursor() throws Exception {
        select("hold-cursor");
        String job = nextJob();
        List<String> options = recovery(2, 100000);
        CompletableFuture<Container.ExecResult> running = submit(PRODUCTS, job, options);
        try {
            awaitHold(2);
            await().atMost(25, TimeUnit.SECONDS).untilAsserted(() -> assertRows(products(1, 2)));
            awaitCheckpoint(job);
            success(engine.savepointJob(job));
            success(running.get(60, TimeUnit.SECONDS));
            awaitClose(2, 45000);
            control("/release");
            success(engine.restoreJob(PRODUCTS, job, options.toArray(new String[0])));
            assertRows(products(1, 3));
            assertEquals(
                    Arrays.asList("", CURSOR, CURSOR), valuesFor(PRODUCT_ROUTE, "search_after"));
            assertEquals(Arrays.asList("password", "get", "get", "password", "get"), kinds());
        } finally {
            cleanup(job);
        }
    }

    @Test
    void restoresAChildPageWithoutExportingFamilyRecords() throws Exception {
        select("hold-child");
        String job = nextJob();
        List<String> options = recovery(2, 100000);
        CompletableFuture<Container.ExecResult> running = submit(VARIANTS, job, options);
        try {
            awaitHold(3);
            await().atMost(25, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    assertRows(
                                            Collections.singletonList(
                                                    variant("variant-left-1", FAMILY))));
            awaitCheckpoint(job);
            success(engine.savepointJob(job));
            success(running.get(60, TimeUnit.SECONDS));
            awaitClose(3, 45000);
            control("/release");
            success(engine.restoreJob(VARIANTS, job, options.toArray(new String[0])));
            assertRows(
                    Arrays.asList(
                            variant("variant-left-1", FAMILY),
                            variant("variant-left-2", FAMILY),
                            variant("variant-right-1", "shirts")));
            assertEquals(Arrays.asList("", "2", "2"), valuesFor(FAMILY_ROUTE, "page"));
            assertEquals(Arrays.asList("", "", "2"), valuesFor(PREFIX + "families", "page"));
        } finally {
            cleanup(job);
        }
    }

    @Test
    void rejectsAChangedSavedPageAndResumesTheUnchangedPageExactly() throws Exception {
        select("long-page");
        String job = nextJob();
        String mismatch = nextJob();
        List<String> fast = recovery(50, 100000);
        CompletableFuture<Container.ExecResult> running = submit(PRODUCTS, job, recovery(50, 2));
        try {
            await().atMost(40, TimeUnit.SECONDS).until(() -> rows().size() >= 4);
            success(engine.savepointJob(job));
            success(running.get(60, TimeUnit.SECONDS));
            List<String> committed = rows();
            assertTrue(
                    committed.size() > 0 && committed.size() < 50,
                    "Savepoint must stop within a page");
            control("/change?value=1");
            int mark = engine.getServerLogs().length();
            failure(
                    engine.restoreJobWithCheckpoint(PRODUCTS, job, mismatch, fast),
                    mark,
                    "saved page has changed");
            assertRows(committed);
            control("/change?value=0");
            success(engine.restoreJob(PRODUCTS, job, fast.toArray(new String[0])));
            assertRows(products(1, 51));
            assertEquals(Arrays.asList("", "", ""), valuesFor(PRODUCT_ROUTE, "search_after"));
        } finally {
            cleanup(job, mismatch);
        }
    }

    @Test
    void restoresTheCompletedSourceWithoutReauthenticating() throws Exception {
        select("catalog");
        String job = nextJob();
        CompletableFuture<Container.ExecResult> running = submit(TERMINAL, job, recovery(2, 24));
        try {
            await().atMost(40, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                assertRows(
                                        Collections.singletonList(row("categories", "furniture")));
                                assertEquals("RUNNING", engine.getJobStatus(job));
                                assertTrue(engine.completedPipeline(job));
                            });
            success(engine.savepointJob(job));
            success(running.get(60, TimeUnit.SECONDS));
            success(engine.restoreJob(TERMINAL, job, recovery(2, 100000).toArray(new String[0])));
            assertEquals(Arrays.asList(TOKEN, CATEGORY_ROUTE), paths());
            assertRows(Collections.singletonList(row("categories", "furniture")));
        } finally {
            cleanup(job);
        }
    }

    private List<String> defaults() {
        return settings(ORIGIN, secret, "current", 2, 5, 10000, 65536, 100000);
    }

    private List<String> recovery(int size, int rate) {
        return settings(ORIGIN, secret, "current", size, 5, 60000, 65536, rate);
    }

    private List<String> settings(
            String origin,
            String clientSecret,
            String measurement,
            int size,
            int maxPages,
            int deadline,
            int bytes,
            int rate) {
        return Arrays.asList(
                "fixture_origin=" + origin,
                "fixture_client=" + client,
                "fixture_secret=" + clientSecret,
                "fixture_user=" + username,
                "fixture_password=" + password,
                "fixture_measurement=" + measurement,
                "fixture_size=" + size,
                "fixture_max_pages=" + maxPages,
                "fixture_deadline=" + deadline,
                "fixture_bytes=" + bytes,
                "fixture_rate=" + rate);
    }

    private void rejected(String config, String mode, String message, int observed)
            throws Exception {
        select(mode);
        int mark = engine.getServerLogs().length();
        failure(run(config, defaults()), mark, message);
        assertEquals(observed, calls().size());
        assertRows(Collections.emptyList());
    }

    private void failure(Container.ExecResult result, int mark, String message) {
        assertNotEquals(0, result.getExitCode());
        String logs =
                engine.getServerLogs().substring(mark) + result.getStdout() + result.getStderr();
        assertTrue(logs.contains(message), "Expected a sanitized failure containing " + message);
        privateLogs(logs);
    }

    private void privateLogs(String logs) {
        String encoded =
                Base64.getEncoder()
                        .encodeToString(
                                (client + ":" + secret).getBytes(StandardCharsets.US_ASCII));
        assertFalse(
                logs.contains(username)
                        || logs.contains(password)
                        || logs.contains(secret)
                        || logs.contains("akeneo-private-access-")
                        || logs.contains("akeneo-private-refresh-")
                        || logs.contains(encoded)
                        || logs.contains(PRIVATE)
                        || logs.contains(CURSOR),
                "Provider data or credentials leaked");
    }

    private static void success(Container.ExecResult result) {
        assertEquals(0, result.getExitCode());
    }

    private Container.ExecResult run(String config, List<String> args) throws Exception {
        return engine.executeJob(config, nextJob(), args.toArray(new String[0]));
    }

    private static String nextJob() {
        return Long.toString(IDS.incrementAndGet());
    }

    private CompletableFuture<Container.ExecResult> submit(
            String config, String job, List<String> args) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return engine.executeJob(config, job, args.toArray(new String[0]));
                    } catch (Exception error) {
                        throw new IllegalStateException("Fixture engine command failed", error);
                    }
                });
    }

    private void awaitHold(int index) {
        await().atMost(45, TimeUnit.SECONDS)
                .until(() -> calls().path(index).path("held").asBoolean());
    }

    private void awaitClose(int index, long limit) {
        await().atMost(15, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            long millis = calls().path(index).path("closed_ms").asLong(-1);
                            assertTrue(
                                    millis >= 0 && millis <= limit,
                                    "Provider did not observe the connection close");
                        });
    }

    private void awaitCheckpoint(String job) {
        long previous = engine.getCompletedCheckpointCount(job);
        await().atMost(30, TimeUnit.SECONDS)
                .until(() -> engine.getCompletedCheckpointCount(job) > previous);
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
        try (Connection sql = sql();
                Statement statement = sql.createStatement()) {
            statement.execute("TRUNCATE TABLE akeneo_rows");
        }
    }

    private List<String> paths() throws Exception {
        List<String> paths = new ArrayList<>();
        for (JsonNode call : calls()) {
            paths.add(call.path("path").asText());
        }
        return paths;
    }

    private List<String> valuesFor(String path, String parameter) throws Exception {
        List<String> found = new ArrayList<>();
        for (JsonNode observed : calls()) {
            if (path.equals(observed.path("path").asText())) {
                found.add(param(observed, parameter));
            }
        }
        return found;
    }

    private static String param(JsonNode observed, String parameter) {
        return observed.path("query").path(parameter).path(0).asText("");
    }

    private JsonNode calls() throws Exception {
        return control("/wire");
    }

    private JsonNode control(String path) throws Exception {
        return get(new URL("http", fixture.getHost(), fixture.getMappedPort(8080), path));
    }

    private static JsonNode get(URL address) throws Exception {
        HttpURLConnection connection = (HttpURLConnection) address.openConnection();
        connection.setConnectTimeout(5000);
        connection.setReadTimeout(5000);
        try (InputStream data = connection.getInputStream()) {
            return JSON.readTree(data);
        } finally {
            connection.disconnect();
        }
    }

    private Connection sql() throws Exception {
        return DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }

    private List<String> rows() throws Exception {
        List<String> data = new ArrayList<>();
        try (Connection sql = sql();
                Statement statement = sql.createStatement();
                ResultSet result =
                        statement.executeQuery(
                                "SELECT resource, id, parent_id, record FROM akeneo_rows")) {
            while (result.next()) {
                data.add(
                        result.getString(1)
                                + "|"
                                + result.getString(2)
                                + "|"
                                + result.getString(3)
                                + "|"
                                + result.getString(4));
            }
        }
        Collections.sort(data);
        return data;
    }

    private void assertRows(List<String> expected) throws Exception {
        List<String> ordered = new ArrayList<>(expected);
        Collections.sort(ordered);
        assertEquals(ordered, rows());
    }

    private List<String> kinds() throws Exception {
        List<String> result = new ArrayList<>();
        for (JsonNode entry : calls()) {
            result.add(entry.path("kind").asText());
        }
        return result;
    }

    private static List<String> products(int first, int end) {
        return IntStream.range(first, end)
                .mapToObj(AkeneoSourceIT::product)
                .collect(Collectors.toList());
    }

    private static String product(int number) {
        String uuid = String.format(Locale.ROOT, "7dc8455f-c25d-4114-a018-%012d", number);
        return "products|"
                + uuid
                + "|null|{\"uuid\":\""
                + uuid
                + "\",\"identifier\":"
                + (number == 1 ? "null" : "\"sku-" + number + "\"")
                + ",\"values\":{\"title\":[{\"locale\":\"fr_FR\",\"scope\":\"ecommerce\",\"data\":\"café 雪\"},"
                + "{\"locale\":\"en_US\",\"scope\":null,\"data\":null}]},"
                + "\"version\":1234567890123456789012345678901234567890,"
                + "\"amount\":9876543210987654321.123456789012345678900}";
    }

    private static String row(String resource, String id) {
        return row(resource, id, null, "");
    }

    private static String variant(String id, String family) {
        return row("family_variants", id, family, "");
    }

    private static String row(String resource, String id, String family, String extra) {
        return resource
                + "|"
                + id
                + "|"
                + family
                + "|{\"code\":\""
                + id
                + "\",\"labels\":{\"fr_FR\":\"café 雪\",\"en_US\":null}"
                + extra
                + "}";
    }

    private static class Engine extends SeaTunnelContainer {
        boolean completedPipeline(String job) throws Exception {
            URL rest =
                    new URL(
                            "http",
                            server.getHost(),
                            server.getMappedPort(8080),
                            "/jobs/checkpoints/" + job);
            for (JsonNode item : get(rest).path("pipelines")) {
                if ("completed-point"
                        .equals(item.path("latestCompleted").path("checkpointType").asText())) {
                    return true;
                }
            }
            return false;
        }
    }
}
