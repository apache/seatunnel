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
package org.apache.seatunnel.e2e.connector.appstoreconnect;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.testcontainers.Testcontainers;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.Signature;
import java.security.cert.Certificate;
import java.security.spec.ECGenParameterSpec;
import java.time.Instant;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

/**
 * Independent Apple-compatible HTTPS fixture for the packaged connector's Zeta tests.
 *
 * <p>Verifies the request contract and ES256 signature and serves deterministic JSON:API documents
 * and real gzip TSV files through separate API and signed-download origins.
 */
final class AppStoreConnectFixture implements AutoCloseable {
    static final String HOST = "host.testcontainers.internal";
    static final String STORE_PASSWORD = "fixture-pass";
    static final String ISSUER = "fixture-issuer-314";
    static final String KEY_ID = "FIXTUREAPPLE";
    static final String VENDOR = "000721";
    static final String PRIVATE_TEXT = "fixture-private-provider-response";
    static final String SIGNED = "fixture-signed-download-capability";
    static final String INSTALL = "App Store Installation and Deletion Standard";
    static final String DOWNLOAD = "App Downloads Standard";
    static final String BEARER = "Bearer ";
    static final String CURSOR = "opaque+=%/next";
    static final String CURSOR_QUERY = "opaque%2B%3D%25%2Fnext";
    static final ObjectMapper JSON = new ObjectMapper();

    final LocalDate today = LocalDate.now(ZoneOffset.UTC);
    final LocalDate daily = today.minusDays(1);
    final LocalDate eventDay = today.minusDays(2);
    final LocalDate historical = today.minusDays(90);
    final YearMonth fiscal = YearMonth.from(today.minusDays(35));
    final Instant reviewFrom = today.minusDays(10).atStartOfDay().toInstant(ZoneOffset.UTC);
    final Path trust;
    final String secret;
    private final KeyPair key;
    private final ExecutorService workers = Executors.newFixedThreadPool(8);
    private final HttpsServer api;
    private final HttpsServer download;
    private final HttpsServer wrongTls;
    private volatile Scenario scenario = new Scenario("matrix");

    AppStoreConnectFixture(Path path) throws Exception {
        trust = path.resolve("appstore-trust.jks");
        KeyPairGenerator signer = KeyPairGenerator.getInstance("EC");
        signer.initialize(new ECGenParameterSpec("secp256r1"));
        key = signer.generateKeyPair();
        secret = Base64.getEncoder().encodeToString(key.getPrivate().getEncoded());
        SSLContext tls = tls(path, HOST, "appstore-test");
        api = server(tls, false);
        download = server(tls, true);
        wrongTls = server(tls(path, "wrong.test.invalid", "appstore-wrong"), false);
        Testcontainers.exposeHostPorts(
                api.getAddress().getPort(),
                download.getAddress().getPort(),
                wrongTls.getAddress().getPort());
    }

    String apiOrigin() {
        return origin(api);
    }

    String wrongOrigin() {
        return origin(wrongTls);
    }

    Scenario select(String mode) {
        Scenario ready = new Scenario(mode);
        scenario = ready;
        return ready;
    }

    private static String origin(HttpsServer server) {
        return "https://" + HOST + ":" + server.getAddress().getPort();
    }

    /**
     * Uses a trusted certificate for the normal host and a second one for mismatch checks.
     *
     * <p>Both certificates are in the trust store so the mismatch case isolates hostname
     * validation.
     */
    private SSLContext tls(Path work, String hostname, String name) throws Exception {
        Path file = work.resolve(name + ".p12");
        Process keytool =
                new ProcessBuilder(
                                Paths.get(System.getProperty("java.home"), "bin", "keytool")
                                        .toString(),
                                "-genkeypair",
                                "-noprompt",
                                "-alias",
                                name,
                                "-keyalg",
                                "RSA",
                                "-keysize",
                                "2048",
                                "-dname",
                                "CN=" + hostname,
                                "-ext",
                                "SAN=dns:" + hostname,
                                "-validity",
                                "2",
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                file.toString(),
                                "-storepass",
                                STORE_PASSWORD)
                        .redirectErrorStream(true)
                        .redirectOutput(work.resolve(name + "-keytool.log").toFile())
                        .start();
        if (!keytool.waitFor(30, TimeUnit.SECONDS) || keytool.exitValue() != 0) {
            keytool.destroyForcibly();
            throw new IllegalStateException("Fixture certificate generation failed");
        }
        char[] password = STORE_PASSWORD.toCharArray();
        KeyStore server = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(file)) {
            server.load(input, password);
        }
        Certificate certificate = server.getCertificate(name);
        KeyStore anchors = KeyStore.getInstance("JKS");
        if (Files.exists(trust)) {
            try (InputStream input = Files.newInputStream(trust)) {
                anchors.load(input, password);
            }
        } else {
            anchors.load(null, password);
        }
        anchors.setCertificateEntry(name, certificate);
        try (OutputStream output = Files.newOutputStream(trust)) {
            anchors.store(output, password);
        }
        KeyManagerFactory managers =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        managers.init(server, password);
        SSLContext tls = SSLContext.getInstance("TLS");
        tls.init(managers.getKeyManagers(), null, null);
        return tls;
    }

    private HttpsServer server(SSLContext context, boolean signed) throws IOException {
        HttpsServer server = HttpsServer.create(new InetSocketAddress(0), 16);
        server.setHttpsConfigurator(new HttpsConfigurator(context));
        server.setExecutor(workers);
        server.createContext(
                "/",
                exchange -> {
                    Scenario active = scenario;
                    Event event = new Event(exchange, signed);
                    active.calls.add(event);
                    try {
                        if (!"GET".equals(exchange.getRequestMethod())) {
                            throw new IllegalArgumentException("expected GET");
                        }
                        if (signed) {
                            serveDownload(active, event, exchange);
                        } else {
                            verifyJwt(active, exchange);
                            serveApi(active, event, exchange);
                        }
                    } catch (IOException disconnected) {
                        event.closed = true;
                    } catch (Exception invalid) {
                        active.invalid.add(event.path + ": " + invalid.getClass().getSimpleName());
                        error(exchange, 400, "FIXTURE_REJECTED", PRIVATE_TEXT);
                    } finally {
                        exchange.close();
                    }
                });
        server.start();
        return server;
    }

    /**
     * Verifies signing using only the fixture public key and the JOSE wire form.
     *
     * <p>The accepted signature also binds the declared audience, issuer, key ID, and validity
     * interval.
     */
    private void verifyJwt(Scenario active, HttpExchange exchange) throws Exception {
        List<String> auth = exchange.getRequestHeaders().get("Authorization");
        if (auth == null || auth.size() != 1 || !auth.get(0).startsWith(BEARER)) {
            throw new IllegalArgumentException("no bearer");
        }
        String token = auth.get(0).substring(BEARER.length());
        String[] pieces = token.split("\\.", -1);
        if (pieces.length != 3 || token.contains("=")) {
            throw new IllegalArgumentException("bad JWT envelope");
        }
        Base64.Decoder url = Base64.getUrlDecoder();
        JsonNode header = JSON.readTree(url.decode(pieces[0]));
        JsonNode claims = JSON.readTree(url.decode(pieces[1]));
        long now = Instant.now().getEpochSecond();
        long iat = claims.path("iat").asLong();
        if (!"ES256".equals(header.path("alg").asText())
                || !KEY_ID.equals(header.path("kid").asText())
                || !"JWT".equals(header.path("typ").asText())
                || !ISSUER.equals(claims.path("iss").asText())
                || !"appstoreconnect-v1".equals(claims.path("aud").asText())
                || !claims.path("iat").isIntegralNumber()
                || !claims.path("exp").isIntegralNumber()
                || iat < now - 900
                || iat > now + 30
                || claims.path("exp").asLong() - iat != 900) {
            throw new IllegalArgumentException("bad JWT claims");
        }
        byte[] signature = url.decode(pieces[2]);
        Signature verifier = Signature.getInstance("SHA256withECDSAinP1363Format");
        verifier.initVerify(key.getPublic());
        verifier.update((pieces[0] + "." + pieces[1]).getBytes(StandardCharsets.US_ASCII));
        if (signature.length != 64 || !verifier.verify(signature)) {
            throw new IllegalArgumentException("bad JWT signature");
        }
        if (!active.tokens.contains(token)) {
            active.tokens.add(token);
        }
    }

    private void serveApi(Scenario active, Event event, HttpExchange ex) throws Exception {
        String path = event.path;
        if (path.equals("/v1/salesReports") || path.equals("/v1/financeReports")) {
            serveDated(active, event, ex);
            return;
        }
        expect("application/json", ex.getRequestHeaders().getFirst("Accept"));
        if ("unauthorized".equals(active.mode)
                || "resign".equals(active.mode) && active.calls.size() == 1) {
            error(ex, 401, "NOT_AUTHORIZED", PRIVATE_TEXT);
            return;
        }
        if ("retry-budget".equals(active.mode)
                || "retry".equals(active.mode) && active.calls.size() == 1) {
            ex.getResponseHeaders()
                    .set("Retry-After", "retry-budget".equals(active.mode) ? "60" : "0");
            error(ex, 429, "RATE_LIMIT_EXCEEDED", PRIVATE_TEXT);
            return;
        }
        if ("forbidden".equals(active.mode)) {
            error(ex, 403, "FORBIDDEN", PRIVATE_TEXT);
            return;
        }
        if ("redirect".equals(active.mode)) {
            ex.getResponseHeaders().set("Location", origin(download) + "/never");
            send(ex, 302, new byte[0], "text/plain");
            return;
        }
        if (path.equals("/v1/apps")) {
            require(event, Set.of("limit"), null, null);
            apps(active, event, ex);
        } else if (path.matches("/v1/apps/app-[ab]/customerReviews")) {
            require(event, Set.of("limit", "sort"), "sort", "-createdDate");
            List<ObjectNode> rows =
                    path.contains("app-a")
                            ? List.of(
                                    review("review-now", reviewFrom.plusSeconds(1)),
                                    review("review-old", reviewFrom.minusSeconds(1)))
                            : List.of(
                                    review("review-boundary", reviewFrom),
                                    review(
                                            "review-future",
                                            today.plusDays(1)
                                                    .atStartOfDay()
                                                    .toInstant(ZoneOffset.UTC)));
            pagedRelation(event, ex, rows.get(1), rows.get(0));
        } else if (path.matches("/v1/apps/app-[ab]/analyticsReportRequests")) {
            String access = event.query.get("filter[accessType]");
            require(event, Set.of("limit", "filter[accessType]"), "filter[accessType]", access);
            if (!Set.of("ONGOING", "ONE_TIME_SNAPSHOT").contains(access)) {
                throw new IllegalArgumentException("unknown access type");
            }
            String request = "req-" + ("ONGOING".equals(access) ? "o" : "h");
            pagedRelation(
                    event, ex, path.contains("app-a") ? request(request, access) : null, null);
        } else if (path.matches("/v1/analyticsReportRequests/req-[oh]/reports")) {
            String name = event.query.get("filter[name]");
            require(event, Set.of("limit", "filter[name]"), "filter[name]", name);
            if (!Set.of(INSTALL, DOWNLOAD).contains(name)) {
                throw new IllegalArgumentException("unknown report name");
            }
            String report =
                    path.split("/")[3].substring(4)
                            + ("App Downloads Standard".equals(name) ? "-download" : "-install");
            pagedRelation(event, ex, report("report-" + report, name), null);
        } else if (path.matches("/v1/analyticsReports/report-[oh]-(install|download)/instances")) {
            require(event, Set.of("limit", "filter[granularity]"), "filter[granularity]", "DAILY");
            String report = path.split("/")[3].substring(7);
            if ("hold-child".equals(active.mode) && !active.release) {
                hold(active, event, ex);
                return;
            }
            pagedRelation(
                    event,
                    ex,
                    instance("ins-" + report, daily),
                    instance("old-" + report, historical));
        } else if (path.matches(
                "/v1/analyticsReportInstances/(ins|old)-[oh]-(install|download)/segments")) {
            require(event, Set.of("limit"), null, null);
            String id = path.split("/")[3];
            if (id.startsWith("old-")) {
                page(ex, List.of(), null);
            } else {
                String segment = "seg-" + id.substring(4);
                if ("multi-segment".equals(active.mode)) {
                    pagedRelation(
                            event,
                            ex,
                            segment(active, segment + "-second", true, false),
                            segment(active, segment, true, false));
                } else {
                    pagedRelation(
                            event,
                            ex,
                            segment(active, segment, true, false),
                            segment(active, "empty-" + segment, false, false));
                }
            }
        } else if (path.matches(
                "/v1/analyticsReportSegments/seg-[oh]-(install|download)(-second)?")) {
            if (!event.query.isEmpty()) {
                throw new IllegalArgumentException("segment detail has a query");
            }
            String id = path.substring(path.lastIndexOf('/') + 1);
            active.details.incrementAndGet();
            if ("hold-detail".equals(active.mode) && !active.release) {
                hold(active, event, ex);
                return;
            }
            ObjectNode supplied = segment(active, id, true, true);
            if ("other-segment".equals(active.mode)) {
                supplied.put("id", "seg-h-install");
            }
            object(ex, JSON.createObjectNode().set("data", supplied));
        } else {
            throw new IllegalArgumentException("unexpected API resource");
        }
    }

    private void apps(Scenario active, Event event, HttpExchange ex) throws Exception {
        boolean second = event.query.containsKey("cursor");
        if ("hold-first".equals(active.mode) && !active.release
                || "hold-page".equals(active.mode) && second && !active.release
                || "stall".equals(active.mode)
                || "trickle".equals(active.mode)) {
            hold(active, event, ex);
        } else if ("bad-json".equals(active.mode)) {
            send(
                    ex,
                    200,
                    "{\"data\":[] ,\"data\":[]}".getBytes(StandardCharsets.UTF_8),
                    "application/json");
        } else if ("bad-error-shape".equals(active.mode)) {
            object(
                    ex,
                    JSON.createObjectNode()
                            .put("errors", PRIVATE_TEXT)
                            .set("data", JSON.createArrayNode()));
        } else if (second) {
            page(
                    ex,
                    List.of(app("app-b")),
                    "cycle".equals(active.mode) ? "?cursor=" + CURSOR_QUERY : null);
        } else {
            String link = "?cursor=" + CURSOR_QUERY + "&limit=2";
            if ("cross-origin".equals(active.mode)) {
                link = origin(download) + "/v1/apps?cursor=" + CURSOR_QUERY;
            } else if ("changed-filter".equals(active.mode)) {
                link = "?cursor=" + CURSOR_QUERY + "&limit=8&filter%5Bname%5D=other";
            }
            page(ex, "empty-page".equals(active.mode) ? List.of() : List.of(app("app-a")), link);
        }
    }

    private void pagedRelation(Event event, HttpExchange ex, ObjectNode retained, ObjectNode first)
            throws IOException {
        if (retained == null) {
            page(ex, List.of(), null);
        } else if (event.query.containsKey("cursor")) {
            page(ex, List.of(retained), null);
        } else {
            page(
                    ex,
                    first == null ? List.of() : List.of(first),
                    apiOrigin() + event.path + "?cursor=" + CURSOR_QUERY + "&limit=2");
        }
    }

    private void serveDated(Scenario active, Event event, HttpExchange ex) throws Exception {
        expect("application/a-gzip", ex.getRequestHeaders().getFirst("Accept"));
        String type = event.query.get("filter[reportType]");
        String date = event.query.get("filter[reportDate]");
        expect(VENDOR, event.query.get("filter[vendorNumber]"));
        if (event.path.equals("/v1/financeReports")) {
            expect(
                    Set.of(
                            "filter[vendorNumber]",
                            "filter[regionCode]",
                            "filter[reportType]",
                            "filter[reportDate]"),
                    event.query.keySet());
            expect("ZZ", event.query.get("filter[regionCode]"));
            expect("FINANCIAL", type);
            if (YearMonth.parse(date).isAfter(fiscal)) {
                throw new IllegalArgumentException("future fiscal month");
            }
        } else {
            expect(
                    Set.of(
                            "filter[vendorNumber]",
                            "filter[frequency]",
                            "filter[reportType]",
                            "filter[reportDate]",
                            "filter[reportSubType]",
                            "filter[version]"),
                    event.query.keySet());
            expect("DAILY", event.query.get("filter[frequency]"));
            expect(
                    "SUBSCRIBER".equals(type) ? "DETAILED" : "SUMMARY",
                    event.query.get("filter[reportSubType]"));
            expect("SALES".equals(type) ? "1_0" : "1_3", event.query.get("filter[version]"));
            if (!Set.of("SALES", "SUBSCRIBER", "SUBSCRIPTION", "SUBSCRIPTION_EVENT").contains(type)
                    || LocalDate.parse(date)
                            .isAfter("SUBSCRIPTION_EVENT".equals(type) ? eventDay : daily)) {
                throw new IllegalArgumentException("unexpected daily report tuple");
            }
        }
        if ("no-report".equals(active.mode)) {
            error(ex, 404, "NOT_FOUND", "There were no sales for the date specified.");
            return;
        }
        if ("unknown-report-404".equals(active.mode)) {
            error(ex, 404, "NOT_FOUND", PRIVATE_TEXT);
            return;
        }
        if ("bad-vendor".equals(active.mode)) {
            error(ex, 400, "PARAMETER_ERROR.INVALID", PRIVATE_TEXT);
            return;
        }
        if ("gone-report".equals(active.mode)) {
            error(ex, 410, "GONE", "The report expired.");
            return;
        }
        if ("hold-dated".equals(active.mode) && !active.release) {
            hold(active, event, ex);
            return;
        }
        byte[] data;
        switch (active.mode) {
            case "bad-gzip":
                data = new byte[] {0x1f, (byte) 0x8b, 1};
                break;
            case "bad-tsv":
                data = gzip("SKU\tAmount\nvalid\t1\n\"bad\t1");
                break;
            case "huge-row":
                data = gzip("SKU\tAmount\n" + "Z".repeat(4096) + "\t1\n");
                break;
            case "oversized":
                data = gzip("SKU\tAmount\n" + "abc\t1000\n".repeat(3000));
                break;
            case "large-wire":
                byte[] noise = new byte[2048];
                new Random(23).nextBytes(noise);
                data = gzip("SKU\tAmount\n" + Base64.getEncoder().encodeToString(noise) + "\t1\n");
                break;
            case "bad-utf8":
                data = gzip(new byte[] {'S', 'K', 'U', '\t', 'N', '\n', (byte) 0xc3, 0x05});
                break;
            default:
                data = reportBytes(type, date, active);
                if ("double-gzip".equals(active.mode)) {
                    ex.getResponseHeaders().set("Content-Encoding", "gzip");
                    data = gzip(data);
                }
        }
        send(ex, 200, data, "application/a-gzip");
    }

    private void serveDownload(Scenario active, Event event, HttpExchange ex) throws Exception {
        if (ex.getRequestHeaders().containsKey("Authorization")
                || !event.path.matches("/download/seg-[oh]-(install|download)(-second)?")
                || !signature(active).equals(event.query.get("signature"))
                || event.query.size() != 1) {
            throw new IllegalArgumentException("signed download contract");
        }
        active.downloads.incrementAndGet();
        if ("hold-download".equals(active.mode) && !active.release) {
            hold(active, event, ex);
            return;
        }
        if ("expired".equals(active.mode) && active.downloads.get() == 1) {
            error(ex, 403, "ExpiredToken", PRIVATE_TEXT);
            return;
        }
        if ("download-redirect".equals(active.mode)) {
            ex.getResponseHeaders().set("Location", apiOrigin() + "/v1/apps");
            send(ex, 302, new byte[0], "text/plain");
            return;
        }
        String id = event.path.substring(event.path.lastIndexOf('/') + 1);
        byte[] data = reportBytes(id, daily.toString(), active);
        if ("checksum".equals(active.mode)) {
            data[data.length - 1] ^= 1;
        }
        send(ex, 200, data, "application/octet-stream");
    }

    /**
     * The same realistic provider payload is returned across unauthenticated URL renewal.
     *
     * <p>The test can change report content deliberately when exercising resume conflict.
     */
    static byte[] reportBytes(String id, String date, Scenario active) throws IOException {
        boolean analytics = id.startsWith("seg-");
        StringBuilder rows = new StringBuilder("\uFEFF");
        rows.append(analytics ? "Date\t" : "");
        rows.append("SKU\tAmount\tDescription\tEmpty\r\n");
        int count = "mid-report".equals(active.mode) ? 64 : 1;
        for (int n = 1; n <= count; n++) {
            if (analytics) {
                rows.append(date).append('\t');
            }
            rows.append("000")
                    .append(n)
                    .append('\t')
                    .append(active.changed ? "10.0000" : "9007199254740993.0100")
                    .append('\t')
                    .append('"')
                    .append(id)
                    .append(" Café 東京\tline\n\"\"yes\"\"")
                    .append('"')
                    .append("\t\r\n");
        }
        return gzip(rows.toString());
    }

    private static byte[] gzip(String text) throws IOException {
        return gzip(text.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] gzip(byte[] content) throws IOException {
        ByteArrayOutputStream encoded = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(encoded)) {
            gzip.write(content);
        }
        return encoded.toByteArray();
    }

    static ObjectNode app(String id) {
        ObjectNode attr =
                JSON.createObjectNode()
                        .put("sku", "000" + id.substring(4))
                        .put("name", "应用 Café 🎵")
                        .put("large", new BigInteger("99999999999999999999999991"))
                        .put("precise", new BigDecimal("0.010200003000"));
        attr.putNull("primaryLocale");
        return record("apps", id, attr);
    }

    static ObjectNode review(String id, Instant created) {
        return record(
                "customerReviews",
                id,
                JSON.createObjectNode()
                        .put("body", "评论\nCafé")
                        .put("createdDate", created.toString())
                        .put("rating", 5)
                        .putNull("reviewerNickname"));
    }

    static ObjectNode request(String id, String access) {
        return record(
                "analyticsReportRequests",
                id,
                JSON.createObjectNode()
                        .put("accessType", access)
                        .put("stoppedDueToInactivity", false));
    }

    static ObjectNode report(String id, String name) {
        return record(
                "analyticsReports",
                id,
                JSON.createObjectNode().put("name", name).put("category", "APP_STORE_ENGAGEMENT"));
    }

    static ObjectNode instance(String id, LocalDate date) {
        return record(
                "analyticsReportInstances",
                id,
                JSON.createObjectNode()
                        .put("granularity", "DAILY")
                        .put("processingDate", date.toString()));
    }

    ObjectNode segment(Scenario active, String id, boolean nonempty, boolean locator)
            throws Exception {
        byte[] encoded = reportBytes(id, daily.toString(), active);
        byte[] digest = MessageDigest.getInstance("MD5").digest(encoded);
        StringBuilder checksum = new StringBuilder();
        for (byte b : digest) {
            checksum.append(String.format("%02x", b & 255));
        }
        ObjectNode attr =
                JSON.createObjectNode()
                        .put("checksum", checksum.toString())
                        .put("sizeInBytes", nonempty ? encoded.length : 0);
        if (locator) {
            String host =
                    "disallowed-host".equals(active.mode)
                            ? "https://unsafe.example.invalid"
                            : origin(download);
            attr.put("url", host + "/download/" + id + "?signature=" + signature(active));
        }
        return record("analyticsReportSegments", id, attr);
    }

    private static String signature(Scenario active) {
        return "expired".equals(active.mode) && active.downloads.get() > 0
                ? SIGNED + "-renewed"
                : SIGNED;
    }

    private static ObjectNode record(String type, String id, ObjectNode attr) {
        ObjectNode resource = JSON.createObjectNode().put("type", type).put("id", id);
        return resource.set("attributes", attr);
    }

    private static void require(Event event, Set<String> allowed, String option, String value) {
        Map<String, String> actual = new LinkedHashMap<>(event.query);
        String cursor = actual.remove("cursor");
        if (cursor != null) {
            expect(CURSOR, cursor);
        }
        expect(allowed, actual.keySet());
        expect("2", actual.get("limit"));
        if (option != null) {
            expect(value, actual.get(option));
        }
    }

    private static void expect(Object expected, Object actual) {
        if (expected == null || !expected.equals(actual)) {
            throw new IllegalArgumentException("request did not match the fixture contract");
        }
    }

    private static void page(HttpExchange ex, List<ObjectNode> records, String next)
            throws IOException {
        ObjectNode doc = JSON.createObjectNode();
        ArrayNode data = doc.putArray("data");
        records.forEach(data::add);
        ObjectNode links = doc.putObject("links");
        if (next == null) {
            links.putNull("next");
        } else {
            links.put("next", next);
        }
        object(ex, doc);
    }

    private static void object(HttpExchange ex, JsonNode value) throws IOException {
        send(ex, 200, JSON.writeValueAsBytes(value), "application/json");
    }

    private static void error(HttpExchange ex, int status, String code, String detail)
            throws IOException {
        ObjectNode value = JSON.createObjectNode();
        value.putArray("errors")
                .addObject()
                .put("status", String.valueOf(status))
                .put("code", code)
                .put("detail", detail);
        send(ex, status, JSON.writeValueAsBytes(value), "application/json");
    }

    private static void send(HttpExchange ex, int status, byte[] data, String media)
            throws IOException {
        ex.getResponseHeaders().set("Content-Type", media);
        ex.sendResponseHeaders(status, data.length == 0 ? -1 : data.length);
        if (data.length > 0) {
            try (OutputStream body = ex.getResponseBody()) {
                body.write(data);
            }
        }
    }

    /**
     * Holds a response open and detects the client socket close before releasing the fixture.
     *
     * <p>A trickle of decoded bytes prevents a socket-idle timeout from standing in for a total
     * deadline.
     */
    private static void hold(Scenario active, Event event, HttpExchange ex) throws IOException {
        event.held = true;
        ex.sendResponseHeaders(200, 0);
        try (OutputStream body = ex.getResponseBody()) {
            long bound = System.nanoTime() + TimeUnit.SECONDS.toNanos(80);
            while (!active.release && System.nanoTime() < bound) {
                if (!"stall".equals(active.mode)) {
                    body.write(' ');
                    body.flush();
                }
                try {
                    Thread.sleep(35);
                } catch (InterruptedException ex1) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        } catch (IOException disconnected) {
            event.closed = true;
        }
    }

    @Override
    public void close() {
        api.stop(0);
        download.stop(0);
        wrongTls.stop(0);
        workers.shutdownNow();
    }

    /**
     * Records the API path, decoded query and download boundary for an independent assertion.
     *
     * <p>Held events also record observation of peer cancellation.
     */
    static final class Event {
        final String path;
        final Map<String, String> query = new LinkedHashMap<>();
        final boolean signed;
        volatile boolean held;
        volatile boolean closed;

        Event(HttpExchange ex, boolean signed) {
            path = ex.getRequestURI().getPath();
            this.signed = signed;
            String raw = ex.getRequestURI().getRawQuery();
            if (raw != null) {
                for (String pair : raw.split("&")) {
                    String[] fields = pair.split("=", 2);
                    query.put(
                            URLDecoder.decode(fields[0], StandardCharsets.UTF_8),
                            URLDecoder.decode(fields[1], StandardCharsets.UTF_8));
                }
            }
        }
    }

    /**
     * Per-job fault mode with observation of physical attempts and validated tokens.
     *
     * <p>Mutable flags let the test release a canceled phase and independently change report bytes.
     */
    static final class Scenario {
        final String mode;
        final List<Event> calls = new CopyOnWriteArrayList<>();
        final List<String> invalid = new CopyOnWriteArrayList<>();
        final List<String> tokens = new CopyOnWriteArrayList<>();
        final AtomicInteger details = new AtomicInteger();
        final AtomicInteger downloads = new AtomicInteger();
        volatile boolean release;
        volatile boolean changed;

        Scenario(String mode) {
            this.mode = mode;
        }
    }
}
