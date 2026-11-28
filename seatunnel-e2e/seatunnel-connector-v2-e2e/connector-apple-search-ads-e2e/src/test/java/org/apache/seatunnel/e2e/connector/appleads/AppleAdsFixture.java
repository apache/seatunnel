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
package org.apache.seatunnel.e2e.connector.appleads;

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
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
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
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.zip.GZIPOutputStream;

/**
 * Independent HTTPS implementation of the Apple Ads v1 and v5 read contracts used by Zeta tests.
 *
 * <p>Checks the OAuth body, account namespace, request method, filters, date and page shapes before
 * returning provider-style JSON. The fixture does not import the connector code.
 */
final class AppleAdsFixture implements AutoCloseable {
    static final ObjectMapper JSON = new ObjectMapper();
    static final String HOST = "host.testcontainers.internal";
    static final String PASSWORD = "apple-ads-fixture";
    static final String CLIENT = "fixture-client+:%@";
    static final String SECRET = "fixture.es256+%/:signed";
    static final String ACCESS_PREFIX = "fixture-apple-ads-access-";
    static final String PRIVATE_DETAIL = "fixture-private-provider-diagnostic";
    static final String V1 = "PLATFORM_V1";
    static final String V5 = "LEGACY_V5";
    static final String ACCOUNT = "9007199254741001";
    static final String ORG = "8000191";
    static final List<String> DATASETS = List.of("campaigns", "adgroups", "keywords", "ads");
    static final List<Long> CAMPAIGN_IDS = List.of(101L, 102L, 103L);

    final LocalDate first = LocalDate.now(ZoneOffset.UTC).minusDays(14);
    final LocalDate last = first.plusDays(2);
    final Path trust;
    private final ExecutorService executor = Executors.newFixedThreadPool(8);
    private final HttpsServer api;
    private final HttpsServer issuer;
    private final HttpsServer wrongTls;
    private volatile Scenario current = new Scenario(V1, "matrix");

    AppleAdsFixture(Path path) throws Exception {
        trust = path.resolve("apple-ads-trust.jks");
        SSLContext tls = tls(path, HOST, "apple-ads-tls");
        api = server(tls, false);
        issuer = server(tls, true);
        wrongTls = server(tls(path, "other.test.invalid", "apple-ads-wrong"), false);
        Testcontainers.exposeHostPorts(
                api.getAddress().getPort(),
                issuer.getAddress().getPort(),
                wrongTls.getAddress().getPort());
    }

    Scenario select(String version, String mode) {
        Scenario next = new Scenario(version, mode);
        current = next;
        return next;
    }

    String api() {
        return origin(api);
    }

    String oauth() {
        return origin(issuer);
    }

    String wrong() {
        return origin(wrongTls);
    }

    private static String origin(HttpsServer server) {
        return "https://" + HOST + ":" + server.getAddress().getPort();
    }

    /**
     * Builds two trusted TLS identities so the failure case isolates hostname verification.
     *
     * <p>The trust store is mounted only in the test engine container.
     */
    private SSLContext tls(Path path, String host, String alias) throws Exception {
        Path key = path.resolve(alias + ".p12");
        Process keytool =
                new ProcessBuilder(
                                Paths.get(System.getProperty("java.home"), "bin", "keytool")
                                        .toString(),
                                "-genkeypair",
                                "-noprompt",
                                "-alias",
                                alias,
                                "-keyalg",
                                "RSA",
                                "-keysize",
                                "2048",
                                "-dname",
                                "CN=" + host,
                                "-ext",
                                "SAN=dns:" + host,
                                "-validity",
                                "2",
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                key.toString(),
                                "-storepass",
                                PASSWORD)
                        .redirectErrorStream(true)
                        .redirectOutput(path.resolve(alias + ".log").toFile())
                        .start();
        if (!keytool.waitFor(30, TimeUnit.SECONDS) || keytool.exitValue() != 0) {
            keytool.destroyForcibly();
            throw new IllegalStateException("Fixture certificate generation failed");
        }
        char[] password = PASSWORD.toCharArray();
        KeyStore keys = KeyStore.getInstance("PKCS12");
        try (InputStream file = Files.newInputStream(key)) {
            keys.load(file, password);
        }
        Certificate certificate = keys.getCertificate(alias);
        KeyStore pinned = KeyStore.getInstance("JKS");
        if (Files.exists(trust)) {
            try (InputStream file = Files.newInputStream(trust)) {
                pinned.load(file, password);
            }
        } else {
            pinned.load(null, password);
        }
        pinned.setCertificateEntry(alias, certificate);
        try (OutputStream file = Files.newOutputStream(trust)) {
            pinned.store(file, password);
        }
        KeyManagerFactory manager =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        manager.init(keys, password);
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(manager.getKeyManagers(), null, null);
        return context;
    }

    private HttpsServer server(SSLContext tls, boolean oauth) throws IOException {
        HttpsServer instance = HttpsServer.create(new InetSocketAddress(0), 16);
        instance.setHttpsConfigurator(new HttpsConfigurator(tls));
        instance.setExecutor(executor);
        instance.createContext(
                "/",
                exchange -> {
                    Scenario selected = current;
                    Event event = new Event(exchange, oauth);
                    selected.events.add(event);
                    try {
                        if (oauth) {
                            issue(selected, event, exchange);
                        } else {
                            query(selected, event, exchange);
                        }
                    } catch (IOException disconnected) {
                        event.closed = true;
                    } catch (RuntimeException invalid) {
                        selected.errors.add(event.path + ": " + invalid.getClass().getSimpleName());
                        fail(exchange, 400);
                    } finally {
                        exchange.close();
                    }
                });
        instance.start();
        return instance;
    }

    /**
     * Verifies form encoding and isolation of the token authority from scoped API requests.
     *
     * <p>Each issuance has a distinct synthetic bearer to test renewal and cache bounds.
     */
    private void issue(Scenario scenario, Event event, HttpExchange ex) throws IOException {
        expect("/auth/oauth2/token", event.path);
        expect("POST", event.method);
        expect(
                "application/x-www-form-urlencoded",
                ex.getRequestHeaders().getFirst("Content-Type"));
        expect(Map.of(), event.query);
        require(event.authorization == null && event.context == null);
        String encoded = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.US_ASCII);
        expect(
                Map.of(
                        "scope",
                        "searchadsorg",
                        "grant_type",
                        "client_credentials",
                        "client_id",
                        CLIENT,
                        "client_secret",
                        SECRET),
                form(encoded));
        int attempt = scenario.tokens.incrementAndGet();
        if ("hold-token".equals(scenario.mode) && !scenario.release) {
            hold(scenario, event, ex, true);
        } else if ("oauth-denied".equals(scenario.mode)) {
            fail(ex, 400);
        } else if ("oauth-redirect".equals(scenario.mode)) {
            ex.getResponseHeaders().set("Location", api() + "/must-not-receive-secret");
            send(ex, 302, new byte[0]);
        } else if ("oauth-drop".equals(scenario.mode)) {
            ex.close();
        } else if ("bad-token".equals(scenario.mode)) {
            json(
                    ex,
                    JSON.createObjectNode()
                            .put("access_token", PRIVATE_DETAIL)
                            .put("expires_in", 0)
                            .put("token_type", "Bearer"));
        } else {
            long expires = "ttl".equals(scenario.mode) && attempt == 1 ? 1 : 3600;
            json(
                    ex,
                    JSON.createObjectNode()
                            .put("access_token", ACCESS_PREFIX + attempt)
                            .put("expires_in", expires)
                            .put("token_type", "Bearer"));
        }
    }

    /**
     * Dispatches only the eight approved read routes after verifying scoped authentication.
     *
     * <p>The fixture independently derives the provider dataset and parent from the wire path and
     * filter objects, including numeric entity and string-array reporting filters.
     */
    private void query(Scenario scenario, Event event, HttpExchange ex) throws IOException {
        boolean platform = V1.equals(scenario.version);
        expect("Bearer " + ACCESS_PREFIX + scenario.tokens.get(), event.authorization);
        expect(platform ? "adAccountId=" + ACCOUNT : "orgId=" + ORG, event.context);
        expect("application/json", ex.getRequestHeaders().getFirst("Accept"));
        int request = scenario.calls.incrementAndGet();
        if ("resign".equals(scenario.mode) && request == 1
                || "unauthorized".equals(scenario.mode)) {
            fail(ex, 401);
            return;
        }
        if ("retry".equals(scenario.mode) && request == 1 || "retry-budget".equals(scenario.mode)) {
            ex.getResponseHeaders()
                    .set("Retry-After", "retry-budget".equals(scenario.mode) ? "60" : "0");
            fail(ex, 429);
            return;
        }
        if ("forbidden".equals(scenario.mode)) {
            fail(ex, 403);
            return;
        }
        if ("retention".equals(scenario.mode)) {
            fail(ex, 400);
            return;
        }
        if ("api-redirect".equals(scenario.mode)) {
            ex.getResponseHeaders().set("Location", oauth() + "/must-not-receive-bearer");
            send(ex, 307, new byte[0]);
            return;
        }
        if ("trickle".equals(scenario.mode) || "stall".equals(scenario.mode)) {
            hold(scenario, event, ex, "trickle".equals(scenario.mode));
            return;
        }
        if ("ttl".equals(scenario.mode) && request == 1) {
            sleep(1100);
        }
        Query parsed = platform ? platform(event, ex) : legacy(event, ex);
        event.parsed = parsed;
        if (holding(scenario, parsed)) {
            hold(scenario, event, ex, true);
            return;
        }
        if ("duplicate-json".equals(scenario.mode)) {
            send(ex, 200, "{\"data\":[] ,\"data\":[]}".getBytes(StandardCharsets.UTF_8));
            return;
        }
        if ("expanded-body".equals(scenario.mode) || "wire-body".equals(scenario.mode)) {
            String value = "x".repeat(27000);
            if ("wire-body".equals(scenario.mode)) {
                byte[] noise = new byte[3072];
                new Random(42).nextBytes(noise);
                value = Base64.getEncoder().encodeToString(noise);
            }
            ex.getResponseHeaders().set("Content-Encoding", "gzip");
            send(
                    ex,
                    200,
                    gzip(JSON.writeValueAsBytes(JSON.createObjectNode().put("payload", value))));
            return;
        }
        if ("api-errors".equals(scenario.mode)) {
            ObjectNode invalid = JSON.createObjectNode();
            invalid.putArray("errors").add(PRIVATE_DETAIL);
            json(ex, invalid);
            return;
        }
        ObjectNode payload = response(scenario, parsed);
        if ("gzip".equals(scenario.mode)) {
            ex.getResponseHeaders().set("Content-Encoding", "gzip");
            send(ex, 200, gzip(JSON.writeValueAsBytes(payload)));
        } else {
            json(ex, payload);
        }
    }

    private static boolean holding(Scenario scenario, Query q) {
        if (scenario.release) {
            return false;
        }
        switch (scenario.mode) {
            case "hold-parent":
                return !q.report && q.dataset.equals("campaigns");
            case "hold-page":
                return !q.report && q.dataset.equals("campaigns") && q.offset > 0;
            case "hold-report-page":
                return q.report && q.offset > 0;
            case "hold-window":
                return q.report && q.first.equals(q.last);
            case "hold-report-parent":
                return q.report && q.campaign == 102;
            default:
                return false;
        }
    }

    private static Query platform(Event event, HttpExchange ex) throws IOException {
        expect("POST", event.method);
        expect(Map.of(), event.query);
        expect("application/json", ex.getRequestHeaders().getFirst("Content-Type"));
        String[] path = event.path.split("/");
        require(path.length == 4 || path.length == 6);
        expect("v1", path[1]);
        boolean report = path.length == 6;
        if (report) {
            expect("reports", path[2]);
            expect("apps", path[3]);
        }
        String dataset = path[report ? 4 : 2];
        require(DATASETS.contains(dataset));
        expect("query", path[path.length - 1]);
        JsonNode body = JSON.readTree(ex.getRequestBody());
        JsonNode paging = body.path("pagination");
        int offset = integral(paging.path("offset"));
        int limit = integral(paging.path("pageSize"));
        JsonNode filters = body.path("filters");
        if (!report) {
            expect(true, paging.path("fetchTotalCount").asBoolean());
            expect(
                    JSON.createArrayNode()
                            .add(JSON.createObjectNode().put("field", "id").put("order", "ASC")),
                    body.path("sorting"));
            JsonNode only = filter(filters);
            switch (dataset) {
                case "campaigns":
                    expect("promotedObjectType", only.path("field").textValue());
                    expect("APPSTORE_APP", only.path("value").textValue());
                    return new Query(true, false, dataset, 0, 0, offset, limit, null, null, null);
                case "adgroups":
                    expect("campaignId", only.path("field").textValue());
                    return new Query(
                            true,
                            false,
                            dataset,
                            integral(only.path("value")),
                            0,
                            offset,
                            limit,
                            null,
                            null,
                            null);
                default:
                    expect("adGroupId", only.path("field").textValue());
                    long group = integral(only.path("value"));
                    return new Query(
                            true,
                            false,
                            dataset,
                            parent(group),
                            group,
                            offset,
                            limit,
                            null,
                            null,
                            null);
            }
        }
        long campaign = 0;
        if (!dataset.equals("campaigns")) {
            JsonNode only = filter(filters);
            expect("campaignId", only.path("field").textValue());
            JsonNode values = only.path("value");
            require(values.isArray() && values.size() == 1 && values.get(0).isTextual());
            campaign = Long.parseLong(values.get(0).textValue());
        } else {
            require(filters.isMissingNode());
        }
        expect(JSON.createArrayNode().add("countryOrRegion"), body.path("groupBy"));
        JsonNode range = body.path("timeRange");
        LocalDate first = LocalDate.parse(range.path("start").textValue());
        LocalDate last = LocalDate.parse(range.path("end").textValue());
        if (first.equals(last)) {
            require(!range.has("granularity"));
        } else {
            expect("DAILY", range.path("granularity").textValue());
        }
        String zone = range.path("timeZone").textValue();
        require(Set.of("UTC", "ORTZ").contains(zone));
        return new Query(true, true, dataset, campaign, 0, offset, limit, first, last, zone);
    }

    private static Query legacy(Event event, HttpExchange ex) throws IOException {
        String[] path = event.path.split("/");
        expect("api", path[1]);
        expect("v5", path[2]);
        if (path[3].equals("reports")) {
            expect("POST", event.method);
            expect(Map.of(), event.query);
            expect("application/json", ex.getRequestHeaders().getFirst("Content-Type"));
            expect("campaigns", path[4]);
            require(path.length == 5 || path.length == 7);
            long campaign = path.length == 5 ? 0 : Long.parseLong(path[5]);
            String dataset = path.length == 5 ? "campaigns" : path[6];
            require(DATASETS.contains(dataset));
            JsonNode body = JSON.readTree(ex.getRequestBody());
            expect("DAILY", body.path("granularity").textValue());
            expect(JSON.createArrayNode().add("countryOrRegion"), body.path("groupBy"));
            expect(false, body.path("returnRowTotals").booleanValue());
            expect(false, body.path("returnGrandTotals").booleanValue());
            JsonNode selector = body.path("selector");
            expect(
                    JSON.createArrayNode()
                            .add(
                                    JSON.createObjectNode()
                                            .put("field", "countryOrRegion")
                                            .put("sortOrder", "ASCENDING")),
                    selector.path("orderBy"));
            JsonNode paging = selector.path("pagination");
            return new Query(
                    false,
                    true,
                    dataset,
                    campaign,
                    0,
                    integral(paging.path("offset")),
                    integral(paging.path("limit")),
                    LocalDate.parse(body.path("startTime").textValue()),
                    LocalDate.parse(body.path("endTime").textValue()),
                    body.path("timeZone").textValue());
        }
        expect("GET", event.method);
        expect("campaigns", path[3]);
        expect(Set.of("offset", "limit"), event.query.keySet());
        long campaign = path.length == 4 ? 0 : Long.parseLong(path[4]);
        long group = path.length == 8 ? Long.parseLong(path[6]) : 0;
        require(path.length == 4 || path.length == 6 || path.length == 8);
        if (path.length > 4) {
            expect("adgroups", path[5]);
        }
        String dataset =
                path.length == 4
                        ? "campaigns"
                        : path.length == 6
                                ? "adgroups"
                                : path[7].equals("targetingkeywords") ? "keywords" : path[7];
        require(DATASETS.contains(dataset));
        return new Query(
                false,
                false,
                dataset,
                campaign,
                group,
                Integer.parseInt(event.query.get("offset")),
                Integer.parseInt(event.query.get("limit")),
                null,
                null,
                null);
    }

    private static JsonNode filter(JsonNode filters) {
        require(filters.isArray() && filters.size() == 1);
        JsonNode filter = filters.get(0);
        expect("EQUALS", filter.path("operator").textValue());
        return filter;
    }

    private static int integral(JsonNode number) {
        require(number.isIntegralNumber() && number.canConvertToInt());
        return number.intValue();
    }

    private static ObjectNode response(Scenario scenario, Query q) {
        List<Long> ids = ids(scenario, q);
        ObjectNode envelope = JSON.createObjectNode();
        ArrayNode rows = JSON.createArrayNode();
        if (q.report) {
            if (q.platform) {
                ObjectNode report = envelope.putObject("result");
                report.set("rows", rows);
                report.putObject("summary").putObject("grandTotal").put("impressions", 999999);
            } else {
                ObjectNode report = envelope.putObject("data").putObject("reportingDataResponse");
                report.set("row", rows);
                report.putObject("grandTotals").put("impressions", 999999);
                envelope.putNull("error");
            }
        } else {
            envelope.set(q.platform ? "result" : "data", rows);
        }
        ObjectNode paging = envelope.putObject("pagination");
        int total = ids.size();
        int offset = "repeat-offset".equals(scenario.mode) && q.offset > 0 ? 0 : q.offset;
        if ("changed-total".equals(scenario.mode) && q.offset > 0) {
            ++total;
            ids = List.of(ids.get(0), ids.get(1), ids.get(2), 104L);
        }
        if ("legacy-ad-list".equals(scenario.mode)
                && !q.platform
                && "ads".equals(q.dataset)
                && !q.report) {
            envelope.remove("pagination");
        } else {
            paging.put(q.platform ? "totalCount" : "totalResults", total)
                    .put(q.platform ? "offset" : "startIndex", offset)
                    .put(q.platform ? "pageSize" : "itemsPerPage", q.limit);
        }
        if ("short".equals(scenario.mode) || "report-short".equals(scenario.mode)) {
            return envelope;
        }
        int remaining = Math.min(q.offset + q.limit, ids.size());
        for (int i = q.offset; i < remaining; i++) {
            Long id = ids.get(i);
            rows.add(
                    q.report
                            ? report(scenario, q, id)
                            : entity(q.platform, q.dataset, id, q.campaign, q.group));
        }
        if ("scope".equals(scenario.mode) && !rows.isEmpty()) {
            ((ObjectNode) rows.get(0)).put(q.platform ? "adAccountId" : "orgId", 42);
        }
        return envelope;
    }

    /**
     * Provides independent page populations for account, entity parent and report parent reads.
     *
     * <p>The large modes cross a 1,000-entity page and a report page with many emitted daily facts.
     */
    static List<Long> ids(Scenario scenario, Query q) {
        if ("empty".equals(scenario.mode)) {
            return List.of();
        }
        if ("big-page".equals(scenario.mode)) {
            return LongStream.rangeClosed(1, 1001).boxed().collect(Collectors.toList());
        }
        if ("long-report".equals(scenario.mode)) {
            return LongStream.rangeClosed(1000, 1063).boxed().collect(Collectors.toList());
        }
        switch (q.dataset) {
            case "campaigns":
                return CAMPAIGN_IDS;
            case "adgroups":
                return groups(q.campaign);
            case "keywords":
                return q.report ? keywords(groups(q.campaign)) : keywords(List.of(q.group));
            default:
                List<Long> groups = q.report ? groups(q.campaign) : List.of(q.group);
                return groups.stream().map(group -> 2000 + group).collect(Collectors.toList());
        }
    }

    static List<Long> groups(long campaign) {
        if (campaign == 101) {
            return List.of(301L, 302L, 303L);
        }
        return campaign == 102 ? List.of(401L) : List.of();
    }

    private static List<Long> keywords(List<Long> groups) {
        List<Long> ids = new ArrayList<>();
        for (Long group : groups) {
            ids.add(1000 + group);
            if (group == 301L) {
                ids.add(3000 + group);
                ids.add(4000 + group);
            }
        }
        return ids;
    }

    static long parent(long group) {
        return group == 401L ? 102L : 101L;
    }

    static long reportGroup(String resource, long id) {
        switch (resource) {
            case "adgroups":
                return id;
            case "keywords":
                return id % 1000;
            case "ads":
                return id - 2000;
            default:
                return 0;
        }
    }

    static long account(boolean platform) {
        return Long.parseLong(platform ? ACCOUNT : ORG);
    }

    static ObjectNode entity(boolean platform, String dataset, long id, long campaign, long group) {
        ObjectNode data =
                JSON.createObjectNode()
                        .put("id", id)
                        .put(platform ? "adAccountId" : "orgId", account(platform))
                        .put("name", "Café 广告 " + id)
                        .put("deleted", false)
                        .put("variation", new BigDecimal("0.1200300004712000"));
        data.putNull("remark");
        if (!dataset.equals("campaigns")) {
            data.put("campaignId", campaign);
        }
        if (group > 0) {
            data.put("adGroupId", group);
        }
        return data;
    }

    private static ObjectNode report(Scenario scenario, Query q, long id) {
        long group = reportGroup(q.dataset, id);
        ObjectNode result = JSON.createObjectNode();
        result.set("metadata", metadata(q.platform, q.dataset, q.campaign, group, id));
        if (q.platform && q.first.equals(q.last)) {
            result.set(
                    "totalMetrics",
                    metrics(
                            scenario,
                            id,
                            null,
                            "ads".equals(q.dataset) && id == 2301 ? null : "US"));
        } else {
            ArrayNode metrics = result.putArray(q.platform ? "granularMetrics" : "granularity");
            for (LocalDate date = q.first; !date.isAfter(q.last); date = date.plusDays(1)) {
                LocalDate value = "bad-date".equals(scenario.mode) ? date.minusDays(7) : date;
                metrics.add(metrics(scenario, id, value, q.platform ? "US" : null));
                if (q.platform && id == 101 && !"long-report".equals(scenario.mode)) {
                    metrics.add(metrics(scenario, id, value, "GB"));
                }
            }
        }
        return result;
    }

    static ObjectNode metadata(
            boolean platform, String dataset, long campaign, long group, long id) {
        String idField;
        switch (dataset) {
            case "campaigns":
                idField = "campaignId";
                break;
            case "adgroups":
                idField = "adGroupId";
                break;
            case "keywords":
                idField = "keywordId";
                break;
            default:
                idField = "adId";
                break;
        }
        ObjectNode metadata =
                JSON.createObjectNode()
                        .put(platform ? "id" : idField, id)
                        .put(platform ? "adAccountId" : "orgId", account(platform))
                        .put("name", "行 " + id);
        if (campaign > 0) {
            metadata.put("campaignId", campaign);
        }
        if (group > 0) {
            metadata.put("adGroupId", group);
        }
        if (!platform) {
            metadata.put("countryOrRegion", "US");
        }
        return metadata;
    }

    static ObjectNode metrics(Scenario scenario, long id, LocalDate day, String country) {
        ObjectNode value = JSON.createObjectNode().put("impressions", 10000 + id);
        value.putObject("localSpend")
                .put("amount", scenario.changed ? "12.00" : "9007199254740993.0100")
                .put("currency", "USD");
        value.put("decimal", new BigDecimal("0.1234567890012345"));
        if (day != null) {
            value.put("date", day.toString());
        }
        if (country != null) {
            value.put("countryOrRegion", country);
        }
        return value;
    }

    private static void expect(Object expected, Object actual) {
        require(expected != null && expected.equals(actual));
    }

    private static void require(boolean condition) {
        if (!condition) {
            throw new IllegalArgumentException("Apple Ads fixture protocol mismatch");
        }
    }

    private static Map<String, String> form(String raw) {
        Map<String, String> out = new LinkedHashMap<>();
        if (raw != null && !raw.isEmpty()) {
            for (String item : raw.split("&")) {
                String[] parts = item.split("=", 2);
                String key = URLDecoder.decode(parts[0], StandardCharsets.UTF_8);
                require(parts.length == 2 && !out.containsKey(key));
                out.put(key, URLDecoder.decode(parts[1], StandardCharsets.UTF_8));
            }
        }
        return out;
    }

    private static byte[] gzip(byte[] data) throws IOException {
        ByteArrayOutputStream encoded = new ByteArrayOutputStream();
        try (GZIPOutputStream output = new GZIPOutputStream(encoded)) {
            output.write(data);
        }
        return encoded.toByteArray();
    }

    private static void json(HttpExchange ex, JsonNode body) throws IOException {
        send(ex, 200, JSON.writeValueAsBytes(body));
    }

    private static void fail(HttpExchange ex, int code) throws IOException {
        send(
                ex,
                code,
                JSON.writeValueAsBytes(JSON.createObjectNode().put("error", PRIVATE_DETAIL)));
    }

    private static void send(HttpExchange ex, int status, byte[] body) throws IOException {
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
        if (body.length > 0) {
            try (OutputStream output = ex.getResponseBody()) {
                output.write(body);
            }
        }
    }

    private static void hold(Scenario scenario, Event event, HttpExchange ex, boolean trickle)
            throws IOException {
        event.held = true;
        ex.sendResponseHeaders(200, 0);
        try (OutputStream output = ex.getResponseBody()) {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(80);
            while (!scenario.release
                    && System.nanoTime() < until
                    && !Thread.currentThread().isInterrupted()) {
                if (trickle) {
                    output.write(' ');
                    output.flush();
                }
                sleep(35);
            }
        } catch (IOException disconnected) {
            event.closed = true;
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        issuer.stop(0);
        api.stop(0);
        wrongTls.stop(0);
        executor.shutdownNow();
    }

    /**
     * Captures the independent provider's parsed read scope.
     *
     * <p>Raw response pages count entities and are separate from the row projection in tests.
     */
    static final class Query {
        final boolean platform;
        final boolean report;
        final String dataset;
        final long campaign;
        final long group;
        final int offset;
        final int limit;
        final LocalDate first;
        final LocalDate last;
        final String timezone;

        Query(
                boolean platform,
                boolean report,
                String dataset,
                long campaign,
                long group,
                int offset,
                int limit,
                LocalDate first,
                LocalDate last,
                String timezone) {
            this.platform = platform;
            this.report = report;
            this.dataset = dataset;
            this.campaign = campaign;
            this.group = group;
            this.offset = offset;
            this.limit = limit;
            this.first = first;
            this.last = last;
            this.timezone = timezone;
        }
    }

    /**
     * Records only incoming test requests; secrets are synthetic and remain fixture-local.
     *
     * <p>Held responses expose whether cancel/savepoint closed the peer connection.
     */
    static final class Event {
        final String path;
        final String method;
        final Map<String, String> query;
        final boolean oauth;
        final String authorization;
        final String context;
        volatile Query parsed;
        volatile boolean held;
        volatile boolean closed;

        Event(HttpExchange ex, boolean oauth) {
            path = ex.getRequestURI().getPath();
            method = ex.getRequestMethod();
            query = form(ex.getRequestURI().getRawQuery());
            this.oauth = oauth;
            authorization = ex.getRequestHeaders().getFirst("Authorization");
            context = ex.getRequestHeaders().getFirst("X-AP-Context");
        }
    }

    /**
     * Isolates one independently generated account/mode and its fault-injection records.
     *
     * <p>Savepoint cases may release a held response or substitute a changed metric snapshot.
     */
    static final class Scenario {
        final String version;
        final String mode;
        final List<Event> events = new CopyOnWriteArrayList<>();
        final List<String> errors = new CopyOnWriteArrayList<>();
        final AtomicInteger tokens = new AtomicInteger();
        final AtomicInteger calls = new AtomicInteger();
        volatile boolean release;
        volatile boolean changed;

        Scenario(String version, String mode) {
            this.version = version;
            this.mode = mode;
        }
    }
}
