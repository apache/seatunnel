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
package org.apache.seatunnel.e2e.connector.amazonads;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.testcontainers.Testcontainers;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

/**
 * Serves three independent HTTPS origins for OAuth, profile-scoped Ads and signed downloads. Its
 * request validation and synthetic response rows are independent of connector classes.
 */
final class AmazonAdsFixture implements AutoCloseable {
    static final String HOST = "host.testcontainers.internal";
    static final String PROFILE_PATH = "/v2/profiles";
    static final String REPORT_PATH = "/reporting/reports";
    static final String TOKEN_PATH = "/auth/o2/token";
    static final String CREATE_TYPE = "application/vnd.createasyncreportrequest.v3+json";
    static final String CREATE_REPLY = "application/vnd.createasyncreportresponse.v3+json";
    static final String STATUS_TYPE = "application/vnd.getasyncreportresponse.v3+json";
    static final String PRIVATE = "fixture-private-provider-body";
    static final String SIGNATURE = "private-report-capability";
    static final String CURSOR = "opaque+/=next";
    static final String STORE_PASSWORD = "fixture-trust";
    static final ObjectMapper JSON = new ObjectMapper();

    final String clientId = "client-fixture";
    final String clientSecret = UUID.randomUUID().toString();
    final String refresh = "Atzr|" + UUID.randomUUID();
    final LocalDate lower = LocalDate.now(ZoneOffset.UTC).minusDays(4);
    final LocalDate upper = lower.plusDays(2);
    final String basic =
            "Basic "
                    + Base64.getEncoder()
                            .encodeToString(
                                    (clientId + ":" + clientSecret)
                                            .getBytes(StandardCharsets.US_ASCII));
    final Path trust;
    private final ExecutorService executor = Executors.newFixedThreadPool(6);
    private final HttpsServer oauth;
    private final HttpsServer api;
    private final HttpsServer download;
    private final HttpsServer mismatchedTls;
    private volatile Scenario scenario = new Scenario("catalog");

    AmazonAdsFixture(Path temporary) throws Exception {
        Path keyFile = temporary.resolve("fixture.p12");
        trust = temporary.resolve("fixture-trust.jks");
        SSLContext tls = tls(temporary, keyFile, trust, HOST, "fixture");
        SSLContext wrong =
                tls(
                        temporary,
                        temporary.resolve("wrong.p12"),
                        trust,
                        "wrong.example.test",
                        "wrong");
        oauth = server(tls, "oauth", this::token);
        api = server(tls, "api", this::api);
        download = server(tls, "download", this::download);
        mismatchedTls = server(wrong, "api", this::api);
        Testcontainers.exposeHostPorts(
                oauth.getAddress().getPort(),
                api.getAddress().getPort(),
                download.getAddress().getPort(),
                mismatchedTls.getAddress().getPort());
    }

    String oauthOrigin() {
        return origin(oauth);
    }

    String apiOrigin() {
        return origin(api);
    }

    String mismatchedTlsOrigin() {
        return origin(mismatchedTls);
    }

    private static String origin(HttpsServer server) {
        return "https://" + HOST + ":" + server.getAddress().getPort();
    }

    Scenario select(String mode) {
        scenario.release = true;
        Scenario next = new Scenario(mode);
        scenario = next;
        return next;
    }

    Scenario scenario() {
        return scenario;
    }

    private HttpsServer server(SSLContext tls, String origin, Handler handler) throws IOException {
        HttpsServer server = HttpsServer.create(new InetSocketAddress(0), 16);
        server.setHttpsConfigurator(new HttpsConfigurator(tls));
        server.setExecutor(executor);
        server.createContext(
                "/",
                exchange -> {
                    Scenario active = scenario;
                    try {
                        handler.handle(active, exchange);
                    } catch (Exception ex) {
                        active.invalid.add(
                                origin
                                        + " fixture request failed: "
                                        + ex.getClass().getSimpleName());
                        exchange.close();
                    }
                });
        server.start();
        return server;
    }

    private void token(Scenario current, HttpExchange exchange) throws Exception {
        Headers headers = exchange.getRequestHeaders();
        Map<String, String> form = parse(new String(body(exchange), StandardCharsets.US_ASCII));
        boolean valid =
                TOKEN_PATH.equals(exchange.getRequestURI().getPath())
                        && "POST".equals(exchange.getRequestMethod())
                        && basic.equals(headers.getFirst("Authorization"))
                        && "application/x-www-form-urlencoded;charset=UTF-8"
                                .equals(headers.getFirst("Content-Type"))
                        && !headers.containsKey("Amazon-Advertising-API-ClientId")
                        && !headers.containsKey("Amazon-Advertising-API-Scope")
                        && form.equals(
                                Map.of("grant_type", "refresh_token", "refresh_token", refresh));
        Event mark = current.add("oauth", exchange, null, valid);
        if (!valid || current.mode.equals("bad-token")) {
            error(exchange, 400, "invalid_grant");
            return;
        }
        if (current.mode.equals("hold-oauth") && hold(current, exchange, mark)) {
            return;
        }
        int counter = current.tokens.incrementAndGet();
        JsonNode data =
                JSON.createObjectNode()
                        .put("access_token", access(counter))
                        .put("token_type", "bearer")
                        .put(
                                "expires_in",
                                current.mode.equals("expiry") && counter == 1 ? 1 : 3600);
        answer(exchange, 200, data, "application/json");
    }

    private static String access(int number) {
        return "Atza|private-fixture-access-" + number;
    }

    private void api(Scenario current, HttpExchange ex) throws Exception {
        String path = ex.getRequestURI().getPath();
        Headers headers = ex.getRequestHeaders();
        String profile = headers.getFirst("Amazon-Advertising-API-Scope");
        boolean scoped =
                PROFILE_PATH.equals(path)
                        ? profile == null
                        : Set.of("111", "222").contains(profile);
        boolean valid =
                ("Bearer " + access(current.tokens.get())).equals(headers.getFirst("Authorization"))
                        && clientId.equals(headers.getFirst("Amazon-Advertising-API-ClientId"))
                        && scoped;
        Event event = current.add("api", ex, profile, valid);
        if (!valid) {
            error(ex, 401, PRIVATE);
            return;
        }
        if (current.mode.equals("denied")) {
            error(ex, 403, PRIVATE);
            return;
        }
        if (current.mode.equals("redirect")) {
            ex.getResponseHeaders().set("Location", origin(download) + "/trap");
            error(ex, 302, PRIVATE);
            return;
        }
        if (current.mode.equals("stall") && hold(current, ex, event)) {
            return;
        }
        if (current.mode.equals("retry")
                && current.events.stream().filter(e -> e.origin.equals("api")).count() == 1) {
            ex.getResponseHeaders().set("Retry-After", "1");
            error(ex, 429, PRIVATE);
            return;
        }
        if (current.mode.equals("busy")) {
            error(ex, 503, PRIVATE);
            return;
        }
        if (current.mode.equals("long-retry")) {
            ex.getResponseHeaders().set("Retry-After", "120");
            error(ex, 429, PRIVATE);
            return;
        }
        if (PROFILE_PATH.equals(path)) {
            Map<String, String> args = parse(ex.getRequestURI().getRawQuery());
            if (!args.equals(Map.of("profileTypeFilter", "seller,vendor", "accessLevel", "view"))) {
                invalid(current, ex, "profile filters");
                return;
            }
            if (current.mode.equals("expiry")) {
                TimeUnit.MILLISECONDS.sleep(1150);
            }
            ArrayNode rows = JSON.createArrayNode();
            profiles().forEach(rows::add);
            answer(ex, 200, rows, "application/json");
            return;
        }
        if (current.mode.equals("reactive")
                && !current.rejectedAccess
                && !path.startsWith(REPORT_PATH)) {
            current.rejectedAccess = true;
            error(ex, 401, PRIVATE);
            return;
        }
        if (REPORT_PATH.equals(path) && "POST".equals(ex.getRequestMethod())) {
            createReport(current, ex, event, profile);
        } else if (path.startsWith(REPORT_PATH + "/") && "GET".equals(ex.getRequestMethod())) {
            status(current, ex, event, profile, path.substring(REPORT_PATH.length() + 1));
        } else {
            direct(current, ex, profile, path);
        }
    }

    private void direct(Scenario current, HttpExchange ex, String profile, String path)
            throws Exception {
        Map<String, String> query = parse(ex.getRequestURI().getRawQuery());
        boolean post = "POST".equals(ex.getRequestMethod());
        JsonNode request = post ? JSON.readTree(body(ex)) : JSON.createObjectNode();
        String attribution =
                "PRODUCTS".equals(request.path("reportType").asText())
                        ? "PRODUCTS"
                        : request.path("groupBy").asText("");
        AmazonAdsFixtureCatalog.Operation route = null;
        for (AmazonAdsFixtureCatalog.Operation item : AmazonAdsFixtureCatalog.DIRECT) {
            if (item.path.equals(path)
                    && (!item.mode.startsWith("AT:") || item.mode.equals("AT:" + attribution))) {
                route = item;
                break;
            }
        }
        if (route == null
                || !route.media.equals(ex.getRequestHeaders().getFirst("Accept"))
                || post && !route.media.equals(ex.getRequestHeaders().getFirst("Content-Type"))) {
            invalid(current, ex, "direct route or media");
            return;
        }
        boolean subsequent = false;
        if (route.mode.equals("OFFSET")) {
            if (post
                    || !"2".equals(query.get("count"))
                    || !Set.of("0", "2").contains(query.get("startIndex"))) {
                invalid(current, ex, "offset count/startIndex");
                return;
            }
            subsequent = "2".equals(query.get("startIndex"));
        } else if (route.mode.equals("QUERY_TOKEN")) {
            if (post
                    || !query.containsKey("pageSize")
                    || query.containsKey("startIndex")
                    || query.containsKey("nextToken") && !CURSOR.equals(query.get("nextToken"))) {
                invalid(current, ex, "budget query cursor");
                return;
            }
            subsequent = query.containsKey("nextToken");
        } else if (route.mode.equals("JSON_TOKEN")) {
            boolean portfolio = route.name.equals("portfolios");
            if (!post
                    || portfolio && !request.path("includeExtendedDataFields").asBoolean()
                    || portfolio && request.has("maxResults")
                    || !portfolio && request.path("maxResults").asInt() != 2
                    || request.has("nextToken")
                            && !CURSOR.equals(request.path("nextToken").asText())) {
                invalid(current, ex, "body cursor or maxResults");
                return;
            }
            if (route.name.equals("sponsored_product_campaigns")
                    || route.name.equals("sponsored_brands_campaigns")) {
                if (!List.of("ENABLED")
                        .equals(
                                AmazonAdsFixtureCatalog.textList(
                                        request.path("stateFilter").path("include")))) {
                    invalid(current, ex, "stateFilter include array");
                    return;
                }
            }
            subsequent = request.has("nextToken");
        } else if (route.mode.startsWith("AT:")) {
            String dateFormat = lower.format(DateTimeFormatter.BASIC_ISO_DATE);
            String toDate = upper.format(DateTimeFormatter.BASIC_ISO_DATE);
            if (!post
                    || !dateFormat.equals(request.path("startDate").asText())
                    || !toDate.equals(request.path("endDate").asText())
                    || request.path("count").asInt() != 300
                    || request.path("metrics").asText().isEmpty()
                    || !request.has("cursorId")
                    || !Set.of("", CURSOR).contains(request.path("cursorId").asText())) {
                invalid(current, ex, "attribution dates or continuation");
                return;
            }
            subsequent = !request.path("cursorId").asText().isEmpty();
        } else {
            String group = request.path("adGroupId").asText();
            String campaign = request.path("campaignId").asText();
            if (!post
                    || !List.of(groupId(profile, 1), groupId(profile, 2)).contains(group)
                    || !campaign.equals(campaignId(group))
                    || request.has("maxResults")
                    || request.has("maxRecommendations")) {
                invalid(current, ex, "ad-group parent arguments");
                return;
            }
            if (route.mode.equals("BID")) {
                if (!"BIDS_FOR_EXISTING_AD_GROUP"
                                .equals(request.path("recommendationType").asText())
                        || request.path("targetingExpressions").size() != 4) {
                    invalid(current, ex, "bid recommendation contract");
                    return;
                }
                answer(ex, 200, bid(group), route.media);
            } else {
                if (!"KEYWORDS_FOR_ADGROUP".equals(request.path("recommendationType").asText())) {
                    invalid(current, ex, "keyword recommendation contract");
                    return;
                }
                ObjectNode output = JSON.createObjectNode();
                output.putArray("keywordTargetList").add(suggestion(group));
                answer(ex, 200, output, route.media);
            }
            return;
        }
        if (current.mode.equals("hold-direct")
                && subsequent
                && hold(current, ex, current.events.get(current.events.size() - 1))) {
            return;
        }
        ArrayNode rows = JSON.createArrayNode();
        int first = subsequent ? 2 : 1;
        rows.add(directRow(route, profile, first));
        if (route.mode.equals("OFFSET") && !subsequent) {
            rows.add(directRow(route, profile, 3));
        }
        if (route.field.isEmpty()) {
            answer(ex, 200, rows, route.media);
            return;
        }
        ObjectNode result = JSON.createObjectNode();
        result.set(route.field, rows);
        String key = route.mode.startsWith("AT:") ? "cursorId" : "nextToken";
        if (!subsequent || current.mode.equals("cursor-cycle")) {
            result.put(key, CURSOR);
        }
        if (route.mode.equals("JSON_TOKEN")) {
            result.put("totalResults", current.mode.equals("lost-page") ? 3 : 2);
        }
        answer(ex, 200, result, route.media);
    }

    private void createReport(Scenario current, HttpExchange ex, Event event, String profile)
            throws Exception {
        ObjectNode request = (ObjectNode) JSON.readTree(body(ex));
        JsonNode conf = request.path("configuration");
        AmazonAdsFixtureCatalog.Projection shape =
                AmazonAdsFixtureCatalog.REPORTS.stream()
                        .filter(item -> item.matches(conf))
                        .findFirst()
                        .orElse(null);
        if (shape == null
                || !CREATE_TYPE.equals(ex.getRequestHeaders().getFirst("Content-Type"))
                || !CREATE_REPLY.equals(ex.getRequestHeaders().getFirst("Accept"))) {
            invalid(current, ex, "report metrics, filter, time unit or media");
            return;
        }
        LocalDate from = LocalDate.parse(request.path("startDate").asText());
        LocalDate to = LocalDate.parse(request.path("endDate").asText());
        if (from.isBefore(lower) || to.isAfter(upper) || from.isAfter(to)) {
            invalid(current, ex, "report dates");
            return;
        }
        String key = profile + "|" + shape.name + "|" + from + "|" + to;
        String id = UUID.nameUUIDFromBytes(key.getBytes(StandardCharsets.UTF_8)).toString();
        Report existing = current.reports.get(id);
        boolean previouslyRequested = existing != null;
        if (existing == null) {
            existing = new Report(id, profile, shape, request);
            current.reports.put(id, existing);
        }
        current.creates.incrementAndGet();
        if (current.mode.equals("hold-create") && hold(current, ex, event)) {
            return;
        }
        if (current.mode.equals("unknown-create")) {
            ex.close();
            return;
        }
        if (current.mode.equals("duplicate")
                || current.mode.equals("mismatch-425")
                || current.mode.equals("forbidden-425")
                || current.mode.equals("hold-create") && previouslyRequested) {
            error(ex, 425, "The Request is a duplicate of : " + id);
            return;
        }
        answer(ex, 200, reportBody(existing, "PENDING"), CREATE_REPLY);
    }

    private void status(Scenario current, HttpExchange ex, Event event, String profile, String id)
            throws Exception {
        Report report = current.reports.get(id);
        if (report == null
                || !report.profile.equals(profile)
                || !STATUS_TYPE.equals(ex.getRequestHeaders().getFirst("Accept"))) {
            invalid(current, ex, "report lookup scope or identity");
            return;
        }
        int seq = report.lookups.incrementAndGet();
        if (current.mode.equals("hold-poll") && seq == 1 && hold(current, ex, event)) {
            return;
        }
        if (current.mode.equals("forbidden-425")) {
            error(ex, 403, PRIVATE);
            return;
        }
        String state = "COMPLETED";
        if (current.mode.equals("failed")) {
            state = "FAILED";
        } else if (current.mode.equals("unknown-status")) {
            state = "STAGED";
        } else if (current.mode.equals("poll-timeout")
                || current.mode.equals("poll-transition") && seq == 1) {
            state = "PENDING";
        } else if (current.mode.equals("poll-transition") && seq == 2) {
            state = "PROCESSING";
        }
        ObjectNode value = reportBody(report, state);
        if (current.mode.equals("mismatch-425")) {
            ((ObjectNode) value.get("configuration"))
                    .put(
                            "timeUnit",
                            "DAILY".equals(report.template.timeUnit) ? "SUMMARY" : "DAILY");
        }
        String base =
                current.mode.equals("foreign-download")
                        ? "https://unapproved.invalid"
                        : origin(download);
        int signature = current.mode.equals("expired-url") && seq == 1 ? 0 : seq;
        value.put("url", base + "/file/" + id + "?signature=" + SIGNATURE + "-" + signature);
        value.put(
                "urlExpiresAt",
                signature == 0
                        ? Instant.now().minusSeconds(60).toString()
                        : Instant.now().plusSeconds(1800).toString());
        answer(ex, 200, value, STATUS_TYPE);
    }

    private void download(Scenario current, HttpExchange ex) throws Exception {
        String path = ex.getRequestURI().getPath();
        String id = path.startsWith("/file/") ? path.substring("/file/".length()) : "";
        String signature = parse(ex.getRequestURI().getRawQuery()).getOrDefault("signature", "");
        Headers headers = ex.getRequestHeaders();
        Report report = current.reports.get(id);
        boolean safe =
                !headers.containsKey("Authorization")
                        && !headers.containsKey("Amazon-Advertising-API-ClientId")
                        && !headers.containsKey("Amazon-Advertising-API-Scope")
                        && signature.startsWith(SIGNATURE + "-")
                        && "GET".equals(ex.getRequestMethod())
                        && report != null;
        Event event = current.add("download", ex, null, safe);
        if (!safe) {
            invalid(current, ex, "download credential isolation");
            return;
        }
        int seq = current.downloads.incrementAndGet();
        if (current.mode.equals("hold-download") && seq == 1 && hold(current, ex, event)) {
            return;
        }
        if (current.mode.equals("download-403") && seq == 1) {
            error(ex, 403, PRIVATE);
            return;
        }
        if (current.mode.equals("download-redirect")) {
            ex.getResponseHeaders().set("Location", apiOrigin() + "/trap?" + SIGNATURE);
            error(ex, 302, PRIVATE);
            return;
        }
        byte[] payload;
        if (current.mode.equals("empty-gzip")) {
            payload = gzip(new byte[0]);
        } else if (current.mode.equals("wire-limit")) {
            byte[] noise = new byte[16384];
            new java.util.Random(319).nextBytes(noise);
            ObjectNode row =
                    JSON.createObjectNode()
                            .put("date", report.request.path("startDate").asText())
                            .put("payload", Base64.getEncoder().encodeToString(noise));
            payload = gzip(JSON.writeValueAsBytes(JSON.createArrayNode().add(row)));
        } else if (current.mode.equals("gzip-limit")) {
            payload =
                    gzip(
                            ("[{\"payload\":\"" + "x".repeat(80000) + "\"}]")
                                    .getBytes(StandardCharsets.UTF_8));
        } else {
            ArrayNode data = JSON.createArrayNode();
            int rows =
                    current.mode.equals("empty-report")
                            ? 0
                            : current.mode.equals("long-report") ? 60 : 1;
            for (int i = 0; i < rows; i++) {
                data.add(
                        reportRow(
                                report.template,
                                report.profile,
                                report.request.path("startDate").asText(),
                                i,
                                current.changed && i == 0));
            }
            payload = gzip(JSON.writeValueAsBytes(data));
        }
        if (current.mode.equals("malformed-gzip")) {
            payload[payload.length - 5] ^= 41;
        } else if (current.mode.equals("truncated-gzip")) {
            payload = Arrays.copyOf(payload, payload.length - 6);
        }
        if (current.mode.equals("double-gzip")) {
            payload = gzip(payload);
            ex.getResponseHeaders().set("Content-Encoding", "gzip");
        } else if (current.mode.equals("transport-gzip")) {
            ex.getResponseHeaders().set("Content-Encoding", "gzip");
        }
        send(ex, 200, payload, "application/octet-stream");
    }

    private static ObjectNode reportBody(Report report, String state) {
        ObjectNode value = report.request.deepCopy();
        return value.put("reportId", report.id).put("status", state);
    }

    static List<ObjectNode> profiles() {
        return List.of(
                profile(111, "US", "Pacific/Pago_Pago"),
                profile(222, "GB", "Europe/London"),
                profile(333, "JP", "Asia/Tokyo"));
    }

    private static ObjectNode profile(int id, String marketplace, String timezone) {
        ObjectNode result = JSON.createObjectNode().put("profileId", id).put("timezone", timezone);
        result.putObject("accountInfo").put("marketplaceStringId", marketplace);
        return result;
    }

    static ObjectNode directRow(
            AmazonAdsFixtureCatalog.Operation route, String profile, int index) {
        ObjectNode result = JSON.createObjectNode();
        String id =
                route.name.equals("sponsored_product_ad_groups")
                        ? groupId(profile, index)
                        : profile + "-" + route.name + "-" + index;
        if (!route.key.isEmpty()) {
            result.put(route.key, id);
        }
        if (route.name.equals("sponsored_product_ad_groups")) {
            result.put("campaignId", campaignId(id));
        }
        result.put("source", route.name).put("value", index).put("text", "café 雪");
        result.putNull("nullable");
        return result;
    }

    static String groupId(String profile, int index) {
        return profile + "000" + index;
    }

    static String campaignId(String group) {
        return "9" + group;
    }

    static ObjectNode bid(String group) {
        ObjectNode result = JSON.createObjectNode();
        result.putArray("bidRecommendations").addObject().put("bid", 19).put("from", group);
        return result;
    }

    static ObjectNode suggestion(String group) {
        return JSON.createObjectNode().put("keyword", "café " + group).put("rank", 4);
    }

    static ObjectNode reportRow(
            AmazonAdsFixtureCatalog.Projection template,
            String profile,
            String from,
            int index,
            boolean changed) {
        ObjectNode row =
                JSON.createObjectNode()
                        .put("slice", template.name)
                        .put("account", profile)
                        .put("counter", changed ? -1 : index)
                        .put(
                                "integer",
                                new java.math.BigInteger(
                                        "1234567890123456789012345678901234567890"))
                        .put(
                                "amount",
                                new java.math.BigDecimal(
                                        "9876543210987654321.1234567890123456789"));
        row.putObject("nested").put("label", "café 雪").putNull("missing");
        if ("DAILY".equals(template.timeUnit)) {
            row.put("date", from);
        }
        return row;
    }

    private static boolean hold(Scenario current, HttpExchange ex, Event event) throws IOException {
        event.held = true;
        if (current.release) {
            return false;
        }
        ex.sendResponseHeaders(200, 1024 * 1024);
        OutputStream response = ex.getResponseBody();
        try (OutputStream out =
                event.origin.equals("download") ? new GZIPOutputStream(response, true) : response) {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
            while (!current.release && until - System.nanoTime() > 0) {
                out.write(' ');
                out.flush();
                TimeUnit.MILLISECONDS.sleep(100);
            }
        } catch (IOException expected) {
            event.closed = true;
        } catch (InterruptedException expected) {
            Thread.currentThread().interrupt();
        } finally {
            ex.close();
        }
        return true;
    }

    private static SSLContext tls(
            Path temporary, Path keyFile, Path trust, String hostname, String alias)
            throws Exception {
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
                                "CN=" + hostname,
                                "-ext",
                                "SAN=dns:" + hostname,
                                "-validity",
                                "2",
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                keyFile.toString(),
                                "-storepass",
                                STORE_PASSWORD)
                        .redirectErrorStream(true)
                        .redirectOutput(temporary.resolve("keytool-" + alias + ".log").toFile())
                        .start();
        if (!keytool.waitFor(30, TimeUnit.SECONDS) || keytool.exitValue() != 0) {
            keytool.destroyForcibly();
            throw new IllegalStateException("Fixture certificate generation failed");
        }
        char[] password = STORE_PASSWORD.toCharArray();
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(keyFile)) {
            keyStore.load(input, password);
        }
        Certificate cert = keyStore.getCertificate(alias);
        KeyStore pinned = KeyStore.getInstance("JKS");
        if (Files.exists(trust)) {
            try (InputStream input = Files.newInputStream(trust)) {
                pinned.load(input, password);
            }
        } else {
            pinned.load(null, password);
        }
        pinned.setCertificateEntry(alias, cert);
        try (OutputStream output = Files.newOutputStream(trust)) {
            pinned.store(output, password);
        }
        KeyManagerFactory manager =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        manager.init(keyStore, password);
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(manager.getKeyManagers(), null, null);
        return context;
    }

    private static byte[] body(HttpExchange exchange) throws IOException {
        try (InputStream input = exchange.getRequestBody()) {
            byte[] data = input.readNBytes(65537);
            if (data.length > 65536) {
                throw new IOException("fixture request too large");
            }
            return data;
        }
    }

    private static Map<String, String> parse(String query) {
        Map<String, String> result = new LinkedHashMap<>();
        if (query != null) {
            for (String pair : query.split("&")) {
                int separator = pair.indexOf('=');
                if (separator >= 0) {
                    result.put(
                            URLDecoder.decode(pair.substring(0, separator), StandardCharsets.UTF_8),
                            URLDecoder.decode(
                                    pair.substring(separator + 1), StandardCharsets.UTF_8));
                }
            }
        }
        return result;
    }

    private static void answer(HttpExchange ex, int status, JsonNode body, String format)
            throws IOException {
        send(ex, status, JSON.writeValueAsBytes(body), format);
    }

    private static void error(HttpExchange ex, int status, String detail) throws IOException {
        answer(ex, status, JSON.createObjectNode().put("detail", detail), "application/json");
    }

    private static void invalid(Scenario state, HttpExchange ex, String reason) throws IOException {
        state.invalid.add(reason);
        error(ex, 422, PRIVATE);
    }

    private static void send(HttpExchange ex, int status, byte[] bytes, String format)
            throws IOException {
        ex.getResponseHeaders().set("Content-Type", format);
        ex.getResponseHeaders().set("Connection", "close");
        ex.sendResponseHeaders(status, bytes.length);
        try (OutputStream target = ex.getResponseBody()) {
            target.write(bytes);
        } finally {
            ex.close();
        }
    }

    private static byte[] gzip(byte[] body) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (GZIPOutputStream compressed = new GZIPOutputStream(bytes)) {
            compressed.write(body);
        }
        return bytes.toByteArray();
    }

    @Override
    public void close() {
        scenario.release = true;
        oauth.stop(0);
        api.stop(0);
        download.stop(0);
        mismatchedTls.stop(0);
        executor.shutdownNow();
    }

    /**
     * A provider operation handled by one of the three isolated origins.
     *
     * <p>The fixture uses separate HTTPS authorities for bearer issuance, API reads and report
     * files.
     */
    private interface Handler {
        void handle(Scenario scenario, HttpExchange exchange) throws Exception;
    }

    /**
     * Captures request order, safe scope, and cancellation without retaining headers or signatures.
     *
     * <p>Engine assertions inspect phase order and boolean authorization outcomes.
     */
    static final class Event {
        final String origin;
        final String path;
        final String method;
        final String profile;
        final long nanos = System.nanoTime();
        final boolean valid;
        volatile boolean held;
        volatile boolean closed;

        Event(String origin, HttpExchange exchange, String profile, boolean valid) {
            this.origin = origin;
            path = exchange.getRequestURI().getPath();
            method = exchange.getRequestMethod();
            this.profile = profile;
            this.valid = valid;
        }
    }

    /**
     * Per-test provider mode with a fully isolated report registry and request trace.
     *
     * <p>Switching scenarios releases blocked requests without sharing their record state.
     */
    static final class Scenario {
        final String mode;
        final List<Event> events = new CopyOnWriteArrayList<>();
        final List<String> invalid = new CopyOnWriteArrayList<>();
        final Map<String, Report> reports = Collections.synchronizedMap(new HashMap<>());
        final AtomicInteger tokens = new AtomicInteger();
        final AtomicInteger creates = new AtomicInteger();
        final AtomicInteger downloads = new AtomicInteger();
        volatile boolean release;
        volatile boolean changed;
        volatile boolean rejectedAccess;

        Scenario(String mode) {
            this.mode = mode;
        }

        Event add(String origin, HttpExchange exchange, String profile, boolean valid) {
            Event event = new Event(origin, exchange, profile, valid);
            events.add(event);
            return event;
        }
    }

    /**
     * A synthetic report representing the exact profile, configuration and inclusive dates
     * submitted.
     *
     * <p>An ID is looked up under the same profile and remains active across controlled engine
     * restarts.
     */
    static final class Report {
        final String id;
        final String profile;
        final AmazonAdsFixtureCatalog.Projection template;
        final ObjectNode request;
        final AtomicInteger lookups = new AtomicInteger();

        Report(
                String id,
                String profile,
                AmazonAdsFixtureCatalog.Projection template,
                ObjectNode request) {
            this.id = id;
            this.profile = profile;
            this.template = template;
            this.request = request;
        }
    }
}
