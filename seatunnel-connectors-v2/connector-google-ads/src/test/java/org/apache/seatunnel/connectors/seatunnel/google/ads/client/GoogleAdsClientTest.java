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

package org.apache.seatunnel.connectors.seatunnel.google.ads.client;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.seatunnel.google.ads.config.GoogleAdsParameters;
import org.apache.seatunnel.connectors.seatunnel.google.ads.exception.GoogleAdsConnectorException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class GoogleAdsClientTest {

    private static final String CUSTOMER_ID = "1234567890";
    private static final String FIELDS_PATH = "/v21/googleAdsFields:search";
    private static final String SEARCH_PATH = "/v21/customers/" + CUSTOMER_ID + "/googleAds:search";

    private HttpServer server;
    private String baseUrl;

    @BeforeEach
    void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.start();
        baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void authenticateFailureRaisesAuthException() {
        server.createContext(
                "/token", exchange -> respondJson(exchange, 400, "{\"error\":\"invalid_grant\"}"));

        try (GoogleAdsClient client = new GoogleAdsClient(buildParams(null))) {
            GoogleAdsConnectorException ex =
                    Assertions.assertThrows(
                            GoogleAdsConnectorException.class, client::authenticate);
            Assertions.assertTrue(ex.getMessage().contains("400"));
        } catch (IOException e) {
            Assertions.fail(e);
        }
    }

    @Test
    void describeFieldsBuildsSchemaInRequestOrderDespiteShuffledResponse() throws IOException {
        registerToken();
        server.createContext(
                FIELDS_PATH,
                exchange ->
                        respondJson(
                                exchange,
                                200,
                                "{\"results\":["
                                        + fieldMeta("metrics.clicks", "INT64")
                                        + ","
                                        + fieldMeta("campaign.name", "STRING")
                                        + ","
                                        + fieldMeta("segments.date", "DATE")
                                        + ","
                                        + fieldMeta("campaign.id", "INT64")
                                        + ","
                                        + fieldMeta("metrics.ctr", "DOUBLE")
                                        + ","
                                        + fieldMeta("campaign.status", "ENUM")
                                        + "]}"));

        try (GoogleAdsClient client = new GoogleAdsClient(buildParams(null))) {
            CatalogTable table =
                    client.describeFields(
                            "google_ads",
                            "campaign",
                            Arrays.asList(
                                    "campaign.id",
                                    "campaign.name",
                                    "campaign.status",
                                    "metrics.clicks",
                                    "metrics.ctr",
                                    "segments.date"));
            SeaTunnelRowType rowType = table.getSeaTunnelRowType();
            Assertions.assertEquals(6, rowType.getTotalFields());
            Assertions.assertEquals("campaign.id", rowType.getFieldName(0));
            Assertions.assertEquals("campaign.name", rowType.getFieldName(1));
            Assertions.assertEquals("campaign.status", rowType.getFieldName(2));
            Assertions.assertEquals("metrics.clicks", rowType.getFieldName(3));
            Assertions.assertEquals("metrics.ctr", rowType.getFieldName(4));
            Assertions.assertEquals("segments.date", rowType.getFieldName(5));
            Assertions.assertEquals(SqlType.BIGINT, rowType.getFieldType(0).getSqlType());
            Assertions.assertEquals(SqlType.STRING, rowType.getFieldType(1).getSqlType());
            Assertions.assertEquals(SqlType.STRING, rowType.getFieldType(2).getSqlType());
            Assertions.assertEquals(SqlType.BIGINT, rowType.getFieldType(3).getSqlType());
            Assertions.assertEquals(SqlType.DOUBLE, rowType.getFieldType(4).getSqlType());
            // DATE deliberately maps to STRING (non-uniform formats like 2026-09)
            Assertions.assertEquals(SqlType.STRING, rowType.getFieldType(5).getSqlType());
        }
    }

    @Test
    void describeFieldsThrowsOnUnknownField() throws IOException {
        registerToken();
        server.createContext(
                FIELDS_PATH,
                exchange ->
                        respondJson(
                                exchange,
                                200,
                                "{\"results\":[" + fieldMeta("campaign.id", "INT64") + "]}"));

        try (GoogleAdsClient client = new GoogleAdsClient(buildParams(null))) {
            GoogleAdsConnectorException ex =
                    Assertions.assertThrows(
                            GoogleAdsConnectorException.class,
                            () ->
                                    client.describeFields(
                                            "google_ads",
                                            "campaign",
                                            Arrays.asList("campaign.id", "campaign.namee")));
            Assertions.assertTrue(ex.getMessage().contains("campaign.namee"));
        }
    }

    @Test
    void searchStreamsPagesAndConvertsTypesWithSnakeToCamelResolution() throws IOException {
        registerToken();
        AtomicInteger searchHits = new AtomicInteger();
        AtomicReference<String> secondRequestBody = new AtomicReference<>();
        server.createContext(
                SEARCH_PATH,
                exchange -> {
                    String requestBody = readBody(exchange);
                    if (searchHits.incrementAndGet() == 1) {
                        respondJson(
                                exchange,
                                200,
                                "{\"results\":["
                                        + "{\"adGroup\":{\"id\":\"111\",\"cpcBidMicros\":\"2500000\","
                                        + "\"status\":\"ENABLED\"},"
                                        + "\"metrics\":{\"ctr\":0.052,\"clicks\":\"42\"}}"
                                        + "],\"nextPageToken\":\"page-2\"}");
                    } else {
                        secondRequestBody.set(requestBody);
                        respondJson(
                                exchange,
                                200,
                                "{\"results\":["
                                        + "{\"adGroup\":{\"id\":\"222\",\"status\":\"PAUSED\"},"
                                        + "\"metrics\":{\"ctr\":0.01,\"clicks\":\"7\"}}"
                                        + "]}");
                    }
                });

        List<String> fieldPaths =
                Arrays.asList(
                        "ad_group.id",
                        "ad_group.cpc_bid_micros",
                        "ad_group.status",
                        "metrics.ctr",
                        "metrics.clicks");
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        fieldPaths.toArray(new String[0]),
                        new SeaTunnelDataType[] {
                            BasicType.LONG_TYPE,
                            BasicType.LONG_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.DOUBLE_TYPE,
                            BasicType.LONG_TYPE
                        });

        try (GoogleAdsClient client = new GoogleAdsClient(buildParams(null))) {
            List<Object[]> rows = new ArrayList<>();
            client.search(
                    CUSTOMER_ID,
                    "SELECT ad_group.id FROM ad_group",
                    fieldPaths,
                    rowType,
                    rows::add);

            Assertions.assertEquals(2, rows.size());
            Assertions.assertEquals(2, searchHits.get());
            // string-encoded int64 parsed to Long; snake_case path resolved via camelCase keys
            Assertions.assertArrayEquals(
                    new Object[] {111L, 2500000L, "ENABLED", 0.052, 42L}, rows.get(0));
            // absent cpc_bid_micros on page 2 -> null
            Assertions.assertArrayEquals(
                    new Object[] {222L, null, "PAUSED", 0.01, 7L}, rows.get(1));
            Assertions.assertTrue(secondRequestBody.get().contains("\"pageToken\":\"page-2\""));
        }
    }

    @Test
    void searchSendsRequiredGoogleAdsHeaders() throws IOException {
        registerToken();
        AtomicReference<String> authHeader = new AtomicReference<>();
        AtomicReference<String> devTokenHeader = new AtomicReference<>();
        AtomicReference<String> loginCidHeader = new AtomicReference<>();
        server.createContext(
                SEARCH_PATH,
                exchange -> {
                    authHeader.set(exchange.getRequestHeaders().getFirst("Authorization"));
                    devTokenHeader.set(exchange.getRequestHeaders().getFirst("developer-token"));
                    loginCidHeader.set(exchange.getRequestHeaders().getFirst("login-customer-id"));
                    respondJson(exchange, 200, "{\"results\":[]}");
                });

        try (GoogleAdsClient client = new GoogleAdsClient(buildParams("9999999999"))) {
            client.search(
                    CUSTOMER_ID,
                    "SELECT campaign.id FROM campaign",
                    Arrays.asList("campaign.id"),
                    new SeaTunnelRowType(
                            new String[] {"campaign.id"},
                            new SeaTunnelDataType[] {BasicType.LONG_TYPE}),
                    row -> {});
            Assertions.assertEquals("Bearer tok-1", authHeader.get());
            Assertions.assertEquals("dev-token", devTokenHeader.get());
            Assertions.assertEquals("9999999999", loginCidHeader.get());
        }
    }

    @Test
    void searchRefreshesTokenOnceAndReplaysOn401() throws IOException {
        AtomicInteger tokenHits = new AtomicInteger();
        server.createContext(
                "/token",
                exchange ->
                        respondJson(
                                exchange,
                                200,
                                "{\"access_token\":\"tok-"
                                        + tokenHits.incrementAndGet()
                                        + "\",\"expires_in\":3600}"));
        AtomicInteger searchHits = new AtomicInteger();
        server.createContext(
                SEARCH_PATH,
                exchange -> {
                    if (searchHits.incrementAndGet() == 1) {
                        respondJson(exchange, 401, "{\"error\":{\"status\":\"UNAUTHENTICATED\"}}");
                    } else {
                        Assertions.assertEquals(
                                "Bearer tok-2",
                                exchange.getRequestHeaders().getFirst("Authorization"));
                        respondJson(exchange, 200, "{\"results\":[{\"campaign\":{\"id\":\"1\"}}]}");
                    }
                });

        try (GoogleAdsClient client = new GoogleAdsClient(buildParams(null))) {
            List<Object[]> rows = new ArrayList<>();
            client.search(
                    CUSTOMER_ID,
                    "SELECT campaign.id FROM campaign",
                    Arrays.asList("campaign.id"),
                    new SeaTunnelRowType(
                            new String[] {"campaign.id"},
                            new SeaTunnelDataType[] {BasicType.LONG_TYPE}),
                    rows::add);
            Assertions.assertEquals(1, rows.size());
            Assertions.assertEquals(2, searchHits.get());
            Assertions.assertEquals(2, tokenHits.get());
        }
    }

    @Test
    void searchRetriesTransient503UpToMaxRetries() throws IOException {
        registerToken();
        AtomicInteger searchHits = new AtomicInteger();
        server.createContext(
                SEARCH_PATH,
                exchange -> {
                    searchHits.incrementAndGet();
                    respondJson(exchange, 503, "{\"error\":\"unavailable\"}");
                });

        try (GoogleAdsClient client = new GoogleAdsClient(buildParams(null))) {
            Assertions.assertThrows(
                    GoogleAdsConnectorException.class,
                    () ->
                            client.search(
                                    CUSTOMER_ID,
                                    "SELECT campaign.id FROM campaign",
                                    Arrays.asList("campaign.id"),
                                    new SeaTunnelRowType(
                                            new String[] {"campaign.id"},
                                            new SeaTunnelDataType[] {BasicType.LONG_TYPE}),
                                    row -> {}));
            // initial attempt + max_retries(2) retries
            Assertions.assertEquals(3, searchHits.get());
        }
    }

    @Test
    void searchDoesNotRetryInvalidQuery400AndSurfacesApiError() throws IOException {
        registerToken();
        AtomicInteger searchHits = new AtomicInteger();
        server.createContext(
                SEARCH_PATH,
                exchange -> {
                    searchHits.incrementAndGet();
                    respondJson(
                            exchange,
                            400,
                            "{\"error\":{\"status\":\"INVALID_ARGUMENT\","
                                    + "\"message\":\"Unrecognized field in the query\"}}");
                });

        try (GoogleAdsClient client = new GoogleAdsClient(buildParams(null))) {
            GoogleAdsConnectorException ex =
                    Assertions.assertThrows(
                            GoogleAdsConnectorException.class,
                            () ->
                                    client.search(
                                            CUSTOMER_ID,
                                            "SELECT bad.field FROM campaign",
                                            Arrays.asList("bad.field"),
                                            new SeaTunnelRowType(
                                                    new String[] {"bad.field"},
                                                    new SeaTunnelDataType[] {
                                                        BasicType.STRING_TYPE
                                                    }),
                                            row -> {}));
            Assertions.assertEquals(1, searchHits.get());
            Assertions.assertTrue(ex.getMessage().contains("Unrecognized field"));
        }
    }

    private void registerToken() {
        server.createContext(
                "/token",
                exchange ->
                        respondJson(
                                exchange, 200, "{\"access_token\":\"tok-1\",\"expires_in\":3600}"));
    }

    private static String fieldMeta(String name, String dataType) {
        return "{\"name\":\"" + name + "\",\"dataType\":\"" + dataType + "\"}";
    }

    private static String readBody(HttpExchange exchange) throws IOException {
        byte[] buf = new byte[8192];
        StringBuilder sb = new StringBuilder();
        int n;
        while ((n = exchange.getRequestBody().read(buf)) > 0) {
            sb.append(new String(buf, 0, n, StandardCharsets.UTF_8));
        }
        return sb.toString();
    }

    private static void respondJson(HttpExchange exchange, int status, String body)
            throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    private GoogleAdsParameters buildParams(String loginCustomerId) {
        Map<String, Object> map = new HashMap<>();
        map.put("developer_token", "dev-token");
        map.put("client_id", "cid");
        map.put("client_secret", "csec");
        map.put("refresh_token", "rtok");
        map.put("customer_id", CUSTOMER_ID);
        map.put("api_endpoint", baseUrl);
        map.put("oauth_endpoint", baseUrl);
        map.put("max_retries", 2);
        map.put("retry_backoff_ms", 10L);
        if (loginCustomerId != null) {
            map.put("login_customer_id", loginCustomerId);
        }
        GoogleAdsParameters params = new GoogleAdsParameters();
        params.buildWithConfig(ReadonlyConfig.fromMap(map));
        return params;
    }
}
