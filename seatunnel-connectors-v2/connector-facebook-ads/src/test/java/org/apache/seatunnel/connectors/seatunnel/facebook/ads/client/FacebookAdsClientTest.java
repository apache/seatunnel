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

package org.apache.seatunnel.connectors.seatunnel.facebook.ads.client;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.facebook.ads.config.FacebookAdsParameters;
import org.apache.seatunnel.connectors.seatunnel.facebook.ads.config.FacebookAdsTableConfig;
import org.apache.seatunnel.connectors.seatunnel.facebook.ads.exception.FacebookAdsConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.facebook.ads.exception.FacebookAdsConnectorException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class FacebookAdsClientTest {

    private static final String AD_ACCOUNT_ID = "1234567890";
    private static final String CAMPAIGNS_PATH = "/v23.0/act_" + AD_ACCOUNT_ID + "/campaigns";

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
    void searchStreamsPagesFollowingAfterCursor() throws IOException {
        AtomicInteger hits = new AtomicInteger();
        AtomicReference<String> secondQuery = new AtomicReference<>();
        server.createContext(
                CAMPAIGNS_PATH,
                exchange -> {
                    if (hits.incrementAndGet() == 1) {
                        respondJson(
                                exchange,
                                200,
                                "{\"data\":["
                                        + "{\"id\":\"111\",\"name\":\"c1\","
                                        + "\"insights\":{\"data\":[{\"spend\":\"1.5\"}]}}"
                                        + "],\"paging\":{\"cursors\":{\"before\":\"b1\","
                                        + "\"after\":\"cursor-2\"},"
                                        + "\"next\":\"https://ignored/next\"}}");
                    } else {
                        secondQuery.set(exchange.getRequestURI().getQuery());
                        respondJson(
                                exchange,
                                200,
                                "{\"data\":[{\"id\":\"222\",\"name\":\"c2\"}],"
                                        + "\"paging\":{\"cursors\":{\"before\":\"b2\","
                                        + "\"after\":\"cursor-3\"}}}");
                    }
                });

        try (FacebookAdsClient client = new FacebookAdsClient(buildParams())) {
            List<Object[]> rows = new ArrayList<>();
            client.search(
                    tableConfig(Arrays.asList("id", "name", "insights", "status")), rows::add);

            Assertions.assertEquals(2, rows.size());
            Assertions.assertEquals(2, hits.get());
            // value nodes as strings; nested object as JSON text; absent field -> null
            Assertions.assertEquals("111", rows.get(0)[0]);
            Assertions.assertEquals("c1", rows.get(0)[1]);
            Assertions.assertEquals("{\"data\":[{\"spend\":\"1.5\"}]}", rows.get(0)[2]);
            Assertions.assertNull(rows.get(0)[3]);
            Assertions.assertEquals("222", rows.get(1)[0]);
            // page 2 requested via the after cursor; page without next ends the read
            Assertions.assertTrue(secondQuery.get().contains("after=cursor-2"));
        }
    }

    @Test
    void searchSendsBearerHeaderAndQueryParams() throws IOException {
        AtomicReference<String> authHeader = new AtomicReference<>();
        AtomicReference<String> query = new AtomicReference<>();
        server.createContext(
                CAMPAIGNS_PATH,
                exchange -> {
                    authHeader.set(exchange.getRequestHeaders().getFirst("Authorization"));
                    query.set(
                            URLDecoder.decode(
                                    exchange.getRequestURI().getRawQuery(),
                                    StandardCharsets.UTF_8.name()));
                    respondJson(exchange, 200, "{\"data\":[]}");
                });

        LinkedHashMap<String, String> extraParams = new LinkedHashMap<>();
        extraParams.put("date_preset", "last_30d");
        FacebookAdsTableConfig table =
                new FacebookAdsTableConfig(
                        "campaigns",
                        AD_ACCOUNT_ID,
                        Arrays.asList("id", "name"),
                        "[{\"field\":\"effective_status\"}]",
                        extraParams,
                        null);

        try (FacebookAdsClient client = new FacebookAdsClient(buildParams())) {
            client.search(table, row -> {});
            Assertions.assertEquals("Bearer test-token", authHeader.get());
            Assertions.assertTrue(query.get().contains("fields=id,name"));
            Assertions.assertTrue(
                    query.get().contains("filtering=[{\"field\":\"effective_status\"}]"));
            Assertions.assertTrue(query.get().contains("date_preset=last_30d"));
            Assertions.assertTrue(query.get().contains("limit=50"));
            Assertions.assertFalse(query.get().contains("access_token"));
        }
    }

    @Test
    void searchFailsFastOn401WithAuthError() throws IOException {
        AtomicInteger hits = new AtomicInteger();
        server.createContext(
                CAMPAIGNS_PATH,
                exchange -> {
                    hits.incrementAndGet();
                    respondJson(
                            exchange,
                            401,
                            "{\"error\":{\"message\":\"Invalid OAuth access token\","
                                    + "\"code\":190}}");
                });

        try (FacebookAdsClient client = new FacebookAdsClient(buildParams())) {
            FacebookAdsConnectorException ex =
                    Assertions.assertThrows(
                            FacebookAdsConnectorException.class,
                            () -> client.search(tableConfig(Arrays.asList("id")), row -> {}));
            Assertions.assertEquals(
                    FacebookAdsConnectorErrorCode.AUTH_FAILED, ex.getSeaTunnelErrorCode());
            Assertions.assertEquals(1, hits.get());
        }
    }

    @Test
    void searchRetriesTransient503UpToMaxRetries() throws IOException {
        AtomicInteger hits = new AtomicInteger();
        server.createContext(
                CAMPAIGNS_PATH,
                exchange -> {
                    hits.incrementAndGet();
                    respondJson(exchange, 503, "{\"error\":\"unavailable\"}");
                });

        try (FacebookAdsClient client = new FacebookAdsClient(buildParams())) {
            Assertions.assertThrows(
                    FacebookAdsConnectorException.class,
                    () -> client.search(tableConfig(Arrays.asList("id")), row -> {}));
            // initial attempt + max_retries(2) retries
            Assertions.assertEquals(3, hits.get());
        }
    }

    @Test
    void searchRetriesRateLimit400WithFacebookErrorCode() throws IOException {
        AtomicInteger hits = new AtomicInteger();
        server.createContext(
                CAMPAIGNS_PATH,
                exchange -> {
                    if (hits.incrementAndGet() == 1) {
                        // code 4 = application request limit reached, carried in an HTTP 400
                        respondJson(
                                exchange,
                                400,
                                "{\"error\":{\"message\":\"Application request limit reached\","
                                        + "\"code\":4}}");
                    } else {
                        respondJson(exchange, 200, "{\"data\":[{\"id\":\"1\"}]}");
                    }
                });

        try (FacebookAdsClient client = new FacebookAdsClient(buildParams())) {
            List<Object[]> rows = new ArrayList<>();
            client.search(tableConfig(Arrays.asList("id")), rows::add);
            Assertions.assertEquals(1, rows.size());
            Assertions.assertEquals(2, hits.get());
        }
    }

    @Test
    void searchDoesNotRetryPlain400AndSurfacesApiError() throws IOException {
        AtomicInteger hits = new AtomicInteger();
        server.createContext(
                CAMPAIGNS_PATH,
                exchange -> {
                    hits.incrementAndGet();
                    respondJson(
                            exchange,
                            400,
                            "{\"error\":{\"message\":\"Unknown fields: bad_field\","
                                    + "\"code\":100}}");
                });

        try (FacebookAdsClient client = new FacebookAdsClient(buildParams())) {
            FacebookAdsConnectorException ex =
                    Assertions.assertThrows(
                            FacebookAdsConnectorException.class,
                            () ->
                                    client.search(
                                            tableConfig(Arrays.asList("bad_field")), row -> {}));
            Assertions.assertEquals(
                    FacebookAdsConnectorErrorCode.REQUEST_FAILED, ex.getSeaTunnelErrorCode());
            Assertions.assertEquals(1, hits.get());
            Assertions.assertTrue(ex.getMessage().contains("Unknown fields"));
        }
    }

    private FacebookAdsTableConfig tableConfig(List<String> fields) {
        return new FacebookAdsTableConfig("campaigns", AD_ACCOUNT_ID, fields, null, null, null);
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

    private FacebookAdsParameters buildParams() {
        Map<String, Object> map = new HashMap<>();
        map.put("access_token", "test-token");
        map.put("ad_account_id", AD_ACCOUNT_ID);
        map.put("api_endpoint", baseUrl);
        map.put("max_retries", 2);
        map.put("retry_backoff_ms", 10L);
        map.put("page_size", 50);
        FacebookAdsParameters params = new FacebookAdsParameters();
        params.buildWithConfig(ReadonlyConfig.fromMap(map));
        return params;
    }
}
