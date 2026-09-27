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

package org.apache.seatunnel.connectors.seatunnel.google.ads.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.seatunnel.google.ads.config.GoogleAdsParameters;
import org.apache.seatunnel.connectors.seatunnel.google.ads.config.GoogleAdsTableConfig;
import org.apache.seatunnel.connectors.seatunnel.google.ads.exception.GoogleAdsConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.google.ads.exception.GoogleAdsConnectorException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Covers the config-resolution logic of {@link GoogleAdsSource}: three-mode dispatch, GAQL
 * building/parsing, table_path handling and field-path validation. The metadata service is stubbed
 * with a local HTTP server (same pattern as GoogleAdsClientTest).
 */
class GoogleAdsSourceTest {

    private static final String CUSTOMER_ID = "1234567890";

    private HttpServer server;
    private String baseUrl;

    @BeforeEach
    void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/token",
                exchange ->
                        respondJson(
                                exchange, 200, "{\"access_token\":\"tok-1\",\"expires_in\":3600}"));
        server.createContext(
                "/v21/googleAdsFields:search",
                exchange ->
                        respondJson(
                                exchange,
                                200,
                                "{\"results\":["
                                        + fieldMeta("campaign.id", "INT64")
                                        + ","
                                        + fieldMeta("campaign.name", "STRING")
                                        + ","
                                        + fieldMeta("metrics.clicks", "INT64")
                                        + ","
                                        + fieldMeta("ad_group.id", "INT64")
                                        + ","
                                        + fieldMeta("ad_group.name", "STRING")
                                        + "]}"));
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
    void resourceModeBuildsGaqlWithFilterAndSchemaInFieldOrder() {
        Map<String, Object> map = baseConfig();
        map.put("resource", "campaign");
        map.put("fields", Arrays.asList("campaign.id", "campaign.name", "metrics.clicks"));
        map.put("filter", "segments.date DURING LAST_7_DAYS");

        GoogleAdsSource source = buildSource(map);

        List<GoogleAdsTableConfig> configs = source.getTableConfigs();
        Assertions.assertEquals(1, configs.size());
        GoogleAdsTableConfig table = configs.get(0);
        Assertions.assertEquals(
                "SELECT campaign.id, campaign.name, metrics.clicks FROM campaign "
                        + "WHERE segments.date DURING LAST_7_DAYS",
                table.getGaql());
        Assertions.assertEquals("campaign", table.getResource());
        Assertions.assertEquals("google_ads.campaign", table.getTableId());
        SeaTunnelRowType rowType = source.getProducedCatalogTables().get(0).getSeaTunnelRowType();
        Assertions.assertEquals("campaign.id", rowType.getFieldName(0));
        Assertions.assertEquals("campaign.name", rowType.getFieldName(1));
        Assertions.assertEquals("metrics.clicks", rowType.getFieldName(2));
        Assertions.assertEquals(SqlType.BIGINT, rowType.getFieldType(0).getSqlType());
        Assertions.assertEquals(SqlType.STRING, rowType.getFieldType(1).getSqlType());
    }

    @Test
    void queryModeParsesFieldsAndResourceFromGaql() {
        Map<String, Object> map = baseConfig();
        map.put("query", "select campaign.id ,  campaign.name from campaign WHERE campaign.id > 0");

        GoogleAdsTableConfig table = buildSource(map).getTableConfigs().get(0);
        Assertions.assertEquals(
                Arrays.asList("campaign.id", "campaign.name"), table.getFieldPaths());
        Assertions.assertEquals("campaign", table.getResource());
        Assertions.assertEquals(
                "select campaign.id ,  campaign.name from campaign WHERE campaign.id > 0",
                table.getGaql());
    }

    @Test
    void queryModeRejectsQueryCombinedWithFields() {
        Map<String, Object> map = baseConfig();
        map.put("query", "SELECT campaign.id FROM campaign");
        map.put("fields", Arrays.asList("campaign.id"));

        GoogleAdsConnectorException ex = assertBuildFails(map);
        Assertions.assertEquals(
                GoogleAdsConnectorErrorCode.INVALID_QUERY, ex.getSeaTunnelErrorCode());
        Assertions.assertTrue(ex.getMessage().contains("mutually exclusive"));
    }

    @Test
    void queryModeRejectsUnparsableGaql() {
        Map<String, Object> map = baseConfig();
        map.put("query", "DELETE FROM campaign");

        GoogleAdsConnectorException ex = assertBuildFails(map);
        Assertions.assertEquals(
                GoogleAdsConnectorErrorCode.INVALID_QUERY, ex.getSeaTunnelErrorCode());
    }

    @Test
    void resourceModeRequiresNonEmptyFields() {
        Map<String, Object> map = baseConfig();
        map.put("resource", "campaign");

        GoogleAdsConnectorException ex = assertBuildFails(map);
        Assertions.assertEquals(
                GoogleAdsConnectorErrorCode.INVALID_QUERY, ex.getSeaTunnelErrorCode());
        Assertions.assertTrue(ex.getMessage().contains("fields"));
    }

    @Test
    void missingAllModesIsRejected() {
        GoogleAdsConnectorException ex = assertBuildFails(baseConfig());
        Assertions.assertEquals(
                GoogleAdsConnectorErrorCode.INVALID_QUERY, ex.getSeaTunnelErrorCode());
    }

    @Test
    void invalidFieldPathsAreRejectedWithOffendingName() {
        for (String bad : Arrays.asList("Campaign.Id", "campaign", "campaign.id;drop")) {
            Map<String, Object> map = baseConfig();
            map.put("resource", "campaign");
            map.put("fields", Arrays.asList(bad));

            GoogleAdsConnectorException ex = assertBuildFails(map);
            Assertions.assertEquals(
                    GoogleAdsConnectorErrorCode.INVALID_QUERY, ex.getSeaTunnelErrorCode());
            Assertions.assertTrue(ex.getMessage().contains(bad), "message should name: " + bad);
        }
    }

    @Test
    void tablesConfigsBuildsMultipleTablesWithPerTableCustomerIdFallback() {
        Map<String, Object> entryWithOverride = new HashMap<>();
        entryWithOverride.put("table_path", "google_ads.campaign");
        entryWithOverride.put("fields", Arrays.asList("campaign.id", "campaign.name"));
        entryWithOverride.put("customer_id", "2222222222");
        Map<String, Object> entryWithFallback = new HashMap<>();
        entryWithFallback.put("table_path", "google_ads.ad_group");
        entryWithFallback.put("query", "SELECT ad_group.id, ad_group.name FROM ad_group");

        Map<String, Object> map = baseConfig();
        map.put("tables_configs", Arrays.asList(entryWithOverride, entryWithFallback));

        List<GoogleAdsTableConfig> configs = buildSource(map).getTableConfigs();
        Assertions.assertEquals(2, configs.size());
        Assertions.assertEquals("google_ads.campaign", configs.get(0).getTableId());
        Assertions.assertEquals("2222222222", configs.get(0).getCustomerId());
        Assertions.assertEquals("google_ads.ad_group", configs.get(1).getTableId());
        Assertions.assertEquals(CUSTOMER_ID, configs.get(1).getCustomerId());
    }

    @Test
    void tablesConfigsRejectsResourceMismatchBetweenTablePathAndQuery() {
        Map<String, Object> entry = new HashMap<>();
        entry.put("table_path", "google_ads.campaign");
        entry.put("query", "SELECT ad_group.id FROM ad_group");

        Map<String, Object> map = baseConfig();
        map.put("tables_configs", Arrays.asList(entry));

        GoogleAdsConnectorException ex = assertBuildFails(map);
        Assertions.assertEquals(
                GoogleAdsConnectorErrorCode.INVALID_QUERY, ex.getSeaTunnelErrorCode());
        Assertions.assertTrue(ex.getMessage().contains("does not match"));
    }

    @Test
    void tablesConfigsRejectsDuplicateTablePath() {
        Map<String, Object> entry1 = new HashMap<>();
        entry1.put("table_path", "google_ads.campaign");
        entry1.put("fields", Arrays.asList("campaign.id"));
        Map<String, Object> entry2 = new HashMap<>();
        entry2.put("table_path", "google_ads.campaign");
        entry2.put("fields", Arrays.asList("campaign.name"));

        Map<String, Object> map = baseConfig();
        map.put("tables_configs", Arrays.asList(entry1, entry2));

        GoogleAdsConnectorException ex = assertBuildFails(map);
        Assertions.assertEquals(
                GoogleAdsConnectorErrorCode.DUPLICATE_RESOURCE, ex.getSeaTunnelErrorCode());
    }

    @Test
    void tablesConfigsRejectsMalformedTablePath() {
        for (String badPath : Arrays.asList("campaign", ".campaign", "google_ads.")) {
            Map<String, Object> entry = new HashMap<>();
            entry.put("table_path", badPath);
            entry.put("fields", Arrays.asList("campaign.id"));

            Map<String, Object> map = baseConfig();
            map.put("tables_configs", Arrays.asList(entry));

            GoogleAdsConnectorException ex = assertBuildFails(map);
            Assertions.assertEquals(
                    GoogleAdsConnectorErrorCode.INVALID_TABLE_PATH,
                    ex.getSeaTunnelErrorCode(),
                    "table_path should be rejected: " + badPath);
        }
    }

    private GoogleAdsConnectorException assertBuildFails(Map<String, Object> map) {
        return Assertions.assertThrows(GoogleAdsConnectorException.class, () -> buildSource(map));
    }

    private GoogleAdsSource buildSource(Map<String, Object> map) {
        ReadonlyConfig config = ReadonlyConfig.fromMap(map);
        GoogleAdsParameters params = new GoogleAdsParameters();
        params.buildWithConfig(config);
        return new GoogleAdsSource(params, config);
    }

    private Map<String, Object> baseConfig() {
        Map<String, Object> map = new HashMap<>();
        map.put("developer_token", "dev-token");
        map.put("client_id", "cid");
        map.put("client_secret", "csec");
        map.put("refresh_token", "rtok");
        map.put("customer_id", CUSTOMER_ID);
        map.put("api_endpoint", baseUrl);
        map.put("oauth_endpoint", baseUrl);
        map.put("max_retries", 1);
        map.put("retry_backoff_ms", 10L);
        return map;
    }

    private static String fieldMeta(String name, String dataType) {
        return "{\"name\":\"" + name + "\",\"dataType\":\"" + dataType + "\"}";
    }

    private static void respondJson(
            com.sun.net.httpserver.HttpExchange exchange, int status, String body)
            throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }
}
