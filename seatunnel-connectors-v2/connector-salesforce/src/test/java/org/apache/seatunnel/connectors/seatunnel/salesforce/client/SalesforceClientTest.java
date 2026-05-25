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

package org.apache.seatunnel.connectors.seatunnel.salesforce.client;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.seatunnel.salesforce.config.SalesforceParameters;
import org.apache.seatunnel.connectors.seatunnel.salesforce.exception.SalesforceConnectorException;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

class SalesforceClientTest {

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
    void authenticateThenDescribeStoresAccessTokenAndInstanceUrl() throws IOException {
        server.createContext(
                "/services/oauth2/token",
                exchange -> respondJson(exchange, 200, tokenResponse(baseUrl, "tok-1")));
        server.createContext(
                "/services/data/v59.0/sobjects/Account/describe",
                exchange -> respondJson(exchange, 200, describeJson("Id", "id")));

        try (SalesforceClient client = new SalesforceClient(buildParams(baseUrl))) {
            client.authenticate();
            CatalogTable table = client.describeObject("salesforce", "Account");
            SeaTunnelRowType rowType = table.getSeaTunnelRowType();
            Assertions.assertEquals(1, rowType.getTotalFields());
            Assertions.assertEquals("Id", rowType.getFieldName(0));
        }
    }

    @Test
    void authenticateFailureRaisesSalesforceException() {
        server.createContext(
                "/services/oauth2/token",
                exchange -> respondJson(exchange, 401, "{\"error\":\"invalid_grant\"}"));

        SalesforceClient client = new SalesforceClient(buildParams(baseUrl));
        SalesforceConnectorException ex =
                Assertions.assertThrows(SalesforceConnectorException.class, client::authenticate);
        Assertions.assertTrue(ex.getMessage().contains("401"));
    }

    @Test
    void describeObjectMapsSalesforceTypesToSeaTunnel() throws IOException {
        server.createContext(
                "/services/oauth2/token",
                exchange -> respondJson(exchange, 200, tokenResponse(baseUrl, "tok-1")));
        server.createContext(
                "/services/data/v59.0/sobjects/Opportunity/describe",
                exchange -> {
                    String body =
                            "{\"fields\":["
                                    + "{\"name\":\"Id\",\"type\":\"id\"},"
                                    + "{\"name\":\"IsClosed\",\"type\":\"boolean\"},"
                                    + "{\"name\":\"Amount\",\"type\":\"double\"},"
                                    + "{\"name\":\"CreatedDate\",\"type\":\"datetime\"},"
                                    + "{\"name\":\"CloseDate\",\"type\":\"date\"}"
                                    + "]}";
                    respondJson(exchange, 200, body);
                });

        try (SalesforceClient client = new SalesforceClient(buildParams(baseUrl))) {
            client.authenticate();
            CatalogTable table = client.describeObject("salesforce", "Opportunity");
            SeaTunnelRowType rowType = table.getSeaTunnelRowType();
            Assertions.assertEquals(5, rowType.getTotalFields());
            Assertions.assertEquals(SqlType.STRING, rowType.getFieldType(0).getSqlType());
            Assertions.assertEquals(SqlType.BOOLEAN, rowType.getFieldType(1).getSqlType());
            Assertions.assertEquals(SqlType.DOUBLE, rowType.getFieldType(2).getSqlType());
            Assertions.assertEquals(SqlType.TIMESTAMP, rowType.getFieldType(3).getSqlType());
            Assertions.assertEquals(SqlType.DATE, rowType.getFieldType(4).getSqlType());
        }
    }

    @Test
    void executeBulkQueryReturnsParsedRows() throws IOException {
        registerBulkJobHandlers("job-1", "JobComplete");
        server.createContext(
                "/services/data/v59.0/jobs/query/job-1/results",
                exchange -> respondCsv(exchange, 200, "Id,Name\n001,Acme\n002,Globex\n", null));

        try (SalesforceClient client = new SalesforceClient(buildParams(baseUrl))) {
            client.authenticate();
            List<Object[]> rows = new ArrayList<>();
            client.executeBulkQuery("SELECT Id, Name FROM Account", 2, rows::add);
            Assertions.assertEquals(2, rows.size());
            Assertions.assertArrayEquals(new Object[] {"001", "Acme"}, rows.get(0));
            Assertions.assertArrayEquals(new Object[] {"002", "Globex"}, rows.get(1));
        }
    }

    @Test
    void executeBulkQueryFollowsSforceLocatorPagination() throws IOException {
        registerBulkJobHandlers("job-1", "JobComplete");
        AtomicInteger resultsHits = new AtomicInteger();
        server.createContext(
                "/services/data/v59.0/jobs/query/job-1/results",
                exchange -> {
                    int hit = resultsHits.incrementAndGet();
                    if (hit == 1) {
                        respondCsv(exchange, 200, "Id\n001\n", "locator-2");
                    } else {
                        respondCsv(exchange, 200, "Id\n002\n", null);
                    }
                });

        try (SalesforceClient client = new SalesforceClient(buildParams(baseUrl))) {
            client.authenticate();
            List<Object[]> rows = new ArrayList<>();
            client.executeBulkQuery("SELECT Id FROM Account", 1, rows::add);
            Assertions.assertEquals(2, rows.size());
            Assertions.assertEquals(2, resultsHits.get());
        }
    }

    @Test
    void executeBulkQueryThrowsWhenJobFails() throws IOException {
        registerBulkJobHandlers("job-1", "Failed");

        try (SalesforceClient client = new SalesforceClient(buildParams(baseUrl))) {
            client.authenticate();
            Assertions.assertThrows(
                    SalesforceConnectorException.class,
                    () -> client.executeBulkQuery("SELECT Id FROM Account", 1, row -> {}));
        }
    }

    @Test
    void executeBulkQueryTimesOutWhenJobNeverCompletes() throws IOException {
        registerBulkJobHandlers("job-1", "InProgress");

        try (SalesforceClient client = new SalesforceClient(buildParams(baseUrl))) {
            client.authenticate();
            Assertions.assertThrows(
                    SalesforceConnectorException.class,
                    () -> client.executeBulkQuery("SELECT Id FROM Account", 1, row -> {}));
        }
    }

    private void registerBulkJobHandlers(String jobId, String terminalState) {
        server.createContext(
                "/services/oauth2/token",
                exchange -> respondJson(exchange, 200, tokenResponse(baseUrl, "tok-1")));
        server.createContext(
                "/services/data/v59.0/jobs/query",
                exchange -> respondJson(exchange, 200, "{\"id\":\"" + jobId + "\"}"));
        server.createContext(
                "/services/data/v59.0/jobs/query/" + jobId,
                exchange -> respondJson(exchange, 200, "{\"state\":\"" + terminalState + "\"}"));
    }

    private static String tokenResponse(String instanceUrl, String token) {
        return "{\"access_token\":\"" + token + "\",\"instance_url\":\"" + instanceUrl + "\"}";
    }

    private static String describeJson(String fieldName, String fieldType) {
        return "{\"fields\":[{\"name\":\"" + fieldName + "\",\"type\":\"" + fieldType + "\"}]}";
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

    private static void respondCsv(HttpExchange exchange, int status, String body, String locator)
            throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "text/csv");
        exchange.getResponseHeaders().add("Sforce-Locator", locator == null ? "null" : locator);
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    private static SalesforceParameters buildParams(String instanceUrl) {
        Map<String, Object> map = new HashMap<>();
        map.put("client_id", "cid");
        map.put("client_secret", "csec");
        map.put("username", "u@example.com");
        map.put("password", "pwd");
        map.put("instance_url", instanceUrl);
        map.put("poll_interval_ms", 10L);
        map.put("job_completion_timeout_ms", 500L);
        SalesforceParameters p = new SalesforceParameters();
        p.buildWithConfig(ReadonlyConfig.fromMap(map));
        return p;
    }
}
