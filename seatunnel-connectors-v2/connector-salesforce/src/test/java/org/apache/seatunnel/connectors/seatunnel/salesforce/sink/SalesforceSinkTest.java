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

package org.apache.seatunnel.connectors.seatunnel.salesforce.sink;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.salesforce.config.SalesforceSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.salesforce.exception.SalesforceConnectorException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SalesforceSinkTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final SeaTunnelRowType ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {"External_Id__c", "Name"},
                    new SeaTunnelDataType[] {BasicType.STRING_TYPE, BasicType.STRING_TYPE});
    private HttpServer server;
    private String url;
    private Map<String, Object> options;
    private final List<SalesforceSinkWriter> writers = new ArrayList<>();
    private final List<JsonNode> requests = Collections.synchronizedList(new ArrayList<>());
    private final List<Integer> sizes = Collections.synchronizedList(new ArrayList<>());
    private final List<String> authorization = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger tokens = new AtomicInteger();
    private volatile BiFunction<Integer, JsonNode, Reply> reply;
    private volatile String retryAfter;

    @BeforeEach
    void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        url = "http://127.0.0.1:" + server.getAddress().getPort();
        options = new HashMap<>();
        options.put("client_id", "client");
        options.put("client_secret", "private-secret");
        options.put("username", "user@example.com");
        options.put("password", "private-password");
        options.put("security_token", "private-token");
        options.put("instance_url", url);
        options.put("object_name", "Account");
        options.put("external_id_field", "External_Id__c");
        options.put("max_retries", 0);
        options.put("retry_interval_ms", 0L);
        options.put("request_timeout_ms", 1000);
        server.createContext(
                "/services/oauth2/token",
                exchange -> {
                    int token = tokens.incrementAndGet();
                    respond(
                            exchange,
                            200,
                            "{\"access_token\":\"token-"
                                    + token
                                    + "\",\"instance_url\":\""
                                    + url
                                    + "\"}");
                });
        reply = (index, body) -> new Reply(200, success(body.path("records").size()));
        server.createContext(
                "/services/data/v59.0/composite/sobjects/Account/External_Id__c",
                exchange -> {
                    if (!"PATCH".equals(exchange.getRequestMethod())) {
                        respond(exchange, 405, "[]");
                        return;
                    }
                    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                    byte[] bytes = new byte[4096];
                    int read;
                    while ((read = exchange.getRequestBody().read(bytes)) != -1) {
                        buffer.write(bytes, 0, read);
                    }
                    JsonNode request = MAPPER.readTree(buffer.toByteArray());
                    requests.add(request);
                    sizes.add(buffer.size());
                    authorization.add(exchange.getRequestHeaders().getFirst("Authorization"));
                    Reply result = reply.apply(requests.size(), request);
                    if (retryAfter != null) {
                        exchange.getResponseHeaders().add("Retry-After", retryAfter);
                    }
                    if (result.status == 307) {
                        exchange.getResponseHeaders().add("Location", url + "/trap");
                    }
                    respond(exchange, result.status, result.body);
                });
        server.start();
    }

    @AfterEach
    void stop() throws IOException {
        try {
            for (SalesforceSinkWriter writer : writers) {
                writer.close();
            }
        } finally {
            server.stop(0);
        }
    }

    @Test
    void sourceAndSinkFactoriesRemainDiscoverable() {
        assertNotNull(
                FactoryUtil.discoverFactory(
                        getClass().getClassLoader(), TableSourceFactory.class, "Salesforce"));
        assertNotNull(
                FactoryUtil.discoverFactory(
                        getClass().getClassLoader(), TableSinkFactory.class, "Salesforce"));
        ConfigValidator.of(ReadonlyConfig.fromMap(options))
                .validate(new SalesforceSinkFactory().optionRule());
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "{}",
                "{\"access_token\":\"secret\"}",
                "invalid-json",
                "{\"access_token\":\"secret\",\"instance_url\":\"https://user:secret@host\"}"
            })
    void rejectsMalformedAuthenticationWithoutLeakingCredentials(String body) {
        server.removeContext("/services/oauth2/token");
        server.createContext("/services/oauth2/token", exchange -> respond(exchange, 200, body));
        SalesforceConnectorException failure =
                assertThrows(SalesforceConnectorException.class, this::writer);
        assertFalse(failure.toString().contains("private-secret"));
        assertFalse(failure.toString().contains("user:secret"));
        assertTrue(requests.isEmpty());
    }

    @Test
    void socketTimeoutHasBoundedAttemptsAndDoesNotCompleteCheckpoint() throws Exception {
        options.put("request_timeout_ms", 1000);
        options.put("max_retries", 1);
        CountDownLatch release = new CountDownLatch(1);
        reply =
                (index, body) -> {
                    try {
                        release.await(60, TimeUnit.SECONDS);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                    }
                    return new Reply(200, success(body.path("records").size()));
                };
        SalesforceSinkWriter writer = writer();
        writer.write(row("1", "first"));
        try {
            SalesforceConnectorException failure =
                    assertThrows(SalesforceConnectorException.class, writer::prepareCommit);
            assertTrue(failure.getMessage().contains("after 2 attempt(s)"));
            assertThrows(SalesforceConnectorException.class, writer::prepareCommit);
        } finally {
            release.countDown();
        }
    }

    @Test
    void flushesByCountAndCheckpointWithDetachedRows() throws IOException {
        options.put("batch_size", 2);
        SalesforceSinkWriter writer = writer();
        SeaTunnelRow first = row("1", "first");
        writer.write(first);
        first.setField(1, "mutated-after-write");
        writer.write(row("2", "second"));
        assertEquals(1, requests.size());
        assertTrue(requests.get(0).path("allOrNone").asBoolean());
        assertEquals("first", requests.get(0).path("records").get(0).path("Name").asText());
        assertEquals(
                "Account",
                requests.get(0).path("records").get(0).path("attributes").path("type").asText());
        writer.write(row("3", null));
        writer.prepareCommit(11);
        assertEquals(2, requests.size());
        assertTrue(requests.get(1).path("records").get(0).path("Name").isNull());
        writer.prepareCommit(12);
        assertEquals(2, requests.size());
    }

    @Test
    void flushesRepeatedKeysInOrder() throws IOException {
        SalesforceSinkWriter writer = writer();
        writer.write(row("customer", "before"));
        SeaTunnelRow update = row("CUSTOMER", "after");
        update.setRowKind(RowKind.UPDATE_AFTER);
        writer.write(update);
        writer.prepareCommit();
        assertEquals(2, requests.size());
        assertEquals("before", requests.get(0).path("records").get(0).path("Name").asText());
        assertEquals("after", requests.get(1).path("records").get(0).path("Name").asText());
    }

    @Test
    void boundsActualUtf8RequestBytes() throws IOException {
        options.put("batch_max_bytes", 180);
        SalesforceSinkWriter writer = writer();
        writer.write(row("1", "雪雪雪雪雪"));
        writer.write(row("2", "雪雪雪雪雪"));
        writer.prepareCommit();
        assertEquals(2, requests.size());
        assertTrue(sizes.stream().allMatch(size -> size <= 180));
    }

    @Test
    void rejectsOversizedRecordWithoutSendingOrReplayingOnClose() throws IOException {
        options.put("batch_max_bytes", 128);
        SalesforceSinkWriter writer = writer();
        assertThrows(
                IllegalArgumentException.class,
                () -> writer.write(row("1", String.join("", Collections.nCopies(200, "x")))));
        assertThrows(IllegalArgumentException.class, writer::prepareCommit);
        writer.close();
        writer.close();
        assertTrue(requests.isEmpty());
    }

    @ParameterizedTest
    @EnumSource(
            value = RowKind.class,
            names = {"DELETE", "UPDATE_BEFORE"})
    void rejectsUnsupportedChangelogKinds(RowKind kind) throws IOException {
        SalesforceSinkWriter writer = writer();
        SeaTunnelRow row = row("1", "name");
        row.setRowKind(kind);
        assertThrows(IllegalArgumentException.class, () -> writer.write(row));
        assertTrue(requests.isEmpty());
    }

    @Test
    void rejectsMissingExternalIdBeforeSending() throws IOException {
        SalesforceSinkWriter writer = writer();
        assertThrows(IllegalArgumentException.class, () -> writer.write(row(null, "name")));
        assertTrue(requests.isEmpty());
    }

    @Test
    void partialResultFailsCheckpointWithoutRetryAndWithoutEchoingData() throws IOException {
        options.put("max_retries", 3);
        reply =
                (index, body) ->
                        new Reply(
                                200,
                                "[{\"id\":\"001\",\"success\":true,\"errors\":[]},"
                                        + "{\"id\":null,\"success\":false,\"errors\":[{\"statusCode\":\"INVALID_FIELD\",\"message\":\"private-secret row value\"}]}]");
        SalesforceSinkWriter writer = writer();
        writer.write(row("1", "one"));
        writer.write(row("2", "two"));
        SalesforceConnectorException failure =
                assertThrows(SalesforceConnectorException.class, writer::prepareCommit);
        assertTrue(failure.getMessage().contains("index 1"));
        assertTrue(failure.getMessage().contains("INVALID_FIELD"));
        assertFalse(failure.toString().contains("private-secret"));
        assertThrows(SalesforceConnectorException.class, writer::prepareCommit);
        writer.close();
        assertEquals(1, requests.size());
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "not-json",
                "[]",
                "{}",
                "[{\"success\":true,\"errors\":[]}]",
                "[{\"id\":\"001\",\"success\":true}]",
                "[{\"success\":\"true\",\"errors\":[]}]"
            })
    void malformedSuccessResponseDoesNotAcknowledgeOrRetry(String body) throws IOException {
        reply = (index, request) -> new Reply(200, body);
        options.put("max_retries", 3);
        SalesforceSinkWriter writer = writer();
        writer.write(row("1", "name"));
        assertThrows(SalesforceConnectorException.class, writer::prepareCommit);
        assertEquals(1, requests.size());
    }

    @ParameterizedTest
    @ValueSource(ints = {429, 500, 502, 503, 504})
    void retriesTransientHttpStatusWithIdenticalPayload(int status) throws IOException {
        options.put("max_retries", 1);
        retryAfter = "0";
        reply =
                (index, body) ->
                        index == 1
                                ? new Reply(status, "[]")
                                : new Reply(200, success(body.path("records").size()));
        SalesforceSinkWriter writer = writer();
        writer.write(row("1", "name"));
        writer.prepareCommit();
        assertEquals(2, requests.size());
        assertEquals(requests.get(0), requests.get(1));
    }

    @Test
    void retryExhaustionIsBounded() throws IOException {
        options.put("max_retries", 2);
        reply = (index, body) -> new Reply(503, "[]");
        SalesforceSinkWriter writer = writer();
        writer.write(row("1", "name"));
        assertThrows(SalesforceConnectorException.class, writer::prepareCommit);
        assertEquals(3, requests.size());
    }

    @Test
    void excessiveRetryAfterFailsWithoutHammeringEndpoint() throws IOException {
        options.put("max_retries", 3);
        retryAfter = "3600";
        reply = (index, body) -> new Reply(429, "[]");
        SalesforceSinkWriter writer = writer();
        writer.write(row("1", "name"));
        assertThrows(SalesforceConnectorException.class, writer::prepareCommit);
        assertEquals(1, requests.size());
    }

    @Test
    void expiredSessionRefreshesOnce() throws IOException {
        options.put("max_retries", 3);
        reply = (index, body) -> index == 1 ? new Reply(401, "[]") : new Reply(200, success(1));
        SalesforceSinkWriter writer = writer();
        writer.write(row("1", "name"));
        writer.prepareCommit();
        assertEquals(2, tokens.get());
        assertEquals("Bearer token-1", authorization.get(0));
        assertEquals("Bearer token-2", authorization.get(1));
    }

    @Test
    void repeatedUnauthorizedResponseDoesNotRefreshForever() throws IOException {
        options.put("max_retries", 3);
        reply = (index, body) -> new Reply(401, "[]");
        SalesforceSinkWriter writer = writer();
        writer.write(row("1", "name"));
        assertThrows(SalesforceConnectorException.class, writer::prepareCommit);
        assertEquals(2, tokens.get());
        assertEquals(2, requests.size());
    }

    @ParameterizedTest
    @ValueSource(ints = {400, 403, 404, 307})
    void permanentErrorsAndRedirectsAreNotRetried(int status) throws IOException {
        AtomicInteger trap = new AtomicInteger();
        server.createContext(
                "/trap",
                exchange -> {
                    trap.incrementAndGet();
                    respond(exchange, 200, success(1));
                });
        options.put("max_retries", 3);
        reply = (index, body) -> new Reply(status, "private-secret");
        SalesforceSinkWriter writer = writer();
        writer.write(row("1", "name"));
        SalesforceConnectorException failure =
                assertThrows(SalesforceConnectorException.class, writer::prepareCommit);
        assertFalse(failure.toString().contains("private-secret"));
        assertEquals(1, requests.size());
        assertEquals(0, trap.get());
    }

    @Test
    void interruptionStopsBackoffAndPreservesInterrupt() throws Exception {
        options.put("max_retries", 3);
        options.put("retry_interval_ms", 60000L);
        CountDownLatch requested = new CountDownLatch(1);
        reply =
                (index, body) -> {
                    requested.countDown();
                    return new Reply(503, "[]");
                };
        SalesforceSinkWriter writer = writer();
        writer.write(row("1", "name"));
        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicReference<Boolean> interrupted = new AtomicReference<>(false);
        Thread thread =
                new Thread(
                        () -> {
                            try {
                                writer.prepareCommit();
                            } catch (Throwable e) {
                                error.set(e);
                                interrupted.set(Thread.currentThread().isInterrupted());
                            }
                        });
        thread.start();
        try {
            assertTrue(requested.await(5, TimeUnit.SECONDS));
            thread.interrupt();
            thread.join(5000);
            assertFalse(thread.isAlive());
            assertInstanceOf(SalesforceConnectorException.class, error.get());
            assertEquals(true, interrupted.get());
            assertEquals(1, requests.size());
        } finally {
            thread.interrupt();
            thread.join(5000);
        }
    }

    @Test
    void closesWithFinalFlushAndIsIdempotent() throws IOException {
        SalesforceSinkWriter writer = writer();
        writer.write(row("1", "name"));
        writer.close();
        writer.close();
        assertEquals(1, requests.size());
        assertThrows(IllegalStateException.class, () -> writer.write(row("2", "name")));
    }

    private SalesforceSinkWriter writer() throws IOException {
        SalesforceSinkWriter writer =
                new SalesforceSinkWriter(
                        ROW_TYPE, new SalesforceSinkConfig(ReadonlyConfig.fromMap(options)));
        writers.add(writer);
        return writer;
    }

    private static SeaTunnelRow row(String key, String name) {
        return new SeaTunnelRow(new Object[] {key, name});
    }

    private static String success(int count) {
        ArrayNode results = MAPPER.createArrayNode();
        for (int i = 0; i < count; i++) {
            results.addObject()
                    .put("id", "001" + i)
                    .put("success", true)
                    .put("created", false)
                    .putArray("errors");
        }
        return results.toString();
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    private static final class Reply {
        final int status;
        final String body;

        Reply(int status, String body) {
            this.status = status;
            this.body = body;
        }
    }
}
