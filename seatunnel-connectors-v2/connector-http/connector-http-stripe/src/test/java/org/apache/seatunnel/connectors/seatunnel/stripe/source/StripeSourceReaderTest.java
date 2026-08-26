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

package org.apache.seatunnel.connectors.seatunnel.stripe.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorException;
import org.apache.seatunnel.connectors.seatunnel.stripe.source.config.StripeSourceParameter;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class StripeSourceReaderTest {

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
    void readsPaymentIntentsAcrossCursorPagesWithBoundedTimeRange() throws Exception {
        List<Map<String, String>> requests = Collections.synchronizedList(new ArrayList<>());
        List<String> authorizationHeaders = Collections.synchronizedList(new ArrayList<>());
        server.createContext(
                "/v1/payment_intents",
                exchange -> {
                    requests.add(parseQuery(exchange.getRequestURI().getRawQuery()));
                    authorizationHeaders.add(
                            exchange.getRequestHeaders().getFirst("Authorization"));
                    String cursor = requests.get(requests.size() - 1).get("starting_after");
                    if (cursor == null) {
                        respondJson(
                                exchange,
                                200,
                                "{\"data\":[{\"id\":\"pi_3\",\"amount\":300},{\"id\":\"pi_2\",\"amount\":200}],\"has_more\":true}");
                    } else {
                        respondJson(
                                exchange,
                                200,
                                "{\"data\":[{\"id\":\"pi_1\",\"amount\":100}],\"has_more\":false}");
                    }
                });

        SingleSplitReaderContext context = mock(SingleSplitReaderContext.class);
        RecordingCollector collector = new RecordingCollector();
        try (StripeSourceReader reader = createReader(context, 2, 0)) {
            reader.open();
            reader.internalPollNext(collector);
        }

        Assertions.assertEquals(3, collector.rows.size());
        Assertions.assertTrue(collector.rows.get(0).getField(0).toString().contains("\"pi_3\""));
        Assertions.assertTrue(collector.rows.get(1).getField(0).toString().contains("\"pi_2\""));
        Assertions.assertTrue(collector.rows.get(2).getField(0).toString().contains("\"pi_1\""));
        Assertions.assertEquals("2", requests.get(0).get("limit"));
        Assertions.assertEquals("100", requests.get(0).get("created[gte]"));
        Assertions.assertEquals("200", requests.get(0).get("created[lt]"));
        Assertions.assertNull(requests.get(0).get("starting_after"));
        Assertions.assertEquals("pi_2", requests.get(1).get("starting_after"));
        Assertions.assertEquals(
                Arrays.asList("Bearer sk_test_secret", "Bearer sk_test_secret"),
                authorizationHeaders);
        verify(context, times(1)).signalNoMoreElement();
    }

    @Test
    void acceptsEmptyFinalPage() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        server.createContext(
                "/v1/payment_intents",
                exchange -> {
                    requests.incrementAndGet();
                    respondJson(exchange, 200, "{\"data\":[],\"has_more\":false}");
                });

        RecordingCollector collector = new RecordingCollector();
        try (StripeSourceReader reader =
                createReader(mock(SingleSplitReaderContext.class), 100, 0)) {
            reader.open();
            reader.internalPollNext(collector);
        }

        Assertions.assertTrue(collector.rows.isEmpty());
        Assertions.assertEquals(1, requests.get());
    }

    @Test
    void retriesRateLimitThenContinues() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        server.createContext(
                "/v1/payment_intents",
                exchange -> {
                    if (requests.incrementAndGet() == 1) {
                        respondJson(exchange, 429, "{\"error\":{\"message\":\"rate limited\"}}");
                    } else {
                        respondJson(
                                exchange, 200, "{\"data\":[{\"id\":\"pi_1\"}],\"has_more\":false}");
                    }
                });

        RecordingCollector collector = new RecordingCollector();
        try (StripeSourceReader reader =
                createReader(mock(SingleSplitReaderContext.class), 100, 1)) {
            reader.open();
            reader.internalPollNext(collector);
        }

        Assertions.assertEquals(2, requests.get());
        Assertions.assertEquals(1, collector.rows.size());
    }

    @Test
    void reportsRateLimitAfterRetryBudgetIsExhausted() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        server.createContext(
                "/v1/payment_intents",
                exchange -> {
                    requests.incrementAndGet();
                    respondJson(exchange, 429, "{\"error\":{\"message\":\"rate limited\"}}");
                });

        try (StripeSourceReader reader =
                createReader(mock(SingleSplitReaderContext.class), 100, 1)) {
            reader.open();
            HttpConnectorException error =
                    Assertions.assertThrows(
                            HttpConnectorException.class,
                            () -> reader.internalPollNext(new RecordingCollector()));
            Assertions.assertTrue(error.getMessage().contains("HTTP 429"));
        }
        Assertions.assertEquals(2, requests.get());
    }

    @Test
    void reportsApiErrorWithoutLeakingSecret() throws Exception {
        server.createContext(
                "/v1/payment_intents",
                exchange ->
                        respondJson(
                                exchange,
                                401,
                                "{\"error\":{\"message\":\"invalid api key sk_test_secret\"}}"));

        try (StripeSourceReader reader =
                createReader(mock(SingleSplitReaderContext.class), 100, 0)) {
            reader.open();
            HttpConnectorException error =
                    Assertions.assertThrows(
                            HttpConnectorException.class,
                            () -> reader.internalPollNext(new RecordingCollector()));
            Assertions.assertTrue(error.getMessage().contains("HTTP 401"));
            Assertions.assertFalse(error.getMessage().contains("sk_test_secret"));
            Assertions.assertTrue(error.getMessage().contains("[REDACTED]"));
        }
    }

    @Test
    void rejectsRepeatedCursorBeforeCollectingDuplicatePage() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        server.createContext(
                "/v1/payment_intents",
                exchange -> {
                    requests.incrementAndGet();
                    respondJson(exchange, 200, "{\"data\":[{\"id\":\"pi_1\"}],\"has_more\":true}");
                });

        RecordingCollector collector = new RecordingCollector();
        try (StripeSourceReader reader =
                createReader(mock(SingleSplitReaderContext.class), 100, 0)) {
            reader.open();
            HttpConnectorException error =
                    Assertions.assertThrows(
                            HttpConnectorException.class, () -> reader.internalPollNext(collector));
            Assertions.assertTrue(error.getMessage().contains("repeated cursor"));
        }
        Assertions.assertEquals(2, requests.get());
        Assertions.assertEquals(1, collector.rows.size());
    }

    @Test
    void rejectsCursorCycleBeforeCollectingRepeatedPage() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        server.createContext(
                "/v1/payment_intents",
                exchange -> {
                    int request = requests.incrementAndGet();
                    String id = request == 2 ? "pi_2" : "pi_1";
                    respondJson(
                            exchange,
                            200,
                            "{\"data\":[{\"id\":\"" + id + "\"}],\"has_more\":true}");
                });

        RecordingCollector collector = new RecordingCollector();
        try (StripeSourceReader reader =
                createReader(mock(SingleSplitReaderContext.class), 100, 0)) {
            reader.open();
            HttpConnectorException error =
                    Assertions.assertThrows(
                            HttpConnectorException.class, () -> reader.internalPollNext(collector));
            Assertions.assertTrue(error.getMessage().contains("repeated cursor"));
        }
        Assertions.assertEquals(3, requests.get());
        Assertions.assertEquals(2, collector.rows.size());
    }

    @Test
    void rejectsHasMoreWithEmptyPage() throws Exception {
        server.createContext(
                "/v1/payment_intents",
                exchange -> respondJson(exchange, 200, "{\"data\":[],\"has_more\":true}"));

        try (StripeSourceReader reader =
                createReader(mock(SingleSplitReaderContext.class), 100, 0)) {
            reader.open();
            HttpConnectorException error =
                    Assertions.assertThrows(
                            HttpConnectorException.class,
                            () -> reader.internalPollNext(new RecordingCollector()));
            Assertions.assertTrue(error.getMessage().contains("has_more=true"));
        }
    }

    private StripeSourceReader createReader(
            SingleSplitReaderContext context, int pageSize, int maxRateLimitRetries) {
        Map<String, Object> options = new HashMap<>();
        options.put("secret_key", "sk_test_secret");
        options.put("api_base_url", baseUrl);
        options.put("page_size", pageSize);
        options.put("created_gte", 100L);
        options.put("created_lt", 200L);
        options.put("rate_limit_max_retries", maxRateLimitRetries);
        options.put("rate_limit_backoff_ms", 0);
        StripeSourceParameter parameter = new StripeSourceParameter();
        parameter.buildWithConfig(ReadonlyConfig.fromMap(options));
        return new StripeSourceReader(parameter, context, millis -> {});
    }

    private static Map<String, String> parseQuery(String rawQuery) throws IOException {
        Map<String, String> query = new LinkedHashMap<>();
        if (rawQuery == null || rawQuery.isEmpty()) {
            return query;
        }
        for (String part : rawQuery.split("&")) {
            String[] entry = part.split("=", 2);
            String key = URLDecoder.decode(entry[0], StandardCharsets.UTF_8.name());
            String value =
                    entry.length == 1
                            ? ""
                            : URLDecoder.decode(entry[1], StandardCharsets.UTF_8.name());
            query.put(key, value);
        }
        return query;
    }

    private static void respondJson(HttpExchange exchange, int statusCode, String body)
            throws IOException {
        byte[] response = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(statusCode, response.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(response);
        }
    }

    private static final class RecordingCollector implements Collector<SeaTunnelRow> {
        private final List<SeaTunnelRow> rows = new ArrayList<>();
        private final Object checkpointLock = new Object();

        @Override
        public void collect(SeaTunnelRow record) {
            rows.add(record);
        }

        @Override
        public Object getCheckpointLock() {
            return checkpointLock;
        }
    }
}
