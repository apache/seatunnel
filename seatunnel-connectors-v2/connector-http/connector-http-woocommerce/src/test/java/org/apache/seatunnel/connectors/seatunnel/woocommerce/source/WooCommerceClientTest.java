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

package org.apache.seatunnel.connectors.seatunnel.woocommerce.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorException;

import org.apache.http.HttpVersion;
import org.apache.http.MessageConstraintException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.io.DefaultHttpResponseParser;
import org.apache.http.impl.io.HttpTransportMetricsImpl;
import org.apache.http.impl.io.SessionInputBufferImpl;
import org.apache.http.message.BasicHttpResponse;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class WooCommerceClientTest {
    @Test
    void configuredTransportRejectsOversizedHeaderLinesAndHeaderCounts() {
        String longHeader =
                "HTTP/1.1 200 OK\r\nX-Test: "
                        + String.join("", Collections.nCopies(8193, "x"))
                        + "\r\n\r\n";
        String manyHeaders =
                "HTTP/1.1 200 OK\r\n"
                        + String.join("", Collections.nCopies(101, "X-Test: x\r\n"))
                        + "\r\n";
        for (String response : new String[] {longHeader, manyHeaders}) {
            SessionInputBufferImpl input =
                    new SessionInputBufferImpl(
                            new HttpTransportMetricsImpl(),
                            1024,
                            -1,
                            WooCommerceClient.MESSAGE_CONSTRAINTS,
                            null);
            input.bind(new ByteArrayInputStream(response.getBytes(StandardCharsets.US_ASCII)));
            DefaultHttpResponseParser parser =
                    new DefaultHttpResponseParser(input, WooCommerceClient.MESSAGE_CONSTRAINTS);
            assertThrows(MessageConstraintException.class, parser::parse);
        }
    }

    @Test
    void responseLimitAppliesWithAndWithoutContentLength() throws Exception {
        Map<String, Object> values = options();
        values.put("max_response_bytes", 1024);
        String body = String.join("", Collections.nCopies(1025, "x"));
        for (boolean knownLength : new boolean[] {true, false}) {
            CloseableHttpResponse response = reply(200, "0", "0", "");
            response.setEntity(
                    new StringEntity(body, StandardCharsets.UTF_8) {
                        @Override
                        public long getContentLength() {
                            return knownLength ? super.getContentLength() : -1;
                        }
                    });
            CloseableHttpClient http = mock(CloseableHttpClient.class);
            when(http.execute(any(HttpUriRequest.class))).thenReturn(response);
            try (WooCommerceClient client =
                    new WooCommerceClient(
                            new WooCommerceConfig(ReadonlyConfig.fromMap(values)), http)) {
                HttpConnectorException error =
                        assertThrows(HttpConnectorException.class, () -> client.page(1));
                assertTrue(error.getMessage().contains("max_response_bytes"));
                assertFalse(error.getMessage().contains(body));
            }
        }
    }

    @Test
    void deadlineAbortsAnActiveAttemptWithoutReportingSuccess() throws Exception {
        Map<String, Object> values = options();
        values.put("request_timeout_ms", 50);
        values.put("max_retries", 0);
        CloseableHttpClient http = mock(CloseableHttpClient.class);
        when(http.execute(any(HttpUriRequest.class)))
                .thenAnswer(
                        invocation -> {
                            HttpGet request = invocation.getArgument(0);
                            CountDownLatch aborted = new CountDownLatch(1);
                            request.setCancellable(
                                    () -> {
                                        aborted.countDown();
                                        return true;
                                    });
                            assertTrue(aborted.await(5, TimeUnit.SECONDS));
                            assertTrue(request.isAborted());
                            throw new IOException("synthetic abort");
                        });
        try (WooCommerceClient client =
                new WooCommerceClient(
                        new WooCommerceConfig(ReadonlyConfig.fromMap(values)), http)) {
            assertThrows(HttpConnectorException.class, () -> client.page(1));
            verify(http, times(1)).execute(any(HttpUriRequest.class));
        }
    }

    @Test
    void offsetsBecomeUtcWallClockQueryDatesWithoutSiteTimezoneShift() {
        Map<String, Object> positive = options();
        positive.put("start_date", "2026-01-01T05:30:00+05:30");
        positive.put("end_date", "2026-02-01T05:30:00+05:30");
        Map<String, Object> negative = options();
        negative.put("start_date", "2025-12-31T19:00:00-05:00");
        negative.put("end_date", "2026-01-31T19:00:00-05:00");
        for (Map<String, Object> values : new Map[] {positive, negative, options()}) {
            WooCommerceConfig config = new WooCommerceConfig(ReadonlyConfig.fromMap(values));
            assertEquals("2026-01-01T00:00:00", config.start);
            assertEquals("2026-02-01T00:00:00", config.end);
        }
    }

    @Test
    void closeWakesRetryAndReleasesTransport() throws Exception {
        Map<String, Object> values = options();
        values.put("retry_delay_ms", 60000);
        CloseableHttpClient http = mock(CloseableHttpClient.class);
        CountDownLatch requested = new CountDownLatch(1);
        when(http.execute(any(HttpUriRequest.class)))
                .thenAnswer(
                        inv -> {
                            requested.countDown();
                            return reply(503, null, null, "");
                        });
        ExecutorService worker = Executors.newSingleThreadExecutor();
        WooCommerceClient client =
                new WooCommerceClient(new WooCommerceConfig(ReadonlyConfig.fromMap(values)), http);
        try {
            Future<?> work =
                    worker.submit(
                            () -> {
                                try {
                                    client.page(1);
                                } catch (Exception error) {
                                    throw new IllegalStateException(error);
                                }
                            });
            assertTrue(requested.await(5, TimeUnit.SECONDS));
            client.close();
            assertThrows(ExecutionException.class, () -> work.get(5, TimeUnit.SECONDS));
            verify(http).close();
        } finally {
            client.close();
            worker.shutdownNow();
            assertTrue(worker.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void interruptAfterLastRowDoesNotReportSuccessfulCompletion() throws Exception {
        CloseableHttpClient http = mock(CloseableHttpClient.class);
        when(http.execute(any(HttpUriRequest.class)))
                .thenReturn(reply(200, "1", "1", "[" + order(1, "1") + "]"));
        SingleSplitReaderContext context = mock(SingleSplitReaderContext.class);
        try (WooCommerceClient client = new WooCommerceClient(config(), http);
                WooCommerceSourceReader reader = reader(client, context)) {
            assertThrows(
                    HttpConnectorException.class,
                    () ->
                            reader.pollNext(
                                    new Collector<SeaTunnelRow>() {
                                        public void collect(SeaTunnelRow row) {
                                            Thread.currentThread().interrupt();
                                        }

                                        public Object getCheckpointLock() {
                                            return this;
                                        }
                                    }));
            verify(context, never()).signalNoMoreElement();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void emptyWindowCompletesAndCheckpointHasNoCredentials() throws Exception {
        CloseableHttpClient http = mock(CloseableHttpClient.class);
        when(http.execute(any(HttpUriRequest.class))).thenReturn(reply(200, "0", "0", "[]"));
        SingleSplitReaderContext context = mock(SingleSplitReaderContext.class);
        try (WooCommerceClient client = new WooCommerceClient(config(), http);
                WooCommerceSourceReader reader = reader(client, context)) {
            assertNull(reader.snapshotState(1).get(0).getState());
            List<SeaTunnelRow> rows = new ArrayList<>();
            reader.pollNext(collector(rows));
            assertTrue(rows.isEmpty());
            verify(context).signalNoMoreElement();
        }
    }

    static final String KEY = "ck_0000000000000000000000000000000000000001";
    static final String SECRET = "cs_0000000000000000000000000000000000000002";

    static Map<String, Object> options() {
        Map<String, Object> v = new LinkedHashMap<>();
        v.put("url", "https://store.example/shop");
        v.put("consumer_key", KEY);
        v.put("consumer_secret", SECRET);
        v.put("start_date", "2026-01-01T00:00:00Z");
        v.put("end_date", "2026-02-01T00:00:00Z");
        v.put("page_size", 2);
        v.put("retry_delay_ms", 1);
        v.put("max_retries", 1);
        Map<String, Object> f = new LinkedHashMap<>();
        f.put("id", "bigint");
        f.put("total", "decimal(12,3)");
        f.put("billing", Collections.singletonMap("email", "string"));
        f.put("line_items", "array<map<string,string>>");
        v.put("schema", Collections.singletonMap("fields", f));
        return v;
    }

    static WooCommerceConfig config() {
        return new WooCommerceConfig(ReadonlyConfig.fromMap(options()));
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "url",
                "consumer_key",
                "consumer_secret",
                "start_date",
                "end_date",
                "page_size",
                "max_pages",
                "decimal_places",
                "max_retries",
                "retry_delay_ms",
                "request_timeout_ms",
                "max_response_bytes"
            })
    void malformedOptionsAreRedacted(String name) {
        Map<String, Object> v = options();
        v.put(name, "private-invalid-value");
        IllegalArgumentException e =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> new WooCommerceConfig(ReadonlyConfig.fromMap(v)));
        assertFalse(e.toString().contains("private-invalid-value"));
        assertNull(e.getCause());
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "http://shop.example",
                "https://user:password@shop.example",
                "https://shop.example?q=x",
                "https://shop.example#x",
                "https://shop.example/a/../b"
            })
    void rejectsUnsafeUrls(String url) {
        Map<String, Object> v = options();
        v.put("url", url);
        assertThrows(
                IllegalArgumentException.class,
                () -> new WooCommerceConfig(ReadonlyConfig.fromMap(v)));
    }

    @Test
    void allPagesPreserveAmountsNestedFieldsAndCompleteOnce() throws Exception {
        CloseableHttpClient http = mock(CloseableHttpClient.class);
        when(http.execute(any(HttpUriRequest.class)))
                .thenAnswer(
                        inv -> {
                            HttpUriRequest request = inv.getArgument(0);
                            assertEquals("GET", request.getMethod());
                            assertEquals(
                                    "Basic "
                                            + Base64.getEncoder()
                                                    .encodeToString(
                                                            (KEY + ":" + SECRET)
                                                                    .getBytes(
                                                                            StandardCharsets
                                                                                    .UTF_8)),
                                    request.getFirstHeader("Authorization").getValue());
                            String query = request.getURI().getRawQuery();
                            assertTrue(query.contains("dates_are_gmt=true"));
                            assertTrue(query.contains("orderby=id"));
                            assertTrue(query.contains("after=2026-01-01T00%3A00%3A00&"));
                            assertTrue(query.contains("before=2026-02-01T00%3A00%3A00&"));
                            assertFalse(query.contains(KEY));
                            assertFalse(query.contains(SECRET));
                            return reply(
                                    200,
                                    "3",
                                    "2",
                                    query.contains("page=1&")
                                            ? "["
                                                    + order(1, "12.345")
                                                    + ","
                                                    + order(2, "0.001")
                                                    + "]"
                                            : "[" + order(3, "999.000") + "]");
                        });
        SingleSplitReaderContext context = mock(SingleSplitReaderContext.class);
        List<SeaTunnelRow> rows = new ArrayList<>();
        try (WooCommerceClient client = new WooCommerceClient(config(), http);
                WooCommerceSourceReader reader = reader(client, context)) {
            Collector<SeaTunnelRow> collector = collector(rows);
            reader.pollNext(collector);
            reader.pollNext(collector);
        }
        assertEquals(3, rows.size());
        assertEquals(new BigDecimal("12.345"), rows.get(0).getField(1));
        assertEquals("buyer@example.test", ((SeaTunnelRow) rows.get(0).getField(2)).getField(0));
        assertEquals("1.234", ((Map<?, ?>[]) rows.get(0).getField(3))[0].get("total"));
        verify(context, times(1)).signalNoMoreElement();
        verify(http, times(2)).execute(any(HttpUriRequest.class));
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "[{",
                "{}",
                "[] []",
                "[{\"id\":1,\"id\":2}]",
                "[{\"id\":0}]",
                "[{\"id\":1.5}]",
                "[{\"id\":9223372036854775808}]"
            })
    void malformedResponseFailsWithoutPII(String body) throws Exception {
        CloseableHttpClient http = mock(CloseableHttpClient.class);
        when(http.execute(any(HttpUriRequest.class))).thenReturn(reply(200, "1", "1", body));
        try (WooCommerceClient client = new WooCommerceClient(config(), http)) {
            HttpConnectorException e =
                    assertThrows(HttpConnectorException.class, () -> client.page(1));
            assertNull(e.getCause());
            assertFalse(e.toString().contains(body));
        }
    }

    @Test
    void missingInconsistentAndExcessiveTotalsFail() throws Exception {
        for (String[] t :
                new String[][] {{null, "1"}, {"3", "1"}, {"2", "2"}, {"10000000", "5000000"}}) {
            CloseableHttpClient http = mock(CloseableHttpClient.class);
            when(http.execute(any(HttpUriRequest.class)))
                    .thenReturn(reply(200, t[0], t[1], "[" + order(1, "1") + "]"));
            try (WooCommerceClient client = new WooCommerceClient(config(), http)) {
                assertThrows(HttpConnectorException.class, () -> client.page(1));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {301, 302, 307, 400, 401, 403, 404})
    void permanentFailuresAreNotRetried(int status) throws Exception {
        CloseableHttpClient http = mock(CloseableHttpClient.class);
        when(http.execute(any(HttpUriRequest.class))).thenReturn(reply(status, null, null, SECRET));
        try (WooCommerceClient client = new WooCommerceClient(config(), http)) {
            HttpConnectorException e =
                    assertThrows(HttpConnectorException.class, () -> client.page(1));
            assertTrue(e.getMessage().contains("HTTP " + status));
            assertNull(e.getCause());
            assertFalse(e.toString().contains(SECRET));
        }
        verify(http, times(1)).execute(any(HttpUriRequest.class));
    }

    @ParameterizedTest
    @ValueSource(ints = {429, 500, 502, 503, 504})
    void transientErrorsRetryBoundedly(int status) throws Exception {
        CloseableHttpClient http = mock(CloseableHttpClient.class);
        when(http.execute(any(HttpUriRequest.class)))
                .thenReturn(reply(status, null, null, ""), reply(200, "0", "0", "[]"));
        try (WooCommerceClient client = new WooCommerceClient(config(), http)) {
            assertEquals(0, client.page(1).total);
        }
        verify(http, times(2)).execute(any(HttpUriRequest.class));
    }

    @Test
    void transportFailureBudgetAndClosedClientAreEnforced() throws Exception {
        CloseableHttpClient http = mock(CloseableHttpClient.class);
        when(http.execute(any(HttpUriRequest.class))).thenThrow(new UnknownHostException(SECRET));
        WooCommerceClient client = new WooCommerceClient(config(), http);
        try {
            HttpConnectorException e =
                    assertThrows(HttpConnectorException.class, () -> client.page(1));
            assertTrue(e.getMessage().contains("budget"));
            assertTrue(e.getMessage().contains("transport: UnknownHostException"));
            assertFalse(e.toString().contains(SECRET));
            assertNull(e.getCause());
            verify(http, times(2)).execute(any(HttpUriRequest.class));
        } finally {
            client.close();
        }
        assertThrows(HttpConnectorException.class, () -> client.page(1));
    }

    @Test
    void changedTotalsOrRepeatedIdsNeverReportCompletion() throws Exception {
        for (String[] last :
                new String[][] {
                    {"4", "[" + order(3, "1") + "," + order(4, "1") + "]"},
                    {"3", "[" + order(2, "1") + "]"}
                }) {
            CloseableHttpClient http = mock(CloseableHttpClient.class);
            when(http.execute(any(HttpUriRequest.class)))
                    .thenReturn(
                            reply(200, "3", "2", "[" + order(1, "1") + "," + order(2, "1") + "]"),
                            reply(200, last[0], "2", last[1]));
            SingleSplitReaderContext context = mock(SingleSplitReaderContext.class);
            try (WooCommerceClient client = new WooCommerceClient(config(), http);
                    WooCommerceSourceReader reader = reader(client, context)) {
                assertThrows(
                        HttpConnectorException.class,
                        () -> reader.pollNext(collector(new ArrayList<>())));
                verify(context, never()).signalNoMoreElement();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"1.2345", "1000000000.000", "1e20", "not-money"})
    void rejectsUnrepresentableMoneyWithoutRounding(String amount) throws Exception {
        CloseableHttpClient http = mock(CloseableHttpClient.class);
        when(http.execute(any(HttpUriRequest.class)))
                .thenReturn(reply(200, "1", "1", "[" + order(1, amount) + "]"));
        try (WooCommerceClient client = new WooCommerceClient(config(), http);
                WooCommerceSourceReader reader =
                        reader(client, mock(SingleSplitReaderContext.class))) {
            HttpConnectorException e =
                    assertThrows(
                            HttpConnectorException.class,
                            () -> reader.pollNext(collector(new ArrayList<>())));
            assertNull(e.getCause());
            assertFalse(e.toString().contains(amount));
        }
    }

    @Test
    void honorsRetryAfterOrFailsInsteadOfRetryingEarly() {
        assertEquals(2000, WooCommerceClient.retryDelay("2", 1));
        assertEquals(10, WooCommerceClient.retryDelay("Wed, 21 Oct 2015 07:28:00 GMT", 10));
        for (String s : new String[] {"61", "9999999999999999999999", "-1", "bad"}) {
            assertThrows(HttpConnectorException.class, () -> WooCommerceClient.retryDelay(s, 1));
        }
    }

    static String order(int id, String amount) {
        return "{\"id\":"
                + id
                + ",\"total\":\""
                + amount
                + "\",\"billing\":{\"email\":\"buyer@example.test\"},\"line_items\":[{\"id\":7,\"total\":\"1.234\"}]}";
    }

    static CloseableHttpResponse reply(int status, String total, String pages, String body) {
        TestResponse r = new TestResponse(status);
        if (total != null) {
            r.addHeader("X-WP-Total", total);
        }
        if (pages != null) {
            r.addHeader("X-WP-TotalPages", pages);
        }
        r.setEntity(new StringEntity(body, StandardCharsets.UTF_8));
        return r;
    }

    static final class TestResponse extends BasicHttpResponse implements CloseableHttpResponse {
        TestResponse(int status) {
            super(HttpVersion.HTTP_1_1, status, "test");
        }

        @Override
        public void close() {}
    }

    static WooCommerceSourceReader reader(
            WooCommerceClient client, SingleSplitReaderContext context) {
        return new WooCommerceSourceReader(
                config(),
                new WooCommerceSource(ReadonlyConfig.fromMap(options()))
                        .getProducedCatalogTables()
                        .get(0),
                context,
                client);
    }

    static Collector<SeaTunnelRow> collector(List<SeaTunnelRow> rows) {
        return new Collector<SeaTunnelRow>() {
            public void collect(SeaTunnelRow row) {
                rows.add(row);
            }

            public Object getCheckpointLock() {
                return rows;
            }
        };
    }
}
