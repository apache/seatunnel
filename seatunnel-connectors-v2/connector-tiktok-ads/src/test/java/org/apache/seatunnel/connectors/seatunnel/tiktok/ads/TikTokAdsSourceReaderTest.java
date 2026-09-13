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

package org.apache.seatunnel.connectors.seatunnel.tiktok.ads;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.tiktok.ads.TikTokAdsReportTest.bytes;
import static org.apache.seatunnel.connectors.seatunnel.tiktok.ads.TikTokAdsReportTest.options;
import static org.apache.seatunnel.connectors.seatunnel.tiktok.ads.TikTokAdsReportTest.page;
import static org.apache.seatunnel.connectors.seatunnel.tiktok.ads.TikTokAdsReportTest.row;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TikTokAdsSourceReaderTest {
    private HttpServer server;
    private ExecutorService executor;
    private TikTokAdsSourceReader reader;
    private Map<String, Object> settings;
    private AtomicInteger requests;
    private final List<Map<String, String>> queries = new ArrayList<>();
    private SourceReader.Context context;
    private final CountDownLatch release = new CountDownLatch(1);

    @BeforeEach
    void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        executor = Executors.newCachedThreadPool();
        server.setExecutor(executor);
        server.start();
        settings = options();
        settings.put("token", "mock-token");
        settings.put("mock_url", "http://127.0.0.1:" + server.getAddress().getPort());
        requests = new AtomicInteger();
        context = mock(SourceReader.Context.class);
    }

    @AfterEach
    void close() throws Exception {
        release.countDown();
        if (reader != null) {
            reader.close();
        }
        server.stop(0);
        executor.shutdownNow();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }

    private void respond(HttpExchange exchange, int status, String body) throws IOException {
        requests.incrementAndGet();
        assertEquals("GET", exchange.getRequestMethod());
        assertEquals("mock-token", exchange.getRequestHeaders().getFirst("Access-Token"));
        assertEquals(TikTokAdsConfig.PATH, exchange.getRequestURI().getPath());
        synchronized (queries) {
            queries.add(
                    URLEncodedUtils.parse(exchange.getRequestURI(), StandardCharsets.UTF_8).stream()
                            .collect(
                                    Collectors.toMap(
                                            NameValuePair::getName, NameValuePair::getValue)));
        }
        byte[] bytes = bytes(body);
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    private List<SeaTunnelRow> run() throws Exception {
        reader =
                new TikTokAdsSourceReader(
                        new TikTokAdsConfig(ReadonlyConfig.fromMap(settings)),
                        new SingleSplitReaderContext(context));
        reader.open();
        Collector<SeaTunnelRow> output = mock(Collector.class);
        when(output.getCheckpointLock()).thenReturn(new Object());
        List<SeaTunnelRow> rows = new ArrayList<>();
        doAnswer(
                        call -> {
                            rows.add(call.getArgument(0));
                            return null;
                        })
                .when(output)
                .collect(any(SeaTunnelRow.class));
        reader.pollNext(output);
        reader.pollNext(output);
        return rows;
    }

    @Test
    void readsPagesUsingExactApiQueryAndSignalsCompletionOnce() throws Exception {
        server.createContext(
                TikTokAdsConfig.PATH,
                exchange ->
                        respond(
                                exchange,
                                200,
                                exchange.getRequestURI().getQuery().contains("page=1&")
                                        ? page(1, 3, row("100"), row("101"))
                                        : page(2, 3, row("102"))));
        assertEquals(3, run().size());
        assertEquals(2, requests.get());
        assertEquals("[\"ad_id\",\"stat_time_day\"]", queries.get(0).get("dimensions"));
        assertEquals("[\"spend\",\"impressions\",\"clicks\"]", queries.get(0).get("metrics"));
        assertEquals("AUCTION_AD", queries.get(0).get("data_level"));
        assertEquals("BASIC", queries.get(0).get("report_type"));
        assertEquals("REGULAR", queries.get(0).get("query_mode"));
        assertEquals("false", queries.get(0).get("query_lifetime"));
        assertEquals("123456789", queries.get(0).get("advertiser_id"));
        assertEquals("2026-09-01", queries.get(0).get("start_date"));
        assertEquals("2026-09-02", queries.get(0).get("end_date"));
        assertEquals("2", queries.get(1).get("page"));
        assertFalse(queries.toString().contains("mock-token"));
        verify(context).signalNoMoreElement();
    }

    @Test
    void emptyReportCompletesWithoutRows() throws Exception {
        server.createContext(TikTokAdsConfig.PATH, exchange -> respond(exchange, 200, page(1, 0)));
        assertTrue(run().isEmpty());
        verify(context).signalNoMoreElement();
    }

    @ParameterizedTest
    @ValueSource(ints = {429, 500, 502, 503, 504})
    void retriesOnlyDocumentedTransientHttpStatusAndDoesNotRepeatRows(int status) throws Exception {
        server.createContext(
                TikTokAdsConfig.PATH,
                exchange -> {
                    if (requests.get() == 0) {
                        exchange.getResponseHeaders().set("Retry-After", "0");
                        respond(exchange, status, "{\"message\":\"mock-token\"}");
                    } else {
                        respond(exchange, 200, page(1, 1, row("100")));
                    }
                });
        assertEquals(1, run().size());
        assertEquals(2, requests.get());
    }

    @ParameterizedTest
    @ValueSource(ints = {301, 302, 400, 401, 403, 404})
    void failsWithoutFollowingRedirectOrRetryingPermanentErrors(int status) {
        server.createContext(
                TikTokAdsConfig.PATH,
                exchange -> {
                    exchange.getResponseHeaders()
                            .set("Location", settings.get("mock_url") + "/leak");
                    respond(exchange, status, "mock-token");
                });
        AtomicInteger redirected = new AtomicInteger();
        server.createContext(
                "/leak",
                exchange -> {
                    redirected.incrementAndGet();
                    exchange.close();
                });
        IOException error = assertThrows(IOException.class, this::run);
        assertFalse(error.toString().contains("mock-token"));
        assertNull(error.getCause());
        assertEquals(1, requests.get());
        assertEquals(0, redirected.get());
    }

    @Test
    void failsNonzeroApiCodeWithoutRetryEvenOnHttp200() {
        server.createContext(
                TikTokAdsConfig.PATH,
                exchange -> respond(exchange, 200, "{\"code\":40100,\"message\":\"mock-token\"}"));
        IOException error = assertThrows(IOException.class, this::run);
        assertTrue(error.getMessage().contains("40100"));
        assertFalse(error.toString().contains("mock-token"));
        assertEquals(1, requests.get());
        verify(context, never()).signalNoMoreElement();
    }

    @Test
    void rejectsThrottleWarningBeforeEmittingRows() {
        server.createContext(
                TikTokAdsConfig.PATH,
                exchange -> {
                    exchange.getResponseHeaders().set("X-Tt-Ads-Throttle", "over 20k mock-token");
                    respond(exchange, 200, page(1, 1, row("100")));
                });
        IOException error = assertThrows(IOException.class, this::run);
        assertTrue(error.getMessage().contains("X-Tt-Ads-Throttle"));
        assertFalse(error.toString().contains("mock-token"));
        assertEquals(1, requests.get());
    }

    @Test
    void rejectsWarningEvenAfterAnEmptyThrottleHeader() {
        server.createContext(
                TikTokAdsConfig.PATH,
                exchange -> {
                    exchange.getResponseHeaders().add("X-Tt-Ads-Throttle", "");
                    exchange.getResponseHeaders().add("X-Tt-Ads-Throttle", "truncated");
                    respond(exchange, 200, page(1, 1, row("100")));
                });
        assertTrue(
                assertThrows(IOException.class, this::run)
                        .getMessage()
                        .contains("X-Tt-Ads-Throttle"));
    }

    @Test
    void boundsChunkedBodyWithoutContentLength() {
        settings.put("max_response_bytes", 1024);
        server.createContext(
                TikTokAdsConfig.PATH,
                exchange -> {
                    requests.incrementAndGet();
                    exchange.sendResponseHeaders(200, 0);
                    try {
                        exchange.getResponseBody().write(new byte[2048]);
                    } finally {
                        exchange.close();
                    }
                });
        assertTrue(
                assertThrows(IOException.class, this::run)
                        .getMessage()
                        .contains("max_response_bytes"));
        assertEquals(1, requests.get());
    }

    @Test
    void rejectsOversizedBodyWithoutRetry() {
        settings.put("max_response_bytes", 1024);
        server.createContext(
                TikTokAdsConfig.PATH,
                exchange ->
                        respond(
                                exchange,
                                200,
                                String.join("", java.util.Collections.nCopies(300, "mock-token"))));
        assertTrue(
                assertThrows(IOException.class, this::run)
                        .getMessage()
                        .contains("max_response_bytes"));
        assertEquals(1, requests.get());
    }

    @Test
    void rejectsChangedTotalsAndRepeatedPagesWithoutSuccessSignal() {
        server.createContext(
                TikTokAdsConfig.PATH,
                exchange ->
                        respond(
                                exchange,
                                200,
                                requests.get() == 0
                                        ? page(1, 3, row("100"), row("101"))
                                        : page(2, 4, row("102"), row("103"))));
        assertTrue(
                assertThrows(IOException.class, this::run).getMessage().contains("totals changed"));
        verify(context, never()).signalNoMoreElement();
    }

    @Test
    void stopsAfterConfiguredRetryCount() {
        settings.put("max_retries", 1);
        server.createContext(
                TikTokAdsConfig.PATH, exchange -> respond(exchange, 503, "mock-token"));
        assertThrows(IOException.class, this::run);
        assertEquals(2, requests.get());
    }

    @Test
    void boundsRetryAfterWithoutLeakingHeader() {
        assertTrue(
                assertThrows(
                                IOException.class,
                                () -> TikTokAdsSourceReader.retryDelay("999", 0, 1000))
                        .getMessage()
                        .contains("max_retry_wait_ms"));
        IOException error =
                assertThrows(
                        IOException.class,
                        () -> TikTokAdsSourceReader.retryDelay("mock-token", 0, 1000));
        assertFalse(error.toString().contains("mock-token"));
        assertNull(error.getCause());
    }

    @Test
    void requestDeadlineIncludesResponseBody() throws Exception {
        settings.put("request_timeout_ms", 200);
        CountDownLatch started = new CountDownLatch(1);
        server.createContext(
                TikTokAdsConfig.PATH,
                exchange -> {
                    exchange.sendResponseHeaders(200, 0);
                    exchange.getResponseBody().write('{');
                    exchange.getResponseBody().flush();
                    started.countDown();
                    try {
                        release.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    exchange.close();
                });
        Future<Throwable> pending =
                executor.submit(
                        () -> {
                            try {
                                run();
                                return null;
                            } catch (Throwable e) {
                                return e;
                            }
                        });
        assertTrue(started.await(5, TimeUnit.SECONDS));
        assertTrue(pending.get(3, TimeUnit.SECONDS) instanceof IOException);
    }

    @Test
    void closeAbortsInflightBodyRead() throws Exception {
        settings.put("request_timeout_ms", 30000);
        CountDownLatch started = new CountDownLatch(1);
        server.createContext(
                TikTokAdsConfig.PATH,
                exchange -> {
                    exchange.sendResponseHeaders(200, 0);
                    exchange.getResponseBody().write('{');
                    exchange.getResponseBody().flush();
                    started.countDown();
                    try {
                        release.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    exchange.close();
                });
        Future<Throwable> pending =
                executor.submit(
                        () -> {
                            try {
                                run();
                                return null;
                            } catch (Throwable e) {
                                return e;
                            }
                        });
        assertTrue(started.await(5, TimeUnit.SECONDS));
        reader.close();
        assertTrue(pending.get(3, TimeUnit.SECONDS) instanceof IOException);
        verify(context, never()).signalNoMoreElement();
    }

    @Test
    void closeWakesActualRetryWait() throws Exception {
        settings.put("max_retry_wait_ms", 30000);
        CountDownLatch responseSent = new CountDownLatch(1);
        server.createContext(
                TikTokAdsConfig.PATH,
                exchange -> {
                    exchange.getResponseHeaders().set("Retry-After", "30");
                    respond(exchange, 429, "{}");
                    responseSent.countDown();
                });
        java.util.concurrent.atomic.AtomicReference<Thread> worker =
                new java.util.concurrent.atomic.AtomicReference<>();
        Future<Throwable> pending =
                executor.submit(
                        () -> {
                            worker.set(Thread.currentThread());
                            try {
                                run();
                                return null;
                            } catch (Throwable e) {
                                return e;
                            }
                        });
        assertTrue(responseSent.await(5, TimeUnit.SECONDS));
        org.awaitility.Awaitility.await()
                .atMost(3, TimeUnit.SECONDS)
                .until(
                        () ->
                                Arrays.stream(worker.get().getStackTrace())
                                        .anyMatch(
                                                frame ->
                                                        frame.getMethodName()
                                                                .equals("awaitRetry")));
        reader.close();
        assertTrue(pending.get(3, TimeUnit.SECONDS) instanceof IOException);
        assertEquals(1, requests.get());
    }

    @Test
    void reportDeadlineBoundsRetryWait() {
        settings.put("report_timeout_ms", 200);
        server.createContext(
                TikTokAdsConfig.PATH,
                exchange -> {
                    exchange.getResponseHeaders().set("Retry-After", "30");
                    respond(exchange, 429, "{}");
                });
        assertTrue(
                assertThrows(IOException.class, this::run)
                        .getMessage()
                        .contains("report_timeout_ms"));
    }
}
