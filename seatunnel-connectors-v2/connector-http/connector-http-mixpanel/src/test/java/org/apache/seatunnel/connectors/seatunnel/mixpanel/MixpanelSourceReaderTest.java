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

package org.apache.seatunnel.connectors.seatunnel.mixpanel;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplit;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MixpanelSourceReaderTest {
    private static final String EVENT =
            "{\"event\":\"purchase\",\"properties\":{\"time\":1767225600,\"distinct_id\":\"fixture-user\",\"$insert_id\":\"a\"}}";
    private HttpServer server;
    private ExecutorService serverThreads;
    private ExecutorService readerThread;
    private SourceReader.Context context;
    private Map<String, Object> options;
    private final List<MixpanelSourceReader> readers = new ArrayList<>();
    private final AtomicInteger requests = new AtomicInteger();

    @BeforeEach
    void setUp() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        serverThreads = Executors.newCachedThreadPool();
        server.setExecutor(serverThreads);
        readerThread = Executors.newSingleThreadExecutor();
        context = mock(SourceReader.Context.class);
        options = MixpanelSourceFactoryTest.options();
        options.put("base_url", "http://127.0.0.1:" + server.getAddress().getPort());
        options.put("allow_insecure_http", true);
        options.put("retry_backoff_ms", 1);
        options.put("max_retry_backoff_ms", 5);
        options.put("read_timeout_ms", 5000);
        server.start();
    }

    @AfterEach
    void tearDown() {
        readers.forEach(MixpanelSourceReader::close);
        server.stop(0);
        readerThread.shutdownNow();
        serverThreads.shutdownNow();
    }

    private MixpanelSourceReader reader() throws Exception {
        MixpanelSource source = new MixpanelSource(ReadonlyConfig.fromMap(options));
        MixpanelSourceReader reader =
                (MixpanelSourceReader) source.createReader(new SingleSplitReaderContext(context));
        readers.add(reader);
        reader.open();
        return reader;
    }

    @SuppressWarnings("unchecked")
    private Collector<SeaTunnelRow> collector(List<SeaTunnelRow> rows) {
        Collector<SeaTunnelRow> collector = mock(Collector.class);
        when(collector.getCheckpointLock()).thenReturn(new Object());
        doAnswer(
                        call -> {
                            rows.add(call.getArgument(0));
                            return null;
                        })
                .when(collector)
                .collect(any(SeaTunnelRow.class));
        return collector;
    }

    private void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
        if (bytes.length == 0) {
            exchange.close();
            return;
        }
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(bytes);
        }
        exchange.close();
    }

    @Test
    void readsRawEventsAndSendsOnlyRequiredQueryParameters() throws Exception {
        server.createContext(
                "/api/2.0/export",
                exchange -> {
                    requests.incrementAndGet();
                    assertEquals("GET", exchange.getRequestMethod());
                    assertEquals(
                            "project_id=42&from_date=2026-01-01&to_date=2026-01-02",
                            exchange.getRequestURI().getRawQuery());
                    assertEquals(
                            "Basic "
                                    + Base64.getEncoder()
                                            .encodeToString(
                                                    "fixture-user:fixture-password"
                                                            .getBytes(StandardCharsets.UTF_8)),
                            exchange.getRequestHeaders().getFirst("Authorization"));
                    respond(exchange, 200, EVENT + "\r\n" + EVENT);
                });
        List<SeaTunnelRow> rows = new ArrayList<>();
        MixpanelSourceReader reader = reader();
        reader.pollNext(collector(rows));
        reader.pollNext(collector(rows));
        assertEquals(2, rows.size());
        assertEquals("purchase", rows.get(0).getField(0));
        assertTrue(rows.get(0).getField(1).toString().contains("\"$insert_id\":\"a\""));
        assertEquals(1, requests.get());
        verify(context, times(1)).signalNoMoreElement();
    }

    @Test
    void mapsNestedPropertiesAndRawObjectThroughJsonDeserializer() throws Exception {
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("event", "string");
        fields.put("time", "bigint");
        fields.put("id", "string");
        fields.put("raw", "string");
        fields.put("absent", "string");
        options.put("schema", Collections.singletonMap("fields", fields));
        Map<String, String> mapping = new LinkedHashMap<>();
        mapping.put("time", "/properties/time");
        mapping.put("id", "/properties/$insert_id");
        mapping.put("raw", "");
        mapping.put("absent", "/properties/not_present");
        options.put("field_mapping", mapping);
        server.createContext("/api/2.0/export", exchange -> respond(exchange, 200, EVENT + "\n"));
        List<SeaTunnelRow> rows = new ArrayList<>();
        reader().pollNext(collector(rows));
        assertEquals(1767225600L, rows.get(0).getField(1));
        assertEquals("a", rows.get(0).getField(2));
        assertEquals(EVENT, rows.get(0).getField(3));
        assertNull(rows.get(0).getField(4));
    }

    @Test
    void emitsBeforeServerFinishesAndReadsMoreThanOneHundredThousandEvents() throws Exception {
        CountDownLatch firstCollected = new CountDownLatch(1);
        AtomicInteger count = new AtomicInteger();
        byte[] line = (EVENT + "\n").getBytes(StandardCharsets.UTF_8);
        server.createContext(
                "/api/2.0/export",
                exchange -> {
                    requests.incrementAndGet();
                    exchange.sendResponseHeaders(200, 0);
                    try (OutputStream output = exchange.getResponseBody()) {
                        output.write(line);
                        output.flush();
                        try {
                            assertTrue(
                                    firstCollected.await(3, TimeUnit.SECONDS),
                                    "source buffered the whole export");
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        for (int i = 1; i < 100001; i++) {
                            output.write(line);
                        }
                    } finally {
                        exchange.close();
                    }
                });
        Collector<SeaTunnelRow> output = collector(new ArrayList<>());
        doAnswer(
                        call -> {
                            count.incrementAndGet();
                            firstCollected.countDown();
                            return null;
                        })
                .when(output)
                .collect(any(SeaTunnelRow.class));
        reader().pollNext(output);
        assertEquals(100001, count.get());
        assertEquals(1, requests.get());
        verify(context).signalNoMoreElement();
    }

    @Test
    void retriesOnly429And5xxBeforeReadingBody() throws Exception {
        server.createContext(
                "/api/2.0/export",
                exchange -> {
                    int request = requests.incrementAndGet();
                    exchange.getResponseHeaders().set("Retry-After", "999999999999999999999");
                    respond(
                            exchange,
                            request == 1 ? 429 : request == 2 ? 503 : 200,
                            request < 3 ? "private error body" : EVENT + "\n");
                });
        List<SeaTunnelRow> rows = new ArrayList<>();
        reader().pollNext(collector(rows));
        assertEquals(1, rows.size());
        assertEquals(3, requests.get());
        verify(context).signalNoMoreElement();
    }

    @Test
    void rejectsAuthenticationFailureWithoutRetryOrPayloadLeak() throws Exception {
        server.createContext(
                "/api/2.0/export",
                exchange -> {
                    requests.incrementAndGet();
                    respond(exchange, 401, "fixture-password query=private-event");
                });
        IOException error =
                assertThrows(
                        IOException.class, () -> reader().pollNext(collector(new ArrayList<>())));
        assertFalse(error.toString().contains("fixture-password"));
        assertFalse(error.toString().contains("private-event"));
        assertTrue(error.getMessage().contains("401"));
        assertNull(error.getCause());
        assertEquals(0, error.getSuppressed().length);
        assertEquals(1, requests.get());
        verify(context, never()).signalNoMoreElement();
    }

    @Test
    void emptyResponseFixtureWorksWithIndependentJdkClient() throws Exception {
        server.createContext(
                "/api/2.0/export",
                exchange -> {
                    int attempt = requests.incrementAndGet() % 3;
                    respond(
                            exchange,
                            attempt == 1 ? 429 : attempt == 2 ? 503 : 200,
                            attempt == 0 ? EVENT + "\n" : "");
                });
        HttpClient control = HttpClient.newHttpClient();
        HttpRequest request =
                HttpRequest.newBuilder(URI.create(options.get("base_url") + "/api/2.0/export"))
                        .build();
        for (int i = 0; i < 30; i++) {
            HttpResponse<String> response =
                    control.send(request, HttpResponse.BodyHandlers.ofString());
            assertEquals(i % 3 == 0 ? 429 : i % 3 == 1 ? 503 : 200, response.statusCode());
            assertEquals(i % 3 == 2 ? EVENT + "\n" : "", response.body());
        }
        assertEquals(30, requests.get());
    }

    @Test
    void retriesEmpty429And503ResponsesWithinOneReader() throws Exception {
        options.put("max_retries", 2);
        server.createContext(
                "/api/2.0/export",
                exchange -> {
                    int attempt = requests.incrementAndGet();
                    exchange.getResponseHeaders().set("Retry-After", "0");
                    respond(
                            exchange,
                            attempt == 1 ? 429 : attempt == 2 ? 503 : 200,
                            attempt < 3 ? "" : EVENT + "\n");
                });
        List<SeaTunnelRow> rows = new ArrayList<>();
        reader().pollNext(collector(rows));
        assertEquals(3, requests.get());
        assertEquals(1, rows.size());
        verify(context, times(1)).signalNoMoreElement();
    }

    @Test
    void boundsRetriesAndDoesNotFollowRedirects() throws Exception {
        AtomicInteger redirected = new AtomicInteger();
        server.createContext(
                "/leak",
                exchange -> {
                    redirected.incrementAndGet();
                    respond(exchange, 200, EVENT);
                });
        for (int status : new int[] {429, 500, 302, 403}) {
            requests.set(0);
            if (status != 429) {
                server.removeContext("/api/2.0/export");
            }
            server.createContext(
                    "/api/2.0/export",
                    exchange -> {
                        requests.incrementAndGet();
                        exchange.getResponseHeaders()
                                .set(
                                        "Location",
                                        "http://127.0.0.1:"
                                                + server.getAddress().getPort()
                                                + "/leak");
                        respond(exchange, status, "private-event");
                    });
            options.put("max_retries", 2);
            assertThrows(IOException.class, () -> reader().pollNext(collector(new ArrayList<>())));
            assertEquals(status == 429 || status == 500 ? 3 : 1, requests.get());
        }
        assertEquals(0, redirected.get(), "redirect target must receive no request or credentials");
        verify(context, never()).signalNoMoreElement();
    }

    @Test
    void malformedLineAbortsWithoutDrainingAnUnfinishedBody() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        server.createContext(
                "/api/2.0/export",
                exchange -> {
                    requests.incrementAndGet();
                    exchange.sendResponseHeaders(200, 0);
                    try (OutputStream output = exchange.getResponseBody()) {
                        output.write("{invalid\n".getBytes(StandardCharsets.UTF_8));
                        output.flush();
                        release.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        exchange.close();
                    }
                });
        options.put("read_timeout_ms", 600000);
        MixpanelSourceReader reader = reader();
        Future<?> failure =
                readerThread.submit(
                        () -> {
                            IOException error =
                                    assertThrows(
                                            IOException.class,
                                            () -> reader.pollNext(collector(new ArrayList<>())));
                            assertTrue(error.getMessage().contains("decode"));
                            assertNull(error.getCause());
                            assertEquals(0, error.getSuppressed().length);
                        });
        try {
            failure.get(3, TimeUnit.SECONDS);
            assertEquals(1, requests.get());
        } finally {
            release.countDown();
        }
        verify(context, never()).signalNoMoreElement();
    }

    @Test
    void failsOnTrailingInvalidJsonAndReplaysWholeWindowFromState() throws Exception {
        server.createContext(
                "/api/2.0/export",
                exchange -> {
                    int attempt = requests.incrementAndGet();
                    respond(
                            exchange,
                            200,
                            EVENT + "\n" + (attempt == 1 ? "{invalid" : EVENT + "\n"));
                });
        List<SeaTunnelRow> rows = new ArrayList<>();
        MixpanelSourceReader first = reader();
        assertThrows(IOException.class, () -> first.pollNext(collector(rows)));
        assertEquals(1, rows.size());
        assertEquals(1, requests.get(), "partial body must not retry in place");
        verify(context, never()).signalNoMoreElement();
        List<SingleSplit> state = first.snapshotState(1);
        assertNull(state.get(0).getState());
        first.close();
        MixpanelSourceReader restored = reader();
        restored.addSplits(state);
        restored.pollNext(collector(rows));
        assertEquals(3, rows.size(), "replay includes the previously emitted event");
        assertEquals(2, requests.get());
        verify(context, times(1)).signalNoMoreElement();
    }

    @Test
    void rejectsTrailingTokensInvalidShapesAndOversizedLines() throws Exception {
        for (String body :
                new String[] {
                    EVENT + " true\n",
                    "null\n",
                    "[]\n",
                    "{}\n",
                    "\n",
                    EVENT + "\n{\"event\":",
                    EVENT + "\n"
                }) {
            try {
                server.removeContext("/api/2.0/export");
            } catch (IllegalArgumentException ignored) {
                // First fixture has no context yet.
            }
            server.createContext(
                    "/api/2.0/export",
                    exchange -> {
                        requests.incrementAndGet();
                        respond(exchange, 200, body);
                    });
            options.put("max_event_bytes", body.equals(EVENT + "\n") ? 16 : 1048576);
            IOException error =
                    assertThrows(
                            IOException.class,
                            () -> reader().pollNext(collector(new ArrayList<>())));
            assertNull(error.getCause());
            if (body.equals(EVENT + "\n")) {
                assertTrue(error.getMessage().contains("max_event_bytes"));
            }
            assertFalse(error.toString().contains("fixture-user"));
        }
        verify(context, never()).signalNoMoreElement();
    }

    @Test
    void failsInterruptedResponseAfterEmittingPrefixWithoutRetry() throws Exception {
        server.createContext(
                "/api/2.0/export",
                exchange -> {
                    requests.incrementAndGet();
                    byte[] line = (EVENT + "\n").getBytes(StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(200, line.length + 200);
                    try {
                        exchange.getResponseBody().write(line);
                        exchange.getResponseBody().flush();
                    } finally {
                        exchange.close();
                    }
                });
        List<SeaTunnelRow> rows = new ArrayList<>();
        assertThrows(IOException.class, () -> reader().pollNext(collector(rows)));
        assertEquals(1, rows.size());
        assertEquals(1, requests.get());
        verify(context, never()).signalNoMoreElement();
    }

    @Test
    void acceptsEmptyExport() throws Exception {
        server.createContext("/api/2.0/export", exchange -> respond(exchange, 200, ""));
        List<SeaTunnelRow> rows = new ArrayList<>();
        reader().pollNext(collector(rows));
        assertTrue(rows.isEmpty());
        verify(context).signalNoMoreElement();
    }

    @Test
    void cancelAndCheckpointProgressWhileCompletionCallbackIsBlocked() throws Exception {
        server.createContext(
                "/api/2.0/export",
                exchange -> {
                    requests.incrementAndGet();
                    respond(exchange, 200, "");
                });
        CountDownLatch completionEntered = new CountDownLatch(1);
        CountDownLatch releaseCompletion = new CountDownLatch(1);
        CountDownLatch cancelReturned = new CountDownLatch(1);
        CountDownLatch checkpointTaken = new CountDownLatch(1);
        doAnswer(
                        call -> {
                            completionEntered.countDown();
                            assertTrue(releaseCompletion.await(10, TimeUnit.SECONDS));
                            return null;
                        })
                .when(context)
                .signalNoMoreElement();
        MixpanelSourceReader reader = reader();
        Collector<SeaTunnelRow> output = collector(new ArrayList<>());
        Future<?> polling =
                readerThread.submit(
                        () -> {
                            reader.pollNext(output);
                            return null;
                        });
        ExecutorService observers = Executors.newFixedThreadPool(2);
        try {
            assertTrue(completionEntered.await(3, TimeUnit.SECONDS));
            Future<?> cancellation =
                    observers.submit(
                            () -> {
                                reader.cancel();
                                cancelReturned.countDown();
                            });
            Future<?> checkpoint =
                    observers.submit(
                            () -> {
                                synchronized (output.getCheckpointLock()) {
                                    assertNull(reader.snapshotState(9).get(0).getState());
                                    checkpointTaken.countDown();
                                }
                                return null;
                            });
            boolean cancelProgress = cancelReturned.await(1, TimeUnit.SECONDS);
            boolean checkpointProgress = checkpointTaken.await(1, TimeUnit.SECONDS);
            assertTrue(
                    cancelProgress && checkpointProgress,
                    "Blocked completion callback: cancel="
                            + cancelProgress
                            + ", checkpoint="
                            + checkpointProgress);
            cancellation.get(1, TimeUnit.SECONDS);
            checkpoint.get(1, TimeUnit.SECONDS);
        } finally {
            releaseCompletion.countDown();
            try {
                polling.get(3, TimeUnit.SECONDS);
            } finally {
                observers.shutdownNow();
            }
        }
        reader.pollNext(output);
        assertEquals(1, requests.get());
        verify(context, times(1)).signalNoMoreElement();
    }

    @Test
    void cancelAndCloseAbortBlockedBodyAndHeaderReads() throws Exception {
        for (boolean headers : new boolean[] {false, true}) {
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            try {
                server.removeContext("/api/2.0/export");
            } catch (IllegalArgumentException ignored) {
                // First fixture has no context yet.
            }
            server.createContext(
                    "/api/2.0/export",
                    exchange -> {
                        if (headers) {
                            exchange.sendResponseHeaders(200, 0);
                            exchange.getResponseBody()
                                    .write((EVENT + "\n").getBytes(StandardCharsets.UTF_8));
                            exchange.getResponseBody().flush();
                        }
                        entered.countDown();
                        try {
                            release.await(5, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } finally {
                            exchange.close();
                        }
                    });
            options.put("read_timeout_ms", 600000);
            MixpanelSourceReader reader = reader();
            Future<?> running =
                    readerThread.submit(
                            () -> {
                                assertThrows(
                                        IOException.class,
                                        () -> reader.pollNext(collector(new ArrayList<>())));
                            });
            try {
                assertTrue(entered.await(3, TimeUnit.SECONDS));
                if (headers) {
                    reader.close();
                } else {
                    reader.cancel();
                }
                running.get(3, TimeUnit.SECONDS);
                reader.cancel();
                reader.close();
            } finally {
                release.countDown();
            }
        }
        verify(context, never()).signalNoMoreElement();
    }

    @Test
    void cancellationBeforeOpenIsSticky() {
        MixpanelSource source = new MixpanelSource(ReadonlyConfig.fromMap(options));
        MixpanelSourceReader reader =
                (MixpanelSourceReader) source.createReader(new SingleSplitReaderContext(context));
        reader.cancel();
        assertThrows(IOException.class, reader::open);
        assertThrows(IOException.class, () -> reader.pollNext(collector(new ArrayList<>())));
        reader.close();
        verify(context, never()).signalNoMoreElement();
    }

    @Test
    void checkpointProgressesWhileExportBodyIsBlocked() throws Exception {
        CountDownLatch firstCollected = new CountDownLatch(1);
        CountDownLatch releaseBody = new CountDownLatch(1);
        CountDownLatch checkpointTaken = new CountDownLatch(1);
        server.createContext(
                "/api/2.0/export",
                exchange -> {
                    exchange.sendResponseHeaders(200, 0);
                    try (OutputStream output = exchange.getResponseBody()) {
                        output.write((EVENT + "\n").getBytes(StandardCharsets.UTF_8));
                        output.flush();
                        releaseBody.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        exchange.close();
                    }
                });
        List<SeaTunnelRow> rows = new ArrayList<>();
        Collector<SeaTunnelRow> output = collector(rows);
        doAnswer(
                        call -> {
                            rows.add(call.getArgument(0));
                            firstCollected.countDown();
                            return null;
                        })
                .when(output)
                .collect(any(SeaTunnelRow.class));
        MixpanelSourceReader reader = reader();
        Future<?> polling =
                readerThread.submit(
                        () -> {
                            try {
                                reader.pollNext(output);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
        ExecutorService checkpoints = Executors.newSingleThreadExecutor();
        try {
            assertTrue(firstCollected.await(3, TimeUnit.SECONDS));
            Future<?> checkpoint =
                    checkpoints.submit(
                            () -> {
                                synchronized (output.getCheckpointLock()) {
                                    try {
                                        assertNull(reader.snapshotState(7).get(0).getState());
                                        checkpointTaken.countDown();
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                }
                            });
            assertTrue(
                    checkpointTaken.await(1, TimeUnit.SECONDS),
                    "checkpoint must not wait for network EOF or retry backoff");
            checkpoint.get(3, TimeUnit.SECONDS);
            verify(context, never()).signalNoMoreElement();
        } finally {
            releaseBody.countDown();
            polling.get(3, TimeUnit.SECONDS);
            checkpoints.shutdownNow();
        }
        assertEquals(1, rows.size());
        verify(context).signalNoMoreElement();
    }

    @Test
    void cancellationInterruptsBackoff() throws Exception {
        CountDownLatch requested = new CountDownLatch(1);
        options.put("retry_backoff_ms", 60000);
        options.put("max_retry_backoff_ms", 60000);
        server.createContext(
                "/api/2.0/export",
                exchange -> {
                    requests.incrementAndGet();
                    respond(exchange, 429, "rate limit");
                    requested.countDown();
                });
        MixpanelSourceReader reader = reader();
        Future<?> running =
                readerThread.submit(
                        () -> {
                            assertThrows(
                                    IOException.class,
                                    () -> reader.pollNext(collector(new ArrayList<>())));
                        });
        assertTrue(requested.await(3, TimeUnit.SECONDS));
        reader.cancel();
        running.get(3, TimeUnit.SECONDS);
        assertEquals(1, requests.get());
        verify(context, never()).signalNoMoreElement();
    }
}
