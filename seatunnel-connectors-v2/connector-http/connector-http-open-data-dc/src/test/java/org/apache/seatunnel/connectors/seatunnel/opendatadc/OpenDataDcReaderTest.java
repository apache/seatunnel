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

package org.apache.seatunnel.connectors.seatunnel.opendatadc;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplit;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.SocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies page commits, checkpoint restore, response budgets and cancellation with local fixtures.
 */
class OpenDataDcReaderTest {
    @Test
    void scansShortPagesAndRestoresCursorWithoutFetchingAnotherUpperBound() throws Exception {
        RecordingContext context = new RecordingContext();
        final List<Long> requests = new ArrayList<>();
        final CrashesApi provider =
                new CrashesApi() {
                    @Override
                    long readUpperBound() {
                        requests.add(-1L);
                        return 19;
                    }

                    @Override
                    JsonNode readPage(long lastId, long bound, int size) throws IOException {
                        requests.add(lastId);
                        assertEquals(19L, bound);
                        assertEquals(2, size);
                        return page(lastId == 0 ? 7 : 19);
                    }
                };
        OpenDataDcReader reader = new OpenDataDcReader(context, provider, 2, 0, "crashes");
        Rows rows = new Rows();
        reader.pollNext(rows);
        OpenDataDcReader restored = new OpenDataDcReader(context, provider, 2, 0, "crashes");
        restored.addSplits(reader.snapshotState(1));
        restored.pollNext(rows);
        restored.pollNext(rows);
        assertEquals(Arrays.asList(-1L, 0L, 7L), requests);
        assertEquals(2, rows.values.size());
        SeaTunnelRow first = rows.values.get(0);
        assertEquals(7L, first.getField(0));
        assertEquals("00123456", first.getField(1));
        assertEquals(LocalDateTime.of(1970, 1, 1, 0, 0, 1), first.getField(2));
        assertEquals("Ward 1", first.getField(3));
        assertEquals(2, first.getField(4));
        assertEquals(38.9D, first.getField(5));
        assertEquals(null, first.getField(6));
        assertEquals(19L, rows.values.get(1).getField(0));
        assertEquals("crashes", first.getTableId());
        assertEquals(1, context.signals);
    }

    @Test
    void checkpointsLastEmittedPageWhileNextPageIsInFlightAndRestoresExactIds() throws Exception {
        CountDownLatch fetchingNextPage = new CountDownLatch(1);
        CountDownLatch finishFetch = new CountDownLatch(1);
        Rows before = new Rows();
        RecordingContext originalContext = new RecordingContext();
        CrashesApi originalApi =
                new CrashesApi() {
                    @Override
                    long readUpperBound() {
                        return 19;
                    }

                    @Override
                    JsonNode readPage(long lastId, long bound, int size) throws IOException {
                        if (lastId == 0) {
                            return page(2, 7);
                        }
                        assertEquals(7L, lastId);
                        fetchingNextPage.countDown();
                        await(finishFetch);
                        return page(12, 19);
                    }
                };
        ExecutorService workers = Executors.newFixedThreadPool(2);
        OpenDataDcReader original =
                new OpenDataDcReader(originalContext, originalApi, 2, 0, "crashes");
        List<SingleSplit> snapshot;
        try {
            original.open();
            original.pollNext(before);
            Future<?> poll =
                    workers.submit(
                            () -> {
                                original.pollNext(before);
                                return null;
                            });
            await(fetchingNextPage);
            // Mirror the checkpoint monitor used by the runtime while deliberately holding the
            // network response. The cursor must still refer to the last committed page.
            Future<List<SingleSplit>> checkpoint =
                    workers.submit(
                            () -> {
                                synchronized (before.getCheckpointLock()) {
                                    return original.snapshotState(42);
                                }
                            });
            snapshot = checkpoint.get(5, TimeUnit.SECONDS);
            ByteBuffer saved = ByteBuffer.wrap(snapshot.get(0).getState());
            assertEquals(19L, saved.getLong());
            assertEquals(7L, saved.getLong());
            assertEquals(2L, saved.getLong());
            original.cancel();
            finishFetch.countDown();
            poll.get(5, TimeUnit.SECONDS);
            assertEquals(Arrays.asList(2L, 7L), ids(before));
            assertEquals(0, originalContext.signals);
        } finally {
            finishFetch.countDown();
            original.close();
            workers.shutdownNow();
        }

        RecordingContext context = new RecordingContext();
        AtomicInteger pages = new AtomicInteger();
        CrashesApi restoredApi =
                new CrashesApi() {
                    @Override
                    long readUpperBound() {
                        throw new AssertionError("Restore must reuse the saved upper bound");
                    }

                    @Override
                    JsonNode readPage(long lastId, long bound, int size) throws IOException {
                        assertEquals(7L, lastId);
                        assertEquals(19L, bound);
                        assertEquals(2, size);
                        pages.incrementAndGet();
                        return page(12, 19);
                    }
                };
        OpenDataDcReader restored = new OpenDataDcReader(context, restoredApi, 2, 0, "crashes");
        try {
            restored.addSplits(snapshot);
            restored.open();
            Rows after = new Rows();
            restored.pollNext(after);
            restored.pollNext(after);
            assertEquals(Arrays.asList(12L, 19L), ids(after));
            assertEquals(1, pages.get());
            assertEquals(1, context.signals);
        } finally {
            restored.close();
        }
    }

    @Test
    void cancelsBeforeOpenAndRepeatedlyAfterCloseWithoutMakingARequest() throws IOException {
        CrashesApi api =
                new CrashesApi() {
                    @Override
                    long readUpperBound() {
                        throw new AssertionError("Cancellation must be sticky before open");
                    }
                };
        OpenDataDcReader reader =
                new OpenDataDcReader(new RecordingContext(), api, 2, 0, "crashes");
        reader.cancel();
        reader.open();
        try {
            reader.pollNext(new Rows());
        } finally {
            reader.close();
        }
        reader.cancel();
        reader.close();
    }

    @Test
    void closeWhileHttpClientPlansRouteDoesNotFailTheReader() throws Exception {
        CountDownLatch planningRoute = new CountDownLatch(1);
        CountDownLatch finishPlanning = new CountDownLatch(1);
        ProxySelector route =
                new ProxySelector() {
                    @Override
                    public List<Proxy> select(URI uri) {
                        planningRoute.countDown();
                        try {
                            await(finishPlanning);
                        } catch (IOException e) {
                            throw new AssertionError(e);
                        }
                        return Collections.singletonList(Proxy.NO_PROXY);
                    }

                    @Override
                    public void connectFailed(URI uri, SocketAddress address, IOException e) {
                        throw new AssertionError("A canceled reader must not connect", e);
                    }
                };
        ExecutorService workers = Executors.newFixedThreadPool(2);
        Rows rows = new Rows();
        RecordingContext context = new RecordingContext();
        OpenDataDcReader reader =
                new OpenDataDcReader(
                        context,
                        new CrashesApi("http://127.0.0.1:1/query", route),
                        2,
                        0,
                        "crashes");
        try {
            Future<?> poll =
                    workers.submit(
                            () -> {
                                reader.pollNext(rows);
                                return null;
                            });
            await(planningRoute);
            workers.submit(
                            () -> {
                                reader.close();
                                return null;
                            })
                    .get(3, TimeUnit.SECONDS);
            finishPlanning.countDown();
            poll.get(3, TimeUnit.SECONDS);
            assertTrue(rows.values.isEmpty());
            assertEquals(0, context.signals);
        } finally {
            finishPlanning.countDown();
            try {
                reader.close();
            } finally {
                workers.shutdownNow();
            }
        }
    }

    @Test
    void abortsAnActiveHttpResponseWithoutWaitingForTheServer() throws Exception {
        CountDownLatch responseStarted = new CountDownLatch(1);
        CountDownLatch finishResponse = new CountDownLatch(1);
        assertCancelsWithoutEmitting(
                responseStarted,
                exchange -> {
                    try {
                        exchange.sendResponseHeaders(200, 0);
                        exchange.getResponseBody()
                                .write("{\"features\":[".getBytes(StandardCharsets.UTF_8));
                        exchange.getResponseBody().flush();
                        responseStarted.countDown();
                        try {
                            finishResponse.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IOException("Interrupted waiting for test fixture", e);
                        }
                    } finally {
                        exchange.close();
                    }
                },
                CrashesApi::new,
                finishResponse::countDown);
    }

    @Test
    void cancelsAnHttp429BackoffWithoutRetrying() throws Exception {
        CountDownLatch inBackoff = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        assertCancelsWithoutEmitting(
                inBackoff,
                exchange -> {
                    try {
                        calls.incrementAndGet();
                        exchange.getResponseHeaders().set("Retry-After", "5");
                        exchange.sendResponseHeaders(429, -1);
                    } finally {
                        exchange.close();
                    }
                },
                endpoint ->
                        new CrashesApi(endpoint) {
                            @Override
                            void awaitRetry(long retryDelay) throws IOException {
                                assertEquals(5000L, retryDelay);
                                inBackoff.countDown();
                                super.awaitRetry(retryDelay);
                            }
                        },
                () -> {});
        assertEquals(1, calls.get());
    }

    @Test
    void stopsAtCapAndShrinksLastRequest() throws IOException {
        RecordingContext context = new RecordingContext();
        final CrashesApi provider =
                new CrashesApi() {
                    @Override
                    long readUpperBound() {
                        return 19;
                    }

                    @Override
                    JsonNode readPage(long lastId, long bound, int size) throws IOException {
                        assertEquals(lastId == 0L ? 2 : 1, size);
                        return page(lastId == 0 ? 7 : 12);
                    }
                };
        OpenDataDcReader reader = new OpenDataDcReader(context, provider, 2, 2, "crashes");
        Rows rows = new Rows();
        reader.pollNext(rows);
        reader.pollNext(rows);
        reader.pollNext(rows);
        assertEquals(2, rows.values.size());
        assertEquals(1, context.signals);
    }

    @Test
    void rejectsDuplicatePageBeforeEmittingAnyRows() throws IOException {
        CrashesApi provider =
                new CrashesApi() {
                    @Override
                    long readUpperBound() {
                        return 19;
                    }

                    @Override
                    JsonNode readPage(long lastId, long bound, int size) throws IOException {
                        return OpenDataDcReaderTest.parse(
                                "{\"features\":[" + feature(7) + "," + feature(7) + "]}");
                    }
                };
        OpenDataDcReader reader = new OpenDataDcReader(null, provider, 2, 0, "crashes");
        Rows rows = new Rows();
        assertThrows(IOException.class, () -> reader.pollNext(rows));
        assertEquals(0, rows.values.size());
    }

    @Test
    void rejectsArcGisErrorAndMalformedContract() {
        assertThrows(IOException.class, () -> parse("{\"error\":{\"code\":400},\"features\":[]}"));
        assertThrows(
                IOException.class,
                () -> parse("{\"features\":{},\"exceededTransferLimit\":false}"));
        assertThrows(
                IOException.class,
                () -> parse("{\"features\":[],\"exceededTransferLimit\":\"true\"}"));
        assertThrows(IOException.class, () -> parse("{\"features\":[]}{\"error\":{\"code\":400}}"));
        assertThrows(IOException.class, () -> parse("{\"features\":[1],\"features\":[]}"));
        assertThrows(
                IOException.class,
                () ->
                        parse(
                                "{\"features\":[],\"extra\":"
                                        + "[".repeat(65)
                                        + "0"
                                        + "]".repeat(65)
                                        + "}"));
        IOException tokenLimit =
                assertThrows(
                        IOException.class,
                        () -> parse("{\"features\":[" + "0,".repeat(200000) + "0]}"));
        assertTrue(tokenLimit.getMessage().contains("token budget"), tokenLimit.getMessage());
    }

    @Test
    void rejectsOversizedChunkedBodyAndHeadersThroughTheHttpClient() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        AtomicInteger calls = new AtomicInteger();
        server.createContext(
                "/body",
                exchange -> {
                    calls.incrementAndGet();
                    try {
                        exchange.sendResponseHeaders(200, 0);
                        exchange.getResponseBody()
                                .write(
                                        ("{\"features\":[],\"padding\":\""
                                                        + "x".repeat(100)
                                                        + "\"}")
                                                .getBytes(StandardCharsets.UTF_8));
                    } finally {
                        exchange.close();
                    }
                });
        server.createContext(
                "/headers",
                exchange -> {
                    calls.incrementAndGet();
                    try {
                        if (exchange.getRequestURI().getPath().endsWith("/many")) {
                            for (int i = 0; i < 65; i++) {
                                exchange.getResponseHeaders().set("X-Header-" + i, "value");
                            }
                        } else {
                            exchange.getResponseHeaders().set("X-Oversized", "x".repeat(9000));
                        }
                        byte[] body = "{\"features\":[]}".getBytes(StandardCharsets.UTF_8);
                        exchange.sendResponseHeaders(200, body.length);
                        exchange.getResponseBody().write(body);
                    } finally {
                        exchange.close();
                    }
                });
        server.start();
        try {
            for (String path : Arrays.asList("body", "headers", "headers/many")) {
                try (CrashesApi api =
                        new CrashesApi(
                                "http://127.0.0.1:" + server.getAddress().getPort() + "/" + path,
                                ProxySelector.getDefault(),
                                64,
                                5000)) {
                    IOException error = assertThrows(IOException.class, api::readUpperBound);
                    if ("body".equals(path)) {
                        assertTrue(error.getMessage().contains("byte budget"), error.getMessage());
                    }
                }
            }
            assertEquals(3, calls.get());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void abortsAContinuouslyDrippingBodyAtTheElapsedDeadline() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch completedBody = new CountDownLatch(1);
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/query",
                exchange -> {
                    try {
                        exchange.sendResponseHeaders(200, 0);
                        exchange.getResponseBody()
                                .write("{\"features\":[".getBytes(StandardCharsets.UTF_8));
                        exchange.getResponseBody().flush();
                        started.countDown();
                        while (!release.await(20, TimeUnit.MILLISECONDS)) {
                            // Each read remains active, so a socket-idle timeout cannot help.
                            exchange.getResponseBody().write(' ');
                            exchange.getResponseBody().flush();
                        }
                        exchange.getResponseBody().write("]}".getBytes(StandardCharsets.UTF_8));
                        completedBody.countDown();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException(e);
                    } finally {
                        exchange.close();
                    }
                });
        server.start();
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try (CrashesApi api =
                new CrashesApi(
                        "http://127.0.0.1:" + server.getAddress().getPort() + "/query",
                        ProxySelector.getDefault(),
                        1024,
                        1000)) {
            Future<IOException> error =
                    worker.submit(() -> assertThrows(IOException.class, api::readUpperBound));
            await(started);
            IOException failure = error.get(3, TimeUnit.SECONDS);
            assertTrue(
                    failure.getMessage().contains("elapsed request deadline"),
                    failure.getMessage());
            assertEquals(1L, completedBody.getCount());
            assertTrue(!api.isCancelled(), "A deadline must fail the GET, not cancel the source");
        } finally {
            release.countDown();
            server.stop(0);
            worker.shutdownNow();
        }
    }

    @Test
    void honorsValidRetryAfterWithoutRetryingBeforeLongDeadlines() throws IOException {
        Instant now = Instant.parse("2026-11-21T08:00:00Z");
        assertEquals(0L, CrashesApi.retryDelayMillis("0", now, 500));
        assertEquals(2000L, CrashesApi.retryDelayMillis("2", now, 500));
        assertEquals(500L, CrashesApi.retryDelayMillis(null, now, 500));
        assertEquals(500L, CrashesApi.retryDelayMillis("bad", now, 500));
        assertEquals(2000L, CrashesApi.retryDelayMillis("Sat, 21 Nov 2026 08:00:02 GMT", now, 500));
        assertThrows(IOException.class, () -> CrashesApi.retryDelayMillis("6", now, 500));
        assertThrows(
                IOException.class,
                () -> CrashesApi.retryDelayMillis("999999999999999999999999999999", now, 500));
        assertThrows(
                IOException.class,
                () -> CrashesApi.retryDelayMillis("Sat, 21 Nov 2026 08:00:06 GMT", now, 500));
    }

    private static void assertCancelsWithoutEmitting(
            CountDownLatch atCancellationPoint,
            HttpHandler handler,
            Function<String, CrashesApi> createApi,
            Runnable releaseFixture)
            throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/query", handler);
        server.start();
        ExecutorService workers = Executors.newFixedThreadPool(2);
        RecordingContext context = new RecordingContext();
        OpenDataDcReader reader =
                new OpenDataDcReader(
                        context,
                        createApi.apply(
                                "http://127.0.0.1:" + server.getAddress().getPort() + "/query"),
                        2,
                        0,
                        "crashes");
        Rows rows = new Rows();
        try {
            reader.open();
            Future<?> poll =
                    workers.submit(
                            () -> {
                                reader.pollNext(rows);
                                return null;
                            });
            await(atCancellationPoint);
            workers.submit(reader::cancel).get(3, TimeUnit.SECONDS);
            poll.get(3, TimeUnit.SECONDS);
            assertTrue(rows.values.isEmpty());
            assertEquals(0, context.signals);
        } finally {
            releaseFixture.run();
            try {
                reader.close();
            } finally {
                server.stop(0);
                workers.shutdownNow();
            }
        }
    }

    private static void await(CountDownLatch latch) throws IOException {
        try {
            assertTrue(latch.await(5, TimeUnit.SECONDS), "Timed out waiting for test fixture");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted waiting for test fixture", e);
        }
    }

    private static List<Long> ids(Rows rows) {
        return rows.values.stream().map(row -> (Long) row.getField(0)).collect(Collectors.toList());
    }

    private static JsonNode page(long first, long second) throws IOException {
        return parse("{\"features\":[" + feature(first) + "," + feature(second) + "]}");
    }

    private static JsonNode page(long id) throws IOException {
        return parse("{\"features\":[" + feature(id) + "]}");
    }

    private static String feature(long id) {
        return "{\"attributes\":{\"OBJECTID\":"
                + id
                + ",\"CCN\":\"00123456\",\"REPORTDATE\":1000,"
                + "\"WARD\":\"Ward 1\",\"TOTAL_VEHICLES\":2,\"LATITUDE\":38.9,\"LONGITUDE\":null}}";
    }

    private static JsonNode parse(String json) throws IOException {
        return CrashesApi.parse(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * Records each end-of-input signal without starting an engine. Tests inspect the counter to
     * distinguish successful completion from cancellation or a rejected page.
     */
    private static class RecordingContext extends SingleSplitReaderContext {
        private int signals;

        private RecordingContext() {
            super(null);
        }

        @Override
        public void signalNoMoreElement() {
            signals++;
        }
    }

    /**
     * Collects emitted rows in encounter order and exposes one checkpoint monitor. Polling and
     * snapshot test threads use that same monitor to verify page/state commit boundaries.
     */
    private static class Rows implements Collector<SeaTunnelRow> {
        private final List<SeaTunnelRow> values = new ArrayList<>();
        private final Object lock = new Object();

        @Override
        public void collect(SeaTunnelRow row) {
            values.add(row);
        }

        @Override
        public Object getCheckpointLock() {
            return lock;
        }
    }
}
