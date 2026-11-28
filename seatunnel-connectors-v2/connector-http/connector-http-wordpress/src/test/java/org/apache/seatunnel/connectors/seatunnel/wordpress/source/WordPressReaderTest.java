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

package org.apache.seatunnel.connectors.seatunnel.wordpress.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Protects ordered public-post decoding, bounded page validation and cancellation ownership.
 * In-flight HTTP fixtures also verify that checkpoint access does not depend on response arrival.
 */
class WordPressReaderTest {
    private static final Instant START = Instant.parse("2024-01-01T00:00:00Z");
    private static final Instant END = Instant.parse("2024-01-31T23:59:59Z");

    private Function<String, String> reply;
    private int status = 200;
    private HttpServer server;
    private final AtomicInteger calls = new AtomicInteger();
    private final AtomicInteger completions = new AtomicInteger();
    private final SingleSplitReaderContext context =
            new SingleSplitReaderContext(null) {
                @Override
                public void signalNoMoreElement() {
                    completions.incrementAndGet();
                }
            };
    private final List<SeaTunnelRow> rows = new ArrayList<>();
    private final Collector<SeaTunnelRow> collector =
            new Collector<SeaTunnelRow>() {
                @Override
                public void collect(SeaTunnelRow record) {
                    rows.add(record);
                }

                @Override
                public Object getCheckpointLock() {
                    return this;
                }
            };

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/posts/",
                exchange -> {
                    calls.incrementAndGet();
                    if (!"GET".equals(exchange.getRequestMethod())
                            || exchange.getRequestHeaders().containsKey("Authorization")) {
                        exchange.sendResponseHeaders(400, -1);
                        exchange.close();
                        return;
                    }
                    byte[] bytes =
                            reply.apply(exchange.getRequestURI().getQuery())
                                    .getBytes(StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(status, bytes.length);
                    try (java.io.OutputStream out = exchange.getResponseBody()) {
                        out.write(bytes);
                    }
                });
        server.start();
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    @Test
    void pagesByIdAndNormalizesTimestamps() throws Exception {
        reply =
                query -> {
                    for (String parameter :
                            new String[] {
                                "number=1",
                                "order_by=ID",
                                "order=ASC",
                                "status=publish",
                                "type=post",
                                "after=2024-01-01T00:00:00Z",
                                "before=2024-01-31T23:59:59Z",
                                "fields=ID,slug,title,URL,date,modified"
                            }) {
                        if (!query.contains(parameter)) {
                            return "{}";
                        }
                    }
                    return page(2, post(query.contains("offset=0") ? 7 : 11));
                };
        try (WordPressReader reader = reader(1, 2)) {
            reader.open();
            reader.pollNext(collector);
            assertEquals(1, calls.get());
            assertEquals(1, rows.size());
            assertEquals(0, completions.get());
            assertNull(reader.snapshotState(1).get(0).getState());
            reader.pollNext(collector);
            assertNull(reader.snapshotState(2).get(0).getState());
            reader.pollNext(collector);
        }
        assertEquals(2, calls.get());
        assertEquals(2, rows.size());
        assertEquals(7L, rows.get(0).getField(0));
        assertEquals(11L, rows.get(1).getField(0));
        assertEquals("sample", rows.get(0).getField(1));
        assertEquals("A &amp; B", rows.get(0).getField(2));
        assertEquals(LocalDateTime.of(2024, 1, 1, 0, 0), rows.get(0).getField(4));
        assertEquals(LocalDateTime.of(2024, 1, 2, 12, 30), rows.get(0).getField(5));
        assertEquals("posts", rows.get(0).getTableId());
        assertEquals(1, completions.get());
    }

    @Test
    void completesAnEmptyResultInOneRequest() throws Exception {
        reply = query -> page(0, "");
        try (WordPressReader reader = reader(1, 3)) {
            reader.open();
            reader.pollNext(collector);
        }
        assertEquals(1, calls.get());
        assertTrue(rows.isEmpty());
        assertEquals(1, completions.get());
    }

    @Test
    void yieldsBetweenValidatedRowsWithoutChangingNullSplitReplay() throws Exception {
        reply = query -> page(2, post(7) + "," + post(11));
        try (WordPressReader reader = reader(2, 1)) {
            reader.open();
            reader.pollNext(collector);
            assertEquals(1, calls.get());
            assertEquals(1, rows.size());
            assertEquals(7L, rows.get(0).getField(0));
            assertEquals(0, completions.get());
            assertNull(reader.snapshotState(1).get(0).getState());
            reader.pollNext(collector);
            assertEquals(1, calls.get());
            assertEquals(2, rows.size());
            assertEquals(11L, rows.get(1).getField(0));
            assertEquals(1, completions.get());
            assertNull(reader.snapshotState(2).get(0).getState());
            reader.pollNext(collector);
            assertEquals(1, calls.get());
        }
    }

    @Test
    void failsBeforeEmittingWhenPageLimitWouldTruncate() throws Exception {
        reply = query -> page(2, post(1));
        assertFailure("max_pages", 1, 1);
        assertTrue(rows.isEmpty());
    }

    @Test
    void rejectsChangingTotals() throws Exception {
        reply = query -> query.contains("offset=0") ? page(2, post(1)) : page(3, post(2));
        assertFailure("count changed", 1, 3);
        assertEquals(1, rows.size());
    }

    @Test
    void rejectsDuplicateIdsAcrossPages() throws Exception {
        reply = query -> page(2, post(1));
        assertFailure("strictly increasing", 1, 2);
        assertEquals(1, rows.size());
    }

    @Test
    void rejectsShortNonfinalPage() throws Exception {
        reply = query -> page(3, post(1));
        assertFailure("page size", 2, 2);
        assertTrue(rows.isEmpty());
    }

    @Test
    void rejectsEntirePageBeforeEmittingInvalidRows() throws Exception {
        reply =
                query ->
                        page(
                                2,
                                post(1)
                                        + ","
                                        + post(2).replace(
                                                        "2024-01-01T01:00:00+01:00",
                                                        "2024-02-01T00:00:00Z"));
        assertFailure("outside the requested window", 2, 1);
        assertTrue(rows.isEmpty());
    }

    @Test
    void reportsHttpFailureWithoutResponseContent() throws Exception {
        status = 429;
        reply = query -> "REMOTE RESPONSE NOT FOR LOGS";
        assertFailure("HTTP 429", 1, 2);
        assertEquals(1, calls.get());
    }

    @Test
    void rejectsDuplicateKeysInResponse() throws Exception {
        reply = query -> "{\"found\":0,\"found\":0,\"posts\":[]}";
        assertFailure("parse response", 1, 2);
        assertTrue(rows.isEmpty());
    }

    @Test
    void cancellationBeforeOpenIsSticky() throws Exception {
        try (WordPressReader reader = reader(1, 1)) {
            reader.cancel();
            reader.cancel();
            assertThrows(IllegalStateException.class, reader::open);
        }
        assertEquals(0, calls.get());
    }

    @Test
    void cancellationAfterOpenPreventsHttpDispatch() throws Exception {
        reply = query -> page(0, "");
        try (WordPressReader reader = reader(1, 1)) {
            reader.open();
            reader.cancel();
            IllegalStateException error =
                    assertThrows(IllegalStateException.class, () -> reader.pollNext(collector));
            assertTrue(error.getMessage().contains("was cancelled"), error.getMessage());
        }
        assertEquals(0, calls.get());
        assertTrue(rows.isEmpty());
        assertEquals(0, completions.get());
    }

    @Test
    void cancellationAbortsInFlightHttpWithoutAcquiringTheReaderMonitor() throws Exception {
        CountDownLatch received = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        holdResponse(received, release);

        ExecutorService tasks = Executors.newFixedThreadPool(2);
        try (WordPressReader reader = reader(1, 1)) {
            reader.open();
            try {
                Future<IllegalStateException> poll =
                        tasks.submit(
                                () ->
                                        assertThrows(
                                                IllegalStateException.class,
                                                () -> reader.pollNext(collector)));
                assertTrue(
                        received.await(5, TimeUnit.SECONDS), "HTTP request did not reach fixture");
                // A barrier must acquire the checkpoint monitor while HTTP remains blocked.
                // The null split state preserves full-window replay rather than a new cursor.
                tasks.submit(
                                () -> {
                                    synchronized (collector.getCheckpointLock()) {
                                        assertNull(reader.snapshotState(1).get(0).getState());
                                    }
                                    return null;
                                })
                        .get(5, TimeUnit.SECONDS);
                // Even a caller holding the reader monitor must not hold up cancellation.
                synchronized (reader) {
                    tasks.submit(reader::cancel).get(5, TimeUnit.SECONDS);
                    IllegalStateException error = poll.get(5, TimeUnit.SECONDS);
                    assertTrue(error.getMessage().contains("was cancelled"), error.getMessage());
                }
                assertEquals(1, calls.get());
                assertTrue(rows.isEmpty());
                assertEquals(0, completions.get());
            } finally {
                release.countDown();
            }
        } finally {
            tasks.shutdownNow();
        }
    }

    @Test
    void closeAbortsInFlightHttpAndCannotReopenReader() throws Exception {
        CountDownLatch received = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        holdResponse(received, release);
        ExecutorService tasks = Executors.newFixedThreadPool(2);
        try (WordPressReader reader = reader(1, 1)) {
            reader.open();
            try {
                Future<IllegalStateException> poll =
                        tasks.submit(
                                () ->
                                        assertThrows(
                                                IllegalStateException.class,
                                                () -> reader.pollNext(collector)));
                assertTrue(
                        received.await(5, TimeUnit.SECONDS), "HTTP request did not reach fixture");
                tasks.submit(
                                () -> {
                                    reader.close();
                                    return null;
                                })
                        .get(5, TimeUnit.SECONDS);
                IllegalStateException error = poll.get(5, TimeUnit.SECONDS);
                assertTrue(error.getMessage().contains("was cancelled"), error.getMessage());
                assertThrows(IllegalStateException.class, reader::open);
                assertEquals(1, calls.get());
                assertTrue(rows.isEmpty());
                assertEquals(0, completions.get());
            } finally {
                release.countDown();
            }
        } finally {
            tasks.shutdownNow();
        }
    }

    @Test
    void rejectsNonPublicSiteAddressFormat() {
        Map<String, Object> options = options();
        options.put("site", "example.wordpress.com/../../");
        assertThrows(
                IllegalArgumentException.class,
                () -> new WordPressSource(ReadonlyConfig.fromMap(options)));
    }

    private void holdResponse(CountDownLatch received, CountDownLatch release) {
        reply =
                query -> {
                    received.countDown();
                    try {
                        if (!release.await(20, TimeUnit.SECONDS)) {
                            throw new AssertionError("Timed out waiting to release HTTP fixture");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(e);
                    }
                    return page(1, post(7));
                };
    }

    private void assertFailure(String expected, int pageSize, int maxPages) throws Exception {
        try (WordPressReader reader = reader(pageSize, maxPages)) {
            reader.open();
            IllegalStateException exception =
                    assertThrows(
                            IllegalStateException.class,
                            () -> {
                                for (int page = 0; page <= maxPages; page++) {
                                    reader.pollNext(collector);
                                }
                            });
            assertTrue(exception.getMessage().contains(expected), exception.getMessage());
        }
        assertEquals(0, completions.get());
    }

    private WordPressReader reader(int pageSize, int maxPages) {
        CatalogTable table =
                new WordPressSource(ReadonlyConfig.fromMap(options()))
                        .getProducedCatalogTables()
                        .get(0);
        return new WordPressReader(
                URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/posts/"),
                START,
                END,
                pageSize,
                maxPages,
                table,
                context);
    }

    private static Map<String, Object> options() {
        Map<String, Object> result = new HashMap<>();
        result.put("site", "sample.wordpress.com");
        result.put("start_date", START.toString());
        result.put("end_date", END.toString());
        return result;
    }

    private static String page(int total, String posts) {
        return "{\"found\":" + total + ",\"posts\":[" + posts + "]}";
    }

    private static String post(long id) {
        return "{\"ID\":"
                + id
                + ",\"slug\":\"sample\",\"title\":\"A &amp; B\",\"URL\":\"https://sample.wordpress.com/post/\","
                + "\"date\":\"2024-01-01T01:00:00+01:00\",\"modified\":\"2024-01-02T11:30:00-01:00\"}";
    }
}
