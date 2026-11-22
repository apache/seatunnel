/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.matomo;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Test;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okhttp3.mockwebserver.SocketPolicy;

import java.io.IOException;
import java.net.URI;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MatomoSourceTest {
    private static final LocalDate FIRST = LocalDate.of(2026, 6, 1);
    private static final LocalDate SECOND = FIRST.plusDays(1);
    private static final String METRICS = "{\"nb_visits\":25,\"nb_actions\":40}";

    private static MatomoSourceReader reader(URI base, LocalDate end, String token) {
        return new MatomoSourceReader(base, 1, FIRST, end, token, 3000, null, "matomo");
    }

    @Test
    void parsesRangeInOrderAndIgnoresExplicitEmptyDays() throws IOException {
        MatomoSourceReader r = reader(URI.create("http://127.0.0.1"), SECOND, null);
        List<SeaTunnelRow> rows =
                r.decode("{\"" + SECOND + "\":[] ,\"" + FIRST + "\":" + METRICS + "}");
        assertEquals(1, rows.size());
        assertArrayEquals(
                new Object[] {1, FIRST.toString(), 25L, 40L, null}, rows.get(0).getFields());
        assertEquals("matomo", rows.get(0).getTableId());
    }

    @Test
    void rejectsMissingDaysAndApplicationErrorsWithoutRepeatingBody() {
        MatomoSourceReader r = reader(URI.create("http://127.0.0.1"), SECOND, null);
        assertThrows(IOException.class, () -> r.decode("{\"" + FIRST + "\":" + METRICS + "}"));
        IOException e =
                assertThrows(
                        IOException.class,
                        () ->
                                r.decode(
                                        "{\"result\":\"error\",\"message\":\"synthetic-sensitive\"}"));
        assertFalse(e.getMessage().contains("synthetic-sensitive"));
        assertThrows(
                IOException.class,
                () ->
                        reader(URI.create("http://127.0.0.1"), FIRST, null)
                                .decode("{\"nb_visits\":-1,\"nb_actions\":40}"));
        assertThrows(
                IOException.class,
                () ->
                        reader(URI.create("http://127.0.0.1"), FIRST, null)
                                .decode("{\"nb_visits\":9223372036854775808,\"nb_actions\":40}"));
    }

    @Test
    void usesAnonymousGetAndPrivatePostWithoutTokenInUri() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(new MockResponse().setBody(METRICS));
            server.enqueue(new MockResponse().setBody(METRICS));
            server.start();
            MatomoSourceReader publicReader = reader(server.url("/index.php").uri(), FIRST, null);
            MatomoSourceReader formReader =
                    reader(server.url("/index.php").uri(), FIRST, "fake +&token");
            publicReader.open();
            formReader.open();
            try {
                assertEquals(1, publicReader.readRows().size());
                assertEquals(1, formReader.readRows().size());
                RecordedRequest get = server.takeRequest(1, TimeUnit.SECONDS);
                RecordedRequest post = server.takeRequest(1, TimeUnit.SECONDS);
                assertEquals("GET", get.getMethod());
                assertTrue(get.getPath().contains("method=VisitsSummary.get"));
                assertEquals("POST", post.getMethod());
                assertEquals("/index.php", post.getPath());
                assertTrue(post.getBody().readUtf8().contains("token_auth=fake+%2B%26token"));
            } finally {
                publicReader.close();
                formReader.close();
            }
        }
    }

    @Test
    void rejectsRedirectsAndHttpErrors() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(
                    new MockResponse().setResponseCode(302).setHeader("Location", "/another"));
            server.enqueue(new MockResponse().setResponseCode(429));
            server.start();
            MatomoSourceReader r = reader(server.url("/index.php").uri(), FIRST, null);
            r.open();
            try {
                assertTrue(
                        assertThrows(IOException.class, r::readRows).getMessage().contains("302"));
                assertTrue(
                        assertThrows(IOException.class, r::readRows).getMessage().contains("429"));
                assertEquals(2, server.getRequestCount());
            } finally {
                r.close();
            }
        }
    }

    @Test
    void cancellationBeforeOpenIsStickyAndDoesNotSendAnyRequest() throws Exception {
        try (MockWebServer server = new MockWebServer();
                MatomoSourceReader reader = reader(server.url("/index.php").uri(), FIRST, null)) {
            server.enqueue(new MockResponse().setBody(METRICS));
            reader.cancel();
            reader.cancel();
            reader.open();
            assertCancelled(assertThrows(IOException.class, reader::readRows));
            reader.close();
            reader.open();
            assertCancelled(assertThrows(IOException.class, reader::readRows));
            assertEquals(0, server.getRequestCount());
        }
    }

    @Test
    void cancellationAbortsAnInFlightRequest() throws Exception {
        assertStopsInFlight(false);
    }

    @Test
    void closingAbortsAnInFlightRequestAndPreventsReopening() throws Exception {
        assertStopsInFlight(true);
    }

    private static void assertStopsInFlight(boolean close) throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE));
            URI uri = server.url("/index.php").uri();
            MatomoSourceReader reader =
                    new MatomoSourceReader(uri, 1, FIRST, FIRST, null, 60_000, null, "matomo");
            ExecutorService worker = Executors.newSingleThreadExecutor();
            try {
                reader.open();
                Future<List<SeaTunnelRow>> rows = worker.submit(reader::readRows);
                assertNotNull(server.takeRequest(10, TimeUnit.SECONDS));
                if (close) {
                    reader.close();
                } else {
                    reader.cancel();
                }
                assertCancelled(
                        assertThrows(ExecutionException.class, () -> rows.get(5, TimeUnit.SECONDS))
                                .getCause());
                reader.open();
                assertCancelled(assertThrows(IOException.class, reader::readRows));
                assertEquals(1, server.getRequestCount());
            } finally {
                reader.close();
                worker.shutdownNow();
                assertTrue(worker.awaitTermination(5, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    void pollDoesNotHoldCheckpointLockDuringHttpAndEmitsNothingAfterCancellation()
            throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE));
            URI uri = server.url("/index.php").uri();
            MatomoSourceReader reader =
                    new MatomoSourceReader(uri, 1, FIRST, FIRST, null, 60_000, null, "matomo");
            ExecutorService worker = Executors.newFixedThreadPool(2);
            Collector<SeaTunnelRow> collector = rejectingCollector();
            try {
                reader.open();
                Future<?> poll =
                        worker.submit(
                                () -> {
                                    reader.pollNext(collector);
                                    return null;
                                });
                assertNotNull(server.takeRequest(10, TimeUnit.SECONDS));
                Future<?> checkpoint =
                        worker.submit(
                                () -> {
                                    synchronized (collector.getCheckpointLock()) {
                                        return null;
                                    }
                                });
                checkpoint.get(5, TimeUnit.SECONDS);
                reader.cancel();
                poll.get(5, TimeUnit.SECONDS);
            } finally {
                reader.close();
                worker.shutdownNow();
                assertTrue(worker.awaitTermination(5, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    void pollPropagatesNonCancellationHttpErrors() throws Exception {
        try (MockWebServer server = new MockWebServer();
                MatomoSourceReader reader = reader(server.url("/index.php").uri(), FIRST, null)) {
            server.enqueue(new MockResponse().setResponseCode(429));
            reader.open();
            assertTrue(
                    assertThrows(IOException.class, () -> reader.pollNext(rejectingCollector()))
                            .getMessage()
                            .contains("429"));
        }
    }

    private static Collector<SeaTunnelRow> rejectingCollector() {
        return new Collector<SeaTunnelRow>() {
            private final Object checkpointLock = new Object();

            @Override
            public void collect(SeaTunnelRow record) {
                throw new AssertionError("Cancelled or failed reads must not emit rows");
            }

            @Override
            public Object getCheckpointLock() {
                return checkpointLock;
            }
        };
    }

    private static void assertCancelled(Throwable error) {
        assertTrue(error instanceof IOException);
        assertEquals("Matomo source was cancelled", error.getMessage());
    }

    @Test
    void validatesHttpsForPrivateTokensAndDateBounds() {
        Map<String, Object> c = new HashMap<>();
        c.put("url", "http://127.0.0.1/index.php");
        c.put("site_id", 1);
        c.put("start_date", FIRST.toString());
        c.put("end_date", SECOND.toString());
        assertEquals("Matomo", new MatomoSource(ReadonlyConfig.fromMap(c)).getPluginName());
        c.put("token", "fake");
        assertThrows(
                IllegalArgumentException.class, () -> new MatomoSource(ReadonlyConfig.fromMap(c)));
        c.put("url", "https://example.org/index.php");
        c.put("end_date", "2029-12-31");
        assertThrows(
                IllegalArgumentException.class, () -> new MatomoSource(ReadonlyConfig.fromMap(c)));
    }
}
