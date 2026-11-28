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
package org.apache.seatunnel.connectors.seatunnel.discourse;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;

import org.junit.jupiter.api.Test;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okhttp3.mockwebserver.SocketPolicy;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises typed response validation, endpoint/privacy constraints and bounded reader
 * cancellation. Real local HTTP responses distinguish elapsed deadlines from socket inactivity
 * timeouts.
 */
class DiscourseSourceTest {
    @Test
    void emitsCategoriesAndChildrenThenSignalsCompletionOnlyOnce() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(
                    json(
                            "{\"category_list\":{\"categories\":[{\"id\":2,\"name\":\"General\",\"slug\":\"general\","
                                    + "\"topic_count\":3,\"post_count\":10,\"subcategory_list\":[{\"id\":4,\"name\":\"Help\","
                                    + "\"slug\":\"help\",\"parent_category_id\":2}]}]}}"));
            DiscourseSource source = source(server.url("/forum/").toString(), "CATEGORIES");
            TestContext context = new TestContext();
            try (DiscourseSourceReader reader = reader(source, context)) {
                List<SeaTunnelRow> rows = new ArrayList<>();
                Collector<SeaTunnelRow> sink =
                        new Collector<SeaTunnelRow>() {
                            @Override
                            public void collect(SeaTunnelRow row) {
                                rows.add(row);
                            }

                            @Override
                            public Object getCheckpointLock() {
                                return this;
                            }
                        };
                reader.pollNext(sink);
                reader.pollNext(sink);
                assertEquals(2, rows.size());
                assertArrayEquals(
                        new Object[] {2L, "General", "general", null, 3L, 10L},
                        rows.get(0).getFields());
                assertArrayEquals(
                        new Object[] {4L, "Help", "help", 2L, null, null}, rows.get(1).getFields());
                assertEquals(1, context.signals);
                RecordedRequest request = server.takeRequest(1, TimeUnit.SECONDS);
                assertEquals("GET", request.getMethod());
                assertEquals(
                        "/forum/categories.json?include_subcategories=true", request.getPath());
                assertNull(request.getHeader("Api-Key"));
                assertNull(request.getHeader("Api-Username"));
                assertEquals(1, server.getRequestCount());
            }
        }
    }

    @Test
    void readsTypedLatestTopicsButNeverFollowsProviderContinuation() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(
                    json(
                            "{\"topic_list\":{\"more_topics_url\":\"https://example.invalid/never\",\"topics\":["
                                    + "{\"id\":4294967296,\"title\":\"Topic\",\"slug\":\"topic\",\"category_id\":3,"
                                    + "\"created_at\":\"2026-01-01T00:00:00Z\",\"posts_count\":7,\"views\":13,\"closed\":false,\"archived\":true}]}}"));
            try (DiscourseSourceReader reader =
                    reader(
                            source(server.url("/").toString(), "LATEST_TOPICS", "topic_limit", 5),
                            new TestContext())) {
                assertArrayEquals(
                        new Object[] {
                            4294967296L,
                            "Topic",
                            "topic",
                            3L,
                            "2026-01-01T00:00:00Z",
                            null,
                            7L,
                            13L,
                            false,
                            true
                        },
                        reader.fetchRows().get(0).getFields());
                assertEquals(
                        "/latest.json?per_page=5",
                        server.takeRequest(1, TimeUnit.SECONDS).getPath());
                assertEquals(1, server.getRequestCount());
            }
        }
    }

    @Test
    void failsClosedOnRedirectAndNeverExposesErrorBody() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(
                    new MockResponse().setResponseCode(302).setHeader("Location", "/unexpected"));
            server.enqueue(
                    new MockResponse().setResponseCode(429).setBody("private response content"));
            try (DiscourseSourceReader reader =
                    reader(source(server.url("/").toString(), "CATEGORIES"), new TestContext())) {
                assertEquals(
                        "Discourse CATEGORIES request returned HTTP 302",
                        assertThrows(IOException.class, reader::fetchRows).getMessage());
                assertEquals(1, server.getRequestCount());
                assertEquals(
                        "Discourse CATEGORIES request returned HTTP 429",
                        assertThrows(IOException.class, reader::fetchRows).getMessage());
                assertEquals(2, server.getRequestCount());
            }
        }
    }

    @Test
    void rejectsInvalidShapeOrRecordInsteadOfReturningPartialData() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(json("{\"category_list\":{}}"));
            server.enqueue(
                    json(
                            "{\"category_list\":{\"categories\":[{\"id\":1,\"name\":\"valid\",\"slug\":\"valid\"},{\"id\":\"x\"}]}}"));
            server.enqueue(json("{\"category_list\":{\"categories\":[]}}"));
            TestContext context = new TestContext();
            try (DiscourseSourceReader reader =
                    reader(source(server.url("/").toString(), "CATEGORIES"), context)) {
                List<SeaTunnelRow> rows = new ArrayList<>();
                Collector<SeaTunnelRow> out =
                        new Collector<SeaTunnelRow>() {
                            @Override
                            public void collect(SeaTunnelRow row) {
                                rows.add(row);
                            }

                            @Override
                            public Object getCheckpointLock() {
                                return this;
                            }
                        };
                assertTrue(
                        assertThrows(IOException.class, () -> reader.pollNext(out))
                                .getMessage()
                                .contains("categories array"));
                assertTrue(
                        assertThrows(IOException.class, () -> reader.pollNext(out))
                                .getMessage()
                                .contains("invalid type for field id"));
                assertTrue(rows.isEmpty());
                assertEquals(0, context.signals);
                reader.pollNext(out);
                assertEquals(1, context.signals);
            }
        }
    }

    @Test
    void rejectsUnsafeSiteAndAuthenticationSettings() {
        assertThrows(
                IllegalArgumentException.class,
                () -> source("https://x.test/?key=secret", "CATEGORIES"));
        assertThrows(
                IllegalArgumentException.class,
                () -> source("https://user:secret@x.test", "CATEGORIES"));
        assertThrows(
                IllegalArgumentException.class,
                () -> source("https://x.test", "CATEGORIES", "token", "secret"));
        assertThrows(
                IllegalArgumentException.class,
                () -> source("http://x.test", "CATEGORIES", "token", "secret", "username", "bot"));
        assertThrows(
                IllegalArgumentException.class,
                () -> source("https://x.test", "CATEGORIES", "token", "a\r\nb", "username", "bot"));
        assertThrows(
                IllegalArgumentException.class,
                () -> source("https://x.test", "LATEST_TOPICS", "topic_limit", 101));
        assertThrows(
                IllegalArgumentException.class,
                () -> source("https://x.test", "CATEGORIES", "topic_limit", 4));
    }

    @Test
    void cancellationIsStickyAndPreventsNetworkAccess() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            DiscourseSource source = source(server.url("/").toString(), "CATEGORIES");
            try (DiscourseSourceReader reader =
                    (DiscourseSourceReader)
                            source.createReader(new SingleSplitReaderContext(new TestContext()))) {
                reader.cancel();
                reader.open();
                reader.cancel();
                assertEquals(
                        "Discourse source was cancelled",
                        assertThrows(IOException.class, reader::fetchRows).getMessage());
                assertEquals(0, server.getRequestCount());
            }
        }
    }

    @Test
    void checkpointsProgressDuringSlowHttpAndTheElapsedDeadlineStopsTheBody() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(
                    json("{\"category_list\":{\"categories\":[{\"id\":1,\"name\":\"category\",\"slug\":\"category\"}]}}")
                            .throttleBody(1, 50, TimeUnit.MILLISECONDS));
            TestContext context = new TestContext();
            try (DiscourseSourceReader reader = deadlineReader(server, context, 2_000)) {
                List<SeaTunnelRow> rows = new ArrayList<>();
                Collector<SeaTunnelRow> output = collecting(rows);
                ExecutorService executor = Executors.newFixedThreadPool(2);
                try {
                    Future<?> poll =
                            executor.submit(
                                    () -> {
                                        reader.pollNext(output);
                                        return null;
                                    });
                    assertTrue(server.takeRequest(1, TimeUnit.SECONDS) != null);
                    Future<byte[]> checkpoint =
                            executor.submit(
                                    () -> {
                                        synchronized (output.getCheckpointLock()) {
                                            assertEquals(1, reader.snapshotState(7).size());
                                            return reader.snapshotState(7).get(0).getState();
                                        }
                                    });
                    assertNull(checkpoint.get(1, TimeUnit.SECONDS));
                    assertFalse(poll.isDone());
                    assertTrue(rows.isEmpty());
                    assertEquals(0, context.signals);
                    ExecutionException error =
                            assertThrows(
                                    ExecutionException.class, () -> poll.get(8, TimeUnit.SECONDS));
                    assertEquals(
                            "Discourse request exceeded the elapsed fetch timeout",
                            error.getCause().getMessage());
                    synchronized (output.getCheckpointLock()) {
                        assertTrue(rows.isEmpty());
                        assertEquals(0, context.signals);
                    }
                    assertEquals(1, server.getRequestCount());
                    // A deadline ends its fetch only; cleanup must not cancel a later attempt.
                    server.enqueue(json("{\"category_list\":{\"categories\":[]}}"));
                    reader.pollNext(output);
                    assertTrue(rows.isEmpty());
                    assertEquals(1, context.signals);
                    assertEquals(2, server.getRequestCount());
                } finally {
                    executor.shutdownNow();
                }
            }
        }
    }

    @Test
    void cancellationAbortsActiveIoWithoutWaitingForTheElapsedDeadline() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE));
            TestContext context = new TestContext();
            try (DiscourseSourceReader reader = deadlineReader(server, context, 30_000)) {
                List<SeaTunnelRow> rows = new ArrayList<>();
                ExecutorService executor = Executors.newSingleThreadExecutor();
                try {
                    Future<?> poll =
                            executor.submit(
                                    () -> {
                                        reader.pollNext(collecting(rows));
                                        return null;
                                    });
                    assertTrue(server.takeRequest(1, TimeUnit.SECONDS) != null);
                    reader.cancel();
                    ExecutionException error =
                            assertThrows(
                                    ExecutionException.class, () -> poll.get(3, TimeUnit.SECONDS));
                    assertTrue(error.getCause() instanceof IOException);
                    assertTrue(rows.isEmpty());
                    assertEquals(0, context.signals);
                    assertEquals(1, server.getRequestCount());
                } finally {
                    executor.shutdownNow();
                }
            }
        }
    }

    @Test
    void cancellationDuringCollectionStopsBothTheRemainingTailAndSuccessSignal() throws Exception {
        // One row isolates the pre-EOF guard; two rows also require the per-row guard.
        for (int count : new int[] {1, 2}) {
            try (MockWebServer server = new MockWebServer()) {
                String first = "{\"id\":1,\"name\":\"one\",\"slug\":\"one\"}";
                String second = count == 2 ? ",{\"id\":2,\"name\":\"two\",\"slug\":\"two\"}" : "";
                server.enqueue(
                        json("{\"category_list\":{\"categories\":[" + first + second + "]}}"));
                TestContext context = new TestContext();
                try (DiscourseSourceReader reader =
                        reader(source(server.url("/").toString(), "CATEGORIES"), context)) {
                    List<SeaTunnelRow> rows = new ArrayList<>();
                    Collector<SeaTunnelRow> output =
                            new Collector<SeaTunnelRow>() {
                                @Override
                                public void collect(SeaTunnelRow row) {
                                    rows.add(row);
                                    reader.cancel();
                                }

                                @Override
                                public Object getCheckpointLock() {
                                    return this;
                                }
                            };
                    assertEquals(
                            "Discourse source was cancelled",
                            assertThrows(IOException.class, () -> reader.pollNext(output))
                                    .getMessage());
                    assertEquals(1, rows.size());
                    assertEquals(1L, rows.get(0).getField(0));
                    assertEquals(0, context.signals);
                    assertEquals(1, server.getRequestCount());
                }
            }
        }
    }

    @Test
    void httpDebugDiagnosticsExcludeActualApiHeadersAndPrivateResponseBody() throws Exception {
        String key = "discourse-private-api-key-marker";
        String username = "discourse-private-username-marker";
        String body = "discourse-private-category-marker";
        LoggerContext logging = (LoggerContext) LogManager.getContext(false);
        Configuration configuration = logging.getConfiguration();
        List<String> categories =
                Arrays.asList("org.apache.http", "org.apache.http.headers", "org.apache.http.wire");
        Map<String, LoggerConfig> previous = new HashMap<>();
        List<String> messages = new CopyOnWriteArrayList<>();
        AbstractAppender capture =
                new AbstractAppender(
                        "discourse-http-log-capture", null, null, true, Property.EMPTY_ARRAY) {
                    @Override
                    public void append(LogEvent event) {
                        messages.add(event.getMessage().getFormattedMessage());
                    }
                };
        capture.start();
        for (String category : categories) {
            previous.put(category, configuration.getLoggers().get(category));
            LoggerConfig diagnostic = new LoggerConfig(category, Level.DEBUG, false);
            diagnostic.addAppender(capture, Level.DEBUG, null);
            configuration.removeLogger(category);
            configuration.addLogger(category, diagnostic);
        }
        logging.updateLoggers();
        try (MockWebServer server = new MockWebServer()) {
            Log headerLog = LogFactory.getLog("org.apache.http.headers");
            assertTrue(headerLog.isDebugEnabled());
            assertTrue(LogFactory.getLog("org.apache.http.wire").isDebugEnabled());
            headerLog.debug("discourse-http-debug-control");
            server.enqueue(
                    json(
                            "{\"category_list\":{\"categories\":[{\"id\":1,\"name\":\""
                                    + body
                                    + "\",\"slug\":\"private\"}]}}"));
            DiscourseSource source = source(server.url("/").toString(), "CATEGORIES");
            // Only the transport fixture constructs a credentialed loopback reader directly;
            // production source validation still requires HTTPS for credentials.
            try (DiscourseSourceReader reader =
                    new DiscourseSourceReader(
                            URI.create(
                                    server.url("/categories.json?include_subcategories=true")
                                            .toString()),
                            key,
                            username,
                            DiscourseSourceOptions.Resource.CATEGORIES,
                            true,
                            source.getProducedCatalogTables().get(0),
                            new SingleSplitReaderContext(new TestContext()))) {
                reader.open();
                assertEquals(body, reader.fetchRows().get(0).getField(1));
            }
            RecordedRequest request = server.takeRequest(1, TimeUnit.SECONDS);
            assertEquals(key, request.getHeader("Api-Key"));
            assertEquals(username, request.getHeader("Api-Username"));
            assertEquals(1, server.getRequestCount());
            String debug = String.join("\n", messages);
            assertTrue(debug.contains("discourse-http-debug-control"));
            assertTrue(
                    debug.contains("Connection request:"),
                    "Real connection diagnostics must be captured");
            assertFalse(debug.contains(key), "API key leaked in HTTP diagnostics");
            assertFalse(debug.contains(username), "API username leaked in HTTP diagnostics");
            assertFalse(debug.contains(body), "Private response body leaked in HTTP diagnostics");
        } finally {
            for (String category : categories) {
                configuration.removeLogger(category);
                if (previous.get(category) != null) {
                    configuration.addLogger(category, previous.get(category));
                }
            }
            logging.updateLoggers();
            capture.stop();
        }
    }

    @Test
    void malformedHttpDiagnosticsNeverRetainPrivateStatusOrChunkBytes() throws Exception {
        String marker = "discourse-private-framing-marker";
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(new MockResponse().setStatus("HTTP/1.1 invalid " + marker));
            server.enqueue(
                    new MockResponse()
                            .setHeader("Transfer-Encoding", "chunked")
                            .setBody(marker + "\r\n"));
            server.enqueue(
                    json("{\"category_list\":{\"categories\":[]}}")
                            .setHeader("X-Private-Oversized", marker.repeat(1024)));
            try (DiscourseSourceReader reader =
                    reader(source(server.url("/").toString(), "CATEGORIES"), new TestContext())) {
                IOException status = assertThrows(IOException.class, reader::fetchRows);
                assertEquals("Discourse HTTP exchange failed", status.getMessage());
                assertNull(status.getCause());
                assertEquals(0, status.getSuppressed().length);
                IOException chunk = assertThrows(IOException.class, reader::fetchRows);
                assertEquals("Discourse response could not be read", chunk.getMessage());
                assertNull(chunk.getCause());
                assertEquals(0, chunk.getSuppressed().length);
                IOException header = assertThrows(IOException.class, reader::fetchRows);
                assertEquals("Discourse HTTP exchange failed", header.getMessage());
                assertNull(header.getCause());
                assertEquals(0, header.getSuppressed().length);
                assertEquals(3, server.getRequestCount());
            }
        }
    }

    @Test
    void interruptionStopsPollingWithoutClearingTheEngineInterrupt() throws Exception {
        try (MockWebServer server = new MockWebServer();
                DiscourseSourceReader reader =
                        reader(
                                source(server.url("/").toString(), "CATEGORIES"),
                                new TestContext())) {
            try {
                Thread.currentThread().interrupt();
                assertEquals(
                        "Discourse source was cancelled",
                        assertThrows(IOException.class, reader::fetchRows).getMessage());
                assertTrue(Thread.currentThread().isInterrupted());
                assertEquals(0, server.getRequestCount());
            } finally {
                // This test owns the synthetic interrupt and must not pass it to JUnit cleanup.
                Thread.interrupted();
            }
        }
    }

    private static DiscourseSourceReader deadlineReader(
            MockWebServer server, TestContext context, long timeoutMs) {
        DiscourseSource source = source(server.url("/").toString(), "CATEGORIES");
        DiscourseSourceReader reader =
                new DiscourseSourceReader(
                        URI.create(
                                server.url("/categories.json?include_subcategories=true")
                                        .toString()),
                        null,
                        null,
                        DiscourseSourceOptions.Resource.CATEGORIES,
                        true,
                        source.getProducedCatalogTables().get(0),
                        new SingleSplitReaderContext(context),
                        timeoutMs);
        reader.open();
        return reader;
    }

    private static Collector<SeaTunnelRow> collecting(List<SeaTunnelRow> rows) {
        return new Collector<SeaTunnelRow>() {
            @Override
            public void collect(SeaTunnelRow row) {
                rows.add(row);
            }

            @Override
            public Object getCheckpointLock() {
                return this;
            }
        };
    }

    /**
     * Records normal source completion without providing unrelated engine metrics or event
     * services.
     */
    private static final class TestContext implements SourceReader.Context {
        int signals;

        @Override
        public int getIndexOfSubtask() {
            return 0;
        }

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.BOUNDED;
        }

        @Override
        public void signalNoMoreElement() {
            signals++;
        }

        @Override
        public void sendSplitRequest() {}

        @Override
        public void sendSourceEventToEnumerator(SourceEvent sourceEvent) {}

        @Override
        public MetricsContext getMetricsContext() {
            throw new UnsupportedOperationException();
        }

        @Override
        public EventListener getEventListener() {
            throw new UnsupportedOperationException();
        }
    }

    private static DiscourseSourceReader reader(
            DiscourseSource source, SourceReader.Context context) {
        DiscourseSourceReader reader =
                (DiscourseSourceReader) source.createReader(new SingleSplitReaderContext(context));
        reader.open();
        return reader;
    }

    private static DiscourseSource source(String url, String resource, Object... options) {
        Map<String, Object> config = new HashMap<>();
        config.put("base_url", url);
        config.put("resource", resource);
        for (int i = 0; i < options.length; i += 2) {
            config.put((String) options[i], options[i + 1]);
        }
        return new DiscourseSource(ReadonlyConfig.fromMap(config));
    }

    private static MockResponse json(String body) {
        return new MockResponse().setHeader("Content-Type", "application/json").setBody(body);
    }
}
