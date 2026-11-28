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

package org.apache.seatunnel.connectors.seatunnel.slack.source;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplit;
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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Real HTTP checks for bounded polling, page handoff, checkpoint restore and owned-resource
 * cleanup.
 */
class SlackSourceReaderTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String ONE_MESSAGE =
            "{\"ok\":true,\"messages\":[{\"ts\":\"1700000000.000001\"}]}";

    @Test
    void restoreResumesEmptyContinuationAndFrozenWindow() throws Exception {
        List<String> queries = Collections.synchronizedList(new ArrayList<>());
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/conversations.history",
                exchange -> {
                    String query = exchange.getRequestURI().getRawQuery();
                    queries.add(query);
                    String body;
                    if (query.contains("cursor=empty")) {
                        body =
                                "{\"ok\":true,\"messages\":[],\"response_metadata\":{\"next_cursor\":\"final\"}}";
                    } else if (query.contains("cursor=final")) {
                        body =
                                "{\"ok\":true,\"messages\":[{\"ts\":\"1700000000.000002\",\"text\":\"世界🚀\",\"metadata\":{\"amount\":1234567890.12345678901}}]}";
                    } else if (query.contains("channel=C2")) {
                        body = "{\"ok\":true,\"messages\":[]}";
                    } else {
                        body =
                                "{\"ok\":true,\"messages\":[{\"ts\":\"1700000000.000001\"}],\"response_metadata\":{\"next_cursor\":\"empty\"}}";
                    }
                    respond(exchange, 200, body);
                });
        server.start();
        try {
            Map<String, Object> options = options(server);
            options.put("channel_ids", Arrays.asList("C1", "C2"));
            RecordingCollector before = new RecordingCollector();
            List<SingleSplit> checkpoint;
            try (SlackSourceReader reader = reader(options, mock(SourceReader.Context.class))) {
                reader.open();
                pollUntilProgress(reader, before, 0, 1, "empty");
                synchronized (before.lock) {
                    checkpoint = reader.snapshotState(1);
                }
                pollUntilProgress(reader, before, 0, 2, "final");
                pollUntilProgress(reader, before, 1, 0, "");
            }
            Assertions.assertEquals("1700000000.000001", before.rows.get(0).getField(1));
            options.put("latest", "1800000000.000000");
            SourceReader.Context context = mock(SourceReader.Context.class);
            RecordingCollector after = new RecordingCollector();
            doAnswer(
                            invocation -> {
                                Assertions.assertFalse(Thread.holdsLock(after.lock));
                                return null;
                            })
                    .when(context)
                    .signalNoMoreElement();
            try (SlackSourceReader reader = reader(options, context)) {
                reader.addSplits(checkpoint);
                reader.open();
                pollUntilProgress(reader, after, 0, 2, "final");
                Assertions.assertTrue(
                        after.rows.isEmpty(), "Empty page must advance without finishing");
                pollUntilProgress(reader, after, 1, 0, "");
                pollUntilProgress(reader, after, 2, 0, "");
                poll(reader, after);
                verify(context, times(1)).signalNoMoreElement();
            }
            Assertions.assertEquals(1, after.rows.size());
            Assertions.assertArrayEquals(
                    new Object[] {
                        "C1",
                        "1700000000.000002",
                        null,
                        null,
                        "世界🚀",
                        null,
                        null,
                        "{\"ts\":\"1700000000.000002\",\"text\":\"世界🚀\",\"metadata\":{\"amount\":1234567890.12345678901}}"
                    },
                    after.rows.get(0).getFields());
            Assertions.assertEquals(6, queries.size());
            Assertions.assertTrue(queries.get(3).contains("cursor=empty"));
            Assertions.assertEquals(2, before.rows.size());
            Assertions.assertEquals(
                    before.rows.get(1).getField(1),
                    after.rows.get(0).getField(1),
                    "Post-checkpoint rows can replay");
            Assertions.assertTrue(
                    queries.stream().allMatch(query -> query.contains("latest=1700000100.000000")));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void heldResponseYieldsPollsAndEmitsOnePageOnlyAfterRelease() throws Exception {
        CountDownLatch requested = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger requests = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/conversations.history",
                exchange -> {
                    requests.incrementAndGet();
                    requested.countDown();
                    try {
                        release.await(10, TimeUnit.SECONDS);
                        respond(exchange, 200, ONE_MESSAGE);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        exchange.close();
                    }
                });
        server.start();
        RecordingCollector collector = new RecordingCollector();
        try (SlackSourceReader reader = reader(options(server), mock(SourceReader.Context.class))) {
            reader.open();
            JsonNode initial = state(reader);
            boundedPoll(reader, collector);
            Assertions.assertTrue(requested.await(5, TimeUnit.SECONDS));
            for (int i = 0; i < 3; i++) {
                boundedPoll(reader, collector);
            }
            Assertions.assertEquals(1, release.getCount(), "The response must still be held");
            Assertions.assertEquals(
                    1, requests.get(), "Repeated polls must reuse one pending page");
            Assertions.assertTrue(collector.rows.isEmpty());
            Assertions.assertTimeoutPreemptively(
                    Duration.ofSeconds(2),
                    () -> {
                        synchronized (collector.lock) {
                            Assertions.assertEquals(initial, state(reader));
                        }
                    });
            release.countDown();
            pollUntilProgress(reader, collector, 1, 0, "");
            Assertions.assertEquals(1, collector.rows.size());
            Assertions.assertEquals("1700000000.000001", collector.rows.get(0).getField(1));
            boundedPoll(reader, collector);
            boundedPoll(reader, collector);
            Assertions.assertEquals(1, requests.get());
            Assertions.assertEquals(
                    1, collector.rows.size(), "The completed future must be consumed once");
        } finally {
            release.countDown();
            server.stop(0);
        }
    }

    @Test
    void gracefulCloseAndCancelReleaseHeldHttpWithoutServerRelease() throws Exception {
        // Both paths must terminate the owned worker before the fixture is released.
        for (boolean cancel : new boolean[] {false, true}) {
            CountDownLatch requested = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext(
                    "/conversations.history",
                    exchange -> {
                        requested.countDown();
                        try {
                            release.await(10, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } finally {
                            exchange.close();
                        }
                    });
            server.start();
            RecordingCollector collector = new RecordingCollector();
            try (SlackSourceReader reader =
                    reader(options(server), mock(SourceReader.Context.class))) {
                reader.open();
                JsonNode initial = state(reader);
                boundedPoll(reader, collector);
                Assertions.assertTrue(requested.await(5, TimeUnit.SECONDS));
                Assertions.assertTimeoutPreemptively(
                        Duration.ofSeconds(2),
                        () -> {
                            if (cancel) {
                                reader.cancel();
                            } else {
                                reader.close();
                            }
                        });
                Assertions.assertEquals(1, release.getCount());
                reader.open();
                boundedPoll(reader, collector);
                Assertions.assertTrue(collector.rows.isEmpty());
                Assertions.assertEquals(initial, state(reader));
            } finally {
                release.countDown();
                server.stop(0);
            }
        }
    }

    @Test
    void cancelInterruptsRetryWaitWithoutOutputOrProgress() throws Exception {
        CountDownLatch responded = new CountDownLatch(1);
        AtomicInteger requests = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/conversations.history",
                exchange -> {
                    requests.incrementAndGet();
                    exchange.getResponseHeaders().add("Retry-After", "30");
                    respond(exchange, 429, "{}");
                    responded.countDown();
                });
        server.start();
        Map<String, Object> options = options(server);
        options.put("max_retries", 1);
        RecordingCollector collector = new RecordingCollector();
        try (SlackSourceReader reader = reader(options, mock(SourceReader.Context.class))) {
            reader.open();
            JsonNode initial = state(reader);
            boundedPoll(reader, collector);
            Assertions.assertTrue(responded.await(5, TimeUnit.SECONDS));
            for (int i = 0; i < 3; i++) {
                boundedPoll(reader, collector);
            }
            Assertions.assertEquals(1, requests.get());
            Assertions.assertTimeoutPreemptively(Duration.ofSeconds(2), reader::cancel);
            boundedPoll(reader, collector);
            Assertions.assertEquals(
                    1, requests.get(), "Canceled backoff must not start another attempt");
            Assertions.assertTrue(collector.rows.isEmpty());
            Assertions.assertEquals(initial, state(reader));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void stalledAndSlowResponsesExhaustDeadlinesWithoutOutputOrProgress() throws Exception {
        for (boolean slowBody : new boolean[] {false, true}) {
            CountDownLatch release = new CountDownLatch(1);
            AtomicInteger requests = new AtomicInteger();
            ExecutorService serverWorkers = Executors.newFixedThreadPool(2);
            HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.setExecutor(serverWorkers);
            server.createContext(
                    "/conversations.history",
                    exchange -> {
                        requests.incrementAndGet();
                        try {
                            if (slowBody) {
                                byte[] bytes = ONE_MESSAGE.getBytes(StandardCharsets.UTF_8);
                                exchange.sendResponseHeaders(200, bytes.length);
                                try (OutputStream output = exchange.getResponseBody()) {
                                    for (byte value : bytes) {
                                        output.write(value);
                                        output.flush();
                                        // Data arrives well inside socket timeout but beyond the
                                        // attempt deadline.
                                        if (release.await(25, TimeUnit.MILLISECONDS)) {
                                            break;
                                        }
                                    }
                                }
                            } else {
                                release.await(10, TimeUnit.SECONDS);
                            }
                        } catch (IOException e) {
                            // Disconnect is expected when the reader's attempt deadline aborts the
                            // socket.
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } finally {
                            exchange.close();
                        }
                    });
            server.start();
            Map<String, Object> options = options(server);
            options.put("request_timeout_ms", 500);
            options.put("max_retries", 1);
            options.put("retry_backoff_ms", 1);
            RecordingCollector collector = new RecordingCollector();
            try (SlackSourceReader reader = reader(options, mock(SourceReader.Context.class))) {
                reader.open();
                JsonNode initial = state(reader);
                IllegalStateException failure =
                        Assertions.assertTimeoutPreemptively(
                                Duration.ofSeconds(5),
                                () ->
                                        Assertions.assertThrows(
                                                IllegalStateException.class,
                                                () -> {
                                                    while (true) {
                                                        poll(reader, collector);
                                                    }
                                                }));
                Assertions.assertTrue(
                        failure.getMessage().contains("HTTP transport retry budget exhausted"));
                Assertions.assertEquals(2, requests.get());
                Assertions.assertTrue(collector.rows.isEmpty());
                Assertions.assertEquals(initial, state(reader));
                Assertions.assertThrows(IllegalStateException.class, () -> poll(reader, collector));
                Assertions.assertEquals(
                        2, requests.get(), "A failed future must not launch a new page");
            } finally {
                release.countDown();
                server.stop(0);
                serverWorkers.shutdownNow();
            }
        }
    }

    @Test
    void httpDebugDiagnosticsExcludeBearerCursorAndResponseBody() throws Exception {
        String token = "slack-private-token-83";
        String cursor = "slack-private-cursor-62";
        String responseMarker = "slack-private-response-45";
        AtomicInteger requests = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/conversations.history",
                exchange -> {
                    if (!("Bearer " + token)
                            .equals(exchange.getRequestHeaders().getFirst("Authorization"))) {
                        respond(exchange, 401, "{}");
                        return;
                    }
                    if (requests.incrementAndGet() == 1) {
                        respond(
                                exchange,
                                200,
                                "{\"ok\":true,\"messages\":[],\"response_metadata\":{\"next_cursor\":\""
                                        + cursor
                                        + "\"}}");
                    } else {
                        if (!exchange.getRequestURI().getQuery().contains("cursor=" + cursor)) {
                            respond(exchange, 400, "{}");
                            return;
                        }
                        respond(
                                exchange,
                                200,
                                "{\"ok\":true,\"messages\":[{\"ts\":\"1700000000.000001\",\"text\":\""
                                        + responseMarker
                                        + "\"}]}");
                    }
                });
        LoggerContext logging = (LoggerContext) LogManager.getContext(false);
        Configuration configuration = logging.getConfiguration();
        String category = "org.apache.http";
        LoggerConfig previous = configuration.getLoggers().get(category);
        List<String> messages = new CopyOnWriteArrayList<>();
        AbstractAppender capture =
                new AbstractAppender(
                        "slack-http-log-capture", null, null, true, Property.EMPTY_ARRAY) {
                    @Override
                    public void append(LogEvent event) {
                        messages.add(event.getMessage().getFormattedMessage());
                    }
                };
        capture.start();
        LoggerConfig diagnostic = new LoggerConfig(category, Level.DEBUG, false);
        diagnostic.addAppender(capture, Level.DEBUG, null);
        configuration.removeLogger(category);
        configuration.addLogger(category, diagnostic);
        logging.updateLoggers();
        server.start();
        try {
            Log requestLineLogger =
                    LogFactory.getLog("org.apache.http.impl.execchain.MainClientExec");
            Assertions.assertTrue(requestLineLogger.isDebugEnabled());
            requestLineLogger.debug("slack-transport-debug-control");
            Map<String, Object> options = options(server);
            options.put("token", token);
            RecordingCollector output = new RecordingCollector();
            try (SlackSourceReader reader = reader(options, mock(SourceReader.Context.class))) {
                reader.open();
                pollUntilProgress(reader, output, 0, 1, cursor);
                pollUntilProgress(reader, output, 1, 0, "");
            }
            Assertions.assertEquals(2, requests.get());
            Assertions.assertEquals(1, output.rows.size());
            Assertions.assertEquals(responseMarker, output.rows.get(0).getField(4));
            String debug = String.join("\n", messages);
            Assertions.assertTrue(debug.contains("slack-transport-debug-control"));
            Assertions.assertTrue(
                    debug.contains("Connection request:"),
                    "Real HTTP diagnostics must be captured");
            Assertions.assertFalse(debug.contains(token), "OAuth token leaked in HTTP diagnostics");
            Assertions.assertFalse(
                    debug.contains(cursor), "Continuation leaked in HTTP diagnostics");
            Assertions.assertFalse(debug.contains(responseMarker), "Private message body leaked");
        } finally {
            server.stop(0);
            configuration.removeLogger(category);
            if (previous != null) {
                configuration.addLogger(category, previous);
            }
            logging.updateLoggers();
            capture.stop();
        }
    }

    @Test
    void invalidOptionsFailWithoutExposingToken() {
        Map<String, Object> options = new HashMap<>();
        options.put("token", "secret-token");
        options.put("channel_ids", Collections.singletonList("C1"));
        for (String key :
                Arrays.asList(
                        "page_size",
                        "request_timeout_ms",
                        "max_response_bytes",
                        "max_pages_per_channel",
                        "retry_backoff_ms")) {
            options.put(key, 0);
            IllegalArgumentException error =
                    Assertions.assertThrows(
                            IllegalArgumentException.class,
                            () -> new SlackSourceConfig(ReadonlyConfig.fromMap(options)));
            Assertions.assertTrue(error.getMessage().contains(key));
            Assertions.assertFalse(error.getMessage().contains("secret-token"));
            options.remove(key);
        }
        options.put("oldest", "1.000001");
        options.put("latest", "1.000001");
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new SlackSourceConfig(ReadonlyConfig.fromMap(options)));
    }

    /**
     * Allows expected fixture disconnects after cancellation or deadline expiry. Unexpected
     * transport failures still fail the driving output/error assertions.
     */
    private static void respond(HttpExchange exchange, int status, String body) {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        try {
            exchange.sendResponseHeaders(status, bytes.length);
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(bytes);
            }
        } catch (IOException e) {
            exchange.close();
        }
    }

    /**
     * Reads the serialized checkpoint progress contract without reflection. Tests can assert page
     * boundaries and unchanged state while HTTP is pending.
     */
    private static JsonNode state(SlackSourceReader reader) throws Exception {
        return MAPPER.readTree(reader.snapshotState(1).get(0).getState());
    }

    /**
     * Marks the sole polling owner so collector assertions reject emission from the HTTP worker.
     */
    private static void poll(SlackSourceReader reader, RecordingCollector collector)
            throws Exception {
        collector.pollingThread = Thread.currentThread();
        reader.pollNext(collector);
    }

    /**
     * Bounds each poll independently while a response or backoff remains held. Synchronous network
     * work cannot satisfy this assertion before fixture release.
     */
    private static void boundedPoll(SlackSourceReader reader, RecordingCollector collector) {
        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(2), () -> poll(reader, collector));
    }

    /**
     * Drives asynchronous page handoff until an explicit checkpoint boundary. Recording the polling
     * owner also makes accidental worker emission fail assertions.
     */
    private static void pollUntilProgress(
            SlackSourceReader reader,
            RecordingCollector collector,
            int channel,
            int pages,
            String cursor) {
        Assertions.assertTimeoutPreemptively(
                Duration.ofSeconds(5),
                () -> {
                    while (true) {
                        poll(reader, collector);
                        JsonNode state = state(reader);
                        if (state.path("channel").intValue() == channel
                                && state.path("pages").intValue() == pages
                                && state.path("cursor").textValue().equals(cursor)) {
                            return;
                        }
                    }
                });
    }

    private static Map<String, Object> options(HttpServer server) {
        Map<String, Object> options = new HashMap<>();
        options.put("token", "fixture-token");
        options.put("channel_ids", Collections.singletonList("C1"));
        options.put("api_root", "http://127.0.0.1:" + server.getAddress().getPort() + "/");
        options.put("latest", "1700000100.000000");
        options.put("request_timeout_ms", 10000);
        options.put("max_retries", 0);
        return options;
    }

    private static SlackSourceReader reader(
            Map<String, Object> options, SourceReader.Context context) {
        return new SlackSourceReader(
                new SlackSourceConfig(ReadonlyConfig.fromMap(options)),
                new SingleSplitReaderContext(context));
    }

    /**
     * Captures exact output rows while checking the checkpoint lock. Collection must run on the
     * designated polling thread, never on the HTTP worker.
     */
    private static final class RecordingCollector implements Collector<SeaTunnelRow> {
        private final Object lock = new Object();
        private final List<SeaTunnelRow> rows = new ArrayList<>();
        private volatile Thread pollingThread;

        @Override
        public void collect(SeaTunnelRow row) {
            Assertions.assertTrue(Thread.holdsLock(lock));
            Assertions.assertSame(
                    pollingThread, Thread.currentThread(), "HTTP workers must not emit rows");
            rows.add(row);
        }

        @Override
        public Object getCheckpointLock() {
            return lock;
        }
    }
}
