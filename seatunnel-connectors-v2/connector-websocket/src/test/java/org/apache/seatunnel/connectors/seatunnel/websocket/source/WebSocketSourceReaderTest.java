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

package org.apache.seatunnel.connectors.seatunnel.websocket.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.websocket.config.WebSocketMessageFormat;
import org.apache.seatunnel.connectors.seatunnel.websocket.config.WebSocketSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.websocket.exception.WebSocketConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.websocket.exception.WebSocketConnectorException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import okhttp3.Response;
import okhttp3.WebSocket;
import okhttp3.WebSocketListener;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

class WebSocketSourceReaderTest {

    private static final long AWAIT_TIMEOUT_MS = 30_000L;
    private static final int NORMAL_CLOSURE_STATUS = 1000;
    private static final String SUBSCRIBE_MESSAGE = "{\"op\":\"subscribe\"}";
    private static final String SECRET_TOKEN = "super-secret-token";

    private MockWebServer server;
    private final BlockingQueue<String> serverReceived = new LinkedBlockingQueue<>();
    private final BlockingQueue<String> reconnectedServerReceived = new LinkedBlockingQueue<>();

    @AfterEach
    void tearDown() throws IOException {
        if (server != null) {
            server.shutdown();
            server = null;
        }
    }

    @Test
    void shouldEmitJsonRowsAndStopAtMaxRecords() throws Exception {
        String url = startServer("{\"id\":1,\"name\":\"alice\"}", "{\"id\":2,\"name\":\"bob\"}");
        Map<String, Object> configMap = baseConfig(url);
        configMap.put(ConnectorCommonOptions.SCHEMA.key(), jsonSchema());
        configMap.put(WebSocketSourceOptions.MAX_RECORDS.key(), 2);

        SourceReader.Context context = mockContext(Boundedness.BOUNDED);
        AtomicBoolean noMoreElement = trackNoMoreElement(context);
        TestCollector collector = new TestCollector();
        try (AbstractSingleSplitReader<SeaTunnelRow> reader = createReader(configMap, context)) {
            reader.open();
            pollUntil(reader, collector, noMoreElement::get);
        }

        Assertions.assertTrue(noMoreElement.get(), "Reader must signal the end of the stream");
        Assertions.assertEquals(2, collector.rows.size());
        Assertions.assertEquals(1, collector.rows.get(0).getField(0));
        Assertions.assertEquals("alice", collector.rows.get(0).getField(1));
        Assertions.assertEquals(2, collector.rows.get(1).getField(0));
        Assertions.assertEquals("bob", collector.rows.get(1).getField(1));
        Mockito.verify(context, Mockito.times(1)).signalNoMoreElement();
    }

    /** A single JSON array frame must be fanned out into one row per element. */
    @Test
    void shouldFanOutJsonArrayIntoMultipleRows() throws Exception {
        String url = startServer("[{\"id\":1,\"name\":\"alice\"},{\"id\":2,\"name\":\"bob\"}]");
        Map<String, Object> configMap = baseConfig(url);
        configMap.put(ConnectorCommonOptions.SCHEMA.key(), jsonSchema());
        configMap.put(WebSocketSourceOptions.MAX_RECORDS.key(), 2);

        SourceReader.Context context = mockContext(Boundedness.BOUNDED);
        AtomicBoolean noMoreElement = trackNoMoreElement(context);
        TestCollector collector = new TestCollector();
        try (AbstractSingleSplitReader<SeaTunnelRow> reader = createReader(configMap, context)) {
            reader.open();
            pollUntil(reader, collector, noMoreElement::get);
        }

        Assertions.assertEquals(2, collector.rows.size());
        Assertions.assertEquals("alice", collector.rows.get(0).getField(1));
        Assertions.assertEquals("bob", collector.rows.get(1).getField(1));
    }

    /** Without a schema every frame is emitted as it was received, in a single "value" column. */
    @Test
    void shouldEmitWholeMessageWhenNoSchemaConfigured() throws Exception {
        String url = startServer("not a json payload");
        Map<String, Object> configMap = baseConfig(url);
        configMap.put(WebSocketSourceOptions.MAX_RECORDS.key(), 1);

        SourceReader.Context context = mockContext(Boundedness.BOUNDED);
        AtomicBoolean noMoreElement = trackNoMoreElement(context);
        TestCollector collector = new TestCollector();
        try (AbstractSingleSplitReader<SeaTunnelRow> reader = createReader(configMap, context)) {
            reader.open();
            pollUntil(reader, collector, noMoreElement::get);
        }

        Assertions.assertEquals(1, collector.rows.size());
        Assertions.assertEquals(1, collector.rows.get(0).getArity());
        Assertions.assertEquals("not a json payload", collector.rows.get(0).getField(0));
    }

    @Test
    void shouldReadTextFormatWithCustomDelimiter() throws Exception {
        String url = startServer("1|alice");
        Map<String, Object> configMap = baseConfig(url);
        configMap.put(ConnectorCommonOptions.SCHEMA.key(), jsonSchema());
        configMap.put(WebSocketSourceOptions.FORMAT.key(), WebSocketMessageFormat.TEXT);
        configMap.put(WebSocketSourceOptions.FIELD_DELIMITER.key(), "|");
        configMap.put(WebSocketSourceOptions.MAX_RECORDS.key(), 1);

        SourceReader.Context context = mockContext(Boundedness.BOUNDED);
        AtomicBoolean noMoreElement = trackNoMoreElement(context);
        TestCollector collector = new TestCollector();
        try (AbstractSingleSplitReader<SeaTunnelRow> reader = createReader(configMap, context)) {
            reader.open();
            pollUntil(reader, collector, noMoreElement::get);
        }

        Assertions.assertEquals(1, collector.rows.size());
        Assertions.assertEquals(1, collector.rows.get(0).getField(0));
        Assertions.assertEquals("alice", collector.rows.get(0).getField(1));
    }

    /** The subscription messages must reach the server in the configured order. */
    @Test
    void shouldSendOpenMessagesInOrder() throws Exception {
        String url = startServer();
        Map<String, Object> configMap = baseConfig(url);
        configMap.put(
                WebSocketSourceOptions.OPEN_MESSAGES.key(),
                Arrays.asList("{\"op\":\"auth\"}", "{\"op\":\"subscribe\"}"));

        SourceReader.Context context = mockContext(Boundedness.UNBOUNDED);
        TestCollector collector = new TestCollector();
        try (AbstractSingleSplitReader<SeaTunnelRow> reader = createReader(configMap, context)) {
            reader.open();
            Assertions.assertEquals("{\"op\":\"auth\"}", awaitServerMessage());
            Assertions.assertEquals("{\"op\":\"subscribe\"}", awaitServerMessage());
            // an unbounded reader must never signal the end of the stream
            reader.pollNext(collector);
            Mockito.verify(context, Mockito.never()).signalNoMoreElement();
        }
    }

    /** In batch mode an idle connection must end the read once read_timeout_ms has elapsed. */
    @Test
    void shouldStopWhenIdleLongerThanReadTimeout() throws Exception {
        String url = startServer("{\"id\":1,\"name\":\"alice\"}");
        Map<String, Object> configMap = baseConfig(url);
        configMap.put(ConnectorCommonOptions.SCHEMA.key(), jsonSchema());
        configMap.put(WebSocketSourceOptions.READ_TIMEOUT_MS.key(), 300);

        SourceReader.Context context = mockContext(Boundedness.BOUNDED);
        AtomicBoolean noMoreElement = trackNoMoreElement(context);
        TestCollector collector = new TestCollector();
        try (AbstractSingleSplitReader<SeaTunnelRow> reader = createReader(configMap, context)) {
            reader.open();
            pollUntil(reader, collector, noMoreElement::get);
        }

        Assertions.assertTrue(noMoreElement.get(), "Idle timeout must end the read");
        Assertions.assertEquals(1, collector.rows.size());
        Assertions.assertEquals("alice", collector.rows.get(0).getField(1));
    }

    /**
     * A connection dropped by the server must be re-established, and the subscription messages must
     * be sent again on the new connection: a server that never receives the subscription again
     * would never push anything to the reconnected client.
     */
    @Test
    void shouldResendOpenMessagesAfterReconnect() throws Exception {
        String url =
                startServerWith(
                        connectionDroppingListener(),
                        subscriptionDrivenListener("{\"id\":1,\"name\":\"alice\"}"));
        Map<String, Object> configMap = baseConfig(url);
        configMap.put(ConnectorCommonOptions.SCHEMA.key(), jsonSchema());
        configMap.put(
                WebSocketSourceOptions.OPEN_MESSAGES.key(),
                Collections.singletonList(SUBSCRIBE_MESSAGE));
        configMap.put(WebSocketSourceOptions.ENABLE_RECONNECT.key(), true);
        configMap.put(WebSocketSourceOptions.RECONNECT_INTERVAL_MS.key(), 100);
        configMap.put(WebSocketSourceOptions.MAX_RECORDS.key(), 1);

        SourceReader.Context context = mockContext(Boundedness.BOUNDED);
        AtomicBoolean noMoreElement = trackNoMoreElement(context);
        TestCollector collector = new TestCollector();
        try (AbstractSingleSplitReader<SeaTunnelRow> reader = createReader(configMap, context)) {
            reader.open();
            pollUntil(reader, collector, noMoreElement::get);
        }

        Assertions.assertEquals(
                SUBSCRIBE_MESSAGE,
                reconnectedServerReceived.poll(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS),
                "The subscription must be sent again on the reconnected connection");
        Assertions.assertEquals(1, collector.rows.size());
        Assertions.assertEquals("alice", collector.rows.get(0).getField(1));
    }

    /**
     * Once the reconnect budget is exhausted the reader must fail the task instead of waiting for
     * messages that can never arrive. The failure message reaches job logs and alerts, so it must
     * identify the server without leaking the credentials that such endpoints commonly carry in the
     * query string of the url.
     */
    @Test
    void shouldFailTheTaskWhenReconnectBudgetIsExhausted() throws Exception {
        String url = startServerWith(connectionDroppingListener()) + "?token=" + SECRET_TOKEN;
        Map<String, Object> configMap = baseConfig(url);
        configMap.put(WebSocketSourceOptions.ENABLE_RECONNECT.key(), true);
        // the very first disconnection already exhausts the budget
        configMap.put(WebSocketSourceOptions.MAX_RECONNECT_TIMES.key(), 0);

        SourceReader.Context context = mockContext(Boundedness.UNBOUNDED);
        TestCollector collector = new TestCollector();
        try (AbstractSingleSplitReader<SeaTunnelRow> reader = createReader(configMap, context)) {
            reader.open();
            WebSocketConnectorException exception = awaitPollFailure(reader, collector);
            Assertions.assertEquals(
                    WebSocketConnectorErrorCode.CONNECT_FAILED, exception.getSeaTunnelErrorCode());
            Assertions.assertFalse(
                    exception.getMessage().contains(SECRET_TOKEN),
                    "The failure message must not leak the url credentials: "
                            + exception.getMessage());
            Assertions.assertTrue(
                    exception.getMessage().contains(server.getHostName()),
                    "The failure message must still identify the server: "
                            + exception.getMessage());
        }
        Mockito.verify(context, Mockito.never()).signalNoMoreElement();
    }

    private String startServer(String... messagesToPush) throws IOException {
        return startServerWith(
                new WebSocketListener() {
                    @Override
                    public void onOpen(WebSocket webSocket, Response response) {
                        for (String message : messagesToPush) {
                            webSocket.send(message);
                        }
                    }

                    @Override
                    public void onMessage(WebSocket webSocket, String text) {
                        serverReceived.add(text);
                    }

                    @Override
                    public void onClosing(WebSocket webSocket, int code, String reason) {
                        // echo the close frame, otherwise the connection stays half closed
                        // and the server cannot shut down
                        webSocket.close(code, reason);
                    }
                });
    }

    /** Serves one enqueued websocket upgrade per listener, in the given order. */
    private String startServerWith(WebSocketListener... listeners) throws IOException {
        server = new MockWebServer();
        for (WebSocketListener listener : listeners) {
            server.enqueue(new MockResponse().withWebSocketUpgrade(listener));
        }
        server.start();
        return "ws://" + server.getHostName() + ":" + server.getPort() + "/";
    }

    /** A server side that drops the connection right after the handshake. */
    private static WebSocketListener connectionDroppingListener() {
        return new WebSocketListener() {
            @Override
            public void onOpen(WebSocket webSocket, Response response) {
                webSocket.close(NORMAL_CLOSURE_STATUS, "dropped on purpose");
            }
        };
    }

    /**
     * A server side that pushes data only in response to a client message, so that the row the
     * reader ends up with is itself the proof that the subscription was re-sent.
     */
    private WebSocketListener subscriptionDrivenListener(String messageToPush) {
        return new WebSocketListener() {
            @Override
            public void onMessage(WebSocket webSocket, String text) {
                reconnectedServerReceived.add(text);
                webSocket.send(messageToPush);
            }

            @Override
            public void onClosing(WebSocket webSocket, int code, String reason) {
                webSocket.close(code, reason);
            }
        };
    }

    private String awaitServerMessage() throws InterruptedException {
        String message = serverReceived.poll(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        Assertions.assertNotNull(message, "Timed out waiting for a message on the server side");
        return message;
    }

    private static Map<String, Object> baseConfig(String url) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(WebSocketSourceOptions.URL.key(), url);
        // keep every poll short so the tests do not idle for a full second
        configMap.put(WebSocketSourceOptions.POLL_TIMEOUT_MS.key(), 50);
        // a reconnect would consume a mock response that was never enqueued
        configMap.put(WebSocketSourceOptions.ENABLE_RECONNECT.key(), false);
        return configMap;
    }

    private static Map<String, Object> jsonSchema() {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("id", "int");
        fields.put("name", "string");
        return Collections.singletonMap("fields", fields);
    }

    private static SourceReader.Context mockContext(Boundedness boundedness) {
        SourceReader.Context context = Mockito.mock(SourceReader.Context.class);
        Mockito.when(context.getBoundedness()).thenReturn(boundedness);
        return context;
    }

    private static AtomicBoolean trackNoMoreElement(SourceReader.Context context) {
        AtomicBoolean noMoreElement = new AtomicBoolean();
        Mockito.doAnswer(
                        invocation -> {
                            noMoreElement.set(true);
                            return null;
                        })
                .when(context)
                .signalNoMoreElement();
        return noMoreElement;
    }

    private static AbstractSingleSplitReader<SeaTunnelRow> createReader(
            Map<String, Object> configMap, SourceReader.Context context) {
        WebSocketSource source = new WebSocketSource(ReadonlyConfig.fromMap(configMap));
        return source.createReader(new SingleSplitReaderContext(context));
    }

    private static void pollUntil(
            AbstractSingleSplitReader<SeaTunnelRow> reader,
            TestCollector collector,
            BooleanSupplier stopCondition)
            throws Exception {
        long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT_MS;
        while (!stopCondition.getAsBoolean()) {
            Assertions.assertTrue(
                    System.currentTimeMillis() < deadline,
                    "Timed out waiting for the reader to reach its stop condition");
            reader.pollNext(collector);
        }
    }

    private static WebSocketConnectorException awaitPollFailure(
            AbstractSingleSplitReader<SeaTunnelRow> reader, TestCollector collector)
            throws Exception {
        long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            try {
                reader.pollNext(collector);
            } catch (WebSocketConnectorException expected) {
                return expected;
            }
        }
        return Assertions.fail("Timed out waiting for the reader to fail the task");
    }

    private static class TestCollector implements Collector<SeaTunnelRow> {
        private final Object lock = new Object();
        private final List<SeaTunnelRow> rows = new ArrayList<>();

        @Override
        public void collect(SeaTunnelRow row) {
            rows.add(row);
        }

        @Override
        public Object getCheckpointLock() {
            return lock;
        }
    }
}
