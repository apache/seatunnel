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
import org.apache.seatunnel.connectors.seatunnel.websocket.config.WebSocketSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.websocket.config.WebSocketSourceOptions;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import okhttp3.Response;
import okhttp3.WebSocket;
import okhttp3.WebSocketListener;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

class WebSocketSourceClientTest {

    private static final long AWAIT_TIMEOUT_MS = 30_000L;
    private static final long POLL_INTERVAL_MS = 10L;
    private static final int QUEUE_CAPACITY = 1;
    private static final int PUSHED_MESSAGES = 20;

    private MockWebServer server;

    @AfterEach
    void tearDown() throws IOException {
        if (server != null) {
            server.shutdown();
            server = null;
        }
    }

    /**
     * A queue that nobody drains any more must not pin the websocket callback thread: the thread
     * has to observe the closed flag and terminate instead of blocking on the queue forever.
     */
    @Test
    void shouldReleaseCallbackThreadWhenClosedWhileQueueIsFull() throws Exception {
        String url = startServer(PUSHED_MESSAGES);
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(WebSocketSourceOptions.URL.key(), url);
        configMap.put(WebSocketSourceOptions.QUEUE_CAPACITY.key(), QUEUE_CAPACITY);
        // a reconnect would consume a mock response that was never enqueued
        configMap.put(WebSocketSourceOptions.ENABLE_RECONNECT.key(), false);

        WebSocketSourceClient client =
                new WebSocketSourceClient(
                        new WebSocketSourceConfig(ReadonlyConfig.fromMap(configMap)));
        client.start();

        // nothing polls the client, so the callback thread must end up waiting inside enqueue():
        // this is also the assertion that the queue really back-pressures the server
        Thread callbackThread = awaitThreadWaitingInEnqueue();

        client.close();

        callbackThread.join(AWAIT_TIMEOUT_MS);
        Assertions.assertFalse(
                callbackThread.isAlive(),
                "The websocket callback thread is still waiting on the full queue after close(), "
                        + "the thread leaked");
    }

    /**
     * Looks for the thread that is currently inside {@link WebSocketSourceClient}'s buffering
     * method. The lookup matches on the stack frame instead of the thread name on purpose, so it
     * does not depend on how the underlying client names its threads.
     */
    private static Thread awaitThreadWaitingInEnqueue() throws InterruptedException {
        long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            Optional<Thread> waiting =
                    Thread.getAllStackTraces().entrySet().stream()
                            .filter(entry -> isInsideEnqueue(entry.getValue()))
                            .map(Map.Entry::getKey)
                            .findFirst();
            if (waiting.isPresent()) {
                return waiting.get();
            }
            Thread.sleep(POLL_INTERVAL_MS);
        }
        return Assertions.fail("Timed out waiting for the callback thread to fill up the queue");
    }

    private static boolean isInsideEnqueue(StackTraceElement[] stackTrace) {
        for (StackTraceElement frame : stackTrace) {
            if (WebSocketSourceClient.class.getName().equals(frame.getClassName())
                    && "enqueue".equals(frame.getMethodName())) {
                return true;
            }
        }
        return false;
    }

    private String startServer(int messagesToPush) throws IOException {
        server = new MockWebServer();
        server.enqueue(
                new MockResponse()
                        .withWebSocketUpgrade(
                                new WebSocketListener() {
                                    @Override
                                    public void onOpen(WebSocket webSocket, Response response) {
                                        for (int i = 0; i < messagesToPush; i++) {
                                            webSocket.send("{\"id\":" + i + "}");
                                        }
                                    }

                                    @Override
                                    public void onClosing(
                                            WebSocket webSocket, int code, String reason) {
                                        // echo the close frame, otherwise the connection stays
                                        // half closed and the server cannot shut down
                                        webSocket.close(code, reason);
                                    }
                                }));
        server.start();
        return "ws://" + server.getHostName() + ":" + server.getPort() + "/";
    }
}
