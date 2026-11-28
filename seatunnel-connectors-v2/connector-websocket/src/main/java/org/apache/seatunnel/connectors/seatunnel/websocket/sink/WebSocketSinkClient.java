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

package org.apache.seatunnel.connectors.seatunnel.websocket.sink;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.WebSocket;
import okhttp3.WebSocketListener;
import okio.ByteString;

import javax.net.ssl.SSLException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

/** Owns the asynchronous send queue and socket of a single writer. */
final class WebSocketSinkClient {
    private static final int NORMAL_CLOSE = 1000;
    private static final int MAX_MESSAGES_BETWEEN_DRAINS = 512;
    private static final long CHECK_INTERVAL_NANOS = TimeUnit.MILLISECONDS.toNanos(10);

    private final WebSocketSinkConfig config;
    private final OkHttpClient http;
    private final Object changed = new Object();
    private final AtomicReference<String> failure = new AtomicReference<>();
    private final AtomicBoolean released = new AtomicBoolean();

    private volatile WebSocket socket;
    private volatile boolean opened;
    private volatile boolean closing;
    private volatile boolean closed;
    private int messagesSinceDrain;

    WebSocketSinkClient(WebSocketSinkConfig config) {
        this.config = config;
        http =
                new OkHttpClient.Builder()
                        .followRedirects(false)
                        .followSslRedirects(false)
                        .retryOnConnectionFailure(false)
                        .connectTimeout(config.connectTimeout, TimeUnit.MILLISECONDS)
                        .readTimeout(config.connectTimeout, TimeUnit.MILLISECONDS)
                        .writeTimeout(config.writeTimeout, TimeUnit.MILLISECONDS)
                        .build();
    }

    void send(byte[] payload, boolean binary) throws IOException {
        WebSocket connection = open();
        long queueLimit = config.queueBytes - (long) payload.length;
        await(
                () -> {
                    long queued = connection.queueSize();
                    if (queued == 0) {
                        messagesSinceDrain = 0;
                    }
                    return queued <= queueLimit && messagesSinceDrain < MAX_MESSAGES_BETWEEN_DRAINS;
                },
                config.writeTimeout,
                "outbound queue admission timed out");
        boolean accepted =
                binary
                        ? connection.send(ByteString.of(payload))
                        : connection.send(new String(payload, StandardCharsets.UTF_8));
        if (!accepted) {
            throw fail("WebSocket send rejected the message");
        }
        messagesSinceDrain++;
        checkFailure();
    }

    private WebSocket open() throws IOException {
        checkFailure();
        if (socket == null) {
            try {
                Request.Builder builder =
                        new Request.Builder().url(config.endpoint.toASCIIString());
                String authorization = config.authorization();
                if (authorization != null) {
                    builder.header("Authorization", authorization);
                }
                socket = http.newWebSocket(builder.build(), new Listener());
            } catch (RuntimeException e) {
                throw fail("could not start opening handshake");
            }
            if (released.get()) {
                socket.cancel();
            }
            await(() -> opened, config.connectTimeout, "opening handshake timed out");
        }
        return socket;
    }

    void drain() throws IOException {
        checkFailure();
        WebSocket connection = socket;
        if (connection != null) {
            await(
                    () -> connection.queueSize() == 0,
                    config.flushTimeout,
                    "outbound queue drain timed out");
            messagesSinceDrain = 0;
        }
    }

    void close() throws IOException {
        try {
            drain();
            WebSocket connection = socket;
            if (connection != null) {
                closing = true;
                if (!connection.close(NORMAL_CLOSE, null)) {
                    throw fail("could not start the normal close handshake");
                }
                await(() -> closed, config.closeTimeout, "close handshake timed out");
            }
        } finally {
            release();
        }
    }

    void abort(String message) {
        failure.compareAndSet(null, message);
        signalChange();
        release();
    }

    private void release() {
        if (released.compareAndSet(false, true)) {
            WebSocket connection = socket;
            if (connection != null && !closed) {
                connection.cancel();
            }
            http.dispatcher().cancelAll();
            http.dispatcher().executorService().shutdown();
            http.connectionPool().evictAll();
        }
    }

    void checkFailure() throws IOException {
        String current = failure.get();
        if (current != null) {
            throw new IOException(current);
        }
        if (Thread.currentThread().isInterrupted()) {
            throw fail("operation interrupted");
        }
    }

    private void await(BooleanSupplier done, int timeout, String timeoutMessage)
            throws IOException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);
        while (true) {
            checkFailure();
            if (done.getAsBoolean()) {
                checkFailure();
                return;
            }
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                throw fail(timeoutMessage);
            }
            synchronized (changed) {
                try {
                    TimeUnit.NANOSECONDS.timedWait(
                            changed, Math.min(remaining, CHECK_INTERVAL_NANOS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw fail("operation interrupted");
                }
            }
        }
    }

    private IOException fail(String message) {
        String sanitized = "WebSocket sink: " + message;
        failure.compareAndSet(null, sanitized);
        signalChange();
        return new IOException(failure.get());
    }

    private void signalChange() {
        synchronized (changed) {
            changed.notifyAll();
        }
    }

    private final class Listener extends WebSocketListener {
        @Override
        public void onOpen(WebSocket webSocket, Response response) {
            opened = true;
            signalChange();
        }

        @Override
        public void onClosing(WebSocket webSocket, int code, String reason) {
            if (!closing || code != NORMAL_CLOSE) {
                fail("peer closed the connection unexpectedly (code " + code + ")");
                webSocket.cancel();
            }
        }

        @Override
        public void onClosed(WebSocket webSocket, int code, String reason) {
            if (closing && code == NORMAL_CLOSE) {
                closed = true;
                signalChange();
            }
        }

        @Override
        public void onFailure(WebSocket webSocket, Throwable cause, Response response) {
            if (response != null && response.code() != 101) {
                fail("opening handshake rejected (HTTP " + response.code() + ")");
            } else if (cause instanceof SSLException) {
                fail("TLS handshake failed");
            } else {
                // Listener exceptions and response bodies may include endpoint or peer data.
                fail(opened ? "connection failed during transmission" : "opening handshake failed");
            }
        }
    }
}
