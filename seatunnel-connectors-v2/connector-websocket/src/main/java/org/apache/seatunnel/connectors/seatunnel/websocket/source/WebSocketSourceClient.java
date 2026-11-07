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

import org.apache.seatunnel.connectors.seatunnel.websocket.config.WebSocketSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.websocket.exception.WebSocketConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.websocket.exception.WebSocketConnectorException;

import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.WebSocket;
import okhttp3.WebSocketListener;
import okio.ByteString;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Keeps a single WebSocket connection to the configured server and buffers every received frame
 * into a bounded queue consumed by {@link WebSocketSourceReader}.
 *
 * <p>The queue is bounded on purpose: the receiving callback thread blocks once it is full, which
 * back-pressures the server instead of growing the heap without limit.
 */
@Slf4j
public class WebSocketSourceClient {

    private static final int NORMAL_CLOSURE_STATUS = 1000;

    /** How long a single buffering attempt waits before re-checking whether the reader is alive. */
    private static final long ENQUEUE_OFFER_TIMEOUT_MS = 500L;

    private final WebSocketSourceConfig config;
    private final BlockingQueue<String> messageQueue;
    private final AtomicInteger reconnectTimes = new AtomicInteger();
    private final AtomicLong droppedMessages = new AtomicLong();

    private OkHttpClient httpClient;
    private ScheduledExecutorService reconnectScheduler;

    private volatile WebSocket webSocket;
    private volatile Throwable fatalError;
    private volatile boolean closed;
    private volatile long connectedTimestamp;

    public WebSocketSourceClient(WebSocketSourceConfig config) {
        this.config = config;
        this.messageQueue = new ArrayBlockingQueue<>(config.getQueueCapacity());
    }

    /** Opens the connection. Called once when the reader is opened. */
    public void start() {
        OkHttpClient.Builder builder =
                new OkHttpClient.Builder()
                        // a WebSocket connection is long lived, any read timeout would kill it
                        .readTimeout(0, TimeUnit.MILLISECONDS)
                        .connectTimeout(config.getConnectTimeoutMs(), TimeUnit.MILLISECONDS);
        if (config.getPingIntervalMs() > 0) {
            builder.pingInterval(config.getPingIntervalMs(), TimeUnit.MILLISECONDS);
        }
        this.httpClient = builder.build();
        this.reconnectScheduler =
                Executors.newSingleThreadScheduledExecutor(
                        runnable -> {
                            Thread thread = new Thread(runnable, "websocket-source-reconnect");
                            thread.setDaemon(true);
                            return thread;
                        });
        connect();
    }

    /**
     * Takes the next buffered message, waiting up to {@code poll_timeout_ms} when the queue is
     * empty.
     *
     * @return the received message, or {@code null} if nothing arrived in time
     */
    public String poll() throws InterruptedException {
        return messageQueue.poll(config.getPollTimeoutMs(), TimeUnit.MILLISECONDS);
    }

    /**
     * When the current connection was established, as the baseline for the idle timeout: time spent
     * waiting for the handshake is connection setup, governed by {@code connect_timeout_ms}, not
     * idleness.
     *
     * @return the timestamp of the last successful handshake, or {@code 0} while no connection has
     *     been established yet. A successful reconnect refreshes it, so the connection that
     *     replaces a broken one is not immediately considered idle either.
     */
    public long getConnectedTimestamp() {
        return connectedTimestamp;
    }

    /**
     * Rethrows a connection failure recorded by the callback thread so the task fails instead of
     * silently hanging forever.
     */
    public void rethrowFatalIfNeeded() {
        Throwable error = fatalError;
        if (error == null) {
            return;
        }
        if (error instanceof WebSocketConnectorException) {
            throw (WebSocketConnectorException) error;
        }
        throw new WebSocketConnectorException(
                WebSocketConnectorErrorCode.CONNECT_FAILED,
                String.format(
                        "Connection to websocket server [%s] failed after [%s] reconnect attempts",
                        config.getMaskedUrl(), reconnectTimes.get()),
                error);
    }

    /** Closes the connection and releases the underlying OkHttp resources. */
    public synchronized void close() {
        closed = true;
        if (reconnectScheduler != null) {
            reconnectScheduler.shutdownNow();
        }
        WebSocket currentWebSocket = webSocket;
        if (currentWebSocket != null) {
            currentWebSocket.close(NORMAL_CLOSURE_STATUS, null);
            webSocket = null;
        }
        if (httpClient != null) {
            httpClient.dispatcher().executorService().shutdown();
            httpClient.connectionPool().evictAll();
        }
    }

    /**
     * Opens a new connection. Synchronized against {@link #close()} because a reconnect task may
     * still fire once the reader is gone, and a connection created after the teardown would never
     * be closed again.
     */
    private synchronized void connect() {
        if (closed) {
            return;
        }
        // the only place that needs the url as configured, everything else logs the masked form
        Request.Builder requestBuilder = new Request.Builder().url(config.getUrl());
        Map<String, String> headers = config.getHeaders();
        if (headers != null) {
            headers.forEach(requestBuilder::addHeader);
        }
        log.info("Connecting to websocket server, url:[{}]", config.getMaskedUrl());
        webSocket = httpClient.newWebSocket(requestBuilder.build(), new SourceWebSocketListener());
    }

    private void onDisconnected(Throwable cause) {
        if (closed) {
            return;
        }
        if (!config.isEnableReconnect()) {
            fatalError = cause;
            return;
        }
        if (reconnectTimes.get() >= config.getMaxReconnectTimes()) {
            log.error(
                    "Reconnect to websocket server [{}] gave up after [{}] attempts",
                    config.getMaskedUrl(),
                    reconnectTimes.get());
            fatalError = cause;
            return;
        }
        log.warn(
                "Websocket connection to [{}] is broken, reconnecting in [{}]ms, attempt [{}/{}]",
                config.getMaskedUrl(),
                config.getReconnectIntervalMs(),
                reconnectTimes.incrementAndGet(),
                config.getMaxReconnectTimes(),
                cause);
        try {
            reconnectScheduler.schedule(
                    this::connect, config.getReconnectIntervalMs(), TimeUnit.MILLISECONDS);
        } catch (Exception scheduleException) {
            if (!closed) {
                fatalError = scheduleException;
            }
        }
    }

    /**
     * Buffers a received frame, making the callback thread wait while the queue is full so that the
     * server is back-pressured instead of the heap growing without limit.
     *
     * <p>Every attempt is bounded on purpose. The whole read loop of a connection runs on a single
     * callback thread that {@link #close()} cannot interrupt, so waiting for room without ever
     * re-checking {@link #closed} would pin that thread forever once the reader stops draining the
     * queue, which is exactly what happens when a batch job reaches its stop condition or a task is
     * cancelled under back-pressure. Frames that arrive after the reader is gone are dropped
     * instead: nothing would ever consume them.
     */
    private void enqueue(String message) {
        try {
            while (!closed) {
                if (messageQueue.offer(message, ENQUEUE_OFFER_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                    return;
                }
            }
            if (droppedMessages.getAndIncrement() == 0) {
                log.warn(
                        "Dropping messages received from websocket server [{}] because the reader is"
                                + " already closed, further drops are not logged",
                        config.getMaskedUrl());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn(
                    "Interrupted while buffering a message from websocket server [{}]",
                    config.getMaskedUrl());
        }
    }

    private class SourceWebSocketListener extends WebSocketListener {

        @Override
        public void onOpen(WebSocket webSocket, Response response) {
            reconnectTimes.set(0);
            connectedTimestamp = System.currentTimeMillis();
            log.info("Websocket connection to [{}] is established", config.getMaskedUrl());
            List<String> openMessages = config.getOpenMessages();
            if (openMessages == null || openMessages.isEmpty()) {
                return;
            }
            for (int i = 0; i < openMessages.size(); i++) {
                // never log the payload itself, it usually carries credentials
                if (!webSocket.send(openMessages.get(i))) {
                    // throwing here would only be swallowed by the callback thread, record the
                    // failure so that the reader can fail the task on its next poll
                    fatalError =
                            new WebSocketConnectorException(
                                    WebSocketConnectorErrorCode.SEND_MESSAGE_FAILED,
                                    String.format(
                                            "Failed to send open message at index [%s] to websocket server [%s]",
                                            i, config.getMaskedUrl()));
                    return;
                }
            }
            log.info("Sent [{}] open messages to [{}]", openMessages.size(), config.getMaskedUrl());
        }

        @Override
        public void onMessage(WebSocket webSocket, String text) {
            enqueue(text);
        }

        @Override
        public void onMessage(WebSocket webSocket, ByteString bytes) {
            enqueue(bytes.utf8());
        }

        @Override
        public void onClosing(WebSocket webSocket, int code, String reason) {
            log.info(
                    "Websocket server [{}] is closing the connection, code:[{}], reason:[{}]",
                    config.getMaskedUrl(),
                    code,
                    reason);
            webSocket.close(NORMAL_CLOSURE_STATUS, null);
        }

        @Override
        public void onClosed(WebSocket webSocket, int code, String reason) {
            onDisconnected(
                    new IllegalStateException(
                            String.format(
                                    "Websocket connection was closed by the server, code:[%s], reason:[%s]",
                                    code, reason)));
        }

        @Override
        public void onFailure(WebSocket webSocket, Throwable t, Response response) {
            onDisconnected(t);
        }
    }
}
