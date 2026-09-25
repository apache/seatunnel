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

    private final WebSocketSourceConfig config;
    private final BlockingQueue<String> messageQueue;

    private OkHttpClient httpClient;
    private ScheduledExecutorService reconnectScheduler;

    private volatile WebSocket webSocket;
    private volatile Throwable fatalError;
    private volatile boolean closed;
    private int reconnectTimes;

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
                        config.getUrl(), reconnectTimes),
                error);
    }

    /** Closes the connection and releases the underlying OkHttp resources. */
    public void close() {
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

    private void connect() {
        Request.Builder requestBuilder = new Request.Builder().url(config.getUrl());
        Map<String, String> headers = config.getHeaders();
        if (headers != null) {
            headers.forEach(requestBuilder::addHeader);
        }
        log.info("Connecting to websocket server, url:[{}]", config.getUrl());
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
        if (reconnectTimes >= config.getMaxReconnectTimes()) {
            log.error(
                    "Reconnect to websocket server [{}] gave up after [{}] attempts",
                    config.getUrl(),
                    reconnectTimes);
            fatalError = cause;
            return;
        }
        reconnectTimes++;
        log.warn(
                "Websocket connection to [{}] is broken, reconnecting in [{}]ms, attempt [{}/{}]",
                config.getUrl(),
                config.getReconnectIntervalMs(),
                reconnectTimes,
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

    private void enqueue(String message) {
        try {
            messageQueue.put(message);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while buffering a message from websocket server");
        }
    }

    private class SourceWebSocketListener extends WebSocketListener {

        @Override
        public void onOpen(WebSocket webSocket, Response response) {
            reconnectTimes = 0;
            log.info("Websocket connection to [{}] is established", config.getUrl());
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
                                            i, config.getUrl()));
                    return;
                }
            }
            log.info("Sent [{}] open messages to [{}]", openMessages.size(), config.getUrl());
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
                    config.getUrl(),
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
