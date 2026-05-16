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

package org.apache.seatunnel.connectors.seatunnel.edgesocket.socket;

import org.apache.seatunnel.connectors.seatunnel.edgesocket.config.EdgeSocketAuthType;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.config.EdgeSocketConfig;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.exception.EdgeSocketConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.exception.EdgeSocketConnectorException;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.util.EdgeSocketLogUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Manages the TCP server lifecycle for EdgeSocket source: bind, accept, single-collector
 * enforcement, authentication, and the receive loop. Dispatches incoming records to the {@link
 * IncomingRecordHandler} callback.
 */
@Slf4j
public class EdgeSocketIngressServer {

    private static final String AUTH_TOKEN_PREFIX = "__AUTH__:";
    private static final String BATCH_PREFIX = "__BATCH__:";
    private static final String BATCH_COMMIT_PREFIX = "__COMMIT__:";

    private final EdgeSocketConfig config;
    private final IncomingRecordHandler handler;
    private final Object lifecycleLock = new Object();

    private volatile ServerSocket serverSocket;
    private volatile ExecutorService receiverExecutor;
    private volatile Future<?> receiverFuture;
    private volatile RuntimeException fatalReceiverException;
    private volatile boolean hasActiveCollector;
    private int remainingOpenRetries;

    public EdgeSocketIngressServer(EdgeSocketConfig config, IncomingRecordHandler handler) {
        this.config = config;
        this.handler = handler;
    }

    /** Start the TCP server and begin accepting collector connections. */
    public void start() {
        synchronized (lifecycleLock) {
            if (isReceiverAlive()) {
                log.warn("Edge socket TCP server is already running, skip duplicate start");
                return;
            }
            fatalReceiverException = null;
            remainingOpenRetries = config.getMaxNumRetries();
            startReceiverLoop();
        }
    }

    /** Stop the TCP server and shut down the receiver thread. */
    public void stop() throws IOException {
        closeServerSocket();
        synchronized (lifecycleLock) {
            stopReceiverLoop();
        }
    }

    public boolean isListening() {
        ServerSocket ss = serverSocket;
        return ss != null && ss.isBound() && !ss.isClosed();
    }

    /**
     * Re-throw any fatal exception caught on the receiver thread. Should be called periodically
     * from the task thread (e.g. in pollNext).
     */
    public void rethrowFatalIfNeeded() {
        RuntimeException receiverException = fatalReceiverException;
        if (receiverException != null) {
            throw receiverException;
        }
    }

    private void openServerSocketWithRetry() {
        if (serverSocket != null && !serverSocket.isClosed()) {
            return;
        }

        int attempt = 1;
        while (isReceiverActive()) {
            try {
                serverSocket = new ServerSocket();
                serverSocket.setReuseAddress(true);
                serverSocket.bind(new InetSocketAddress(config.getPort()));
                serverSocket.setSoTimeout(config.getAcceptTimeoutMs());
                log.info(
                        "Edge socket ingress started, bind host:[{}], port:[{}], endpoint:[{}], attempt:[{}]",
                        "0.0.0.0",
                        config.getPort(),
                        config.getEndpoint(),
                        attempt);
                return;
            } catch (IOException bindException) {
                try {
                    closeServerSocket();
                } catch (IOException closeException) {
                    throw new EdgeSocketConnectorException(
                            EdgeSocketConnectorErrorCode.SOURCE_BIND_FAILED,
                            "Close edge socket ingress server failed during reopen",
                            closeException);
                }
                if (!tryConsumeRetryBudget(bindException)) {
                    throw new EdgeSocketConnectorException(
                            EdgeSocketConnectorErrorCode.SOURCE_REOPEN_EXHAUSTED,
                            String.format(
                                    "Bind edge socket ingress %s:%s failed after exhausting retries",
                                    "0.0.0.0", config.getPort()),
                            bindException);
                }
                attempt++;
                if (isInterruptedDuringRetryWait()) {
                    return;
                }
            }
        }

        if (!isReceiverActive()) {
            return;
        }
        throw new EdgeSocketConnectorException(
                EdgeSocketConnectorErrorCode.SOURCE_BIND_FAILED,
                String.format(
                        "Unexpected bind state for edge socket ingress %s:%s",
                        "0.0.0.0", config.getPort()));
    }

    private void suspendServerSocket() {
        ServerSocket current = serverSocket;
        serverSocket = null;
        if (current != null) {
            try {
                current.close();
            } catch (IOException closeException) {
                log.warn("Close server socket during collector session failed", closeException);
            }
        }
    }

    private void closeServerSocket() throws IOException {
        ServerSocket current = serverSocket;
        serverSocket = null;
        if (current != null) {
            current.close();
        }
    }

    private void receiveLoop() {
        while (isReceiverActive()) {
            try {
                openServerSocketWithRetry();
                if (!isReceiverActive()) {
                    return;
                }
            } catch (EdgeSocketConnectorException openException) {
                if (!isReceiverActive()) {
                    return;
                }
                if (openException.getSeaTunnelErrorCode()
                        == EdgeSocketConnectorErrorCode.SOURCE_REOPEN_EXHAUSTED) {
                    throw openException;
                }
                log.warn(
                        "Open edge socket ingress failed on {}:{}, retrying",
                        "0.0.0.0",
                        config.getPort(),
                        openException);
                if (isInterruptedDuringRetryWait()) {
                    return;
                }
                continue;
            } catch (RuntimeException openException) {
                if (!isReceiverActive()) {
                    return;
                }
                log.warn(
                        "Open edge socket ingress failed on {}:{}, retrying",
                        "0.0.0.0",
                        config.getPort(),
                        openException);
                if (isInterruptedDuringRetryWait()) {
                    return;
                }
                continue;
            }
            ServerSocket currentServerSocket = serverSocket;
            if (currentServerSocket == null || currentServerSocket.isClosed()) {
                continue;
            }
            try (Socket collectorSocket = currentServerSocket.accept();
                    BufferedReader reader =
                            new BufferedReader(
                                    new InputStreamReader(
                                            collectorSocket.getInputStream(),
                                            StandardCharsets.UTF_8));
                    BufferedWriter writer =
                            new BufferedWriter(
                                    new OutputStreamWriter(
                                            collectorSocket.getOutputStream(),
                                            StandardCharsets.UTF_8))) {
                collectorSocket.setSoTimeout(config.getAcceptTimeoutMs());
                log.info(
                        "Accepted edge collector connection from {}",
                        collectorSocket.getRemoteSocketAddress());
                if (hasActiveCollector) {
                    log.warn(
                            "Rejected edge collector from {}: another collector is already connected",
                            collectorSocket.getRemoteSocketAddress());
                    writeResponse(writer, EdgeSocketResponseCode.REJECTED);
                    continue;
                }
                hasActiveCollector = true;
                drainBacklogConnections(currentServerSocket);
                suspendServerSocket();
                try {
                    if (!authenticateCollector(
                            reader, writer, collectorSocket.getRemoteSocketAddress())) {
                        continue;
                    }
                    receiveFromCollector(reader, writer);
                } finally {
                    hasActiveCollector = false;
                }
            } catch (SocketTimeoutException timeoutException) {
                log.debug(
                        "Accept edge collector connection timeout on {}:{}, continue waiting",
                        "0.0.0.0",
                        config.getPort());
            } catch (IOException acceptException) {
                if (!isReceiverActive()) {
                    return;
                }
                log.warn(
                        "Failed to accept edge collector connection on {}:{}, retrying",
                        "0.0.0.0",
                        config.getPort(),
                        acceptException);
                if (isInterruptedDuringRetryWait()) {
                    return;
                }
            } catch (RuntimeException runtimeException) {
                if (!isReceiverActive()) {
                    return;
                }
                log.warn("Edge socket receiver loop runtime exception, retrying", runtimeException);
                if (isInterruptedDuringRetryWait()) {
                    return;
                }
            }
        }
    }

    private boolean authenticateCollector(
            BufferedReader reader, BufferedWriter writer, Object remoteAddress) throws IOException {
        if (config.getAuthType() != EdgeSocketAuthType.TOKEN) {
            writeResponse(writer, EdgeSocketResponseCode.AUTH_FAILED);
            log.warn("Unsupported auth type: {}, from {}", config.getAuthType(), remoteAddress);
            return false;
        }
        String authLine;
        try {
            authLine = reader.readLine();
        } catch (SocketTimeoutException timeoutException) {
            writeResponse(writer, EdgeSocketResponseCode.AUTH_FAILED);
            log.warn(
                    "Collector authentication timeout from {}, connection rejected", remoteAddress);
            return false;
        }
        if (authLine == null) {
            writeResponse(writer, EdgeSocketResponseCode.AUTH_FAILED);
            log.warn("Collector from {} closed connection before authentication", remoteAddress);
            return false;
        }
        String presentedToken = parseAuthToken(authLine);
        if (!config.getToken().equals(presentedToken)) {
            writeResponse(writer, EdgeSocketResponseCode.AUTH_FAILED);
            log.warn("Collector authentication failed from {}", remoteAddress);
            return false;
        }
        writeResponse(writer, EdgeSocketResponseCode.ACK);
        return true;
    }

    private String parseAuthToken(String authLine) {
        if (authLine.startsWith(AUTH_TOKEN_PREFIX)) {
            return authLine.substring(AUTH_TOKEN_PREFIX.length());
        }
        return authLine;
    }

    private void receiveFromCollector(BufferedReader reader, BufferedWriter writer)
            throws IOException {
        while (isReceiverActive()) {
            String record;
            try {
                record = reader.readLine();
            } catch (SocketTimeoutException timeoutException) {
                continue;
            }
            if (record == null) {
                return;
            }
            writeResponse(writer, dispatchRequest(record));
        }
    }

    private String dispatchRequest(String request) {
        String normalizedRequest = stripTailCarriageReturn(request);
        if (normalizedRequest.startsWith(BATCH_COMMIT_PREFIX)) {
            Long batchId = parseBatchId(normalizedRequest, BATCH_COMMIT_PREFIX);
            if (batchId == null || batchId <= 0) {
                log.warn("Invalid COMMIT batchId in request: {}", normalizedRequest);
                return EdgeSocketResponseCode.RETRY.getCode();
            }
            return handler.handleCommitRequest(batchId);
        }
        if (!normalizedRequest.startsWith(BATCH_PREFIX)) {
            log.warn(
                    "Unrecognized collector request, expected {} or {} prefix: {}",
                    BATCH_PREFIX,
                    BATCH_COMMIT_PREFIX,
                    EdgeSocketLogUtils.abbreviateForLog(normalizedRequest));
            return EdgeSocketResponseCode.RETRY.getCode();
        }
        int separatorIndex = normalizedRequest.indexOf(':', BATCH_PREFIX.length());
        if (separatorIndex < 0) {
            log.warn(
                    "Malformed BATCH request, missing payload separator: {}",
                    EdgeSocketLogUtils.abbreviateForLog(normalizedRequest));
            return EdgeSocketResponseCode.RETRY.getCode();
        }
        Long batchId = parseBatchId(normalizedRequest.substring(0, separatorIndex), BATCH_PREFIX);
        if (batchId == null || batchId <= 0) {
            log.warn(
                    "Invalid BATCH batchId in request: {}",
                    EdgeSocketLogUtils.abbreviateForLog(normalizedRequest));
            return EdgeSocketResponseCode.RETRY.getCode();
        }
        String payload = normalizedRequest.substring(separatorIndex + 1);
        return handler.handleBatchRecord(batchId, payload);
    }

    private void drainBacklogConnections(ServerSocket ss) {
        int originalTimeout;
        try {
            originalTimeout = ss.getSoTimeout();
            ss.setSoTimeout(1);
        } catch (IOException e) {
            return;
        }
        try {
            while (true) {
                try (Socket backlogged = ss.accept();
                        BufferedWriter w =
                                new BufferedWriter(
                                        new OutputStreamWriter(
                                                backlogged.getOutputStream(),
                                                StandardCharsets.UTF_8))) {
                    log.warn(
                            "Rejected backlog collector from {}: another collector is already connected",
                            backlogged.getRemoteSocketAddress());
                    writeResponse(w, EdgeSocketResponseCode.REJECTED);
                } catch (SocketTimeoutException drained) {
                    break;
                }
            }
        } catch (IOException e) {
            log.warn("Drain backlog connections failed", e);
        } finally {
            try {
                ss.setSoTimeout(originalTimeout);
            } catch (IOException ignored) {
                // Safe to ignore: suspendServerSocket() closes this socket right after drain
            }
        }
    }

    private void startReceiverLoop() {
        receiverExecutor =
                Executors.newSingleThreadExecutor(
                        runnable -> {
                            Thread thread =
                                    new Thread(
                                            runnable, "edge-socket-receiver-" + config.getPort());
                            thread.setDaemon(true);
                            return thread;
                        });
        receiverFuture =
                receiverExecutor.submit(
                        () -> {
                            try {
                                receiveLoop();
                            } catch (RuntimeException receiverException) {
                                fatalReceiverException = receiverException;
                                throw receiverException;
                            }
                        });
    }

    private void stopReceiverLoop() {
        if (receiverFuture != null) {
            receiverFuture.cancel(true);
            receiverFuture = null;
        }
        if (receiverExecutor != null) {
            receiverExecutor.shutdownNow();
            try {
                if (!receiverExecutor.awaitTermination(3, TimeUnit.SECONDS)) {
                    log.warn("Edge socket receiver executor did not terminate within timeout");
                }
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
            } finally {
                receiverExecutor = null;
            }
        }
    }

    private void writeResponse(BufferedWriter writer, String response) throws IOException {
        writer.write(response);
        writer.newLine();
        writer.flush();
    }

    private void writeResponse(BufferedWriter writer, EdgeSocketResponseCode responseCode)
            throws IOException {
        writeResponse(writer, responseCode.getCode());
    }

    private boolean isInterruptedDuringRetryWait() {
        try {
            TimeUnit.MILLISECONDS.sleep(config.getReconnectIntervalMs());
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return true;
        }
    }

    private String stripTailCarriageReturn(String value) {
        if (value.endsWith("\r")) {
            return value.substring(0, value.length() - 1);
        }
        return value;
    }

    private Long parseBatchId(String input, String prefix) {
        if (!input.startsWith(prefix)) {
            return null;
        }
        try {
            return Long.parseLong(input.substring(prefix.length()));
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private boolean isReceiverActive() {
        ExecutorService executor = receiverExecutor;
        return executor != null
                && !executor.isShutdown()
                && !Thread.currentThread().isInterrupted();
    }

    private boolean isReceiverAlive() {
        Future<?> future = receiverFuture;
        return future != null && !future.isDone();
    }

    private boolean tryConsumeRetryBudget(IOException bindException) {
        if (remainingOpenRetries < 0) {
            return true;
        }
        if (remainingOpenRetries == 0) {
            log.error(
                    "Edge socket ingress bind retry budget exhausted on {}:{}",
                    "0.0.0.0",
                    config.getPort(),
                    bindException);
            return false;
        }
        remainingOpenRetries--;
        return true;
    }
}
