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

package org.apache.seatunnel.connectors.seatunnel.edgesocket.source;

import org.apache.seatunnel.connectors.seatunnel.edgesocket.channel.BlockingIngressChannel;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.channel.IngressChannel;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.config.EdgeSocketConfig;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.exception.EdgeSocketConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.exception.EdgeSocketConnectorException;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.protocol.EdgeSocketResponseCode;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.protocol.IncomingRecordHandler;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.protocol.IngressProtocolHandler;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Slf4j
public class EdgeSocketIngressServer {

    private final EdgeSocketConfig config;
    private final IngressProtocolHandler protocolHandler;
    private final Object lifecycleLock = new Object();

    private volatile ServerSocket serverSocket;
    private volatile ExecutorService receiverExecutor;
    private volatile Future<?> receiverFuture;
    private volatile RuntimeException fatalReceiverException;
    private volatile boolean hasActiveCollector;
    private int remainingOpenRetries;

    public EdgeSocketIngressServer(EdgeSocketConfig config, IncomingRecordHandler handler) {
        this.config = config;
        this.protocolHandler = new IngressProtocolHandler(config, handler);
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
                remainingOpenRetries = config.getMaxNumRetries();
                log.info(
                        "Edge socket ingress started, bind host:[{}], port:[{}], endpoint:[{}], attempt:[{}]",
                        "0.0.0.0",
                        config.getPort(),
                        config.getEndpoint(),
                        attempt);
                return;
            } catch (IOException bindException) {
                closeServerSocketQuietly();
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
    }

    private void closeServerSocket() throws IOException {
        ServerSocket current = serverSocket;
        serverSocket = null;
        if (current != null) {
            current.close();
        }
    }

    private void closeServerSocketQuietly() {
        try {
            closeServerSocket();
        } catch (IOException e) {
            log.warn(
                    "Failed to close edge socket server socket on {}:{}",
                    "0.0.0.0",
                    config.getPort(),
                    e);
        }
    }

    private void receiveLoop() {
        while (isReceiverActive()) {
            try {
                openServerSocketWithRetry();
                if (!isReceiverActive()) {
                    return;
                }
                serveOneCollector();
            } catch (SocketTimeoutException ignored) {
            } catch (Exception e) {
                if (shouldExitOnException(e)) {
                    return;
                }
            }
        }
    }

    private boolean shouldExitOnException(Exception e) {
        if (!isReceiverActive()) {
            return true;
        }
        if (e instanceof EdgeSocketConnectorException) {
            EdgeSocketConnectorException ece = (EdgeSocketConnectorException) e;
            if (ece.getSeaTunnelErrorCode()
                    == EdgeSocketConnectorErrorCode.SOURCE_REOPEN_EXHAUSTED) {
                throw ece;
            }
        }
        log.warn("Edge socket receiver loop exception, retrying", e);
        return isInterruptedDuringRetryWait();
    }

    private void serveOneCollector() throws IOException {
        ServerSocket ss = serverSocket;
        if (ss == null || ss.isClosed()) {
            return;
        }

        Socket raw = ss.accept();
        raw.setSoTimeout(config.getAcceptTimeoutMs());
        try (IngressChannel channel = new BlockingIngressChannel(raw)) {
            log.info("Accepted edge collector connection from {}", channel.remoteAddress());
            if (hasActiveCollector) {
                log.warn(
                        "Rejected edge collector from {}: another collector is already connected",
                        channel.remoteAddress());
                channel.writeLine(EdgeSocketResponseCode.REJECTED.getCode());
                return;
            }
            activateAndServe(channel);
        }
    }

    private void activateAndServe(IngressChannel channel) throws IOException {
        hasActiveCollector = true;
        try {
            if (!protocolHandler.authenticate(channel)) {
                return;
            }
            closeServerSocket();
            protocolHandler.receiveLoop(channel, this::isReceiverActive);
        } finally {
            hasActiveCollector = false;
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
                                log.error(
                                        "Edge socket receiver thread fatal exception",
                                        receiverException);
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

    private boolean isInterruptedDuringRetryWait() {
        try {
            TimeUnit.MILLISECONDS.sleep(config.getReconnectIntervalMs());
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return true;
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
