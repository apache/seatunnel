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

package org.apache.seatunnel.edge.agent.transport.socket;

import org.apache.seatunnel.edge.agent.transport.EdgeCollectorTransport;
import org.apache.seatunnel.edge.agent.transport.config.EdgeTransportConfig;
import org.apache.seatunnel.edge.agent.transport.config.EdgeTransportEndpoints;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class EdgeTransportClient implements EdgeCollectorTransport {

    private static final Logger LOG = LoggerFactory.getLogger(EdgeTransportClient.class);

    private final EdgeTransportConfig config;
    private final EdgeSocketSocketFactory socketFactory;
    private final EdgeSocketLineTransport lineTransport;

    private final Object connectionLock = new Object();

    private InetSocketAddress endpoint;
    private SocketHolder activeSocket;

    public EdgeTransportClient(EdgeTransportConfig config) {
        this(config, EdgeSocketSocketFactory.DEFAULT);
    }

    EdgeTransportClient(EdgeTransportConfig config, EdgeSocketSocketFactory socketFactory) {
        this.config = Objects.requireNonNull(config, "config");
        this.socketFactory = Objects.requireNonNull(socketFactory, "socketFactory");
        this.lineTransport = new EdgeSocketLineTransport(config);
        this.endpoint = EdgeTransportEndpoints.toSocketAddress(config.getEndpoint());
    }

    @Override
    public void open() {
        synchronized (connectionLock) {
            try {
                ensureAuthenticatedSession();
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted during edge transport open", ex);
            }
        }
    }

    @Override
    public void send(long batchId, String payload) throws IOException, InterruptedException {
        sendUntilReceived(batchId, payload);
    }

    @Override
    public void sendUntilReceived(long batchId, String payload)
            throws IOException, InterruptedException {
        synchronized (connectionLock) {
            IOException lastIo = null;
            InterruptedException lastInterrupted = null;
            for (int cycle = 0; cycle < config.getMaxReconnectCycles(); cycle++) {
                try {
                    ensureAuthenticatedSession();
                    lineTransport.sendBatchUntilReceived(
                            activeSocket.reader, activeSocket.writer, batchId, payload);
                    return;
                } catch (EdgeSocketCollectorRejectedException ex) {
                    invalidateSession();
                    throw ex;
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    lastInterrupted = ex;
                    break;
                } catch (IOException ex) {
                    lastIo = ex;
                    LOG.warn("Edge transport IO failure, will reconnect. batchId={}", batchId, ex);
                    invalidateSession();
                }
            }
            if (lastInterrupted != null) {
                throw lastInterrupted;
            }
            if (lastIo != null) {
                throw lastIo;
            }
            throw new IOException(
                    "sendUntilReceived exhausted reconnect cycles for batchId=" + batchId);
        }
    }

    @Override
    public boolean probeReachable() throws IOException {
        synchronized (connectionLock) {
            try (Socket socket = socketFactory.connect(endpoint, config.getConnectTimeoutMs())) {
                return socket.isConnected();
            } catch (IOException ex) {
                LOG.debug("Probe failed for {}", endpoint, ex);
                return false;
            }
        }
    }

    @Override
    public void close() {
        synchronized (connectionLock) {
            invalidateSession();
        }
    }

    private void invalidateSession() {
        if (activeSocket != null) {
            try {
                activeSocket.close();
            } catch (IOException ex) {
                LOG.debug("Error closing edge socket session", ex);
            }
            activeSocket = null;
        }
    }

    private void ensureAuthenticatedSession() throws IOException, InterruptedException {
        if (activeSocket != null) {
            return;
        }
        long backoff = config.getInitialBackoffMs();
        for (int cycle = 0; cycle < config.getMaxReconnectCycles(); cycle++) {
            Socket socket = null;
            try {
                socket = socketFactory.connect(endpoint, config.getConnectTimeoutMs());
                socket.setSoTimeout(config.getReadTimeoutMs());
                BufferedReader reader =
                        new BufferedReader(
                                new InputStreamReader(
                                        socket.getInputStream(), StandardCharsets.UTF_8));
                BufferedWriter writer =
                        new BufferedWriter(
                                new OutputStreamWriter(
                                        socket.getOutputStream(), StandardCharsets.UTF_8));
                lineTransport.authenticate(reader, writer);
                activeSocket = new SocketHolder(socket, reader, writer);
                LOG.info("Connected and authenticated to edge ingress {}", endpoint);
                return;
            } catch (EdgeSocketCollectorRejectedException ex) {
                if (socket != null) {
                    try {
                        socket.close();
                    } catch (IOException closeEx) {
                        LOG.debug("Error closing socket after REJECTED", closeEx);
                    }
                }
                throw ex;
            } catch (IOException connectOrAuthEx) {
                if (socket != null) {
                    try {
                        socket.close();
                    } catch (IOException closeEx) {
                        LOG.debug("Error closing socket after connect/auth failure", closeEx);
                    }
                }
                LOG.warn("Connect/auth failed for {}", endpoint, connectOrAuthEx);
                backoff =
                        Math.min(
                                config.getMaxBackoffMs(),
                                Math.max(backoff, config.getInitialBackoffMs()) * 2);
                EdgeTransportConfig.sleepQuiet(backoff);
            }
        }
        throw new IOException(
                "Cannot connect to edge ingress "
                        + endpoint
                        + " after "
                        + config.getMaxReconnectCycles()
                        + " cycles");
    }

    private static final class SocketHolder implements AutoCloseable {
        private final Socket socket;
        private final BufferedReader reader;
        private final BufferedWriter writer;

        SocketHolder(Socket socket, BufferedReader reader, BufferedWriter writer) {
            this.socket = socket;
            this.reader = reader;
            this.writer = writer;
        }

        @Override
        public void close() throws IOException {
            IOException first = null;
            try {
                socket.shutdownOutput();
            } catch (IOException ex) {
                first = ex;
            }
            try {
                socket.close();
            } catch (IOException ex) {
                if (first == null) {
                    first = ex;
                }
            }
            if (first != null) {
                throw first;
            }
        }
    }
}
