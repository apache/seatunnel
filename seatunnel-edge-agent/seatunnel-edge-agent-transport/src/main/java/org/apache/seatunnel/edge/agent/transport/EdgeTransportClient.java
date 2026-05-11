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

package org.apache.seatunnel.edge.agent.transport;

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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * EdgeSocket collector client: resolves ingress hosts from {@link JobTaskGroupAddressesLookup}
 * (typically {@link SeaTunnelEdgeTransportClients#jobTaskGroupAddressesLookup}), parses worker host
 * lists via {@link JobTaskGroupAddressParser}, connects with retry/backoff, and on transport or
 * auth failures opens a new session after re-fetching task-group addresses from Zeta (same RPC as
 * {@link SeaTunnelEdgeTransportClients#jobTaskGroupAddressesLookup}).
 *
 * <p>For production wiring from Zeta cluster addresses and {@link
 * org.apache.seatunnel.engine.client.SeaTunnelClient}, use {@link
 * SeaTunnelEdgeTransportClients#newEdgeTransportClient}.
 *
 * <p>The deprecated no-arg constructor keeps legacy bootstrap compatibility only ({@link #open} is
 * a no-op; I/O throws {@link IOException}). Prefer {@link #EdgeTransportClient(EdgeTransportConfig,
 * JobTaskGroupAddressesLookup)} or the SeaTunnel factory above.
 *
 * <p>Single-threaded usage is assumed unless callers synchronize externally.
 */
public class EdgeTransportClient implements EdgeCollectorTransport {

    private static final Logger LOG = LoggerFactory.getLogger(EdgeTransportClient.class);

    private final EdgeTransportConfig config;
    private final JobTaskGroupAddressesLookup addressesLookup;
    private final EdgeSocketSocketFactory socketFactory;
    private final EdgeSocketLineTransport lineTransport;

    private final Object connectionLock = new Object();

    private List<InetSocketAddress> endpoints = new ArrayList<>();
    private int endpointIndex;
    private SocketHolder activeSocket;

    /**
     * Legacy stub instance (open/close no-op; discovery/send throw {@link IOException}). Prefer
     * {@link SeaTunnelEdgeTransportClients#newEdgeTransportClient} or {@link
     * #EdgeTransportClient(EdgeTransportConfig, JobTaskGroupAddressesLookup)}.
     */
    @Deprecated
    public EdgeTransportClient() {
        this(null, null, EdgeSocketSocketFactory.DEFAULT);
    }

    public EdgeTransportClient(
            EdgeTransportConfig config, JobTaskGroupAddressesLookup addressesLookup) {
        this(config, addressesLookup, EdgeSocketSocketFactory.DEFAULT);
    }

    EdgeTransportClient(
            EdgeTransportConfig config,
            JobTaskGroupAddressesLookup addressesLookup,
            EdgeSocketSocketFactory socketFactory) {
        this.config = config;
        this.addressesLookup = addressesLookup;
        this.socketFactory = Objects.requireNonNull(socketFactory, "socketFactory");
        if (config != null ^ addressesLookup != null) {
            throw new IllegalArgumentException(
                    "config and addressesLookup must both be null (stub) or both non-null");
        }
        this.lineTransport = config != null ? new EdgeSocketLineTransport(config) : null;
    }

    private boolean isStub() {
        return config == null;
    }

    /**
     * Ensures task-group discovery ran at least once. Does not open a persistent TCP session yet.
     *
     * @throws IOException if discovery JSON cannot be parsed or yields no hosts
     */
    @Override
    public void discoverEndpoints() throws IOException {
        if (isStub()) {
            throw new IOException("EdgeTransportClient is not configured for discovery");
        }
        synchronized (connectionLock) {
            refreshEndpointsFromJob();
        }
    }

    /**
     * Opens a connection (if needed), performs TOKEN auth, and validates readiness.
     *
     * <p>Checked exceptions are wrapped so callers compiled against the legacy no-arg bootstrap
     * keep a non-throwing signature.
     */
    @Override
    public void open() {
        if (isStub()) {
            return;
        }
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

    /**
     * Sends one batch line and blocks until the ingress acknowledges checkpoint durability via
     * {@code ACK:&lt;batchId&gt;} semantics from {@code __COMMIT__}.
     */
    @Override
    public void sendBatchAndAwaitAck(long batchId, String payload)
            throws IOException, InterruptedException {
        Objects.requireNonNull(payload, "payload");
        if (isStub()) {
            throw new IOException("EdgeTransportClient is not configured for EdgeSocket");
        }
        synchronized (connectionLock) {
            IOException lastIo = null;
            InterruptedException lastInterrupted = null;
            for (int cycle = 0; cycle < config.getMaxFullDiscoveryCycles(); cycle++) {
                try {
                    ensureAuthenticatedSession();
                    lineTransport.sendBatchUntilReceived(
                            activeSocket.reader, activeSocket.writer, batchId, payload);
                    lineTransport.awaitCommitAck(activeSocket.reader, activeSocket.writer, batchId);
                    return;
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    lastInterrupted = ex;
                    break;
                } catch (IOException ex) {
                    lastIo = ex;
                    LOG.warn(
                            "Edge transport IO failure, will reconnect/rediscover. batchId={}",
                            batchId,
                            ex);
                    invalidateSession();
                    try {
                        reconnectWithRediscover();
                    } catch (InterruptedException interruptReconnect) {
                        Thread.currentThread().interrupt();
                        throw interruptReconnect;
                    }
                }
            }
            if (lastInterrupted != null) {
                throw lastInterrupted;
            }
            if (lastIo != null) {
                throw lastIo;
            }
            throw new IOException(
                    "sendBatchAndAwaitAck exhausted rediscovery cycles for batchId=" + batchId);
        }
    }

    /**
     * TCP-level probe: resolves endpoints if empty, then attempts {@link Socket#connect} on
     * candidates until one succeeds (connection is closed before returning).
     *
     * @return {@code true} if any endpoint accepted a TCP connection within {@link
     *     EdgeTransportConfig#getConnectTimeoutMs()}
     */
    @Override
    public boolean probeReachable() throws IOException {
        if (isStub()) {
            return false;
        }
        synchronized (connectionLock) {
            if (endpoints.isEmpty()) {
                refreshEndpointsFromJob();
            }
            int n = endpoints.size();
            if (n == 0) {
                return false;
            }
            for (int i = 0; i < n; i++) {
                InetSocketAddress address = endpoints.get((endpointIndex + i) % n);
                try (Socket socket = socketFactory.connect(address, config.getConnectTimeoutMs())) {
                    return socket.isConnected();
                } catch (IOException ex) {
                    LOG.debug("Probe failed for {}", address, ex);
                }
            }
            return false;
        }
    }

    @Override
    public void close() {
        synchronized (connectionLock) {
            invalidateSession();
            endpoints.clear();
        }
    }

    private void refreshEndpointsFromJob() throws IOException {
        String json = addressesLookup.getJobTaskGroupAddresses(config.getJobId());
        List<String> hosts = JobTaskGroupAddressParser.parseDistinctHosts(json);
        if (hosts.isEmpty()) {
            throw new IOException(
                    "No hosts discovered for jobId="
                            + config.getJobId()
                            + " (getJobTaskGroupAddresses empty or malformed)");
        }
        List<InetSocketAddress> next = new ArrayList<>(hosts.size());
        for (String host : hosts) {
            next.add(new InetSocketAddress(host, config.getEdgeIngressPort()));
        }
        this.endpoints = next;
        LOG.info(
                "Discovered {} edge ingress candidate host(s) for job {}",
                next.size(),
                config.getJobId());
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

    private void reconnectWithRediscover() throws IOException, InterruptedException {
        long backoff = config.getInitialBackoffMs();
        IOException discoveryFailure = null;
        for (int attempt = 0; attempt < config.getMaxFullDiscoveryCycles(); attempt++) {
            try {
                refreshEndpointsFromJob();
            } catch (IOException ex) {
                discoveryFailure = ex;
                LOG.warn(
                        "Rediscover endpoints failed (attempt {}/{})",
                        attempt + 1,
                        config.getMaxFullDiscoveryCycles(),
                        ex);
            }
            if (endpoints.isEmpty()) {
                EdgeTransportConfig.sleepQuiet(backoff);
                backoff =
                        Math.min(
                                config.getMaxBackoffMs(),
                                Math.max(backoff, config.getInitialBackoffMs()) * 2);
                continue;
            }
            rotatePreferredEndpoint();
            int triesPerCycle = Math.max(1, endpoints.size());
            for (int t = 0; t < triesPerCycle; t++) {
                InetSocketAddress address = pickNextEndpointAddress();
                try {
                    Socket socket = socketFactory.connect(address, config.getConnectTimeoutMs());
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
                    LOG.info("Connected and authenticated to edge ingress {}", address);
                    return;
                } catch (IOException connectOrAuthEx) {
                    LOG.warn("Connect/auth failed for {}", address, connectOrAuthEx);
                    backoff =
                            Math.min(
                                    config.getMaxBackoffMs(),
                                    Math.max(backoff, config.getInitialBackoffMs()) * 2);
                    EdgeTransportConfig.sleepQuiet(backoff);
                }
            }
        }
        if (discoveryFailure != null) {
            throw discoveryFailure;
        }
        throw new IOException(
                "Reconnect exhausted after "
                        + config.getMaxFullDiscoveryCycles()
                        + " rediscover cycles");
    }

    private void ensureAuthenticatedSession() throws IOException, InterruptedException {
        if (activeSocket != null) {
            return;
        }
        reconnectWithRediscover();
    }

    private InetSocketAddress pickNextEndpointAddress() {
        if (endpoints.isEmpty()) {
            throw new IllegalStateException("No endpoints");
        }
        InetSocketAddress addr = endpoints.get(endpointIndex % endpoints.size());
        endpointIndex = (endpointIndex + 1) % endpoints.size();
        return addr;
    }

    private void rotatePreferredEndpoint() {
        if (!endpoints.isEmpty()) {
            endpointIndex = (endpointIndex + 1) % endpoints.size();
        }
    }

    private static final class SocketHolder implements AutoCloseable {
        private final Socket socket;
        final BufferedReader reader;
        final BufferedWriter writer;

        private SocketHolder(Socket socket, BufferedReader reader, BufferedWriter writer) {
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
