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

package org.apache.seatunnel.connectors.seatunnel.coap;

import org.eclipse.californium.core.CoapServer;
import org.eclipse.californium.core.config.CoapConfig;
import org.eclipse.californium.core.network.CoapEndpoint;
import org.eclipse.californium.elements.RawDataChannel;
import org.eclipse.californium.elements.UDPConnector;
import org.eclipse.californium.elements.config.Configuration;
import org.eclipse.californium.elements.config.SystemConfig;
import org.eclipse.californium.elements.config.UdpConfig;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/** Creates per-task UDP endpoints and closes their threads, timers, and transport together. */
final class CoapRuntime implements AutoCloseable {
    // Keep peer admission alive until the SDK's exchange window and next sweep have elapsed.
    private static final long PEER_IDLE =
            TimeUnit.SECONDS.toNanos(CoapSettings.EXCHANGE_LIFETIME_SECONDS + 2);

    final CoapEndpoint endpoint;
    final CoapServer server;
    private final ProtocolExecutor executor;
    private boolean closed;

    private CoapRuntime(Configuration config, UDPConnector transport, int threads, boolean listen) {
        executor = new ProtocolExecutor(threads);
        endpoint =
                new CoapEndpoint.Builder().setConfiguration(config).setConnector(transport).build();
        if (listen) {
            server = new CoapServer(config);
            server.addEndpoint(endpoint);
            server.setExecutors(executor, executor, true);
        } else {
            server = null;
            endpoint.setExecutors(executor, executor);
        }
    }

    static CoapRuntime listener(CoapSettings.Listener settings) {
        Configuration config = configuration(settings, settings.peers);
        UDPConnector transport =
                new BoundedPeers(
                        new InetSocketAddress(settings.address, settings.port),
                        config,
                        settings.peers);
        return new CoapRuntime(config, transport, settings.concurrency, true);
    }

    static CoapRuntime sender(CoapSettings.Sender settings) {
        Configuration config = configuration(settings, 10);
        UDPConnector transport = new UDPConnector(new InetSocketAddress(0), config);
        return new CoapRuntime(config, transport, 1, false);
    }

    private static Configuration configuration(CoapSettings settings, int peers) {
        // Supplying the definitions directly avoids the SDK's default configuration file.
        return new Configuration(
                        SystemConfig.DEFINITIONS, CoapConfig.DEFINITIONS, UdpConfig.DEFINITIONS)
                .set(CoapConfig.MAX_ACTIVE_PEERS, peers)
                .set(
                        CoapConfig.EXCHANGE_LIFETIME,
                        CoapSettings.EXCHANGE_LIFETIME_SECONDS,
                        TimeUnit.SECONDS)
                .set(CoapConfig.DEDUPLICATOR, CoapConfig.DEDUPLICATOR_PEERS_MARK_AND_SWEEP)
                .set(CoapConfig.PEERS_MARK_AND_SWEEP_MESSAGES, CoapSettings.DEDUP_PER_PEER)
                .set(CoapConfig.MARK_AND_SWEEP_INTERVAL, 1, TimeUnit.SECONDS)
                .set(CoapConfig.DEDUPLICATOR_AUTO_REPLACE, false)
                .set(CoapConfig.USE_MESSAGE_OFFLOADING, true)
                .set(CoapConfig.MULTICAST_BASE_MID, 0)
                .set(CoapConfig.MAX_SERVER_OBSERVES, 1)
                .set(CoapConfig.PREFERRED_BLOCK_SIZE, 512)
                .set(CoapConfig.MAX_MESSAGE_SIZE, 1024)
                .set(CoapConfig.MAX_RESOURCE_BODY_SIZE, settings.maxBytes)
                .set(
                        CoapConfig.BLOCKWISE_STATUS_LIFETIME,
                        settings.blockTimeout,
                        TimeUnit.MILLISECONDS)
                .set(
                        CoapConfig.BLOCKWISE_STATUS_INTERVAL,
                        Math.min(1000, settings.blockTimeout),
                        TimeUnit.MILLISECONDS)
                .set(CoapConfig.BLOCKWISE_ENTITY_TOO_LARGE_AUTO_FAILOVER, false)
                .set(CoapConfig.STRICT_EMPTY_MESSAGE_FORMAT, true)
                .set(UdpConfig.UDP_DATAGRAM_SIZE, CoapSettings.DATAGRAM_BYTES)
                .set(UdpConfig.UDP_RECEIVER_THREAD_COUNT, 1)
                .set(UdpConfig.UDP_SENDER_THREAD_COUNT, 1)
                .set(UdpConfig.UDP_CONNECTOR_OUT_CAPACITY, CoapSettings.OUTBOUND_DATAGRAMS);
    }

    /** Opens the socket. For a listener, the resource must already have been installed. */
    synchronized void start() throws IOException {
        if (closed) {
            throw new IOException("CoAP endpoint is closed");
        }
        try {
            if (server == null) {
                endpoint.start();
            } else {
                server.start();
            }
        } catch (IOException | RuntimeException e) {
            close();
            throw new IOException("CoAP failed to start UDP endpoint");
        }
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            if (server == null) {
                endpoint.destroy();
            } else {
                server.destroy();
            }
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * Limits the raw work accepted by the protocol executor. Loss before CoAP parsing has UDP
     * semantics. A decoded application request with a full row queue receives 5.03.
     */
    private static final class ProtocolExecutor extends ScheduledThreadPoolExecutor {
        private final Semaphore pending;

        private ProtocolExecutor(int threads) {
            super(
                    threads,
                    task -> {
                        Thread worker = new Thread(task, "seatunnel-coap-protocol");
                        worker.setDaemon(true);
                        return worker;
                    });
            pending = new Semaphore(threads * CoapSettings.PROTOCOL_LIMIT_PER_WORKER);
            setRemoveOnCancelPolicy(true);
            setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        }

        @Override
        public void execute(Runnable task) {
            if (!pending.tryAcquire()) {
                throw new RejectedExecutionException("CoAP protocol work queue is full");
            }
            try {
                super.execute(
                        () -> {
                            try {
                                task.run();
                            } finally {
                                pending.release();
                            }
                        });
            } catch (RejectedExecutionException e) {
                pending.release();
                throw e;
            }
        }
    }

    /** Limits live peer addresses before the SDK allocates per-peer exchange or block state. */
    private static final class BoundedPeers extends UDPConnector {
        private final Map<InetSocketAddress, Long> peers = new LinkedHashMap<>(16, 0.75f, true);
        private final int limit;

        private BoundedPeers(InetSocketAddress address, Configuration config, int limit) {
            super(address, config);
            this.limit = limit;
        }

        @Override
        public void setRawDataReceiver(RawDataChannel delegate) {
            super.setRawDataReceiver(
                    packet -> {
                        if (!packet.isMulticast() && admit(packet.getInetSocketAddress())) {
                            delegate.receiveData(packet);
                        }
                    });
        }

        private synchronized boolean admit(InetSocketAddress peer) {
            long now = System.nanoTime();
            Iterator<Long> expiry = peers.values().iterator();
            while (expiry.hasNext() && now - expiry.next() >= PEER_IDLE) {
                expiry.remove();
            }
            if (!peers.containsKey(peer) && peers.size() == limit) {
                return false;
            }
            peers.put(peer, now);
            return true;
        }
    }
}
