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

package org.apache.seatunnel.connectors.seatunnel.udp;

import org.apache.seatunnel.api.common.metrics.Counter;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.net.StandardProtocolFamily;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.DatagramChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.seatunnel.connectors.seatunnel.udp.UdpPacketFormat.InvalidDatagram;

/**
 * Owns nonblocking listeners with a single reusable packet buffer. Backpressure is confined to the
 * kernel's bounded receive queue, so no receiver thread can outlive the reader.
 */
@Slf4j
final class UdpReader implements SourceReader<SeaTunnelRow, UdpSplit> {
    private final UdpConfig config;
    private final Context context;
    private final String tableId;
    private final ByteBuffer buffer;
    private final int[] separatorPrefix;
    private final long idleNanos;
    private final Counter malformed;
    private final Counter oversized;

    // Assignments originate on an engine operation thread. Socket ownership and timestamps are
    // advanced by the polling thread. The channel registry allows nonblocking cross-thread close.
    private final ConcurrentLinkedQueue<UdpSplit> additions = new ConcurrentLinkedQueue<>();
    private final Map<Integer, Listener> listeners = new LinkedHashMap<>();
    private final Map<Integer, DatagramChannel> channels = new ConcurrentHashMap<>();
    private final AtomicBoolean cancelled = new AtomicBoolean();
    private volatile boolean noMoreSplits;
    // Poller-owned terminal signal; late assignments cannot complete before the expected count.
    private boolean signaledEof;
    // Delivered by the enumerator with each split, including restored terminal ports.
    private int expectedPorts;
    private int nextChannel;

    UdpReader(UdpConfig config, Context context, String tableId) {
        this.config = config;
        this.context = context;
        this.tableId = tableId;
        this.buffer = ByteBuffer.allocate(config.maxDatagram + 1);
        this.idleNanos = TimeUnit.MILLISECONDS.toNanos(config.idleMs);
        this.separatorPrefix = prefix(config.separator);
        malformed = context.getMetricsContext().counter("udpMalformedDatagrams");
        oversized = context.getMetricsContext().counter("udpOversizedDatagrams");
    }

    @Override
    public void open() {}

    /**
     * Binds assigned ports and processes at most a configured number of datagrams. Nonblocking
     * receive returns immediately on an idle listener, leaving the engine free to checkpoint.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws IOException {
        synchronized (output.getCheckpointLock()) {
            if (cancelled.get() || signaledEof) {
                return;
            }
            acceptAssignments();
            List<Listener> active = new ArrayList<>();
            for (Listener listener : listeners.values()) {
                if (!listener.finished) {
                    active.add(listener);
                }
            }
            if (!active.isEmpty()) {
                int idle = 0;
                for (int count = 0;
                        count < config.maxPoll && idle < active.size() && !cancelled.get(); ) {
                    nextChannel %= active.size();
                    Listener listener = active.get(nextChannel++);
                    if (listener.finished || !receive(listener, output)) {
                        idle++;
                    } else {
                        count++;
                        idle = 0;
                    }
                }
            }
            if (!cancelled.get()
                    && noMoreSplits
                    && expectedPorts > 0
                    && listeners.size() == expectedPorts
                    && listeners.values().stream().allMatch(l -> l.finished)) {
                signaledEof = true;
                context.signalNoMoreElement();
            }
        }
    }

    /**
     * Capture both bound listener progress and assignments delivered before their first poll.
     *
     * <p>The engine holds the collector monitor so transitions cannot race a checkpoint snapshot.
     */
    @Override
    public List<UdpSplit> snapshotState(long checkpointId) {
        Map<Integer, UdpSplit> state = new LinkedHashMap<>();
        for (Listener listener : listeners.values()) {
            state.put(listener.port, new UdpSplit(listener.port, listener.finished, expectedPorts));
        }
        for (UdpSplit split : additions) {
            state.merge(split.port, split, (first, second) -> first.finished ? first : second);
        }
        return new ArrayList<>(state.values());
    }

    @Override
    public void addSplits(List<UdpSplit> splits) {
        additions.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        noMoreSplits = true;
    }

    /**
     * Close the published channel registry independently of the collector monitor. The sticky flag
     *
     * <p>also makes a concurrent listener publication close itself instead of surviving
     * cancellation.
     */
    @Override
    public void cancel() {
        if (cancelled.compareAndSet(false, true)) {
            for (DatagramChannel channel : channels.values()) {
                closeChannel(channel);
            }
            channels.clear();
        }
    }

    @Override
    public void close() {
        cancel();
    }

    private void acceptAssignments() throws IOException {
        UdpSplit split;
        while (!cancelled.get() && (split = additions.poll()) != null) {
            expectedPorts = split.readerPortCount;
            Listener previous = listeners.get(split.port);
            if (previous == null) {
                Listener listener = new Listener(split, split.finished ? null : listen(split.port));
                listeners.put(split.port, listener);
            } else if (split.finished && !previous.finished) {
                finish(previous);
            }
        }
    }

    private DatagramChannel listen(int port) throws IOException {
        DatagramChannel channel =
                DatagramChannel.open(
                        config.address instanceof Inet6Address
                                ? StandardProtocolFamily.INET6
                                : StandardProtocolFamily.INET);
        channels.put(port, channel);
        boolean retained = false;
        try {
            if (cancelled.get()) {
                return channel;
            }
            channel.setOption(StandardSocketOptions.SO_REUSEADDR, false);
            channel.setOption(StandardSocketOptions.SO_RCVBUF, config.receiveBuffer);
            channel.configureBlocking(false);
            channel.bind(new InetSocketAddress(config.address, port));
            if (!cancelled.get()) {
                retained = true;
                log.info(
                        "UDP listener ready on port {} (reader {})",
                        port,
                        context.getIndexOfSubtask());
            }
            return channel;
        } catch (IOException e) {
            if (cancelled.get()) {
                return channel;
            }
            throw new IOException("UDP source could not bind configured port " + port, e);
        } finally {
            if (!retained) {
                channels.remove(port, channel);
                closeChannel(channel);
            }
        }
    }

    private boolean receive(Listener listener, Collector<SeaTunnelRow> output) throws IOException {
        buffer.clear();
        InetSocketAddress sender;
        try {
            sender = (InetSocketAddress) listener.channel.receive(buffer);
        } catch (ClosedChannelException e) {
            if (cancelled.get()) {
                return false;
            }
            throw e;
        }
        long now = System.nanoTime();
        if (cancelled.get()) {
            return false;
        }
        if (sender == null) {
            if (idleNanos > 0 && now - listener.lastReceived >= idleNanos) {
                finish(listener);
            }
            return false;
        }
        listener.lastReceived = now;
        if (buffer.position() > config.maxDatagram) {
            oversized.inc();
            reject(listener.port, "payload exceeds max_datagram_bytes");
        } else {
            buffer.flip();
            emit(listener.port, sender, output);
        }
        return true;
    }

    private void emit(int port, InetSocketAddress sender, Collector<SeaTunnelRow> output)
            throws IOException {
        List<Object[]> values;
        try {
            values =
                    config.format.decode(
                            buffer, config.separator, separatorPrefix, config.maxRecords);
        } catch (InvalidDatagram e) {
            malformed.inc();
            reject(port, e.getMessage());
            return;
        }
        String host = sender.getAddress().getHostAddress();
        for (int index = 0; index < values.size() && !cancelled.get(); index++) {
            Object[] payload = values.get(index);
            int size = payload.length;
            Object[] fields = Arrays.copyOf(payload, size + 4);
            fields[size] = host;
            fields[size + 1] = sender.getPort();
            fields[size + 2] = port;
            fields[size + 3] = index;
            SeaTunnelRow row = new SeaTunnelRow(fields);
            row.setTableId(tableId);
            output.collect(row);
        }
    }

    private void reject(int port, String reason) throws IOException {
        if (config.failMalformed) {
            throw new IOException("UDP source rejected a datagram on port " + port + ": " + reason);
        }
    }

    private void finish(Listener listener) {
        listener.finished = true;
        channels.remove(listener.port, listener.channel);
        closeChannel(listener.channel);
    }

    private static void closeChannel(DatagramChannel channel) {
        try {
            channel.close();
        } catch (IOException e) {
            log.warn("UDP source could not close a listener", e);
        }
    }

    private static int[] prefix(byte[] delimiter) {
        int[] prefix = new int[delimiter.length];
        int matched = 0;
        for (int i = 1; i < delimiter.length; i++) {
            while (matched > 0 && delimiter[i] != delimiter[matched]) {
                matched = prefix[matched - 1];
            }
            if (delimiter[i] == delimiter[matched]) {
                matched++;
            }
            prefix[i] = matched;
        }
        return prefix;
    }

    /**
     * Tracks one port and its inactivity/terminal state on the polling thread. Socket closure is
     *
     * <p>independently visible through the concurrent registry, so cancellation need not own this
     * object.
     */
    private static final class Listener {
        final int port;
        final DatagramChannel channel;
        boolean finished;
        long lastReceived = System.nanoTime();

        Listener(UdpSplit split, DatagramChannel channel) {
            this.port = split.port;
            this.channel = channel;
            this.finished = split.finished;
        }
    }
}
