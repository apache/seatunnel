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

import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Assigns each configured port to one reader and retains only coordinator-owned pending splits.
 *
 * <p>Late restored terminal flags participate in assignment accounting before no-more-split
 * signalling.
 */
final class UdpEnumerator implements SourceSplitEnumerator<UdpSplit, UdpEnumerator.State> {
    private final Context<UdpSplit> context;
    private final List<Integer> ports;
    // Coordinator-owned until assignSplit returns; reader snapshots then retain port progress.
    private final Map<Integer, UdpSplit> pending = new LinkedHashMap<>();
    // Tracks delivered ports so an early run cannot signal completion before late restored splits.
    private final Set<Integer> accounted = new HashSet<>();
    private final Set<Integer> signaled = new HashSet<>();
    private boolean running;

    UdpEnumerator(Context<UdpSplit> context, List<Integer> ports, State state) {
        this.context = context;
        this.ports = ports;
        if (context.currentParallelism() < 1 || context.currentParallelism() > ports.size()) {
            throw UdpConfig.invalid("source parallelism must be between 1 and the number of ports");
        }
        if (state == null) {
            ports.forEach(port -> pending.put(port, new UdpSplit(port, false, 0)));
        } else {
            if (!ports.equals(state.ports)) {
                throw UdpConfig.invalid("ports cannot change during checkpoint restore");
            }
            state.pending.forEach(split -> pending.put(split.port, split));
        }
    }

    @Override
    public void open() {}

    @Override
    public synchronized void run() {
        running = true;
        assignRegisteredReaders();
    }

    @Override
    public synchronized void close() {
        running = false;
    }

    /**
     * Merge restored reader ownership without discarding a previously terminal port. Reader states
     *
     * <p>can arrive after coordinator startup, so completion signalling waits for all expected
     * ports.
     */
    @Override
    public synchronized void addSplitsBack(List<UdpSplit> splits, int subtaskId) {
        for (UdpSplit split : splits) {
            if (!ports.contains(split.port)) {
                throw UdpConfig.invalid("checkpoint contains a port absent from this source");
            }
            pending.merge(split.port, split, (first, second) -> first.finished ? first : second);
        }
        if (running) {
            assignRegisteredReaders();
        }
    }

    @Override
    public synchronized int currentUnassignedSplitSize() {
        return pending.size();
    }

    @Override
    public synchronized void handleSplitRequest(int subtaskId) {
        if (running) {
            assign(subtaskId);
        }
    }

    @Override
    public synchronized void registerReader(int subtaskId) {
        if (running) {
            assign(subtaskId);
        }
    }

    /**
     * In-flight listeners are recorded by their readers. Retain coordinator-owned pending ports so
     * an earlier checkpoint taken before delivery can still start the listener.
     */
    @Override
    public synchronized State snapshotState(long checkpointId) {
        return new State(new ArrayList<>(ports), new ArrayList<>(pending.values()));
    }

    private void assignRegisteredReaders() {
        for (int reader : context.registeredReaders()) {
            assign(reader);
        }
    }

    private void assign(int reader) {
        int parallelism = context.currentParallelism();
        int expected = (ports.size() - 1 - reader) / parallelism + 1;
        List<UdpSplit> assignment = new ArrayList<>();
        for (int index = reader; index < ports.size(); index += parallelism) {
            int port = ports.get(index);
            UdpSplit split = pending.get(port);
            if (split != null) {
                assignment.add(new UdpSplit(port, split.finished, expected));
            }
        }
        if (!assignment.isEmpty()) {
            context.assignSplit(reader, assignment);
            for (UdpSplit split : assignment) {
                pending.remove(split.port);
                accounted.add(split.port);
            }
        }
        // Do not signal an idle-timeout reader before all late restore assignments are known.
        int accountedForReader = 0;
        for (int index = reader; index < ports.size(); index += parallelism) {
            if (accounted.contains(ports.get(index))) {
                accountedForReader++;
            }
        }
        if (accountedForReader == expected && signaled.add(reader)) {
            context.signalNoMoreSplits(reader);
        }
    }

    /**
     * Stores the configured port identities and coordinator-owned pending assignments for
     * restoration.
     *
     * <p>Assigned listeners are checkpointed by readers; this state contains no packets or replay
     * offset.
     */
    static final class State implements Serializable {
        private static final long serialVersionUID = 1L;
        private final List<Integer> ports;
        private final List<UdpSplit> pending;

        private State(List<Integer> ports, List<UdpSplit> pending) {
            this.ports = ports;
            this.pending = pending;
        }
    }
}
