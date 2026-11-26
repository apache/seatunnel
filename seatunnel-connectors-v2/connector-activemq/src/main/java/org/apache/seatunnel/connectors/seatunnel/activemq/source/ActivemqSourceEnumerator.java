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

package org.apache.seatunnel.connectors.seatunnel.activemq.source;

import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Assigns logical consumer slots, not queue offsets; broker acknowledgements determine replay. */
public class ActivemqSourceEnumerator
        implements SourceSplitEnumerator<
                ActivemqSourceEnumerator.Split, ActivemqSourceEnumerator.State> {
    private final Context<Split> context;
    private final Map<Integer, Split> pending = new LinkedHashMap<>();
    private int consumerSlots;

    public ActivemqSourceEnumerator(Context<Split> context) {
        this.context = context;
    }

    public ActivemqSourceEnumerator(Context<Split> context, State state) {
        this.context = context;
        this.consumerSlots = state.consumerSlots;
        state.pending.forEach(split -> pending.put(split.id, split));
    }

    @Override
    public void open() {}

    /** Preserve restored ownership, adding only new consumer slots when parallelism increases. */
    @Override
    public synchronized void run() {
        for (int i = consumerSlots; i < context.currentParallelism(); i++) {
            pending.putIfAbsent(i, new Split(i));
        }
        consumerSlots = Math.max(consumerSlots, context.currentParallelism());
        for (Integer reader : context.registeredReaders()) {
            assign(reader);
        }
    }

    private void assign(int reader) {
        if (consumerSlots == 0) {
            return;
        }
        List<Split> splits = new ArrayList<>();
        for (Split split : pending.values()) {
            if (split.id % context.currentParallelism() == reader) {
                splits.add(split);
            }
        }
        if (!splits.isEmpty()) {
            context.assignSplit(reader, splits);
            splits.forEach(split -> pending.remove(split.id));
        }
        context.signalNoMoreSplits(reader);
    }

    @Override
    public synchronized void addSplitsBack(List<Split> splits, int subtaskId) {
        splits.forEach(split -> pending.put(split.id, split));
        for (Integer reader : context.registeredReaders()) {
            assign(reader);
        }
    }

    @Override
    public synchronized void registerReader(int subtaskId) {
        assign(subtaskId);
    }

    @Override
    public synchronized void handleSplitRequest(int subtaskId) {
        assign(subtaskId);
    }

    @Override
    public synchronized int currentUnassignedSplitSize() {
        return pending.size();
    }

    @Override
    public synchronized State snapshotState(long checkpointId) {
        return new State(consumerSlots, new ArrayList<>(pending.values()));
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    @Override
    public void close() {}

    public static final class Split implements SourceSplit {
        private static final long serialVersionUID = 1L;
        private final int id;

        public Split(int id) {
            this.id = id;
        }

        @Override
        public String splitId() {
            return "consumer-" + id;
        }
    }

    public static final class State implements Serializable {
        private static final long serialVersionUID = 1L;
        private final int consumerSlots;
        private final List<Split> pending;

        private State(int consumerSlots, List<Split> pending) {
            this.consumerSlots = consumerSlots;
            this.pending = pending;
        }
    }
}
