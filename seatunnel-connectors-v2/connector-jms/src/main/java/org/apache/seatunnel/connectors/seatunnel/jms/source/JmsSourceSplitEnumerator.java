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

package org.apache.seatunnel.connectors.seatunnel.jms.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Assigns one logical consumer slot per reader, as the ActiveMQ source does. Slots are kept across
 * restores and only added when parallelism grows.
 */
public class JmsSourceSplitEnumerator
        implements SourceSplitEnumerator<JmsSourceSplit, JmsSourceState> {
    private final Context<JmsSourceSplit> context;
    private final Map<Integer, JmsSourceSplit> unassigned = new LinkedHashMap<>();
    private int consumerSlots;

    public JmsSourceSplitEnumerator(Context<JmsSourceSplit> context) {
        this.context = context;
    }

    public JmsSourceSplitEnumerator(Context<JmsSourceSplit> context, JmsSourceState state) {
        this.context = context;
        this.consumerSlots = state.getConsumerSlots();
        state.getUnassigned().forEach(split -> unassigned.put(split.getId(), split));
    }

    @Override
    public void open() {}

    @Override
    public synchronized void run() {
        for (int i = consumerSlots; i < context.currentParallelism(); i++) {
            unassigned.putIfAbsent(i, new JmsSourceSplit(i));
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
        List<JmsSourceSplit> splits = new ArrayList<>();
        for (JmsSourceSplit split : unassigned.values()) {
            if (split.getId() % context.currentParallelism() == reader) {
                splits.add(split);
            }
        }
        if (!splits.isEmpty()) {
            context.assignSplit(reader, splits);
            splits.forEach(split -> unassigned.remove(split.getId()));
        }
        context.signalNoMoreSplits(reader);
    }

    @Override
    public synchronized void addSplitsBack(List<JmsSourceSplit> splits, int subtaskId) {
        splits.forEach(split -> unassigned.put(split.getId(), split));
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
        return unassigned.size();
    }

    @Override
    public synchronized JmsSourceState snapshotState(long checkpointId) {
        return new JmsSourceState(consumerSlots, new ArrayList<>(unassigned.values()));
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    @Override
    public void close() {}
}
