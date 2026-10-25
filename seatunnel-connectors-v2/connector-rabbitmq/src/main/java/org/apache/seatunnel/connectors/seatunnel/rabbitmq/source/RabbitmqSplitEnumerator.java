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

package org.apache.seatunnel.connectors.seatunnel.rabbitmq.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqConfig;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.split.RabbitmqSplit;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.split.RabbitmqSplitEnumeratorState;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The split enumerator for the RabbitMQ source.
 *
 * <p>This class is responsible for discovering the configured RabbitMQ queues (splits),
 * distributing them to the registered source readers, and managing the assignment state to prevent
 * data duplication during failover recovery.
 */
@Slf4j
public class RabbitmqSplitEnumerator
        implements SourceSplitEnumerator<RabbitmqSplit, RabbitmqSplitEnumeratorState> {

    private final SourceSplitEnumerator.Context<RabbitmqSplit> context;
    private final Map<String, RabbitmqSplit> pendingSplits = new ConcurrentHashMap<>();
    private final Map<String, RabbitmqSplit> assignedSplits = new ConcurrentHashMap<>();
    private final Object stateLock = new Object();
    private final Set<Integer> assignedReaders = Collections.synchronizedSet(new HashSet<>());

    /**
     * Constructor for RabbitmqSplitEnumerator.
     *
     * @param context enumerator context
     * @param rabbitmqConfig rabbitmq config
     * @param queues list of queue names to consume
     */
    public RabbitmqSplitEnumerator(
            SourceSplitEnumerator.Context<RabbitmqSplit> context,
            RabbitmqConfig rabbitmqConfig,
            List<String> queues) {
        this.context = context;
        for (String queue : queues) {
            log.info("Discovered queue for processing: {}", queue);
            this.pendingSplits.put(queue, new RabbitmqSplit(queue));
        }
    }

    /**
     * Constructor for restoring RabbitmqSplitEnumerator from state.
     *
     * @param context enumerator context
     * @param rabbitmqConfig rabbitmq config
     * @param queues list of queue names to consume
     * @param checkpointState checkpoint state
     */
    public RabbitmqSplitEnumerator(
            SourceSplitEnumerator.Context<RabbitmqSplit> context,
            RabbitmqConfig rabbitmqConfig,
            List<String> queues,
            RabbitmqSplitEnumeratorState checkpointState) {
        this(context, rabbitmqConfig, queues);

        if (checkpointState != null && checkpointState.getAssignedSplits() != null) {
            this.assignedSplits.putAll(checkpointState.getAssignedSplits());
            for (String assignedSplitId : checkpointState.getAssignedSplits().keySet()) {
                this.pendingSplits.remove(assignedSplitId);
            }
            log.info("Restored from state. Already assigned splits: {}", assignedSplits.keySet());
        }
    }

    @Override
    public void run() {
        assignSplitsToReaders();
    }

    @Override
    public void open() {
        // do nothing
    }

    private void assignSplitsToReaders() {
        synchronized (stateLock) {
            Set<Integer> registeredReaders = context.registeredReaders();
            if (registeredReaders.isEmpty()) {
                return;
            }
            if (!pendingSplits.isEmpty()) {
                List<String> splitIds = new ArrayList<>(pendingSplits.keySet());
                int numReaders = registeredReaders.size();
                List<Integer> readersList = new ArrayList<>(registeredReaders);
                Collections.sort(readersList);

                for (int i = 0; i < splitIds.size(); i++) {
                    String splitId = splitIds.get(i);
                    int readerId = readersList.get(i % numReaders);

                    RabbitmqSplit split = pendingSplits.remove(splitId);
                    if (split != null) {
                        context.assignSplit(readerId, split);
                        assignedSplits.put(splitId, split);
                        log.info("Assigned split {} to reader {}", splitId, readerId);
                    }
                }
            }
            if (pendingSplits.isEmpty()) {
                for (int readerId : registeredReaders) {
                    context.signalNoMoreSplits(readerId);
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }

    @Override
    public void addSplitsBack(List<RabbitmqSplit> splits, int subtaskId) {
        log.info("Splits returned from reader {}: {}", subtaskId, splits);
        synchronized (stateLock) {
            for (RabbitmqSplit split : splits) {
                pendingSplits.put(split.splitId(), split);
                assignedSplits.remove(split.splitId());
            }
        }
        assignSplitsToReaders();
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplits.size();
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        // do nothing
    }

    @Override
    public void registerReader(int subtaskId) {
        log.info("Reader {} registered", subtaskId);
        assignedReaders.add(subtaskId);
        assignSplitsToReaders();
    }

    @Override
    public RabbitmqSplitEnumeratorState snapshotState(long checkpointId) throws Exception {
        synchronized (stateLock) {
            return new RabbitmqSplitEnumeratorState(new HashMap<>(assignedSplits));
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // do nothing
    }
}
