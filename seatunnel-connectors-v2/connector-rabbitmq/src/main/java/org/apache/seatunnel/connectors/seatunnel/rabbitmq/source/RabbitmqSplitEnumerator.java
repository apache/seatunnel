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
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.split.RabbitmqSplit;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.split.RabbitmqSplitEnumeratorState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

public class RabbitmqSplitEnumerator
        implements SourceSplitEnumerator<RabbitmqSplit, RabbitmqSplitEnumeratorState> {

    private static final Logger log = LoggerFactory.getLogger(RabbitmqSplitEnumerator.class);
    private final Context<RabbitmqSplit> context;
    private final List<CatalogTable> catalogTables;
    private final ConcurrentLinkedQueue<RabbitmqSplit> pendingSplits;
    private final Object lock = new Object();

    public RabbitmqSplitEnumerator(
            Context<RabbitmqSplit> context, List<CatalogTable> catalogTables) {
        this.context = context;
        this.catalogTables = catalogTables;
        this.pendingSplits = new ConcurrentLinkedQueue<>();
    }

    @Override
    public void open() {
        // do nothing
    }

    @Override
    public void run() throws Exception {
        Set<RabbitmqSplit> discovered = discoverSplits();
        if (!discovered.isEmpty()) {
            pendingSplits.addAll(discovered);
            assignSplits();
        } else {
            log.warn("RUN WARNING: No splits were discovered! Check your config.");
        }
    }

    private Set<RabbitmqSplit> discoverSplits() {
        Set<RabbitmqSplit> splits = Collections.newSetFromMap(new HashMap<>());

        if (catalogTables == null || catalogTables.isEmpty()) {
            log.error("CRITICAL: No catalog tables provided to Enumerator!");
            return splits;
        }

        log.info("Enumerator starting discovery on {} tables...", catalogTables.size());

        int tableIndex = 0;
        for (CatalogTable table : catalogTables) {
            tableIndex++;
            Map<String, String> options = table.getOptions();

            log.info("--- Table #{} Options Dump ---", tableIndex);
            for (Map.Entry<String, String> entry : options.entrySet()) {
                log.info("   Key: '{}', Value: '{}'", entry.getKey(), entry.getValue());
            }
            String queueName = options.get("queue_name");
            if (queueName == null) {
                queueName = options.get("rabbitmq.queue.name");
            }
            if (queueName != null) {
                log.info(">>> SUCCESS: Discovered queue '{}' for table #{}", queueName, tableIndex);
                splits.add(new RabbitmqSplit(queueName, queueName));
            } else {
                log.error(
                        ">>> FAILURE: Could not find 'queue_name' in Table #{} options!",
                        tableIndex);
            }
        }

        log.info("Discovery finished. Found {} total splits.", splits.size());
        return splits;
    }

    private void assignSplits() {
        synchronized (lock) {
            if (context.registeredReaders().isEmpty()) {
                log.info("No readers registered yet. Splits will be assigned later.");
                return;
            }

            int readerId = 0;

            List<RabbitmqSplit> splitsToAssign = new ArrayList<>();
            while (!pendingSplits.isEmpty()) {
                splitsToAssign.add(pendingSplits.poll());
            }

            if (!splitsToAssign.isEmpty()) {
                log.info("Assigning {} splits to reader {}", splitsToAssign.size(), readerId);
                context.assignSplit(readerId, splitsToAssign);
                context.signalNoMoreSplits(readerId);
            }
        }
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }

    @Override
    public void addSplitsBack(List<RabbitmqSplit> splits, int subtaskId) {
        synchronized (lock) {
            if (splits != null) {
                pendingSplits.addAll(splits);
                assignSplits();
            }
        }
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
        log.info("Reader {} registered. Checking for pending splits...", subtaskId);
        assignSplits();
    }

    @Override
    public RabbitmqSplitEnumeratorState snapshotState(long checkpointId) throws Exception {
        return new RabbitmqSplitEnumeratorState();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // do nothing
    }
}
