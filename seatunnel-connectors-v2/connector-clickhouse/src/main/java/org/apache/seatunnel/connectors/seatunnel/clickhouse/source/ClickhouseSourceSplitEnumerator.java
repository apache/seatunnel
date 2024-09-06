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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseCatalogConfig;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.state.ClickhouseSourceState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ClickhouseSourceSplitEnumerator
        implements SourceSplitEnumerator<ClickhouseSourceSplit, ClickhouseSourceState> {

    private static final Logger log =
            LoggerFactory.getLogger(ClickhouseSourceSplitEnumerator.class);

    private final Context<ClickhouseSourceSplit> context;
    private final Map<Integer, List<ClickhouseSourceSplit>> pendingSplits;
    private final ConcurrentLinkedQueue<TablePath> pendingTables;
    private final Object stateLock = new Object();
    private final Map<TablePath, ClickhouseCatalogConfig> tableClickhouseCatalogConfigMap;

    public ClickhouseSourceSplitEnumerator(
            Context<ClickhouseSourceSplit> enumeratorContext,
            Map<TablePath, ClickhouseCatalogConfig> tableClickhouseCatalogConfigMap) {
        this(enumeratorContext, tableClickhouseCatalogConfigMap, null);
    }

    public ClickhouseSourceSplitEnumerator(
            Context<ClickhouseSourceSplit> enumeratorContext,
            Map<TablePath, ClickhouseCatalogConfig> tableClickhouseCatalogConfigMap,
            ClickhouseSourceState checkpointState) {
        this.context = enumeratorContext;
        this.tableClickhouseCatalogConfigMap = tableClickhouseCatalogConfigMap;
        if (checkpointState == null) {
            this.pendingTables =
                    new ConcurrentLinkedQueue<>(tableClickhouseCatalogConfigMap.keySet());
            this.pendingSplits = new HashMap<>();
        } else {
            this.pendingTables = new ConcurrentLinkedQueue<>(checkpointState.getPendingTables());
            this.pendingSplits = new HashMap<>(checkpointState.getPendingSplits());
        }
    }

    @Override
    public void open() {}

    @Override
    public void run() throws Exception {
        Set<Integer> readers = context.registeredReaders();
        while (!pendingTables.isEmpty()) {
            synchronized (stateLock) {
                TablePath tablePath = pendingTables.poll();
                log.info("Splitting table {}.", tablePath);

                Collection<ClickhouseSourceSplit> splits =
                        discoverySplits(tableClickhouseCatalogConfigMap.get(tablePath));
                log.info("Split table {} into {} splits.", tablePath, splits.size());

                addPendingSplit(splits);
            }

            synchronized (stateLock) {
                assignSplit(readers);
            }
        }
    }

    @Override
    public void close() throws IOException {}

    private Collection<ClickhouseSourceSplit> discoverySplits(
            ClickhouseCatalogConfig clickhouseCatalogConfig) {
        // todo Multiple splits are returned while waiting to support slice reading
        ClickhouseSourceSplit clickhouseSourceSplit = new ClickhouseSourceSplit();
        clickhouseSourceSplit.setTablePath(
                clickhouseCatalogConfig.getCatalogTable().getTablePath());
        clickhouseSourceSplit.setClickhouseCatalogConfig(clickhouseCatalogConfig);
        HashSet<ClickhouseSourceSplit> splitSet = new HashSet<>();
        splitSet.add(clickhouseSourceSplit);
        return splitSet;
    }

    private void addPendingSplit(Collection<ClickhouseSourceSplit> splits) {
        int readerCount = context.currentParallelism();
        for (ClickhouseSourceSplit split : splits) {
            // Since there's only one split for each tablePath, we'll assign the Task to the
            // tablePath for now
            int ownerReader = getSplitOwner(split.getTablePath().toString(), readerCount);
            log.info("Assigning {} to {} reader.", split, ownerReader);
            pendingSplits.computeIfAbsent(ownerReader, r -> new ArrayList<>()).add(split);
        }
    }

    private void assignSplit(Collection<Integer> readers) {
        log.debug("Assign pendingSplits to readers {}", readers);

        for (int reader : readers) {
            List<ClickhouseSourceSplit> assignmentForReader = pendingSplits.remove(reader);
            if (assignmentForReader != null && !assignmentForReader.isEmpty()) {
                log.info("Assign splits {} to reader {}", assignmentForReader, reader);
                try {
                    context.assignSplit(reader, assignmentForReader);
                    context.signalNoMoreSplits(reader);
                } catch (Exception e) {
                    log.error(
                            "Failed to assign splits {} to reader {}",
                            assignmentForReader,
                            reader,
                            e);
                    pendingSplits.put(reader, assignmentForReader);
                }
            }
        }
    }

    private int getSplitOwner(String splitId, int numReaders) {
        return (splitId.hashCode() & Integer.MAX_VALUE) % numReaders;
    }

    @Override
    public void addSplitsBack(List<ClickhouseSourceSplit> splits, int subtaskId) {
        if (!splits.isEmpty()) {
            synchronized (stateLock) {
                addPendingSplit(splits, subtaskId);
                if (context.registeredReaders().contains(subtaskId)) {
                    assignSplit(Collections.singletonList(subtaskId));
                } else {
                    log.warn(
                            "Reader {} is not registered. Pending splits {} are not assigned.",
                            subtaskId,
                            splits);
                }
            }
        }
        log.info("Add back splits {} to ClickhouseSourceSplitEnumerator.", splits.size());
    }

    private void addPendingSplit(Collection<ClickhouseSourceSplit> splits, int ownerReader) {
        pendingSplits.computeIfAbsent(ownerReader, r -> new ArrayList<>()).addAll(splits);
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplits.size();
    }

    @Override
    public void handleSplitRequest(int subtaskId) {}

    @Override
    public void registerReader(int subtaskId) {
        log.debug("Register reader {} to ClickhouseSourceSplitEnumerator.", subtaskId);
        synchronized (stateLock) {
            if (!pendingSplits.isEmpty()) {
                assignSplit(Collections.singletonList(subtaskId));
            }
        }
    }

    @Override
    public ClickhouseSourceState snapshotState(long checkpointId) throws Exception {
        synchronized (stateLock) {
            return new ClickhouseSourceState(
                    new ArrayList(pendingTables), new HashMap<>(pendingSplits));
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
