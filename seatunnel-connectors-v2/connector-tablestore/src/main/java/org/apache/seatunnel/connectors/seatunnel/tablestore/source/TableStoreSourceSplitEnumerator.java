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
package org.apache.seatunnel.connectors.seatunnel.tablestore.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.tablestore.config.TableStoreConfig;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class TableStoreSourceSplitEnumerator
        implements SourceSplitEnumerator<TableStoreSourceSplit, TableStoreSourceState> {

    private final SourceSplitEnumerator.Context<TableStoreSourceSplit> enumeratorContext;
    private final Map<Integer, List<TableStoreSourceSplit>> pendingSplits;
    private final TableStoreConfig tableStoreConfig;

    private final Object stateLock = new Object();
    private volatile boolean shouldEnumerate;

    /**
     * @param enumeratorContext
     * @param tableStoreConfig
     */
    public TableStoreSourceSplitEnumerator(
            Context<TableStoreSourceSplit> enumeratorContext, TableStoreConfig tableStoreConfig) {
        this(enumeratorContext, tableStoreConfig, null);
    }

    public TableStoreSourceSplitEnumerator(
            Context<TableStoreSourceSplit> enumeratorContext,
            TableStoreConfig tableStoreConfig,
            TableStoreSourceState sourceState) {
        this.enumeratorContext = enumeratorContext;
        this.tableStoreConfig = tableStoreConfig;
        this.pendingSplits = new HashMap<>();
        this.shouldEnumerate = sourceState == null;
        if (sourceState != null) {
            this.shouldEnumerate = sourceState.isShouldEnumerate();
            this.pendingSplits.putAll(sourceState.getPendingSplits());
        }
    }

    @Override
    public void open() {}

    @Override
    public void run() throws Exception {
        Set<Integer> readers = enumeratorContext.registeredReaders();
        if (shouldEnumerate) {
            Set<TableStoreSourceSplit> newSplits = getTableStoreDBSourceSplit();
            synchronized (stateLock) {
                addPendingSplit(newSplits);
                shouldEnumerate = false;
            }
            assignSplit(readers);
        }
    }

    private void assignSplit(Set<Integer> readers) {
        for (int reader : readers) {
            List<TableStoreSourceSplit> assignmentForReader = pendingSplits.remove(reader);
            if (assignmentForReader != null && !assignmentForReader.isEmpty()) {
                log.info("Assign splits {} to reader {}", assignmentForReader, reader);
                try {
                    enumeratorContext.assignSplit(reader, assignmentForReader);
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

    private Set<TableStoreSourceSplit> getTableStoreDBSourceSplit() {

        Set<TableStoreSourceSplit> allSplit = new HashSet<>();
        String tables = tableStoreConfig.getTable();
        String[] tableArr = tables.split(",");
        for (int i = 0; i < tableArr.length; i++) {
            allSplit.add(
                    new TableStoreSourceSplit(
                            i, tableArr[i], tableStoreConfig.getPrimaryKeys().get(i)));
        }
        return allSplit;
    }

    private void addPendingSplit(Collection<TableStoreSourceSplit> splits) {
        int readerCount = enumeratorContext.currentParallelism();
        for (TableStoreSourceSplit split : splits) {
            int ownerReader = split.getSplitId() % readerCount;
            pendingSplits.computeIfAbsent(ownerReader, k -> new ArrayList<>()).add(split);
        }
    }

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'close'");
    }

    @Override
    public void addSplitsBack(List<TableStoreSourceSplit> splits, int subtaskId) {
        log.debug("Add back splits {} to tablestore.", splits);
        if (!splits.isEmpty()) {
            addPendingSplit(splits);
            assignSplit(Collections.singleton(subtaskId));
            enumeratorContext.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplits.size();
    }

    @Override
    public void handleSplitRequest(int subtaskId) {}

    @Override
    public void registerReader(int subtaskId) {
        log.debug("Register reader {} to TablestoreSplitEnumerator.", subtaskId);
        if (!pendingSplits.isEmpty()) {
            assignSplit(Collections.singleton(subtaskId));
        }
    }

    @Override
    public TableStoreSourceState snapshotState(long checkpointId) throws Exception {
        synchronized (stateLock) {
            return new TableStoreSourceState(shouldEnumerate, pendingSplits);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
