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
import org.apache.seatunnel.connectors.seatunnel.tablestore.config.TablestoreOptions;

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
public class TableStoreDBSourceSplitEnumerator
        implements SourceSplitEnumerator<TableStoreDBSourceSplit, TableStoreDBSourceState> {

    private final SourceSplitEnumerator.Context<TableStoreDBSourceSplit> enumeratorContext;
    private final Map<Integer, List<TableStoreDBSourceSplit>> pendingSplits;
    private final TablestoreOptions tablestoreOptions;

    private final Object stateLock = new Object();
    private volatile boolean shouldEnumerate;

    /**
     * @param enumeratorContext
     * @param tablestoreOptions
     */
    public TableStoreDBSourceSplitEnumerator(
            Context<TableStoreDBSourceSplit> enumeratorContext,
            TablestoreOptions tablestoreOptions) {
        this(enumeratorContext, tablestoreOptions, null);
    }

    public TableStoreDBSourceSplitEnumerator(
            Context<TableStoreDBSourceSplit> enumeratorContext,
            TablestoreOptions tablestoreOptions,
            TableStoreDBSourceState sourceState) {
        this.enumeratorContext = enumeratorContext;
        this.tablestoreOptions = tablestoreOptions;
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
            Set<TableStoreDBSourceSplit> newSplits = getTableStoreDBSourceSplit();
            synchronized (stateLock) {
                addPendingSplit(newSplits);
                shouldEnumerate = false;
            }
            assignSplit(readers);
        }
    }

    private void assignSplit(Set<Integer> readers) {
        for (int reader : readers) {
            List<TableStoreDBSourceSplit> assignmentForReader = pendingSplits.remove(reader);
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

    private Set<TableStoreDBSourceSplit> getTableStoreDBSourceSplit() {

        Set<TableStoreDBSourceSplit> allSplit = new HashSet<>();
        String tables = tablestoreOptions.getTable();
        String[] tableArr = tables.split(",");
        for (int i = 0; i < tableArr.length; i++) {
            allSplit.add(
                    new TableStoreDBSourceSplit(
                            i, tableArr[i], tablestoreOptions.getPrimaryKeys().get(i)));
        }
        return allSplit;
    }

    private void addPendingSplit(Collection<TableStoreDBSourceSplit> splits) {
        int readerCount = enumeratorContext.currentParallelism();
        for (TableStoreDBSourceSplit split : splits) {
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
    public void addSplitsBack(List<TableStoreDBSourceSplit> splits, int subtaskId) {
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
    public TableStoreDBSourceState snapshotState(long checkpointId) throws Exception {
        synchronized (stateLock) {
            return new TableStoreDBSourceState(shouldEnumerate, pendingSplits);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
