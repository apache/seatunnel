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

package org.apache.seatunnel.connectors.seatunnel.paimon.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import org.apache.commons.collections.map.HashedMap;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.Split;

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
import java.util.stream.Collectors;

/** Paimon source split enumerator, used to calculate the splits for every reader. */
@Slf4j
public class PaimonSourceSplitEnumerator
        implements SourceSplitEnumerator<PaimonSourceSplit, PaimonSourceState> {

    /** Source split enumerator context */
    private final Context<PaimonSourceSplit> context;

    private Map<Integer, List<PaimonSourceSplit>> pendingSplit;

    private volatile boolean shouldEnumerate;

    private final Object stateLock = new Object();

    /** The table that wants to read */
    private final Table table;

    public PaimonSourceSplitEnumerator(Context<PaimonSourceSplit> context, Table table) {
        this(context, table, null);
    }

    public PaimonSourceSplitEnumerator(
            Context<PaimonSourceSplit> context, Table table, PaimonSourceState sourceState) {
        this.context = context;
        this.table = table;
        this.pendingSplit = new HashMap<>();
        this.shouldEnumerate = sourceState == null;
        if (sourceState != null) {
            this.shouldEnumerate = sourceState.isShouldEnumerate();
            this.pendingSplit.putAll(sourceState.getPendingSplits());
        }
    }

    @Override
    public void open() {
        this.pendingSplit = new HashedMap();
    }

    @Override
    public void run() throws Exception {
        Set<Integer> readers = context.registeredReaders();
        if (shouldEnumerate) {
            Set<PaimonSourceSplit> newSplits = getTableSplits();
            synchronized (stateLock) {
                addPendingSplit(newSplits);
                shouldEnumerate = false;
            }

            assignSplit(readers);
        }
        log.debug(
                "No more splits to assign." + " Sending NoMoreSplitsEvent to reader {}.", readers);
        readers.forEach(context::signalNoMoreSplits);
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }

    @Override
    public void addSplitsBack(List<PaimonSourceSplit> splits, int subtaskId) {
        if (!splits.isEmpty()) {
            addPendingSplit(splits);
            assignSplit(Collections.singletonList(subtaskId));
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplit.size();
    }

    @Override
    public void registerReader(int subtaskId) {
        log.debug("Register reader {} to PaimonSourceSplitEnumerator.", subtaskId);
        if (!pendingSplit.isEmpty()) {
            assignSplit(Collections.singletonList(subtaskId));
        }
    }

    @Override
    public PaimonSourceState snapshotState(long checkpointId) throws Exception {
        synchronized (stateLock) {
            return new PaimonSourceState(pendingSplit, shouldEnumerate);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // do nothing
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        // do nothing
    }

    private void addPendingSplit(Collection<PaimonSourceSplit> splits) {
        int readerCount = context.currentParallelism();
        for (PaimonSourceSplit split : splits) {
            int ownerReader = getSplitOwner(split.splitId(), readerCount);
            log.info("Assigning {} to {} reader.", split.getSplit().toString(), ownerReader);
            pendingSplit.computeIfAbsent(ownerReader, r -> new ArrayList<>()).add(split);
        }
    }

    /** Assign split by reader task id */
    private void assignSplit(Collection<Integer> readers) {

        log.debug("Assign pendingSplits to readers {}", readers);

        for (int reader : readers) {
            List<PaimonSourceSplit> assignmentForReader = pendingSplit.remove(reader);
            if (assignmentForReader != null && !assignmentForReader.isEmpty()) {
                log.info(
                        "Assign splits {} to reader {}",
                        assignmentForReader.stream()
                                .map(p -> p.getSplit().toString())
                                .collect(Collectors.joining(",")),
                        reader);
                try {
                    context.assignSplit(reader, assignmentForReader);
                } catch (Exception e) {
                    log.error(
                            "Failed to assign splits {} to reader {}",
                            assignmentForReader,
                            reader,
                            e);
                    pendingSplit.put(reader, assignmentForReader);
                }
            }
        }
    }

    /** Get all splits of table */
    private Set<PaimonSourceSplit> getTableSplits() {
        final Set<PaimonSourceSplit> tableSplits = new HashSet<>();
        // TODO Support columns projection
        final List<Split> splits = table.newReadBuilder().newScan().plan().splits();
        splits.forEach(split -> tableSplits.add(new PaimonSourceSplit(split)));
        return tableSplits;
    }

    /** Hash algorithm for assigning splits to readers */
    private static int getSplitOwner(String tp, int numReaders) {
        return (tp.hashCode() & Integer.MAX_VALUE) % numReaders;
    }
}
