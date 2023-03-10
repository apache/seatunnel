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

package org.apache.seatunnel.connectors.seatunnel.fts.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import org.apache.flink.table.store.table.Table;
import org.apache.flink.table.store.table.source.Split;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class FlinkTableStoreSourceSplitEnumerator
        implements SourceSplitEnumerator<FlinkTableStoreSourceSplit, FlinkTableStoreSourceState> {

    /** Source split enumerator context */
    private final Context<FlinkTableStoreSourceSplit> context;

    /** The splits that has assigned */
    private final Set<FlinkTableStoreSourceSplit> assignedSplit;

    /** The splits that have not assigned */
    private Set<FlinkTableStoreSourceSplit> pendingSplit;

    /** The table that wants to read */
    private final Table table;

    public FlinkTableStoreSourceSplitEnumerator(
            Context<FlinkTableStoreSourceSplit> context, Table table) {
        this.context = context;
        this.table = table;
        this.assignedSplit = new HashSet<>();
    }

    public FlinkTableStoreSourceSplitEnumerator(
            Context<FlinkTableStoreSourceSplit> context,
            Table table,
            FlinkTableStoreSourceState sourceState) {
        this.context = context;
        this.table = table;
        this.assignedSplit = sourceState.getAssignedSplits();
    }

    @Override
    public void open() {
        this.pendingSplit = new HashSet<>();
    }

    @Override
    public void run() throws Exception {
        // do nothing
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }

    @Override
    public void addSplitsBack(List<FlinkTableStoreSourceSplit> splits, int subtaskId) {
        if (!splits.isEmpty()) {
            pendingSplit.addAll(splits);
            assignSplit(subtaskId);
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplit.size();
    }

    @Override
    public void registerReader(int subtaskId) {
        pendingSplit = getTableSplits();
        assignSplit(subtaskId);
    }

    @Override
    public FlinkTableStoreSourceState snapshotState(long checkpointId) throws Exception {
        return new FlinkTableStoreSourceState(assignedSplit);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // do nothing
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        // do nothing
    }

    /** Assign split by reader task id */
    private void assignSplit(int taskId) {
        ArrayList<FlinkTableStoreSourceSplit> currentTaskSplits = new ArrayList<>();
        if (context.currentParallelism() == 1) {
            // if parallelism == 1, we should assign all the splits to reader
            currentTaskSplits.addAll(pendingSplit);
        } else {
            // if parallelism > 1, according to hashCode of split's id to determine whether to
            // allocate the current task
            for (FlinkTableStoreSourceSplit fileSourceSplit : pendingSplit) {
                int splitOwner =
                        getSplitOwner(fileSourceSplit.splitId(), context.currentParallelism());
                if (splitOwner == taskId) {
                    currentTaskSplits.add(fileSourceSplit);
                }
            }
        }
        // assign splits
        context.assignSplit(taskId, currentTaskSplits);
        // save the state of assigned splits
        assignedSplit.addAll(currentTaskSplits);
        // remove the assigned splits from pending splits
        currentTaskSplits.forEach(split -> pendingSplit.remove(split));
        log.info(
                "SubTask {} is assigned to [{}]",
                taskId,
                currentTaskSplits.stream()
                        .map(FlinkTableStoreSourceSplit::splitId)
                        .collect(Collectors.joining(",")));
        context.signalNoMoreSplits(taskId);
    }

    /** Get all splits of table */
    private Set<FlinkTableStoreSourceSplit> getTableSplits() {
        Set<FlinkTableStoreSourceSplit> tableSplits = new HashSet<>();
        // TODO Support columns projection
        List<Split> splits = table.newScan().plan().splits();
        splits.forEach(split -> tableSplits.add(new FlinkTableStoreSourceSplit(split)));
        return tableSplits;
    }

    /** Hash algorithm for assigning splits to readers */
    private static int getSplitOwner(String tp, int numReaders) {
        return (tp.hashCode() & Integer.MAX_VALUE) % numReaders;
    }
}
