/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.hbase.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.hbase.client.HbaseClient;
import org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseParameters;

import org.apache.hadoop.hbase.client.RegionLocator;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class HbaseSourceSplitEnumerator
        implements SourceSplitEnumerator<HbaseSourceSplit, HbaseSourceState> {
    /** Source split enumerator context */
    private final Context<HbaseSourceSplit> context;

    /** The splits that has assigned */
    private final Set<HbaseSourceSplit> assignedSplit;

    /** The splits that have not assigned */
    private Set<HbaseSourceSplit> pendingSplit;

    private HbaseParameters hbaseParameters;

    private HbaseClient hbaseClient;

    public HbaseSourceSplitEnumerator(
            Context<HbaseSourceSplit> context, HbaseParameters hbaseParameters) {
        this(context, hbaseParameters, new HashSet<>());
    }

    public HbaseSourceSplitEnumerator(
            Context<HbaseSourceSplit> context,
            HbaseParameters hbaseParameters,
            HbaseSourceState sourceState) {
        this(context, hbaseParameters, sourceState.getAssignedSplits());
    }

    private HbaseSourceSplitEnumerator(
            Context<HbaseSourceSplit> context,
            HbaseParameters hbaseParameters,
            Set<HbaseSourceSplit> assignedSplit) {
        this.context = context;
        this.hbaseParameters = hbaseParameters;
        this.assignedSplit = assignedSplit;
        this.hbaseClient = HbaseClient.createInstance(hbaseParameters);
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
    public void addSplitsBack(List<HbaseSourceSplit> splits, int subtaskId) {
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
    public HbaseSourceState snapshotState(long checkpointId) throws Exception {
        return new HbaseSourceState(assignedSplit);
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
        ArrayList<HbaseSourceSplit> currentTaskSplits = new ArrayList<>();
        if (context.currentParallelism() == 1) {
            // if parallelism == 1, we should assign all the splits to reader
            currentTaskSplits.addAll(pendingSplit);
        } else {
            // if parallelism > 1, according to hashCode of split's id to determine whether to
            // allocate the current task
            for (HbaseSourceSplit sourceSplit : pendingSplit) {
                final int splitOwner =
                        getSplitOwner(sourceSplit.splitId(), context.currentParallelism());
                if (splitOwner == taskId) {
                    currentTaskSplits.add(sourceSplit);
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
                        .map(HbaseSourceSplit::splitId)
                        .collect(Collectors.joining(",")));
        context.signalNoMoreSplits(taskId);
    }

    /** Get all splits of table */
    private Set<HbaseSourceSplit> getTableSplits() {
        List<HbaseSourceSplit> splits = new ArrayList<>();

        try {
            RegionLocator regionLocator = hbaseClient.getRegionLocator(hbaseParameters.getTable());
            byte[][] startKeys = regionLocator.getStartKeys();
            byte[][] endKeys = regionLocator.getEndKeys();
            if (startKeys.length != endKeys.length) {
                throw new IOException(
                        "Failed to create Splits for HBase table {}. HBase start keys and end keys not equal."
                                + hbaseParameters.getTable());
            }

            int i = 0;
            while (i < startKeys.length) {
                splits.add(new HbaseSourceSplit(i, startKeys[i], endKeys[i]));
                i++;
            }
            return new HashSet<>(splits);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** Hash algorithm for assigning splits to readers */
    private static int getSplitOwner(String tp, int numReaders) {
        return (tp.hashCode() & Integer.MAX_VALUE) % numReaders;
    }
}
