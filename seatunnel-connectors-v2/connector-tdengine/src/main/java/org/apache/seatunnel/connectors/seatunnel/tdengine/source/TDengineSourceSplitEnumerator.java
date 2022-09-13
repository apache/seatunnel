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

package org.apache.seatunnel.connectors.seatunnel.tdengine.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.tdengine.state.TDengineSourceState;

import com.google.common.collect.Sets;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TDengineSourceSplitEnumerator implements SourceSplitEnumerator<TDengineSourceSplit, TDengineSourceState> {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private final SourceSplitEnumerator.Context<TDengineSourceSplit> context;
    private final TDengineSourceConfig config;
    private Set<TDengineSourceSplit> pendingSplit = Sets.newConcurrentHashSet();
    private Set<TDengineSourceSplit> assignedSplit = Sets.newConcurrentHashSet();

    public TDengineSourceSplitEnumerator(TDengineSourceConfig config, SourceSplitEnumerator.Context<TDengineSourceSplit> context) {
        this.config = config;
        this.context = context;
    }

    public TDengineSourceSplitEnumerator(TDengineSourceConfig config, TDengineSourceState sourceState, SourceSplitEnumerator.Context<TDengineSourceSplit> context) {
        this(config, context);
        this.assignedSplit = sourceState.getAssignedSplit();
    }

    private static int getSplitOwner(String tp, int numReaders) {
        return tp.hashCode() % numReaders;
    }

    @Override
    public void open() {
    }

    @Override
    public void run() {
        pendingSplit = getAllSplit();
        assignSplit(context.registeredReaders());
    }

    /**
     * split the time range into numPartitions parts if numPartitions is 1, use the whole time range if numPartitions < (end - start), use (start-end) partitions
     * <p>
     * eg: start = 1, end = 10, numPartitions = 2 sql = "select * from test"
     * <p>
     * split result
     * <p>
     * split 1: select * from test  where (time >= 1 and time < 6)
     * <p>
     * split 2: select * from test  where (time >= 6 and time < 11)
     */
    private Set<TDengineSourceSplit> getAllSplit() {
        String sql = "select * from " + config.getStable();
        Set<TDengineSourceSplit> sourceSplits = Sets.newHashSet();
        // no need numPartitions, use one partition
        if (config.getPartitionsNum() <= 1) {
            sourceSplits.add(new TDengineSourceSplit("0", sql));
            return sourceSplits;
        }
        long start = config.getLowerBound();
        long end = config.getUpperBound();
        int partitionsNum = config.getPartitionsNum();
        if (end - start < partitionsNum) {
            partitionsNum = (int) (end - start);
        }
        int size = (int) (end - start) / partitionsNum;
        long currentStart = start;
        int i = 0;
        while (i < partitionsNum) {
            //Left closed right away
            long currentEnd = i == partitionsNum - 1 ? end + 1 : currentStart + size;
            String query = " where ts >= '" + Instant.ofEpochMilli(currentStart).atZone(ZoneId.systemDefault()).format(FORMATTER) + "' and ts < '" + Instant.ofEpochMilli(currentEnd).atZone(ZoneId.systemDefault()).format(FORMATTER) + "'";
            String finalSQL = sql + query;
            sourceSplits.add(new TDengineSourceSplit(String.valueOf(i + System.nanoTime()), finalSQL));
            i++;
            currentStart += size;
        }
        return sourceSplits;
    }

    @Override
    public void addSplitsBack(List<TDengineSourceSplit> splits, int subtaskId) {
        if (!splits.isEmpty()) {
            pendingSplit.addAll(splits);
            assignSplit(Collections.singletonList(subtaskId));
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplit.size();
    }

    @Override
    public void registerReader(int subtaskId) {
        if (!pendingSplit.isEmpty()) {
            assignSplit(Collections.singletonList(subtaskId));
        }
    }

    private void assignSplit(Collection<Integer> taskIDList) {
        Map<Integer, List<TDengineSourceSplit>> readySplit = new HashMap<>(Common.COLLECTION_SIZE);
        for (int taskID : taskIDList) {
            readySplit.computeIfAbsent(taskID, id -> new ArrayList<>());
        }
        pendingSplit.forEach(s -> readySplit.get(getSplitOwner(s.splitId(), taskIDList.size()))
            .add(s));
        readySplit.forEach(context::assignSplit);
        assignedSplit.addAll(pendingSplit);
        pendingSplit.clear();
    }

    @Override
    public TDengineSourceState snapshotState(long checkpointId) {
        return new TDengineSourceState(assignedSplit);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        //nothing to do
    }

    @Override
    public void close() {
        //nothing to do
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        //nothing to do
    }
}
