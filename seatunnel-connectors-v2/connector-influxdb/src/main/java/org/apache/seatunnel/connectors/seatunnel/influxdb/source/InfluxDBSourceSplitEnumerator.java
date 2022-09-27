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

package org.apache.seatunnel.connectors.seatunnel.influxdb.source;

import static org.apache.seatunnel.connectors.seatunnel.influxdb.config.InfluxDBConfig.SQL_WHERE;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.connectors.seatunnel.influxdb.config.InfluxDBConfig;
import org.apache.seatunnel.connectors.seatunnel.influxdb.state.InfluxDBSourceState;

import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class InfluxDBSourceSplitEnumerator implements SourceSplitEnumerator<InfluxDBSourceSplit, InfluxDBSourceState> {
    final InfluxDBConfig config;

    private final Context<InfluxDBSourceSplit> context;
    private Set<InfluxDBSourceSplit> pendingSplit;
    private Set<InfluxDBSourceSplit> assignedSplit;

    public InfluxDBSourceSplitEnumerator(SourceSplitEnumerator.Context<InfluxDBSourceSplit> context, InfluxDBConfig config) {
        this.context = context;
        this.config = config;
    }

    public InfluxDBSourceSplitEnumerator(SourceSplitEnumerator.Context<InfluxDBSourceSplit> context, InfluxDBSourceState sourceState, InfluxDBConfig config) {
        this(context, config);
        this.assignedSplit = sourceState.getAssignedSplit();
    }

    @Override
    public void open() {
        //nothing to do
    }

    @Override
    public void run() throws Exception {
        pendingSplit = getInfluxDBSplit();
        assignSplit(context.registeredReaders());
    }

    @Override
    public void close() throws IOException {
        //nothing to do
    }

    @Override
    public void addSplitsBack(List splits, int subtaskId) {
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
    public void handleSplitRequest(int subtaskId) {
        //nothing to do
    }

    @Override
    public void registerReader(int subtaskId) {
        if (!pendingSplit.isEmpty()) {
            assignSplit(Collections.singletonList(subtaskId));
        }
    }

    @Override
    public InfluxDBSourceState snapshotState(long checkpointId) throws Exception {
        return new InfluxDBSourceState(assignedSplit);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        //nothing to do

    }

    private Set<InfluxDBSourceSplit> getInfluxDBSplit() {
        String sql = config.getSql();
        Set<InfluxDBSourceSplit> influxDBSourceSplits = new HashSet<>();
        // no need numPartitions, use one partition
        if (config.getPartitionNum() == 0) {
            influxDBSourceSplits.add(new InfluxDBSourceSplit(InfluxDBConfig.DEFAULT_PARTITIONS, sql));
            return influxDBSourceSplits;
        }
        //calculate numRange base on (lowerBound upperBound partitionNum)
        List<Pair<Long, Long>> rangePairs = genSplitNumRange(config.getLowerBound(), config.getUpperBound(), config.getPartitionNum());

        String[] sqls = sql.split(SQL_WHERE);
        if (sqls.length > 2) {
            throw new IllegalArgumentException("sql should not contain more than one where");
        }

        int i = 0;
        while (i < rangePairs.size()) {
            String query = " where (" + config.getSplitKey() + " >= " + rangePairs.get(i).getLeft() + " and " + config.getSplitKey()  + " < " + rangePairs.get(i).getRight() + ") ";
            i++;
            query = sqls[0] + query;
            if (sqls.length > 1) {
                query = query + " and ( " + sqls[1] + " ) ";
            }
            influxDBSourceSplits.add(new InfluxDBSourceSplit(String.valueOf(i + System.nanoTime()), query));
        }
        return influxDBSourceSplits;
    }

    public static List<Pair<Long, Long>> genSplitNumRange(long lowerBound, long upperBound, int splitNum) {
        List<Pair<Long, Long>> rangeList = new ArrayList<>();
        int numPartitions = splitNum;
        int size = (int) (upperBound - lowerBound) / numPartitions + 1;
        int remainder = (int) ((upperBound + 1 - lowerBound) % numPartitions);
        if (upperBound - lowerBound < numPartitions) {
            numPartitions = (int) (upperBound - lowerBound);
        }
        long currentStart = lowerBound;
        int i = 0;
        while (i < numPartitions) {
            rangeList.add(Pair.of(currentStart, currentStart + size));
            i++;
            currentStart += size;
            if (i + 1 <= numPartitions) {
                currentStart = currentStart - remainder;
            }
        }
        return rangeList;
    }

    private void assignSplit(Collection<Integer> taskIDList) {
        Map<Integer, List<InfluxDBSourceSplit>> readySplit = new HashMap<>(Common.COLLECTION_SIZE);
        for (int taskID : taskIDList) {
            readySplit.computeIfAbsent(taskID, id -> new ArrayList<>());
        }
        pendingSplit.forEach(s -> readySplit.get(getSplitOwner(s.splitId(), taskIDList.size()))
                .add(s));
        readySplit.forEach(context::assignSplit);
        assignedSplit.addAll(pendingSplit);
        pendingSplit.clear();
    }

    private static int getSplitOwner(String tp, int numReaders) {
        return tp.hashCode() % numReaders;
    }

}
