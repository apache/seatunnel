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

import static org.apache.seatunnel.connectors.seatunnel.influxdb.config.SourceConfig.SQL_WHERE;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.influxdb.config.SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.influxdb.exception.InfluxdbConnectorException;
import org.apache.seatunnel.connectors.seatunnel.influxdb.state.InfluxDBSourceState;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class InfluxDBSourceSplitEnumerator implements SourceSplitEnumerator<InfluxDBSourceSplit, InfluxDBSourceState> {
    final SourceConfig config;
    private final Context<InfluxDBSourceSplit> context;
    private final Map<Integer, List<InfluxDBSourceSplit>> pendingSplit;
    private final Object stateLock = new Object();
    private volatile boolean shouldEnumerate;

    public InfluxDBSourceSplitEnumerator(SourceSplitEnumerator.Context<InfluxDBSourceSplit> context, SourceConfig config) {
        this(context, null, config);
    }

    public InfluxDBSourceSplitEnumerator(SourceSplitEnumerator.Context<InfluxDBSourceSplit> context, InfluxDBSourceState sourceState, SourceConfig config) {
        this.context = context;
        this.config = config;
        this.pendingSplit = new HashMap<>();
        this.shouldEnumerate = sourceState == null;
        if (sourceState != null) {
            this.shouldEnumerate = sourceState.isShouldEnumerate();
            this.pendingSplit.putAll(sourceState.getPendingSplit());
        }
    }

    @Override
    public void run() {
        Set<Integer> readers = context.registeredReaders();
        if (shouldEnumerate) {
            Set<InfluxDBSourceSplit> newSplits = getInfluxDBSplit();

            synchronized (stateLock) {
                addPendingSplit(newSplits);
                shouldEnumerate = false;
            }

            assignSplit(readers);
        }

        log.debug("No more splits to assign." +
                " Sending NoMoreSplitsEvent to reader {}.", readers);
        readers.forEach(context::signalNoMoreSplits);
    }

    @Override
    public void addSplitsBack(List splits, int subtaskId) {
        log.debug("Add back splits {} to InfluxDBSourceSplitEnumerator.",
                splits);
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
        log.debug("Register reader {} to InfluxDBSourceSplitEnumerator.",
                subtaskId);
        if (!pendingSplit.isEmpty()) {
            assignSplit(Collections.singletonList(subtaskId));
        }
    }

    @Override
    public InfluxDBSourceState snapshotState(long checkpointId) {
        synchronized (stateLock) {
            return new InfluxDBSourceState(shouldEnumerate, pendingSplit);
        }
    }

    private Set<InfluxDBSourceSplit> getInfluxDBSplit() {
        String sql = config.getSql();
        Set<InfluxDBSourceSplit> influxDBSourceSplits = new HashSet<>();
        // no need numPartitions, use one partition
        if (config.getPartitionNum() == 0) {
            influxDBSourceSplits.add(new InfluxDBSourceSplit(SourceConfig.DEFAULT_PARTITIONS, sql));
            return influxDBSourceSplits;
        }
        //calculate numRange base on (lowerBound upperBound partitionNum)
        List<Pair<Long, Long>> rangePairs = genSplitNumRange(config.getLowerBound(), config.getUpperBound(), config.getPartitionNum());

        String[] sqls = sql.split(SQL_WHERE.key());
        if (sqls.length > 2) {
            throw new InfluxdbConnectorException(CommonErrorCode.ILLEGAL_ARGUMENT,
                "sql should not contain more than one where");
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

    private void addPendingSplit(Collection<InfluxDBSourceSplit> splits) {
        int readerCount = context.currentParallelism();
        for (InfluxDBSourceSplit split : splits) {
            int ownerReader = getSplitOwner(split.splitId(), readerCount);
            log.info("Assigning {} to {} reader.", split, ownerReader);
            pendingSplit.computeIfAbsent(ownerReader, r -> new ArrayList<>())
                    .add(split);
        }
    }

    private void assignSplit(Collection<Integer> readers) {
        log.debug("Assign pendingSplits to readers {}", readers);

        for (int reader : readers) {
            List<InfluxDBSourceSplit> assignmentForReader = pendingSplit.remove(reader);
            if (assignmentForReader != null && !assignmentForReader.isEmpty()) {
                log.info("Assign splits {} to reader {}",
                        assignmentForReader, reader);
                try {
                    context.assignSplit(reader, assignmentForReader);
                } catch (Exception e) {
                    log.error("Failed to assign splits {} to reader {}",
                            assignmentForReader, reader, e);
                    pendingSplit.put(reader, assignmentForReader);
                }
            }
        }
    }

    private static int getSplitOwner(String tp, int numReaders) {
        return (tp.hashCode() & Integer.MAX_VALUE) % numReaders;
    }

    @Override
    public void open() {
        //nothing to do
    }

    @Override
    public void close() {
        //nothing to do
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        //nothing to do

    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        throw new InfluxdbConnectorException(CommonErrorCode.UNSUPPORTED_OPERATION,
            String.format("Unsupported handleSplitRequest: %d", subtaskId));
    }

}
