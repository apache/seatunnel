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
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.tdengine.exception.TDengineConnectorException;
import org.apache.seatunnel.connectors.seatunnel.tdengine.state.TDengineSourceState;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
public class TDengineSourceSplitEnumerator
        implements SourceSplitEnumerator<TDengineSourceSplit, TDengineSourceState> {

    private final SourceSplitEnumerator.Context<TDengineSourceSplit> context;
    private final TDengineSourceConfig config;
    private final StableMetadata stableMetadata;
    private volatile boolean shouldEnumerate;
    private final Object stateLock = new Object();
    private final Map<Integer, List<TDengineSourceSplit>> pendingSplits = new ConcurrentHashMap<>();

    public TDengineSourceSplitEnumerator(
            StableMetadata stableMetadata,
            TDengineSourceConfig config,
            SourceSplitEnumerator.Context<TDengineSourceSplit> context) {
        this(stableMetadata, config, null, context);
    }

    public TDengineSourceSplitEnumerator(
            StableMetadata stableMetadata,
            TDengineSourceConfig config,
            TDengineSourceState sourceState,
            SourceSplitEnumerator.Context<TDengineSourceSplit> context) {
        this.config = config;
        this.context = context;
        this.stableMetadata = stableMetadata;
        this.shouldEnumerate = sourceState == null;
        if (sourceState != null) {
            this.shouldEnumerate = sourceState.isShouldEnumerate();
            this.pendingSplits.putAll(sourceState.getPendingSplits());
        }
    }

    private static int getSplitOwner(String tp, int numReaders) {
        return (tp.hashCode() & Integer.MAX_VALUE) % numReaders;
    }

    @Override
    public void open() {}

    @Override
    public void run() {
        Set<Integer> readers = context.registeredReaders();
        if (shouldEnumerate) {
            List<TDengineSourceSplit> newSplits = discoverySplits();

            synchronized (stateLock) {
                addPendingSplit(newSplits);
                shouldEnumerate = false;
            }

            assignSplit(readers);
        }

        log.info("No more splits to assign." + " Sending NoMoreSplitsEvent to reader {}.", readers);
        readers.forEach(context::signalNoMoreSplits);
    }

    private void addPendingSplit(List<TDengineSourceSplit> newSplits) {
        int readerCount = context.currentParallelism();
        for (TDengineSourceSplit split : newSplits) {
            int ownerReader = getSplitOwner(split.splitId(), readerCount);
            pendingSplits.computeIfAbsent(ownerReader, r -> new ArrayList<>()).add(split);
        }
    }

    private List<TDengineSourceSplit> discoverySplits() {
        final String timestampFieldName = stableMetadata.getTimestampFieldName();
        final List<TDengineSourceSplit> splits = new ArrayList<>();
        for (String subTableName : stableMetadata.getSubTableNames()) {
            TDengineSourceSplit splitBySubTable =
                    createSplitBySubTable(subTableName, timestampFieldName);
            splits.add(splitBySubTable);
        }
        return splits;
    }

    private TDengineSourceSplit createSplitBySubTable(
            String subTableName, String timestampFieldName) {
        String selectFields =
                Arrays.stream(stableMetadata.getRowType().getFieldNames())
                        .skip(1)
                        .map(name -> String.format("`%s`", name))
                        .collect(Collectors.joining(","));
        String subTableSQL =
                String.format(
                        "select %s from %s.`%s`", selectFields, config.getDatabase(), subTableName);
        String start = config.getLowerBound();
        String end = config.getUpperBound();
        if (start != null || end != null) {
            String startCondition = null;
            String endCondition = null;
            // Left closed right away
            if (start != null) {
                startCondition = timestampFieldName + " >= '" + start + "'";
            }
            if (end != null) {
                endCondition = timestampFieldName + " < '" + end + "'";
            }
            String query = String.join(" and ", startCondition, endCondition);
            subTableSQL = subTableSQL + " where " + query;
        }

        return new TDengineSourceSplit(subTableName, subTableSQL);
    }

    @Override
    public void addSplitsBack(List<TDengineSourceSplit> splits, int subtaskId) {
        log.info("Add back splits {} to TDengineSourceSplitEnumerator.", splits);
        if (!splits.isEmpty()) {
            addPendingSplit(splits);
            assignSplit(Collections.singletonList(subtaskId));
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplits.size();
    }

    @Override
    public void registerReader(int subtaskId) {
        log.info("Register reader {} to TDengineSourceSplitEnumerator.", subtaskId);
        if (!pendingSplits.isEmpty()) {
            assignSplit(Collections.singletonList(subtaskId));
        }
    }

    private void assignSplit(Collection<Integer> readers) {
        log.info("Assign pendingSplits to readers {}", readers);

        for (int reader : readers) {
            List<TDengineSourceSplit> assignmentForReader = pendingSplits.remove(reader);
            if (assignmentForReader != null && !assignmentForReader.isEmpty()) {
                log.info("Assign splits {} to reader {}", assignmentForReader, reader);
                try {
                    context.assignSplit(reader, assignmentForReader);
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

    @Override
    public TDengineSourceState snapshotState(long checkpointId) {
        synchronized (stateLock) {
            return new TDengineSourceState(shouldEnumerate, pendingSplits);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    @Override
    public void close() {}

    @Override
    public void handleSplitRequest(int subtaskId) {
        throw new TDengineConnectorException(
                CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                String.format("Unsupported handleSplitRequest: %d", subtaskId));
    }
}
