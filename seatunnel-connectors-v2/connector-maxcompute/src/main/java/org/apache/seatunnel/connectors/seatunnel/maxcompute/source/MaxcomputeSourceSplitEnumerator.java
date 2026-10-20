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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.util.MaxcomputeUtil;

import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class MaxcomputeSourceSplitEnumerator
        implements SourceSplitEnumerator<MaxcomputeSourceSplit, MaxcomputeSourceState> {
    private final Context<MaxcomputeSourceSplit> enumeratorContext;
    private final Map<Integer, Set<MaxcomputeSourceSplit>> pendingSplits;
    private Set<MaxcomputeSourceSplit> assignedSplits;
    private final ReadonlyConfig readonlyConfig;
    private final Map<TablePath, SourceTableInfo> sourceTableInfos;
    private final Object stateLock = new Object();

    public MaxcomputeSourceSplitEnumerator(
            SourceSplitEnumerator.Context<MaxcomputeSourceSplit> enumeratorContext,
            ReadonlyConfig readonlyConfig,
            Map<TablePath, SourceTableInfo> sourceTableInfos) {
        this.enumeratorContext = enumeratorContext;
        this.readonlyConfig = readonlyConfig;
        this.sourceTableInfos = sourceTableInfos;
        this.pendingSplits = new HashMap<>();
        this.assignedSplits = new HashSet<>();
    }

    public MaxcomputeSourceSplitEnumerator(
            SourceSplitEnumerator.Context<MaxcomputeSourceSplit> enumeratorContext,
            ReadonlyConfig readonlyConfig,
            Map<TablePath, SourceTableInfo> sourceTableInfos,
            MaxcomputeSourceState sourceState) {
        this(enumeratorContext, readonlyConfig, sourceTableInfos);
        this.assignedSplits = sourceState.getAssignedSplit();
    }

    @Override
    public void open() {}

    @Override
    public void run() throws Exception {
        synchronized (stateLock) {
            discoverySplits();
        }
        synchronized (stateLock) {
            assignPendingSplits();
        }
    }

    @Override
    public void close() throws IOException {}

    @Override
    public void addSplitsBack(List<MaxcomputeSourceSplit> splits, int subtaskId) {
        addSplitChangeToPendingAssignments(splits);
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplits.size();
    }

    @Override
    public void registerReader(int subtaskId) {}

    @Override
    public MaxcomputeSourceState snapshotState(long checkpointId) {
        synchronized (stateLock) {
            return new MaxcomputeSourceState(assignedSplits);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    @Override
    public void handleSplitRequest(int subtaskId) {}

    // visible for testing
    static Set<MaxcomputeSourceSplit> computeSplits(
            int numReaders,
            Collection<SourceTableInfo> sourceTableInfos,
            Map<TablePath, Long> tableRecordCounts) {
        Set<MaxcomputeSourceSplit> allSplit = new LinkedHashSet<>();
        int chunkIndex = 0;
        for (SourceTableInfo sourceTableInfo : sourceTableInfos) {
            TablePath tablePath = sourceTableInfo.getCatalogTable().getTablePath();
            long recordCount = tableRecordCounts.get(tablePath);
            int splitRow = MaxcomputeSourceOptions.SPLIT_ROW.defaultValue();
            if (sourceTableInfo.getSplitRow() != null && sourceTableInfo.getSplitRow() > 0) {
                splitRow = sourceTableInfo.getSplitRow();
            }
            for (long num = 0; num < recordCount; num += splitRow) {
                int ownerReader = chunkIndex % numReaders;
                allSplit.add(
                        new MaxcomputeSourceSplit(
                                num,
                                Math.min((long) splitRow, recordCount - num),
                                tablePath,
                                ownerReader));
                chunkIndex++;
            }
        }
        return allSplit;
    }

    private void discoverySplits() throws TunnelException {
        int numReaders = enumeratorContext.currentParallelism();
        Map<TablePath, Long> tableRecordCounts = new HashMap<>();
        for (SourceTableInfo sourceTableInfo : sourceTableInfos.values()) {
            TableTunnel.DownloadSession session =
                    MaxcomputeUtil.getDownloadSession(
                            readonlyConfig,
                            sourceTableInfo.getCatalogTable().getTablePath(),
                            sourceTableInfo.getPartitionSpec());
            tableRecordCounts.put(
                    sourceTableInfo.getCatalogTable().getTablePath(), session.getRecordCount());
        }

        Set<MaxcomputeSourceSplit> allSplit =
                computeSplits(numReaders, sourceTableInfos.values(), tableRecordCounts);
        assignedSplits.forEach(allSplit::remove);

        addSplitChangeToPendingAssignments(allSplit);
        log.debug("Assigned {} to {} readers.", allSplit, numReaders);
        log.info("Calculated splits successfully, the size of splits is {}.", allSplit.size());
    }

    private void addSplitChangeToPendingAssignments(Collection<MaxcomputeSourceSplit> newSplits) {
        for (MaxcomputeSourceSplit split : newSplits) {
            int ownerReader = split.getIndex() % enumeratorContext.currentParallelism();
            pendingSplits.computeIfAbsent(ownerReader, r -> new LinkedHashSet<>()).add(split);
        }
    }

    private void assignPendingSplits() {
        // Check if there's any pending splits for given readers
        for (int pendingReader : enumeratorContext.registeredReaders()) {
            // Remove pending assignment for the reader
            final Set<MaxcomputeSourceSplit> pendingAssignmentForReader =
                    pendingSplits.remove(pendingReader);

            if (pendingAssignmentForReader != null && !pendingAssignmentForReader.isEmpty()) {
                // Mark pending splits as already assigned
                assignedSplits.addAll(pendingAssignmentForReader);
                // Assign pending splits to reader
                log.info(
                        "Assigning splits to readers {} {}",
                        pendingReader,
                        pendingAssignmentForReader);
                enumeratorContext.assignSplit(
                        pendingReader, new ArrayList<>(pendingAssignmentForReader));
            }
            enumeratorContext.signalNoMoreSplits(pendingReader);
        }
    }
}
