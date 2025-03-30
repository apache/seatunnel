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
import org.apache.seatunnel.connectors.seatunnel.maxcompute.util.MaxcomputeUtil;

import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.SPLIT_ROW;

@Slf4j
public class MaxcomputeSourceSplitEnumerator
        implements SourceSplitEnumerator<MaxcomputeSourceSplit, MaxcomputeSourceState> {
    private final Context<MaxcomputeSourceSplit> enumeratorContext;
    private final Map<Integer, Set<MaxcomputeSourceSplit>> pendingSplits;
    private Set<MaxcomputeSourceSplit> assignedSplits;
    private final ReadonlyConfig readonlyConfig;
    private final Map<TablePath, SourceTableInfo> sourceTableInfos;

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
        discoverySplits();
        assignPendingSplits();
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
        return new MaxcomputeSourceState(assignedSplits);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    @Override
    public void handleSplitRequest(int subtaskId) {}

    private void discoverySplits() throws TunnelException {
        int numReaders = enumeratorContext.currentParallelism();
        Set<MaxcomputeSourceSplit> allSplit = new HashSet<>();
        for (SourceTableInfo sourceTableInfo : sourceTableInfos.values()) {
            Set<MaxcomputeSourceSplit> splits = new HashSet<>();
            TableTunnel.DownloadSession session =
                    MaxcomputeUtil.getDownloadSession(
                            readonlyConfig,
                            sourceTableInfo.getCatalogTable().getTablePath(),
                            sourceTableInfo.getPartitionSpec());
            long recordCount = session.getRecordCount();
            int splitRowNum = (int) Math.ceil((double) recordCount / numReaders);
            int splitRow = SPLIT_ROW.defaultValue();
            if (sourceTableInfo.getSplitRow() != null && sourceTableInfo.getSplitRow() > 0) {
                splitRow = sourceTableInfo.getSplitRow();
            }
            int splitIndex = 0;
            for (int i = 0; i < numReaders; i++) {
                int readerStart = i * splitRowNum;
                int readerEnd = (int) Math.min((i + 1) * splitRowNum, recordCount);
                for (int num = readerStart; num < readerEnd; num += splitRow) {
                    splits.add(
                            new MaxcomputeSourceSplit(
                                    num,
                                    Math.min(splitRow, readerEnd - num),
                                    sourceTableInfo.getCatalogTable().getTablePath(),
                                    splitIndex));
                }
            }
            assignedSplits.forEach(splits::remove);
            allSplit.addAll(splits);
        }
        addSplitChangeToPendingAssignments(allSplit);
        log.debug("Assigned {} to {} readers.", allSplit, numReaders);
        log.info("Calculated splits successfully, the size of splits is {}.", allSplit.size());
    }

    private void addSplitChangeToPendingAssignments(Collection<MaxcomputeSourceSplit> newSplits) {
        for (MaxcomputeSourceSplit split : newSplits) {
            int ownerReader = split.getIndex() % enumeratorContext.currentParallelism();
            pendingSplits.computeIfAbsent(ownerReader, r -> new HashSet<>()).add(split);
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
