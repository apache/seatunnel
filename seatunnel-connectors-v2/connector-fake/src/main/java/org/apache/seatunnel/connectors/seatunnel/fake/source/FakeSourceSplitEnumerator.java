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

package org.apache.seatunnel.connectors.seatunnel.fake.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.fake.config.FakeConfig;
import org.apache.seatunnel.connectors.seatunnel.fake.config.MultipleTableFakeSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.fake.state.FakeSourceState;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class FakeSourceSplitEnumerator
        implements SourceSplitEnumerator<FakeSourceSplit, FakeSourceState> {
    private final SourceSplitEnumerator.Context<FakeSourceSplit> enumeratorContext;
    private final Map<Integer, Set<FakeSourceSplit>> pendingSplits;

    private final MultipleTableFakeSourceConfig multipleTableFakeSourceConfig;
    /** Partitions that have been assigned to readers. */
    private final Set<FakeSourceSplit> assignedSplits;

    private final Object lock = new Object();

    public FakeSourceSplitEnumerator(
            SourceSplitEnumerator.Context<FakeSourceSplit> enumeratorContext,
            MultipleTableFakeSourceConfig multipleTableFakeSourceConfig,
            Set<FakeSourceSplit> assignedSplits) {
        this.enumeratorContext = enumeratorContext;
        this.pendingSplits = new HashMap<>();
        this.multipleTableFakeSourceConfig = multipleTableFakeSourceConfig;
        this.assignedSplits = new HashSet<>(assignedSplits);
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
    public void addSplitsBack(List<FakeSourceSplit> splits, int subtaskId) {
        log.debug("Fake source add splits back {}, subtaskId:{}", splits, subtaskId);
        addSplitChangeToPendingAssignments(splits);
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplits.size();
    }

    @Override
    public void handleSplitRequest(int subtaskId) {}

    @Override
    public void registerReader(int subtaskId) {
        // nothing
    }

    @Override
    public FakeSourceState snapshotState(long checkpointId) throws Exception {
        log.debug("Get lock, begin snapshot fakesource split enumerator...");
        synchronized (lock) {
            log.debug("Begin snapshot fakesource split enumerator...");
            return new FakeSourceState(assignedSplits);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    private void discoverySplits() {
        Set<FakeSourceSplit> allSplit = new HashSet<>();
        log.info("Starting to calculate splits.");
        int numReaders = enumeratorContext.currentParallelism();
        for (FakeConfig fakeConfig : multipleTableFakeSourceConfig.getFakeConfigs()) {
            String tableId = fakeConfig.getCatalogTable().getTableId().toTablePath().toString();
            int readerRowNum = fakeConfig.getRowNum();
            int splitNum = fakeConfig.getSplitNum();
            int splitRowNum = (int) Math.ceil((double) readerRowNum / splitNum);
            for (int i = 0; i < numReaders; i++) {
                int index = i;
                for (int num = 0; num < readerRowNum; index += numReaders, num += splitRowNum) {
                    allSplit.add(
                            new FakeSourceSplit(
                                    tableId, index, Math.min(splitRowNum, readerRowNum - num)));
                }
            }
            log.info(
                    "Calculated splits for table {} successfully, the size of splits is {}.",
                    tableId,
                    allSplit.size());
        }

        assignedSplits.forEach(allSplit::remove);
        addSplitChangeToPendingAssignments(allSplit);
        log.info("Assigned {} to {} readers.", allSplit, numReaders);
        log.info("Calculated splits successfully, the size of splits is {}.", allSplit.size());
    }

    private void addSplitChangeToPendingAssignments(Collection<FakeSourceSplit> newSplits) {
        for (FakeSourceSplit split : newSplits) {
            int ownerReader = split.getSplitId() % enumeratorContext.currentParallelism();
            pendingSplits.computeIfAbsent(ownerReader, r -> new HashSet<>()).add(split);
        }
    }

    private void assignPendingSplits() {
        // Check if there's any pending splits for given readers
        for (int pendingReader : enumeratorContext.registeredReaders()) {
            // Remove pending assignment for the reader
            final Set<FakeSourceSplit> pendingAssignmentForReader =
                    pendingSplits.remove(pendingReader);

            if (pendingAssignmentForReader != null && !pendingAssignmentForReader.isEmpty()) {
                // Mark pending splits as already assigned
                synchronized (lock) {
                    assignedSplits.addAll(pendingAssignmentForReader);
                    // Assign pending splits to reader
                    log.info(
                            "Assigning splits to readers {} {}",
                            pendingReader,
                            pendingAssignmentForReader);
                    enumeratorContext.assignSplit(
                            pendingReader, new ArrayList<>(pendingAssignmentForReader));
                    enumeratorContext.signalNoMoreSplits(pendingReader);
                }
            }
        }
    }
}
