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
import org.apache.seatunnel.connectors.seatunnel.fake.state.FakeSourceState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FakeSourceSplitEnumerator implements SourceSplitEnumerator<FakeSourceSplit, FakeSourceState> {

    private static final Logger LOG = LoggerFactory.getLogger(FakeSourceSplitEnumerator.class);
    private final SourceSplitEnumerator.Context<FakeSourceSplit> enumeratorContext;
    private final Map<Integer, Set<FakeSourceSplit>> pendingSplits;

    private final FakeConfig fakeConfig;
    /**
     * Partitions that have been assigned to readers.
     */
    private final Set<FakeSourceSplit> assignedSplits;

    public FakeSourceSplitEnumerator(SourceSplitEnumerator.Context<FakeSourceSplit> enumeratorContext,
                                     FakeConfig config,
                                     Set<FakeSourceSplit> assignedSplits) {
        this.enumeratorContext = enumeratorContext;
        this.pendingSplits = new HashMap<>();
        this.fakeConfig = config;
        this.assignedSplits = new HashSet<>(assignedSplits);
    }

    @Override
    public void open() {
        // No connection needs to be opened
    }

    @Override
    public void run() throws Exception {
        discoverySplits();
        assignPendingSplits();
    }

    @Override
    public void close() throws IOException {
        // nothing
    }

    @Override
    public void addSplitsBack(List<FakeSourceSplit> splits, int subtaskId) {
        addSplitChangeToPendingAssignments(splits);
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplits.size();
    }

    @Override
    public void handleSplitRequest(int subtaskId) {

    }

    @Override
    public void registerReader(int subtaskId) {
        // nothing
    }

    @Override
    public FakeSourceState snapshotState(long checkpointId) throws Exception {
        return new FakeSourceState(assignedSplits);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {

    }

    private void discoverySplits() {
        Set<FakeSourceSplit> allSplit = new HashSet<>();
        LOG.info("Starting to calculate splits.");
        int numReaders = enumeratorContext.currentParallelism();
        int readerRowNum = fakeConfig.getRowNum();
        int splitNum  = fakeConfig.getSplitNum();
        int splitRowNum = (int) Math.ceil((double) readerRowNum / splitNum);
        for (int i = 0; i < numReaders; i++) {
            int index = i;
            for (int num = 0; num < readerRowNum; index += numReaders, num += splitRowNum) {
                allSplit.add(new FakeSourceSplit(index, Math.min(splitRowNum, readerRowNum - num)));
            }
        }

        assignedSplits.forEach(allSplit::remove);
        addSplitChangeToPendingAssignments(allSplit);
        LOG.debug("Assigned {} to {} readers.", allSplit, numReaders);
        LOG.info("Calculated splits successfully, the size of splits is {}.", allSplit.size());
    }

    private void addSplitChangeToPendingAssignments(Collection<FakeSourceSplit> newSplits) {
        for (FakeSourceSplit split : newSplits) {
            int ownerReader = split.getSplitId() % enumeratorContext.currentParallelism();
            pendingSplits.computeIfAbsent(ownerReader, r -> new HashSet<>())
                .add(split);
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
                assignedSplits.addAll(pendingAssignmentForReader);
                // Assign pending splits to reader
                LOG.info("Assigning splits to readers {} {}", pendingReader, pendingAssignmentForReader);
                enumeratorContext.assignSplit(pendingReader, new ArrayList<>(pendingAssignmentForReader));
                enumeratorContext.signalNoMoreSplits(pendingReader);
            }
        }
    }
}
