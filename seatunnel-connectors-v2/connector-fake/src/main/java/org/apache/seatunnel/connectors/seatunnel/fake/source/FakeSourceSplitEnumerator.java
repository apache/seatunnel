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
import org.apache.seatunnel.connectors.seatunnel.fake.state.FakeSourceState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FakeSourceSplitEnumerator implements SourceSplitEnumerator<FakeSourceSplit, FakeSourceState> {

    private static final Logger LOG = LoggerFactory.getLogger(FakeSourceSplitEnumerator.class);
    private final SourceSplitEnumerator.Context<FakeSourceSplit> enumeratorContext;
    private final Map<Integer, Set<FakeSourceSplit>> pendingSplits;
    private final Integer totalRowNum;

    public FakeSourceSplitEnumerator(SourceSplitEnumerator.Context<FakeSourceSplit> enumeratorContext, Integer totalRowNum) {
        this.enumeratorContext = enumeratorContext;
        this.pendingSplits = new HashMap<>();
        this.totalRowNum = totalRowNum;
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

    }

    @Override
    public int currentUnassignedSplitSize() {
        return 0;
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
        return null;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {

    }

    private void discoverySplits() {
        List<FakeSourceSplit> allSplit = new ArrayList<>();
        LOG.info("Starting to calculate splits.");
        if (null != totalRowNum) {

            for (int i = 1; i <= enumeratorContext.currentParallelism(); i++) {
                allSplit.add(new FakeSourceSplit(this.splitRowNum(totalRowNum, enumeratorContext.currentParallelism(), i), i));
            }
        } else {
            allSplit.add(new FakeSourceSplit(null, 0));
        }
        int numReaders = enumeratorContext.currentParallelism();
        for (FakeSourceSplit split : allSplit) {
            int ownerReader = split.getSplitId() % numReaders;
            pendingSplits.computeIfAbsent(ownerReader, r -> new HashSet<>())
                .add(split);
        }
        LOG.debug("Assigned {} to {} readers.", allSplit, numReaders);
        LOG.info("Calculated splits successfully, the size of splits is {}.", allSplit.size());
    }

    private void assignPendingSplits() {
        // Check if there's any pending splits for given readers
        for (int pendingReader : enumeratorContext.registeredReaders()) {
            // Remove pending assignment for the reader
            final Set<FakeSourceSplit> pendingAssignmentForReader =
                pendingSplits.remove(pendingReader);

            if (pendingAssignmentForReader != null && !pendingAssignmentForReader.isEmpty()) {
                // Assign pending splits to reader
                LOG.info("Assigning splits to readers {}", pendingAssignmentForReader);
                enumeratorContext.assignSplit(pendingReader, new ArrayList<>(pendingAssignmentForReader));
                enumeratorContext.signalNoMoreSplits(pendingReader);
            }
        }
    }

    private Integer splitRowNum(int countNum, int splitNum, int current) {
        if (countNum < splitNum && countNum % splitNum == countNum) {
            if (current == 1) {
                return countNum;
            } else {
                return 0;
            }
        } else if (countNum == splitNum && countNum % splitNum == 0) {
            return countNum / splitNum;
        } else if (countNum > splitNum) {
            int a = countNum - (countNum % splitNum);
            if (current == 1) {
                return (a / splitNum) + (countNum % splitNum);
            } else {
                return a / splitNum;
            }
        }
        return 0;
    }
}
