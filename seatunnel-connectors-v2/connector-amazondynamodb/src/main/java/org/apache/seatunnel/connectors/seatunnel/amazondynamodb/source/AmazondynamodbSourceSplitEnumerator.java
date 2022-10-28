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

package org.apache.seatunnel.connectors.seatunnel.amazondynamodb.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.amazondynamodb.state.AmazonDynamodbSourceState;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class AmazondynamodbSourceSplitEnumerator implements SourceSplitEnumerator<AmazondynamodbSourceSplit, AmazonDynamodbSourceState> {

    private final SourceSplitEnumerator.Context<AmazondynamodbSourceSplit> enumeratorContext;
    private final Map<Integer, Set<AmazondynamodbSourceSplit>> pendingSplits;

    public AmazondynamodbSourceSplitEnumerator(SourceSplitEnumerator.Context<AmazondynamodbSourceSplit> enumeratorContext) {
        this.enumeratorContext = enumeratorContext;
        this.pendingSplits = new HashMap<>();
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
    public void addSplitsBack(List<AmazondynamodbSourceSplit> splits, int subtaskId) {

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
    public AmazonDynamodbSourceState snapshotState(long checkpointId) throws Exception {
        return null;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {

    }

    private void discoverySplits() {
        List<AmazondynamodbSourceSplit> allSplit = new ArrayList<>();
        log.info("Starting to calculate splits.");
        allSplit.add(new AmazondynamodbSourceSplit(0));
        int numReaders = enumeratorContext.currentParallelism();
        log.debug("Assigned {} to {} readers.", allSplit, numReaders);
        log.info("Calculated splits successfully, the size of splits is {}.", allSplit.size());
    }

    private void assignPendingSplits() {
        // Check if there's any pending splits for given readers
        for (int pendingReader : enumeratorContext.registeredReaders()) {
            // Remove pending assignment for the reader
            final Set<AmazondynamodbSourceSplit> pendingAssignmentForReader =
                pendingSplits.remove(pendingReader);

            if (pendingAssignmentForReader != null && !pendingAssignmentForReader.isEmpty()) {
                // Assign pending splits to reader
                log.info("Assigning splits to readers {}", pendingAssignmentForReader);
                enumeratorContext.assignSplit(pendingReader, new ArrayList<>(pendingAssignmentForReader));
                enumeratorContext.signalNoMoreSplits(pendingReader);
            }
        }
    }
}
