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

package org.apache.seatunnel.connectors.seatunnel.paimon.source.enumerator;

import org.apache.seatunnel.connectors.seatunnel.paimon.source.PaimonSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.paimon.source.PaimonSourceState;

import org.apache.paimon.table.source.TableScan;

import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;

import java.util.Deque;
import java.util.Set;

/** Paimon source split enumerator, used to calculate the splits for every reader. */
@Slf4j
public class PaimonBatchSourceSplitEnumerator extends AbstractSplitEnumerator {

    public PaimonBatchSourceSplitEnumerator(
            Context<PaimonSourceSplit> context,
            Deque<PaimonSourceSplit> pendingSplits,
            @Nullable Long nextSnapshotId,
            TableScan tableScan,
            int splitMaxPerTask) {
        super(context, pendingSplits, nextSnapshotId, tableScan, splitMaxPerTask);
    }

    @Override
    public void run() throws Exception {
        this.processDiscoveredSplits(this.scanNextSnapshot(), null);
        Set<Integer> readers = context.registeredReaders();
        log.debug(
                "No more splits to assign." + " Sending NoMoreSplitsEvent to reader {}.", readers);
        readers.forEach(context::signalNoMoreSplits);
    }

    @Override
    public PaimonSourceState snapshotState(long checkpointId) throws Exception {
        return new PaimonSourceState(pendingSplits, null);
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        // do nothing
    }
}
