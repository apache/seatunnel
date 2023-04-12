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

package org.apache.seatunnel.connectors.seatunnel.iceberg.source.enumerator;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.source.enumerator.scan.IcebergScanContext;
import org.apache.seatunnel.connectors.seatunnel.iceberg.source.enumerator.scan.IcebergScanSplitPlanner;
import org.apache.seatunnel.connectors.seatunnel.iceberg.source.split.IcebergFileScanTaskSplit;

import org.apache.iceberg.Table;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.Set;

@Slf4j
public class IcebergBatchSplitEnumerator extends AbstractSplitEnumerator {

    private final IcebergScanContext icebergScanContext;
    private final boolean shouldEnumerate;
    private volatile boolean splitLoadReady;

    public IcebergBatchSplitEnumerator(
            @NonNull SourceSplitEnumerator.Context<IcebergFileScanTaskSplit> context,
            @NonNull IcebergScanContext icebergScanContext,
            @NonNull SourceConfig sourceConfig,
            IcebergSplitEnumeratorState restoreState) {
        super(
                context,
                sourceConfig,
                restoreState != null ? restoreState.getPendingSplits() : Collections.emptyMap());
        this.icebergScanContext = icebergScanContext;
        // split enumeration is not needed during restore scenario
        this.shouldEnumerate = restoreState == null;
    }

    @Override
    public synchronized void run() {
        Set<Integer> readers = context.registeredReaders();
        if (shouldEnumerate) {
            loadAllSplitsToPendingSplits(icebergTableLoader.loadTable());
            assignPendingSplits(readers);
        }

        log.debug("No more splits to assign. Sending NoMoreSplitsEvent to readers {}.", readers);
        readers.forEach(context::signalNoMoreSplits);
    }

    @Override
    public IcebergSplitEnumeratorState snapshotState(long checkpointId) {
        if (splitLoadReady) {
            return new IcebergSplitEnumeratorState(null, pendingSplits);
        }
        return null;
    }

    public void handleSplitRequest(int subtaskId) {
        throw new UnsupportedOperationException("Unsupported handleSplitRequest: " + subtaskId);
    }

    private void loadAllSplitsToPendingSplits(Table table) {
        List<IcebergFileScanTaskSplit> newSplits =
                IcebergScanSplitPlanner.planSplits(table, icebergScanContext);
        addPendingSplits(newSplits);
        splitLoadReady = true;
    }
}
