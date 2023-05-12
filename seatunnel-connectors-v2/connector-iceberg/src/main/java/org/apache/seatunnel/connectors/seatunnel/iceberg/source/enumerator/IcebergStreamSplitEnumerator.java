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
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class IcebergStreamSplitEnumerator extends AbstractSplitEnumerator {

    private final IcebergScanContext icebergScanContext;
    private final AtomicReference<IcebergEnumeratorPosition> enumeratorPosition;

    public IcebergStreamSplitEnumerator(
            @NonNull SourceSplitEnumerator.Context<IcebergFileScanTaskSplit> context,
            @NonNull IcebergScanContext icebergScanContext,
            @NonNull SourceConfig sourceConfig,
            IcebergSplitEnumeratorState restoreState) {
        super(
                context,
                sourceConfig,
                restoreState != null ? restoreState.getPendingSplits() : Collections.emptyMap());
        this.icebergScanContext = icebergScanContext;
        this.enumeratorPosition = new AtomicReference<>();
        if (restoreState != null) {
            enumeratorPosition.set(restoreState.getLastEnumeratedPosition());
        }
    }

    @Override
    public IcebergSplitEnumeratorState snapshotState(long checkpointId) throws Exception {
        return new IcebergSplitEnumeratorState(enumeratorPosition.get(), pendingSplits);
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        if (isOpen()) {
            synchronized (this) {
                if (pendingSplits.isEmpty() || pendingSplits.get(subtaskId) == null) {
                    refreshPendingSplits();
                }
                assignPendingSplits(Collections.singleton(subtaskId));
            }
        }
    }

    @Override
    protected List<IcebergFileScanTaskSplit> loadNewSplits(Table table) {
        IcebergEnumerationResult result =
                IcebergScanSplitPlanner.planStreamSplits(
                        table, icebergScanContext, enumeratorPosition.get());
        if (!Objects.equals(result.getFromPosition(), enumeratorPosition.get())) {
            log.info(
                    "Skip {} loaded splits because the scan starting position doesn't match "
                            + "the current enumerator position: enumerator position = {}, scan starting position = {}",
                    result.getSplits().size(),
                    enumeratorPosition.get(),
                    result.getFromPosition());
            return Collections.emptyList();
        } else {
            enumeratorPosition.set(result.getToPosition());
            log.debug("Update enumerator position to {}", result.getToPosition());
            return result.getSplits();
        }
    }
}
