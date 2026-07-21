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

package org.apache.seatunnel.connectors.cdc.base.source.progress;

import org.apache.seatunnel.api.cdc.CdcProgressLifecycle;
import org.apache.seatunnel.api.cdc.CdcProgressPosition;
import org.apache.seatunnel.api.cdc.CdcProgressValue;
import org.apache.seatunnel.api.cdc.CdcReaderProgressReport;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.split.state.IncrementalSplitState;
import org.apache.seatunnel.connectors.cdc.base.source.split.state.SourceSplitStateBase;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/** Maintains the latest immutable report without performing I/O from the record-emission path. */
public final class CdcReaderProgressTracker {

    private final String connectorType;
    private final String positionType;
    private final LongSupplier clock;
    private final AtomicReference<CdcReaderProgressReport> report;
    private volatile ReaderState latestState;
    private volatile Long latestSourceEventAt;

    public CdcReaderProgressTracker(String connectorType, String positionType) {
        this(connectorType, positionType, System::currentTimeMillis);
    }

    CdcReaderProgressTracker(String connectorType, String positionType, LongSupplier clock) {
        this.connectorType = connectorType;
        this.positionType = positionType;
        this.clock = clock;
        this.report =
                new AtomicReference<>(
                        new CdcReaderProgressReport(
                                connectorType,
                                CdcProgressLifecycle.UNKNOWN,
                                null,
                                CdcProgressValue.unavailable(),
                                CdcProgressValue.unsupported(),
                                CdcProgressValue.unsupported(),
                                0L,
                                null));
    }

    public void recordSplitState(SourceSplitStateBase splitState) {
        recordState(splitState);
    }

    public void recordEmission(SourceSplitStateBase splitState, Long sourceEventTime) {
        if (sourceEventTime != null && sourceEventTime > 0) {
            latestSourceEventAt = sourceEventTime;
        }
        recordState(splitState);
    }

    public CdcReaderProgressReport current() {
        ReaderState state = latestState;
        if (state == null) {
            return report.get();
        }
        CdcReaderProgressReport previous = report.get();
        CdcProgressPosition position = CdcProgressPositions.fromOffset(positionType, state.offset);
        CdcProgressValue<CdcProgressPosition> consumedPosition =
                position == null
                        ? CdcProgressValue.unavailable()
                        : CdcProgressValue.exact(position);
        long now = clock.getAsLong();
        long lastPositionChangeAt =
                positionChanged(previous.getCurrentConsumedPosition(), position)
                        ? now
                        : previous.getLastPositionChangeAt();
        CdcReaderProgressReport current =
                new CdcReaderProgressReport(
                        connectorType,
                        state.lifecycle,
                        state.splitId,
                        consumedPosition,
                        CdcProgressValue.unsupported(),
                        CdcProgressValue.unsupported(),
                        lastPositionChangeAt,
                        latestSourceEventAt);
        report.set(current);
        return current;
    }

    private void recordState(SourceSplitStateBase splitState) {
        if (splitState.isSnapshotSplitState()) {
            latestState =
                    new ReaderState(splitState.splitId(), CdcProgressLifecycle.SNAPSHOT, null);
            return;
        }
        IncrementalSplitState incrementalState = splitState.asIncrementalSplitState();
        latestState =
                new ReaderState(
                        splitState.splitId(),
                        incrementalState.isEnterPureIncrementPhase()
                                ? CdcProgressLifecycle.INCREMENTAL
                                : CdcProgressLifecycle.CATCH_UP,
                        incrementalState.getStartupOffset());
    }

    private boolean positionChanged(
            CdcProgressValue<CdcProgressPosition> previous, CdcProgressPosition currentPosition) {
        CdcProgressPosition previousPosition = previous.getValue();
        if (previousPosition == null || currentPosition == null) {
            return previousPosition != currentPosition;
        }
        return !Objects.equals(previousPosition.getType(), currentPosition.getType())
                || previousPosition.getSchemaVersion() != currentPosition.getSchemaVersion()
                || !previousPosition.getValues().equals(currentPosition.getValues());
    }

    private static final class ReaderState {
        private final String splitId;
        private final CdcProgressLifecycle lifecycle;
        private final Offset offset;

        private ReaderState(String splitId, CdcProgressLifecycle lifecycle, Offset offset) {
            this.splitId = splitId;
            this.lifecycle = lifecycle;
            this.offset = offset;
        }
    }
}
