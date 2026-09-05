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

/** Maintains the latest immutable report without performing I/O from the record-emission path. */
public final class CdcReaderProgressTracker {

    private final String connectorType;
    private final String positionType;
    private final CdcReaderProgressReport initialReport;
    private final AtomicReference<ReaderState> latestState = new AtomicReference<>();

    public CdcReaderProgressTracker(String connectorType, String positionType) {
        this.connectorType = connectorType;
        this.positionType = positionType;
        this.initialReport =
                new CdcReaderProgressReport(
                        connectorType,
                        CdcProgressLifecycle.UNKNOWN,
                        null,
                        CdcProgressValue.unavailable(),
                        CdcProgressValue.unsupported(),
                        CdcProgressValue.unsupported(),
                        0L,
                        null);
    }

    public void recordSplitState(SourceSplitStateBase splitState) {
        recordState(splitState, null, false, 0L);
    }

    public void recordEmission(
            SourceSplitStateBase splitState, Long sourceEventTime, long observedAt) {
        recordState(splitState, sourceEventTime, true, observedAt);
    }

    public CdcReaderProgressReport current() {
        ReaderState state = latestState.get();
        if (state == null) {
            return initialReport;
        }
        CdcProgressPosition position = CdcProgressPositions.fromOffset(positionType, state.offset);
        CdcProgressValue<CdcProgressPosition> consumedPosition =
                position == null
                        ? CdcProgressValue.unavailable()
                        : CdcProgressValue.exact(position);
        return new CdcReaderProgressReport(
                connectorType,
                state.lifecycle,
                state.splitId,
                consumedPosition,
                CdcProgressValue.unsupported(),
                CdcProgressValue.unsupported(),
                state.lastPositionChangeAt,
                state.latestSourceEventAt);
    }

    private void recordState(
            SourceSplitStateBase splitState,
            Long sourceEventTime,
            boolean emissionObserved,
            long observedAt) {
        String splitId = splitState.splitId();
        CdcProgressLifecycle lifecycle;
        Offset offset = null;
        if (splitState.isSnapshotSplitState()) {
            lifecycle = CdcProgressLifecycle.SNAPSHOT;
        } else {
            IncrementalSplitState incrementalState = splitState.asIncrementalSplitState();
            lifecycle =
                    incrementalState.isEnterPureIncrementPhase()
                            ? CdcProgressLifecycle.INCREMENTAL
                            : CdcProgressLifecycle.CATCH_UP;
            offset = incrementalState.getStartupOffset();
        }
        Offset currentOffset = offset;
        latestState.updateAndGet(
                previous ->
                        nextState(
                                previous,
                                splitId,
                                lifecycle,
                                currentOffset,
                                sourceEventTime,
                                emissionObserved,
                                observedAt));
    }

    private ReaderState nextState(
            ReaderState previous,
            String splitId,
            CdcProgressLifecycle lifecycle,
            Offset offset,
            Long sourceEventTime,
            boolean emissionObserved,
            long observedAt) {
        boolean emittedPositionChanged =
                emissionObserved
                        && offset != null
                        && (previous == null
                                || !previous.emissionObserved
                                || !Objects.equals(previous.offset, offset));
        long lastPositionChangeAt =
                emittedPositionChanged
                        ? observedAt
                        : previous == null ? 0L : previous.lastPositionChangeAt;
        Long latestSourceEventAt =
                sourceEventTime != null && sourceEventTime > 0
                        ? sourceEventTime
                        : previous == null ? null : previous.latestSourceEventAt;
        return new ReaderState(
                splitId,
                lifecycle,
                offset,
                lastPositionChangeAt,
                latestSourceEventAt,
                emissionObserved || (previous != null && previous.emissionObserved));
    }

    private static final class ReaderState {
        private final String splitId;
        private final CdcProgressLifecycle lifecycle;
        private final Offset offset;
        private final long lastPositionChangeAt;
        private final Long latestSourceEventAt;
        private final boolean emissionObserved;

        private ReaderState(
                String splitId,
                CdcProgressLifecycle lifecycle,
                Offset offset,
                long lastPositionChangeAt,
                Long latestSourceEventAt,
                boolean emissionObserved) {
            this.splitId = splitId;
            this.lifecycle = lifecycle;
            this.offset = offset;
            this.lastPositionChangeAt = lastPositionChangeAt;
            this.latestSourceEventAt = latestSourceEventAt;
            this.emissionObserved = emissionObserved;
        }
    }
}
