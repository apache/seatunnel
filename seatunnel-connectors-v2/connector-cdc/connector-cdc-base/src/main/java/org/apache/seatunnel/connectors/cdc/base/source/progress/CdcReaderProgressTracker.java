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

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.api.cdc.CdcProgressLifecycle;
import org.apache.seatunnel.api.cdc.CdcProgressPosition;
import org.apache.seatunnel.api.cdc.CdcProgressValue;
import org.apache.seatunnel.api.cdc.CdcReaderProgressReport;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.split.state.IncrementalSplitState;
import org.apache.seatunnel.connectors.cdc.base.source.split.state.SourceSplitStateBase;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/** Maintains the latest immutable report without performing I/O from the record-emission path. */
public final class CdcReaderProgressTracker {

    private static final long PUBLICATION_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(1);
    private final String connectorType;
    private final String positionType;
    private final CdcReaderProgressReport initialReport;
    private final AtomicReference<ReaderState> latestState = new AtomicReference<>();
    private final LongSupplier nanoClock;

    public CdcReaderProgressTracker(String connectorType, String positionType) {
        this(connectorType, positionType, System::nanoTime);
    }

    @VisibleForTesting
    public CdcReaderProgressTracker(
            String connectorType, String positionType, LongSupplier nanoClock) {
        this.connectorType = connectorType;
        this.positionType = positionType;
        this.nanoClock = nanoClock;
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

    /** Records a configured/restored start, not evidence that this reader consumed a record. */
    public void recordSplitState(SourceSplitStateBase splitState) {
        recordState(splitState, null, false, 0L);
    }

    /**
     * Tests the publication budget without reading offset coordinates. Call only after successful
     * processing; first consumption and split/lifecycle changes bypass the sampling interval.
     */
    public boolean shouldRecordEmission(SourceSplitStateBase splitState) {
        ReaderState previous = latestState.get();
        return previous == null
                || !previous.emissionObserved
                || !Objects.equals(previous.splitId, splitState.splitId())
                || previous.lifecycle != lifecycle(splitState)
                || nanoClock.getAsLong() - previous.publishedAtNanos >= PUBLICATION_INTERVAL_NANOS;
    }

    /** Publishes a selected successful observation as one detached position/lifecycle tuple. */
    public void recordEmission(
            SourceSplitStateBase splitState, Long sourceEventTime, long observedAt) {
        recordState(splitState, sourceEventTime, true, observedAt);
    }

    /** Samples one immutable state; an unconsumed starting position is only best effort. */
    public CdcReaderProgressReport current() {
        ReaderState state = latestState.get();
        if (state == null) {
            return initialReport;
        }
        CdcProgressPosition position = state.position;
        CdcProgressValue<CdcProgressPosition> consumedPosition =
                position == null
                        ? CdcProgressValue.unavailable()
                        : state.emissionObserved
                                ? CdcProgressValue.exact(position)
                                : CdcProgressValue.bestEffort(position);
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
        CdcProgressLifecycle lifecycle = lifecycle(splitState);
        Offset offset = null;
        if (!splitState.isSnapshotSplitState()) {
            IncrementalSplitState incrementalState = splitState.asIncrementalSplitState();
            offset = incrementalState.getStartupOffset();
        }
        // Offsets can be mutated in place before processing succeeds. Detach coordinates at
        // publication so polling never observes that live state or advances failed emissions.
        CdcProgressPosition position = CdcProgressPositions.fromOffset(positionType, offset);
        long publishedAtNanos = nanoClock.getAsLong();
        latestState.updateAndGet(
                previous ->
                        nextState(
                                previous,
                                splitId,
                                lifecycle,
                                position,
                                sourceEventTime,
                                emissionObserved,
                                observedAt,
                                publishedAtNanos));
    }

    private static CdcProgressLifecycle lifecycle(SourceSplitStateBase splitState) {
        if (splitState.isSnapshotSplitState()) {
            return CdcProgressLifecycle.SNAPSHOT;
        }
        return splitState.asIncrementalSplitState().isEnterPureIncrementPhase()
                ? CdcProgressLifecycle.INCREMENTAL
                : CdcProgressLifecycle.CATCH_UP;
    }

    private ReaderState nextState(
            ReaderState previous,
            String splitId,
            CdcProgressLifecycle lifecycle,
            CdcProgressPosition position,
            Long sourceEventTime,
            boolean emissionObserved,
            long observedAt,
            long publishedAtNanos) {
        if (!emissionObserved || (previous != null && !Objects.equals(previous.splitId, splitId))) {
            previous = null;
        }
        boolean emittedPositionChanged =
                emissionObserved
                        && position != null
                        && (previous == null
                                || !previous.emissionObserved
                                || previous.position == null
                                || !previous.position.getValues().equals(position.getValues()));
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
                position,
                lastPositionChangeAt,
                latestSourceEventAt,
                emissionObserved || (previous != null && previous.emissionObserved),
                publishedAtNanos);
    }

    private static final class ReaderState {
        private final String splitId;
        private final CdcProgressLifecycle lifecycle;
        private final CdcProgressPosition position;
        private final long lastPositionChangeAt;
        private final Long latestSourceEventAt;
        private final boolean emissionObserved;
        private final long publishedAtNanos;

        private ReaderState(
                String splitId,
                CdcProgressLifecycle lifecycle,
                CdcProgressPosition position,
                long lastPositionChangeAt,
                Long latestSourceEventAt,
                boolean emissionObserved,
                long publishedAtNanos) {
            this.splitId = splitId;
            this.lifecycle = lifecycle;
            this.position = position;
            this.lastPositionChangeAt = lastPositionChangeAt;
            this.latestSourceEventAt = latestSourceEventAt;
            this.emissionObserved = emissionObserved;
            this.publishedAtNanos = publishedAtNanos;
        }
    }
}
