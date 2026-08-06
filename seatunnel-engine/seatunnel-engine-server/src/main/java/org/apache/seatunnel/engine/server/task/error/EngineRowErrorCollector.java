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

package org.apache.seatunnel.engine.server.task.error;

import org.apache.seatunnel.api.common.error.RowErrorCollector;
import org.apache.seatunnel.api.common.error.RowErrorEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/** Routes connector-reported row errors to the shared ErrorHandler. */
public final class EngineRowErrorCollector implements RowErrorCollector {

    private static final int MAX_RECORDED_TERMINAL_ROWS = 10_000;
    private static final int RECORDED_TERMINAL_ROWS_LOW_WATERMARK = 9_000;

    private final ErrorHandler<SeaTunnelRow> errorHandler;
    private final String pluginName;
    private final AtomicLong collectedErrors = new AtomicLong();
    private final AtomicLong routedErrors = new AtomicLong();
    private final AtomicLong droppedErrors = new AtomicLong();
    private final List<CollectedRowErrorOutcome> terminalOutcomes = new ArrayList<>();
    // Recently drained terminal outcomes let late multi-table success callbacks avoid counting a
    // row twice after the flow has already recorded its final write result.
    private final Map<IdentityRowKey, Long> recordedTerminalRows = new LinkedHashMap<>();
    // Pending probes are scoped to rows currently inside a direct multi-table write call. A row is
    // treated as evicted only when its own recorded marker is trimmed while this probe is pending.
    private final Map<IdentityRowKey, TerminalOutcomeProbe> pendingTerminalOutcomeProbes =
            new LinkedHashMap<>();
    private long recordedTerminalRowSequence;

    public EngineRowErrorCollector(ErrorHandler<SeaTunnelRow> errorHandler, String pluginName) {
        this.errorHandler = Objects.requireNonNull(errorHandler, "errorHandler must not be null");
        this.pluginName = Objects.requireNonNull(pluginName, "pluginName must not be null");
    }

    @Override
    public void collect(RowErrorEvent event) {
        Objects.requireNonNull(event, "event must not be null");
        SeaTunnelRow row = event.getRow();
        Throwable error = event.getError();

        String tableId = row.getTableId();
        RowErrorContext ctx =
                new RowErrorContext("SINK", "SINK", pluginName, tableId == null ? "" : tableId);
        ErrorHandler.ErrorHandleResult result = errorHandler.onError(ctx, row, error);
        collectedErrors.incrementAndGet();
        if (result == ErrorHandler.ErrorHandleResult.ROUTED_TO_ERROR_SINK) {
            routedErrors.incrementAndGet();
        } else {
            droppedErrors.incrementAndGet();
        }
        synchronized (terminalOutcomes) {
            terminalOutcomes.add(new CollectedRowErrorOutcome(row, result));
        }
    }

    @Override
    public void collectWriteSuccess(SeaTunnelRow row) {
        Objects.requireNonNull(row, "row must not be null");
        synchronized (terminalOutcomes) {
            terminalOutcomes.add(CollectedRowErrorOutcome.written(row));
        }
    }

    public long getCollectedErrors() {
        return collectedErrors.get();
    }

    public long getRoutedErrors() {
        return routedErrors.get();
    }

    public long getDroppedErrors() {
        return droppedErrors.get();
    }

    public List<CollectedRowErrorOutcome> drainTerminalOutcomes(boolean rememberRecordedRows) {
        synchronized (terminalOutcomes) {
            List<CollectedRowErrorOutcome> drained = new ArrayList<>(terminalOutcomes);
            terminalOutcomes.clear();
            if (rememberRecordedRows) {
                for (CollectedRowErrorOutcome outcome : drained) {
                    if (outcome.isTerminalWriteOutcome()) {
                        recordedTerminalRows.put(
                                new IdentityRowKey(outcome.getRow()),
                                ++recordedTerminalRowSequence);
                    }
                }
                // These entries only bridge a late multi-table callback after the flow has already
                // drained outcomes. Evict only the oldest markers so newly recorded in-flight rows
                // keep suppressing duplicate success callbacks when the bound is crossed.
                trimRecordedTerminalRows();
            }
            return drained;
        }
    }

    /** Starts tracking whether this row's already-recorded terminal marker is evicted. */
    public void beginTerminalOutcomeProbe(SeaTunnelRow row) {
        synchronized (terminalOutcomes) {
            pendingTerminalOutcomeProbes.put(new IdentityRowKey(row), new TerminalOutcomeProbe());
        }
    }

    /**
     * Consumes an exact terminal outcome for the row, or reports eviction only if this row's own
     * recorded marker was trimmed while its probe was active.
     */
    public Optional<CollectedRowErrorOutcome> consumeTerminalOutcome(SeaTunnelRow row) {
        IdentityRowKey rowKey = new IdentityRowKey(row);
        synchronized (terminalOutcomes) {
            for (int i = 0; i < terminalOutcomes.size(); i++) {
                CollectedRowErrorOutcome outcome = terminalOutcomes.get(i);
                if (outcome.getRow() == row) {
                    terminalOutcomes.remove(i);
                    pendingTerminalOutcomeProbes.remove(rowKey);
                    return Optional.of(outcome);
                }
            }
            if (recordedTerminalRows.remove(rowKey) != null) {
                pendingTerminalOutcomeProbes.remove(rowKey);
                return Optional.of(CollectedRowErrorOutcome.recorded(row));
            }
            TerminalOutcomeProbe probe = pendingTerminalOutcomeProbes.remove(rowKey);
            if (probe != null && probe.isRecordedMarkerEvicted()) {
                return Optional.of(CollectedRowErrorOutcome.evicted(row));
            }
        }
        return Optional.empty();
    }

    /**
     * Clears the pending probe when a direct write throws before the late callback can consume it.
     */
    public void clearTerminalOutcomeProbe(SeaTunnelRow row) {
        synchronized (terminalOutcomes) {
            pendingTerminalOutcomeProbes.remove(new IdentityRowKey(row));
        }
    }

    private void trimRecordedTerminalRows() {
        if (recordedTerminalRows.size() <= MAX_RECORDED_TERMINAL_ROWS) {
            return;
        }
        Iterator<Map.Entry<IdentityRowKey, Long>> iterator =
                recordedTerminalRows.entrySet().iterator();
        while (recordedTerminalRows.size() > RECORDED_TERMINAL_ROWS_LOW_WATERMARK
                && iterator.hasNext()) {
            Map.Entry<IdentityRowKey, Long> evictedEntry = iterator.next();
            TerminalOutcomeProbe probe = pendingTerminalOutcomeProbes.get(evictedEntry.getKey());
            if (probe != null) {
                probe.markRecordedMarkerEvicted();
            }
            iterator.remove();
        }
    }

    private static final class IdentityRowKey {
        private final SeaTunnelRow row;

        private IdentityRowKey(SeaTunnelRow row) {
            this.row = row;
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof IdentityRowKey && row == ((IdentityRowKey) obj).row;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(row);
        }
    }

    private static final class TerminalOutcomeProbe {
        private boolean recordedMarkerEvicted;

        private void markRecordedMarkerEvicted() {
            recordedMarkerEvicted = true;
        }

        private boolean isRecordedMarkerEvicted() {
            return recordedMarkerEvicted;
        }
    }

    public static final class CollectedRowErrorOutcome {
        private final SeaTunnelRow row;
        private final ErrorHandler.ErrorHandleResult result;
        private final boolean recorded;
        private final boolean written;
        private final boolean evicted;

        private CollectedRowErrorOutcome(SeaTunnelRow row, ErrorHandler.ErrorHandleResult result) {
            this(row, result, false, false, false);
        }

        private CollectedRowErrorOutcome(
                SeaTunnelRow row,
                ErrorHandler.ErrorHandleResult result,
                boolean recorded,
                boolean written,
                boolean evicted) {
            this.row = row;
            this.result = result;
            this.recorded = recorded;
            this.written = written;
            this.evicted = evicted;
        }

        private static CollectedRowErrorOutcome recorded(SeaTunnelRow row) {
            return new CollectedRowErrorOutcome(row, null, true, false, false);
        }

        private static CollectedRowErrorOutcome written(SeaTunnelRow row) {
            return new CollectedRowErrorOutcome(row, null, false, true, false);
        }

        private static CollectedRowErrorOutcome evicted(SeaTunnelRow row) {
            return new CollectedRowErrorOutcome(row, null, false, false, true);
        }

        public SeaTunnelRow getRow() {
            return row;
        }

        public ErrorHandler.ErrorHandleResult getResult() {
            return result;
        }

        public boolean isRecorded() {
            return recorded;
        }

        public boolean isWritten() {
            return written;
        }

        public boolean isEvicted() {
            return evicted;
        }

        private boolean isTerminalWriteOutcome() {
            return !recorded && !evicted;
        }
    }
}
