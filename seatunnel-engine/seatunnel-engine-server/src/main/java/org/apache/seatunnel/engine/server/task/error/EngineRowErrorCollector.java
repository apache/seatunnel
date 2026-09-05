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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/** Routes connector-reported row errors to the shared ErrorHandler. */
public final class EngineRowErrorCollector implements RowErrorCollector {

    private final ErrorHandler<SeaTunnelRow> errorHandler;
    private final String pluginName;
    private final AtomicLong collectedErrors = new AtomicLong();
    private final AtomicLong routedErrors = new AtomicLong();
    private final AtomicLong droppedErrors = new AtomicLong();
    private final Map<IdentityRowKey, CollectedRowErrorOutcome> terminalOutcomes =
            new LinkedHashMap<>();
    // Pending probes are scoped to rows currently inside a direct multi-table write call. A row is
    // remembered after drain only when its own probe is active.
    private final Map<IdentityRowKey, TerminalOutcomeProbe> pendingTerminalOutcomeProbes =
            new LinkedHashMap<>();

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
            terminalOutcomes.put(
                    new IdentityRowKey(row), new CollectedRowErrorOutcome(row, result));
        }
    }

    @Override
    public void collectWriteSuccess(SeaTunnelRow row) {
        Objects.requireNonNull(row, "row must not be null");
        synchronized (terminalOutcomes) {
            terminalOutcomes.put(new IdentityRowKey(row), CollectedRowErrorOutcome.written(row));
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
            List<CollectedRowErrorOutcome> drained = new ArrayList<>(terminalOutcomes.values());
            terminalOutcomes.clear();
            if (rememberRecordedRows) {
                for (CollectedRowErrorOutcome outcome : drained) {
                    TerminalOutcomeProbe probe =
                            pendingTerminalOutcomeProbes.get(new IdentityRowKey(outcome.getRow()));
                    if (probe != null && outcome.isTerminalWriteOutcome()) {
                        probe.markRecorded();
                    }
                }
            }
            return drained;
        }
    }

    /** Starts tracking whether this row's terminal outcome is drained before the late callback. */
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
            CollectedRowErrorOutcome outcome = terminalOutcomes.remove(rowKey);
            if (outcome != null) {
                pendingTerminalOutcomeProbes.remove(rowKey);
                return Optional.of(outcome);
            }
            TerminalOutcomeProbe probe = pendingTerminalOutcomeProbes.remove(rowKey);
            if (probe != null && probe.isRecorded()) {
                return Optional.of(CollectedRowErrorOutcome.recorded(row));
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
        private boolean recorded;

        private void markRecorded() {
            recorded = true;
        }

        private boolean isRecorded() {
            return recorded;
        }
    }

    public static final class CollectedRowErrorOutcome {
        private final SeaTunnelRow row;
        private final ErrorHandler.ErrorHandleResult result;
        private final boolean recorded;
        private final boolean written;

        private CollectedRowErrorOutcome(SeaTunnelRow row, ErrorHandler.ErrorHandleResult result) {
            this(row, result, false, false);
        }

        private CollectedRowErrorOutcome(
                SeaTunnelRow row,
                ErrorHandler.ErrorHandleResult result,
                boolean recorded,
                boolean written) {
            this.row = row;
            this.result = result;
            this.recorded = recorded;
            this.written = written;
        }

        private static CollectedRowErrorOutcome recorded(SeaTunnelRow row) {
            return new CollectedRowErrorOutcome(row, null, true, false);
        }

        private static CollectedRowErrorOutcome written(SeaTunnelRow row) {
            return new CollectedRowErrorOutcome(row, null, false, true);
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

        private boolean isTerminalWriteOutcome() {
            return !recorded;
        }
    }
}
