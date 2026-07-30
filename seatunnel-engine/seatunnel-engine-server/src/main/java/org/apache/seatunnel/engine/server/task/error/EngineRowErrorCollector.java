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
import java.util.IdentityHashMap;
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
    private final List<CollectedRowErrorOutcome> terminalOutcomes = new ArrayList<>();
    private final Map<SeaTunnelRow, Boolean> recordedTerminalRows = new IdentityHashMap<>();

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
                    recordedTerminalRows.put(outcome.getRow(), Boolean.TRUE);
                }
            }
            return drained;
        }
    }

    public Optional<CollectedRowErrorOutcome> consumeTerminalOutcome(SeaTunnelRow row) {
        synchronized (terminalOutcomes) {
            for (int i = 0; i < terminalOutcomes.size(); i++) {
                CollectedRowErrorOutcome outcome = terminalOutcomes.get(i);
                if (outcome.getRow() == row) {
                    terminalOutcomes.remove(i);
                    return Optional.of(outcome);
                }
            }
            if (recordedTerminalRows.remove(row) != null) {
                return Optional.of(CollectedRowErrorOutcome.recorded(row));
            }
        }
        return Optional.empty();
    }

    public static final class CollectedRowErrorOutcome {
        private final SeaTunnelRow row;
        private final ErrorHandler.ErrorHandleResult result;
        private final boolean recorded;

        private CollectedRowErrorOutcome(SeaTunnelRow row, ErrorHandler.ErrorHandleResult result) {
            this(row, result, false);
        }

        private CollectedRowErrorOutcome(
                SeaTunnelRow row, ErrorHandler.ErrorHandleResult result, boolean recorded) {
            this.row = row;
            this.result = result;
            this.recorded = recorded;
        }

        private static CollectedRowErrorOutcome recorded(SeaTunnelRow row) {
            return new CollectedRowErrorOutcome(row, null, true);
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
    }
}
