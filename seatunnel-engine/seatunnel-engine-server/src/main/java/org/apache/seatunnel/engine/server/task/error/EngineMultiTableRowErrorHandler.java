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

import org.apache.seatunnel.api.common.error.RowErrorClassification;
import org.apache.seatunnel.api.common.error.SupportRowLevelErrorClassifier;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.multitablesink.MultiTableRowErrorHandler;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import lombok.extern.slf4j.Slf4j;

import java.util.function.BiConsumer;

/** Bridges multi-table sink sub-writer errors to the shared ErrorHandler. */
@Slf4j
public class EngineMultiTableRowErrorHandler implements MultiTableRowErrorHandler {

    private final ErrorHandler<SeaTunnelRow> errorHandler;
    private final RowErrorClassifier<SeaTunnelRow> rowErrorClassifier;
    private final String pluginName;
    private final BiConsumer<SeaTunnelRow, ErrorHandlingSinkWriter.WriteOutcome> outcomeConsumer;
    private final EngineRowErrorCollector rowErrorCollector;

    public EngineMultiTableRowErrorHandler(
            ErrorHandler<SeaTunnelRow> errorHandler,
            RowErrorClassifier<SeaTunnelRow> rowErrorClassifier,
            String pluginName) {
        this(errorHandler, rowErrorClassifier, pluginName, (row, outcome) -> {}, null);
    }

    public EngineMultiTableRowErrorHandler(
            ErrorHandler<SeaTunnelRow> errorHandler,
            RowErrorClassifier<SeaTunnelRow> rowErrorClassifier,
            String pluginName,
            BiConsumer<SeaTunnelRow, ErrorHandlingSinkWriter.WriteOutcome> outcomeConsumer) {
        this(errorHandler, rowErrorClassifier, pluginName, outcomeConsumer, null);
    }

    public EngineMultiTableRowErrorHandler(
            ErrorHandler<SeaTunnelRow> errorHandler,
            RowErrorClassifier<SeaTunnelRow> rowErrorClassifier,
            String pluginName,
            BiConsumer<SeaTunnelRow, ErrorHandlingSinkWriter.WriteOutcome> outcomeConsumer,
            EngineRowErrorCollector rowErrorCollector) {
        this.errorHandler = errorHandler;
        this.rowErrorClassifier = rowErrorClassifier;
        this.pluginName = pluginName;
        this.outcomeConsumer = outcomeConsumer;
        this.rowErrorCollector = rowErrorCollector;
    }

    @Override
    public boolean handleRowError(
            SinkWriter<SeaTunnelRow, ?, ?> writer, String tableId, SeaTunnelRow row, Throwable t) {
        String effectiveTableId = tableId != null ? tableId : resolveTableId(row);

        if (!isRowError(writer, row, t)) {
            // System-level error: let caller treat it as fatal.
            log.warn(
                    "Multi-table sink encountered non-row-level error in plugin [{}], table [{}]: {}",
                    pluginName,
                    effectiveTableId,
                    t != null ? t.getMessage() : null,
                    t);
            return false;
        }

        RowErrorContext ctx = new RowErrorContext("SINK", "SINK", pluginName, effectiveTableId);

        // Delegate to shared ErrorHandler; it may throw when thresholds or queue policies demand
        // a job failure.
        log.debug(
                "Routing multi-table row-level error to error handler for plugin [{}], table [{}]: {}",
                pluginName,
                effectiveTableId,
                t != null ? t.getMessage() : null);
        ErrorHandler.ErrorHandleResult result = errorHandler.onError(ctx, row, t);
        outcomeConsumer.accept(row, ErrorHandlingSinkWriter.toWriteOutcome(result));
        return true;
    }

    @Override
    public void beginCollectedRowErrorOutcomeProbe(SeaTunnelRow row) {
        if (rowErrorCollector != null && row != null) {
            rowErrorCollector.beginTerminalOutcomeProbe(row);
        }
    }

    @Override
    public boolean consumeCollectedRowErrorOutcome(SeaTunnelRow row) {
        if (rowErrorCollector == null) {
            return false;
        }
        return rowErrorCollector
                .consumeTerminalOutcome(row)
                .map(
                        outcome -> {
                            if (outcome.isWritten()) {
                                outcomeConsumer.accept(
                                        outcome.getRow(),
                                        ErrorHandlingSinkWriter.WriteOutcome.WRITTEN);
                            } else if (!outcome.isRecorded()) {
                                outcomeConsumer.accept(
                                        outcome.getRow(),
                                        ErrorHandlingSinkWriter.toWriteOutcome(
                                                outcome.getResult()));
                            }
                            return true;
                        })
                .orElse(false);
    }

    @Override
    public void clearCollectedRowErrorOutcomeProbe(SeaTunnelRow row) {
        if (rowErrorCollector != null && row != null) {
            rowErrorCollector.clearTerminalOutcomeProbe(row);
        }
    }

    private boolean isRowError(
            SinkWriter<SeaTunnelRow, ?, ?> writer, SeaTunnelRow row, Throwable t) {
        try {
            if (writer instanceof SupportRowLevelErrorClassifier) {
                @SuppressWarnings("unchecked")
                SupportRowLevelErrorClassifier<SeaTunnelRow> support =
                        (SupportRowLevelErrorClassifier<SeaTunnelRow>) writer;
                RowErrorClassification classification = support.classifyRowError(t, row);
                return classification != null && classification.isRowError();
            }
        } catch (Throwable ex) {
            if (ex instanceof Error) {
                throw (Error) ex;
            }
            log.debug(
                    "SupportRowLevelErrorClassifier.classifyRowError threw exception, fallback to classifier",
                    ex);
        }

        if (rowErrorClassifier == null) {
            return false;
        }

        RowErrorContext ctx =
                new RowErrorContext("SINK", "SINK", pluginName, tableIdOrRowTableId(row, null));
        return rowErrorClassifier.isRowError(t, row, ctx);
    }

    private String tableIdOrRowTableId(SeaTunnelRow row, String tableId) {
        if (tableId != null) {
            return tableId;
        }
        return resolveTableId(row);
    }

    private String resolveTableId(SeaTunnelRow row) {
        if (row == null) {
            return "";
        }
        String tableId = row.getTableId();
        return tableId == null ? "" : tableId;
    }
}
