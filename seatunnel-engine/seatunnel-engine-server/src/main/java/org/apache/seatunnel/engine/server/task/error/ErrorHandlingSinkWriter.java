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
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSinkWriter;
import org.apache.seatunnel.api.sink.multitablesink.MultiTableSinkWriter;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.function.RunnableWithException;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/** SinkWriter wrapper that adds row-level error handling. */
@Slf4j
public class ErrorHandlingSinkWriter<T, CommT, StateT> implements SinkWriter<T, CommT, StateT> {

    public enum WriteOutcome {
        WRITTEN,
        ROUTED_TO_ERROR_SINK,
        DROPPED
    }

    private final SinkWriter<T, CommT, StateT> delegate;
    private final ErrorHandler<T> errorHandler;
    private final RowErrorClassifier<T> rowErrorClassifier;
    private final String pluginName;

    public ErrorHandlingSinkWriter(
            SinkWriter<T, CommT, StateT> delegate,
            ErrorHandler<T> errorHandler,
            RowErrorClassifier<T> rowErrorClassifier,
            String pluginName) {
        this.delegate = delegate;
        this.errorHandler = errorHandler;
        this.rowErrorClassifier = rowErrorClassifier;
        this.pluginName = pluginName;
    }

    /** Adds ErrorData flushing to the sink timer while preserving the connector's flush action. */
    public void registerFlushAction(SinkWriter.Context context) {
        RunnableWithException delegateFlushAction = context.getFlushAction();
        context.registerFlushAction(
                () -> {
                    if (delegateFlushAction != null) {
                        delegateFlushAction.run();
                    }
                    flushErrorHandler();
                });
    }

    @Override
    public void write(T element) throws IOException {
        writeWithOutcome(element);
    }

    public WriteOutcome writeWithOutcome(T element) throws IOException {
        if (errorHandler != null) {
            errorHandler.incrementTotalRecords();
        }
        try {
            delegate.write(element);
            return WriteOutcome.WRITTEN;
        } catch (Throwable ex) {
            if (ex instanceof Error) {
                throw (Error) ex;
            }
            if (errorHandler == null || !isRowError(element, ex)) {
                if (ex instanceof IOException) {
                    throw (IOException) ex;
                }
                if (ex instanceof RuntimeException) {
                    throw (RuntimeException) ex;
                }
                throw new IOException(ex);
            }

            RowErrorContext ctx =
                    new RowErrorContext("SINK", "SINK", pluginName, resolveTableId(element));
            return toWriteOutcome(errorHandler.onError(ctx, element, ex));
        }
    }

    public boolean wrapsMultiTableSinkWriter() {
        return delegate instanceof MultiTableSinkWriter;
    }

    static WriteOutcome toWriteOutcome(ErrorHandler.ErrorHandleResult result) {
        return result == ErrorHandler.ErrorHandleResult.ROUTED_TO_ERROR_SINK
                ? WriteOutcome.ROUTED_TO_ERROR_SINK
                : WriteOutcome.DROPPED;
    }

    @SuppressWarnings("unchecked")
    private boolean isRowError(T row, Throwable t) {
        try {
            if (delegate instanceof SupportRowLevelErrorClassifier) {
                RowErrorClassification classification =
                        ((SupportRowLevelErrorClassifier<T>) delegate).classifyRowError(t, row);
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

        RowErrorContext ctx = new RowErrorContext("SINK", "SINK", pluginName, resolveTableId(row));
        return rowErrorClassifier.isRowError(t, row, ctx);
    }

    private String resolveTableId(T row) {
        if (row instanceof SeaTunnelRow) {
            String tableId = ((SeaTunnelRow) row).getTableId();
            return tableId == null ? "" : tableId;
        }
        return "";
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent event) throws IOException {
        SupportSchemaEvolutionSinkWriter.applySchemaChangeToWriter(delegate, event);
    }

    @Override
    public Optional<CommT> prepareCommit() throws IOException {
        return delegate.prepareCommit();
    }

    @Override
    public Optional<CommT> prepareCommit(long checkpointId) throws IOException {
        return delegate.prepareCommit(checkpointId);
    }

    @Override
    public List<StateT> snapshotState(long checkpointId) throws IOException {
        List<StateT> states = delegate.snapshotState(checkpointId);
        flushErrorHandler(checkpointId);
        if (errorHandler != null) {
            errorHandler.snapshotState(checkpointId);
        }
        return states;
    }

    @Override
    public void abortPrepare() {
        delegate.abortPrepare();
    }

    @Override
    public void close() throws IOException {
        IOException closeException = null;
        try {
            delegate.close();
        } catch (IOException e) {
            closeException = e;
        } finally {
            if (errorHandler != null) {
                try {
                    errorHandler.close();
                } catch (Exception e) {
                    log.error("Failed to close ErrorHandler for sink writer", e);
                    IOException errorHandlerCloseException = toIOException(e);
                    if (closeException == null) {
                        closeException = errorHandlerCloseException;
                    } else {
                        closeException.addSuppressed(errorHandlerCloseException);
                    }
                }
            }
        }
        if (closeException != null) {
            throw closeException;
        }
    }

    private void flushErrorHandler() throws IOException {
        flushErrorHandler(null);
    }

    private void flushErrorHandler(Long checkpointId) throws IOException {
        if (errorHandler == null) {
            return;
        }
        try {
            if (checkpointId == null) {
                errorHandler.flush();
            } else {
                errorHandler.flush(checkpointId);
            }
        } catch (Exception e) {
            throw toIOException(e);
        }
    }

    private IOException toIOException(Exception e) {
        if (e instanceof IOException) {
            return (IOException) e;
        }
        return new IOException(e);
    }
}
