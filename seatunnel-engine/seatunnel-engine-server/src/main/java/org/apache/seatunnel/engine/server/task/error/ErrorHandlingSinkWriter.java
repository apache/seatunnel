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

import org.apache.seatunnel.api.common.SupportRowLevelError;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/** SinkWriter wrapper that adds row-level error handling. */
@Slf4j
public class ErrorHandlingSinkWriter<T, CommT, StateT> implements SinkWriter<T, CommT, StateT> {

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

    @Override
    public void write(T element) throws IOException {
        if (errorHandler != null) {
            errorHandler.incrementTotalRecords();
        }
        try {
            delegate.write(element);
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
            errorHandler.onError(ctx, element, ex);
        }
    }

    @SuppressWarnings("unchecked")
    private boolean isRowError(T row, Throwable t) {
        try {
            if (delegate instanceof SupportRowLevelError) {
                return ((SupportRowLevelError<T>) delegate).isRowError(t, row);
            }
        } catch (Throwable ex) {
            if (ex instanceof Error) {
                throw (Error) ex;
            }
            log.debug(
                    "SupportRowLevelError.isRowError threw exception, fallback to classifier", ex);
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
        delegate.applySchemaChange(event);
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
        return delegate.snapshotState(checkpointId);
    }

    @Override
    public void abortPrepare() {
        delegate.abortPrepare();
    }

    @Override
    public void close() throws IOException {
        try {
            delegate.close();
        } finally {
            if (errorHandler != null) {
                try {
                    errorHandler.close();
                } catch (Exception e) {
                    log.error("Failed to close ErrorHandler for sink writer", e);
                }
            }
        }
    }
}
