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

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.error.RowErrorClassification;
import org.apache.seatunnel.api.common.error.SupportRowLevelErrorClassifier;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelFlatMapTransform;

import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;

@Slf4j
public class ErrorHandlingFlatMapTransform<T> implements SeaTunnelFlatMapTransform<T> {

    private final SeaTunnelFlatMapTransform<T> delegate;
    private final ErrorHandler<T> errorHandler;
    private final RowErrorClassifier<T> rowErrorClassifier;

    public ErrorHandlingFlatMapTransform(
            SeaTunnelFlatMapTransform<T> delegate,
            ErrorHandler<T> errorHandler,
            RowErrorClassifier<T> rowErrorClassifier) {
        this.delegate = delegate;
        this.errorHandler = errorHandler;
        this.rowErrorClassifier = rowErrorClassifier;
    }

    @Override
    public List<T> flatMap(T row) {
        if (errorHandler != null) {
            errorHandler.incrementTotalRecords();
        }
        try {
            return delegate.flatMap(row);
        } catch (Throwable t) {
            if (t instanceof Error) {
                throw (Error) t;
            }
            if (errorHandler == null || !isRowError(row, t)) {
                if (t instanceof RuntimeException) {
                    throw (RuntimeException) t;
                }
                throw new RuntimeException(t);
            }
            RowErrorContext ctx =
                    new RowErrorContext(
                            "TRANSFORM",
                            "TRANSFORM",
                            delegate.getPluginName(),
                            resolveTableId(row));
            errorHandler.onError(ctx, row, t);
            return Collections.emptyList();
        }
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

        RowErrorContext ctx =
                new RowErrorContext(
                        "TRANSFORM", "TRANSFORM", delegate.getPluginName(), resolveTableId(row));
        return rowErrorClassifier.isRowError(t, row, ctx);
    }

    private String resolveTableId(T row) {
        if (row instanceof SeaTunnelRow) {
            String tableId = ((SeaTunnelRow) row).getTableId();
            return tableId == null ? "" : tableId;
        }
        return "";
    }

    // ---- SeaTunnelTransform delegation ----

    @Override
    public void open() {
        delegate.open();
    }

    @Override
    public CatalogTable getProducedCatalogTable() {
        return delegate.getProducedCatalogTable();
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return delegate.getProducedCatalogTables();
    }

    @Override
    public SchemaChangeEvent mapSchemaChangeEvent(SchemaChangeEvent schemaChangeEvent) {
        return delegate.mapSchemaChangeEvent(schemaChangeEvent);
    }

    @Override
    @Deprecated
    public void setTypeInfo(SeaTunnelDataType<T> inputDataType) {
        delegate.setTypeInfo(inputDataType);
    }

    @Override
    public void setInputCatalogTables(List<CatalogTable> inputCatalogTables) {
        delegate.setInputCatalogTables(inputCatalogTables);
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public String getPluginName() {
        return delegate.getPluginName();
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        delegate.setJobContext(jobContext);
    }
}
