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

package org.apache.seatunnel.transform.common;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.transform.exception.ErrorDataTransformException;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractCatalogSupportTransform implements SeaTunnelTransform<SeaTunnelRow> {
    protected final ErrorHandleWay rowErrorHandleWay;
    protected CatalogTable inputCatalogTable;

    protected volatile CatalogTable outputCatalogTable;

    public AbstractCatalogSupportTransform(@NonNull CatalogTable inputCatalogTable) {
        this(inputCatalogTable, CommonOptions.ROW_ERROR_HANDLE_WAY_OPTION.defaultValue());
    }

    public AbstractCatalogSupportTransform(
            @NonNull CatalogTable inputCatalogTable, ErrorHandleWay rowErrorHandleWay) {
        this.inputCatalogTable = inputCatalogTable;
        this.rowErrorHandleWay = rowErrorHandleWay;
    }

    @Override
    public SeaTunnelRow map(SeaTunnelRow row) {
        try {
            return transformRow(row);
        } catch (ErrorDataTransformException e) {
            if (e.getErrorHandleWay() != null) {
                ErrorHandleWay errorHandleWay = e.getErrorHandleWay();
                if (errorHandleWay.allowSkipThisRow()) {
                    log.debug("Skip row due to error", e);
                    return null;
                }
                throw e;
            }
            if (rowErrorHandleWay.allowSkip()) {
                log.debug("Skip row due to error", e);
                return null;
            }
            throw e;
        }
    }

    /**
     * Outputs transformed row data.
     *
     * @param inputRow upstream input row data
     */
    protected abstract SeaTunnelRow transformRow(SeaTunnelRow inputRow);

    @Override
    public CatalogTable getProducedCatalogTable() {
        if (outputCatalogTable == null) {
            synchronized (this) {
                if (outputCatalogTable == null) {
                    outputCatalogTable = transformCatalogTable();
                }
            }
        }

        return outputCatalogTable;
    }

    private CatalogTable transformCatalogTable() {
        TableIdentifier tableIdentifier = transformTableIdentifier();
        TableSchema tableSchema = transformTableSchema();
        return CatalogTable.of(
                tableIdentifier,
                tableSchema,
                inputCatalogTable.getOptions(),
                inputCatalogTable.getPartitionKeys(),
                inputCatalogTable.getComment());
    }

    protected abstract TableSchema transformTableSchema();

    protected abstract TableIdentifier transformTableIdentifier();
}
