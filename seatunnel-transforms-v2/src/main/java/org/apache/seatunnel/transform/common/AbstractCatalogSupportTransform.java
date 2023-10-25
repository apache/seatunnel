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
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import lombok.NonNull;

public abstract class AbstractCatalogSupportTransform extends AbstractSeaTunnelTransform {
    protected CatalogTable inputCatalogTable;

    protected volatile CatalogTable outputCatalogTable;

    public AbstractCatalogSupportTransform(@NonNull CatalogTable inputCatalogTable) {
        this.inputCatalogTable = inputCatalogTable;
    }

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
        CatalogTable catalogTable =
                CatalogTable.of(
                        tableIdentifier,
                        tableSchema,
                        inputCatalogTable.getOptions(),
                        inputCatalogTable.getPartitionKeys(),
                        inputCatalogTable.getComment());
        return catalogTable;
    }

    protected abstract TableSchema transformTableSchema();

    protected abstract TableIdentifier transformTableIdentifier();

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        if (outputRowType != null) {
            return outputRowType;
        }
        return getProducedCatalogTable().getTableSchema().toPhysicalRowDataType();
    }
}
