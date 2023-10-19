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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class AbstractMultiCatalogSupportTransform
        implements SeaTunnelTransform<SeaTunnelRow> {

    protected List<CatalogTable> inputCatalogTables;

    protected List<CatalogTable> outputCatalogTables;

    protected Map<String, SeaTunnelTransform<SeaTunnelRow>> transformMap;

    public AbstractMultiCatalogSupportTransform(
            List<CatalogTable> inputCatalogTables, ReadonlyConfig config) {
        this.inputCatalogTables = inputCatalogTables;
        this.transformMap = new HashMap<>();
        inputCatalogTables.forEach(
                inputCatalogTable -> {
                    String tableId = inputCatalogTable.getTableId().toTablePath().toString();
                    transformMap.put(tableId, buildTransform(inputCatalogTable, config));
                });

        this.outputCatalogTables =
                inputCatalogTables.stream()
                        .map(
                                inputCatalogTable -> {
                                    String tableName =
                                            inputCatalogTable.getTableId().toTablePath().toString();
                                    return transformMap.get(tableName).getProducedCatalogTable();
                                })
                        .collect(Collectors.toList());
    }

    @Override
    public SeaTunnelRow map(SeaTunnelRow row) {
        if (transformMap.size() == 1) {
            return transformMap.values().iterator().next().map(row);
        }
        return transformMap.get(row.getTableId()).map(row);
    }

    protected abstract SeaTunnelTransform<SeaTunnelRow> buildTransform(
            CatalogTable inputCatalogTable, ReadonlyConfig config);

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return outputCatalogTables;
    }

    @Override
    public CatalogTable getProducedCatalogTable() {
        return outputCatalogTables.get(0);
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return outputCatalogTables.get(0).getTableSchema().toPhysicalRowDataType();
    }

    @Override
    public void setTypeInfo(SeaTunnelDataType<SeaTunnelRow> inputDataType) {}

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {}
}
