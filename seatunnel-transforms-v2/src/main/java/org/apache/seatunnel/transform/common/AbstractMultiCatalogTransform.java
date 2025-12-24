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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Abstract class for multi-table transform. It is used to split the input data into multiple table
 * transforms.
 */
public abstract class AbstractMultiCatalogTransform implements SeaTunnelTransform<SeaTunnelRow> {

    protected List<CatalogTable> inputCatalogTables;

    protected List<CatalogTable> outputCatalogTables;

    protected Map<String, SeaTunnelTransform<SeaTunnelRow>> transformMap;

    public AbstractMultiCatalogTransform(
            List<CatalogTable> inputCatalogTables, ReadonlyConfig config) {
        this.inputCatalogTables = inputCatalogTables;
        this.transformMap = new HashMap<>();
        Pattern tableMatchRegex =
                Pattern.compile(config.get(TransformCommonOptions.TABLE_MATCH_REGEX));
        Map<String, ReadonlyConfig> singleTableConfig =
                config.get(TransformCommonOptions.MULTI_TABLES).stream()
                        .map(ReadonlyConfig::fromMap)
                        .filter(c -> c.get(TransformCommonOptions.TABLE_PATH) != null)
                        .collect(
                                Collectors.toMap(
                                        c -> c.get(TransformCommonOptions.TABLE_PATH),
                                        Function.identity()));

        inputCatalogTables.forEach(
                inputCatalogTable -> {
                    String tableId = inputCatalogTable.getTableId().toTablePath().toString();
                    ReadonlyConfig tableConfig;
                    if (singleTableConfig.containsKey(tableId)) {
                        tableConfig = singleTableConfig.get(tableId);
                    } else if (tableMatchRegex.matcher(tableId).matches()) {
                        tableConfig = config;
                    } else {
                        tableConfig = null;
                    }
                    if (tableConfig != null) {
                        transformMap.put(tableId, buildTransform(inputCatalogTable, tableConfig));
                    } else {
                        transformMap.put(tableId, new IdentityTransform(inputCatalogTable));
                    }
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
    public void setTypeInfo(SeaTunnelDataType<SeaTunnelRow> inputDataType) {}

    public static class IdentityTransform extends AbstractCatalogSupportMapTransform {
        private final CatalogTable catalogTable;

        @Override
        public String getPluginName() {
            return "Identity";
        }

        public IdentityTransform(CatalogTable catalogTable) {
            super(catalogTable);
            this.catalogTable = catalogTable;
        }

        @Override
        protected SeaTunnelRow transformRow(SeaTunnelRow row) {
            return row;
        }

        @Override
        protected TableSchema transformTableSchema() {
            return catalogTable.getTableSchema();
        }

        @Override
        protected TableIdentifier transformTableIdentifier() {
            return catalogTable.getTableId();
        }

        @Override
        public void setTypeInfo(SeaTunnelDataType<SeaTunnelRow> inputDataType) {}
    }
}
