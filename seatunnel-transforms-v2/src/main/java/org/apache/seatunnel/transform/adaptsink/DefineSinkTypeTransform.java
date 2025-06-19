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

package org.apache.seatunnel.transform.adaptsink;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportMapTransform;

import java.util.Map;
import java.util.stream.Collectors;

public class DefineSinkTypeTransform extends AbstractCatalogSupportMapTransform {

    private final Map<String, DefineSinkTypeTransformConfig.DefineColumnType> columnConfig;

    public DefineSinkTypeTransform(
            DefineSinkTypeTransformConfig config, CatalogTable inputCatalogTable) {
        super(inputCatalogTable);
        this.columnConfig = config.toMap();
        columnConfig
                .keySet()
                .forEach(
                        key -> {
                            if (inputCatalogTable.getTableSchema().indexOf(key) < 0) {
                                throw new IllegalArgumentException(
                                        String.format(
                                                "Column %s not found in table %s rowtype : %s",
                                                key,
                                                inputCatalogTable.getTablePath(),
                                                inputCatalogTable.getSeaTunnelRowType()));
                            }
                        });
    }

    @Override
    public String getPluginName() {
        return DefineSinkTypeTransformConfig.PLUGIN_NAME;
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        return inputCatalogTable.getTableId();
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        return inputRow;
    }

    @Override
    protected TableSchema transformTableSchema() {
        return TableSchema.builder()
                .primaryKey(inputCatalogTable.getTableSchema().getPrimaryKey())
                .constraintKey(inputCatalogTable.getTableSchema().getConstraintKeys())
                .columns(
                        inputCatalogTable.getTableSchema().getColumns().stream()
                                .map(
                                        column -> {
                                            if (!columnConfig.containsKey(column.getName())) {
                                                return column;
                                            }

                                            DefineSinkTypeTransformConfig.DefineColumnType
                                                    defineColumnType =
                                                            columnConfig.get(column.getName());
                                            Column newColumn = column.copy();
                                            newColumn.setSinkType(defineColumnType.getType());
                                            return newColumn;
                                        })
                                .collect(Collectors.toList()))
                .build();
    }
}
