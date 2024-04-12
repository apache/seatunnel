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

package org.apache.seatunnel.transform.filter;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportTransform;
import org.apache.seatunnel.transform.exception.TransformCommonError;

import org.apache.commons.collections4.CollectionUtils;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class FilterFieldTransform extends AbstractCatalogSupportTransform {
    public static final String PLUGIN_NAME = "Filter";
    private int[] inputValueIndex;
    private final List<String> fields;

    public FilterFieldTransform(
            @NonNull ReadonlyConfig config, @NonNull CatalogTable catalogTable) {
        super(catalogTable);
        SeaTunnelRowType seaTunnelRowType = catalogTable.getTableSchema().toPhysicalRowDataType();
        fields = config.get(FilterFieldTransformConfig.KEY_FIELDS);
        List<String> canNotFoundFields =
                fields.stream()
                        .filter(field -> seaTunnelRowType.indexOf(field, false) == -1)
                        .collect(Collectors.toList());

        if (!CollectionUtils.isEmpty(canNotFoundFields)) {
            throw TransformCommonError.cannotFindInputFieldsError(
                    getPluginName(), canNotFoundFields);
        }
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        // todo reuse array container if not remove fields
        Object[] values = new Object[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            values[i] = inputRow.getField(inputValueIndex[i]);
        }
        SeaTunnelRow outputRow = new SeaTunnelRow(values);
        outputRow.setRowKind(inputRow.getRowKind());
        outputRow.setTableId(inputRow.getTableId());
        return outputRow;
    }

    @Override
    protected TableSchema transformTableSchema() {
        List<Column> outputColumns = new ArrayList<>();

        SeaTunnelRowType seaTunnelRowType =
                inputCatalogTable.getTableSchema().toPhysicalRowDataType();

        inputValueIndex = new int[fields.size()];
        ArrayList<String> outputFieldNames = new ArrayList<>();
        List<Column> inputColumns = inputCatalogTable.getTableSchema().getColumns();
        for (int i = 0; i < fields.size(); i++) {
            String field = fields.get(i);
            int inputFieldIndex = seaTunnelRowType.indexOf(field);
            inputValueIndex[i] = inputFieldIndex;
            outputColumns.add(inputColumns.get(inputFieldIndex).copy());
            outputFieldNames.add(inputColumns.get(inputFieldIndex).getName());
        }

        List<ConstraintKey> outputConstraintKeys =
                inputCatalogTable.getTableSchema().getConstraintKeys().stream()
                        .filter(
                                key -> {
                                    List<String> constraintColumnNames =
                                            key.getColumnNames().stream()
                                                    .map(
                                                            ConstraintKey.ConstraintKeyColumn
                                                                    ::getColumnName)
                                                    .collect(Collectors.toList());
                                    return outputFieldNames.containsAll(constraintColumnNames);
                                })
                        .map(ConstraintKey::copy)
                        .collect(Collectors.toList());

        PrimaryKey copiedPrimaryKey = null;
        PrimaryKey primaryKey = inputCatalogTable.getTableSchema().getPrimaryKey();
        if (primaryKey != null && outputFieldNames.containsAll(primaryKey.getColumnNames())) {
            copiedPrimaryKey = primaryKey.copy();
        }

        return TableSchema.builder()
                .columns(outputColumns)
                .primaryKey(copiedPrimaryKey)
                .constraintKey(outputConstraintKeys)
                .build();
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        return inputCatalogTable.getTableId().copy();
    }
}
