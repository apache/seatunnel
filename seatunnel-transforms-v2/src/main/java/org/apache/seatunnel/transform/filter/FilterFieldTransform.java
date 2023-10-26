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
import org.apache.seatunnel.transform.exception.FilterFieldTransformErrorCode;
import org.apache.seatunnel.transform.exception.TransformException;

import org.apache.commons.collections4.CollectionUtils;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class FilterFieldTransform extends AbstractCatalogSupportTransform {
    public static final String PLUGIN_NAME = "Filter";
    private int[] inputValueIndex;
    private String[] fields;

    public FilterFieldTransform(
            @NonNull ReadonlyConfig config, @NonNull CatalogTable catalogTable) {
        super(catalogTable);
        SeaTunnelRowType seaTunnelRowType = catalogTable.getTableSchema().toPhysicalRowDataType();
        fields = config.get(FilterFieldTransformConfig.KEY_FIELDS).toArray(new String[0]);
        List<String> canNotFoundFields =
                Arrays.stream(fields)
                        .filter(field -> seaTunnelRowType.indexOf(field) == -1)
                        .collect(Collectors.toList());

        if (!CollectionUtils.isEmpty(canNotFoundFields)) {
            throw new TransformException(
                    FilterFieldTransformErrorCode.FILTER_FIELD_NOT_FOUND,
                    canNotFoundFields.toString());
        }
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        // todo reuse array container if not remove fields
        Object[] values = new Object[fields.length];
        for (int i = 0; i < fields.length; i++) {
            values[i] = inputRow.getField(inputValueIndex[i]);
        }
        return new SeaTunnelRow(values);
    }

    @Override
    protected TableSchema transformTableSchema() {
        List<String> filterFields = Arrays.asList(fields);
        List<Column> outputColumns = new ArrayList<>();

        SeaTunnelRowType seaTunnelRowType =
                inputCatalogTable.getTableSchema().toPhysicalRowDataType();

        inputValueIndex = new int[filterFields.size()];
        ArrayList<String> outputFieldNames = new ArrayList<>();
        for (int i = 0; i < filterFields.size(); i++) {
            String field = filterFields.get(i);
            int inputFieldIndex = seaTunnelRowType.indexOf(field);
            if (inputFieldIndex == -1) {
                throw new IllegalArgumentException(
                        "Cannot find [" + field + "] field in input row type");
            }
            inputValueIndex[i] = inputFieldIndex;
            outputColumns.add(
                    inputCatalogTable.getTableSchema().getColumns().get(inputFieldIndex).copy());
            outputFieldNames.add(
                    inputCatalogTable.getTableSchema().getColumns().get(inputFieldIndex).getName());
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
        if (inputCatalogTable.getTableSchema().getPrimaryKey() != null
                && outputFieldNames.containsAll(
                        inputCatalogTable.getTableSchema().getPrimaryKey().getColumnNames())) {
            copiedPrimaryKey = inputCatalogTable.getTableSchema().getPrimaryKey().copy();
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
