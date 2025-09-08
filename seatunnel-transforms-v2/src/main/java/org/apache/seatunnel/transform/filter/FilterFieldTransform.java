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
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportMapTransform;
import org.apache.seatunnel.transform.exception.TransformCommonError;

import org.apache.commons.collections4.CollectionUtils;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class FilterFieldTransform extends AbstractCatalogSupportMapTransform {
    public static final String PLUGIN_NAME = "Filter";

    private int[] inputValueIndexList;

    private final List<String> includeFields;
    private final List<String> excludeFields;

    public FilterFieldTransform(
            @NonNull ReadonlyConfig config, @NonNull CatalogTable catalogTable) {
        super(catalogTable);
        SeaTunnelRowType seaTunnelRowType = catalogTable.getTableSchema().toPhysicalRowDataType();
        includeFields = config.get(FilterFieldTransformConfig.INCLUDE_FIELDS);
        excludeFields = config.get(FilterFieldTransformConfig.EXCLUDE_FIELDS);
        // exactly only one should be set
        ConfigValidator.of(config)
                .validate(
                        OptionRule.builder()
                                .exclusive(
                                        FilterFieldTransformConfig.INCLUDE_FIELDS,
                                        FilterFieldTransformConfig.EXCLUDE_FIELDS)
                                .build());
        List<String> canNotFoundFields =
                Stream.concat(
                                Optional.ofNullable(includeFields).orElse(new ArrayList<>())
                                        .stream(),
                                Optional.ofNullable(excludeFields).orElse(new ArrayList<>())
                                        .stream())
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
        return inputRow.copy(inputValueIndexList);
    }

    @Override
    protected TableSchema transformTableSchema() {
        List<Column> outputColumns = new ArrayList<>();

        SeaTunnelRowType seaTunnelRowType =
                inputCatalogTable.getTableSchema().toPhysicalRowDataType();

        ArrayList<String> outputFieldNames = new ArrayList<>();
        List<Column> inputColumns = inputCatalogTable.getTableSchema().getColumns();
        // include
        if (Objects.nonNull(includeFields)) {
            inputValueIndexList = new int[includeFields.size()];
            for (int i = 0; i < includeFields.size(); i++) {
                String fieldName = includeFields.get(i);
                int inputFieldIndex = seaTunnelRowType.indexOf(fieldName);
                inputValueIndexList[i] = inputFieldIndex;
                outputColumns.add(inputColumns.get(inputFieldIndex).copy());
                outputFieldNames.add(inputColumns.get(inputFieldIndex).getName());
            }
        }

        // exclude
        if (Objects.nonNull(excludeFields)) {
            inputValueIndexList = new int[inputColumns.size() - excludeFields.size()];
            int index = 0;
            for (int i = 0; i < inputColumns.size(); i++) {
                // if the field is not in the fields, then add it to the outputColumns
                if (!excludeFields.contains(inputColumns.get(i).getName())) {
                    String fieldName = inputColumns.get(i).getName();
                    int inputFieldIndex = seaTunnelRowType.indexOf(fieldName);
                    inputValueIndexList[index++] = inputFieldIndex;
                    outputColumns.add(inputColumns.get(i).copy());
                    outputFieldNames.add(inputColumns.get(i).getName());
                }
            }
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
