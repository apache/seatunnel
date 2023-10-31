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

package org.apache.seatunnel.transform.fieldmapper;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportTransform;
import org.apache.seatunnel.transform.exception.FieldMapperTransformErrorCode;
import org.apache.seatunnel.transform.exception.FieldMapperTransformException;
import org.apache.seatunnel.transform.exception.TransformException;

import org.apache.commons.collections4.CollectionUtils;

import com.google.common.collect.Lists;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class FieldMapperTransform extends AbstractCatalogSupportTransform {
    public static String PLUGIN_NAME = "FieldMapper";
    private FieldMapperTransformConfig config;
    private List<Integer> needReaderColIndex;

    public FieldMapperTransform(
            @NonNull FieldMapperTransformConfig config, @NonNull CatalogTable catalogTable) {
        super(catalogTable);
        this.config = config;
        Map<String, String> fieldMapper = config.getFieldMapper();
        SeaTunnelRowType seaTunnelRowType = catalogTable.getTableSchema().toPhysicalRowDataType();
        List<String> notFoundField =
                fieldMapper.keySet().stream()
                        .filter(field -> seaTunnelRowType.indexOf(field) == -1)
                        .collect(Collectors.toList());
        if (!CollectionUtils.isEmpty(notFoundField)) {
            throw new TransformException(
                    FieldMapperTransformErrorCode.INPUT_FIELD_NOT_FOUND, notFoundField.toString());
        }
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        Map<String, String> fieldMapper = config.getFieldMapper();
        Object[] outputDataArray = new Object[fieldMapper.size()];
        for (int i = 0; i < outputDataArray.length; i++) {
            outputDataArray[i] = inputRow.getField(needReaderColIndex.get(i));
        }
        SeaTunnelRow outputRow = new SeaTunnelRow(outputDataArray);
        outputRow.setRowKind(inputRow.getRowKind());
        outputRow.setTableId(inputRow.getTableId());
        return outputRow;
    }

    @Override
    protected TableSchema transformTableSchema() {
        Map<String, String> fieldMapper = config.getFieldMapper();

        List<Column> inputColumns = inputCatalogTable.getTableSchema().getColumns();
        SeaTunnelRowType seaTunnelRowType =
                inputCatalogTable.getTableSchema().toPhysicalRowDataType();
        List<Column> outputColumns = new ArrayList<>(fieldMapper.size());
        needReaderColIndex = new ArrayList<>(fieldMapper.size());
        ArrayList<String> inputFieldNames = Lists.newArrayList(seaTunnelRowType.getFieldNames());
        ArrayList<String> outputFieldNames = new ArrayList<>();
        fieldMapper.forEach(
                (key, value) -> {
                    int fieldIndex = inputFieldNames.indexOf(key);
                    if (fieldIndex < 0) {
                        throw new FieldMapperTransformException(
                                FieldMapperTransformErrorCode.INPUT_FIELD_NOT_FOUND,
                                "Can not found field " + key + " from inputRowType");
                    }
                    Column oldColumn = inputColumns.get(fieldIndex);
                    PhysicalColumn outputColumn =
                            PhysicalColumn.of(
                                    value,
                                    oldColumn.getDataType(),
                                    oldColumn.getColumnLength(),
                                    oldColumn.isNullable(),
                                    oldColumn.getDefaultValue(),
                                    oldColumn.getComment());
                    outputColumns.add(outputColumn);
                    outputFieldNames.add(outputColumn.getName());
                    needReaderColIndex.add(fieldIndex);
                });

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
                .primaryKey(copiedPrimaryKey)
                .columns(outputColumns)
                .constraintKey(outputConstraintKeys)
                .build();
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        return inputCatalogTable.getTableId().copy();
    }
}
