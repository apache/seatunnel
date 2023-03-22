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
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.SeaTunnelException;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public abstract class MultipleFieldOutputTransform extends AbstractSeaTunnelTransform {
    private Column[] outputColumns;
    private int[] fieldsIndex;
    private SeaTunnelRowContainerGenerator rowContainerGenerator;

    public MultipleFieldOutputTransform(@NonNull CatalogTable catalogTable) {
        super(catalogTable);
    }

    @Override
    protected TableSchema transformTableSchema() {
        outputColumns = getOutputColumns();
        TableSchema.Builder builder =
            TableSchema.builder().primaryKey(inputCatalogTable.getTableSchema().getPrimaryKey())
                .constraintKey(inputCatalogTable.getTableSchema().getConstraintKeys());
        List<Column> copyInputColumns =
            inputCatalogTable.getTableSchema().getColumns().stream().map(Column::copy).collect(Collectors.toList());

        int addFieldCount = 0;
        this.fieldsIndex = new int[outputColumns.length];
        for (int i = 0 ; i < outputColumns.length ; i++) {
            for (int j  = 0; j < copyInputColumns.size() ; j++) {
                if (copyInputColumns.get(j).getName().equals(outputColumns[i].getName())) {
                    copyInputColumns.set(j, outputColumns[i]);
                } else {
                    addFieldCount ++;
                    copyInputColumns.add(outputColumns[i]);
                }
            }
        }

        TableSchema outputTableSchema = builder.columns(copyInputColumns).build();
        if (addFieldCount > 0) {
            int inputFieldLength = inputCatalogTable.getTableSchema().toPhysicalRowDataType().getTotalFields();
            int outputFieldLength = copyInputColumns.size();

            rowContainerGenerator =
                new SeaTunnelRowContainerGenerator() {
                    @Override
                    public SeaTunnelRow apply(SeaTunnelRow inputRow) {
                        // todo reuse array container
                        Object[] outputFieldValues = new Object[outputFieldLength];
                        System.arraycopy(
                            inputRow.getFields(),
                            0,
                            outputFieldValues,
                            0,
                            inputFieldLength);

                        SeaTunnelRow outputRow = new SeaTunnelRow(outputFieldValues);
                        outputRow.setTableId(inputRow.getTableId());
                        outputRow.setRowKind(inputRow.getRowKind());
                        return outputRow;
                    }
                };
        } else {
            rowContainerGenerator = SeaTunnelRowContainerGenerator.REUSE_ROW;
        }

        log.info("Changed input table schema: {} to output table schema: {}", inputCatalogTable.getTableSchema(), outputTableSchema);

        return outputTableSchema;
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        return inputCatalogTable.getTableId();
    }

    @Override
    protected SeaTunnelRowType transformRowType(SeaTunnelRowType inputRowType) {
        outputColumns = getOutputColumns();
        setInputRowType(inputRowType);

        this.outputFieldNames = getOutputFieldNames();
        this.fieldsIndex = new int[outputFieldNames.length];
        if (outputFieldNames.length != new HashSet<>(Arrays.asList(outputFieldNames)).size()) {
            throw new IllegalStateException(
                "Duplicate field names are not allowed. field names: " + outputFieldNames);
        }

        SeaTunnelDataType[] outputFieldDataTypes = getOutputFieldDataTypes();
        if (outputFieldNames.length != outputFieldDataTypes.length) {
            throw new IllegalStateException(
                "Field name and field type count mismatch, field names: "
                    + outputFieldNames
                    + ", field types: "
                    + outputFieldDataTypes);
        }

        List<String> fieldNames = new ArrayList<>(Arrays.asList(inputRowType.getFieldNames()));
        List<SeaTunnelDataType> fieldDataTypes =
            new ArrayList<>(Arrays.asList(inputRowType.getFieldTypes()));

        int addFieldCount = 0;
        for (int i = 0; i < outputFieldNames.length; i++) {
            String outputFieldName = outputFieldNames[i];
            SeaTunnelDataType outputFieldDataType = outputFieldDataTypes[i];

            int index = fieldNames.indexOf(outputFieldName);
            if (index != -1) {
                if (!outputFieldDataType.equals(fieldDataTypes.get(index))) {
                    fieldDataTypes.set(index, outputFieldDataType);
                }
                fieldsIndex[i] = index;
            } else {
                addFieldCount++;

                fieldNames.add(outputFieldName);
                fieldDataTypes.add(outputFieldDataType);
                fieldsIndex[i] = fieldNames.indexOf(outputFieldName);
            }
        }

        if (addFieldCount > 0) {
            int inputFieldLength = inputRowType.getTotalFields();
            int outputFieldLength = fieldNames.size();

            rowContainerGenerator =
                new SeaTunnelRowContainerGenerator() {
                    @Override
                    public SeaTunnelRow apply(SeaTunnelRow inputRow) {
                        // todo reuse array container
                        Object[] outputFieldValues = new Object[outputFieldLength];
                        System.arraycopy(
                            inputRow.getFields(),
                            0,
                            outputFieldValues,
                            0,
                            inputFieldLength);

                        SeaTunnelRow outputRow = new SeaTunnelRow(outputFieldValues);
                        outputRow.setTableId(inputRow.getTableId());
                        outputRow.setRowKind(inputRow.getRowKind());
                        return outputRow;
                    }
                };
        } else {
            rowContainerGenerator = SeaTunnelRowContainerGenerator.REUSE_ROW;
        }

        SeaTunnelRowType outputRowType =
            new SeaTunnelRowType(
                fieldNames.toArray(TYPE_ARRAY_STRING),
                fieldDataTypes.toArray(TYPE_ARRAY_SEATUNNEL_DATA_TYPE));
        log.info("Changed input row type: {} to output row type: {}", inputRowType, outputRowType);

        return outputRowType;
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        Object[] fieldValues = getOutputFieldValues(new SeaTunnelRowAccessor(inputRow));
        SeaTunnelRow outputRow = rowContainerGenerator.apply(inputRow);
        for (int i = 0; i < outputColumns.length; i++) {
            outputRow.setField(fieldsIndex[i], fieldValues == null ? null : fieldValues[i]);
        }
        return outputRow;
    }

    /**
     * Outputs new fields value
     *
     * @param inputRow The inputRow of upstream input.
     * @return
     */
    protected abstract Object[] getOutputFieldValues(SeaTunnelRowAccessor inputRow);

    protected abstract Column[] getOutputColumns();

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        if (outputRowType != null) {
            return outputRowType;
        }
        return getProducedCatalogTable().getTableSchema().toPhysicalRowDataType();
    }
}
