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
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public abstract class SingleFieldOutputTransform extends AbstractCatalogSupportTransform {

    private int fieldIndex;
    private SeaTunnelRowContainerGenerator rowContainerGenerator;

    public SingleFieldOutputTransform(@NonNull CatalogTable inputCatalogTable) {
        super(inputCatalogTable);
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        Object fieldValue = getOutputFieldValue(new SeaTunnelRowAccessor(inputRow));

        SeaTunnelRow outputRow = rowContainerGenerator.apply(inputRow);
        outputRow.setField(fieldIndex, fieldValue);
        return outputRow;
    }

    /**
     * Outputs new field value
     *
     * @param inputRow The inputRow of upstream input.
     */
    protected abstract Object getOutputFieldValue(SeaTunnelRowAccessor inputRow);

    @Override
    protected TableSchema transformTableSchema() {
        Column outputColumn = getOutputColumn();
        List<ConstraintKey> copiedConstraintKeys =
                inputCatalogTable.getTableSchema().getConstraintKeys().stream()
                        .map(ConstraintKey::copy)
                        .collect(Collectors.toList());

        TableSchema.Builder builder = TableSchema.builder();
        if (inputCatalogTable.getTableSchema().getPrimaryKey() != null) {
            builder.primaryKey(inputCatalogTable.getTableSchema().getPrimaryKey().copy());
        }
        builder.constraintKey(copiedConstraintKeys);
        List<Column> columns =
                inputCatalogTable.getTableSchema().getColumns().stream()
                        .map(Column::copy)
                        .collect(Collectors.toList());

        int addFieldCount = 0;
        Optional<Column> optional =
                columns.stream()
                        .filter(c -> c.getName().equals(outputColumn.getName()))
                        .findFirst();
        if (optional.isPresent()) {
            Column originalColumn = optional.get();
            int originalColumnIndex = columns.indexOf(originalColumn);
            if (!originalColumn.getDataType().equals(outputColumn.getDataType())) {
                columns.set(originalColumnIndex, originalColumn.copy(outputColumn.getDataType()));
            }
            this.fieldIndex = originalColumnIndex;
        } else {
            addFieldCount++;
            columns.add(outputColumn);
            this.fieldIndex = columns.indexOf(outputColumn);
        }

        TableSchema outputTableSchema = builder.columns(columns).build();
        if (addFieldCount > 0) {
            this.fieldIndex = outputTableSchema.getColumns().size() - 1;
            int inputFieldLength =
                    inputCatalogTable.getTableSchema().toPhysicalRowDataType().getTotalFields();
            int outputFieldLength = outputTableSchema.getColumns().size();

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

        log.info(
                "Changed input table schema: {} to output table schema: {}",
                inputCatalogTable.getTableSchema(),
                outputTableSchema);

        return outputTableSchema;
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        return inputCatalogTable.getTableId().copy();
    }

    protected abstract Column getOutputColumn();

    public int getFieldIndex() {
        return fieldIndex;
    }

    public SeaTunnelRowContainerGenerator getRowContainerGenerator() {
        return rowContainerGenerator;
    }
}
