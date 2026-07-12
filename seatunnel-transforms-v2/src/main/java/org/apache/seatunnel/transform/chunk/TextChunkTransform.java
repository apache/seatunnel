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

package org.apache.seatunnel.transform.chunk;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportFlatMapTransform;
import org.apache.seatunnel.transform.exception.TransformCommonError;

import lombok.NonNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class TextChunkTransform extends AbstractCatalogSupportFlatMapTransform {

    public static final String PLUGIN_NAME = "TextChunk";

    private final TextChunkTransformConfig config;
    private final int textFieldIndex;

    private int outputFieldCount;
    private int chunkFieldIndex;
    private int chunkIndexFieldIndex;

    public TextChunkTransform(
            @NonNull TextChunkTransformConfig config, @NonNull CatalogTable inputCatalogTable) {
        super(inputCatalogTable);
        this.config = config;
        SeaTunnelRowType rowType = inputCatalogTable.getTableSchema().toPhysicalRowDataType();
        try {
            this.textFieldIndex = rowType.indexOf(config.getTextField());
        } catch (IllegalArgumentException e) {
            throw TransformCommonError.cannotFindInputFieldError(
                    getPluginName(), config.getTextField());
        }
        this.outputCatalogTable = getProducedCatalogTable();
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    protected List<SeaTunnelRow> transformRow(SeaTunnelRow inputRow) {
        Object value = inputRow.getField(textFieldIndex);
        if (value == null || value.toString().isEmpty()) {
            return Collections.emptyList();
        }
        List<String> chunks =
                TextChunker.split(
                        value.toString(),
                        config.getSeparators(),
                        config.getChunkSize(),
                        config.getOverlapSize());

        List<SeaTunnelRow> outputRows = new ArrayList<>(chunks.size());
        for (int i = 0; i < chunks.size(); i++) {
            Object[] fields = Arrays.copyOf(inputRow.getFields(), outputFieldCount);
            fields[chunkFieldIndex] = chunks.get(i);
            fields[chunkIndexFieldIndex] = i;
            SeaTunnelRow outputRow = new SeaTunnelRow(fields);
            outputRow.setTableId(inputRow.getTableId());
            outputRow.setRowKind(inputRow.getRowKind());
            outputRow.setOptions(inputRow.getOptions());
            outputRows.add(outputRow);
        }
        return outputRows;
    }

    @Override
    protected TableSchema transformTableSchema() {
        List<Column> columns =
                inputCatalogTable.getTableSchema().getColumns().stream()
                        .map(Column::copy)
                        .collect(Collectors.toList());

        this.chunkFieldIndex =
                addOrReplace(columns, config.getOutputField(), BasicType.STRING_TYPE);
        this.chunkIndexFieldIndex =
                addOrReplace(columns, config.getChunkIndexField(), BasicType.INT_TYPE);
        this.outputFieldCount = columns.size();

        List<ConstraintKey> outputConstraintKeys =
                inputCatalogTable.getTableSchema().getConstraintKeys().stream()
                        .map(ConstraintKey::copy)
                        .collect(Collectors.toList());

        TableSchema.Builder builder = TableSchema.builder().columns(columns);

        PrimaryKey primaryKey = inputCatalogTable.getTableSchema().getPrimaryKey();
        if (primaryKey != null) {
            builder.primaryKey(primaryKey.copy());
        }
        builder.constraintKey(outputConstraintKeys);

        return builder.build();
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        return inputCatalogTable.getTableId().copy();
    }

    private static int addOrReplace(List<Column> columns, String name, SeaTunnelDataType<?> type) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equals(name)) {
                columns.set(i, columns.get(i).copy(type));
                return i;
            }
        }
        columns.add(PhysicalColumn.of(name, type, (Long) null, true, null, ""));
        return columns.size() - 1;
    }
}
