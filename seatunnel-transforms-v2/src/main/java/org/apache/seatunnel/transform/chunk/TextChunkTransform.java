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
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class TextChunkTransform extends AbstractCatalogSupportFlatMapTransform {

    public static final String PLUGIN_NAME = "TextChunk";

    private static final double OVERLAP_WARN_RATIO = 0.5;

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
        if (config.getOverlapSize() >= config.getChunkSize() * OVERLAP_WARN_RATIO) {
            log.warn(
                    "Configured overlap_size={} exceeds {}% of chunk_size={}, which may cause "
                            + "row-count and memory amplification. Consider a smaller overlap_size.",
                    config.getOverlapSize(),
                    (int) (OVERLAP_WARN_RATIO * 100),
                    config.getChunkSize());
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
            if (config.isSkipEmptyText()) {
                log.debug(
                        "Dropping row because text_field '{}' is null or empty (skip_empty_text=true)",
                        config.getTextField());
                return Collections.emptyList();
            }
            return Collections.singletonList(buildOutputRow(inputRow, null, 0));
        }
        List<String> chunks =
                TextChunker.split(
                        value.toString(),
                        config.getSeparators(),
                        config.getChunkSize(),
                        config.getOverlapSize());

        List<SeaTunnelRow> outputRows = new ArrayList<>(chunks.size());
        for (int i = 0; i < chunks.size(); i++) {
            outputRows.add(buildOutputRow(inputRow, chunks.get(i), i));
        }
        return outputRows;
    }

    private SeaTunnelRow buildOutputRow(SeaTunnelRow inputRow, String chunk, int chunkIndex) {
        Object[] fields = Arrays.copyOf(inputRow.getFields(), outputFieldCount);
        fields[chunkFieldIndex] = chunk;
        fields[chunkIndexFieldIndex] = chunkIndex;
        SeaTunnelRow outputRow = new SeaTunnelRow(fields);
        outputRow.setTableId(inputRow.getTableId());
        outputRow.setRowKind(inputRow.getRowKind());
        outputRow.setOptions(inputRow.getOptions());
        return outputRow;
    }

    @Override
    protected TableSchema transformTableSchema() {
        List<Column> columns =
                inputCatalogTable.getTableSchema().getColumns().stream()
                        .map(Column::copy)
                        .collect(Collectors.toList());

        this.chunkFieldIndex =
                addOrReplace(columns, config.getOutputField(), BasicType.STRING_TYPE, true);
        this.chunkIndexFieldIndex =
                addOrReplace(columns, config.getChunkIndexField(), BasicType.INT_TYPE, false);
        this.outputFieldCount = columns.size();

        TableSchema.Builder builder = TableSchema.builder().columns(columns);

        PrimaryKey primaryKey = inputCatalogTable.getTableSchema().getPrimaryKey();
        if (primaryKey != null) {
            builder.primaryKey(extendPrimaryKeyWithChunkIndex(primaryKey));
        }

        List<ConstraintKey> outputConstraintKeys =
                inputCatalogTable.getTableSchema().getConstraintKeys().stream()
                        .map(
                                key ->
                                        key.getConstraintType()
                                                        == ConstraintKey.ConstraintType.UNIQUE_KEY
                                                ? extendUniqueConstraintKeyWithChunkIndex(key)
                                                : key.copy())
                        .collect(Collectors.toList());
        builder.constraintKey(outputConstraintKeys);

        return builder.build();
    }

    private PrimaryKey extendPrimaryKeyWithChunkIndex(PrimaryKey primaryKey) {
        List<String> columnNames = new ArrayList<>(primaryKey.getColumnNames());
        if (!Boolean.TRUE.equals(primaryKey.getEnableAutoId())
                && !columnNames.contains(config.getChunkIndexField())) {
            columnNames.add(config.getChunkIndexField());
        }
        return PrimaryKey.of(primaryKey.getPrimaryKey(), columnNames, primaryKey.getEnableAutoId());
    }

    private ConstraintKey extendUniqueConstraintKeyWithChunkIndex(ConstraintKey uniqueKey) {
        List<ConstraintKey.ConstraintKeyColumn> keyColumns =
                uniqueKey.getColumnNames().stream()
                        .map(ConstraintKey.ConstraintKeyColumn::copy)
                        .collect(Collectors.toList());
        boolean present =
                keyColumns.stream()
                        .anyMatch(c -> c.getColumnName().equals(config.getChunkIndexField()));
        if (!present) {
            keyColumns.add(
                    ConstraintKey.ConstraintKeyColumn.of(
                            config.getChunkIndexField(), ConstraintKey.ColumnSortType.ASC));
        }
        return ConstraintKey.of(
                uniqueKey.getConstraintType(), uniqueKey.getConstraintName(), keyColumns);
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        return inputCatalogTable.getTableId().copy();
    }

    private static int addOrReplace(
            List<Column> columns, String name, SeaTunnelDataType<?> type, boolean nullable) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equals(name)) {
                columns.set(i, columns.get(i).copy(type));
                return i;
            }
        }
        columns.add(PhysicalColumn.of(name, type, (Long) null, nullable, null, ""));
        return columns.size() - 1;
    }
}
