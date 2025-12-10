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

package org.apache.seatunnel.transform.pivot;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.transform.SeaTunnelBatchTransform;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Pivot Transform that converts multiple rows into columns.
 *
 * <p>This transform implements {@link SeaTunnelBatchTransform} to support stateful batch processing
 * with checkpoint capability.
 *
 * <p>Key features:
 *
 * <ul>
 *   <li>Groups rows by specified key columns
 *   <li>Pivots a column's values into new columns
 *   <li>Supports checkpoint for fault tolerance
 *   <li>Configurable buffer size for memory management
 * </ul>
 */
@Slf4j
public class PivotTransform implements SeaTunnelBatchTransform<SeaTunnelRow, PivotGroupState> {

    private static final long serialVersionUID = 1L;

    private final CatalogTable inputCatalogTable;
    private final ReadonlyConfig config;

    // Configuration
    private final List<String> groupByKeys;
    private final String pivotColumn;
    private final String valueColumn;
    private final List<String> pivotValues;
    private final int maxBufferSize;
    private final long groupTimeoutMs;

    // Column indices
    private int[] groupByIndices;
    private int pivotColumnIndex;
    private int valueColumnIndex;

    // State: Map from groupKey to PivotGroupState
    private final Map<String, PivotGroupState> groupBuffer;

    // Output schema
    private CatalogTable outputCatalogTable;
    private SeaTunnelRowType outputRowType;

    // Serializer
    private final PivotStateSerializer stateSerializer;

    public PivotTransform(CatalogTable inputCatalogTable, ReadonlyConfig config) {
        this.inputCatalogTable = inputCatalogTable;
        this.config = config;

        // Parse configuration
        this.groupByKeys = config.get(PivotTransformConfig.GROUP_BY_KEYS);
        this.pivotColumn = config.get(PivotTransformConfig.PIVOT_COLUMN);
        this.valueColumn = config.get(PivotTransformConfig.VALUE_COLUMN);
        this.pivotValues = config.get(PivotTransformConfig.PIVOT_VALUES);
        this.maxBufferSize = config.get(PivotTransformConfig.MAX_BUFFER_SIZE);
        this.groupTimeoutMs = config.get(PivotTransformConfig.GROUP_TIMEOUT_MS);

        // Initialize buffer
        this.groupBuffer = new LinkedHashMap<>();
        this.stateSerializer = new PivotStateSerializer();

        // Validate configuration
        validateConfig();
    }

    private void validateConfig() {
        if (groupByKeys == null || groupByKeys.isEmpty()) {
            throw new IllegalArgumentException("group_by_keys must be specified");
        }
        if (pivotColumn == null || pivotColumn.isEmpty()) {
            throw new IllegalArgumentException("pivot_column must be specified");
        }
        if (valueColumn == null || valueColumn.isEmpty()) {
            throw new IllegalArgumentException("value_column must be specified");
        }
        if (pivotValues == null || pivotValues.isEmpty()) {
            throw new IllegalArgumentException(
                    "pivot_values must be specified to define the output columns");
        }
    }

    @Override
    public void open() {
        // Initialize column indices
        SeaTunnelRowType inputRowType = inputCatalogTable.getSeaTunnelRowType();
        String[] fieldNames = inputRowType.getFieldNames();

        this.groupByIndices = new int[groupByKeys.size()];
        for (int i = 0; i < groupByKeys.size(); i++) {
            int index = findFieldIndex(fieldNames, groupByKeys.get(i));
            if (index < 0) {
                throw new IllegalArgumentException(
                        "Group by column not found: " + groupByKeys.get(i));
            }
            groupByIndices[i] = index;
        }

        this.pivotColumnIndex = findFieldIndex(fieldNames, pivotColumn);
        if (pivotColumnIndex < 0) {
            throw new IllegalArgumentException("Pivot column not found: " + pivotColumn);
        }

        this.valueColumnIndex = findFieldIndex(fieldNames, valueColumn);
        if (valueColumnIndex < 0) {
            throw new IllegalArgumentException("Value column not found: " + valueColumn);
        }

        // Build output schema
        buildOutputSchema();

        log.info(
                "PivotTransform opened: groupByKeys={}, pivotColumn={}, valueColumn={}, pivotValues={}",
                groupByKeys,
                pivotColumn,
                valueColumn,
                pivotValues);
    }

    private int findFieldIndex(String[] fieldNames, String fieldName) {
        for (int i = 0; i < fieldNames.length; i++) {
            if (fieldNames[i].equals(fieldName)) {
                return i;
            }
        }
        return -1;
    }

    private void buildOutputSchema() {
        SeaTunnelRowType inputRowType = inputCatalogTable.getSeaTunnelRowType();
        SeaTunnelDataType<?> valueType = inputRowType.getFieldType(valueColumnIndex);

        // Output columns: group_by columns + pivot value columns
        List<Column> outputColumns = new ArrayList<>();

        // Add group by columns
        for (int i = 0; i < groupByKeys.size(); i++) {
            String colName = groupByKeys.get(i);
            int sourceIndex = groupByIndices[i];
            SeaTunnelDataType<?> colType = inputRowType.getFieldType(sourceIndex);
            outputColumns.add(PhysicalColumn.of(colName, colType, 0, true, null, null));
        }

        // Add pivot value columns
        for (String pivotValue : pivotValues) {
            outputColumns.add(PhysicalColumn.of(pivotValue, valueType, 0, true, null, null));
        }

        // Build output row type
        String[] outputFieldNames =
                outputColumns.stream().map(Column::getName).toArray(String[]::new);
        SeaTunnelDataType<?>[] outputFieldTypes =
                outputColumns.stream().map(Column::getDataType).toArray(SeaTunnelDataType[]::new);
        this.outputRowType = new SeaTunnelRowType(outputFieldNames, outputFieldTypes);

        // Build output catalog table
        TableSchema outputSchema = TableSchema.builder().columns(outputColumns).build();

        TableIdentifier outputTableId =
                TableIdentifier.of(
                        inputCatalogTable.getTableId().getCatalogName(),
                        inputCatalogTable.getTableId().getDatabaseName(),
                        inputCatalogTable.getTableId().getSchemaName(),
                        inputCatalogTable.getTableId().getTableName());

        this.outputCatalogTable =
                CatalogTable.of(
                        outputTableId,
                        outputSchema,
                        inputCatalogTable.getOptions(),
                        inputCatalogTable.getPartitionKeys(),
                        inputCatalogTable.getComment());
    }

    @Override
    public void collect(SeaTunnelRow row) {
        // Extract group key
        String groupKey = buildGroupKey(row);

        // Get or create group state
        PivotGroupState state =
                groupBuffer.computeIfAbsent(
                        groupKey,
                        k -> {
                            Object[] groupByValues = new Object[groupByIndices.length];
                            for (int i = 0; i < groupByIndices.length; i++) {
                                groupByValues[i] = row.getField(groupByIndices[i]);
                            }
                            return new PivotGroupState(
                                    groupKey, new HashMap<>(), groupByValues, row.getTableId());
                        });

        // Extract pivot key and value
        Object pivotKey = row.getField(pivotColumnIndex);
        Object value = row.getField(valueColumnIndex);

        if (pivotKey != null) {
            String pivotKeyStr = pivotKey.toString();
            // Only store if it's a configured pivot value
            if (pivotValues.contains(pivotKeyStr)) {
                state.getPivotedValues().put(pivotKeyStr, value);
            }
        }

        state.touch();

        // Check if buffer is full
        if (maxBufferSize > 0 && groupBuffer.size() > maxBufferSize) {
            log.debug(
                    "Buffer size {} exceeded max {}, flushing...",
                    groupBuffer.size(),
                    maxBufferSize);
            // Flush will be triggered by the engine during checkpoint
        }
    }

    private String buildGroupKey(SeaTunnelRow row) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < groupByIndices.length; i++) {
            if (i > 0) {
                sb.append("_");
            }
            Object value = row.getField(groupByIndices[i]);
            sb.append(value == null ? "null" : value.toString());
        }
        return sb.toString();
    }

    @Override
    public List<SeaTunnelRow> flush() {
        if (groupBuffer.isEmpty()) {
            return Collections.emptyList();
        }

        List<SeaTunnelRow> outputRows = new ArrayList<>();

        for (PivotGroupState state : groupBuffer.values()) {
            SeaTunnelRow outputRow = buildOutputRow(state);
            outputRows.add(outputRow);
        }

        log.info("Flushed {} groups, produced {} rows", groupBuffer.size(), outputRows.size());

        // Clear the buffer after flush
        groupBuffer.clear();

        return outputRows;
    }

    private SeaTunnelRow buildOutputRow(PivotGroupState state) {
        int totalColumns = groupByKeys.size() + pivotValues.size();
        Object[] fields = new Object[totalColumns];

        // Copy group by values
        for (int i = 0; i < state.getGroupByValues().length; i++) {
            fields[i] = state.getGroupByValues()[i];
        }

        // Copy pivot values
        for (int i = 0; i < pivotValues.size(); i++) {
            String pivotValue = pivotValues.get(i);
            fields[groupByKeys.size() + i] = state.getPivotedValues().get(pivotValue);
        }

        SeaTunnelRow outputRow = new SeaTunnelRow(fields);
        outputRow.setTableId(state.getTableId());
        outputRow.setRowKind(org.apache.seatunnel.api.table.type.RowKind.INSERT);

        return outputRow;
    }

    @Override
    public List<PivotGroupState> snapshotState(long checkpointId) throws Exception {
        log.info(
                "Snapshot state for checkpoint {}, buffer size: {}",
                checkpointId,
                groupBuffer.size());
        return new ArrayList<>(groupBuffer.values());
    }

    @Override
    public void restoreState(List<PivotGroupState> states) throws Exception {
        log.info("Restoring {} states", states.size());
        groupBuffer.clear();
        for (PivotGroupState state : states) {
            groupBuffer.put(state.getGroupKey(), state);
        }
    }

    @Override
    public Optional<Serializer<PivotGroupState>> getStateSerializer() {
        return Optional.of(stateSerializer);
    }

    @Override
    public boolean hasBufferedData() {
        return !groupBuffer.isEmpty();
    }

    @Override
    public int getBufferSize() {
        return groupBuffer.size();
    }

    @Override
    public CatalogTable getProducedCatalogTable() {
        if (outputCatalogTable == null) {
            // Lazy initialization
            open();
        }
        return outputCatalogTable;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(getProducedCatalogTable());
    }

    @Override
    public String getPluginName() {
        return PivotTransformConfig.PLUGIN_NAME;
    }

    @Override
    public void close() {
        groupBuffer.clear();
        log.info("PivotTransform closed");
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        log.debug("Checkpoint {} completed", checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        log.warn("Checkpoint {} aborted", checkpointId);
    }
}
