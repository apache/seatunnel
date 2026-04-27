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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.schema.event.AlterTableEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Abstract class for multi-table transform. It is used to split the input data into multiple table
 * transforms.
 */
public abstract class AbstractMultiCatalogTransform implements SeaTunnelTransform<SeaTunnelRow> {

    protected List<CatalogTable> inputCatalogTables;

    protected List<CatalogTable> outputCatalogTables;

    protected Map<String, SeaTunnelTransform<SeaTunnelRow>> transformMap;

    public AbstractMultiCatalogTransform(
            List<CatalogTable> inputCatalogTables, ReadonlyConfig config) {
        this.inputCatalogTables = inputCatalogTables;
        this.transformMap = new HashMap<>();
        Pattern tableMatchRegex =
                Pattern.compile(config.get(TransformCommonOptions.TABLE_MATCH_REGEX));
        Map<String, ReadonlyConfig> singleTableConfig =
                config.get(TransformCommonOptions.MULTI_TABLES).stream()
                        .map(ReadonlyConfig::fromMap)
                        .filter(c -> c.get(TransformCommonOptions.TABLE_PATH) != null)
                        .collect(
                                Collectors.toMap(
                                        c -> c.get(TransformCommonOptions.TABLE_PATH),
                                        Function.identity()));

        inputCatalogTables.forEach(
                inputCatalogTable -> {
                    String tableId = inputCatalogTable.getTableId().toTablePath().toString();
                    ReadonlyConfig tableConfig;
                    if (singleTableConfig.containsKey(tableId)) {
                        tableConfig = singleTableConfig.get(tableId);
                    } else if (tableMatchRegex.matcher(tableId).matches()) {
                        tableConfig = config;
                    } else {
                        tableConfig = null;
                    }
                    if (tableConfig != null) {
                        transformMap.put(tableId, buildTransform(inputCatalogTable, tableConfig));
                    } else {
                        transformMap.put(tableId, createIdentityTransform(inputCatalogTable));
                    }
                });

        this.outputCatalogTables =
                inputCatalogTables.stream()
                        .map(
                                inputCatalogTable -> {
                                    String tableName =
                                            inputCatalogTable.getTableId().toTablePath().toString();
                                    return transformMap.get(tableName).getProducedCatalogTable();
                                })
                        .collect(Collectors.toList());
    }

    protected abstract SeaTunnelTransform<SeaTunnelRow> buildTransform(
            CatalogTable inputCatalogTable, ReadonlyConfig config);

    protected abstract SeaTunnelTransform<SeaTunnelRow> createIdentityTransform(
            CatalogTable catalogTable);

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return outputCatalogTables;
    }

    @Override
    public CatalogTable getProducedCatalogTable() {
        return outputCatalogTables.get(0);
    }

    /**
     * Dispatches the schema-change event to the inner per-table transform that owns the affected
     * table, then refreshes {@link #outputCatalogTables} so downstream sees the new schema.
     *
     * <p>Without this override, the wrapper would inherit the {@link SeaTunnelTransform} no-op
     * default — the inner transforms in {@link #transformMap} would never see ALTER, their cached
     * {@code inputCatalogTable} would stay at the original schema, and post-ALTER rows would have
     * their new column values silently truncated by the inner transform's row container.
     */
    @Override
    public SchemaChangeEvent mapSchemaChangeEvent(SchemaChangeEvent event) {
        String targetTableId = event.tablePath().toString();
        SeaTunnelTransform<SeaTunnelRow> targetTransform = transformMap.get(targetTableId);
        if (targetTransform != null) {
            targetTransform.mapSchemaChangeEvent(event);
            refreshOutputCatalogTables();
            // Propagate this transform's actual produced catalog through the chain via the event's
            // changeAfter field (existing API designed for exactly this). Downstream transforms
            // and the sink read changeAfter to adopt upstream's actual row layout instead of
            // re-applying ALTER locally — which would diverge from the actual row order.
            if (event instanceof AlterTableEvent) {
                outputCatalogTables.stream()
                        .filter(t -> t.getTableId().toTablePath().toString().equals(targetTableId))
                        .findFirst()
                        .ifPresent(produced -> ((AlterTableEvent) event).setChangeAfter(produced));
            }
        }
        return event;
    }

    /**
     * Re-derives this wrapper (and its inner per-table transforms) from upstream's post-event
     * produced schema. Called by the engine after an upstream transform has mapped a schema-change
     * event. This ensures inner transforms see the same column order their actual data rows carry,
     * instead of applying ALTER events to a stale local view.
     */
    @Override
    public void setInputCatalogTables(List<CatalogTable> newInputCatalogTables) {
        if (newInputCatalogTables == null || newInputCatalogTables.isEmpty()) {
            return;
        }
        this.inputCatalogTables = newInputCatalogTables;
        for (CatalogTable newInput : newInputCatalogTables) {
            String tableId = newInput.getTableId().toTablePath().toString();
            SeaTunnelTransform<SeaTunnelRow> inner = transformMap.get(tableId);
            if (inner instanceof AbstractSeaTunnelTransform) {
                ((AbstractSeaTunnelTransform<?, ?>) inner).setInputCatalogTable(newInput);
            }
        }
        refreshOutputCatalogTables();
    }

    private void refreshOutputCatalogTables() {
        this.outputCatalogTables =
                inputCatalogTables.stream()
                        .map(
                                inputCatalogTable -> {
                                    String tableName =
                                            inputCatalogTable.getTableId().toTablePath().toString();
                                    SeaTunnelTransform<SeaTunnelRow> inner =
                                            transformMap.get(tableName);
                                    return inner != null
                                            ? inner.getProducedCatalogTable()
                                            : inputCatalogTable;
                                })
                        .collect(Collectors.toList());
    }

    @Override
    public void setTypeInfo(SeaTunnelDataType<SeaTunnelRow> inputDataType) {}
}
