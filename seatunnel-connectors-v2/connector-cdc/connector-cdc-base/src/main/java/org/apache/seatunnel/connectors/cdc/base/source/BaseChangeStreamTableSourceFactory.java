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

package org.apache.seatunnel.connectors.cdc.base.source;

import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.ChangeStreamTableSourceFactory;
import org.apache.seatunnel.api.table.factory.ChangeStreamTableSourceState;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.cdc.base.schema.SchemaChangeEventFilter;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;

import io.debezium.relational.TableId;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * CDC Base class for {@link ChangeStreamTableSourceFactory}. It provides a default implementation
 * for {@link ChangeStreamTableSourceFactory#restoreSource(TableSourceFactoryContext,
 * ChangeStreamTableSourceState)}. The default implementation will restore the source using the
 * checkpoint tables in the {@link ChangeStreamTableSourceState}.
 */
@Slf4j
public abstract class BaseChangeStreamTableSourceFactory implements ChangeStreamTableSourceFactory {
    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        SchemaChangeEventFilter.validateOptions(context.getOptions());
        return restoreSource(context, Collections.emptyList());
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> restoreSource(
                    TableSourceFactoryContext context,
                    ChangeStreamTableSourceState<StateT, SplitT> state) {
        SchemaChangeEventFilter.validateOptions(context.getOptions());
        return restoreSource(context, getRestoreTableStruct(state));
    }

    public abstract <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> restoreSource(
                    TableSourceFactoryContext context, List<CatalogTable> restoreTableStruct);

    protected <SplitT extends SourceSplit, StateT extends Serializable>
            List<CatalogTable> getRestoreTableStruct(
                    ChangeStreamTableSourceState<StateT, SplitT> state) {
        if (state == null || state.getSplits() == null) {
            log.info("Restore source using checkpoint tables is empty");
            return Collections.emptyList();
        }
        List<IncrementalSplit> incrementalSplits =
                state.getSplits().stream()
                        .filter(Objects::nonNull)
                        .flatMap(List::stream)
                        .filter(Objects::nonNull)
                        .map(SourceSplitBase.class::cast)
                        .filter(SourceSplitBase::isIncrementalSplit)
                        .map(SourceSplitBase::asIncrementalSplit)
                        .collect(Collectors.toList());
        if (incrementalSplits.isEmpty()) {
            log.info("Restore source using checkpoint tables is empty");
            return Collections.emptyList();
        }
        if (incrementalSplits.size() == 1) {
            List<CatalogTable> singleSplitTables =
                    extractCheckpointTables(incrementalSplits.get(0));
            if (!singleSplitTables.isEmpty()) {
                log.info("Restore source using checkpoint tables: {}", singleSplitTables);
                return singleSplitTables;
            }
            log.info("Restore source using checkpoint tables is empty");
            return Collections.emptyList();
        }

        // When incremental.parallelism > 1, each parallel reader checkpoints an IncrementalSplit
        // carrying its view of checkpointTables. Merge them by TablePath, preferring the table
        // schema from the split that actually owns the table.
        Map<TablePath, CatalogTable> mergedTables = new LinkedHashMap<>();
        Set<TablePath> ownedTablePaths = new HashSet<>();
        for (IncrementalSplit incrementalSplit : incrementalSplits) {
            List<CatalogTable> splitTables = extractCheckpointTables(incrementalSplit);
            if (splitTables.isEmpty()) {
                continue;
            }
            Set<TableId> splitOwnedTables =
                    incrementalSplit.getTableIds() == null
                            ? Collections.emptySet()
                            : new HashSet<>(incrementalSplit.getTableIds());
            for (CatalogTable table : splitTables) {
                TablePath tablePath = table.getTablePath();
                TableId tableId =
                        new TableId(
                                table.getTableId().getDatabaseName(),
                                table.getTableId().getSchemaName(),
                                table.getTableId().getTableName());
                if (splitOwnedTables.contains(tableId)) {
                    mergedTables.put(tablePath, table);
                    ownedTablePaths.add(tablePath);
                } else if (!ownedTablePaths.contains(tablePath)) {
                    mergedTables.putIfAbsent(tablePath, table);
                }
            }
        }
        if (!mergedTables.isEmpty()) {
            List<CatalogTable> checkpointTableStruct = new ArrayList<>(mergedTables.values());
            log.info("Restore source using checkpoint tables: {}", checkpointTableStruct);
            return checkpointTableStruct;
        }

        log.info("Restore source using checkpoint tables is empty");
        return Collections.emptyList();
    }

    private List<CatalogTable> extractCheckpointTables(IncrementalSplit incrementalSplit) {
        if (incrementalSplit.getCheckpointTables() != null
                && !incrementalSplit.getCheckpointTables().isEmpty()) {
            return incrementalSplit.getCheckpointTables();
        }
        if (incrementalSplit.getCheckpointDataType() != null) {
            // TODO: Waiting for remove of compatible logic
            return CatalogTableUtil.convertDataTypeToCatalogTables(
                    incrementalSplit.getCheckpointDataType(), "default.default");
        }
        return Collections.emptyList();
    }

    protected List<CatalogTable> mergeTableStruct(
            List<CatalogTable> dbTableStruct, List<CatalogTable> restoreTableStruct) {
        if (!restoreTableStruct.isEmpty()) {
            Map<TablePath, CatalogTable> restoreTableMap =
                    restoreTableStruct.stream()
                            .collect(Collectors.toMap(CatalogTable::getTablePath, t -> t));

            List<CatalogTable> mergedTableStruct =
                    dbTableStruct.stream()
                            .map(e -> restoreTableMap.getOrDefault(e.getTablePath(), e))
                            .collect(Collectors.toList());
            log.info("Merge db table struct with checkpoint table struct: {}", mergedTableStruct);
            return mergedTableStruct;
        }
        return dbTableStruct;
    }
}
