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
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;

import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public abstract class BaseChangeStreamTableSourceFactory implements ChangeStreamTableSourceFactory {
    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return restoreSource(context, Collections.emptyList());
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> restoreSource(
                    TableSourceFactoryContext context,
                    ChangeStreamTableSourceState<StateT, SplitT> state) {
        return restoreSource(context, getRestoreTableStruct(state));
    }

    public abstract <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> restoreSource(
                    TableSourceFactoryContext context, List<CatalogTable> restoreTableStruct);

    protected <SplitT extends SourceSplit, StateT extends Serializable>
            List<CatalogTable> getRestoreTableStruct(
                    ChangeStreamTableSourceState<StateT, SplitT> state) {
        List<IncrementalSplit> incrementalSplits =
                state.getSplits().stream()
                        .flatMap(List::stream)
                        .filter(e -> e != null)
                        .map(e -> SourceSplitBase.class.cast(e))
                        .filter(e -> e.isIncrementalSplit())
                        .map(e -> e.asIncrementalSplit())
                        .collect(Collectors.toList());
        if (incrementalSplits.size() > 1) {
            throw new UnsupportedOperationException(
                    "Multiple incremental splits are not supported");
        }

        if (incrementalSplits.size() == 1) {
            IncrementalSplit incrementalSplit = incrementalSplits.get(0);
            if (incrementalSplit.getCheckpointTables() != null) {
                List<CatalogTable> checkpointTableStruct = incrementalSplit.getCheckpointTables();
                log.info("Restore source using checkpoint tables: {}", checkpointTableStruct);
                return checkpointTableStruct;
            }
            if (incrementalSplit.getCheckpointDataType() != null) {
                // TODO: Waiting for remove of compatible logic
                List<CatalogTable> checkpointDataTypeStruct =
                        CatalogTableUtil.convertDataTypeToCatalogTables(
                                incrementalSplit.getCheckpointDataType(), "default.default");
                log.info("Restore source using checkpoint tables: {}", checkpointDataTypeStruct);
                return checkpointDataTypeStruct;
            }
        }

        log.info("Restore source using checkpoint tables is empty");
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
