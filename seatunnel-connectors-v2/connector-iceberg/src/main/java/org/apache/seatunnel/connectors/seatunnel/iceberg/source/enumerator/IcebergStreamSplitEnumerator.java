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

package org.apache.seatunnel.connectors.seatunnel.iceberg.source.enumerator;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.source.enumerator.scan.IcebergScanContext;
import org.apache.seatunnel.connectors.seatunnel.iceberg.source.enumerator.scan.IcebergScanSplitPlanner;
import org.apache.seatunnel.connectors.seatunnel.iceberg.source.split.IcebergFileScanTaskSplit;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
public class IcebergStreamSplitEnumerator extends AbstractSplitEnumerator {

    private final ConcurrentMap<TablePath, IcebergEnumeratorPosition> tableOffsets;
    private volatile boolean initialized = false;

    public IcebergStreamSplitEnumerator(
            Context<IcebergFileScanTaskSplit> context,
            SourceConfig sourceConfig,
            Map<TablePath, CatalogTable> catalogTables,
            Map<TablePath, Pair<Schema, Schema>> tableSchemaProjections) {
        this(context, sourceConfig, catalogTables, tableSchemaProjections, null);
    }

    public IcebergStreamSplitEnumerator(
            Context<IcebergFileScanTaskSplit> context,
            SourceConfig sourceConfig,
            Map<TablePath, CatalogTable> catalogTables,
            Map<TablePath, Pair<Schema, Schema>> tableSchemaProjections,
            IcebergSplitEnumeratorState state) {
        super(context, sourceConfig, catalogTables, tableSchemaProjections, state);
        this.tableOffsets = new ConcurrentHashMap<>();
        if (state != null) {
            if (state.getLastEnumeratedPosition() != null) {
                // TODO: Waiting for migration to complete before remove
                state.setPendingTable(
                        catalogTables.values().stream().findFirst().get().getTablePath());
            }
            this.tableOffsets.putAll(state.getTableOffsets());
        }
    }

    @Override
    public void run() throws Exception {
        Set<Integer> readers = context.registeredReaders();
        while (true) {
            for (TablePath tablePath : pendingTables) {
                checkThrowInterruptedException();

                synchronized (stateLock) {
                    log.info("Scan table {}.", tablePath);

                    Collection<IcebergFileScanTaskSplit> splits = loadSplits(tablePath);
                    log.info("Scan table {} into {} splits.", tablePath, splits.size());
                    addPendingSplits(splits);
                    assignPendingSplits(readers);
                }
            }

            if (Boolean.FALSE.equals(initialized)) {
                initialized = true;
            }

            stateLock.wait(sourceConfig.getIncrementScanInterval());
        }
    }

    @Override
    public IcebergSplitEnumeratorState snapshotState(long checkpointId) throws Exception {
        synchronized (stateLock) {
            return new IcebergSplitEnumeratorState(
                    new ArrayList<>(pendingTables),
                    new HashMap<>(pendingSplits),
                    new HashMap<>(tableOffsets));
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        if (initialized) {
            stateLock.notifyAll();
        }
    }

    private List<IcebergFileScanTaskSplit> loadSplits(TablePath tablePath) {
        Table table = loadTable(tablePath);
        IcebergEnumeratorPosition offset = tableOffsets.get(tablePath);
        Pair<Schema, Schema> tableSchemaProjection = tableSchemaProjections.get(tablePath);
        IcebergScanContext scanContext =
                IcebergScanContext.streamScanContext(
                        sourceConfig,
                        sourceConfig.getTableConfig(tablePath),
                        tableSchemaProjection.getRight());
        IcebergEnumerationResult result =
                IcebergScanSplitPlanner.planStreamSplits(table, scanContext, offset);
        if (!Objects.equals(result.getFromPosition(), offset)) {
            log.info(
                    "Skip {} loaded splits because the scan starting position doesn't match "
                            + "the current enumerator position: enumerator position = {}, scan starting position = {}",
                    result.getSplits().size(),
                    tableOffsets.get(tablePath),
                    result.getFromPosition());
            return Collections.emptyList();
        } else {
            tableOffsets.put(tablePath, result.getToPosition());
            log.debug("Update enumerator position to {}", result.getToPosition());
            return result.getSplits();
        }
    }
}
