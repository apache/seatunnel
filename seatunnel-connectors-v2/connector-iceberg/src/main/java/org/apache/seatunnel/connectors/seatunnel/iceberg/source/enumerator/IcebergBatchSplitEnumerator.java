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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class IcebergBatchSplitEnumerator extends AbstractSplitEnumerator {

    public IcebergBatchSplitEnumerator(
            Context<IcebergFileScanTaskSplit> context,
            SourceConfig sourceConfig,
            Map<TablePath, CatalogTable> catalogTables,
            Map<TablePath, Pair<Schema, Schema>> tableSchemaProjections) {
        this(context, sourceConfig, catalogTables, tableSchemaProjections, null);
    }

    public IcebergBatchSplitEnumerator(
            Context<IcebergFileScanTaskSplit> context,
            SourceConfig sourceConfig,
            Map<TablePath, CatalogTable> catalogTables,
            Map<TablePath, Pair<Schema, Schema>> tableSchemaProjections,
            IcebergSplitEnumeratorState state) {
        super(context, sourceConfig, catalogTables, tableSchemaProjections, state);
    }

    @Override
    public void run() throws Exception {
        Set<Integer> readers = context.registeredReaders();
        while (!pendingTables.isEmpty()) {
            synchronized (stateLock) {
                checkThrowInterruptedException();

                TablePath tablePath = pendingTables.poll();
                log.info("Splitting table {}.", tablePath);

                Collection<IcebergFileScanTaskSplit> splits = loadSplits(tablePath);
                log.info("Split table {} into {} splits.", tablePath, splits.size());

                addPendingSplits(splits);
            }

            synchronized (stateLock) {
                assignPendingSplits(readers);
            }
        }

        log.debug(
                "No more splits to assign." + " Sending NoMoreSplitsEvent to reader {}.", readers);
        readers.forEach(context::signalNoMoreSplits);
    }

    @Override
    public IcebergSplitEnumeratorState snapshotState(long checkpointId) throws Exception {
        synchronized (stateLock) {
            return new IcebergSplitEnumeratorState(
                    new ArrayList<>(pendingTables), new HashMap<>(pendingSplits));
        }
    }

    private List<IcebergFileScanTaskSplit> loadSplits(TablePath tablePath) {
        Table table = loadTable(tablePath);
        Pair<Schema, Schema> tableSchemaProjection = tableSchemaProjections.get(tablePath);
        IcebergScanContext scanContext =
                IcebergScanContext.scanContext(
                        sourceConfig,
                        sourceConfig.getTableConfig(tablePath),
                        tableSchemaProjection.getRight());
        return IcebergScanSplitPlanner.planSplits(table, scanContext);
    }
}
