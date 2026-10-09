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

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;
import org.apache.seatunnel.shade.org.apache.commons.lang3.tuple.Pair;

import org.apache.seatunnel.api.source.scheduler.AsyncTaskKey;
import org.apache.seatunnel.api.source.scheduler.AsyncTaskOptions;
import org.apache.seatunnel.api.source.scheduler.Cancellable;
import org.apache.seatunnel.api.source.scheduler.CoordinatorScheduler;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.source.enumerator.scan.IcebergScanContext;
import org.apache.seatunnel.connectors.seatunnel.iceberg.source.enumerator.scan.IcebergScanSplitPlanner;
import org.apache.seatunnel.connectors.seatunnel.iceberg.source.split.IcebergFileScanTaskSplit;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;

import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
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

    private static final AsyncTaskKey MANAGED_DISCOVERY_TASK_KEY =
            AsyncTaskKey.of("iceberg-stream-split-discovery");
    private static final AsyncTaskKey MANAGED_DISCOVERY_TIMER_KEY =
            AsyncTaskKey.of("iceberg-stream-split-discovery-tick");
    private static final Duration MANAGED_CAPACITY_RETRY_DELAY = Duration.ofMillis(100);

    private final ConcurrentMap<TablePath, IcebergEnumeratorPosition> tableOffsets;
    private CoordinatorScheduler coordinatorScheduler;
    private Cancellable managedDiscoveryTimer;
    private boolean managedCoordinator;
    private boolean discoveryInProgress;
    private boolean closed;

    @VisibleForTesting volatile boolean initialized = false;

    public IcebergStreamSplitEnumerator(
            Context<IcebergFileScanTaskSplit> context,
            IcebergSourceConfig sourceConfig,
            Map<TablePath, CatalogTable> catalogTables,
            Map<TablePath, Pair<Schema, Schema>> tableSchemaProjections) {
        this(context, sourceConfig, catalogTables, tableSchemaProjections, null);
    }

    public IcebergStreamSplitEnumerator(
            Context<IcebergFileScanTaskSplit> context,
            IcebergSourceConfig sourceConfig,
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
    public void open() {
        super.open();
        managedCoordinator = context.isManagedCoordinatorRuntime();
        if (managedCoordinator) {
            coordinatorScheduler = context.getCoordinatorScheduler();
        }
    }

    @Override
    public void run() throws Exception {
        if (managedCoordinator) {
            triggerManagedDiscovery();
            return;
        }
        Set<Integer> readers = context.registeredReaders();
        while (true) {
            for (TablePath tablePath : pendingTables) {
                synchronized (stateLock) {
                    checkThrowInterruptedException();

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

            synchronized (stateLock) {
                stateLock.wait(sourceConfig.getIncrementScanInterval());
            }
        }
    }

    @Override
    public IcebergSplitEnumeratorState snapshotState(long checkpointId) throws Exception {
        if (managedCoordinator) {
            return new IcebergSplitEnumeratorState(
                    new ArrayList<>(pendingTables),
                    new HashMap<>(pendingSplits),
                    new HashMap<>(tableOffsets));
        }
        synchronized (stateLock) {
            return new IcebergSplitEnumeratorState(
                    new ArrayList<>(pendingTables),
                    new HashMap<>(pendingSplits),
                    new HashMap<>(tableOffsets));
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        if (managedCoordinator) {
            triggerManagedDiscovery();
            return;
        }
        if (initialized) {
            synchronized (stateLock) {
                stateLock.notifyAll();
            }
        }
    }

    @Override
    public void close() throws java.io.IOException {
        closed = true;
        if (managedDiscoveryTimer != null) {
            managedDiscoveryTimer.cancel();
            managedDiscoveryTimer = null;
        }
        super.close();
    }

    private void triggerManagedDiscovery() {
        if (closed || discoveryInProgress) {
            return;
        }
        if (managedDiscoveryTimer != null) {
            managedDiscoveryTimer.cancel();
            managedDiscoveryTimer = null;
        }
        if (!context.isAssignmentCapacityAvailable()) {
            scheduleManagedDiscovery(MANAGED_CAPACITY_RETRY_DELAY);
            return;
        }
        discoveryInProgress = true;
        Map<TablePath, IcebergEnumeratorPosition> offsetsSnapshot = new HashMap<>(tableOffsets);
        List<TablePath> tablesSnapshot = new ArrayList<>(pendingTables);
        coordinatorScheduler.callAsync(
                MANAGED_DISCOVERY_TASK_KEY,
                () -> discoverManagedSplits(tablesSnapshot, offsetsSnapshot),
                (results, failure) -> {
                    discoveryInProgress = false;
                    if (failure != null) {
                        throw new IllegalStateException(
                                "Iceberg streaming split discovery failed", failure);
                    }
                    for (Map.Entry<TablePath, IcebergEnumerationResult> entry :
                            results.entrySet()) {
                        TablePath tablePath = entry.getKey();
                        IcebergEnumerationResult result = entry.getValue();
                        if (!Objects.equals(
                                result.getFromPosition(), tableOffsets.get(tablePath))) {
                            log.info(
                                    "Discard stale Iceberg discovery result for table {}.",
                                    tablePath);
                            continue;
                        }
                        tableOffsets.put(tablePath, result.getToPosition());
                        addPendingSplits(result.getSplits());
                    }
                    assignPendingSplits(context.registeredReaders());
                    initialized = true;
                    scheduleNextManagedDiscovery();
                },
                AsyncTaskOptions.builder().timeout(Duration.ofMinutes(5)).build());
    }

    private void scheduleNextManagedDiscovery() {
        if (!closed && sourceConfig.getIncrementScanInterval() > 0) {
            scheduleManagedDiscovery(Duration.ofMillis(sourceConfig.getIncrementScanInterval()));
        }
    }

    private void scheduleManagedDiscovery(Duration delay) {
        if (closed) {
            return;
        }
        managedDiscoveryTimer =
                coordinatorScheduler.scheduleInCoordinatorThread(
                        MANAGED_DISCOVERY_TIMER_KEY,
                        delay,
                        () -> {
                            managedDiscoveryTimer = null;
                            triggerManagedDiscovery();
                        });
    }

    private Map<TablePath, IcebergEnumerationResult> discoverManagedSplits(
            List<TablePath> tablesSnapshot,
            Map<TablePath, IcebergEnumeratorPosition> offsetsSnapshot) {
        Map<TablePath, IcebergEnumerationResult> results = new HashMap<>();
        for (TablePath tablePath : tablesSnapshot) {
            Table table = loadTable(tablePath);
            Pair<Schema, Schema> tableSchemaProjection = tableSchemaProjections.get(tablePath);
            IcebergScanContext scanContext =
                    IcebergScanContext.streamScanContext(
                            sourceConfig,
                            sourceConfig.getTableConfig(tablePath),
                            tableSchemaProjection.getRight());
            results.put(
                    tablePath,
                    IcebergScanSplitPlanner.planStreamSplits(
                            table, scanContext, offsetsSnapshot.get(tablePath)));
        }
        return results;
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
