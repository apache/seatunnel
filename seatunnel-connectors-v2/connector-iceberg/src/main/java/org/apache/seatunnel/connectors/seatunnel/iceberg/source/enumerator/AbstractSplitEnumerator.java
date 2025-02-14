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

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.iceberg.IcebergCatalogLoader;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.source.split.IcebergFileScanTaskSplit;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public abstract class AbstractSplitEnumerator
        implements SourceSplitEnumerator<IcebergFileScanTaskSplit, IcebergSplitEnumeratorState> {

    protected final Context<IcebergFileScanTaskSplit> context;
    protected final SourceConfig sourceConfig;
    protected final Map<TablePath, CatalogTable> tables;
    protected final Map<TablePath, Pair<Schema, Schema>> tableSchemaProjections;
    protected final Catalog icebergCatalog;
    protected final Object stateLock = new Object();

    protected final BlockingQueue<TablePath> pendingTables;
    protected final Map<Integer, List<IcebergFileScanTaskSplit>> pendingSplits;

    public AbstractSplitEnumerator(
            Context<IcebergFileScanTaskSplit> context,
            SourceConfig sourceConfig,
            Map<TablePath, CatalogTable> catalogTables,
            Map<TablePath, Pair<Schema, Schema>> tableSchemaProjections) {
        this(context, sourceConfig, catalogTables, tableSchemaProjections, null);
    }

    public AbstractSplitEnumerator(
            Context<IcebergFileScanTaskSplit> context,
            SourceConfig sourceConfig,
            Map<TablePath, CatalogTable> catalogTables,
            Map<TablePath, Pair<Schema, Schema>> tableSchemaProjections,
            IcebergSplitEnumeratorState state) {
        this.context = context;
        this.sourceConfig = sourceConfig;
        this.tables = catalogTables;
        this.tableSchemaProjections = tableSchemaProjections;
        this.icebergCatalog = new IcebergCatalogLoader(sourceConfig).loadCatalog();
        this.pendingTables = new ArrayBlockingQueue<>(catalogTables.size());
        this.pendingSplits = new HashMap<>();
        if (state == null) {
            this.pendingTables.addAll(
                    catalogTables.values().stream()
                            .map(CatalogTable::getTablePath)
                            .collect(Collectors.toList()));
        } else {
            this.pendingTables.addAll(state.getPendingTables());
            state.getPendingSplits().values().stream()
                    .flatMap(
                            (Function<
                                            List<IcebergFileScanTaskSplit>,
                                            Stream<IcebergFileScanTaskSplit>>)
                                    splits -> splits.stream())
                    .map(
                            (Function<IcebergFileScanTaskSplit, IcebergFileScanTaskSplit>)
                                    split -> {
                                        // TODO: Waiting for old version migration to complete
                                        // before remove
                                        if (split.getTablePath() == null) {
                                            new IcebergFileScanTaskSplit(
                                                    catalogTables.values().stream()
                                                            .findFirst()
                                                            .get()
                                                            .getTablePath(),
                                                    split.getTask(),
                                                    split.getRecordOffset());
                                        }
                                        return null;
                                    })
                    .forEach(
                            split ->
                                    pendingSplits
                                            .computeIfAbsent(
                                                    getSplitOwner(
                                                            split.splitId(),
                                                            context.currentParallelism()),
                                                    r -> new ArrayList<>())
                                            .add(split));
        }
    }

    @Override
    public void open() {
        log.info("Open split enumerator.");
    }

    @Override
    public void addSplitsBack(List<IcebergFileScanTaskSplit> splits, int subtaskId) {
        if (!splits.isEmpty()) {
            synchronized (stateLock) {
                addPendingSplits(splits);
                if (context.registeredReaders().contains(subtaskId)) {
                    assignPendingSplits(Collections.singleton(subtaskId));
                } else {
                    log.warn(
                            "Reader {} is not registered. Pending splits {} are not assigned.",
                            subtaskId,
                            splits);
                }
            }
        }
        log.info("Add back splits {} to JdbcSourceSplitEnumerator.", splits.size());
    }

    @Override
    public int currentUnassignedSplitSize() {
        if (!pendingTables.isEmpty()) {
            return pendingTables.size();
        }
        if (!pendingSplits.isEmpty()) {
            return pendingSplits.values().stream().mapToInt(List::size).sum();
        }
        return 0;
    }

    @Override
    public void handleSplitRequest(int subtaskId) {}

    @Override
    public void registerReader(int subtaskId) {
        log.debug("Adding reader {} to IcebergSourceEnumerator.", subtaskId);
        synchronized (stateLock) {
            assignPendingSplits(Collections.singleton(subtaskId));
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}

    @SneakyThrows
    @Override
    public void close() throws IOException {
        log.info("Close split enumerator.");
        if (icebergCatalog instanceof AutoCloseable) {
            ((AutoCloseable) icebergCatalog).close();
        }
    }

    protected Table loadTable(TablePath tablePath) {
        return icebergCatalog.loadTable(
                TableIdentifier.of(tablePath.getDatabaseName(), tablePath.getTableName()));
    }

    protected void checkThrowInterruptedException() throws InterruptedException {
        if (Thread.currentThread().isInterrupted()) {
            log.info("Enumerator thread is interrupted.");
            throw new InterruptedException("Enumerator thread is interrupted.");
        }
    }

    private static int getSplitOwner(String splitId, int numReaders) {
        return (splitId.hashCode() & Integer.MAX_VALUE) % numReaders;
    }

    protected void addPendingSplits(Collection<IcebergFileScanTaskSplit> newSplits) {
        int numReaders = context.currentParallelism();
        for (IcebergFileScanTaskSplit newSplit : newSplits) {
            int ownerReader = getSplitOwner(newSplit.splitId(), numReaders);
            pendingSplits.computeIfAbsent(ownerReader, r -> new ArrayList<>()).add(newSplit);
            log.info("Assigning {} to {} reader.", newSplit, ownerReader);
        }
    }

    protected void assignPendingSplits(Set<Integer> pendingReaders) {
        for (int pendingReader : pendingReaders) {
            List<IcebergFileScanTaskSplit> pendingAssignmentForReader =
                    pendingSplits.remove(pendingReader);
            if (pendingAssignmentForReader != null && !pendingAssignmentForReader.isEmpty()) {
                log.info(
                        "Assign splits {} to reader {}", pendingAssignmentForReader, pendingReader);
                try {
                    context.assignSplit(pendingReader, pendingAssignmentForReader);
                } catch (Exception e) {
                    log.error(
                            "Failed to assign splits {} to reader {}",
                            pendingAssignmentForReader,
                            pendingReader,
                            e);
                    pendingSplits.put(pendingReader, pendingAssignmentForReader);
                }
            }
        }
    }
}
