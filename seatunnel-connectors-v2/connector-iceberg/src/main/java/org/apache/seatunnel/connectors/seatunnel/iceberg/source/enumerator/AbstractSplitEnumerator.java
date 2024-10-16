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
import org.apache.seatunnel.connectors.seatunnel.iceberg.IcebergTableLoader;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.source.split.IcebergFileScanTaskSplit;

import org.apache.iceberg.Table;

import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public abstract class AbstractSplitEnumerator
        implements SourceSplitEnumerator<IcebergFileScanTaskSplit, IcebergSplitEnumeratorState> {

    protected final Context<IcebergFileScanTaskSplit> context;
    protected final SourceConfig sourceConfig;
    protected final Map<Integer, List<IcebergFileScanTaskSplit>> pendingSplits;

    protected IcebergTableLoader icebergTableLoader;
    @Getter private volatile boolean isOpen = false;
    private CatalogTable catalogTable;

    public AbstractSplitEnumerator(
            @NonNull SourceSplitEnumerator.Context<IcebergFileScanTaskSplit> context,
            @NonNull SourceConfig sourceConfig,
            @NonNull Map<Integer, List<IcebergFileScanTaskSplit>> pendingSplits,
            CatalogTable catalogTable) {
        this.context = context;
        this.sourceConfig = sourceConfig;
        this.pendingSplits = new HashMap<>(pendingSplits);
        this.catalogTable = catalogTable;
    }

    @Override
    public void open() {
        icebergTableLoader = IcebergTableLoader.create(sourceConfig, catalogTable);
        icebergTableLoader.open();
        isOpen = true;
    }

    @Override
    public void run() {
        refreshPendingSplits();
        assignPendingSplits(context.registeredReaders());
    }

    @Override
    public void close() throws IOException {
        icebergTableLoader.close();
        isOpen = false;
    }

    @Override
    public void addSplitsBack(List<IcebergFileScanTaskSplit> splits, int subtaskId) {
        addPendingSplits(splits);
        if (context.registeredReaders().contains(subtaskId)) {
            assignPendingSplits(Collections.singleton(subtaskId));
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplits.size();
    }

    @Override
    public void registerReader(int subtaskId) {
        log.debug("Adding reader {} to IcebergSourceEnumerator.", subtaskId);
        assignPendingSplits(Collections.singleton(subtaskId));
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}

    protected void refreshPendingSplits() {
        List<IcebergFileScanTaskSplit> newSplits = loadNewSplits(icebergTableLoader.loadTable());
        addPendingSplits(newSplits);
    }

    protected abstract List<IcebergFileScanTaskSplit> loadNewSplits(Table table);

    private void addPendingSplits(Collection<IcebergFileScanTaskSplit> newSplits) {
        int numReaders = context.currentParallelism();
        for (IcebergFileScanTaskSplit newSplit : newSplits) {
            int ownerReader = (newSplit.splitId().hashCode() & Integer.MAX_VALUE) % numReaders;
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
