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

package org.apache.seatunnel.connectors.pinecone.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;

import io.pinecone.clients.Index;
import io.pinecone.clients.Pinecone;
import io.pinecone.proto.DescribeIndexStatsResponse;
import io.pinecone.proto.NamespaceSummary;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.seatunnel.connectors.pinecone.config.PineconeSourceConfig.API_KEY;

@Slf4j
public class PineconeSourceSplitEnumertor
        implements SourceSplitEnumerator<PineconeSourceSplit, PineconeSourceState> {
    private final Map<TablePath, CatalogTable> tables;
    private final Context<PineconeSourceSplit> context;
    private final ConcurrentLinkedQueue<TablePath> pendingTables;
    private final Map<Integer, List<PineconeSourceSplit>> pendingSplits;
    private final Object stateLock = new Object();
    private final Pinecone pinecone;

    private ReadonlyConfig config;

    public PineconeSourceSplitEnumertor(
            Context<PineconeSourceSplit> context,
            ReadonlyConfig config,
            Map<TablePath, CatalogTable> sourceTables,
            PineconeSourceState sourceState) {
        this.context = context;
        this.tables = sourceTables;
        this.config = config;
        if (sourceState == null) {
            this.pendingTables = new ConcurrentLinkedQueue<>(tables.keySet());
            this.pendingSplits = new HashMap<>();
        } else {
            this.pendingTables = new ConcurrentLinkedQueue<>(sourceState.getPendingTables());
            this.pendingSplits = new HashMap<>(sourceState.getPendingSplits());
        }
        pinecone = new Pinecone.Builder(config.get(API_KEY)).build();
    }

    @Override
    public void open() {}

    /** The method is executed by the engine only once. */
    @Override
    public void run() throws Exception {
        log.info("Starting pinecone split enumerator.");
        Set<Integer> readers = context.registeredReaders();
        while (!pendingTables.isEmpty()) {
            synchronized (stateLock) {
                TablePath tablePath = pendingTables.poll();
                log.info("begin to split table path: {}", tablePath);
                Collection<PineconeSourceSplit> splits = generateSplits(tables.get(tablePath));
                log.info("end to split table {} into {} splits.", tablePath, splits.size());

                addPendingSplit(splits);
            }

            synchronized (stateLock) {
                assignSplit(readers);
            }
        }

        log.info("No more splits to assign." + " Sending NoMoreSplitsEvent to reader {}.", readers);
        readers.forEach(context::signalNoMoreSplits);
    }

    private void assignSplit(Collection<Integer> readers) {
        log.info("Assign pendingSplits to readers {}", readers);

        for (int reader : readers) {
            List<PineconeSourceSplit> assignmentForReader = pendingSplits.remove(reader);
            if (assignmentForReader != null && !assignmentForReader.isEmpty()) {
                log.debug("Assign splits {} to reader {}", assignmentForReader, reader);
                context.assignSplit(reader, assignmentForReader);
            }
        }
    }

    private void addPendingSplit(Collection<PineconeSourceSplit> splits) {
        int readerCount = context.currentParallelism();
        for (PineconeSourceSplit split : splits) {
            int ownerReader = getSplitOwner(split.splitId(), readerCount);
            log.info("Assigning {} to {} reader.", split, ownerReader);

            pendingSplits.computeIfAbsent(ownerReader, r -> new ArrayList<>()).add(split);
        }
    }

    private Collection<PineconeSourceSplit> generateSplits(CatalogTable catalogTable) {
        Index index = pinecone.getIndexConnection(catalogTable.getTablePath().getTableName());
        DescribeIndexStatsResponse describeIndexStatsResponse = index.describeIndexStats();
        Map<String, NamespaceSummary> namespaceSummaryMap =
                describeIndexStatsResponse.getNamespacesMap();
        List<PineconeSourceSplit> splits = new ArrayList<>();
        for (String namespace : namespaceSummaryMap.keySet()) {
            PineconeSourceSplit pineconeSourceSplit =
                    PineconeSourceSplit.builder()
                            .tablePath(catalogTable.getTablePath())
                            .splitId(catalogTable.getTablePath().getTableName() + "-" + namespace)
                            .namespace(namespace)
                            .build();
            splits.add(pineconeSourceSplit);
        }
        return splits;
    }

    private static int getSplitOwner(String tp, int numReaders) {
        return (tp.hashCode() & Integer.MAX_VALUE) % numReaders;
    }

    /**
     * Called to close the enumerator, in case it holds on to any resources, like threads or network
     * connections.
     */
    @Override
    public void close() throws IOException {}

    /**
     * Add a split back to the split enumerator. It will only happen when a {@link SourceReader}
     * fails and there are splits assigned to it after the last successful checkpoint.
     *
     * @param splits The split to add back to the enumerator for reassignment.
     * @param subtaskId The id of the subtask to which the returned splits belong.
     */
    @Override
    public void addSplitsBack(List<PineconeSourceSplit> splits, int subtaskId) {
        if (!splits.isEmpty()) {
            synchronized (stateLock) {
                addPendingSplit(splits, subtaskId);
                if (context.registeredReaders().contains(subtaskId)) {
                    assignSplit(Collections.singletonList(subtaskId));
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

    private void addPendingSplit(Collection<PineconeSourceSplit> splits, int ownerReader) {
        pendingSplits.computeIfAbsent(ownerReader, r -> new ArrayList<>()).addAll(splits);
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingTables.isEmpty() && pendingSplits.isEmpty() ? 0 : 1;
    }

    @Override
    public void handleSplitRequest(int subtaskId) {}

    @Override
    public void registerReader(int subtaskId) {
        log.info("Register reader {} to MilvusSourceSplitEnumerator.", subtaskId);
        if (!pendingSplits.isEmpty()) {
            synchronized (stateLock) {
                assignSplit(Collections.singletonList(subtaskId));
            }
        }
    }

    /**
     * If the source is bounded, checkpoint is not triggered.
     *
     * @param checkpointId
     */
    @Override
    public PineconeSourceState snapshotState(long checkpointId) throws Exception {
        synchronized (stateLock) {
            return new PineconeSourceState(
                    new ArrayList(pendingTables), new HashMap<>(pendingSplits));
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
