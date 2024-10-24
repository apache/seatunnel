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

package org.apache.seatunnel.connectors.seatunnel.milvus.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectionErrorCode;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;

import io.milvus.client.MilvusClient;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.DescribeCollectionResponse;
import io.milvus.grpc.FieldSchema;
import io.milvus.grpc.ShowPartitionsResponse;
import io.milvus.param.ConnectParam;
import io.milvus.param.R;
import io.milvus.param.collection.DescribeCollectionParam;
import io.milvus.param.partition.ShowPartitionsParam;
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

@Slf4j
public class MilvusSourceSplitEnumertor
        implements SourceSplitEnumerator<MilvusSourceSplit, MilvusSourceState> {

    private final Map<TablePath, CatalogTable> tables;
    private final Context<MilvusSourceSplit> context;
    private final ConcurrentLinkedQueue<TablePath> pendingTables;
    private final Map<Integer, List<MilvusSourceSplit>> pendingSplits;
    private final Object stateLock = new Object();
    private MilvusClient client = null;

    private final ReadonlyConfig config;

    public MilvusSourceSplitEnumertor(
            Context<MilvusSourceSplit> context,
            ReadonlyConfig config,
            Map<TablePath, CatalogTable> sourceTables,
            MilvusSourceState sourceState) {
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
    }

    @Override
    public void open() {
        ConnectParam connectParam =
                ConnectParam.newBuilder()
                        .withUri(config.get(MilvusSourceConfig.URL))
                        .withToken(config.get(MilvusSourceConfig.TOKEN))
                        .build();
        this.client = new MilvusServiceClient(connectParam);
    }

    @Override
    public void run() throws Exception {
        log.info("Starting milvus split enumerator.");
        Set<Integer> readers = context.registeredReaders();
        while (!pendingTables.isEmpty()) {
            synchronized (stateLock) {
                TablePath tablePath = pendingTables.poll();
                log.info("begin to split table path: {}", tablePath);
                Collection<MilvusSourceSplit> splits = generateSplits(tables.get(tablePath));
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

    private Collection<MilvusSourceSplit> generateSplits(CatalogTable table) {
        log.info("Start splitting table {} into chunks by partition...", table.getTablePath());
        String database = table.getTablePath().getDatabaseName();
        String collection = table.getTablePath().getTableName();
        R<DescribeCollectionResponse> describeCollectionResponseR =
                client.describeCollection(
                        DescribeCollectionParam.newBuilder()
                                .withDatabaseName(database)
                                .withCollectionName(collection)
                                .build());
        boolean hasPartitionKey =
                describeCollectionResponseR.getData().getSchema().getFieldsList().stream()
                        .anyMatch(FieldSchema::getIsPartitionKey);
        List<MilvusSourceSplit> milvusSourceSplits = new ArrayList<>();
        if (!hasPartitionKey) {
            ShowPartitionsParam showPartitionsParam =
                    ShowPartitionsParam.newBuilder()
                            .withDatabaseName(database)
                            .withCollectionName(collection)
                            .build();
            R<ShowPartitionsResponse> showPartitionsResponseR =
                    client.showPartitions(showPartitionsParam);
            if (showPartitionsResponseR.getStatus() != R.Status.Success.getCode()) {
                throw new MilvusConnectorException(
                        MilvusConnectionErrorCode.LIST_PARTITIONS_FAILED,
                        "Failed to show partitions: " + showPartitionsResponseR.getMessage());
            }
            List<String> partitionList = showPartitionsResponseR.getData().getPartitionNamesList();
            for (String partitionName : partitionList) {
                MilvusSourceSplit milvusSourceSplit =
                        MilvusSourceSplit.builder()
                                .tablePath(table.getTablePath())
                                .splitId(createSplitId(table.getTablePath(), partitionName))
                                .partitionName(partitionName)
                                .build();
                log.info("Generated split: {}", milvusSourceSplit);
                milvusSourceSplits.add(milvusSourceSplit);
            }
        } else {
            MilvusSourceSplit milvusSourceSplit =
                    MilvusSourceSplit.builder()
                            .tablePath(table.getTablePath())
                            .splitId(createSplitId(table.getTablePath(), "0"))
                            .build();
            log.info("Generated split: {}", milvusSourceSplit);
            milvusSourceSplits.add(milvusSourceSplit);
        }
        return milvusSourceSplits;
    }

    protected String createSplitId(TablePath tablePath, String index) {
        return String.format("%s-%s", tablePath, index);
    }

    private void addPendingSplit(Collection<MilvusSourceSplit> splits) {
        int readerCount = context.currentParallelism();
        for (MilvusSourceSplit split : splits) {
            int ownerReader = getSplitOwner(split.splitId(), readerCount);
            log.info("Assigning {} to {} reader.", split, ownerReader);

            pendingSplits.computeIfAbsent(ownerReader, r -> new ArrayList<>()).add(split);
        }
    }

    private static int getSplitOwner(String tp, int numReaders) {
        return (tp.hashCode() & Integer.MAX_VALUE) % numReaders;
    }

    private void assignSplit(Collection<Integer> readers) {
        log.info("Assign pendingSplits to readers {}", readers);

        for (int reader : readers) {
            List<MilvusSourceSplit> assignmentForReader = pendingSplits.remove(reader);
            if (assignmentForReader != null && !assignmentForReader.isEmpty()) {
                log.debug("Assign splits {} to reader {}", assignmentForReader, reader);
                context.assignSplit(reader, assignmentForReader);
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    @Override
    public void addSplitsBack(List<MilvusSourceSplit> splits, int subtaskId) {
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

    private void addPendingSplit(Collection<MilvusSourceSplit> splits, int ownerReader) {
        pendingSplits.computeIfAbsent(ownerReader, r -> new ArrayList<>()).addAll(splits);
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingTables.isEmpty() && pendingSplits.isEmpty() ? 0 : 1;
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        throw new MilvusConnectorException(
                CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                String.format("Unsupported handleSplitRequest: %d", subtaskId));
    }

    @Override
    public void registerReader(int subtaskId) {
        log.info("Register reader {} to MilvusSourceSplitEnumerator.", subtaskId);
        if (!pendingSplits.isEmpty()) {
            synchronized (stateLock) {
                assignSplit(Collections.singletonList(subtaskId));
            }
        }
    }

    @Override
    public MilvusSourceState snapshotState(long checkpointId) throws Exception {
        synchronized (stateLock) {
            return new MilvusSourceState(
                    new ArrayList(pendingTables), new HashMap<>(pendingSplits));
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
