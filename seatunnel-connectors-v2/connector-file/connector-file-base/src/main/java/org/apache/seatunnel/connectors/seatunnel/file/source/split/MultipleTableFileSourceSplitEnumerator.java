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

package org.apache.seatunnel.connectors.seatunnel.file.source.split;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseFileSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseMultipleTableFileSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.source.FileSourceDocumentRouting;
import org.apache.seatunnel.connectors.seatunnel.file.source.state.FileSourceState;

import org.apache.commons.collections4.CollectionUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
public class MultipleTableFileSourceSplitEnumerator
        implements SourceSplitEnumerator<FileSourceSplit, FileSourceState> {

    private static final int LOG_SPLIT_ID_LIMIT = 50;

    private final Context<FileSourceSplit> context;
    private final Set<FileSourceSplit> allSplit;
    private final Set<FileSourceSplit> assignedSplit;
    private final List<BaseFileSourceConfig> fileSourceConfigs;
    private final AtomicInteger assignCount = new AtomicInteger(0);
    private final Object lock = new Object();
    private final FileSplitStrategy fileSplitStrategy;
    private final Set<String> documentRoutingTableIds;

    public MultipleTableFileSourceSplitEnumerator(
            Context<FileSourceSplit> context,
            BaseMultipleTableFileSourceConfig multipleTableFileSourceConfig,
            FileSplitStrategy fileSplitStrategy) {
        this(context, multipleTableFileSourceConfig, fileSplitStrategy, Collections.emptySet());
    }

    public MultipleTableFileSourceSplitEnumerator(
            Context<FileSourceSplit> context,
            BaseMultipleTableFileSourceConfig multipleTableFileSourceConfig,
            FileSplitStrategy fileSplitStrategy,
            Set<String> documentRoutingTableIds) {
        this.context = context;
        this.fileSourceConfigs = multipleTableFileSourceConfig.getFileSourceConfigs();
        this.assignedSplit = new HashSet<>();
        this.allSplit = new TreeSet<>(Comparator.comparing(FileSourceSplit::splitId));
        this.fileSplitStrategy = fileSplitStrategy;
        this.documentRoutingTableIds =
                new HashSet<>(
                        documentRoutingTableIds == null
                                ? Collections.emptySet()
                                : documentRoutingTableIds);
    }

    public MultipleTableFileSourceSplitEnumerator(
            Context<FileSourceSplit> context,
            BaseMultipleTableFileSourceConfig multipleTableFileSourceConfig,
            FileSourceState fileSourceState) {
        this(
                context,
                multipleTableFileSourceConfig,
                new DefaultFileSplitStrategy(),
                Collections.emptySet(),
                fileSourceState);
    }

    public MultipleTableFileSourceSplitEnumerator(
            Context<FileSourceSplit> context,
            BaseMultipleTableFileSourceConfig multipleTableFileSourceConfig,
            FileSplitStrategy fileSplitStrategy,
            FileSourceState fileSourceState) {
        this(
                context,
                multipleTableFileSourceConfig,
                fileSplitStrategy,
                Collections.emptySet(),
                fileSourceState);
    }

    public MultipleTableFileSourceSplitEnumerator(
            Context<FileSourceSplit> context,
            BaseMultipleTableFileSourceConfig multipleTableFileSourceConfig,
            FileSplitStrategy fileSplitStrategy,
            Set<String> documentRoutingTableIds,
            FileSourceState fileSourceState) {
        this(context, multipleTableFileSourceConfig, fileSplitStrategy, documentRoutingTableIds);
        this.assignedSplit.addAll(fileSourceState.getAssignedSplit());
    }

    @Override
    public void open() {
        boolean hasMultiSplits = false;
        Map<String, Integer> splitCountByTable = new HashMap<>();
        for (BaseFileSourceConfig fileSourceConfig : fileSourceConfigs) {
            String tableId =
                    fileSourceConfig.getCatalogTable().getTableId().toTablePath().toString();
            List<String> filePaths = fileSourceConfig.getFilePathsForSplitEnumerator();
            for (String filePath : filePaths) {
                List<FileSourceSplit> splits = fileSplitStrategy.split(tableId, filePath);
                splitCountByTable.merge(tableId, splits.size(), Integer::sum);
                allSplit.addAll(splits);
                if (splits.size() > 1) {
                    hasMultiSplits = true;
                    log.info(
                            "Split file [{}] for table [{}] into {} splits",
                            filePath,
                            tableId,
                            splits.size());
                }
            }
        }
        if (hasMultiSplits) {
            log.info(
                    "Split enumeration finished, total splits: {}, splits by table: {}",
                    allSplit.size(),
                    splitCountByTable);
        }
    }

    @Override
    public void addSplitsBack(List<FileSourceSplit> splits, int subtaskId) {
        if (CollectionUtils.isEmpty(splits)) {
            return;
        }
        allSplit.addAll(splits);
        assignSplit(subtaskId);
    }

    @Override
    public int currentUnassignedSplitSize() {
        return allSplit.size() - assignedSplit.size();
    }

    @Override
    public void handleSplitRequest(int subtaskId) {}

    @Override
    public void registerReader(int subtaskId) {}

    @Override
    public FileSourceState snapshotState(long checkpointId) {
        synchronized (lock) {
            return new FileSourceState(assignedSplit);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // do nothing.
    }

    private void assignSplit(int taskId) {
        List<FileSourceSplit> currentTaskSplits = new ArrayList<>();
        if (documentRoutingTableIds.isEmpty() && context.currentParallelism() == 1) {
            // if parallelism == 1, we should assign all the splits to reader
            currentTaskSplits.addAll(allSplit);
        } else {
            // if parallelism > 1, according to polling strategy to determine whether to
            // allocate the current task
            assignCount.set(0);
            for (FileSourceSplit fileSourceSplit : allSplit) {
                int splitOwner =
                        getSplitOwner(
                                fileSourceSplit,
                                assignCount.getAndIncrement(),
                                context.currentParallelism());
                if (splitOwner == taskId) {
                    currentTaskSplits.add(fileSourceSplit);
                }
            }
        }
        // assign splits
        context.assignSplit(taskId, currentTaskSplits);
        // save the state of assigned splits
        assignedSplit.addAll(currentTaskSplits);

        log.info(
                "SubTask {} is assigned to [{}], size {}",
                taskId,
                summarizeSplitIds(currentTaskSplits),
                currentTaskSplits.size());
        context.signalNoMoreSplits(taskId);
    }

    private static String summarizeSplitIds(List<FileSourceSplit> splits) {
        if (splits.isEmpty()) {
            return "";
        }
        if (splits.size() <= LOG_SPLIT_ID_LIMIT) {
            return splits.stream().map(FileSourceSplit::splitId).collect(Collectors.joining(","));
        }
        return splits.stream()
                        .limit(LOG_SPLIT_ID_LIMIT)
                        .map(FileSourceSplit::splitId)
                        .collect(Collectors.joining(","))
                + ",...("
                + (splits.size() - LOG_SPLIT_ID_LIMIT)
                + " more)";
    }

    private int getSplitOwner(FileSourceSplit split, int assignCount, int numReaders) {
        if (documentRoutingTableIds.contains(split.getTableId())) {
            return getDocumentRouteOwner(split, numReaders);
        }
        return getRoundRobinSplitOwner(assignCount, numReaders);
    }

    private static int getRoundRobinSplitOwner(int assignCount, int numReaders) {
        return assignCount % numReaders;
    }

    private static int getDocumentRouteOwner(FileSourceSplit split, int numReaders) {
        if (split.getStart() != 0L || split.getLength() >= 0L) {
            throw new IllegalStateException(
                    "Document routing requires whole-file splits, but got split "
                            + split.splitId());
        }
        String documentId = FileSourceDocumentRouting.buildDocumentId(split.getFilePath());
        return FileSourceDocumentRouting.routeBucket(documentId, numReaders);
    }

    @Override
    public void run() throws Exception {
        for (int i = 0; i < context.currentParallelism(); i++) {
            log.info("Assigned splits to reader [{}]", i);
            synchronized (lock) {
                assignSplit(i);
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (fileSplitStrategy instanceof Closeable) {
            ((Closeable) fileSplitStrategy).close();
            return;
        }
        if (fileSplitStrategy instanceof AutoCloseable) {
            try {
                ((AutoCloseable) fileSplitStrategy).close();
            } catch (Exception e) {
                if (e instanceof IOException) {
                    throw (IOException) e;
                }
                throw new IOException(e);
            }
        }
    }
}
