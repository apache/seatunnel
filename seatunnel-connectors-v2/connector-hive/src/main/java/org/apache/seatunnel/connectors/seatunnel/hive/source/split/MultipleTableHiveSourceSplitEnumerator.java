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

package org.apache.seatunnel.connectors.seatunnel.hive.source.split;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.file.source.state.FileSourceState;
import org.apache.seatunnel.connectors.seatunnel.hive.source.config.HiveSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.hive.source.config.MultipleTableHiveSourceConfig;

import org.apache.commons.collections4.CollectionUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
public class MultipleTableHiveSourceSplitEnumerator
        implements SourceSplitEnumerator<FileSourceSplit, FileSourceState> {

    private final SourceSplitEnumerator.Context<FileSourceSplit> context;
    private final Set<FileSourceSplit> allSplit;
    private final Set<FileSourceSplit> assignedSplit;
    private final Map<String, List<String>> filePathMap;
    private final AtomicInteger assignCount = new AtomicInteger(0);

    public MultipleTableHiveSourceSplitEnumerator(
            SourceSplitEnumerator.Context<FileSourceSplit> context,
            MultipleTableHiveSourceConfig multipleTableLocalFileSourceConfig) {
        this.context = context;
        this.filePathMap =
                multipleTableLocalFileSourceConfig.getHiveSourceConfigs().stream()
                        .collect(
                                Collectors.toMap(
                                        localFileSourceConfig ->
                                                localFileSourceConfig
                                                        .getCatalogTable()
                                                        .getTableId()
                                                        .toTablePath()
                                                        .toString(),
                                        HiveSourceConfig::getFilePaths));
        this.assignedSplit = new HashSet<>();
        this.allSplit = new TreeSet<>(Comparator.comparing(FileSourceSplit::splitId));
    }

    public MultipleTableHiveSourceSplitEnumerator(
            SourceSplitEnumerator.Context<FileSourceSplit> context,
            MultipleTableHiveSourceConfig multipleTableLocalFileSourceConfig,
            FileSourceState localFileSourceState) {
        this(context, multipleTableLocalFileSourceConfig);
        this.assignedSplit.addAll(localFileSourceState.getAssignedSplit());
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
    public void registerReader(int subtaskId) {
        for (Map.Entry<String, List<String>> filePathEntry : filePathMap.entrySet()) {
            String tableId = filePathEntry.getKey();
            List<String> filePaths = filePathEntry.getValue();
            for (String filePath : filePaths) {
                allSplit.add(new FileSourceSplit(tableId, filePath));
            }
        }
        assignSplit(subtaskId);
    }

    @Override
    public FileSourceState snapshotState(long checkpointId) {
        return new FileSourceState(assignedSplit);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // do nothing.
    }

    private void assignSplit(int taskId) {
        List<FileSourceSplit> currentTaskSplits = new ArrayList<>();
        if (context.currentParallelism() == 1) {
            // if parallelism == 1, we should assign all the splits to reader
            currentTaskSplits.addAll(allSplit);
        } else {
            // if parallelism > 1, according to polling strategy to determine whether to
            // allocate the current task
            assignCount.set(0);
            for (FileSourceSplit fileSourceSplit : allSplit) {
                int splitOwner =
                        getSplitOwner(assignCount.getAndIncrement(), context.currentParallelism());
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
                currentTaskSplits.stream()
                        .map(FileSourceSplit::splitId)
                        .collect(Collectors.joining(",")),
                currentTaskSplits.size());
        context.signalNoMoreSplits(taskId);
    }

    private static int getSplitOwner(int assignCount, int numReaders) {
        return assignCount % numReaders;
    }

    @Override
    public void open() {
        // do nothing
    }

    @Override
    public void run() throws Exception {
        // do nothing
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }
}
