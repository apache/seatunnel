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
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.hadoop.HadoopFileSystemProxy;
import org.apache.seatunnel.connectors.seatunnel.file.source.state.FileSourceState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class FileSourceSplitEnumerator
        implements SourceSplitEnumerator<FileSourceSplit, FileSourceState> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileSourceSplitEnumerator.class);

    private final Context<FileSourceSplit> context;
    private final Set<FileSourceSplit> allSplit =
            new TreeSet<>(Comparator.comparing(FileSourceSplit::splitId));
    private Set<FileSourceSplit> assignedSplit;
    private final List<String> filePaths;
    private final Object lock = new Object();
    private final AtomicInteger assignCount = new AtomicInteger(0);

    // File splitting configuration
    private final boolean enableFileSplit;
    private final int fileSplitSizeMB;
    private final FileFormat fileFormat;
    private final HadoopConf hadoopConf;
    private transient HadoopFileSystemProxy hadoopFileSystemProxy;

    public FileSourceSplitEnumerator(
            SourceSplitEnumerator.Context<FileSourceSplit> context, List<String> filePaths) {
        this(context, filePaths, null, null, false, 0, null);
    }

    public FileSourceSplitEnumerator(
            SourceSplitEnumerator.Context<FileSourceSplit> context,
            List<String> filePaths,
            FileSourceState sourceState) {
        this(context, filePaths, sourceState, null, false, 0, null);
    }

    /** Constructor with file splitting configuration */
    public FileSourceSplitEnumerator(
            SourceSplitEnumerator.Context<FileSourceSplit> context,
            List<String> filePaths,
            FileSourceState sourceState,
            FileFormat fileFormat,
            boolean enableFileSplit,
            int fileSplitSizeMB,
            HadoopConf hadoopConf) {
        this.context = context;
        this.filePaths = filePaths;
        this.assignedSplit = new HashSet<>();
        this.fileFormat = fileFormat;
        this.enableFileSplit = enableFileSplit;
        this.fileSplitSizeMB = fileSplitSizeMB;
        this.hadoopConf = hadoopConf;

        if (sourceState != null) {
            this.assignedSplit = sourceState.getAssignedSplit();
        }

        if (this.hadoopConf != null) {
            this.hadoopFileSystemProxy = new HadoopFileSystemProxy(this.hadoopConf);
        }
    }

    @Override
    public void open() {
        this.allSplit.addAll(discoverySplits());
    }

    @Override
    public void run() {
        for (int i = 0; i < context.currentParallelism(); i++) {
            LOGGER.info("Assigned splits to reader [{}]", i);
            synchronized (lock) {
                assignSplit(i);
            }
        }
    }

    private Set<FileSourceSplit> discoverySplits() {
        Set<FileSourceSplit> fileSourceSplits = new HashSet<>();

        for (String filePath : filePaths) {
            try {
                // Check if file splitting is enabled and supported
                if (enableFileSplit
                        && fileSplitSizeMB > 0
                        && fileFormat != null
                        && FileSplitUtils.supportsSplitting(fileFormat)
                        && hadoopFileSystemProxy != null) {

                    // Generate splits for this file
                    List<FileSourceSplit> splits =
                            FileSplitUtils.generateFileSplits(
                                    filePath,
                                    null,
                                    fileFormat,
                                    fileSplitSizeMB,
                                    hadoopFileSystemProxy);
                    fileSourceSplits.addAll(splits);

                    LOGGER.info("File {} split into {} parts", filePath, splits.size());
                } else {
                    // Use traditional single file split
                    fileSourceSplits.add(new FileSourceSplit(filePath));
                }
            } catch (IOException e) {
                LOGGER.warn(
                        "Failed to split file {}, using single split: {}",
                        filePath,
                        e.getMessage());
                // Fall back to single split
                fileSourceSplits.add(new FileSourceSplit(filePath));
            }
        }

        return fileSourceSplits;
    }

    @Override
    public void close() throws IOException {
        if (hadoopFileSystemProxy != null) {
            try {
                hadoopFileSystemProxy.close();
            } catch (Exception e) {
                LOGGER.warn("Failed to close HadoopFileSystemProxy: {}", e.getMessage());
            }
        }
    }

    @Override
    public void addSplitsBack(List<FileSourceSplit> splits, int subtaskId) {
        if (!splits.isEmpty()) {
            allSplit.addAll(splits);
            assignSplit(subtaskId);
        }
    }

    private void assignSplit(int taskId) {
        ArrayList<FileSourceSplit> currentTaskSplits = new ArrayList<>();
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

        LOGGER.info(
                "SubTask {} is assigned to [{}]",
                taskId,
                currentTaskSplits.stream()
                        .map(FileSourceSplit::splitId)
                        .collect(Collectors.joining(",")));
        context.signalNoMoreSplits(taskId);
    }

    private static int getSplitOwner(int assignCount, int numReaders) {
        return assignCount % numReaders;
    }

    @Override
    public int currentUnassignedSplitSize() {
        return allSplit.size() - assignedSplit.size();
    }

    @Override
    public void registerReader(int subtaskId) {
        // do nothing
    }

    @Override
    public FileSourceState snapshotState(long checkpointId) {
        synchronized (lock) {
            return new FileSourceState(assignedSplit);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    @Override
    public void handleSplitRequest(int subtaskId) {}
}
