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
import org.apache.seatunnel.connectors.seatunnel.file.source.state.FileSourceState;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class FileSourceSplitEnumerator implements SourceSplitEnumerator<FileSourceSplit, FileSourceState> {
    private final Context<FileSourceSplit> context;
    private Set<FileSourceSplit> pendingSplit;
    private Set<FileSourceSplit> assignedSplit;
    private final List<String> filePaths;

    public FileSourceSplitEnumerator(SourceSplitEnumerator.Context<FileSourceSplit> context, List<String> filePaths) {
        this.context = context;
        this.filePaths = filePaths;
        this.assignedSplit = new HashSet<>();
    }

    public FileSourceSplitEnumerator(SourceSplitEnumerator.Context<FileSourceSplit> context, List<String> filePaths,
                                     FileSourceState sourceState) {
        this(context, filePaths);
        this.assignedSplit = sourceState.getAssignedSplit();
    }

    @Override
    public void open() {
        this.pendingSplit = new HashSet<>();
    }

    @Override
    public void run() {
        // do nothing
    }

    private Set<FileSourceSplit> getFileSplit() {
        Set<FileSourceSplit> fileSourceSplits = new HashSet<>();
        filePaths.forEach(k -> fileSourceSplits.add(new FileSourceSplit(k)));
        return fileSourceSplits;
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }

    @Override
    public void addSplitsBack(List<FileSourceSplit> splits, int subtaskId) {
        if (!splits.isEmpty()) {
            pendingSplit.addAll(splits);
            assignSplit(subtaskId);
        }
    }

    private void assignSplit(int taskId) {
        ArrayList<FileSourceSplit> currentTaskSplits = new ArrayList<>();
        if (context.currentParallelism() == 1) {
            // if parallelism == 1, we should assign all the splits to reader
            currentTaskSplits.addAll(pendingSplit);
        } else {
            // if parallelism > 1, according to hashCode of split's id to determine whether to allocate the current task
            for (FileSourceSplit fileSourceSplit : pendingSplit) {
                int splitOwner = getSplitOwner(fileSourceSplit.splitId(), context.currentParallelism());
                if (splitOwner == taskId) {
                    currentTaskSplits.add(fileSourceSplit);
                }
            }
        }
        // assign splits
        context.assignSplit(taskId, currentTaskSplits);
        // save the state of assigned splits
        assignedSplit.addAll(currentTaskSplits);
        // remove the assigned splits from pending splits
        currentTaskSplits.forEach(split -> pendingSplit.remove(split));
        log.info("SubTask {} is assigned to [{}]", taskId, currentTaskSplits.stream().map(FileSourceSplit::splitId).collect(Collectors.joining(",")));
        context.signalNoMoreSplits(taskId);
    }

    private static int getSplitOwner(String tp, int numReaders) {
        return (tp.hashCode() & Integer.MAX_VALUE) % numReaders;
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplit.size();
    }

    @Override
    public void registerReader(int subtaskId) {
        pendingSplit = getFileSplit();
        assignSplit(subtaskId);
    }

    @Override
    public FileSourceState snapshotState(long checkpointId) {
        return new FileSourceState(assignedSplit);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {

    }

    @Override
    public void handleSplitRequest(int subtaskId) {

    }
}
