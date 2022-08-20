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
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.connectors.seatunnel.file.source.state.FileSourceState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FileSourceSplitEnumerator implements SourceSplitEnumerator<FileSourceSplit, FileSourceState> {
    private final Context<FileSourceSplit> context;
    private Set<FileSourceSplit> pendingSplit;
    private Set<FileSourceSplit> assignedSplit;
    private final List<String> filePaths;

    public FileSourceSplitEnumerator(Context<FileSourceSplit> context, List<String> filePaths) {
        this.context = context;
        this.filePaths = filePaths;
    }

    public FileSourceSplitEnumerator(Context<FileSourceSplit> context, List<String> filePaths,
                                     FileSourceState sourceState) {
        this(context, filePaths);
        this.assignedSplit = sourceState.getAssignedSplit();
    }

    @Override
    public void open() {
        this.assignedSplit = new HashSet<>();
        this.pendingSplit = new HashSet<>();
    }

    @Override
    public void run() {
        pendingSplit = getHiveFileSplit();
        assignSplit(context.registeredReaders());
    }

    private Set<FileSourceSplit> getHiveFileSplit() {
        Set<FileSourceSplit> hiveSourceSplits = new HashSet<>();
        filePaths.forEach(k -> hiveSourceSplits.add(new FileSourceSplit(k)));
        return hiveSourceSplits;

    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void addSplitsBack(List<FileSourceSplit> splits, int subtaskId) {
        if (!splits.isEmpty()) {
            pendingSplit.addAll(splits);
            assignSplit(Collections.singletonList(subtaskId));
        }
    }

    private void assignSplit(Collection<Integer> taskIDList) {
        Map<Integer, List<FileSourceSplit>> readySplit = new HashMap<>(Common.COLLECTION_SIZE);
        for (int taskID : taskIDList) {
            readySplit.computeIfAbsent(taskID, id -> new ArrayList<>());
        }

        pendingSplit.forEach(s -> readySplit.get(getSplitOwner(s.splitId(), taskIDList.size()))
                .add(s));
        readySplit.forEach(context::assignSplit);
        assignedSplit.addAll(pendingSplit);
        pendingSplit.clear();
    }

    private static int getSplitOwner(String tp, int numReaders) {
        return tp.hashCode() % numReaders;
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplit.size();
    }

    @Override
    public void registerReader(int subtaskId) {
        if (!pendingSplit.isEmpty()) {
            assignSplit(Collections.singletonList(subtaskId));
        }
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
