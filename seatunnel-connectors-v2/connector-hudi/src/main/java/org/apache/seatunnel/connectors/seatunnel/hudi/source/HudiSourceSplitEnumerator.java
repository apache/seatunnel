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

package org.apache.seatunnel.connectors.seatunnel.hudi.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.connectors.seatunnel.hudi.util.HudiUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HudiSourceSplitEnumerator implements SourceSplitEnumerator<HudiSourceSplit, HudiSourceState> {

    private final Context<HudiSourceSplit> context;
    private Set<HudiSourceSplit> pendingSplit;
    private Set<HudiSourceSplit> assignedSplit;
    private final String tablePath;
    private final String confPaths;

    public HudiSourceSplitEnumerator(SourceSplitEnumerator.Context<HudiSourceSplit> context, String tablePath, String confPaths) {
        this.context = context;
        this.tablePath = tablePath;
        this.confPaths = confPaths;
    }

    public HudiSourceSplitEnumerator(SourceSplitEnumerator.Context<HudiSourceSplit> context, String tablePath,
                                     String confPaths,
                                     HudiSourceState sourceState) {
        this(context, tablePath, confPaths);
        this.assignedSplit = sourceState.getAssignedSplit();
    }

    @Override
    public void open() {
        this.assignedSplit = new HashSet<>();
        this.pendingSplit = new HashSet<>();
    }

    @Override
    public void run() throws Exception {
        pendingSplit = getHudiSplit();
        assignSplit(context.registeredReaders());
    }

    private Set<HudiSourceSplit> getHudiSplit() throws IOException {
        Set<HudiSourceSplit> hudiSourceSplits = new HashSet<>();
        Path path = new Path(tablePath);
        Configuration configuration = HudiUtil.getConfiguration(confPaths);
        JobConf jobConf = HudiUtil.toJobConf(configuration);
        FileInputFormat.setInputPaths(jobConf, path);
        HoodieParquetInputFormat inputFormat = new HoodieParquetInputFormat();
        inputFormat.setConf(jobConf);
        for (InputSplit split : inputFormat.getSplits(jobConf, 0)) {
            hudiSourceSplits.add(new HudiSourceSplit(split.toString(), split));
        }
        return hudiSourceSplits;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void addSplitsBack(List<HudiSourceSplit> splits, int subtaskId) {
        if (!splits.isEmpty()) {
            pendingSplit.addAll(splits);
            assignSplit(Collections.singletonList(subtaskId));
        }
    }

    private void assignSplit(Collection<Integer> taskIdList) {
        Map<Integer, List<HudiSourceSplit>> readySplit = new HashMap<>(Common.COLLECTION_SIZE);
        for (int taskId : taskIdList) {
            readySplit.computeIfAbsent(taskId, id -> new ArrayList<>());
        }

        pendingSplit.forEach(s -> readySplit.get(getSplitOwner(s.splitId(), taskIdList.size())).add(s));
        readySplit.forEach(context::assignSplit);
        assignedSplit.addAll(pendingSplit);
        pendingSplit.clear();
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
        if (!pendingSplit.isEmpty()) {
            assignSplit(Collections.singletonList(subtaskId));
        }
    }

    @Override
    public HudiSourceState snapshotState(long checkpointId) {
        return new HudiSourceState(assignedSplit);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {

    }

    @Override
    public void handleSplitRequest(int subtaskId) {

    }
}
