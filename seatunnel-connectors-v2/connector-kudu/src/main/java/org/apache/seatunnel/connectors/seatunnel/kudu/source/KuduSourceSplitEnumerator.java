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

package org.apache.seatunnel.connectors.seatunnel.kudu.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.kudu.state.KuduSourceState;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class KuduSourceSplitEnumerator implements SourceSplitEnumerator<KuduSourceSplit, KuduSourceState> {

    private final SourceSplitEnumerator.Context<KuduSourceSplit> enumeratorContext;
    private PartitionParameter partitionParameter;
    List<KuduSourceSplit> allSplit = new ArrayList<>();
    private Long maxVal;
    private Long minVal;
    private Long batchSize;
    private Integer batchNum;

    public KuduSourceSplitEnumerator(SourceSplitEnumerator.Context<KuduSourceSplit> enumeratorContext, PartitionParameter partitionParameter) {
        this.enumeratorContext = enumeratorContext;
        this.partitionParameter = partitionParameter;
    }

    @Override
    public void open() {

    }

    @Override
    public void run() {

    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void addSplitsBack(List<KuduSourceSplit> splits, int subtaskId) {

    }

    @Override
    public int currentUnassignedSplitSize() {
        return 0;
    }

    @Override
    public void handleSplitRequest(int subtaskId) {

    }

    @Override
    public void registerReader(int subtaskId) {
        int parallelism = enumeratorContext.currentParallelism();
        if (allSplit.isEmpty()) {
            if (null != partitionParameter) {
                Serializable[][] parameterValues = getParameterValues(partitionParameter.minValue, partitionParameter.maxValue, parallelism);
                for (int i = 0; i < parameterValues.length; i++) {
                    allSplit.add(new KuduSourceSplit(parameterValues[i], i));
                }
            } else {
                allSplit.add(new KuduSourceSplit(null, 0));
            }
        }
        // Filter the split that the current task needs to run
        List<KuduSourceSplit> splits = allSplit.stream().filter(p -> p.splitId % parallelism == subtaskId).collect(Collectors.toList());
        enumeratorContext.assignSplit(subtaskId, splits);
        enumeratorContext.signalNoMoreSplits(subtaskId);
    }

    private Serializable[][] getParameterValues(Long minVal, Long maxVal, int parallelism) {
        this.maxVal = maxVal;
        this.minVal = minVal;
        long maxElemCount = (maxVal - minVal) + 1;
        batchNum = parallelism;
        getBatchSizeAndBatchNum(parallelism);
        long bigBatchNum = maxElemCount - (batchSize - 1) * batchNum;

        Serializable[][] parameters = new Serializable[batchNum][2];
        long start = minVal;
        for (int i = 0; i < batchNum; i++) {
            long end = start + batchSize - 1 - (i >= bigBatchNum ? 1 : 0);
            parameters[i] = new Long[]{start, end};
            start = end + 1;
        }
        return parameters;

    }

    private void getBatchSizeAndBatchNum(int parallelism) {
        batchNum = parallelism;
        long maxElemCount = (maxVal - minVal) + 1;
        if (batchNum > maxElemCount) {
            batchNum = (int) maxElemCount;
        }
        this.batchNum = batchNum;
        this.batchSize = new Double(Math.ceil((double) maxElemCount / batchNum)).longValue();
    }

    @Override
    public KuduSourceState snapshotState(long checkpointId) throws Exception {
        return null;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {

    }
}
