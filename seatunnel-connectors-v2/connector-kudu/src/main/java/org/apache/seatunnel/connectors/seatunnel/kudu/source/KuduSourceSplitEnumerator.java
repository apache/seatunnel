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
import org.apache.seatunnel.connectors.seatunnel.kudu.utils.KuduColumn;

import org.apache.kudu.Type;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class KuduSourceSplitEnumerator
        implements SourceSplitEnumerator<KuduSourceSplit, KuduSourceState> {

    private final Context<KuduSourceSplit> enumeratorContext;
    private final PartitionParameter partitionParameter;
    List<KuduSourceSplit> allSplit = new ArrayList<>();

    public KuduSourceSplitEnumerator(
            Context<KuduSourceSplit> enumeratorContext, PartitionParameter partitionParameter) {
        this.enumeratorContext = enumeratorContext;
        this.partitionParameter = partitionParameter;
    }

    @Override
    public void open() {}

    @Override
    public void run() {}

    @Override
    public void close() throws IOException {}

    @Override
    public void addSplitsBack(List<KuduSourceSplit> splits, int subtaskId) {}

    @Override
    public int currentUnassignedSplitSize() {
        return 0;
    }

    @Override
    public void handleSplitRequest(int subtaskId) {}

    @Override
    public void registerReader(int subtaskId) {
        int parallelism = enumeratorContext.currentParallelism();
        if (allSplit.isEmpty()) {
            if (null != partitionParameter) {
                Serializable[][] parameterValues =
                        getParameterValues(
                                partitionParameter.rangeValue,
                                partitionParameter.getPartitionColumnType(),
                                parallelism);
                for (int i = 0; i < parameterValues.length; i++) {
                    allSplit.add(new KuduSourceSplit(parameterValues[i], i));
                }
            } else {
                allSplit.add(new KuduSourceSplit(null, 0));
            }
        }
        // Filter the split that the current task needs to run
        List<KuduSourceSplit> splits =
                allSplit.stream()
                        .filter(p -> p.splitId % parallelism == subtaskId)
                        .collect(Collectors.toList());
        enumeratorContext.assignSplit(subtaskId, splits);
        enumeratorContext.signalNoMoreSplits(subtaskId);
    }

    private Serializable[][] getParameterValues(
            ArrayList<Object> rangeValue, Type type, int parallelism) {
        int bucketNum = rangeValue.size() - 1;
        int batchNum = Math.min(parallelism, bucketNum);
        Serializable[][] parameters = new Serializable[batchNum][2];
        int intervalNum = bucketNum / batchNum;
        int remainNum = bucketNum % batchNum;
        Object start = rangeValue.get(0);
        int index = 0;
        for (int i = 0; i < bucketNum; ) {
            if (remainNum > 0) {
                i += 1;
                remainNum -= 1;
            }
            i = i + intervalNum;
            Object end = rangeValue.get(i);
            parameters[index++] = KuduColumn.getSerializable(type, start, end);
            start = end;
        }
        return parameters;
    }

    @Override
    public KuduSourceState snapshotState(long checkpointId) throws Exception {
        return null;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
