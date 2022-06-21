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

package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.split.JdbcNumericBetweenParametersProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSourceState;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class JdbcSourceSplitEnumerator implements SourceSplitEnumerator<JdbcSourceSplit, JdbcSourceState> {

    Context<JdbcSourceSplit> enumeratorContext;
    List<JdbcSourceSplit> allSplit = new ArrayList<>();
    JdbcSourceOptions jdbcSourceOptions;
    PartitionParameter partitionParameter;

    public JdbcSourceSplitEnumerator(Context<JdbcSourceSplit> enumeratorContext, JdbcSourceOptions jdbcSourceOptions, PartitionParameter partitionParameter) {
        this.enumeratorContext = enumeratorContext;
        this.jdbcSourceOptions = jdbcSourceOptions;
        this.partitionParameter = partitionParameter;
    }

    @Override
    public void open() {
    }

    @Override
    public void run() throws Exception {
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void addSplitsBack(List<JdbcSourceSplit> splits, int subtaskId) {
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
                JdbcNumericBetweenParametersProvider jdbcNumericBetweenParametersProvider = new JdbcNumericBetweenParametersProvider(partitionParameter.minValue, partitionParameter.maxValue).ofBatchNum(parallelism);
                Serializable[][] parameterValues = jdbcNumericBetweenParametersProvider.getParameterValues();
                for (int i = 0; i < parameterValues.length; i++) {
                    allSplit.add(new JdbcSourceSplit(parameterValues[i], i));
                }
            } else {
                allSplit.add(new JdbcSourceSplit(null, 0));
            }
        }
        // Filter the split that the current task needs to run
        List<JdbcSourceSplit> splits = allSplit.stream().filter(p -> p.splitId % parallelism == subtaskId).collect(Collectors.toList());
        enumeratorContext.assignSplit(subtaskId, splits);
        enumeratorContext.signalNoMoreSplits(subtaskId);
    }

    @Override
    public JdbcSourceState snapshotState(long checkpointId) throws Exception {
        return null;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {

    }
}
