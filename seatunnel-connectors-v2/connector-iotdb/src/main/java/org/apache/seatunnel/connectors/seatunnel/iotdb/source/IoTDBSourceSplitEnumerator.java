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

package org.apache.seatunnel.connectors.seatunnel.iotdb.source;

import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.SourceConfig.LOWER_BOUND;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.SourceConfig.NUM_PARTITIONS;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.SourceConfig.SQL;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.SourceConfig.UPPER_BOUND;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.constant.SourceConstants.DEFAULT_PARTITIONS;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.constant.SourceConstants.SQL_WHERE;
import static org.apache.iotdb.tsfile.common.constant.QueryConstant.RESERVED_TIME;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.connectors.seatunnel.iotdb.state.IoTDBSourceState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IoTDBSourceSplitEnumerator implements SourceSplitEnumerator<IoTDBSourceSplit, IoTDBSourceState> {

    private final Context<IoTDBSourceSplit> context;
    private Set<IoTDBSourceSplit> pendingSplit;
    private Set<IoTDBSourceSplit> assignedSplit;
    private Map<String, Object> conf;

    public IoTDBSourceSplitEnumerator(SourceSplitEnumerator.Context<IoTDBSourceSplit> context, Map<String, Object> conf) {
        this.context = context;
        this.conf = conf;
    }

    public IoTDBSourceSplitEnumerator(Context<IoTDBSourceSplit> context, IoTDBSourceState sourceState, Map<String, Object> conf) {
        this(context, conf);
        this.assignedSplit = sourceState.getAssignedSplit();
    }

    @Override
    public void open() {
        this.assignedSplit = new HashSet<>();
        this.pendingSplit = new HashSet<>();
    }

    @Override
    public void run() {
        pendingSplit = getIotDBSplit();
        assignSplit(context.registeredReaders());
    }

    /**
     * split the time range into numPartitions parts
     * if numPartitions is 1, use the whole time range
     * if numPartitions < (end - start), use (start-end) partitions
     * <p>
     * eg: start = 1, end = 10, numPartitions = 2
     * sql = "select * from test where age > 0 and age < 10"
     * <p>
     * split result
     * <p>
     * split 1: select * from test  where (time >= 1 and time < 6)  and (  age > 0 and age < 10 )
     * <p>
     * split 2: select * from test  where (time >= 6 and time < 11) and (  age > 0 and age < 10 )
     */
    private Set<IoTDBSourceSplit> getIotDBSplit() {
        String sql = conf.get(SQL).toString();
        Set<IoTDBSourceSplit> iotDBSourceSplits = new HashSet<>();
        // no need numPartitions, use one partition
        if (!conf.containsKey(NUM_PARTITIONS)) {
            iotDBSourceSplits.add(new IoTDBSourceSplit(DEFAULT_PARTITIONS, sql));
            return iotDBSourceSplits;
        }
        long start = Long.parseLong(conf.get(LOWER_BOUND).toString());
        long end = Long.parseLong(conf.get(UPPER_BOUND).toString());
        int numPartitions = Integer.parseInt(conf.get(NUM_PARTITIONS).toString());
        String[] sqls = sql.split(SQL_WHERE);
        int size = (int) (end - start) / numPartitions + 1;
        int remainder = (int) ((end + 1 - start) % numPartitions);
        if (end - start < numPartitions) {
            numPartitions = (int) (end - start);
        }
        long currentStart = start;
        int i = 0;
        while (i < numPartitions) {
            String query = " where (" + RESERVED_TIME + " >= " + currentStart + " and " + RESERVED_TIME + " < " + (currentStart + size) + ") ";
            i++;
            currentStart += size;
            if (i + 1 <= numPartitions) {
                currentStart = currentStart - remainder;
            }
            query = sqls[0] + query;
            if (sqls.length > 1) {
                query = query + " and ( " + sqls[1] + " ) ";
            }
            iotDBSourceSplits.add(new IoTDBSourceSplit(String.valueOf(i + System.nanoTime()), query));
        }
        return iotDBSourceSplits;
    }

    @Override
    public void addSplitsBack(List<IoTDBSourceSplit> splits, int subtaskId) {
        if (!splits.isEmpty()) {
            pendingSplit.addAll(splits);
            assignSplit(Collections.singletonList(subtaskId));
        }
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

    private void assignSplit(Collection<Integer> taskIDList) {
        Map<Integer, List<IoTDBSourceSplit>> readySplit = new HashMap<>(Common.COLLECTION_SIZE);
        for (int taskID : taskIDList) {
            readySplit.computeIfAbsent(taskID, id -> new ArrayList<>());
        }
        pendingSplit.forEach(s -> readySplit.get(getSplitOwner(s.splitId(), taskIDList.size()))
                .add(s));
        readySplit.forEach(context::assignSplit);
        assignedSplit.addAll(pendingSplit);
        pendingSplit.clear();
    }

    @Override
    public IoTDBSourceState snapshotState(long checkpointId) throws Exception {
        return new IoTDBSourceState(assignedSplit);
    }

    private static int getSplitOwner(String tp, int numReaders) {
        return tp.hashCode() % numReaders;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        //nothing to do
    }

    @Override
    public void close() throws IOException {
        //nothing to do
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        //nothing to do
    }
}
