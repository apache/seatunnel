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
import static org.apache.seatunnel.connectors.seatunnel.iotdb.constant.SourceConstants.SQL_ALIGN;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.constant.SourceConstants.SQL_WHERE;
import static org.apache.iotdb.tsfile.common.constant.QueryConstant.RESERVED_TIME;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.iotdb.exception.IotdbConnectorException;
import org.apache.seatunnel.connectors.seatunnel.iotdb.state.IoTDBSourceState;

import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class IoTDBSourceSplitEnumerator implements SourceSplitEnumerator<IoTDBSourceSplit, IoTDBSourceState> {

    /**
     * A SQL statement can contain at most one where
     * We split the SQL using the where keyword
     * Therefore, it can be split into two SQL at most
     */
    private static final int SQL_WHERE_SPLIT_LENGTH = 2;

    private final Object stateLock = new Object();
    private final Context<IoTDBSourceSplit> context;
    private final Map<String, Object> conf;
    private final Map<Integer, List<IoTDBSourceSplit>> pendingSplit;
    private volatile boolean shouldEnumerate;

    public IoTDBSourceSplitEnumerator(SourceSplitEnumerator.Context<IoTDBSourceSplit> context,
                                      Map<String, Object> conf) {
        this(context, conf, null);
    }

    public IoTDBSourceSplitEnumerator(SourceSplitEnumerator.Context<IoTDBSourceSplit> context,
                                      Map<String, Object> conf,
                                      IoTDBSourceState sourceState) {
        this.context = context;
        this.conf = conf;
        this.pendingSplit = new HashMap<>();
        this.shouldEnumerate = sourceState == null;
        if (sourceState != null) {
            this.shouldEnumerate = sourceState.isShouldEnumerate();
            this.pendingSplit.putAll(sourceState.getPendingSplit());
        }
    }

    @Override
    public void open() {
    }

    @Override
    public void run() {
        Set<Integer> readers = context.registeredReaders();
        if (shouldEnumerate) {
            Set<IoTDBSourceSplit> newSplits = getIotDBSplit();

            synchronized (stateLock) {
                addPendingSplit(newSplits);
                shouldEnumerate = false;
            }

            assignSplit(readers);
        }

        log.debug("No more splits to assign." +
            " Sending NoMoreSplitsEvent to reader {}.", readers);
        readers.forEach(context::signalNoMoreSplits);
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
        String sql = conf.get(SQL.key()).toString();
        Set<IoTDBSourceSplit> iotDBSourceSplits = new HashSet<>();
        // no need numPartitions, use one partition
        if (!conf.containsKey(NUM_PARTITIONS.key())) {
            iotDBSourceSplits.add(new IoTDBSourceSplit(DEFAULT_PARTITIONS, sql));
            return iotDBSourceSplits;
        }
        long start = Long.parseLong(conf.get(LOWER_BOUND.key()).toString());
        long end = Long.parseLong(conf.get(UPPER_BOUND.key()).toString());
        int numPartitions = Integer.parseInt(conf.get(NUM_PARTITIONS.key()).toString());
        String sqlBase = sql;
        String sqlAlign = null;
        String sqlCondition = null;
        String[] sqls = sqlBase.split("(?i)" + SQL_ALIGN);
        if (sqls.length > 1) {
            sqlBase = sqls[0];
            sqlAlign = sqls[1];
        }
        sqls = sqlBase.split("(?i)" + SQL_WHERE);
        if (sqls.length > SQL_WHERE_SPLIT_LENGTH) {
            throw new IotdbConnectorException(CommonErrorCode.ILLEGAL_ARGUMENT,
                "sql should not contain more than one where");
        }
        if (sqls.length > 1) {
            sqlBase = sqls[0];
            sqlCondition = sqls[1];
        }
        long size = (end - start) / numPartitions + 1;
        long remainder = (end + 1 - start) % numPartitions;
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
            query = sqlBase + query;
            if (!Strings.isNullOrEmpty(sqlCondition)) {
                query = query + " and ( " + sqlCondition + " ) ";
            }
            if (!Strings.isNullOrEmpty(sqlAlign)) {
                query = query + " align by " + sqlAlign;
            }
            iotDBSourceSplits.add(new IoTDBSourceSplit(String.valueOf(query.hashCode()), query));
        }
        return iotDBSourceSplits;
    }

    @Override
    public void addSplitsBack(List<IoTDBSourceSplit> splits, int subtaskId) {
        log.debug("Add back splits {} to IoTDBSourceSplitEnumerator.",
            splits);
        if (!splits.isEmpty()) {
            addPendingSplit(splits);
            assignSplit(Collections.singletonList(subtaskId));
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplit.size();
    }

    @Override
    public void registerReader(int subtaskId) {
        log.debug("Register reader {} to IoTDBSourceSplitEnumerator.",
            subtaskId);
        if (!pendingSplit.isEmpty()) {
            assignSplit(Collections.singletonList(subtaskId));
        }
    }

    private void addPendingSplit(Collection<IoTDBSourceSplit> splits) {
        int readerCount = context.currentParallelism();
        for (IoTDBSourceSplit split : splits) {
            int ownerReader = getSplitOwner(split.splitId(), readerCount);
            log.info("Assigning {} to {} reader.", split, ownerReader);
            pendingSplit.computeIfAbsent(ownerReader, r -> new ArrayList<>())
                .add(split);
        }
    }

    private void assignSplit(Collection<Integer> readers) {
        log.debug("Assign pendingSplits to readers {}", readers);

        for (int reader : readers) {
            List<IoTDBSourceSplit> assignmentForReader = pendingSplit.remove(reader);
            if (assignmentForReader != null && !assignmentForReader.isEmpty()) {
                log.info("Assign splits {} to reader {}",
                    assignmentForReader, reader);
                try {
                    context.assignSplit(reader, assignmentForReader);
                } catch (Exception e) {
                    log.error("Failed to assign splits {} to reader {}",
                        assignmentForReader, reader, e);
                    pendingSplit.put(reader, assignmentForReader);
                }
            }
        }
    }

    @Override
    public IoTDBSourceState snapshotState(long checkpointId) throws Exception {
        synchronized (stateLock) {
            return new IoTDBSourceState(shouldEnumerate, pendingSplit);
        }
    }

    private static int getSplitOwner(String tp, int numReaders) {
        return (tp.hashCode() & Integer.MAX_VALUE) % numReaders;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        //nothing to do
    }

    @Override
    public void close() {
        //nothing to do
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        throw new IotdbConnectorException(CommonErrorCode.UNSUPPORTED_OPERATION,
            String.format("Unsupported handleSplitRequest: %d", subtaskId));
    }
}
