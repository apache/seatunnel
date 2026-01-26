/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.hbase.source;

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.hbase.client.HbaseClient;
import org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseParameters;
import org.apache.seatunnel.connectors.seatunnel.hbase.exception.HbaseConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hbase.exception.HbaseConnectorException;
import org.apache.seatunnel.connectors.seatunnel.hbase.util.HBaseUtil;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.util.Bytes;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class HbaseSourceSplitEnumerator
        implements SourceSplitEnumerator<HbaseSourceSplit, HbaseSourceState> {
    /** Source split enumerator context */
    private final Context<HbaseSourceSplit> context;

    /** The splits that has assigned */
    private final Set<HbaseSourceSplit> assignedSplit;

    /** The splits that have not assigned */
    private Set<HbaseSourceSplit> pendingSplit;

    /** Whether the pending splits have been initialized */
    private boolean initialized = false;

    private HbaseParameters hbaseParameters;

    private HbaseClient hbaseClient;

    public HbaseSourceSplitEnumerator(
            Context<HbaseSourceSplit> context, HbaseParameters hbaseParameters) {
        this(context, hbaseParameters, new HashSet<>(), null);
    }

    public HbaseSourceSplitEnumerator(
            Context<HbaseSourceSplit> context,
            HbaseParameters hbaseParameters,
            HbaseSourceState sourceState) {
        this(context, hbaseParameters, sourceState.getAssignedSplits(), null);
    }

    @VisibleForTesting
    public HbaseSourceSplitEnumerator(
            Context<HbaseSourceSplit> context,
            HbaseParameters hbaseParameters,
            HbaseClient hbaseClient) {
        this(context, hbaseParameters, new HashSet<>(), hbaseClient);
    }

    @VisibleForTesting
    public HbaseSourceSplitEnumerator(
            Context<HbaseSourceSplit> context,
            HbaseParameters hbaseParameters,
            HbaseSourceState sourceState,
            HbaseClient hbaseClient) {
        this(context, hbaseParameters, sourceState.getAssignedSplits(), hbaseClient);
    }

    private HbaseSourceSplitEnumerator(
            Context<HbaseSourceSplit> context,
            HbaseParameters hbaseParameters,
            Set<HbaseSourceSplit> assignedSplit) {
        this(context, hbaseParameters, assignedSplit, null);
    }

    private HbaseSourceSplitEnumerator(
            Context<HbaseSourceSplit> context,
            HbaseParameters hbaseParameters,
            Set<HbaseSourceSplit> assignedSplit,
            HbaseClient hbaseClient) {
        this.context = context;
        this.hbaseParameters = hbaseParameters;
        this.assignedSplit = assignedSplit;
        this.hbaseClient = hbaseClient;
    }

    @Override
    public void open() {
        this.pendingSplit = new HashSet<>();
        this.initialized = false;
    }

    @Override
    public void run() throws Exception {
        // do nothing
    }

    @Override
    public void close() throws IOException {
        if (this.hbaseClient != null) {
            try {
                this.hbaseClient.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void addSplitsBack(List<HbaseSourceSplit> splits, int subtaskId) {
        if (!splits.isEmpty()) {
            pendingSplit.addAll(splits);
            if (context.registeredReaders().contains(subtaskId)) {
                assignSplit(subtaskId);
            }
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplit.size();
    }

    @Override
    public void registerReader(int subtaskId) {
        initializePendingSplits();
        assignSplit(subtaskId);
    }

    private void initializePendingSplits() {
        if (initialized) {
            return;
        }
        Set<HbaseSourceSplit> tableSplits = getTableSplits();
        Set<String> existedSplitIds =
                pendingSplit.stream().map(HbaseSourceSplit::splitId).collect(Collectors.toSet());
        if (!assignedSplit.isEmpty()) {
            existedSplitIds.addAll(
                    assignedSplit.stream()
                            .map(HbaseSourceSplit::splitId)
                            .collect(Collectors.toSet()));
        }
        pendingSplit.addAll(
                tableSplits.stream()
                        .filter(split -> !existedSplitIds.contains(split.splitId()))
                        .collect(Collectors.toSet()));
        initialized = true;
    }

    @Override
    public HbaseSourceState snapshotState(long checkpointId) throws Exception {
        return new HbaseSourceState(assignedSplit);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // do nothing
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        // do nothing
    }

    /** Assign split by reader task id */
    private void assignSplit(int taskId) {
        ArrayList<HbaseSourceSplit> currentTaskSplits = new ArrayList<>();
        if (context.currentParallelism() == 1) {
            // if parallelism == 1, we should assign all the splits to reader
            currentTaskSplits.addAll(pendingSplit);
        } else {
            // if parallelism > 1, according to hashCode of split's id to determine whether to
            // allocate the current task
            for (HbaseSourceSplit sourceSplit : pendingSplit) {
                final int splitOwner =
                        getSplitOwner(sourceSplit.splitId(), context.currentParallelism());
                if (splitOwner == taskId) {
                    currentTaskSplits.add(sourceSplit);
                }
            }
        }
        // assign splits
        context.assignSplit(taskId, currentTaskSplits);
        // save the state of assigned splits
        assignedSplit.addAll(currentTaskSplits);
        // remove the assigned splits from pending splits
        currentTaskSplits.forEach(split -> pendingSplit.remove(split));
        log.info(
                "SubTask {} is assigned to [{}]",
                taskId,
                currentTaskSplits.stream()
                        .map(HbaseSourceSplit::splitId)
                        .collect(Collectors.joining(",")));
        context.signalNoMoreSplits(taskId);
    }

    @VisibleForTesting
    public Set<HbaseSourceSplit> getTableSplits() {
        String namespace = hbaseParameters.getNamespace();
        TableName tableName = TableName.valueOf(namespace, hbaseParameters.getTable());
        try {
            HbaseClient hbaseClient = getHbaseClient();
            log.info("Enumerating HBase source splits for table [{}]", tableName.getNameAsString());
            if (!hbaseClient.tableExists(tableName.getNameAsString())) {
                String errorMsg =
                        String.format(
                                "HBase table [%s] does not exist", tableName.getNameAsString());
                log.error(errorMsg);
                throw new HbaseConnectorException(
                        HbaseConnectorErrorCode.TABLE_QUERY_EXCEPTION, errorMsg);
            }

            try (RegionLocator regionLocator =
                    hbaseClient.getRegionLocator(namespace, hbaseParameters.getTable())) {
                byte[][] startKeys = regionLocator.getStartKeys();
                byte[][] endKeys = regionLocator.getEndKeys();
                if (startKeys.length == 0 || endKeys.length == 0) {
                    String errorMsg =
                            String.format(
                                    "No region information found for HBase table [%s], please check whether the table exists "
                                            + "and current user has permission to access it",
                                    tableName.getNameAsString());
                    log.error(errorMsg);
                    throw new HbaseConnectorException(
                            HbaseConnectorErrorCode.TABLE_QUERY_EXCEPTION, errorMsg);
                }
                List<HbaseSourceSplit> splits = new ArrayList<>();
                boolean isBinaryRowkey = hbaseParameters.isBinaryRowkey();
                byte[] userStartRowkey =
                        HBaseUtil.convertRowKey(hbaseParameters.getStartRowkey(), isBinaryRowkey);
                byte[] userEndRowkey =
                        HBaseUtil.convertRowKey(hbaseParameters.getEndRowkey(), isBinaryRowkey);
                HBaseUtil.validateRowKeyRange(userStartRowkey, userEndRowkey);

                int i = 0;
                while (i < startKeys.length) {
                    byte[] regionStartKey = startKeys[i];
                    byte[] regionEndKey = endKeys[i];
                    if (userEndRowkey.length > 0
                            && Bytes.compareTo(userEndRowkey, regionStartKey) <= 0
                            && Bytes.compareTo(regionStartKey, HConstants.EMPTY_BYTE_ARRAY) != 0) {
                        i++;
                        continue;
                    }

                    if (userStartRowkey.length > 0
                            && Bytes.compareTo(userStartRowkey, regionEndKey) >= 0
                            && Bytes.compareTo(regionEndKey, HConstants.EMPTY_BYTE_ARRAY) != 0) {
                        i++;
                        continue;
                    }
                    byte[] splitStartKey =
                            userStartRowkey.length > 0
                                            && (Bytes.compareTo(
                                                                    regionStartKey,
                                                                    HConstants.EMPTY_BYTE_ARRAY)
                                                            == 0
                                                    || Bytes.compareTo(
                                                                    userStartRowkey, regionStartKey)
                                                            > 0)
                                    ? userStartRowkey
                                    : regionStartKey;

                    byte[] splitEndKey =
                            userEndRowkey.length > 0
                                            && (Bytes.compareTo(
                                                                    regionEndKey,
                                                                    HConstants.EMPTY_BYTE_ARRAY)
                                                            == 0
                                                    || Bytes.compareTo(userEndRowkey, regionEndKey)
                                                            < 0)
                                    ? userEndRowkey
                                    : regionEndKey;

                    splits.add(new HbaseSourceSplit(i, splitStartKey, splitEndKey));
                    i++;
                }
                return new HashSet<>(splits);
            }
        } catch (IOException e) {
            String errorMsg =
                    String.format(
                            "Failed to enumerate splits for HBase table [%s]",
                            tableName.getNameAsString());
            log.error(errorMsg, e);
            throw new HbaseConnectorException(
                    HbaseConnectorErrorCode.TABLE_QUERY_EXCEPTION, errorMsg, e);
        }
    }

    private synchronized HbaseClient getHbaseClient() {
        if (hbaseClient == null) {
            hbaseClient = HbaseClient.createInstance(hbaseParameters);
        }
        return hbaseClient;
    }

    /** Hash algorithm for assigning splits to readers */
    private static int getSplitOwner(String tp, int numReaders) {
        return (tp.hashCode() & Integer.MAX_VALUE) % numReaders;
    }
}
