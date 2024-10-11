/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.sls.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.connectors.seatunnel.sls.config.StartMode;
import org.apache.seatunnel.connectors.seatunnel.sls.state.SlsSourceState;

import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.Consts;
import com.aliyun.openservices.log.common.ConsumerGroup;
import com.aliyun.openservices.log.common.ConsumerGroupShardCheckPoint;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.response.ConsumerGroupCheckPointResponse;
import com.aliyun.openservices.log.response.ListConsumerGroupResponse;
import com.aliyun.openservices.log.response.ListShardResponse;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
public class SlsSourceSplitEnumerator
        implements SourceSplitEnumerator<SlsSourceSplit, SlsSourceState> {

    private final Client slsCleint;
    private final ConsumerMetaData consumerMetaData;

    private final long discoveryIntervalMillis;

    private final Context<SlsSourceSplit> context;
    private final Map<Integer, SlsSourceSplit> pendingSplit;
    private final Map<Integer, SlsSourceSplit> assignedSplit;

    private SlsSourceState slsSourceState;

    private ScheduledExecutorService executor;
    private ScheduledFuture<?> scheduledFuture;

    public SlsSourceSplitEnumerator(
            SlsSourceConfig slsSourceConfig, Context<SlsSourceSplit> context) {
        this.context = context;
        this.slsCleint =
                new Client(
                        slsSourceConfig.getEndpoint(),
                        slsSourceConfig.getAccessKeyId(),
                        slsSourceConfig.getAccessKeySecret());
        this.assignedSplit = new HashMap<>();
        this.pendingSplit = new HashMap<>();
        this.consumerMetaData = slsSourceConfig.getConsumerMetaData();
        this.discoveryIntervalMillis = slsSourceConfig.getDiscoveryIntervalMillis();
    }

    public SlsSourceSplitEnumerator(
            SlsSourceConfig slsSourceConfig,
            Context<SlsSourceSplit> context,
            SlsSourceState slsSourceState) {
        this.context = context;
        this.slsCleint =
                new Client(
                        slsSourceConfig.getEndpoint(),
                        slsSourceConfig.getAccessKeyId(),
                        slsSourceConfig.getAccessKeySecret());
        this.assignedSplit = new HashMap<>();
        this.pendingSplit = new HashMap<>();
        this.consumerMetaData = slsSourceConfig.getConsumerMetaData();
        this.discoveryIntervalMillis = slsSourceConfig.getDiscoveryIntervalMillis();

        /** now only from sls cursor for restore */
        this.slsSourceState = slsSourceState;
        if (slsSourceState != null) {}
    }

    @Override
    public void open() {
        if (discoveryIntervalMillis > 0) {
            this.executor =
                    Executors.newScheduledThreadPool(
                            1,
                            runnable -> {
                                Thread thread = new Thread(runnable);
                                thread.setDaemon(true);
                                thread.setName("sls-shard-dynamic-discovery");
                                return thread;
                            });
            this.scheduledFuture =
                    executor.scheduleWithFixedDelay(
                            () -> {
                                try {
                                    discoverySplits();
                                } catch (Exception e) {
                                    log.error("Dynamic discovery failure:", e);
                                }
                            },
                            discoveryIntervalMillis,
                            discoveryIntervalMillis,
                            TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void run() throws Exception {
        fetchPendingShardSplit();
        assignSplit();
    }

    @Override
    public void close() throws IOException {}

    @Override
    public void addSplitsBack(List<SlsSourceSplit> splits, int subtaskId) {
        if (!splits.isEmpty()) {
            splits.forEach(split -> pendingSplit.put(split.getShardId(), split));
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        return 0;
    }

    @Override
    public void handleSplitRequest(int subtaskId) {}

    @Override
    public void registerReader(int subtaskId) {
        if (!pendingSplit.isEmpty()) {
            assignSplit();
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}

    private void discoverySplits() throws LogException {
        fetchPendingShardSplit();
        assignSplit();
    }

    private void fetchPendingShardSplit() throws LogException {
        String project = this.consumerMetaData.getProject();
        String logStore = this.consumerMetaData.getLogstore();
        String consumer = this.consumerMetaData.getConsumerGroup();
        StartMode startMode = this.consumerMetaData.getStartMode();
        int fetachSize = this.consumerMetaData.getFetchSize();
        Consts.CursorMode autoCursorReset = this.consumerMetaData.getAutoCursorReset();
        ListShardResponse shards = this.slsCleint.ListShard(project, logStore);
        shards.GetShards()
                .forEach(
                        shard -> {
                            if (!assignedSplit.containsKey(shard.getShardId())) {
                                if (!pendingSplit.containsKey(shard.getShardId())) {
                                    String cursor = "";
                                    try {
                                        cursor =
                                                initShardCursor(
                                                        project,
                                                        logStore,
                                                        consumer,
                                                        shard.getShardId(),
                                                        startMode,
                                                        autoCursorReset);
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                    if (cursor.equals("")) {
                                        throw new RuntimeException("shard cursor error");
                                    }
                                    SlsSourceSplit split =
                                            new SlsSourceSplit(
                                                    project,
                                                    logStore,
                                                    consumer,
                                                    shard.getShardId(),
                                                    cursor,
                                                    fetachSize);
                                    pendingSplit.put(shard.getShardId(), split);
                                }
                            }
                        });
    }

    private String initShardCursor(
            String project,
            String logStore,
            String consumer,
            int shardIdKey,
            StartMode cursorMode,
            Consts.CursorMode autoCursorReset)
            throws Exception {
        switch (cursorMode) {
            case EARLIEST:
                try {
                    return this.slsCleint
                            .GetCursor(project, logStore, shardIdKey, Consts.CursorMode.BEGIN)
                            .GetCursor();
                } catch (LogException e) {
                    throw new RuntimeException(e);
                }
            case LATEST:
                try {
                    return this.slsCleint
                            .GetCursor(project, logStore, shardIdKey, Consts.CursorMode.END)
                            .GetCursor();
                } catch (LogException e) {
                    throw new RuntimeException(e);
                }
            case GROUP_CURSOR:
                try {
                    boolean groupExists = checkConsumerGroupExists(project, logStore, consumer);
                    if (!groupExists) {
                        createConsumerGroup(project, logStore, consumer);
                    }
                    ConsumerGroupCheckPointResponse response =
                            this.slsCleint.GetCheckPoint(project, logStore, consumer, shardIdKey);
                    List<ConsumerGroupShardCheckPoint> checkpoints = response.getCheckPoints();
                    if (checkpoints.size() == 1) {
                        ConsumerGroupShardCheckPoint checkpoint = checkpoints.get(0);
                        if (!checkpoint.getCheckPoint().equals("")) {
                            return checkpoint.getCheckPoint();
                        }
                    }
                    return this.slsCleint
                            .GetCursor(project, logStore, shardIdKey, autoCursorReset)
                            .GetCursor();
                } catch (LogException e) {
                    if (e.GetErrorCode().equals("ConsumerGroupNotExist")) {
                        return this.slsCleint
                                .GetCursor(project, logStore, shardIdKey, autoCursorReset)
                                .GetCursor();
                    }
                    throw new RuntimeException(e);
                }
        }
        throw new RuntimeException(
                project + ":" + logStore + ":" + consumer + ":" + cursorMode + ":" + "fail");
    }

    private synchronized void assignSplit() {
        Map<Integer, List<SlsSourceSplit>> readySplit = new HashMap<>(Common.COLLECTION_SIZE);
        // init task from Parallelism
        for (int taskID = 0; taskID < context.currentParallelism(); taskID++) {
            readySplit.computeIfAbsent(taskID, id -> new ArrayList<>());
        }
        // Determine if split has been assigned
        pendingSplit.forEach(
                (key, value) -> {
                    if (!assignedSplit.containsKey(key)) {
                        readySplit
                                .get(
                                        getSplitOwner(
                                                value.getShardId(), context.currentParallelism()))
                                .add(value);
                    }
                });
        // assigned split
        readySplit.forEach(
                (id, split) -> {
                    context.assignSplit(id, split);
                    if (discoveryIntervalMillis <= 0) {
                        context.signalNoMoreSplits(id);
                    }
                });
        // record assigned split
        assignedSplit.putAll(pendingSplit);
        pendingSplit.clear();
    }

    private static int getSplitOwner(int shardId, int numReaders) {
        return shardId % numReaders;
    }

    @Override
    public SlsSourceState snapshotState(long checkpointId) throws Exception {
        return new SlsSourceState(new HashSet<>(assignedSplit.values()));
    }

    public boolean checkConsumerGroupExists(String project, String logstore, String consumerGroup)
            throws Exception {
        ListConsumerGroupResponse response = this.slsCleint.ListConsumerGroup(project, logstore);
        if (response != null) {
            for (ConsumerGroup item : response.GetConsumerGroups()) {
                if (item.getConsumerGroupName().equals(consumerGroup)) {
                    return true;
                }
            }
        }
        return false;
    }

    public void createConsumerGroup(
            final String project, final String logstore, final String consumerGroupName)
            throws LogException {
        ConsumerGroup consumerGroup = new ConsumerGroup(consumerGroupName, 100, false);
        try {
            this.slsCleint.CreateConsumerGroup(project, logstore, consumerGroup);
        } catch (LogException ex) {
            if ("ConsumerGroupAlreadyExist".equals(ex.GetErrorCode())) {}

            throw ex;
        }
    }
}
