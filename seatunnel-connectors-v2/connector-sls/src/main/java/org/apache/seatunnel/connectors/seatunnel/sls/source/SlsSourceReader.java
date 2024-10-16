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

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.sls.serialization.FastLogDeserialization;

import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.request.PullLogsRequest;
import com.aliyun.openservices.log.response.PullLogsResponse;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

@Slf4j
public class SlsSourceReader implements SourceReader<SeaTunnelRow, SlsSourceSplit> {
    private static final long THREAD_WAIT_TIME = 500L;
    private final SourceReader.Context context;
    private volatile boolean running = false;
    private final LinkedBlockingQueue<SlsSourceSplit> pendingShardsQueue;
    private final Set<SlsSourceSplit> sourceSplits;
    private final Map<String, SlsConsumerThread> consumerThreadMap;
    private final SlsSourceConfig slsSourceConfig;
    private final ExecutorService executorService;

    private final Map<Long, Map<String, SlsSourceSplit>> checkpointOffsetMap;

    SlsSourceReader(SlsSourceConfig slsSourceConfig, Context context) {
        this.pendingShardsQueue = new LinkedBlockingQueue();
        this.sourceSplits = new HashSet<>();
        this.consumerThreadMap = new ConcurrentHashMap<>();
        this.slsSourceConfig = slsSourceConfig;
        this.context = context;
        this.executorService =
                Executors.newCachedThreadPool(r -> new Thread(r, "Sls Source Data Consumer"));
        this.checkpointOffsetMap = new ConcurrentHashMap<>();
    }

    @Override
    public void open() throws Exception {}

    @Override
    public void close() throws IOException {
        if (executorService != null) {
            executorService.shutdownNow();
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> collector) throws Exception {
        if (!running) {
            Thread.sleep(THREAD_WAIT_TIME);
            return;
        }

        while (!pendingShardsQueue.isEmpty()) {
            sourceSplits.add(pendingShardsQueue.poll());
        }
        /** thread for Client */
        sourceSplits.forEach(
                sourceSplit ->
                        consumerThreadMap.computeIfAbsent(
                                sourceSplit.splitId(),
                                s -> {
                                    SlsConsumerThread thread =
                                            new SlsConsumerThread(slsSourceConfig);
                                    executorService.submit(thread);
                                    return thread;
                                }));
        List<SlsSourceSplit> finishedSplits = new CopyOnWriteArrayList<>();
        FastLogDeserialization fastLogDeserialization =
                slsSourceConfig.getConsumerMetaData().getDeserializationSchema();
        sourceSplits.forEach(
                sourceSplit -> {
                    CompletableFuture<Boolean> completableFuture = new CompletableFuture<>();
                    try {
                        consumerThreadMap
                                .get(sourceSplit.splitId())
                                .getTasks()
                                .put(
                                        consumer -> {
                                            try {
                                                PullLogsRequest request =
                                                        new PullLogsRequest(
                                                                sourceSplit.getProject(),
                                                                sourceSplit.getLogStore(),
                                                                sourceSplit.getShardId(),
                                                                sourceSplit.getFetchSize(),
                                                                sourceSplit.getStartCursor());
                                                PullLogsResponse response =
                                                        consumer.pullLogs(request);
                                                List<LogGroupData> logGroupDatas =
                                                        response.getLogGroups();
                                                fastLogDeserialization.deserialize(
                                                        logGroupDatas, collector);
                                                sourceSplit.setStartCursor(
                                                        response.getNextCursor());
                                                completableFuture.complete(true);
                                            } catch (Throwable e) {
                                                log.error("pull logs failed", e);
                                                completableFuture.completeExceptionally(e);
                                                throw new RuntimeException(e);
                                            }
                                            completableFuture.complete(false);
                                        });
                        if (completableFuture.get()) {
                            finishedSplits.add(sourceSplit);
                        }
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                });

        // batch mode only for explore data, so do not update cursor
        if (Boundedness.BOUNDED.equals(context.getBoundedness())) {
            for (SlsSourceSplit split : finishedSplits) {
                split.setFinish(true);
            }
            if (sourceSplits.stream().allMatch(SlsSourceSplit::isFinish)) {
                log.info("sls batch mode finished");
                context.signalNoMoreElement();
            }
        }
    }

    @Override
    public List<SlsSourceSplit> snapshotState(long checkpointId) throws Exception {
        checkpointOffsetMap.put(
                checkpointId,
                sourceSplits.stream()
                        .collect(Collectors.toMap(SlsSourceSplit::splitId, SlsSourceSplit::copy)));
        return sourceSplits.stream().map(SlsSourceSplit::copy).collect(Collectors.toList());
    }

    // received splits and do somethins for this
    @Override
    public void addSplits(List<SlsSourceSplit> splits) {
        running = true;
        splits.forEach(
                s -> {
                    try {
                        pendingShardsQueue.put(s);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @Override
    public void handleNoMoreSplits() {
        log.info("receive no more splits message, this reader will not add new split.");
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        if (!checkpointOffsetMap.containsKey(checkpointId)) {
            log.warn("checkpoint {} do not exist or have already been committed.", checkpointId);
        } else {
            checkpointOffsetMap
                    .remove(checkpointId)
                    .forEach(
                            (sharId, slsSourceSplit) -> {
                                try {
                                    consumerThreadMap
                                            .get(sharId)
                                            .getTasks()
                                            .put(
                                                    client -> {
                                                        // now only default onCheckpointCommit
                                                        try {
                                                            client.UpdateCheckPoint(
                                                                    slsSourceSplit.getProject(),
                                                                    slsSourceSplit.getLogStore(),
                                                                    slsSourceSplit.getConsumer(),
                                                                    slsSourceSplit.getShardId(),
                                                                    slsSourceSplit
                                                                            .getStartCursor());
                                                        } catch (LogException e) {
                                                            log.error(
                                                                    "LogException: commit cursor to sls failed",
                                                                    e);
                                                            throw new RuntimeException(e);
                                                        }
                                                    });
                                } catch (InterruptedException e) {
                                    log.error(
                                            "InterruptedException: commit cursor to sls failed", e);
                                }
                            });
        }
    }
}
