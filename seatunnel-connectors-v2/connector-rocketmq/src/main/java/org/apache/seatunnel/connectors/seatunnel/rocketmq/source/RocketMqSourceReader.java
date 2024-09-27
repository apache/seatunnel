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

package org.apache.seatunnel.connectors.seatunnel.rocketmq.source;

import org.apache.seatunnel.shade.com.google.common.collect.Maps;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.exception.RocketMqConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.exception.RocketMqConnectorException;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

@Slf4j
public class RocketMqSourceReader implements SourceReader<SeaTunnelRow, RocketMqSourceSplit> {

    private static final long THREAD_WAIT_TIME = 500L;

    private final Context context;
    private final ConsumerMetadata metadata;
    private final Set<RocketMqSourceSplit> sourceSplits;
    private final Map<Long, Map<MessageQueue, Long>> checkpointOffsets;
    private final Map<MessageQueue, RocketMqConsumerThread> consumerThreads;
    private final ExecutorService executorService;
    private final DeserializationSchema<SeaTunnelRow> deserializationSchema;

    private final LinkedBlockingQueue<RocketMqSourceSplit> pendingPartitionsQueue;

    private volatile boolean running = false;

    public RocketMqSourceReader(
            ConsumerMetadata metadata,
            DeserializationSchema<SeaTunnelRow> deserializationSchema,
            Context context) {
        this.metadata = metadata;
        this.context = context;
        this.sourceSplits = new HashSet<>();
        this.deserializationSchema = deserializationSchema;
        this.consumerThreads = new ConcurrentHashMap<>();
        this.checkpointOffsets = new ConcurrentHashMap<>();
        this.executorService =
                Executors.newCachedThreadPool(r -> new Thread(r, "RocketMq Source Data Consumer"));
        pendingPartitionsQueue = new LinkedBlockingQueue<>();
        // Set `rocketmq.client.logUseSlf4j` to `true` to avoid create many
        // `AsyncAppender-Dispatcher-Thread`
        System.setProperty("rocketmq.client.logUseSlf4j", "true");
    }

    @Override
    public void open() throws Exception {
        // No-op
    }

    @Override
    public void close() throws IOException {
        if (executorService != null) {
            executorService.shutdownNow();
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        if (!running) {
            Thread.sleep(THREAD_WAIT_TIME);
            return;
        }
        while (!pendingPartitionsQueue.isEmpty()) {
            sourceSplits.add(pendingPartitionsQueue.poll());
        }
        sourceSplits.forEach(
                sourceSplit ->
                        consumerThreads.computeIfAbsent(
                                sourceSplit.getMessageQueue(),
                                s -> {
                                    RocketMqConsumerThread thread =
                                            new RocketMqConsumerThread(metadata);
                                    executorService.submit(thread);
                                    return thread;
                                }));
        sourceSplits.forEach(
                sourceSplit -> {
                    CompletableFuture<Void> completableFuture = new CompletableFuture<>();
                    try {
                        RocketMqConsumerThread rocketMqConsumerThread =
                                consumerThreads.get(sourceSplit.getMessageQueue());
                        rocketMqConsumerThread
                                .getTasks()
                                .put(
                                        consumer -> {
                                            try {
                                                rocketMqConsumerThread.assign(sourceSplit);
                                                MessageQueue assignedMessageQueue =
                                                        sourceSplit.getMessageQueue();
                                                List<MessageExt> records =
                                                        consumer.poll(
                                                                metadata.getBaseConfig()
                                                                        .getPollTimeoutMillis());
                                                if (records.isEmpty()) {
                                                    log.warn(
                                                            "Rocketmq consumer can not pull data, split {}, start offset {}, end offset {}",
                                                            sourceSplit.getMessageQueue(),
                                                            sourceSplit.getStartOffset(),
                                                            sourceSplit.getEndOffset());
                                                }
                                                List<MessageExt> messages =
                                                        records.stream()
                                                                .filter(
                                                                        record ->
                                                                                isQueueMatch(
                                                                                        assignedMessageQueue,
                                                                                        record))
                                                                .collect(Collectors.toList());
                                                long lastOffset = -1;
                                                for (MessageExt record : messages) {
                                                    deserializationSchema.deserialize(
                                                            record.getBody(), output);
                                                    lastOffset = record.getQueueOffset();
                                                    if (Boundedness.BOUNDED.equals(
                                                                    context.getBoundedness())
                                                            && record.getQueueOffset()
                                                                    >= sourceSplit.getEndOffset()) {
                                                        break;
                                                    }
                                                }
                                                if (lastOffset >= 0) {
                                                    // set start offset for next poll cycleLife
                                                    sourceSplit.setStartOffset(lastOffset + 1);
                                                    rocketMqConsumerThread.markLastPolledOffset(
                                                            lastOffset);
                                                }
                                                if (lastOffset >= sourceSplit.getEndOffset()) {
                                                    // just for bounded mode
                                                    sourceSplit.setEndOffset(lastOffset);
                                                }
                                            } catch (Throwable e) {
                                                completableFuture.completeExceptionally(e);
                                            }
                                            completableFuture.complete(null);
                                        });
                    } catch (InterruptedException e) {
                        throw new RocketMqConnectorException(
                                RocketMqConnectorErrorCode.CONSUME_DATA_FAILED, e);
                    }
                    completableFuture.join();
                });

        if (Boundedness.BOUNDED.equals(context.getBoundedness())) {
            // signal to the source that we have reached the end of the data.
            context.signalNoMoreElement();
        }
    }

    private boolean isQueueMatch(MessageQueue assignedMessageQueue, MessageExt record) {
        return Objects.equals(assignedMessageQueue.getTopic(), record.getTopic())
                && Objects.equals(assignedMessageQueue.getBrokerName(), record.getBrokerName())
                && Objects.equals(assignedMessageQueue.getQueueId(), record.getQueueId());
    }

    @Override
    public List<RocketMqSourceSplit> snapshotState(long checkpointId) throws Exception {
        List<RocketMqSourceSplit> pendingSplit =
                sourceSplits.stream().map(RocketMqSourceSplit::copy).collect(Collectors.toList());
        Map<MessageQueue, Long> offsets =
                checkpointOffsets.computeIfAbsent(checkpointId, id -> Maps.newConcurrentMap());
        for (RocketMqSourceSplit split : pendingSplit) {
            offsets.put(split.getMessageQueue(), split.getStartOffset());
        }
        return pendingSplit;
    }

    @Override
    public void addSplits(List<RocketMqSourceSplit> splits) {
        running = true;
        splits.forEach(
                s -> {
                    try {
                        pendingPartitionsQueue.put(s);
                    } catch (InterruptedException e) {
                        throw new RocketMqConnectorException(
                                RocketMqConnectorErrorCode.ADD_SPLIT_CHECKPOINT_FAILED, e);
                    }
                });
    }

    @Override
    public void handleNoMoreSplits() {
        // No-op
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        if (!checkpointOffsets.containsKey(checkpointId)) {
            log.warn("checkpoint {} do not exist or have already been committed.", checkpointId);
        } else {
            Map<MessageQueue, Long> messageQueueOffset = checkpointOffsets.remove(checkpointId);
            for (Map.Entry<MessageQueue, Long> entry : messageQueueOffset.entrySet()) {
                MessageQueue messageQueue = entry.getKey();
                Long offset = entry.getValue();
                try {
                    if (messageQueue != null && offset != null) {
                        RocketMqConsumerThread rocketMqConsumerThread =
                                consumerThreads.get(messageQueue);
                        if (rocketMqConsumerThread != null) {
                            rocketMqConsumerThread
                                    .getTasks()
                                    .put(
                                            consumer -> {
                                                if (this.metadata.isEnabledCommitCheckpoint()) {
                                                    consumer.getOffsetStore()
                                                            .updateOffset(
                                                                    messageQueue, offset, false);
                                                    consumer.getOffsetStore().persist(messageQueue);
                                                }
                                            });
                        }
                    }
                } catch (InterruptedException e) {
                    log.error("commit offset failed", e);
                }
            }
        }
    }
}
