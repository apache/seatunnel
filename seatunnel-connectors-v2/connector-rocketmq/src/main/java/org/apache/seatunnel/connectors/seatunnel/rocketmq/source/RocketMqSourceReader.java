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

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordEmitter;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordsWithSplitIds;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.SourceReaderOptions;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.source.fetch.RocketMQSourceFetcherManager;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author 02211659 bianxiang
 * @date 2025-08-19 10:06:05
 */
public class RocketMqSourceReader
        extends SingleThreadMultiplexSourceReaderBase<
                MessageExt, SeaTunnelRow, RocketMQPartitionSplit, RocketMQPartitionSplitState> {

    private static final Logger logger = LoggerFactory.getLogger(RocketMqSourceReader.class);

    private final Context context;

    // These maps need to be concurrent because it will be accessed by both the main thread
    // and the split fetcher thread in the callback.
    private final SortedMap<Long, Map<MessageQueue, Long>> checkpointOffsetMap;
    private final ConcurrentMap<MessageQueue, Long> offsetsOfFinishedSplits;
    private final ConsumerMetadata metadata;

    public RocketMqSourceReader(
            BlockingQueue<RecordsWithSplitIds<MessageExt>> elementsQueue,
            SingleThreadFetcherManager<MessageExt, RocketMQPartitionSplit>
                    rocketMQSourceFetcherManager,
            RecordEmitter<MessageExt, SeaTunnelRow, RocketMQPartitionSplitState> recordEmitter,
            SourceReaderOptions options,
            ConsumerMetadata metadata,
            Context context) {
        super(elementsQueue, rocketMQSourceFetcherManager, recordEmitter, options, context);
        this.metadata = metadata;
        this.context = context;
        this.checkpointOffsetMap = Collections.synchronizedSortedMap(new TreeMap<>());
        this.offsetsOfFinishedSplits = new ConcurrentHashMap<>();
        // `AsyncAppender-Dispatcher-Thread`
        System.setProperty("rocketmq.client.logUseSlf4j", "true");
    }

    @Override
    protected void onSplitFinished(Map<String, RocketMQPartitionSplitState> finishedSplitIds) {
        finishedSplitIds.forEach(
                (ignored, splitState) -> {
                    if (splitState.getCurrentOffset() >= 0) {
                        offsetsOfFinishedSplits.put(
                                splitState.getMessageQueue(), splitState.getCurrentOffset());
                    } else if (splitState.getEndOffset() >= 0) {
                        offsetsOfFinishedSplits.put(
                                splitState.getMessageQueue(), splitState.getEndOffset());
                    }
                });
    }

    @Override
    protected RocketMQPartitionSplitState initializedState(RocketMQPartitionSplit split) {
        return new RocketMQPartitionSplitState(split);
    }

    @Override
    protected RocketMQPartitionSplit toSplitType(
            String splitId, RocketMQPartitionSplitState splitState) {
        return splitState.toRocketMQPartitionSplit();
    }

    @Override
    public void handleNoMoreSplits() {
        // No-op
    }

    @Override
    public List<RocketMQPartitionSplit> snapshotState(long checkpointId) {
        List<RocketMQPartitionSplit> sourceSplits = super.snapshotState(checkpointId);
        if (!this.metadata.isEnabledCommitCheckpoint()) {
            return sourceSplits;
        }
        if (sourceSplits.isEmpty() && offsetsOfFinishedSplits.isEmpty()) {
            logger.debug(
                    "checkpoint {} does not have an offset to submit for splits", checkpointId);
            checkpointOffsetMap.put(checkpointId, Collections.emptyMap());
        } else {
            Map<MessageQueue, Long> offsetAndMetadataMap =
                    checkpointOffsetMap.computeIfAbsent(checkpointId, id -> new HashMap<>());
            for (RocketMQPartitionSplit rocketMQPartitionSplit : sourceSplits) {
                if (rocketMQPartitionSplit.getStartOffset() >= 0) {
                    offsetAndMetadataMap.put(
                            rocketMQPartitionSplit.getMessageQueue(),
                            rocketMQPartitionSplit.getStartOffset());
                }
            }
            offsetAndMetadataMap.putAll(offsetsOfFinishedSplits);
        }
        return sourceSplits;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        logger.debug("Committing offsets for checkpoint {}", checkpointId);
        if (!this.metadata.isEnabledCommitCheckpoint()) {
            logger.debug("Submitting offsets after snapshot completion is prohibited");
            return;
        }
        Map<MessageQueue, Long> committedOffsets = checkpointOffsetMap.get(checkpointId);
        logger.info(
                "Committing offsets for checkpoint {}", JsonUtils.toJsonString(committedOffsets));
        if (committedOffsets == null) {
            logger.debug("Offsets for checkpoint {} have already been committed.", checkpointId);
            return;
        }

        if (committedOffsets.isEmpty()) {
            logger.debug("There are no offsets to commit for checkpoint {}.", checkpointId);
            removeAllOffsetsToCommitUpToCheckpoint(checkpointId);
            return;
        }

        ((RocketMQSourceFetcherManager) splitFetcherManager)
                .commitOffsets(
                        committedOffsets,
                        () -> {
                            offsetsOfFinishedSplits
                                    .keySet()
                                    .removeIf(committedOffsets::containsKey);
                            removeAllOffsetsToCommitUpToCheckpoint(checkpointId);
                        });
    }

    private void removeAllOffsetsToCommitUpToCheckpoint(long checkpointId) {
        while (!checkpointOffsetMap.isEmpty() && checkpointOffsetMap.firstKey() <= checkpointId) {
            checkpointOffsetMap.remove(checkpointOffsetMap.firstKey());
        }
    }
}
