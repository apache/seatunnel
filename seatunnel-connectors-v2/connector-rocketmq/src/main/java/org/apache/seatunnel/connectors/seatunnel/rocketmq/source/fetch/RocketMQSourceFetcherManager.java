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

package org.apache.seatunnel.connectors.seatunnel.rocketmq.source.fetch;

import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordsWithSplitIds;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.fetcher.SplitFetcher;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.fetcher.SplitFetcherTask;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.splitreader.SplitReader;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.source.OffsetCommitCallback;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.source.RocketMQPartitionSplit;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.source.RocketMQPartitionSplitReader;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;

public class RocketMQSourceFetcherManager
        extends SingleThreadFetcherManager<MessageExt, RocketMQPartitionSplit> {

    public RocketMQSourceFetcherManager(
            BlockingQueue<RecordsWithSplitIds<MessageExt>> elementsQueue,
            Supplier<SplitReader<MessageExt, RocketMQPartitionSplit>> splitReaderSupplier) {
        super(elementsQueue, splitReaderSupplier);
    }

    public void commitOffsets(
            Map<MessageQueue, Long> committedOffsets, OffsetCommitCallback callback) {
        if (committedOffsets.isEmpty()) {
            return;
        }

        SplitFetcher<MessageExt, RocketMQPartitionSplit> splitFetcher = fetchers.get(0);
        if (splitFetcher != null) {
            commit(splitFetcher, committedOffsets, callback);
        } else {
            splitFetcher = createSplitFetcher();
            commit(splitFetcher, committedOffsets, callback);
            startFetcher(splitFetcher);
        }
    }

    private void commit(
            SplitFetcher<MessageExt, RocketMQPartitionSplit> splitFetcher,
            Map<MessageQueue, Long> committedOffsets,
            OffsetCommitCallback callback) {
        RocketMQPartitionSplitReader rocketMQReader =
                (RocketMQPartitionSplitReader) splitFetcher.getSplitReader();

        splitFetcher.addTask(
                new SplitFetcherTask() {
                    @Override
                    public void run() {
                        rocketMQReader.notifyCheckpointComplete(committedOffsets, callback);
                    }

                    @Override
                    public void wakeUp() {}
                });
    }
}
