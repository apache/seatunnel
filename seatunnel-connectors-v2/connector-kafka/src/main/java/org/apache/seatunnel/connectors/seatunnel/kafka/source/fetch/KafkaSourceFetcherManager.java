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

package org.apache.seatunnel.connectors.seatunnel.kafka.source.fetch;

import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordsWithSplitIds;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.fetcher.SplitFetcher;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.fetcher.SplitFetcherTask;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.splitreader.SplitReader;
import org.apache.seatunnel.connectors.seatunnel.kafka.source.KafkaPartitionSplitReader;
import org.apache.seatunnel.connectors.seatunnel.kafka.source.KafkaSourceSplit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class KafkaSourceFetcherManager
        extends SingleThreadFetcherManager<ConsumerRecord<byte[], byte[]>, KafkaSourceSplit> {

    private static final Logger logger = LoggerFactory.getLogger(KafkaSourceFetcherManager.class);

    public KafkaSourceFetcherManager(
            BlockingQueue<RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>>> elementsQueue,
            Supplier<SplitReader<ConsumerRecord<byte[], byte[]>, KafkaSourceSplit>>
                    splitReaderSupplier) {
        super(elementsQueue, splitReaderSupplier);
    }

    public KafkaSourceFetcherManager(
            BlockingQueue<RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>>> elementsQueue,
            Supplier<SplitReader<ConsumerRecord<byte[], byte[]>, KafkaSourceSplit>>
                    splitReaderSupplier,
            Consumer<Collection<String>> splitFinishedHook) {
        super(elementsQueue, splitReaderSupplier, splitFinishedHook);
    }

    public void commitOffsets(
            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit, OffsetCommitCallback callback) {
        logger.debug("Committing offsets {}", offsetsToCommit);
        if (offsetsToCommit.isEmpty()) {
            return;
        }
        SplitFetcher<ConsumerRecord<byte[], byte[]>, KafkaSourceSplit> splitFetcher =
                fetchers.get(0);
        if (splitFetcher != null) {
            // The fetcher thread is still running. This should be the majority of the cases.
            enqueueOffsetsCommitTask(splitFetcher, offsetsToCommit, callback);
        } else {
            splitFetcher = createSplitFetcher();
            enqueueOffsetsCommitTask(splitFetcher, offsetsToCommit, callback);
            startFetcher(splitFetcher);
        }
    }

    private void enqueueOffsetsCommitTask(
            SplitFetcher<ConsumerRecord<byte[], byte[]>, KafkaSourceSplit> splitFetcher,
            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit,
            OffsetCommitCallback callback) {
        KafkaPartitionSplitReader kafkaReader =
                (KafkaPartitionSplitReader) splitFetcher.getSplitReader();

        splitFetcher.addTask(
                new SplitFetcherTask() {
                    @Override
                    public void run() throws IOException {
                        kafkaReader.notifyCheckpointComplete(offsetsToCommit, callback);
                    }

                    @Override
                    public void wakeUp() {}
                });
    }
}
