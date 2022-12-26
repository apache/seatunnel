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

package org.apache.seatunnel.connectors.seatunnel.kafka.source;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.kafka.exception.KafkaConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.kafka.exception.KafkaConnectorException;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

@Slf4j
public class KafkaSourceReader implements SourceReader<SeaTunnelRow, KafkaSourceSplit> {

    private static final long THREAD_WAIT_TIME = 500L;
    private static final long POLL_TIMEOUT = 10000L;

    private final SourceReader.Context context;
    private final ConsumerMetadata metadata;
    private final Set<KafkaSourceSplit> sourceSplits;
    private final Map<Long, Map<TopicPartition, Long>> checkpointOffsetMap;
    private final Map<TopicPartition, KafkaConsumerThread> consumerThreadMap;
    private final ExecutorService executorService;
    private final DeserializationSchema<SeaTunnelRow> deserializationSchema;

    private final LinkedBlockingQueue<KafkaSourceSplit> pendingPartitionsQueue;

    private volatile boolean running = false;

    KafkaSourceReader(ConsumerMetadata metadata,
                      DeserializationSchema<SeaTunnelRow> deserializationSchema,
                      SourceReader.Context context) {
        this.metadata = metadata;
        this.context = context;
        this.sourceSplits = new HashSet<>();
        this.deserializationSchema = deserializationSchema;
        this.consumerThreadMap = new ConcurrentHashMap<>();
        this.checkpointOffsetMap = new ConcurrentHashMap<>();
        this.executorService = Executors.newCachedThreadPool(
            r -> new Thread(r, "Kafka Source Data Consumer"));
        pendingPartitionsQueue = new LinkedBlockingQueue<>();
    }

    @Override
    public void open() {
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

        while (pendingPartitionsQueue.size() != 0) {
            sourceSplits.add(pendingPartitionsQueue.poll());
        }
        sourceSplits.forEach(sourceSplit -> consumerThreadMap.computeIfAbsent(sourceSplit.getTopicPartition(), s -> {
            KafkaConsumerThread thread = new KafkaConsumerThread(metadata);
            executorService.submit(thread);
            return thread;
        }));
        sourceSplits.forEach(sourceSplit -> {
            CompletableFuture<Void> completableFuture = new CompletableFuture<>();
            try {
                consumerThreadMap.get(sourceSplit.getTopicPartition()).getTasks().put(consumer -> {
                    try {
                        Set<TopicPartition> partitions = Sets.newHashSet(sourceSplit.getTopicPartition());
                        StringDeserializer stringDeserializer = new StringDeserializer();
                        stringDeserializer.configure(Maps.fromProperties(this.metadata.getProperties()), false);
                        consumer.assign(partitions);
                        if (sourceSplit.getStartOffset() >= 0) {
                            consumer.seek(sourceSplit.getTopicPartition(), sourceSplit.getStartOffset());
                        }
                        ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(POLL_TIMEOUT));
                        for (TopicPartition partition : partitions) {
                            List<ConsumerRecord<byte[], byte[]>> recordList = records.records(partition);
                            for (ConsumerRecord<byte[], byte[]> record : recordList) {

                                deserializationSchema.deserialize(record.value(), output);

                                if (Boundedness.BOUNDED.equals(context.getBoundedness()) &&
                                    record.offset() >= sourceSplit.getEndOffset()) {
                                    break;
                                }
                            }
                            long lastOffset = -1;
                            if (!recordList.isEmpty()) {
                                lastOffset = recordList.get(recordList.size() - 1).offset();
                                sourceSplit.setStartOffset(lastOffset + 1);
                            }

                            if (lastOffset >= sourceSplit.getEndOffset()) {
                                sourceSplit.setEndOffset(lastOffset);
                            }
                        }
                    } catch (Exception e) {
                        completableFuture.completeExceptionally(e);
                    }
                    completableFuture.complete(null);
                });
            } catch (InterruptedException e) {
                throw new KafkaConnectorException(KafkaConnectorErrorCode.CONSUME_DATA_FAILED, e);
            }
            completableFuture.join();
        });

        if (Boundedness.BOUNDED.equals(context.getBoundedness())) {
            // signal to the source that we have reached the end of the data.
            context.signalNoMoreElement();
        }
    }

    @Override
    public List<KafkaSourceSplit> snapshotState(long checkpointId) {
        checkpointOffsetMap.put(checkpointId, sourceSplits.stream()
            .collect(Collectors.toMap(KafkaSourceSplit::getTopicPartition, KafkaSourceSplit::getEndOffset)));
        return sourceSplits.stream().map(KafkaSourceSplit::copy).collect(Collectors.toList());
    }

    @Override
    public void addSplits(List<KafkaSourceSplit> splits) {
        running = true;
        splits.forEach(s -> {
            try {
                pendingPartitionsQueue.put(s);
            } catch (InterruptedException e) {
                throw new KafkaConnectorException(KafkaConnectorErrorCode.ADD_SPLIT_CHECKPOINT_FAILED, e);
            }
        });
    }

    @Override
    public void handleNoMoreSplits() {
        log.info("receive no more splits message, this reader will not add new split.");
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        if (!checkpointOffsetMap.containsKey(checkpointId)) {
            log.warn("checkpoint {} do not exist or have already been committed.", checkpointId);
        } else {
            checkpointOffsetMap.remove(checkpointId).forEach((topicPartition, offset) -> {
                try {
                    consumerThreadMap.get(topicPartition)
                        .getTasks().put(consumer -> {
                            if (this.metadata.isCommitOnCheckpoint()) {
                                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                                offsets.put(topicPartition, new OffsetAndMetadata(offset));
                                consumer.commitSync(offsets);
                            }
                        });
                } catch (InterruptedException e) {
                    log.error("commit offset to kafka failed", e);
                }
            });
        }
    }

}
