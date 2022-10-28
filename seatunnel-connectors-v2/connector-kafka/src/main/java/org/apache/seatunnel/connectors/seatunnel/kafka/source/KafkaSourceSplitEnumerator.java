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

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaSourceState;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public class KafkaSourceSplitEnumerator implements SourceSplitEnumerator<KafkaSourceSplit, KafkaSourceState> {

    private static final String CLIENT_ID_PREFIX = "seatunnel";

    private final ConsumerMetadata metadata;
    private final Context<KafkaSourceSplit> context;
    private AdminClient adminClient;

    private Set<KafkaSourceSplit> pendingSplit;
    private final Set<KafkaSourceSplit> assignedSplit;

    KafkaSourceSplitEnumerator(ConsumerMetadata metadata, Context<KafkaSourceSplit> context) {
        this.metadata = metadata;
        this.context = context;
        this.assignedSplit = new HashSet<>();
        this.pendingSplit = new HashSet<>();
    }

    KafkaSourceSplitEnumerator(ConsumerMetadata metadata, Context<KafkaSourceSplit> context,
                               KafkaSourceState sourceState) {
        this(metadata, context);
    }

    @Override
    public void open() {
        this.adminClient = initAdminClient(this.metadata.getProperties());
    }

    @Override
    public void run() throws ExecutionException, InterruptedException {
        getTopicInfo().forEach(split -> {
            if (!assignedSplit.contains(split)) {
                pendingSplit.add(split);
            }
        });
        assignSplit();
    }

    @Override
    public void close() throws IOException {
        if (this.adminClient != null) {
            adminClient.close();
        }
    }

    @Override
    public void addSplitsBack(List<KafkaSourceSplit> splits, int subtaskId) {
        if (!splits.isEmpty()) {
            pendingSplit.addAll(convertToNextSplit(splits));
            assignSplit();
        }
    }

    private Collection<? extends KafkaSourceSplit> convertToNextSplit(List<KafkaSourceSplit> splits) {
        try {
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> listOffsets =
                getKafkaPartitionLatestOffset(splits.stream().map(KafkaSourceSplit::getTopicPartition).collect(Collectors.toList()));
            splits.forEach(split -> {
                split.setStartOffset(split.getEndOffset() + 1);
                split.setEndOffset(listOffsets.get(split.getTopicPartition()).offset());
            });
            return splits;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplit.size();
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        // Do nothing because Kafka source push split.
    }

    @Override
    public void registerReader(int subtaskId) {
        if (!pendingSplit.isEmpty()) {
            assignSplit();
        }
    }

    @Override
    public KafkaSourceState snapshotState(long checkpointId) throws Exception {
        return new KafkaSourceState(assignedSplit);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // Do nothing
    }

    private AdminClient initAdminClient(Properties properties) {
        Properties props = new Properties(properties);
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.metadata.getBootstrapServers());
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, CLIENT_ID_PREFIX + "-enumerator-admin-client-" + this.hashCode());
        return AdminClient.create(props);
    }

    private Set<KafkaSourceSplit> getTopicInfo() throws ExecutionException, InterruptedException {
        Collection<String> topics;
        if (this.metadata.isPattern()) {
            Pattern pattern = Pattern.compile(this.metadata.getTopic());
            topics = this.adminClient.listTopics().names().get().stream()
                .filter(t -> pattern.matcher(t).matches()).collect(Collectors.toSet());
        } else {
            topics = Arrays.asList(this.metadata.getTopic().split(","));
        }
        log.info("Discovered topics: {}", topics);

        Collection<TopicPartition> partitions =
            adminClient.describeTopics(topics).all().get().values().stream().flatMap(t -> t.partitions().stream()
                .map(p -> new TopicPartition(t.name(), p.partition()))).collect(Collectors.toSet());
        return getKafkaPartitionLatestOffset(partitions).entrySet().stream().map(partition -> {
            KafkaSourceSplit split = new KafkaSourceSplit(partition.getKey());
            split.setEndOffset(partition.getValue().offset());
            return split;
        }).collect(Collectors.toSet());
    }

    private Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> getKafkaPartitionLatestOffset(Collection<TopicPartition> partitions) throws InterruptedException, ExecutionException {
        return adminClient.listOffsets(partitions.stream().collect(Collectors.toMap(p -> p, p -> OffsetSpec.latest())))
            .all().get();
    }

    private void assignSplit() {
        Map<Integer, List<KafkaSourceSplit>> readySplit = new HashMap<>(Common.COLLECTION_SIZE);
        for (int taskID = 0; taskID < context.currentParallelism(); taskID++) {
            readySplit.computeIfAbsent(taskID, id -> new ArrayList<>());
        }

        pendingSplit.forEach(s -> {
            if (!assignedSplit.contains(s)) {
                readySplit.get(getSplitOwner(s.getTopicPartition(), context.currentParallelism()))
                    .add(s);
            }
        });

        readySplit.forEach(context::assignSplit);

        assignedSplit.addAll(pendingSplit);
        pendingSplit.clear();
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    private static int getSplitOwner(TopicPartition tp, int numReaders) {
        int startIndex = ((tp.topic().hashCode() * 31) & 0x7FFFFFFF) % numReaders;
        return (startIndex + tp.partition()) % numReaders;
    }

}
