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

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class KafkaSourceSplitEnumerator implements SourceSplitEnumerator<KafkaSourceSplit, KafkaSourceState> {

    private static final String CLIENT_ID_PREFIX = "seatunnel";

    private final ConsumerMetadata metadata;
    private final Context<KafkaSourceSplit> context;
    private AdminClient adminClient;

    private Set<TopicPartition> pendingSplit;
    private Set<TopicPartition> assignedSplit;

    KafkaSourceSplitEnumerator(ConsumerMetadata metadata, Context<KafkaSourceSplit> context) {
        this.metadata = metadata;
        this.context = context;
    }

    KafkaSourceSplitEnumerator(ConsumerMetadata metadata, Context<KafkaSourceSplit> context,
                               KafkaSourceState sourceState) {
        this(metadata, context);
        this.assignedSplit = sourceState.getAssignedSplit();
    }

    @Override
    public void open() {
        this.adminClient = initAdminClient(this.metadata.getProperties());
        this.assignedSplit = new HashSet<>();
        this.pendingSplit = new HashSet<>();
    }

    @Override
    public void run() throws ExecutionException, InterruptedException {
        pendingSplit = getTopicInfo().stream().flatMap(t -> t.partitions().stream()
                .map(p -> new TopicPartition(t.name(), p.partition()))).collect(Collectors.toSet());
        assignSplit(context.registeredReaders());
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
            pendingSplit.addAll(splits.stream().map(KafkaSourceSplit::getTopicPartition).collect(Collectors.toList()));
            assignSplit(Collections.singletonList(subtaskId));
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
            assignSplit(Collections.singletonList(subtaskId));
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
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.metadata.getBootstrapServer());
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, CLIENT_ID_PREFIX + "-enumerator-admin-client");
        return AdminClient.create(props);
    }

    private List<TopicDescription> getTopicInfo() throws ExecutionException, InterruptedException {
        Collection<String> topics;
        if (this.metadata.isPattern()) {
            Pattern pattern = Pattern.compile(this.metadata.getTopic());
            topics = this.adminClient.listTopics().names().get().stream()
                    .filter(t -> pattern.matcher(t).matches()).collect(Collectors.toSet());
        } else {
            topics = Arrays.asList(this.metadata.getTopic().split(","));
        }
        return new ArrayList<>(adminClient.describeTopics(topics).allTopicNames().get().values());
    }

    private void assignSplit(Collection<Integer> taskIDList) {
        Map<Integer, List<KafkaSourceSplit>> readySplit = new HashMap<>(Common.COLLECTION_SIZE);
        for (int taskID : taskIDList) {
            readySplit.computeIfAbsent(taskID, id -> new ArrayList<>());
        }
        pendingSplit.forEach(s -> readySplit.get(getSplitOwner(s, taskIDList.size())).add(new KafkaSourceSplit(s)));

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
