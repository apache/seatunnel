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

package org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.discoverer;

import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.topic.TopicPartition;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public interface PulsarDiscoverer extends Serializable {
    Set<TopicPartition> getSubscribedTopicPartitions(
        PulsarAdmin pulsarAdmin);

    static List<TopicPartition> toTopicPartitions(String topicName, int partitionSize) {
        if (partitionSize == PartitionedTopicMetadata.NON_PARTITIONED) {
            // For non-partitioned topic.
            return Collections.singletonList(new TopicPartition(topicName, -1));
        } else {
            return IntStream.range(0, partitionSize)
                .boxed()
                .map(partitionId -> new TopicPartition(topicName, partitionId))
                .collect(Collectors.toList());
        }
    }
}
