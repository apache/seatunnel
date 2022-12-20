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

import org.apache.seatunnel.connectors.seatunnel.pulsar.exception.PulsarConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.pulsar.exception.PulsarConnectorException;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.topic.TopicPartition;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * the implements of consuming multiple topics.
 */
public class TopicListDiscoverer implements PulsarDiscoverer {

    private final List<String> topics;

    public TopicListDiscoverer(List<String> topics) {
        this.topics = topics;
    }

    @Override
    public Set<TopicPartition> getSubscribedTopicPartitions(PulsarAdmin pulsarAdmin) {
        return topics.parallelStream()
            .map(topicName -> {
                String completeTopicName = TopicName.get(topicName).getPartitionedTopicName();
                try {
                    PartitionedTopicMetadata metadata =
                        pulsarAdmin.topics().getPartitionedTopicMetadata(completeTopicName);
                    return PulsarDiscoverer.toTopicPartitions(topicName, metadata.partitions);
                } catch (PulsarAdminException e) {
                    // This method would cause the failure for subscriber.
                    throw new PulsarConnectorException(PulsarConnectorErrorCode.SUBSCRIBE_TOPIC_FAILED, e);
                }
            })
            .filter(Objects::nonNull)
            .flatMap(Collection::stream)
            .collect(Collectors.toSet());
    }
}
