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

package org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.discoverer;

import org.apache.seatunnel.connectors.seatunnel.pulsar.exception.PulsarConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.pulsar.exception.PulsarConnectorException;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.topic.TopicPartition;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class TopicPatternDiscoverer implements PulsarDiscoverer {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(TopicPatternDiscoverer.class);

    private final Pattern topicPattern;
    private final RegexSubscriptionMode subscriptionMode;
    private final String namespace;

    public TopicPatternDiscoverer(Pattern topicPattern) {
        this.topicPattern = topicPattern;

        this.subscriptionMode = RegexSubscriptionMode.AllTopics;
        // Extract the namespace from topic pattern regex.
        // If no namespace provided in the regex, we would directly use "default" as the namespace.
        TopicName destination = TopicName.get(topicPattern.toString());
        NamespaceName namespaceName = destination.getNamespaceObject();
        this.namespace = namespaceName.toString();
    }

    @Override
    public Set<TopicPartition> getSubscribedTopicPartitions(PulsarAdmin pulsarAdmin) {
        LOG.debug("Fetching descriptions for all topics on pulsar cluster");
        try {
            return pulsarAdmin
                .namespaces()
                .getTopics(namespace)
                .parallelStream()
                .filter(this::matchesSubscriptionMode)
                .filter(topic -> topicPattern.matcher(topic).find())
                .map(topicName -> {
                    String completeTopicName = TopicName.get(topicName).getPartitionedTopicName();
                    try {
                        PartitionedTopicMetadata metadata =
                            pulsarAdmin.topics().getPartitionedTopicMetadata(completeTopicName);
                        return PulsarDiscoverer.toTopicPartitions(topicName, metadata.partitions);
                    } catch (PulsarAdminException e) {
                        // This method would cause the failure for subscriber.
                        throw new PulsarConnectorException(PulsarConnectorErrorCode.GET_TOPIC_PARTITION_FAILED, e);
                    }
                }).filter(Objects::nonNull)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
        } catch (PulsarAdminException e) {
            // This method would cause the failure for subscriber.
            throw new PulsarConnectorException(PulsarConnectorErrorCode.GET_TOPIC_PARTITION_FAILED, e);
        }
    }

    private boolean matchesSubscriptionMode(String topic) {
        TopicName topicName = TopicName.get(topic);
        // Filter the topic persistence.
        switch (subscriptionMode) {
            case PersistentOnly:
                return topicName.isPersistent();
            case NonPersistentOnly:
                return !topicName.isPersistent();
            default:
                // RegexSubscriptionMode.AllTopics
                return true;
        }
    }
}
