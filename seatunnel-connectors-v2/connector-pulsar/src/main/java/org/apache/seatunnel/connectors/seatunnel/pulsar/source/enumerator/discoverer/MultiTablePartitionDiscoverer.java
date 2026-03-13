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

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.pulsar.exception.PulsarConnectorException;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.topic.TopicPartition;

import org.apache.pulsar.client.admin.PulsarAdmin;

import lombok.Getter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MultiTablePartitionDiscoverer implements PulsarDiscoverer {

    private static final long serialVersionUID = 7777745279743885587L;

    private final List<TableDiscovererPair> discovererPairs;
    private final Map<TopicPartition, TablePath> partitionToTablePath = new HashMap<>();

    public MultiTablePartitionDiscoverer(List<TableDiscovererPair> discovererPairs) {
        this.discovererPairs = discovererPairs;
    }

    @Override
    public Set<TopicPartition> getSubscribedTopicPartitions(PulsarAdmin admin) {
        Set<TopicPartition> allPartitions = new HashSet<>();
        partitionToTablePath.clear();

        for (TableDiscovererPair pair : discovererPairs) {
            Set<TopicPartition> partitions = pair.discoverer.getSubscribedTopicPartitions(admin);
            for (TopicPartition tp : partitions) {
                TablePath existing = partitionToTablePath.put(tp, pair.tablePath);
                if (existing != null && !existing.equals(pair.tablePath)) {
                    throw new PulsarConnectorException(
                            SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                            String.format(
                                    "TopicPartition '%s' matched by both '%s' and '%s'",
                                    tp, existing, pair.tablePath));
                }
            }
            allPartitions.addAll(partitions);
        }
        return allPartitions;
    }

    public TablePath getTablePath(TopicPartition partition) {
        return partitionToTablePath.get(partition);
    }

    public boolean hasTopicPattern() {
        return discovererPairs.stream().anyMatch(TableDiscovererPair::isTopicPattern);
    }

    public static class TableDiscovererPair {
        public final TablePath tablePath;
        public final PulsarDiscoverer discoverer;
        @Getter public final boolean topicPattern;

        public TableDiscovererPair(
                TablePath tablePath, PulsarDiscoverer discoverer, boolean topicPattern) {
            this.tablePath = tablePath;
            this.discoverer = discoverer;
            this.topicPattern = topicPattern;
        }
    }
}
