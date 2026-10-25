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

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.topic.TopicPartition;

import org.apache.pulsar.client.admin.PulsarAdmin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Getter;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Resolves multi-table topic ownership for Pulsar source.
 *
 * <p>The discoverer pairs are evaluated in {@code tables_configs} declaration order. If multiple
 * {@code topic-pattern} entries match the same topic, the first matching table keeps ownership and
 * later matches are ignored. This makes topic routing deterministic even when new topics appear
 * after the job has started.
 */
public class MultiTablePartitionDiscoverer implements PulsarDiscoverer {

    private static final long serialVersionUID = 7777745279743885587L;
    private static final Logger LOG = LoggerFactory.getLogger(MultiTablePartitionDiscoverer.class);

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
                TablePath existing = partitionToTablePath.putIfAbsent(tp, pair.tablePath);
                if (existing != null && !existing.equals(pair.tablePath)) {
                    LOG.warn(
                            "TopicPartition '{}' matched by multiple table configs. Keeping '{}' and ignoring '{}'.",
                            tp,
                            existing,
                            pair.tablePath);
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

    public static class TableDiscovererPair implements Serializable {
        private static final long serialVersionUID = -7486130997031326385L;

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
