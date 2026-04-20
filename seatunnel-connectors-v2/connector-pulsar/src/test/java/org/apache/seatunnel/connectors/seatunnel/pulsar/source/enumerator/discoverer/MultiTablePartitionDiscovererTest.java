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
import org.apache.seatunnel.common.utils.SerializationUtils;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.topic.TopicPartition;

import org.apache.pulsar.client.admin.PulsarAdmin;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

class MultiTablePartitionDiscovererTest {

    @Test
    void shouldKeepFirstMatchingPatternWhenTopicMatchesMultipleTables() {
        TopicPartition sharedTopic =
                new TopicPartition("persistent://public/default/orders-vip", 0);
        TopicPartition generalTopic =
                new TopicPartition("persistent://public/default/orders-standard", 0);

        MultiTablePartitionDiscoverer discoverer =
                new MultiTablePartitionDiscoverer(
                        Arrays.asList(
                                new MultiTablePartitionDiscoverer.TableDiscovererPair(
                                        TablePath.of("db.orders_general"),
                                        new TestingDiscoverer(sharedTopic, generalTopic),
                                        true),
                                new MultiTablePartitionDiscoverer.TableDiscovererPair(
                                        TablePath.of("db.orders_vip"),
                                        new TestingDiscoverer(sharedTopic),
                                        true)));

        Set<TopicPartition> partitions = discoverer.getSubscribedTopicPartitions(null);

        Assertions.assertEquals(2, partitions.size());
        Assertions.assertEquals(
                TablePath.of("db.orders_general"), discoverer.getTablePath(sharedTopic));
        Assertions.assertEquals(
                TablePath.of("db.orders_general"), discoverer.getTablePath(generalTopic));
    }

    @Test
    void shouldAssignNonOverlappingTopicsToLaterPatterns() {
        TopicPartition generalTopic =
                new TopicPartition("persistent://public/default/orders-standard", 0);
        TopicPartition vipTopic = new TopicPartition("persistent://public/default/orders-vip", 0);

        MultiTablePartitionDiscoverer discoverer =
                new MultiTablePartitionDiscoverer(
                        Arrays.asList(
                                new MultiTablePartitionDiscoverer.TableDiscovererPair(
                                        TablePath.of("db.orders_general"),
                                        new TestingDiscoverer(generalTopic),
                                        true),
                                new MultiTablePartitionDiscoverer.TableDiscovererPair(
                                        TablePath.of("db.orders_vip"),
                                        new TestingDiscoverer(vipTopic),
                                        true)));

        discoverer.getSubscribedTopicPartitions(null);

        Assertions.assertEquals(
                TablePath.of("db.orders_general"), discoverer.getTablePath(generalTopic));
        Assertions.assertEquals(TablePath.of("db.orders_vip"), discoverer.getTablePath(vipTopic));
    }

    @Test
    void shouldSerializeMultiTableDiscovererForSparkExecution() {
        MultiTablePartitionDiscoverer discoverer =
                new MultiTablePartitionDiscoverer(
                        Arrays.asList(
                                new MultiTablePartitionDiscoverer.TableDiscovererPair(
                                        TablePath.of("db.orders"),
                                        new TestingDiscoverer(
                                                new TopicPartition(
                                                        "persistent://public/default/orders", 0)),
                                        false),
                                new MultiTablePartitionDiscoverer.TableDiscovererPair(
                                        TablePath.of("db.users"),
                                        new TestingDiscoverer(
                                                new TopicPartition(
                                                        "persistent://public/default/users", 0)),
                                        false)));

        Assertions.assertDoesNotThrow(() -> SerializationUtils.serialize(discoverer));
    }

    private static final class TestingDiscoverer implements PulsarDiscoverer {
        private static final long serialVersionUID = 1L;

        private final Set<TopicPartition> partitions;

        private TestingDiscoverer(TopicPartition... partitions) {
            this.partitions =
                    Collections.unmodifiableSet(new LinkedHashSet<>(Arrays.asList(partitions)));
        }

        @Override
        public Set<TopicPartition> getSubscribedTopicPartitions(PulsarAdmin pulsarAdmin) {
            return partitions;
        }
    }
}
